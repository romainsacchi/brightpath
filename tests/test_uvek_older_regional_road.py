"""Old all-size road services retain signed tkm and reviewed parent inventories."""

import json
from collections import defaultdict
from copy import deepcopy

import pytest
from test_uvek_reference_signs import consumer, contexts
from test_uvek_refrigerant_disposal import target_context

from brightpath import BrightwayInventory, TechnosphereProfile
from brightpath.background.reference_signs import select_profile_sign_rules
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from scripts import review_uvek_older_regional_road as review

POLICY = MigrationPolicy(on_information_loss=PolicyAction.WARN, on_heuristic_mapping=PolicyAction.WARN)
VERSIONS = ("3.6", "3.7", "3.8", "3.9", "3.9.1", "3.10", "3.10.1")


def resource():
    return json.loads((review.ROOT / "brightpath/data/migrations/uvek/ecoinvent-to-uvek-2025.json").read_text())


def selected(version):
    return {
        review.identity(r["source"]): r
        for r in select_profile_sign_rules(resource(), TechnosphereProfile("ecoinvent", version, "cutoff"))["replace"]
    }


def batch(version, amount):
    label = f"ecoinvent-{version}-cutoff"
    sources = {
        review.identity(row["source"]): row["source"]
        for row in review.APPROVAL["parent_profiles"]
        if row["label"] == label
    }
    sources.update(
        {
            source: row["source"]
            for source, row in review.SOURCES.items()
            if any(p["label"] == label for p in row["profiles"])
        }
    )
    data = None
    for source in sources.values():
        for kind, uncertainty in (("technosphere", 2), ("substitution", 3)):
            part = consumer(source, amount, uncertainty, kind)
            part[0]["exchanges"][1]["caller_metadata"] = {"retain": [1, 2]}
            if data is None:
                data = part
            else:
                data[0]["exchanges"].extend(part[0]["exchanges"][1:])
    data[0]["exchanges"].append(
        {
            "name": "Carbon dioxide, fossil",
            "categories": ["air"],
            "unit": "kilogram",
            "type": "biosphere",
            "amount": 0.5,
        }
    )
    return data


@pytest.mark.parametrize("version", VERSIONS)
@pytest.mark.parametrize("release", ("2025", "2026"))
@pytest.mark.parametrize("amount", (-2, 0, 2))
def test_every_corrected_profile_retains_exact_parent_graph_and_signed_caller_state(version, release, amount):
    data = batch(version, amount)
    before = deepcopy(data)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", version))
    result = inventory.migrate_background(target_context(release), policy=POLICY)
    chosen = selected(version)
    recipes = resource()["recipes"]
    by_recipe = {d["brightpath_conversion"]["recipe_id"]: d for d in result.data[1:]}
    roots = set()
    assert result.data[0]["exchanges"][0] == before[0]["exchanges"][0]
    assert result.data[0]["exchanges"][-1] == before[0]["exchanges"][-1]
    for old, new in zip(before[0]["exchanges"][1:-1], result.data[0]["exchanges"][1:-1], strict=True):
        rule = chosen[review.identity(old)]
        factor = rule.get("conversion_factor", 1)
        assert new["amount"] == old["amount"] * factor
        assert new["type"] == old["type"] and new["caller_metadata"] == old["caller_metadata"]
        assert new["scale"] == old["scale"]
        if factor == -1:
            assert new["formula"] == "(dose) * (-1.0)"
            if old["uncertainty type"] == 2:
                assert new["negative"] is not old["negative"] and new["loc"] == old["loc"]
            else:
                assert new["loc"] == -old["loc"]
                assert (new["minimum"], new["maximum"]) == (-old["maximum"], -old["minimum"])
        else:
            assert factor == 1
            for field in ("loc", "minimum", "maximum", "negative"):
                if field in old:
                    assert new[field] == old[field]
        if "recipe_ref" in rule["target"]:
            rid = rule["target"]["recipe_ref"]
            roots.add(rid)
            assert new["input"][1] == by_recipe[rid]["code"]
        else:
            assert review.identity(new) == review.identity(rule["target"])
            assert new["name"] == "Transport, freight, lorry, fleet average"
            assert new["unit"] == "ton kilometer" and new["location"] == "RER"
    reachable = set()

    def visit(rid):
        if rid in reachable:
            return
        reachable.add(rid)
        helper = by_recipe[rid]
        assert helper["exchanges"][0]["type"] == "production" and helper["exchanges"][0]["amount"] == 1
        actual = helper["exchanges"][1:]
        expected = recipes[rid]["inputs"]
        assert len(actual) == len(expected)
        for old, new in zip(expected, actual, strict=True):
            assert new["amount"] == old["amount"]
            if "recipe" in old:
                child = old["recipe"]
                assert review.identity(new) == review.identity(by_recipe[child])
                visit(child)
            elif old.get("type") == "biosphere":
                assert (new["name"], new["categories"], new["unit"]) == (old["name"], old["categories"], old["unit"])
            else:
                assert review.identity(new) == review.identity(old)

    for rid in roots:
        visit(rid)
    assert reachable == by_recipe.keys()
    assert result.last_migration_report.losses
    assert inventory.data == data == before


def test_exact_profile_scope_consent_and_foreground_ownership():
    data = batch("3.10", -2)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.10"))
    with pytest.raises(MigrationError):
        inventory.migrate_background(target_context("2025"), policy=MigrationPolicy.strict())
    assert inventory.data == data
    for row in review.SOURCES.values():
        owned = consumer(row["source"], -2)
        owned.append({**row["source"], "exchanges": [{**row["source"], "type": "production", "amount": 1}]})
        assert (
            BrightwayInventory.from_data(owned, context=contexts("ecoinvent", "3.10"))
            .migrate_background(target_context("2025"), policy=POLICY)
            .data
            == owned
        )
    for version in ("3.10.2", "3.11", "3.12", "3.13"):
        for source in review.SOURCES:
            assert selected(version)[source]["reference_flow_sign_review"]["status"] == "unresolved"
    with pytest.raises(MigrationError, match="system models"):
        BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.10", "consequential")).migrate_background(
            target_context("2025"), policy=MigrationPolicy.permissive()
        )


@pytest.mark.parametrize("change", ("none", "inventory", "description", "missing", "duplicate", "sign", "native_role"))
def test_full_source_and_native_role_evidence_drift_requires_review(change):
    inventories = defaultdict(list)
    observations = {"ecoinvent": defaultdict(set), "uvek": defaultdict(set)}
    for row in review.APPROVAL["inventories"]:
        source = review.identity(row["source"])
        inventories[source].append({**deepcopy(row), "source": list(source)})
        observations[row["label"].split("-")[0]][source].add((row["label"], row["reference_amount"]))
    native = {review.identity(r["source"]): deepcopy(r) for r in review.APPROVAL["native_roles"]}
    source = next(iter(review.SOURCES))
    if change == "inventory":
        inventories[source][0]["inventory_sha256"] = "0" * 64
    elif change == "description":
        inventories[source][0]["product_information_sha256"] = "0" * 64
    elif change == "missing":
        inventories[source].pop()
    elif change == "duplicate":
        inventories[source].append(deepcopy(inventories[source][0]))
    elif change == "sign":
        observations["ecoinvent"][source] = {(label, -sign) for label, sign in observations["ecoinvent"][source]}
    elif change == "native_role":
        role = native[next(iter(native))]
        role["native_reference_is_input"] = not role["native_reference_is_input"]
    if change == "none":
        review.validate_inventories(inventories, observations, native)
    else:
        with pytest.raises(ValueError, match="repeat the scientific review"):
            review.validate_inventories(inventories, observations, native)


@pytest.mark.parametrize("change", ("parent_binding", "parent_body", "protected_body"))
def test_partial_propagation_or_changed_protected_recipe_requires_review(change):
    value = resource()
    review.validate_bindings(value)
    if change == "parent_binding":
        row = review.APPROVAL["parent_rule_bindings"][0]
        rule = next(r for r in value["replace"] if r["source"] == row["source"])
        entry = next(
            p
            for p in rule["reference_flow_sign_review"]["profiles"]
            if "-".join(p["source_profile"][f] for f in ("family", "version", "system_model")) == row["label"]
        )
        entry["conversion_factor"] *= -1
    elif change == "parent_body":
        value["recipes"][review.APPROVAL["parent_profiles"][0]["target_recipe"]]["inputs"][0]["amount"] += 1
    else:
        value["recipes"][next(iter(review.APPROVAL["protected_custom_recipe_bindings"]))]["inputs"][0]["amount"] += 1
    with pytest.raises(ValueError, match="repeat the scientific review"):
        review.validate_bindings(value)
