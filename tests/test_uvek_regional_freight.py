"""Regional freight corrections retain exact tkm, rail shares and caller ownership."""

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
from scripts import review_uvek_digester_stock as digester_stock
from scripts import review_uvek_regional_freight as review

POLICY = MigrationPolicy(on_heuristic_mapping=PolicyAction.WARN, on_information_loss=PolicyAction.WARN)


@pytest.mark.parametrize("version", ("3.11", "3.12"))
@pytest.mark.parametrize("release", ("2025", "2026", "native2026"))
@pytest.mark.parametrize("amount", (-2, 0, 2))
def test_complete_road_services_and_swiss_rail_shares_preserve_caller_state(version, release, amount):
    data = None
    for row in review.SOURCES.values():
        for kind, uncertainty in (("technosphere", 2), ("substitution", 3)):
            example = consumer(row["source"], amount, uncertainty, kind)
            example[0]["exchanges"][1]["caller_metadata"] = {"keep": [1, 2]}
            if data is None:
                data = example
            else:
                data[0]["exchanges"].append(example[0]["exchanges"][1])
    data[0]["exchanges"].append(
        {
            "name": "Carbon dioxide, fossil",
            "categories": ["air"],
            "unit": "kilogram",
            "type": "biosphere",
            "amount": 0.5,
        }
    )
    before = deepcopy(data)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", version))
    result = inventory.migrate_background(target_context(release), policy=POLICY)
    assert len(result.data) == 2
    helper = result.data[1]
    for old, new in zip(before[0]["exchanges"][1:-1], result.data[0]["exchanges"][1:-1], strict=True):
        row = review.SOURCES[review.identity(old)]
        assert new["amount"] == old["amount"] == amount
        assert all(new.get(k) == v for k, v in old.items() if k not in (*review.FIELDS, "input"))
        if row["kind"] == "heavy_road":
            assert {f: new[f] for f in review.FIELDS} == row["target"]
        else:
            assert new["input"][1] == helper["code"]
            (profile,) = [p for p in row["profiles"] if p["label"] == f"ecoinvent-{version}-cutoff"]
            assert helper["brightpath_conversion"]["recipe_id"] == profile["target"]["recipe_ref"]
            assert [{f: e[f] for f in (*review.FIELDS, "type", "amount")} for e in helper["exchanges"][1:]] == profile[
                "inputs"
            ]
            assert len(helper["exchanges"]) == 3
    assert result.data[0]["exchanges"][0] == before[0]["exchanges"][0]
    assert result.data[0]["exchanges"][-1]["amount"] == 0.5
    assert any(i.code == "migration.technosphere_information_loss" for i in result.last_migration_report.issues)
    assert inventory.data == data == before
    assert result.migrate_background(target_context(release)).data == result.data


@pytest.mark.parametrize("version", ("3.11", "3.12"))
@pytest.mark.parametrize("release", ("2025", "2026", "native2026"))
@pytest.mark.parametrize("amount", (-2, 0, 2))
def test_all_424_affected_parent_profiles_keep_signed_demand_and_complete_graph(version, release, amount):
    rows = [r for r in review.APPROVAL["parent_profiles"] if r["label"] == f"ecoinvent-{version}-cutoff"]
    assert len(rows) == {"3.11": 211, "3.12": 213}[version]
    data = None
    for row in rows:
        example = consumer(row["source"], amount, 3)
        if data is None:
            data = example
        else:
            data[0]["exchanges"].append(example[0]["exchanges"][1])
    before = deepcopy(data)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", version))
    result = inventory.migrate_background(target_context(release), policy=POLICY)
    helpers = {r["code"]: r for r in result.data[1:]}
    resource = json.loads((review.ROOT / "brightpath/data/migrations/uvek/ecoinvent-to-uvek-2025.json").read_text())
    selected = select_profile_sign_rules(resource, TechnosphereProfile("ecoinvent", version, "cutoff"))
    rules = {review.identity(r["source"]): r for r in selected["replace"]}
    for old, new, row in zip(before[0]["exchanges"][1:], result.data[0]["exchanges"][1:], rows, strict=True):
        rule = rules[review.identity(row["source"])]
        factor = rule["conversion_factor"]
        assert factor in (-1, 1) and new["amount"] == amount * factor
        helper = helpers[new["input"][1]]
        replacement = digester_stock.SOURCES.get(review.identity(row["source"]))
        expected_recipe = (
            next(p["recipe_id"] for p in replacement["profiles"] if p["label"] == row["label"])
            if replacement
            else row["target_recipe"]
        )
        assert helper["brightpath_conversion"]["recipe_id"] == expected_recipe
        assert new["formula"] == ("dose" if factor == 1 else "(dose) * (-1.0)")
        assert new["loc"] == old["loc"] * factor and new["scale"] == old["scale"]
    assert inventory.data == data == before


@pytest.mark.parametrize("release", ("2025", "native2026"))
def test_strict_consent_consequential_unknown_profiles_and_foreground_ownership(release):
    rows = list(review.SOURCES.values())
    for row in rows:
        data = consumer(row["source"], -2, 3)
        before = deepcopy(data)
        for version, model, policy in (
            ("3.12", "cutoff", MigrationPolicy.strict()),
            ("3.12", "consequential", MigrationPolicy.permissive()),
            ("3.10", "cutoff", MigrationPolicy.permissive()),
        ):
            inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", version, model))
            with pytest.raises(MigrationError):
                inventory.migrate_background(target_context(release), policy=policy)
            assert inventory.data == before
        data.append({**row["source"], "exchanges": [{**row["source"], "type": "production", "amount": 1}]})
        inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.12"))
        assert inventory.migrate_background(target_context(release), policy=POLICY).data == data


def evidence():
    inventories = defaultdict(list)
    observations = {"ecoinvent": defaultdict(set), "uvek": defaultdict(set)}
    for row in review.APPROVAL["inventories"]:
        source = review.identity(row["source"])
        inventories[source].append({**deepcopy(row), "source": list(source)})
        observations[row["label"].split("-")[0]][source].add((row["label"], row["reference_amount"]))
    native = {review.identity(r["source"]): deepcopy(r) for r in review.APPROVAL["native_roles"]}
    return inventories, observations, native


@pytest.mark.parametrize(
    "mutation", ("none", "inventory", "metadata", "missing", "duplicate", "negative", "native_waste")
)
def test_full_source_target_and_parent_inventories_cannot_drift(mutation):
    inventories, observations, native = evidence()
    source = next(iter(review.SOURCES))
    if mutation == "inventory":
        inventories[source][0]["inventory_sha256"] = "0" * 64
    elif mutation == "metadata":
        inventories[source][0]["activity_comment_sha256"] = "0" * 64
    elif mutation == "missing":
        inventories[source].pop()
    elif mutation == "duplicate":
        inventories[source].append(deepcopy(inventories[source][0]))
    elif mutation == "negative":
        observations["ecoinvent"][source] = {(label, -1) for label, _ in observations["ecoinvent"][source]}
    elif mutation == "native_waste":
        native[next(iter(native))]["native_flow_type"] = "WASTE_FLOW"
    if mutation == "none":
        review.validate_inventories(inventories, observations, native)
    else:
        with pytest.raises(ValueError, match="repeat the scientific review"):
            review.validate_inventories(inventories, observations, native)


@pytest.mark.parametrize("mutation", ("parent", "parent_recipe", "custom_recipe"))
def test_partial_parent_propagation_or_custom_vector_changes_are_rejected(mutation):
    resource = json.loads((review.ROOT / "brightpath/data/migrations/uvek/ecoinvent-to-uvek-2025.json").read_text())
    review.validate_bindings(resource)
    if mutation == "parent":
        source = review.APPROVAL["parent_rule_bindings"][0]["source"]
        rule = next(r for r in resource["replace"] if r["source"] == source)
        version = review.APPROVAL["parent_rule_bindings"][0]["label"].split("-")[1]
        profile = next(
            p for p in rule["reference_flow_sign_review"]["profiles"] if p["source_profile"]["version"] == version
        )
        profile["conversion_factor"] *= -1
    elif mutation == "parent_recipe":
        rid = review.APPROVAL["parent_profiles"][0]["target_recipe"]
        resource["recipes"][rid]["inputs"][0]["amount"] *= 2
    else:
        rid = next(iter(review.APPROVAL["protected_custom_recipe_bindings"]))
        resource["recipes"][rid]["inputs"] = []
    with pytest.raises(ValueError, match="repeat the scientific review"):
        review.validate_bindings(resource)
