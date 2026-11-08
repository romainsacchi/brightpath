"""Keep waste-specific treatment, exact omitted handoffs and complete multimodal freight."""

from collections import defaultdict
from copy import deepcopy

import pytest
from test_uvek_reference_signs import FIELDS, consumer, contexts
from test_uvek_refrigerant_disposal import target_context

from brightpath import BackgroundContext, BiosphereProfile, BrightwayInventory, TechnosphereProfile
from brightpath.background.reference_signs import select_profile_sign_rules
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource
from scripts import review_uvek_additional_cutoff_waste_markets as review

POLICY = MigrationPolicy(on_heuristic_mapping=PolicyAction.WARN, on_information_loss=PolicyAction.WARN)
VERSIONS = ("3.7", "3.8", "3.9", "3.9.1", "3.10", "3.10.1", "3.11", "3.12")


def profiles(version):
    label = f"ecoinvent-{version}-cutoff"
    return {source: p for source, row in review.SOURCES.items() for p in row["profiles"] if p["label"] == label}


def batch(version, amount):
    data = None
    for source in profiles(version):
        for kind, distribution in (("technosphere", 2), ("substitution", 3)):
            part = consumer(dict(zip(FIELDS, source, strict=True)), amount, distribution, kind)
            part[0]["exchanges"][1]["caller_metadata"] = {"retain": [1, 2]}
            if data is None:
                data = part
            else:
                data[0]["exchanges"].append(part[0]["exchanges"][1])
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
def test_signed_markets_keep_original_treatment_fractions_freight_and_foreground_fields(version, release, amount):
    data = batch(version, amount)
    original = deepcopy(data)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", version))
    result = inventory.migrate_background(target_context(release), policy=POLICY)
    by_code = {d["code"]: d for d in result.data[1:]}
    by_recipe = {d["brightpath_conversion"]["recipe_id"]: d for d in result.data[1:]}
    assert result.data[0]["exchanges"][0] == original[0]["exchanges"][0]
    assert result.data[0]["exchanges"][-1] == original[0]["exchanges"][-1]
    for before, after in zip(original[0]["exchanges"][1:-1], result.data[0]["exchanges"][1:-1], strict=True):
        assert after["amount"] == -before["amount"] and after["type"] == before["type"]
        assert after["formula"] == "(dose) * (-1.0)" and after["scale"] == before["scale"]
        assert after["caller_metadata"] == before["caller_metadata"]
        if before["uncertainty type"] == 2:
            assert after["loc"] == before["loc"] and after["negative"] is not before["negative"]
        else:
            assert after["loc"] == -before["loc"]
            assert (after["minimum"], after["maximum"]) == (-before["maximum"], -before["minimum"])
        profile = profiles(version)[tuple(before[f] for f in FIELDS)]
        parent = by_code[after["input"][1]]
        assert parent["brightpath_conversion"]["recipe_id"].startswith("additional-cutoff-waste-market-")
        assert not any(e["type"] == "biosphere" for e in parent["exchanges"])
        actual = [e for e in parent["exchanges"] if e["type"] != "production"]
        assert len(actual) == len(profile["inputs"])
        for expected, exchange in zip(profile["inputs"], actual, strict=True):
            assert exchange["amount"] == expected["amount"]
            if "recipe" in expected:
                supplier = by_recipe[expected["recipe"]]
                assert tuple(exchange[f] for f in FIELDS) == tuple(supplier[f] for f in FIELDS)
                if "input" in exchange:
                    assert by_code[exchange["input"][1]] is supplier
            else:
                assert tuple(exchange[f] for f in FIELDS) == tuple(expected[f] for f in FIELDS)
        for omitted in profile["omissions"]:
            assert "omit signed" in parent["comment"] and str(omitted["amount"]) in parent["comment"]
            assert not any(e["name"] == omitted["source"][0] for e in parent["exchanges"])
        assert all(e["amount"] > 0 for e in actual)
    assert not any(
        "aircraft" in e["name"] or "steam" in e["name"] or "operation, lorry" in e["name"].lower()
        for d in result.data[1:]
        for e in d["exchanges"]
        if e["type"] == "technosphere"
    )
    assert result.last_migration_report.losses and inventory.data == data == original


def test_source_shares_are_not_normalized_around_the_omitted_recovery_branch():
    result = BrightwayInventory.from_data(batch("3.12", -1), context=contexts("ecoinvent", "3.12")).migrate_background(
        target_context("2025"), policy=POLICY
    )
    helpers = {d["code"]: d for d in result.data[1:]}
    for e in result.data[0]["exchanges"][1:-1]:
        assert e["amount"] == 1
        parent = helpers[e["input"][1]]
        assert not parent["brightpath_conversion"].get("zero_burden")
    assert any(
        e["type"] == "technosphere" and e["unit"] == "kilogram" and 0 < e["amount"] < 1
        for d in result.data[1:]
        if d["brightpath_conversion"]["recipe_id"].startswith("additional-cutoff-waste-market-")
        for e in d["exchanges"]
    )
    assert any(e["type"] == "biosphere" and e["amount"] > 0 for d in result.data[1:] for e in d["exchanges"])


@pytest.mark.parametrize("version", VERSIONS)
def test_canonical_graph_keeps_independent_reverse_suppliers_and_all_generated_foreground_owners(version):
    canonical = BrightwayInventory.from_data(
        batch(version, -1), context=contexts("ecoinvent", version)
    ).migrate_background(target_context("2025"), policy=POLICY)
    original = deepcopy(canonical.data)
    result = canonical.migrate_background(
        BackgroundContext(TechnosphereProfile("ecoinvent", "3.12", "cutoff"), BiosphereProfile("ecoinvent", "3.10")),
        policy=MigrationPolicy(
            on_heuristic_mapping=PolicyAction.WARN,
            on_information_loss=PolicyAction.WARN,
            on_inferred_reverse=PolicyAction.WARN,
        ),
    )
    assert result.data[0] == original[0] and len(result.data) == len(original)
    assert [d["code"] for d in result.data[1:]] == [d["code"] for d in original[1:]]
    assert canonical.data == original


@pytest.mark.parametrize("release", ("2025", "2026", "native2026"))
def test_all_24_positive_steam_profiles_stay_blocked_transactionally_under_permissive_policy(release):
    count = 0
    for source, row in review.SOURCES.items():
        for profile in row["pending_profiles"]:
            count += 1
            version = profile["label"].split("-")[1]
            data = consumer(dict(zip(FIELDS, source, strict=True)), -1)
            original = deepcopy(data)
            inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", version))
            with pytest.raises(MigrationError) as caught:
                inventory.migrate_background(target_context(release), policy=MigrationPolicy.permissive())
            assert any(i.code == "migration.reference_flow_sign_unresolved" for i in caught.value.report.issues)
            assert inventory.data == original == data
    assert count == 24


def test_native_physical_biosphere_gaps_reject_without_extending_other_flow_approvals():
    data = batch("3.12", -1)
    original = deepcopy(data)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.12"))
    with pytest.raises(MigrationError) as caught:
        inventory.migrate_background(target_context("native2026"), policy=POLICY)
    assert any("biosphere" in i.code and "unresolved" in i.code for i in caught.value.report.issues)
    assert inventory.data == original == data


def test_strict_loss_refusal_foreground_ownership_and_uninspected_versions_and_models():
    data = batch("3.12", -1)
    original = deepcopy(data)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.12"))
    with pytest.raises(MigrationError):
        inventory.migrate_background(
            target_context("2025"), policy=MigrationPolicy(on_heuristic_mapping=PolicyAction.WARN)
        )
    assert inventory.data == original
    for source in review.SOURCES:
        supplier = dict(zip(FIELDS, source, strict=True))
        data.append({**supplier, "exchanges": [{**supplier, "type": "production", "amount": -1}]})
    assert (
        BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.12"))
        .migrate_background(target_context("2025"), policy=POLICY)
        .data
        == data
    )
    resource = load_uvek_technosphere_resource()
    for version in ("3.6", "3.10.2", "3.13"):
        selected = select_profile_sign_rules(resource, TechnosphereProfile("ecoinvent", version, "cutoff"))
        blocked = [r for r in selected["replace"] if tuple(r["source"][f] for f in FIELDS) in review.SOURCES]
        assert len(blocked) == 14 and all(r["reference_flow_sign_review"]["status"] == "unresolved" for r in blocked)
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(
            batch("3.12", -1), context=contexts("ecoinvent", "3.12", "consequential")
        ).migrate_background(target_context("2025"), policy=MigrationPolicy.permissive())


def inventories():
    data = defaultdict(list)
    observations = {"ecoinvent": defaultdict(set), "uvek": defaultdict(set)}
    for row in review.APPROVAL["inventories"]:
        source = tuple(row["source"][f] for f in FIELDS)
        data[source].append({**deepcopy(row), "source": list(source)})
        observations[row["label"].split("-")[0]][source].add((row["label"], row["reference_amount"]))
    native = {tuple(r["source"][f] for f in FIELDS): deepcopy(r) for r in review.APPROVAL["native_roles"]}
    return data, observations, native


@pytest.mark.parametrize(
    "change",
    (
        "inventory_sha256",
        "activity_comment_sha256",
        "product_information_sha256",
        "missing",
        "duplicate",
        "patch",
        "native_role",
    ),
)
def test_inventory_reference_metadata_patch_and_role_drift_requires_reinspection(change):
    data, observations, native = inventories()
    review.validate_inventories(data, observations, native)
    source = next(iter(data))
    if change in ("inventory_sha256", "activity_comment_sha256", "product_information_sha256"):
        data[source][0][change] = "0" * 64
    elif change == "missing":
        data[source].pop()
    elif change == "duplicate":
        data[source].append(deepcopy(data[source][0]))
    elif change == "patch":
        row = next(r for rows in data.values() for r in rows if "3.10.1" in r["label"])
        row["label"] = "ecoinvent-3.10.2-cutoff"
    else:
        next(iter(native.values()))["native_reference_is_input"] = True
    with pytest.raises(ValueError):
        review.validate_inventories(data, observations, native)


def test_child_recipe_mutation_cannot_inherit_parent_approval():
    resource = deepcopy(load_uvek_technosphere_resource())
    rid = next(iter(review.APPROVAL["recipe_dependencies"]))
    next(e for e in resource["recipes"][rid]["inputs"] if e["amount"] != 0)["amount"] *= 2
    with pytest.raises(ValueError):
        review.validate_bindings(resource)


@pytest.mark.parametrize("change", ("treatment", "omission", "sea", "steam", "full_road"))
def test_changed_treatment_recovery_freight_or_full_service_boundary_is_rejected(monkeypatch, change):
    approval = deepcopy(review.APPROVAL)
    if change == "treatment":
        profile = approval["rules"][0]["profiles"][0]
        next(e for e in profile["inputs"] if "recipe" in e)["amount"] += 0.1
    elif change == "omission":
        omission = next(o for row in approval["rules"] for profile in row["profiles"] for o in profile["omissions"])
        omission["amount"] *= 2
    elif change == "sea":
        choice = next(c for c in approval["reverse_bindings"] if c["source"] == review.SEA)
        choice["source"]["name"] = "Transport, transoceanic freight ship"
    elif change == "steam":
        choice = next(c for c in approval["freight_choices"] if c["kind"] == "rail")
        choice["members"][0]["traction"] = "steam"
    else:
        approval["full_road_service_proofs"][0]["has_maintenance"] = False
    monkeypatch.setattr(review, "APPROVAL", approval)
    with pytest.raises(ValueError):
        review.validate_bindings(load_uvek_technosphere_resource())
