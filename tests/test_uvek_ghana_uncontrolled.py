"""Preserve uncontrolled refrigerator emissions, recovery credits and treatment demand."""

from collections import defaultdict
from copy import deepcopy

import pytest
from test_uvek_reference_signs import FIELDS, consumer, contexts
from test_uvek_refrigerant_disposal import target_context

from brightpath import BrightwayInventory, TechnosphereProfile
from brightpath.background.reference_signs import select_profile_sign_rules
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource
from scripts import review_uvek_ghana_uncontrolled as review

POLICY = MigrationPolicy(on_heuristic_mapping=PolicyAction.WARN, on_information_loss=PolicyAction.WARN)
CASES = [(source, p) for source, row in review.SOURCES.items() for p in row["profiles"]]
QUANTITY_FIELDS = ("amount", "uncertainty type", "loc", "scale", "shape", "negative", "minimum", "maximum", "formula")


def evidence():
    records = deepcopy(review.APPROVAL["inventories"])
    observations = defaultdict(lambda: defaultdict(set))
    review.extend_observations(records, observations)
    return records, observations


def root_and_dependencies(result, profile):
    helpers = {d["brightpath_conversion"]["recipe_id"]: d for d in result.data if "brightpath_conversion" in d}
    return helpers[profile["recipe_id"]], helpers


@pytest.mark.parametrize("source,profile", CASES)
@pytest.mark.parametrize("release", ("2025", "2026"))
@pytest.mark.parametrize("amount", (-2, 0, 2))
def test_all_emissions_credits_and_treatment_inputs_survive_signed_demand(source, profile, release, amount):
    version = profile["label"].split("-")[1]
    data = consumer(dict(zip(FIELDS, source, strict=True)), amount, 2)
    data[0]["exchanges"].append(
        consumer(dict(zip(FIELDS, source, strict=True)), amount, 3, "substitution")[0]["exchanges"][1]
    )
    data[0]["exchanges"].append(
        {
            "name": "Carbon dioxide, fossil",
            "categories": ["air"],
            "unit": "kilogram",
            "amount": 0.5,
            "type": "biosphere",
        }
    )
    before = deepcopy(data)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", version))
    result = inventory.migrate_background(target_context(release), policy=POLICY)
    root, helpers = root_and_dependencies(result, profile)
    assert len(result.data) == 4 and len(helpers) == 3
    assert root["unit"] == "unit" and root["location"] == "GH"
    assert root["exchanges"][0]["type"] == "production" and root["exchanges"][0]["amount"] == 1
    for old, new in zip(before[0]["exchanges"][1:-1], result.data[0]["exchanges"][1:-1], strict=True):
        assert new["amount"] == -amount and new["type"] == old["type"]
        assert new["formula"] == "(dose) * (-1.0)" and new["scale"] == old["scale"]
        if old["uncertainty type"] == 2:
            assert new["loc"] == old["loc"] and new["negative"] is not old["negative"]
        else:
            assert new["loc"] == -old["loc"]
            assert (new["minimum"], new["maximum"]) == (-old["maximum"], -old["minimum"])
        assert new["input"][1] == root["code"]
    actual = root["exchanges"][1:]
    assert len(actual) == 12 and sum(e["type"] == "biosphere" for e in actual) == 5
    for expected, final in zip(profile["recipe"]["inputs"], actual, strict=True):
        assert all(final.get(f) == expected.get(f) for f in QUANTITY_FIELDS)
        if "recipe" in expected:
            assert expected["amount"] < 0
            assert all(final[f] == helpers[expected["recipe"]][f] for f in FIELDS)
            assert len(helpers[expected["recipe"]]["exchanges"]) == 1
        elif expected["type"] == "biosphere":
            assert all(final[f] == expected[f] for f in ("name", "categories", "unit"))
        else:
            assert expected["amount"] > 0
            assert all(final[f] == expected[f] for f in FIELDS)
    assert result.data[0]["exchanges"][0] == before[0]["exchanges"][0]
    assert result.data[0]["exchanges"][-1]["amount"] == 0.5
    assert result.last_migration_report.losses
    assert not any(i.code.endswith("link_unresolved") for i in result.last_migration_report.issues)
    assert not any(i.code == "migration.biosphere_omission" for i in result.last_migration_report.losses)
    assert inventory.data == data == before
    assert result.migrate_background(target_context(release)).data == result.data


@pytest.mark.parametrize("source,profile", CASES)
def test_native_preserves_physical_emissions_or_refuses_missing_definitions_transactionally(source, profile):
    data = consumer(dict(zip(FIELDS, source, strict=True)), -2, 3)
    before = deepcopy(data)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", profile["label"].split("-")[1]))
    permissive = inventory.migrate_background(target_context("native2026"), policy=MigrationPolicy.permissive())
    root, _ = root_and_dependencies(permissive, profile)
    old = [e for e in profile["recipe"]["inputs"] if e.get("type") == "biosphere"]
    new = [e for e in root["exchanges"] if e.get("type") == "biosphere"]
    assert len(old) == len(new) == 5
    for a, b in zip(old, new, strict=True):
        assert b["unit"] == a["unit"] == "kilogram"
        assert all(a.get(f) == b.get(f) for f in QUANTITY_FIELDS)
    unresolved = [i for i in permissive.last_migration_report.issues if i.code.endswith("link_unresolved")]
    if unresolved:
        with pytest.raises(MigrationError) as caught:
            inventory.migrate_background(target_context("native2026"), policy=POLICY)
        assert any(i.code.endswith("link_unresolved") for i in caught.value.report.issues)
    else:
        assert inventory.migrate_background(target_context("native2026"), policy=POLICY).data == permissive.data
    assert not any(i.code == "migration.biosphere_omission" for i in permissive.last_migration_report.losses)
    assert inventory.data == data == before


@pytest.mark.parametrize(
    "mutation", ("missing", "duplicate", "hash", "formula", "metadata", "sign", "profile", "canonical", "reverse")
)
def test_changed_complete_source_boundary_or_identity_dependencies_require_new_review(mutation, monkeypatch, tmp_path):
    records, observations = evidence()
    review.validate_inventories(records, observations)
    if mutation == "missing":
        records.pop()
    elif mutation == "duplicate":
        records[-1] = records[0]
    elif mutation == "hash":
        records[0]["inventory_sha256"] = "0" * 64
    elif mutation == "formula":
        records[0]["formula_sha256"] = "0" * 64
    elif mutation == "metadata":
        records[0]["activity_comment_sha256"] = "0" * 64
    elif mutation == "sign":
        source = next(iter(review.SOURCES))
        observations["ecoinvent"][source].add((review.SOURCES[source]["profiles"][0]["label"], 1))
    elif mutation == "profile":
        observations["ecoinvent"][next(iter(review.SOURCES))].add(("ecoinvent-3.12.1-cutoff", -1))
    else:
        for path in review.APPROVAL["canonical_identity_dependency_sha256"]:
            target = tmp_path / path
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes((review.ROOT / path).read_bytes())
        if mutation == "canonical":
            path = next(iter(review.APPROVAL["canonical_identity_dependency_sha256"]))
        else:
            path = review.APPROVAL["independent_reverse"]["resource_path"]
        target = tmp_path / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text("changed")
        monkeypatch.setattr(review, "ROOT", tmp_path)
    with pytest.raises(ValueError, match="changed"):
        review.validate_inventories(records, observations)


@pytest.mark.parametrize("mutation", ("supplier", "factor", "observations"))
def test_projection_rejects_unreviewed_supplier_reference_or_profile(mutation):
    records, observations = evidence()
    source, row = next(iter(review.SOURCES.items()))
    rule = {"source": row["source"], "target": deepcopy(row["previous_target"])}
    if mutation == "supplier":
        rule["target"]["name"] = "Controlled dismantling is not uncontrolled dismantling"
    elif mutation == "factor":
        rule["conversion_factor"] = -1
    else:
        observations["ecoinvent"][source].add(("ecoinvent-3.12.1-cutoff", -1))
    with pytest.raises(ValueError, match="changed"):
        review.forward_decision(rule, observations["ecoinvent"][source], "test binding", None)


@pytest.mark.parametrize("source", sorted(review.SOURCES))
def test_foreground_ownership_and_uninspected_contexts_remain_protected(source):
    data = consumer(dict(zip(FIELDS, source, strict=True)), -2, 3)
    before = deepcopy(data)
    for version, model in (("3.12", "consequential"), ("3.9.2", "cutoff")):
        inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", version, model))
        with pytest.raises(MigrationError):
            inventory.migrate_background(target_context("2025"), policy=MigrationPolicy.permissive())
        assert inventory.data == data == before
    selected = select_profile_sign_rules(
        load_uvek_technosphere_resource(), TechnosphereProfile("ecoinvent", "3.12.1", "cutoff")
    )
    rule = next(r for r in selected["replace"] if review.identity(r["source"]) == source)
    assert rule["reference_flow_sign_review"]["status"] == "unresolved"
    supplier = dict(zip(FIELDS, source, strict=True))
    data.append({**supplier, "exchanges": [{**supplier, "amount": -1, "type": "production"}]})
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.12")).migrate_background(
        target_context("2025"), policy=POLICY
    )
    assert result.data == data


@pytest.mark.parametrize("mutation", ("factor", "target", "recipe"))
def test_existing_component_mappings_and_allocated_stocks_cannot_change_without_review(mutation):
    resource = deepcopy(load_uvek_technosphere_resource())
    review.validate_bindings(resource)
    profile = next(iter(review.SOURCES.values()))["profiles"][0]
    source = review.identity(profile["component_bindings"][0]["source"])
    rule = next(r for r in resource["replace"] if review.identity(r["source"]) == source)
    if mutation == "recipe":
        rid = next(iter(review.APPROVAL["component_helper_sha256"]))
        resource["recipes"][rid]["name"] += " changed"
    else:
        # Changing an exact-profile mapping must also fail; a literal default is not its replacement.
        selected_profiles = rule.get("reference_flow_sign_review", {}).get("profiles", [])
        candidates = [p for p in selected_profiles if p["source_profile"]["version"] == profile["label"].split("-")[1]]
        changed = candidates[0] if candidates else rule
        if mutation == "factor":
            changed["conversion_factor"] = 999
        else:
            changed["target"] = {
                "name": "Unapproved",
                "reference product": "Unapproved",
                "location": "GLO",
                "unit": "kilogram",
            }
    with pytest.raises(ValueError, match="changed"):
        review.validate_bindings(resource)
