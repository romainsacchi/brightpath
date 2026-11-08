"""Open burning and open dumping retain source releases instead of an incinerator proxy."""

import math
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
from scripts import review_uvek_pp_uncontrolled_source as review

POLICY = MigrationPolicy(on_heuristic_mapping=PolicyAction.WARN, on_information_loss=PolicyAction.WARN)
CASES = [(source, p) for source, row in review.SOURCES.items() for p in row["profiles"]]


def evidence():
    records = deepcopy(review.APPROVAL["inventories"])
    observations = defaultdict(lambda: defaultdict(set))
    review.extend_observations(records, observations)
    return records, observations


@pytest.mark.parametrize("source,profile", CASES)
@pytest.mark.parametrize("release", ("2025", "2026"))
@pytest.mark.parametrize("amount", (-2, 0, 2))
def test_all_source_emissions_and_uncertainty_survive_signed_waste_demand(source, profile, release, amount):
    version = profile["label"].split("-")[1]
    data = consumer(dict(zip(FIELDS, source, strict=True)), amount, 2)
    data[0]["exchanges"].append(
        consumer(dict(zip(FIELDS, source, strict=True)), amount, 3, "substitution")[0]["exchanges"][1]
    )
    for e in data[0]["exchanges"][1:]:
        e["caller_metadata"] = {"preserve": [1, 2]}
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
    assert len(result.data) == 2
    helper = result.data[1]
    assert helper["location"] == "GLO" and helper["unit"] == "kilogram"
    assert helper["exchanges"][0]["type"] == "production" and helper["exchanges"][0]["amount"] == 1
    for old, new in zip(before[0]["exchanges"][1:-1], result.data[0]["exchanges"][1:-1], strict=True):
        assert new["amount"] == -amount and new["type"] == old["type"]
        assert new["formula"] == "(dose) * (-1.0)" and new["scale"] == old["scale"]
        assert new["caller_metadata"] == old["caller_metadata"]
        if old["uncertainty type"] == 2:
            assert new["loc"] == old["loc"] and new["negative"] is not old["negative"]
        else:
            assert new["loc"] == -old["loc"]
            assert (new["minimum"], new["maximum"]) == (-old["maximum"], -old["minimum"])
        assert new["input"][1] == helper["code"]
    actual = helper["exchanges"][1:]
    assert len(actual) == len(profile["recipe"]["inputs"])
    for old, new in zip(profile["recipe"]["inputs"], actual, strict=True):
        assert new["type"] == "biosphere"
        for field in (
            "name",
            "categories",
            "unit",
            "amount",
            "uncertainty type",
            "loc",
            "scale",
            "negative",
            "minimum",
            "maximum",
        ):
            assert new.get(field) == old.get(field)
    assert not helper["brightpath_conversion"].get("zero_burden")
    assert result.data[0]["exchanges"][0] == before[0]["exchanges"][0]
    assert result.data[0]["exchanges"][-1]["amount"] == 0.5
    assert not any(i.code.endswith("link_unresolved") for i in result.last_migration_report.issues)
    assert not any(i.code == "migration.biosphere_omission" for i in result.last_migration_report.losses)
    assert inventory.data == data == before
    assert result.migrate_background(target_context(release)).data == result.data


@pytest.mark.parametrize("source,profile", CASES)
def test_native_coverage_refuses_transactionally_or_retains_every_unsupported_physical_link(source, profile):
    version = profile["label"].split("-")[1]
    data = consumer(dict(zip(FIELDS, source, strict=True)), -2, 3)
    before = deepcopy(data)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", version))
    with pytest.raises(MigrationError) as caught:
        inventory.migrate_background(target_context("native2026"), policy=POLICY)
    assert any(i.code == "background.biosphere_link_unresolved" for i in caught.value.report.issues)
    assert inventory.data == before
    result = inventory.migrate_background(target_context("native2026"), policy=MigrationPolicy.permissive())
    assert len(result.data) == 2
    helper = result.data[1]
    physical = helper["exchanges"][1:]
    assert len(physical) == len(profile["recipe"]["inputs"])
    for original, final in zip(profile["recipe"]["inputs"], physical, strict=True):
        assert final["type"] == "biosphere"
        factor = 1
        if final["unit"] != original["unit"]:
            # Existing native water identities use kg rather than the canonical m3.
            # This registered bridge scales both the amount and lognormal location.
            assert original["name"] == final["name"] == "Water"
            assert original["unit"] == "cubic meter" and final["unit"] == "kilogram"
            factor = 1000
        assert final["amount"] == pytest.approx(original["amount"] * factor)
        assert final["uncertainty type"] == original["uncertainty type"] == 2
        assert final["loc"] == pytest.approx(original["loc"] + math.log(factor))
        assert final["scale"] == original["scale"]
        for field in ("minimum", "maximum"):
            if field in original:
                assert final[field] == pytest.approx(original[field] * factor)
            else:
                assert field not in final
        if factor == 1:
            assert final.get("negative") == original.get("negative")
        else:
            assert final["negative"] == bool(original.get("negative", original["amount"] < 0))
    unresolved = [i for i in result.last_migration_report.issues if i.code.endswith("link_unresolved")]

    def key(row):
        return (row["name"], tuple(row["categories"]), row["unit"])

    original_keys = {key(e) for e in profile["recipe"]["inputs"]}
    assert unresolved and all(i.code == "background.biosphere_link_unresolved" for i in unresolved)
    assert {key(i.details["identity"]) for i in unresolved} <= original_keys
    assert not any(i.code == "migration.biosphere_omission" for i in result.last_migration_report.losses)
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
        rule["target"]["name"] = "Controlled landfill is not an open dump"
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


def test_only_existing_exact_release_same_cas_nickel_synonyms_are_reused():
    aliases = review.APPROVAL["existing_reviewed_publisher_synonyms"]
    assert len(aliases) == 3
    assert {a["label"] for a in aliases} == {"ecoinvent-3.6-cutoff", "ecoinvent-3.7-cutoff", "ecoinvent-3.8-cutoff"}
    for entry in aliases:
        row = entry["approved_identity_review"]
        assert row["identity_kind"] == "reviewed_publisher_synonym" and row["status"] == "verified"
        assert row["source_cas"] == row["target_cas"] == "014701-22-5"
        assert entry["source_definition"]["categories"] == row["target"]["categories"] == ["water", "ground-"]
        assert entry["source_definition"]["unit"] == row["target"]["unit"] == "kilogram"
    assert sum(len(p["recipe"]["inputs"]) for row in review.SOURCES.values() for p in row["profiles"]) == 1719
