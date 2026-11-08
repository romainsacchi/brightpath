"""Preserve older PET disposal mixtures and the positive polyester transfer coordinate."""

import json
import math
from collections import Counter, defaultdict
from copy import deepcopy
from pathlib import Path

import pytest
from test_uvek_reference_signs import consumer, contexts
from test_uvek_refrigerant_disposal import target_context

from brightpath import BrightwayInventory, TechnosphereProfile
from brightpath.background.reference_signs import select_profile_sign_rules
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from scripts import review_uvek_older_pet_whole_freight as review

R = Path(__file__).resolve().parents[1]
F = ("name", "reference product", "location", "unit")

DRAFT = {
    "parent_rows": [
        {"source": row["source"], "profile": profile} for row in review.SOURCES.values() for profile in row["profiles"]
    ]
}
BASE = json.loads((R / "brightpath/data/migrations/uvek/ecoinvent-to-uvek-2025.json").read_text())
POLICY = MigrationPolicy(on_heuristic_mapping=PolicyAction.WARN, on_information_loss=PolicyAction.WARN)
COEF = (
    "amount",
    "uncertainty type",
    "loc",
    "scale",
    "shape",
    "minimum",
    "maximum",
    "negative",
    "pedigree",
    "scale without pedigree",
    "formula",
)


def key(row):
    return tuple(row.get(f) for f in F)


@pytest.mark.parametrize("row", DRAFT["parent_rows"])
@pytest.mark.parametrize("release", ("2025", "2026"))
@pytest.mark.parametrize("amount", (-3, 0, 3))
@pytest.mark.parametrize("kind,uncertainty", (("technosphere", 2), ("substitution", 3)))
def test_original_fates_freight_quantities_and_uncertainty_survive(row, release, amount, kind, uncertainty):
    version = row["profile"]["label"].split("-")[1]
    data = consumer(row["source"], amount, uncertainty, kind)
    data[0]["exchanges"][1]["caller_metadata"] = {"preserve": [1, 2]}
    separate = {
        "type": "biosphere",
        "name": "Carbon dioxide, fossil",
        "categories": ["air"],
        "unit": "kilogram",
        "amount": 0.5,
    }
    data[0]["exchanges"].append(deepcopy(separate))
    before = deepcopy(data)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", version))
    result = inventory.migrate_background(target_context(release), policy=POLICY)
    assert inventory.data == before == data
    source = before[0]["exchanges"][1]
    after = result.data[0]["exchanges"][1]
    sign = row["profile"]["source_sign"]
    assert after["amount"] == amount * sign and after["type"] == kind
    assert after["caller_metadata"] == source["caller_metadata"]
    assert after["formula"] == ("(dose) * (-1.0)" if sign == -1 else "dose")
    assert after["scale"] == source["scale"]
    assert after["loc"] == (source["loc"] if uncertainty == 2 else sign * source["loc"])
    if uncertainty == 2:
        assert after["negative"] == (source["negative"] if sign == 1 else not source["negative"])
    assert (
        result.data[0]["exchanges"][-1]["amount"] == separate["amount"]
        and result.data[0]["exchanges"][-1]["name"] == separate["name"]
    )
    helpers = {d["code"]: d for d in result.data[1:]}
    root = helpers[after["input"][1]]
    actual = [e for e in root["exchanges"] if e["type"] != "production"]
    expected = row["profile"]["inputs"]
    assert sorted(json.dumps({f: e[f] for f in COEF if f in e}, sort_keys=True) for e in actual) == sorted(
        json.dumps({f: e[f] for f in COEF if f in e}, sort_keys=True) for e in expected
    )
    assert len(actual) == len(expected)
    assert math.isclose(sum(e["amount"] for e in actual if e["unit"] == "kilogram"), 1, rel_tol=1e-14)
    if sign == -1:
        modes = Counter(
            (
                "rail"
                if "rail" in e["name"].lower()
                else "barge" if "barge" in e["name"].lower() else "sea" if "ship" in e["name"].lower() else "road"
            )
            for e in actual
            if e["unit"] == "ton kilometer"
        )
        assert modes == (
            {"rail": 6, "barge": 2, "sea": 1, "road": 1}
            if version in ("3.6", "3.7", "3.8")
            else {"rail": 3, "barge": 1, "sea": 1, "road": 1}
        )
    else:
        assert len(actual) == 8 and all(e["amount"] > 0 for e in actual)
        assert not any(
            e["name"] == "Fleece production, polyethylene terephthalate"
            for d in helpers.values()
            for e in d["exchanges"]
        )
    assert any(i.code == "migration.technosphere_information_loss" for i in result.last_migration_report.issues)


@pytest.mark.parametrize("case", range(6))
def test_missing_native_physical_definitions_refuse_without_mutating_source(case):
    row = DRAFT["parent_rows"][case]
    data = consumer(row["source"], -1)
    before = deepcopy(data)
    version = row["profile"]["label"].split("-")[1]
    selected = select_profile_sign_rules(BASE, TechnosphereProfile("ecoinvent", version, "cutoff"))
    (rule,) = [r for r in selected["replace"] if key(r["source"]) == key(row["source"])]
    assert rule["reference_flow_sign_review"]["status"] == "verified"
    source = BrightwayInventory.from_data(data, context=contexts("ecoinvent", version))
    with pytest.raises(MigrationError) as caught:
        source.migrate_background(target_context("native2026"), policy=POLICY)
    assert any(i.code == "background.biosphere_link_unresolved" for i in caught.value.report.issues)
    assert not any(i.code == "migration.reference_flow_sign_unresolved" for i in caught.value.report.issues)
    assert source.data == before == data


@pytest.mark.parametrize("change", ("inventory", "metadata", "reference", "profile"))
def test_stale_full_source_and_lifecycle_evidence_refuses(change):
    records = deepcopy(review.APPROVAL["inventories"])
    observations = defaultdict(lambda: defaultdict(set))
    review.validate_inventories(records, observations, {})
    if change == "inventory":
        records[0]["inventory_sha256"] = "0" * 64
    elif change == "metadata":
        records[0]["metadata_sha256"] = "0" * 64
    elif change == "reference":
        records[0]["reference_amount"] = 0
    else:
        observations["ecoinvent"][key(DRAFT["parent_rows"][0]["source"])].add(("ecoinvent-3.13-cutoff", -1))
    with pytest.raises(ValueError, match="Older PET"):
        review.validate_inventories(records, observations, {})


def test_treatment_and_whole_freight_bindings_cannot_change_silently():
    review.validate_bindings(BASE)
    changed = deepcopy(BASE)
    binding = next(b for b in review.APPROVAL["bindings"] if "internal_recipe" not in b)
    (rule,) = [r for r in changed["replace"] if key(r["source"]) == key(binding["source"])]
    rule["confidence"] = 0.123
    with pytest.raises(ValueError, match="binding changed"):
        review.validate_bindings(changed)


@pytest.mark.parametrize("version", ("3.9.2", "3.13"))
def test_unknown_profiles_do_not_inherit_the_new_whole_service_review(version):
    selected = select_profile_sign_rules(BASE, TechnosphereProfile("ecoinvent", version, "cutoff"))
    rows = [r for r in selected["replace"] if key(r["source"]) in review.SOURCES]
    assert len(rows) == 2 and all(r["reference_flow_sign_review"]["status"] == "unresolved" for r in rows)


def test_exact_authoring_replay_preserves_the_four_modern_bodies():
    ledger = json.loads((R / "brightpath/data/export/uvek_reference_flow_sign_review.json").read_text())["resources"][
        BASE["name"]
    ]
    assert len(review.SOURCES) == 2 and len(DRAFT["parent_rows"]) == 6 and len(review.APPROVAL["inventories"]) == 440
    for row in review.SOURCES.values():
        previous = ledger[row["id"]]
        observations = {tuple(p) for p in previous["source_observations"]}
        records = (
            [r for r in review.legacy.APPROVAL["inventories"] if key(r["source"]) == key(row["source"])]
            if row["legacy_decision_sha256"]
            else [r for r in review.APPROVAL["inventories"] if key(r["source"]) == key(row["source"])]
        )
        actual = review.forward_decision(
            previous["original_rule"], observations, records, previous["binding"], previous["original_review"]
        )
        assert actual == previous
        assert "PENDING" not in json.dumps(actual)
        if row["legacy_decision_sha256"]:
            old = review.legacy.forward_decision(
                previous["original_rule"], observations, records, previous["binding"], previous["original_review"]
            )
            assert review.digest(old) == row["legacy_decision_sha256"] and len(old["recipes"]) == 4
            assert all(BASE["recipes"][rid] == body for rid, body in old["recipes"].items())
        else:
            assert actual["factor"] == 1


@pytest.mark.parametrize("case", (0, 5))
def test_complete_hierarchy_survives_brightway_export(case, tmp_path):
    row = DRAFT["parent_rows"][case]
    data = consumer(row["source"], 3, 2)
    data[0]["parameters"] = [{"name": "dose", "amount": 3}]
    data[0]["comment"] = "Synthetic older PET/positive polyester export fixture."
    data[0]["exchanges"][1]["caller_metadata"] = {"retain": [1, 2]}
    converted = BrightwayInventory.from_data(
        data, context=contexts("ecoinvent", row["profile"]["label"].split("-")[1])
    ).migrate_background(target_context("2025"), policy=POLICY)
    before = deepcopy(converted.data)
    restored = BrightwayInventory.from_excel(converted.write_excel(tmp_path / "older-pet.xlsx"))
    assert not restored.validate().has_errors
    assert converted.data == before
    assert restored.background_profile == converted.background_profile
    assert restored.data[0]["exchanges"][1]["caller_metadata"] == {"retain": [1, 2]}
    by_identity = {key(d): d for d in restored.data}
    for helper in converted.data[1:]:
        final = by_identity[key(helper)]
        # Excel's numeric cells can round the final floating-point digit.
        # Relative-only tolerance keeps every nonzero trace quantity nonzero.
        assert [e["amount"] for e in final["exchanges"]] == pytest.approx(
            [e["amount"] for e in helper["exchanges"]], rel=5e-15, abs=0
        )
        for source, target in zip(helper["exchanges"], final["exchanges"], strict=True):
            if source.get("uncertainty type", 0) not in (0, 1):
                for field in COEF:
                    if field not in source:
                        continue
                    value = source[field]
                    if isinstance(value, (int, float)) and not isinstance(value, bool):
                        assert target[field] == pytest.approx(value, rel=5e-15, abs=0)
                    else:
                        assert target[field] == value
