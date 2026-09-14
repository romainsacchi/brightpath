import copy
import hashlib
import json
from pathlib import Path

import pytest

from brightpath import DATA_DIR, BackgroundProfile, BrightwayInventory
from brightpath.core import MigrationPolicy
from scripts.review_uvek_mapping import identity
from scripts.review_uvek_priority_inventory import DIESEL_SOURCE, WOOD_SOURCE, assess, target_for


def summary(name, product="electricity, high voltage", unit="kilowatt hour"):
    return {
        "identity": {"name": name, "reference product": product, "location": "CH", "unit": unit},
        "reference_amount": 1.0,
        "technosphere_exchanges": 1,
        "biosphere_exchanges": 0,
        "normalized_inputs": {
            "heat_mj": 0.0,
            "electricity_kwh": 0.0,
            "machine_diesel_mj": 0.0,
            "wood_kg": 0.0,
            "diesel_kg": 0.0,
        },
        "fuel_providers": [],
        "documentation_flags": {
            "mentions_1000_seedlings": False,
            "mentions_exergy_allocation": False,
            "mentions_orc": False,
            "mentions_sncr": False,
        },
    }


def tree_pair(condition="heated"):
    source = summary(f"tree seedling production, in {condition} greenhouse", "tree seedling, for planting", "unit")
    source["identity"]["location"] = "RER"
    source["documentation_flags"]["mentions_1000_seedlings"] = True
    source["normalized_inputs"].update(
        electricity_kwh=0.0704, machine_diesel_mj=0.0184, heat_mj=0.0027 if condition == "unheated" else 0.801
    )
    target = copy.deepcopy(source)
    target["identity"] = target_for(source["identity"])
    return source, target


@pytest.mark.parametrize("condition", ["heated", "unheated"])
def test_seedling_reference_normalization_is_not_inferred_from_name(condition):
    source, target = tree_pair(condition)
    outcome, confidence, _, _ = assess(source, target, target)
    assert outcome == "accept_seedling_reference_basis"
    assert confidence == 0.8
    target["normalized_inputs"]["heat_mj"] *= 1000
    with pytest.raises(ValueError, match="fingerprint"):
        assess(source, target, target)


@pytest.mark.parametrize("problem", ["condition", "reference_amount", "documentation", "product"])
def test_seedling_audit_rejects_incompatible_basis(problem):
    source, target = tree_pair()
    if problem == "condition":
        target["identity"]["name"] = target["identity"]["name"].replace("heated", "unheated")
    elif problem == "reference_amount":
        target["reference_amount"] = 1000
    elif problem == "documentation":
        source["documentation_flags"]["mentions_1000_seedlings"] = False
    else:
        source["identity"]["reference product"] = "fruit tree seedling, for planting"
    with pytest.raises(ValueError):
        assess(source, target, target)


def diesel_pair():
    source = summary(DIESEL_SOURCE)
    source["documentation_flags"]["mentions_exergy_allocation"] = True
    source["normalized_inputs"]["diesel_kg"] = 0.18
    source["biosphere_exchanges"] = 10
    target = summary("diesel target")
    target["identity"] = target_for(source["identity"])
    target["documentation_flags"]["mentions_exergy_allocation"] = True
    target["fuel_providers"] = [{"biosphere_exchanges": 10}]
    return source, target, summary("allocation heat")


def test_zero_direct_emissions_does_not_ignore_burdens_at_fuel_supplier():
    source, target, branch = diesel_pair()
    assert target["biosphere_exchanges"] == branch["biosphere_exchanges"] == 0
    assert assess(source, target, branch)[0] == "accept_diesel_allocation_proxy"
    target["fuel_providers"] = []
    with pytest.raises(ValueError, match="operating burdens"):
        assess(source, target, branch)


def test_diesel_requires_allocation_documentation():
    source, target, branch = diesel_pair()
    source["documentation_flags"]["mentions_exergy_allocation"] = False
    with pytest.raises(ValueError, match="Exergy allocation"):
        assess(source, target, branch)


def test_wood_keeps_low_confidence_despite_better_operating_proxy():
    source = summary(WOOD_SOURCE)
    source["documentation_flags"].update(mentions_orc=True, mentions_sncr=True)
    source["normalized_inputs"]["wood_kg"] = 0.84
    source["biosphere_exchanges"] = 41
    target = copy.deepcopy(source)
    target["identity"] = target_for(source["identity"])
    outcome, confidence, _, limitations = assess(source, target, summary("allocation heat"))
    assert outcome == "accept_wood_operating_proxy"
    assert confidence < 0.5
    assert any("allocation split" in item for item in limitations)
    target["normalized_inputs"]["wood_kg"] = 0
    with pytest.raises(ValueError, match="Wood combustion"):
        assess(source, target, summary("allocation heat"))


def test_changed_branch_burden_pattern_requires_new_review():
    source, target, branch = diesel_pair()
    branch["normalized_inputs"]["diesel_kg"] = 0.1
    with pytest.raises(ValueError, match="operating inputs"):
        assess(source, target, branch)


def packaged():
    root = Path(__file__).resolve().parents[1]
    evidence_path = root / "docs/uvek_priority_inventory_evidence.json"
    evidence = json.loads(evidence_path.read_text())
    overrides = json.loads((DATA_DIR / "export/uvek_reviewed_overrides.json").read_text())
    decisions = [row for row in overrides["decisions"] if row.get("review_version") == 4]
    return evidence_path, evidence, decisions


def test_all_packaged_inventory_decisions_are_reproducible_from_sparse_evidence():
    evidence_path, evidence, decisions = packaged()
    sources = {identity(row["identity"]): row for row in evidence["source_activities"]}
    targets = {identity(row["identity"]): row for row in evidence["target_activities"]}
    digest = hashlib.sha256(evidence_path.read_bytes()).hexdigest()
    assert len(decisions) == 62
    for row in decisions:
        result = assess(
            sources[identity(row["source"])], targets[identity(row["target"])], targets[identity(row["branch_target"])]
        )
        assert result[:2] == (row["review_outcome"], row["confidence"])
        assert row["inventory_evidence"]["sha256"] == digest
        assert row["inventory_evidence"]["amount_factor"] == 1
    assert sum(row["target"] != row["branch_target"] for row in decisions) == 59
    assert sum(row["confidence"] < 0.5 for row in decisions) == 52


def test_packaged_resource_preserves_comparison_and_inventory_provenance():
    _, _, decisions = packaged()
    resource = json.loads((DATA_DIR / "migrations/uvek/ecoinvent-to-uvek-2025.json").read_text())
    indexed = {identity(row["source"]): row for row in resource["replace"]}
    assert len(resource["replace"]) == 31488
    assert sum(row["confidence"] < 0.5 for row in resource["replace"]) == 21618
    for decision in decisions:
        current = indexed[identity(decision["source"])]
        assert current["target"] == decision["target"]
        assert current["review_provenance"]["inventory_evidence"] == decision["inventory_evidence"]
        assert "comparison_source_revision" in current["review_provenance"]
        assert "source_revision" not in current["review_provenance"]
        assert current.get("conversion_factor", 1) == 1


@pytest.mark.parametrize(
    "condition,location", [("heated", "RER"), ("unheated", "RER"), ("heated", "RoW"), ("unheated", "RoW")]
)
def test_seedling_migration_preserves_exchange_amount_and_greenhouse_condition(condition, location):
    source = {
        "name": f"tree seedling production, in {condition} greenhouse",
        "reference product": "tree seedling, for planting",
        "location": location,
        "unit": "unit",
    }
    inventory = BrightwayInventory.from_data(
        [
            {
                "name": "foreground service",
                "reference product": "service",
                "location": "GLO",
                "unit": "unit",
                "exchanges": [
                    {
                        "name": "foreground service",
                        "reference product": "service",
                        "location": "GLO",
                        "unit": "unit",
                        "type": "production",
                        "amount": 1.0,
                    },
                    source | {"amount": 250.0, "type": "technosphere"},
                ],
            }
        ],
        background_profile=BackgroundProfile("ecoinvent", "3.10", "cutoff"),
    )
    migrated = inventory.migrate_background(
        BackgroundProfile("uvek", "2025", "cutoff"), policy=MigrationPolicy.permissive()
    )
    output = migrated.data[0]["exchanges"][1]
    assert output["amount"] == 250.0
    assert output["name"] == target_for(source)["name"]
    assert inventory.data[0]["exchanges"][1]["name"] == source["name"]
