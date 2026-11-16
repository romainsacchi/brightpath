"""Explicit Mixed-plastic model reuse retains current market inputs and original model audits."""

import json
from copy import deepcopy

import pytest
from test_uvek_reference_signs import consumer, contexts

from brightpath import (
    DATA_DIR,
    BackgroundContext,
    BiosphereProfile,
    BrightwayInventory,
    FormatProfile,
    InventoryContext,
    TechnosphereProfile,
)
from brightpath.background.rule_approvals import compatibility_rule_fingerprint
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.migrations.resources import load_uvek_technosphere_resource


@pytest.mark.parametrize("amount", [-2, 2])
@pytest.mark.parametrize("release", ["2025", "2026"])
def test_current_market_keeps_source_statistics_and_historical_omission_ownership(amount, release):
    review = json.loads((DATA_DIR / "export/uvek_hydrogen_train_plastic_312_review.json").read_text())
    row = review["records"][0]
    resource = load_uvek_technosphere_resource()
    rule = next(r for r in resource["replace"] if r["id"] == row["rule_id"])
    source = TechnosphereProfile("ecoinvent", "3.12", "cutoff")
    bridge = TechnosphereProfile("uvek", "2025", "cutoff")
    policy = MigrationPolicy(
        approved_compatibility_rules=(compatibility_rule_fingerprint(resource, "replace", rule, source, bridge),),
        on_information_loss=PolicyAction.WARN,
    )
    data = consumer(row["source"], amount, 3)
    data[0]["exchanges"].append(
        {
            "name": "Carbon dioxide, fossil",
            "categories": ["air"],
            "unit": "kilogram",
            "type": "biosphere",
            "amount": 0.125,
        }
    )
    before = deepcopy(data)
    context = InventoryContext(
        FormatProfile("brightway_excel"), BackgroundContext(source, BiosphereProfile("ecoinvent", "3.12"))
    )
    inventory = BrightwayInventory.from_data(data, context=context)
    converted = inventory.migrate_background(contexts("uvek", release).background, policy=policy)
    assert data == before and inventory.data == before
    assert converted.data[0]["exchanges"][1]["amount"] == -amount
    assert converted.data[0]["exchanges"][1]["loc"] == -amount
    assert converted.data[0]["exchanges"][1]["formula"] == "(dose) * (-1.0)"
    assert converted.data[0]["exchanges"][-1]["amount"] == 0.125
    helpers = converted.data[1:]
    assert len(helpers) == 12
    market = next(h for h in helpers if h["brightpath_conversion"]["recipe_id"] == row["recipe_id"])
    actual = [e for e in market["exchanges"] if e["type"] != "production"]
    expected = review["recipes"][row["recipe_id"]]["inputs"]
    assert len(actual) == 13
    for got, want in zip(actual, expected, strict=True):
        for key in ["amount", "uncertainty type", "loc", "scale", "scale without pedigree", "pedigree"]:
            if key in want:
                assert got[key] == want[key]
        assert "inventory_source_profile" not in got
    models = [h for h in helpers if h is not market]
    assert len(models) == 11
    assert all(
        h["brightpath_conversion"]["inventory_source_profile"] == review["inventory_model_profile"] for h in models
    )
    assert all(h["brightpath_conversion"]["caller_source_profile"] == review["source_profile"] for h in models)
    losses = [x for x in converted.last_migration_report.losses if x.code == "migration.recipe_source_input_omission"]
    assert len(losses) == 0
    for loss in losses:
        details = loss.to_dict()["details"]
        assert details["source_profile"] == review["inventory_model_profile"]
        assert details["caller_source_profile"] == review["source_profile"]
    boundaries = [
        x for x in converted.last_migration_report.losses if x.to_dict()["details"].get("inventory_source_profile")
    ]
    assert len(boundaries) == 11


def test_plastic_review_replay_is_idempotent_and_rejects_model_drift():
    from scripts.review_uvek_hydrogen_train_plastic_312 import apply_hydrogen_train_plastic_312_review

    resource = deepcopy(load_uvek_technosphere_resource())
    before = deepcopy(resource)
    apply_hydrogen_train_plastic_312_review(resource)
    assert resource == before
    review = json.loads((DATA_DIR / "export/uvek_hydrogen_train_plastic_312_review.json").read_text())
    key = next(iter(review["recipe_dependency_sha256"]))
    resource["recipes"][key]["inputs"][0]["amount"] += 1
    before = deepcopy(resource)
    with pytest.raises(ValueError, match="dependency changed"):
        apply_hydrogen_train_plastic_312_review(resource)
    assert resource == before
