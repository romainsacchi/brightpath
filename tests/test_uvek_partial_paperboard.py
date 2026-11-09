"""Nonempty uncontrolled routes remain explicit losses; supported shares are not normalized."""

import json
from copy import deepcopy

import pytest
from test_uvek_reference_signs import consumer, contexts
from test_uvek_refrigerant_disposal import target_context

from brightpath import DATA_DIR, BrightwayInventory
from brightpath.core.policies import MigrationPolicy
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource
from scripts.review_uvek_partial_paperboard import apply_partial_paperboard_review

REVIEW = json.loads((DATA_DIR / "export/uvek_partial_paperboard_review.json").read_text())
ROW = REVIEW["records"][0]


@pytest.mark.parametrize("profile", ROW["profiles"], ids=lambda p: p["label"])
@pytest.mark.parametrize("amount", [-2, 0, 2])
@pytest.mark.parametrize("uncertainty", [2, 3])
def test_partial_helper_preserves_caller_freight_fraction_and_all_unsupported_audits(profile, amount, uncertainty):
    data = consumer(ROW["source"], amount, uncertainty)
    emission = {
        "name": "Carbon dioxide, fossil",
        "categories": ["air"],
        "unit": "kilogram",
        "type": "biosphere",
        "amount": 7,
    }
    data[0]["exchanges"].append(emission)
    before = deepcopy(data)
    result = BrightwayInventory.from_data(
        data, context=contexts("ecoinvent", profile["source_profile"]["version"])
    ).migrate_background(target_context("2026"), policy=MigrationPolicy.permissive())
    assert data == before and len(result.data) == 2
    actual = result.data[0]["exchanges"][1]
    original = before[0]["exchanges"][1]
    assert actual["amount"] == -amount and actual["formula"] == "(dose) * (-1.0)"
    assert actual["scale"] == original["scale"]
    if uncertainty == 2:
        assert actual["loc"] == original["loc"] and actual["negative"] is (not original["negative"])
    else:
        assert (
            actual["loc"] == -original["loc"]
            and actual["minimum"] == -original["maximum"]
            and actual["maximum"] == -original["minimum"]
        )
    assert result.data[0]["exchanges"][-1] == emission
    helper = result.data[1]
    assert helper["name"].startswith("PARTIAL SCREENING:")
    assert helper["brightpath_conversion"]["recipe_id"] == profile["generated_recipe_id"]
    inputs = [e for e in helper["exchanges"] if e["type"] != "production"]
    assert len(inputs) == len(profile["inputs"])
    assert all(
        all(actual[k] == v for k, v in expected.items())
        for actual, expected in zip(inputs, profile["inputs"], strict=True)
    )
    (treatment,) = [e for e in inputs if e["unit"] == "kilogram"]
    assert treatment["name"] == "Disposal, packaging cardboard, 19.6% water, to municipal incineration"
    assert treatment["amount"] == 0.125163193694194
    assert profile["final_unsupported_mass_fraction"] == pytest.approx(0.874836806305805, rel=1e-14, abs=0)
    assert not any("landfill" in e["name"] or "cork" in e["name"] or "aircraft" in e["name"] for e in inputs)
    losses = [
        i.to_dict()["details"]
        for i in result.last_migration_report.losses
        if i.code == "migration.recipe_source_input_omission"
    ]
    assert len(losses) == 11
    assert [i["omission"] for i in sorted(losses, key=lambda item: item["omission_index"])] == profile[
        "source_input_omissions"
    ]
    assert all(
        i["source_profile"] == profile["source_profile"] and i["basis"] == {"amount": 1, "unit": "kilogram"}
        for i in losses
    )
    assert helper["brightpath_conversion"]["source_input_omissions"] == profile["source_input_omissions"]
    assert not helper["brightpath_conversion"].get("zero_burden")
    assert all(w["nonproduction_exchanges"] > 0 for w in ROW["terminal_witnesses"])


@pytest.mark.parametrize(
    "version,model", [("3.6", "cutoff"), ("3.11", "cutoff"), ("3.9", "consequential"), ("3.12", "consequential")]
)
def test_partial_qualification_does_not_cross_release_or_system_model(version, model):
    source = BrightwayInventory.from_data(consumer(ROW["source"], -1), context=contexts("ecoinvent", version, model))
    before = deepcopy(source.data)
    with pytest.raises(MigrationError):
        source.migrate_background(target_context("2026"), policy=MigrationPolicy.permissive())
    assert source.data == before


@pytest.mark.parametrize("drift", ["none", "late_profile", "omission", "component"])
def test_partial_authoring_is_atomic_and_freezes_losses_and_transports(drift):
    resource = deepcopy(load_uvek_technosphere_resource())
    if drift == "late_profile":
        resource["recipes"][ROW["generated_recipe_ids"]["3.12"]]["inputs"][0]["amount"] += 1
    elif drift == "omission":
        resource["recipes"][ROW["generated_recipe_ids"]["3.9"]]["source_input_omissions"][0]["source_exchange"][
            "amount"
        ] *= 2
    elif drift == "component":
        bid = ROW["profiles"][0]["bindings"][0]["rule_id"]
        next(r for r in resource["replace"] if r["id"] == bid)["confidence"] = 0.123
    before = deepcopy(resource)
    if drift == "none":
        apply_partial_paperboard_review(resource)
    else:
        with pytest.raises(ValueError):
            apply_partial_paperboard_review(resource)
    assert resource == before


def test_new_native_catalog_requires_a_fresh_counterpart_search(tmp_path):
    resource = deepcopy(load_uvek_technosphere_resource())
    before = deepcopy(resource)
    review = tmp_path / "brightpath/data/export/uvek_partial_paperboard_review.json"
    review.parent.mkdir(parents=True)
    review.write_bytes((DATA_DIR / "export/uvek_partial_paperboard_review.json").read_bytes())
    catalog = tmp_path / "brightpath/data" / REVIEW["native_search"]["catalog_path"]
    catalog.parent.mkdir(parents=True)
    catalog.write_text('{"new_uncontrolled_supplier":true}')
    with pytest.raises(ValueError, match="native catalog"):
        apply_partial_paperboard_review(resource, root=tmp_path)
    assert resource == before
