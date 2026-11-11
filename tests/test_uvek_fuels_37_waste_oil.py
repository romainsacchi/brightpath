"""Allocated waste-oil omission stays explicit, scoped and sign-correct."""

import json
from copy import deepcopy
from dataclasses import replace

import pytest
from test_uvek_reference_signs import consumer, contexts
from test_uvek_refrigerant_disposal import target_context

from brightpath import DATA_DIR, BrightwayInventory, TechnosphereProfile
from brightpath.background.reference_signs import select_profile_sign_rules
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource
from brightpath.migrations.schema import validate_migration_resource
from scripts.review_uvek_fuels_37_waste_oil import apply_fuels_37_waste_oil_review

REVIEW = json.loads((DATA_DIR / "export/uvek_fuels_37_waste_oil_review.json").read_text())
RECIPE_ID = "fuels-37-allocated-waste-mineral-oil"
RECIPE = REVIEW["recipes"][RECIPE_ID]


@pytest.mark.parametrize("amount", [-2.5, 2.5])
@pytest.mark.parametrize("uncertainty", [2, 3])
def test_treatment_sign_statistics_and_omission_audit(amount, uncertainty):
    source = BrightwayInventory.from_data(
        consumer(REVIEW["original_rule"]["source"], amount, uncertainty), context=contexts("ecoinvent", "3.7", "cutoff")
    )
    before = deepcopy(source.data)
    policy = replace(
        MigrationPolicy.permissive(),
        on_invalid_source=PolicyAction.ERROR,
        on_invalid_target=PolicyAction.ERROR,
        on_unresolved_link=PolicyAction.ERROR,
        minimum_coverage=1,
    )
    result = source.migrate_background(target_context("2026"), policy=policy)
    edge = result.data[0]["exchanges"][1]
    assert edge["amount"] == -amount and edge["formula"] == "(dose) * (-1.0)"
    assert edge["uncertainty type"] == uncertainty and edge["scale"] == 0.2
    if uncertainty == 2:
        assert edge["negative"] is (-amount < 0)
    else:
        assert edge["loc"] == -amount
    helper = next(a for a in result.data if a.get("code") == edge["input"][1])
    inputs = [e for e in helper["exchanges"] if e["type"] != "production"]
    assert [e["amount"] for e in inputs] == [0.01932, 0.0112, 0.397600148816246, 0.397566014085865]
    assert helper["brightpath_conversion"]["source_input_omissions"] == RECIPE["source_input_omissions"]
    assert (
        len(
            [
                loss
                for loss in result.last_migration_report.losses
                if loss.code == "migration.recipe_source_input_omission"
            ]
        )
        == 1
    )
    assert source.data == before


def test_fixed_allocation_does_not_invent_incineration_or_zero_stock():
    assert REVIEW["allocation_evidence"]["zero_nominal_waste_coordinates"] == 57
    assert REVIEW["allocation_evidence"]["zero_bounded_uncertainty_rows"] == 57
    assert sum(e["amount"] for e in RECIPE["inputs"] if e["unit"] == "kilogram") == pytest.approx(0.795166162902111)
    assert not RECIPE.get("zero_burden")
    assert all("recipe" not in e and "zero_burden_supplier" not in e for e in RECIPE["inputs"])
    omission = RECIPE["source_input_omissions"][0]
    assert omission["source_exchange"]["amount"] == -0.204833837097889
    assert "patch proxy witness only" in omission["rationale"]
    assert REVIEW["source_witness"]["inspected_profile"]["version"] == "3.7.1"


@pytest.mark.parametrize("version", ["3.8", "3.9"])
def test_no_approval_inferred_for_other_source_releases(version):
    selected = select_profile_sign_rules(
        {"replace": [REVIEW["compiled_rule"]]}, TechnosphereProfile("ecoinvent", version, "cutoff")
    )["replace"][0]
    assert selected["reference_flow_sign_review"]["status"] == "unresolved"


def test_author_replays_without_changing_existing_content():
    resource = deepcopy(load_uvek_technosphere_resource())
    before = deepcopy(resource)
    apply_fuels_37_waste_oil_review(resource)
    assert resource == before


def test_omission_cannot_be_relabelled_to_another_source_profile():
    resource = deepcopy(load_uvek_technosphere_resource())
    resource["recipes"][RECIPE_ID]["source_input_omissions"][0]["source_profile"]["version"] = "3.8"
    with pytest.raises(MigrationError, match="verified source activity/profile"):
        validate_migration_resource(resource)
