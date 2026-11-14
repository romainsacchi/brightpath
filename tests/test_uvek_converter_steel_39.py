"""Source3.9 BOF steel keeps cast-kg basis with explicit charge and grade differences."""

import json
from copy import deepcopy

import pytest
from test_uvek_reference_signs import consumer, contexts

from brightpath import DATA_DIR, BrightwayInventory, TechnosphereProfile
from brightpath.background.rule_approvals import compatibility_rule_fingerprint
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource
from brightpath.migrations.scaling import scale_exchange

REVIEW = json.loads((DATA_DIR / "export/uvek_converter_steel_39_review.json").read_text())
ROW = REVIEW["records"][0]


def policy(approved=True):
    r = load_uvek_technosphere_resource()
    rule = next(x for x in r["replace"] if x["id"] == ROW["rule_id"])
    fp = compatibility_rule_fingerprint(
        r,
        "replace",
        rule,
        TechnosphereProfile("ecoinvent", "3.9", "cutoff"),
        TechnosphereProfile("uvek", "2025", "cutoff"),
    )
    return MigrationPolicy(
        approved_compatibility_rules=(fp,) if approved else (), on_information_loss=PolicyAction.WARN
    )


@pytest.mark.parametrize("amount", [-2.5, 2.5])
@pytest.mark.parametrize("uncertainty", [2, 3])
@pytest.mark.parametrize("release", ["2025", "2026"])
def test_whole_chemical_keeps_signed_demand_uncertainty_and_separate_emissions(amount, uncertainty, release):
    data = consumer(ROW["original_rule"]["source"], amount, uncertainty)
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
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.9")).migrate_background(
        contexts("uvek", release).background, policy=policy()
    )
    assert data == before and len(result.data) == 1
    actual = result.data[0]["exchanges"][1]
    expected = deepcopy(before[0]["exchanges"][1])
    scale_exchange(expected, 1)
    for k in ("amount", "uncertainty type", "loc", "scale", "negative", "formula", "minimum", "maximum"):
        if k in expected:
            assert actual[k] == expected[k]
    assert all(actual[k] == value for k, value in ROW["original_rule"]["target"].items())
    assert result.data[0]["exchanges"][-1] == before[0]["exchanges"][-1]


def test_exact_39_service_requires_its_own_approval():
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(
            consumer(ROW["original_rule"]["source"], -1), context=contexts("ecoinvent", "3.9")
        ).migrate_background(contexts("uvek", "2026").background, policy=policy(False))


@pytest.mark.parametrize(
    "version,model", [("3.8", "cutoff"), ("3.9.1", "cutoff"), ("3.10", "cutoff"), ("3.9", "consequential")]
)
def test_39_scope_does_not_expand_to_other_profiles(version, model):
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(
            consumer(ROW["original_rule"]["source"], -1), context=contexts("ecoinvent", version, model)
        ).migrate_background(contexts("uvek", "2026").background, policy=policy())


def test_updated_charge_and_grade_are_explicitly_not_claimed_equal():
    facts = ROW["boundary_checks"]
    assert facts["kind"] == "complete_primary_BOF_cast_low_alloyed_steel"
    assert facts["source39_pig_iron_kg"] == 0.867944612 and facts["native_pig_iron_kg"] == 0.9
    assert facts["source39_scrap_kg"] == 0.198864791 and facts["native_scrap_kg"] == 0.12501
    assert facts["source39_explicit_ferrochromium_kg"] == 0 and facts["native_explicit_ferrochromium_kg"] == 0.032853
    assert facts["identical_grade_assay_independently_established"] is False
    assert facts["charge_yield_grade_or_rolling_multiplier_added"] is False
    assert ROW["source_witness"]["reference_amounts"] == [1]
    assert all(w["reference_amounts"] == [1] for w in ROW["native_witnesses"].values())
    assert (
        ROW["compiled_rule"]["reference_flow_sign_review"]["profiles"][:-1]
        == ROW["original_rule"]["reference_flow_sign_review"]["profiles"]
    )
    assert ROW["compiled_rule"]["target"] == ROW["original_rule"]["target"]
    assert "Add no rolling" in ROW["compiled_rule"]["technosphere_information_loss"]
    assert REVIEW["new_helpers"] == 0 and REVIEW["new_source_omissions"] == []
    assert all(REVIEW["prior36_recorded_primary_hash_checks"].values())
    archived = json.loads((DATA_DIR / "export/uvek_archived_unreferenced_reviews.json").read_text())["reviews"]
    for key, card in REVIEW["retired_review_cards"].items():
        assert archived[key] == card
        assert key not in load_uvek_technosphere_resource()["reviews"]


def test_cast_steel_permission_does_not_authorize_electric_steel():
    source = {
        "name": "steel production, electric, low-alloyed",
        "reference product": "steel, low-alloyed",
        "location": "RER",
        "unit": "kilogram",
    }
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(consumer(source, 1), context=contexts("ecoinvent", "3.9")).migrate_background(
            contexts("uvek", "2026").background, policy=policy()
        )


@pytest.mark.parametrize("drift", [None, "parent", "retired_card"])
def test_new_replay_is_atomic_and_idempotent(drift):
    from scripts.review_uvek_converter_steel_39 import apply_converter_steel_39_review

    resource = deepcopy(load_uvek_technosphere_resource())
    if drift == "parent":
        next(r for r in resource["replace"] if r["id"] == ROW["rule_id"])["confidence"] = 0.1
    elif drift == "retired_card":
        resource["reviews"].update(deepcopy(REVIEW["retired_review_cards"]))
        resource["reviews"][next(iter(REVIEW["retired_review_cards"]))]["rationale"] += " drift"
    before = deepcopy(resource)
    if drift:
        with pytest.raises(ValueError):
            apply_converter_steel_39_review(resource)
    else:
        apply_converter_steel_39_review(resource)
    assert resource == before


def test_new_replay_and_archive_reproduce_the_compiled_resource_from_frozen_parent():
    from uvek_review_helpers import before_converter_steel_39_review

    from scripts.archive_uvek_unreferenced_reviews import archive_unreferenced_reviews
    from scripts.review_uvek_converter_steel_39 import apply_converter_steel_39_review

    expected = deepcopy(load_uvek_technosphere_resource())
    resource = before_converter_steel_39_review(deepcopy(expected))
    apply_converter_steel_39_review(resource)
    archive_unreferenced_reviews(resource)
    assert resource == expected
