"""Chemical heat keeps source MJ and all retained physical-water uncertainty."""

import json
from copy import deepcopy
from dataclasses import replace

import pytest
from test_uvek_reference_signs import consumer, contexts
from test_uvek_refrigerant_disposal import target_context

from brightpath import DATA_DIR, BrightwayInventory, TechnosphereProfile
from brightpath.background.reference_signs import select_profile_sign_rules
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.migrations.resources import load_uvek_technosphere_resource
from scripts.review_uvek_fuels_38_chemical_heat import apply_fuels_38_chemical_heat_review, digest

REVIEW = json.loads((DATA_DIR / "export/uvek_fuels_38_chemical_heat_review.json").read_text())


def resource_at_38_refinement_stage():
    """Replay the earlier frozen stage before its explicitly reviewed 3.9 successor."""
    resource = deepcopy(load_uvek_technosphere_resource())
    later = json.loads((DATA_DIR / "export/uvek_pcc_last_four_review.json").read_text())
    row = next(r for r in later["records"] if r["rule_id"] == REVIEW["rule_id"])
    parent = next(r for r in resource["replace"] if r["id"] == REVIEW["rule_id"])
    assert parent == row["compiled_rule"] and row["original_rule"] == REVIEW["compiled_rule"]
    parent.clear()
    parent.update(deepcopy(REVIEW["compiled_rule"]))
    return resource


def migrate(amount=1, uncertainty=3):
    source = BrightwayInventory.from_data(
        consumer(REVIEW["original_rule"]["source"], amount, uncertainty),
        context=contexts("ecoinvent", "3.8", "cutoff"),
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
    assert source.data == before
    edge = result.data[0]["exchanges"][1]
    ds = next(a for a in result.data if a.get("code") == edge["input"][1])
    return result, edge, ds


@pytest.mark.parametrize("amount", [-2.5, 2.5])
@pytest.mark.parametrize("uncertainty", [2, 3])
def test_mj_caller_quantity_is_not_a_steam_mass_factor(amount, uncertainty):
    _, edge, _ = migrate(amount, uncertainty)
    assert edge["amount"] == amount and edge["unit"] == "megajoule"
    assert edge["formula"] == "dose" and edge["scale"] == 0.2
    assert edge["uncertainty type"] == uncertainty
    if uncertainty == 2:
        assert edge["negative"] is (amount < 0)
    else:
        assert edge["loc"] == amount


def test_heat_once_and_all_nine_original_water_marginal_distributions():
    _, _, ds = migrate()
    rows = [e for e in ds["exchanges"] if e["type"] != "production"]
    assert len(rows) == 10
    assert rows[0]["name"] == "Heat, unspecific, in chemical plant"
    assert rows[0]["amount"] == 1 and rows[0]["unit"] == "megajoule"
    assert len([e for e in rows if e["type"] == "technosphere" and e["unit"] == "kilogram"]) == 7
    assert len([e for e in rows if e["type"] == "biosphere"]) == 2
    for actual, witness in zip(rows[1:], REVIEW["retained_water_rows"], strict=True):
        for k, value in witness["statistics"].items():
            assert actual[k] == value
        assert actual["uncertainty type"] == 2
        if witness["source_categories"]:
            assert actual["categories"][0] == witness["source_categories"][0]
            assert actual["unit"] == "cubic meter"
        else:
            assert actual["name"] == "Tap water, at user" and actual["unit"] == "kilogram"


def test_all_other_eight_profile_selections_at_38_stage_and_old_helpers_are_unchanged():
    resource = resource_at_38_refinement_stage()
    for proof in REVIEW["preserved_profiles"]:
        selected = next(
            q
            for q in select_profile_sign_rules(resource, TechnosphereProfile(**proof["source_profile"]))["replace"]
            if q["id"] == REVIEW["rule_id"]
        )
        assert digest(selected) == proof["selected_rule_sha256"]
    for rid, expected in REVIEW["recipe_dependencies"].items():
        assert digest(resource["recipes"][rid]) == expected


def test_refinement_replay_rejects_changed_water_statistics_without_mutation():
    resource = resource_at_38_refinement_stage()
    before = deepcopy(resource)
    apply_fuels_38_chemical_heat_review(resource)
    assert resource == before
    resource["recipes"][REVIEW["new_recipe_id"]]["inputs"][1]["scale"] = 0
    changed = deepcopy(resource)
    with pytest.raises(ValueError, match="Refined chemical-heat helper changed"):
        apply_fuels_38_chemical_heat_review(resource)
    assert resource == changed
