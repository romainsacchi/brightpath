"""Exact-profile grid extensions must preserve earlier inspected behavior."""

import json
from copy import deepcopy

import pytest

from brightpath import DATA_DIR, TechnosphereProfile
from brightpath.background.reference_signs import select_profile_sign_rules
from brightpath.migrations.resources import load_uvek_technosphere_resource
from scripts.review_uvek_geothermal_36_grids import REVIEW_PATH, apply_geothermal_36_grid_review


def test_geothermal_grid_extension_is_idempotent_and_preserves_39_decision():
    review = json.loads((DATA_DIR.parent.parent / REVIEW_PATH).read_text())
    row = next(r for r in review["records"] if r["resource_action"] == "append_exact_profile")
    resource = deepcopy(load_uvek_technosphere_resource())
    rule = next(r for r in resource["replace"] if r["id"] == row["rule_id"])
    rule.clear()
    rule.update(deepcopy(row["previous_rule"]))
    earlier = select_profile_sign_rules(resource, TechnosphereProfile("ecoinvent", "3.9", "cutoff"))
    apply_geothermal_36_grid_review(resource)
    assert select_profile_sign_rules(resource, TechnosphereProfile("ecoinvent", "3.9", "cutoff")) == earlier
    after = deepcopy(resource)
    apply_geothermal_36_grid_review(resource)
    assert resource == after
    selected = select_profile_sign_rules(resource, TechnosphereProfile("ecoinvent", "3.6", "cutoff"))
    updated = next(r for r in selected["replace"] if r["id"] == row["rule_id"])
    assert updated["conversion_factor"] == 1
    assert updated["reference_flow_sign_review"]["status"] == "verified"
    assert updated["target"] == row["target"]


def test_changed_grid_parent_refuses_without_partial_mutation():
    resource = deepcopy(load_uvek_technosphere_resource())
    review = json.loads((DATA_DIR.parent.parent / REVIEW_PATH).read_text())
    rule = next(r for r in resource["replace"] if r["id"] == review["records"][-1]["rule_id"])
    rule["target"]["location"] = "unreviewed location"
    before = deepcopy(resource)
    with pytest.raises(ValueError, match="Geothermal grid rule"):
        apply_geothermal_36_grid_review(resource)
    assert resource == before
