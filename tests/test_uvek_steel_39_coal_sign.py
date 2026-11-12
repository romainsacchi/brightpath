"""The inspected coal mass reference does not change historical selections."""

import json
from copy import deepcopy

import pytest

from brightpath import DATA_DIR, TechnosphereProfile
from brightpath.background.reference_signs import select_profile_sign_rules
from brightpath.migrations.resources import load_uvek_technosphere_resource
from scripts.review_uvek_steel_39_coal_sign import apply_steel_39_coal_sign_review


def test_historical_38_selection_is_identical():
    resource = load_uvek_technosphere_resource()
    review = json.loads((DATA_DIR / "export/uvek_steel_39_remaining_supplies_review.json").read_text())
    row = next(r for r in review["records"] if r["rule_id"] == "bp-5ede2be860ad337d")
    historical = deepcopy(resource)
    parent = next(r for r in historical["replace"] if r["id"] == row["rule_id"])
    parent.clear()
    parent.update(row["original_rule"])
    profile = TechnosphereProfile("ecoinvent", "3.8", "cutoff")
    assert select_profile_sign_rules(resource, profile) == select_profile_sign_rules(historical, profile)


@pytest.mark.parametrize("drift", [False, True])
def test_replay_is_atomic(drift):
    resource = deepcopy(load_uvek_technosphere_resource())
    if drift:
        next(r for r in resource["replace"] if r["id"] == "bp-5ede2be860ad337d")["confidence"] = 0.1
    before = deepcopy(resource)
    if drift:
        with pytest.raises(ValueError, match="parent changed"):
            apply_steel_39_coal_sign_review(resource)
    else:
        apply_steel_39_coal_sign_review(resource)
    assert resource == before
