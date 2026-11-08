"""Bare landfills must retain their stage and inspected reference profile."""

from copy import deepcopy
import json

import pytest

from brightpath import DATA_DIR
from brightpath.background.reference_signs import select_profile_sign_rules
from brightpath.core.context import TechnosphereProfile
from brightpath.migrations.resources import load_uvek_technosphere_resource
from scripts.review_uvek_pilot_landfills import apply_pilot_landfill_review


def test_bare_wool_landfill_has_no_collection_wrapper_and_keeps_profile_gate():
    resource = load_uvek_technosphere_resource()
    for version, status in (("3.6", "verified"), ("3.9", "unresolved")):
        selected = select_profile_sign_rules(resource, TechnosphereProfile("ecoinvent", version, "cutoff"))
        rule = next(r for r in selected["replace"] if r["id"] == "bp-b39070e2c2728597")
        assert rule["target"]["name"] == "Disposal, mineral wool, 0% water, to construction waste landfill"
        assert rule["conversion_factor"] == -1
        assert rule["reference_flow_sign_review"]["status"] == status
        assert rule["report_correspondence"]
        assert "Do not add the wrapper collection freight" in rule["technosphere_information_loss"]


def test_landfill_replay_changes_only_the_wrong_wrapper():
    resource = deepcopy(load_uvek_technosphere_resource())
    expected = deepcopy(resource)
    review = json.loads((DATA_DIR / "export/uvek_pilot_bare_landfill_review.json").read_text())
    (decision,) = [r for r in review["records"] if r["correct_endpoint"]]
    next(r for r in resource["replace"] if r["id"] == decision["rule_id"])["target"] = decision["previous_target"]
    apply_pilot_landfill_review(resource)
    assert resource == expected
    apply_pilot_landfill_review(resource)
    assert resource == expected


@pytest.mark.parametrize("change", ["factor", "target"])
def test_changed_landfill_coordinate_refuses_without_mutation(change):
    resource = deepcopy(load_uvek_technosphere_resource())
    rule = next(r for r in resource["replace"] if r["id"] == "bp-b39070e2c2728597")
    if change == "factor":
        rule["conversion_factor"] = 1
    else:
        rule["target"] = {**rule["target"], "location": "unreviewed"}
    before = deepcopy(resource)
    with pytest.raises(ValueError, match="reference coordinate changed"):
        apply_pilot_landfill_review(resource)
    assert resource == before


def test_changed_landfill_evidence_requires_review(tmp_path):
    path = tmp_path / "brightpath/data/export/uvek_pilot_bare_landfill_review.json"
    path.parent.mkdir(parents=True)
    path.write_text("{}")
    with pytest.raises(ValueError, match="evidence changed"):
        apply_pilot_landfill_review({}, tmp_path)
