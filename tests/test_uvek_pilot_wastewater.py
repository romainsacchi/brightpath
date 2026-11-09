"""Wastewater treatment must retain its class without extra residential collection."""

from copy import deepcopy
import json

import pytest

from brightpath import DATA_DIR
from brightpath.background.reference_signs import select_profile_sign_rules
from brightpath.core.context import TechnosphereProfile
from brightpath.migrations.resources import load_uvek_technosphere_resource
from scripts.review_uvek_pilot_wastewater import apply_pilot_wastewater_review


def test_class_two_wastewater_has_no_residential_collection_wrapper_and_keeps_profile_gate():
    resource = load_uvek_technosphere_resource()
    for version, status in (("3.6", "verified"), ("3.9", "unresolved")):
        selected = select_profile_sign_rules(resource, TechnosphereProfile("ecoinvent", version, "cutoff"))
        rule = next(r for r in selected["replace"] if r["id"] == "bp-fb5a0385ef4ba237")
        assert rule["target"]["name"] == "Treatment, sewage, to wastewater treatment, class 2"
        assert rule["target"]["unit"] == "cubic meter"
        assert rule["conversion_factor"] == -1
        assert rule["reference_flow_sign_review"]["status"] == status
        assert rule["report_correspondence"]
        assert "71100 population-capacity equivalents" in rule["technosphere_information_loss"]


def test_wastewater_replay_changes_only_the_wrong_endpoint():
    resource = deepcopy(load_uvek_technosphere_resource())
    expected = deepcopy(resource)
    review = json.loads((DATA_DIR / "export/uvek_pilot_wastewater_endpoint_review.json").read_text())
    (decision,) = review["records"]
    next(r for r in resource["replace"] if r["id"] == decision["rule_id"])["target"] = decision["previous_target"]
    apply_pilot_wastewater_review(resource)
    assert resource == expected
    apply_pilot_wastewater_review(resource)
    assert resource == expected


@pytest.mark.parametrize("change", ["factor", "target", "source"])
def test_changed_wastewater_coordinate_refuses_without_mutation(change):
    resource = deepcopy(load_uvek_technosphere_resource())
    rule = next(r for r in resource["replace"] if r["id"] == "bp-fb5a0385ef4ba237")
    if change == "factor":
        rule["conversion_factor"] = 1
    else:
        rule[change] = {**rule[change], "location": "unreviewed"}
    before = deepcopy(resource)
    with pytest.raises(ValueError, match="reference coordinate changed"):
        apply_pilot_wastewater_review(resource)
    assert resource == before


def test_changed_wastewater_evidence_requires_review(tmp_path):
    path = tmp_path / "brightpath/data/export/uvek_pilot_wastewater_endpoint_review.json"
    path.parent.mkdir(parents=True)
    path.write_text("{}")
    with pytest.raises(ValueError, match="evidence changed"):
        apply_pilot_wastewater_review({}, tmp_path)
