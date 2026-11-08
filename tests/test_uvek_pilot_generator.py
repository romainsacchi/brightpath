"""Fuel normalization stays exact-profile, positive-reference and reproducible."""

from copy import deepcopy
import json

import pytest

from brightpath import DATA_DIR
from brightpath.background.reference_signs import select_profile_sign_rules
from brightpath.core.context import TechnosphereProfile
from brightpath.migrations.resources import load_uvek_technosphere_resource
from scripts.review_uvek_pilot_generator import apply_pilot_generator_fuel_review


def test_generator_fuel_draw_and_source_profile_gate():
    review = json.loads((DATA_DIR / "export/uvek_pilot_generator_fuel_review.json").read_text())
    (decision,) = review["rules"]
    resource = load_uvek_technosphere_resource()
    for version, status in (("3.6", "verified"), ("3.9", "unresolved")):
        selected = select_profile_sign_rules(resource, TechnosphereProfile("ecoinvent", version, "cutoff"))
        rule = next(r for r in selected["replace"] if r["id"] == decision["rule_id"])
        assert rule["conversion_factor"] * decision["native_diesel_kg_per_reference"] == pytest.approx(
            decision["source_diesel_kg_per_reference"], rel=1e-15
        )
        assert rule["reference_flow_sign_review"]["status"] == status
        assert rule["target"] == decision["target"]
        assert rule["report_correspondence"]
        assert "18.5kW" in rule["technosphere_information_loss"]
        assert "no electrical efficiency is inferred" in rule["technosphere_information_loss"]


def test_generator_authoring_replay_changes_only_approved_rule():
    resource = deepcopy(load_uvek_technosphere_resource())
    expected = deepcopy(resource)
    next(r for r in resource["replace"] if r["id"] == "bp-07968f74bde91ad7")["conversion_factor"] = 1
    apply_pilot_generator_fuel_review(resource)
    assert resource == expected
    apply_pilot_generator_fuel_review(resource)
    assert resource == expected


@pytest.mark.parametrize("change", ["factor", "target", "source"])
def test_changed_generator_authoring_coordinate_refuses_before_mutation(change):
    resource = deepcopy(load_uvek_technosphere_resource())
    rule = next(r for r in resource["replace"] if r["id"] == "bp-07968f74bde91ad7")
    if change == "factor":
        rule["conversion_factor"] = 3
    else:
        rule[change] = {**rule[change], "location": "unreviewed"}
    before = deepcopy(resource)
    with pytest.raises(ValueError, match="coordinate changed|supplier or unit changed"):
        apply_pilot_generator_fuel_review(resource)
    assert resource == before


def test_generator_evidence_drift_requires_review(tmp_path):
    path = tmp_path / "brightpath/data/export/uvek_pilot_generator_fuel_review.json"
    path.parent.mkdir(parents=True)
    path.write_text("{}")
    with pytest.raises(ValueError, match="evidence changed"):
        apply_pilot_generator_fuel_review({}, tmp_path)
