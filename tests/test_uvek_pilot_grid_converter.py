"""Closer native suppliers preserve process stages and exact profile gates."""

import json
from copy import deepcopy

import pytest
from uvek_review_helpers import before_converter_steel_39_review

from brightpath import DATA_DIR
from brightpath.background.reference_signs import select_profile_sign_rules
from brightpath.core.context import TechnosphereProfile
from brightpath.migrations.resources import load_uvek_technosphere_resource
from scripts.review_uvek_pilot_grid_converter import apply_pilot_grid_converter_review


@pytest.mark.parametrize(
    "rule_id,version,name,location,limitation",
    [
        ("bp-4389bb6ee9ab35e9", "3.8", "Electricity, medium voltage, at grid", "ENTSO-E", "no added loss factor"),
        ("bp-ac05197a0852076a", "3.6", "Steel, converter, low-alloyed, at plant", "RER", "Add no rolling"),
    ],
)
def test_complete_native_boundary_and_exact_source_profile(rule_id, version, name, location, limitation):
    resource = load_uvek_technosphere_resource()
    for source, status in ((version, "verified"), ("3.10", "unresolved")):
        selected = select_profile_sign_rules(resource, TechnosphereProfile("ecoinvent", source, "cutoff"))
        rule = next(r for r in selected["replace"] if r["id"] == rule_id)
        assert rule["reference_flow_sign_review"]["status"] == status
        assert rule["target"]["name"] == name and rule["target"]["location"] == location
        assert rule["conversion_factor"] == 1 and rule["report_correspondence"]
        assert limitation in rule["technosphere_information_loss"]


def test_authoring_replay_changes_only_the_two_reviewed_suppliers():
    resource = before_converter_steel_39_review(deepcopy(load_uvek_technosphere_resource()))
    expected = deepcopy(resource)
    review = json.loads((DATA_DIR / "export/uvek_pilot_grid_converter_review.json").read_text())
    for decision in review["rules"]:
        next(r for r in resource["replace"] if r["id"] == decision["rule_id"])["target"] = decision["previous_target"]
    apply_pilot_grid_converter_review(resource)
    assert resource == expected
    apply_pilot_grid_converter_review(resource)
    assert resource == expected


@pytest.mark.parametrize("coordinate", ["target", "source"])
def test_unreviewed_supplier_drift_refuses_before_any_rule_mutation(coordinate):
    resource = deepcopy(load_uvek_technosphere_resource())
    rule = next(r for r in resource["replace"] if r["id"] == "bp-ac05197a0852076a")
    rule[coordinate] = {**rule[coordinate], "location": "unreviewed"}
    before = deepcopy(resource)
    with pytest.raises(ValueError, match="source, supplier or unit changed"):
        apply_pilot_grid_converter_review(resource)
    assert resource == before


def test_evidence_drift_refuses_before_resource_lookup(tmp_path):
    path = tmp_path / "brightpath/data/export/uvek_pilot_grid_converter_review.json"
    path.parent.mkdir(parents=True)
    path.write_text("{}")
    with pytest.raises(ValueError, match="approval changed"):
        apply_pilot_grid_converter_review({}, tmp_path)
