"""Dedicated incinerators retain exact waste identity and inspected profile gates."""

from copy import deepcopy
import json

import pytest

from brightpath import DATA_DIR
from brightpath.background.reference_signs import select_profile_sign_rules
from brightpath.core.context import TechnosphereProfile
from brightpath.migrations.resources import load_uvek_technosphere_resource
from scripts.review_uvek_pilot_incinerators import apply_pilot_incinerator_review


@pytest.fixture(scope="module")
def review():
    return json.loads((DATA_DIR / "export/uvek_pilot_incinerator_endpoint_review.json").read_text())


@pytest.mark.parametrize(
    "rule_id,material",
    [
        ("bp-e51642145510fc9c", "glass, 0% water"),
        ("bp-76fc3358de421a01", "polyethylene, 0.4% water"),
        ("bp-fa607ec8232396e4", "polypropylene, 15.9% water"),
        ("bp-7393e642acca8e35", "polystyrene, 0.2% water"),
    ],
)
def test_exact_source_profile_selects_dedicated_endpoint(review, rule_id, material):
    resource = load_uvek_technosphere_resource()
    decision = next(r for r in review["records"] if r["rule_id"] == rule_id)
    for version in ("3.6", "3.9"):
        selected = select_profile_sign_rules(resource, TechnosphereProfile("ecoinvent", version, "cutoff"))
        rule = next(r for r in selected["replace"] if r["id"] == rule_id)
        assert rule["target"] == decision["target"] != decision["previous_target"]
        assert rule["target"]["name"] == f"Disposal, {material}, to municipal incineration"
        assert rule["conversion_factor"] == -1
        assert rule["reference_flow_sign_review"]["status"] == ("verified" if version == "3.6" else "unresolved")
        assert rule["mapping_method"] == "heuristic_similarity"
    raw_rule = next(r for r in resource["replace"] if r["id"] == rule_id)
    assert raw_rule["reference_flow_sign_review"]["status"] == "unresolved"
    assert "wrapper" in raw_rule["technosphere_information_loss"]


def test_authoring_replay_is_idempotent_and_changes_only_reviewed_rules(review):
    resource = deepcopy(load_uvek_technosphere_resource())
    before = deepcopy(resource)
    for decision in review["records"]:
        next(r for r in resource["replace"] if r["id"] == decision["rule_id"])["target"] = deepcopy(
            decision["previous_target"]
        )
    apply_pilot_incinerator_review(resource)
    assert resource == before
    apply_pilot_incinerator_review(resource)
    assert resource == before


@pytest.mark.parametrize("change", ["factor", "target"])
def test_changed_authoring_coordinate_refuses_before_any_mutation(review, change):
    resource = deepcopy(load_uvek_technosphere_resource())
    rule = next(r for r in resource["replace"] if r["id"] == review["records"][-1]["rule_id"])
    if change == "factor":
        rule["conversion_factor"] = 1
    else:
        rule["target"] = {**rule["target"], "location": "unreviewed"}
    before = deepcopy(resource)
    with pytest.raises(ValueError, match="reference coordinate changed"):
        apply_pilot_incinerator_review(resource)
    assert resource == before


def test_changed_review_bytes_require_a_new_scientific_review(tmp_path):
    path = tmp_path / "brightpath/data/export/uvek_pilot_incinerator_endpoint_review.json"
    path.parent.mkdir(parents=True)
    path.write_text("{}")
    with pytest.raises(ValueError, match="evidence changed"):
        apply_pilot_incinerator_review({}, tmp_path)
