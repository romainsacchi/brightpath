"""Secondary billets are product supplies even when the source says treatment."""

from copy import deepcopy
import json

import pytest

from brightpath import DATA_DIR
from brightpath.background.reference_signs import select_profile_sign_rules
from brightpath.core.context import TechnosphereProfile
from brightpath.migrations.resources import load_uvek_technosphere_resource
from scripts.review_uvek_pilot_secondary_aluminium import apply_pilot_secondary_aluminium_review


def test_old_scrap_secondary_billets_keep_positive_mass_basis_and_profile_gate():
    resource = load_uvek_technosphere_resource()
    for version, status in (("3.6", "verified"), ("3.9", "unresolved")):
        selected = select_profile_sign_rules(resource, TechnosphereProfile("ecoinvent", version, "cutoff"))
        rule = next(r for r in selected["replace"] if r["id"] == "bp-84737a1a1790ee76")
        assert rule["source"]["reference product"] == "aluminium, cast alloy"
        assert rule["target"]["name"] == "Aluminium, secondary, from old scrap, at plant"
        assert rule["conversion_factor"] == 1 and rule["target"]["unit"] == "kilogram"
        assert rule["report_correspondence"] and rule["reference_flow_sign_review"]["status"] == status
        assert "without a waste-reference reversal" in rule["technosphere_information_loss"]


def test_authoring_replay_changes_only_the_secondary_aluminium_supplier():
    resource = deepcopy(load_uvek_technosphere_resource())
    expected = deepcopy(resource)
    review = json.loads((DATA_DIR / "export/uvek_pilot_secondary_aluminium_pcb_review.json").read_text())
    (decision,) = review["rules"]
    next(r for r in resource["replace"] if r["id"] == decision["rule_id"])["target"] = decision["previous_target"]
    apply_pilot_secondary_aluminium_review(resource)
    assert resource == expected
    apply_pilot_secondary_aluminium_review(resource)
    assert resource == expected


@pytest.mark.parametrize("coordinate", ["source", "target"])
def test_supplier_drift_refuses_before_mutation(coordinate):
    resource = deepcopy(load_uvek_technosphere_resource())
    rule = next(r for r in resource["replace"] if r["id"] == "bp-84737a1a1790ee76")
    rule[coordinate] = {**rule[coordinate], "location": "unreviewed"}
    before = deepcopy(resource)
    with pytest.raises(ValueError, match="source, supplier or unit changed"):
        apply_pilot_secondary_aluminium_review(resource)
    assert resource == before


def test_evidence_drift_refuses_before_resource_lookup(tmp_path):
    path = tmp_path / "brightpath/data/export/uvek_pilot_secondary_aluminium_pcb_review.json"
    path.parent.mkdir(parents=True)
    path.write_text("{}")
    with pytest.raises(ValueError, match="approval changed"):
        apply_pilot_secondary_aluminium_review({}, tmp_path)
