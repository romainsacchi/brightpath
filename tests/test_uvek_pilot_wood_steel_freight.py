"""Closer suppliers keep dry-mass, unrolled steel and complete freight bases."""

from copy import deepcopy
import json

import pytest

from brightpath import DATA_DIR
from brightpath.background.reference_signs import select_profile_sign_rules
from brightpath.core.context import TechnosphereProfile
from brightpath.migrations.resources import load_uvek_technosphere_resource
from scripts.review_uvek_pilot_wood_steel_freight import apply_pilot_wood_steel_freight_review


@pytest.mark.parametrize(
    "rule_id,versions,uninspected,name,unit,warning",
    [
        (
            "bp-84bd9cf473cdeee9",
            ["3.6", "3.9"],
            "3.12",
            "Wood chips, production mix, wet, measured as dry mass, at forest road & at sawmill",
            "kilogram",
            "kg dry matter",
        ),
        (
            "bp-4529f104ab14ba19",
            ["3.12"],
            "3.6",
            "Steel, electric, chromium steel 18/8, at plant",
            "kilogram",
            "Do not add hot rolling",
        ),
        (
            "bp-07d1ab8592869587",
            ["3.6"],
            "3.9",
            "Transport, freight, lorry, 32t-40t gross weight, fleet average",
            "ton kilometer",
            "net tkm",
        ),
    ],
)
def test_native_function_and_quantity_basis_keep_exact_profile_gate(
    rule_id, versions, uninspected, name, unit, warning
):
    resource = load_uvek_technosphere_resource()
    for version in [*versions, uninspected]:
        selected = select_profile_sign_rules(resource, TechnosphereProfile("ecoinvent", version, "cutoff"))
        rule = next(r for r in selected["replace"] if r["id"] == rule_id)
        assert rule["target"]["name"] == name and rule["target"]["location"] == "RER"
        assert rule["source"]["unit"] == rule["target"]["unit"] == unit
        assert rule["conversion_factor"] == 1 and rule["report_correspondence"]
        assert rule["reference_flow_sign_review"]["status"] == ("verified" if version in versions else "unresolved")
        assert warning in rule["technosphere_information_loss"]


def test_authoring_replay_changes_only_the_three_reviewed_suppliers():
    resource = deepcopy(load_uvek_technosphere_resource())
    expected = deepcopy(resource)
    review = json.loads((DATA_DIR / "export/uvek_pilot_wood_steel_freight_review.json").read_text())
    assert len(review["rules"]) == 3
    for decision in review["rules"]:
        next(r for r in resource["replace"] if r["id"] == decision["rule_id"])["target"] = decision["previous_target"]
    apply_pilot_wood_steel_freight_review(resource)
    assert resource == expected
    apply_pilot_wood_steel_freight_review(resource)
    assert resource == expected


@pytest.mark.parametrize("coordinate", ["source", "target"])
def test_changed_supplier_refuses_before_any_batch_mutation(coordinate):
    resource = deepcopy(load_uvek_technosphere_resource())
    rule = next(r for r in resource["replace"] if r["id"] == "bp-07d1ab8592869587")
    rule[coordinate] = {**rule[coordinate], "location": "unreviewed"}
    before = deepcopy(resource)
    with pytest.raises(ValueError, match="source, supplier or unit changed"):
        apply_pilot_wood_steel_freight_review(resource)
    assert resource == before


def test_evidence_drift_refuses_before_resource_lookup(tmp_path):
    path = tmp_path / "brightpath/data/export/uvek_pilot_wood_steel_freight_review.json"
    path.parent.mkdir(parents=True)
    path.write_text("{}")
    with pytest.raises(ValueError, match="approval changed"):
        apply_pilot_wood_steel_freight_review({}, tmp_path)
