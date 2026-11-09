"""The generic copper market keeps a complete metal supply and profile boundary."""

from copy import deepcopy
import hashlib
import json

import pytest

from brightpath import DATA_DIR
from brightpath.background.reference_signs import select_profile_sign_rules
from brightpath.core.context import TechnosphereProfile
from brightpath.migrations.resources import load_uvek_technosphere_resource
from scripts.review_uvek_pilot_copper_supply import apply_pilot_copper_supply_review


def test_generic_copper_uses_complete_supply_only_for_inspected_source():
    resource = load_uvek_technosphere_resource()
    for version, status in (("3.6", "verified"), ("3.9", "unresolved")):
        selected = select_profile_sign_rules(resource, TechnosphereProfile("ecoinvent", version, "cutoff"))
        rule = next(r for r in selected["replace"] if r["id"] == "bp-1cb5578cf49cfd48")
        assert rule["source"]["reference product"] == "copper"
        assert rule["target"]["name"] == "Copper, at regional storage"
        assert rule["target"]["location"] == "RER" and rule["target"]["unit"] == "kilogram"
        assert rule["conversion_factor"] == 1 and rule["report_correspondence"]
        assert rule["reference_flow_sign_review"]["status"] == status
        assert "primary/secondary shares" in rule["technosphere_information_loss"]
        assert "differs from the historical cathode-review row" in rule["technosphere_information_loss"]


def test_pinned_source_rename_preserves_copper_product_and_unit():
    review = json.loads((DATA_DIR / "export/uvek_pilot_metal_chemical_supply_review.json").read_text())
    (alias,) = review["source_alias_evidence"]
    raw = (DATA_DIR / alias["path"]).read_bytes()
    assert hashlib.sha256(raw).hexdigest() == alias["sha256"]
    rule = next(r for r in json.loads(raw)["replace"] if r["source"] == alias["source"])
    assert rule["target"] == alias["target"]
    assert rule["source"]["reference product"] == "copper"
    assert rule["target"]["reference product"] == "copper, cathode"
    assert rule["source"]["unit"] == rule["target"]["unit"] == "kg"
    assert rule.get("conversion_factor", 1) == 1


def test_authoring_replay_changes_only_the_reviewed_copper_rule():
    resource = deepcopy(load_uvek_technosphere_resource())
    expected = deepcopy(resource)
    review = json.loads((DATA_DIR / "export/uvek_pilot_metal_chemical_supply_review.json").read_text())
    (decision,) = review["rules"]
    next(r for r in resource["replace"] if r["id"] == decision["rule_id"])["target"] = decision["previous_target"]
    apply_pilot_copper_supply_review(resource)
    assert resource == expected
    apply_pilot_copper_supply_review(resource)
    assert resource == expected


@pytest.mark.parametrize("coordinate", ["target", "source"])
def test_supplier_drift_refuses_without_changing_other_rules(coordinate):
    resource = deepcopy(load_uvek_technosphere_resource())
    rule = next(r for r in resource["replace"] if r["id"] == "bp-1cb5578cf49cfd48")
    rule[coordinate] = {**rule[coordinate], "location": "unreviewed"}
    before = deepcopy(resource)
    with pytest.raises(ValueError, match="source, supplier or unit changed"):
        apply_pilot_copper_supply_review(resource)
    assert resource == before


def test_evidence_drift_refuses_before_mutation(tmp_path):
    path = tmp_path / "brightpath/data/export/uvek_pilot_metal_chemical_supply_review.json"
    path.parent.mkdir(parents=True)
    path.write_text("{}")
    with pytest.raises(ValueError, match="approval changed"):
        apply_pilot_copper_supply_review({}, tmp_path)
