"""Geothermal dependencies retain exact profile boundaries and treatment mixtures."""

import json
from copy import deepcopy

import pytest

from brightpath import DATA_DIR, TechnosphereProfile
from brightpath.background.reference_signs import select_profile_sign_rules
from brightpath.migrations.resources import load_uvek_technosphere_resource
from scripts.review_uvek_geothermal_36_markets import REVIEW_PATH, apply_geothermal_36_market_review


def test_market_replay_is_atomic_idempotent_and_keeps_existing_wastewater_profile():
    evidence = json.loads((DATA_DIR.parent.parent / REVIEW_PATH).read_text())
    resource = deepcopy(load_uvek_technosphere_resource())
    wastewater = next(r for r in evidence["records"] if "wastewater" in r["recipe_id"])
    old = deepcopy(resource)
    for row in evidence["records"]:
        parent = next(r for r in resource["replace"] if r["id"] == row["rule_id"])
        parent.clear()
        parent.update(deepcopy(row["previous_rule"]))
    earlier = select_profile_sign_rules(resource, TechnosphereProfile("ecoinvent", "3.9", "cutoff"))
    apply_geothermal_36_market_review(resource)
    selected = select_profile_sign_rules(resource, TechnosphereProfile("ecoinvent", "3.9", "cutoff"))
    old_rule = next(r for r in earlier["replace"] if r["id"] == wastewater["rule_id"])
    new_rule = next(r for r in selected["replace"] if r["id"] == wastewater["rule_id"])
    assert {k: v for k, v in old_rule.items() if k != "review_ref"} == {
        k: v for k, v in new_rule.items() if k != "review_ref"
    }
    assert resource == old
    apply_geothermal_36_market_review(resource)
    assert resource == old
    parent = next(r for r in resource["replace"] if r["id"] == evidence["records"][-1]["rule_id"])
    parent["target"]["location"] = "unreviewed"
    before = deepcopy(resource)
    with pytest.raises(ValueError, match="parent"):
        apply_geothermal_36_market_review(resource)
    assert resource == before


def test_wastewater_classes_drilling_routes_and_cement_grades_remain_distinct():
    evidence = json.loads((DATA_DIR.parent.parent / REVIEW_PATH).read_text())
    recipes = evidence["recipes"]
    sewage = recipes["geothermal-36-wastewater"]["inputs"]
    assert len(sewage) == 5
    assert {e["name"].rsplit(" ", 1)[-1] for e in sewage} == set("12345")
    assert sum(e["amount"] for e in sewage) == pytest.approx(1)
    drilling = recipes["geothermal-36-drilling-waste"]["inputs"]
    treatments = [e for e in drilling if e.get("unit") == "kilogram"]
    assert [e["amount"] for e in treatments] == [0.6, 0.4]
    assert "landfarming" in treatments[0]["name"]
    assert "residual material landfill" in treatments[1]["name"]
    assert all(e["amount"] > 0 for e in drilling)
    assert not any("aircraft" in e.get("name", "") or "coal freight" in e.get("name", "") for e in drilling)
    cement = recipes["geothermal-36-portland-cement"]["inputs"]
    grades = [e for e in cement if e.get("unit") == "kilogram"]
    assert len(grades) == 2
    assert sum(e["amount"] for e in grades) == pytest.approx(1)
    assert not any("slag" in e.get("name", "").lower() for e in cement)
