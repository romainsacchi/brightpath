"""New source-profile markets preserve old physical rules and current uncertainties."""

import json
from copy import deepcopy

import pytest

from brightpath import DATA_DIR, TechnosphereProfile
from brightpath.background.reference_signs import select_profile_sign_rules
from scripts.review_uvek_hydrogen_train_three_markets_312 import apply_hydrogen_train_three_markets_312_review


def evidence():
    return json.loads((DATA_DIR / "export/uvek_hydrogen_train_three_markets_312_review.json").read_text())


def original_resource():
    review = evidence()
    return {
        "replace": [deepcopy(row["original_rule"]) for row in review["records"]],
        "reviews": deepcopy(review["retired_reviews"]),
        "source_profiles": [{"system_model": "cutoff"}],
    }


def physical_rules(resource, version):
    selected = select_profile_sign_rules(resource, TechnosphereProfile("ecoinvent", version, "cutoff"))
    return [
        {k: v for k, v in r.items() if k not in {"review_ref", "technosphere_information_loss"}}
        for r in selected["replace"]
    ]


def test_replay_is_idempotent_and_preserves_previous_profile_physics():
    resource = original_resource()
    before = deepcopy(resource)
    apply_hydrogen_train_three_markets_312_review(resource)
    once = deepcopy(resource)
    apply_hydrogen_train_three_markets_312_review(resource)
    assert resource == once
    for version in ("3.9", "3.10", "3.11"):
        assert physical_rules(resource, version) == physical_rules(before, version)
    selected = select_profile_sign_rules(resource, TechnosphereProfile("ecoinvent", "3.12", "cutoff"))
    assert len(selected["replace"]) == 3
    for rule in selected["replace"]:
        assert rule["reference_flow_sign_review"]["status"] == "verified"
        assert rule["reference_flow_sign_review"]["source_sign"] == rule["conversion_factor"]
        assert rule["reference_flow_sign_review"]["target_sign"] == 1
        assert rule["target"]["recipe_ref"].startswith("train-312-")
    with pytest.raises(ValueError, match="system model"):
        select_profile_sign_rules(resource, TechnosphereProfile("ecoinvent", "3.12", "consequential"))


@pytest.mark.parametrize("change", ["parent", "recipe"])
def test_drift_fails_before_any_mutation(change):
    resource = original_resource()
    review = evidence()
    if change == "parent":
        resource["replace"][-1]["conversion_factor"] = 2
    else:
        key, recipe = next(iter(review["recipes"].items()))
        resource["recipes"] = {key: {**recipe, "inputs": []}}
    before = deepcopy(resource)
    with pytest.raises(ValueError, match="changed"):
        apply_hydrogen_train_three_markets_312_review(resource)
    assert resource == before


def test_market_freight_uncertainties_and_allocated_silica_basis_are_explicit():
    review = evidence()
    recipes = review["recipes"]
    freights = [e for recipe in recipes.values() for e in recipe["inputs"] if e["unit"] == "ton kilometer"]
    assert len(freights) == 18
    assert all(e["uncertainty type"] == 2 and e["scale"] == pytest.approx(0.44944410108488464) for e in freights)
    assert all(e["scale without pedigree"] == pytest.approx(0.34641016151377546) for e in freights)
    allocation = review["cryolite_coproduct_allocation_check"]
    assert allocation["ratio"] == pytest.approx(0.0312)
    assert allocation["nonzero_groups_checked"] == 14
    hazardous = recipes["train-312-hazardous-waste-market"]
    portions = [e for e in hazardous["inputs"] if e["name"].startswith("Disposal, hazardous waste")]
    assert len(portions) == 2 and [e["amount"] for e in portions] == [0.5, 0.5]
    assert portions[0]["comment"] != portions[1]["comment"]
    assert all("recipe" not in e for e in hazardous["inputs"])
