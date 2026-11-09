"""Country PE markets preserve their source fates and separately collected freight."""

from copy import deepcopy
import hashlib
import json

import pytest

from brightpath import BrightwayInventory, DATA_DIR
from brightpath.core.policies import MigrationPolicy
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource
from scripts.review_uvek_pilot_nitrate_pe_country import apply_pilot_nitrate_pe_country_review
from test_uvek_reference_signs import consumer, contexts
from test_uvek_refrigerant_disposal import target_context

REVIEW = json.loads((DATA_DIR / "export/uvek_pilot_nitrate_pe_country_review.json").read_text())


@pytest.mark.parametrize("row", REVIEW["records"], ids=lambda r: r["source"]["location"])
@pytest.mark.parametrize("amount", [-2, 0, 2])
@pytest.mark.parametrize("uncertainty", [2, 3])
def test_country_markets_keep_waste_sign_uncertainty_and_complete_fates(row, amount, uncertainty):
    data = consumer(row["source"], amount, uncertainty)
    before = deepcopy(data)
    source = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.9"))
    result = source.migrate_background(target_context("2026"), policy=MigrationPolicy.permissive())
    assert source.data == data == before
    records = result.data
    actual = records[0]["exchanges"][1]
    original = before[0]["exchanges"][1]
    assert actual["amount"] == -amount and actual["formula"] == "(dose) * (-1.0)"
    assert actual["scale"] == original["scale"]
    if uncertainty == 2:
        assert actual["loc"] == original["loc"] and actual["negative"] is not original["negative"]
    else:
        assert actual["loc"] == -original["loc"]
        assert actual["minimum"] == -original["maximum"] and actual["maximum"] == -original["minimum"]
    helper = next(r for r in records if r.get("code") == actual["input"][1])
    inputs = [x for x in helper["exchanges"] if x["type"] != "production"]
    expected = row["profiles"][0]["inputs"]
    assert len(inputs) == len(expected)
    by_recipe = {r.get("brightpath_conversion", {}).get("recipe_id"): r for r in records[1:]}
    assert len(records) == 2 + sum("recipe" in e for e in expected)
    for actual_input, original_input in zip(inputs, expected, strict=True):
        if "recipe" in original_input:
            fate = by_recipe[original_input["recipe"]]
            assert all(actual_input[f] == fate[f] for f in ("name", "reference product", "location", "unit"))
            assert sum(e["type"] == "biosphere" for e in fate["exchanges"]) in {79, 108}
            assert not any("incineration" in e["name"] for e in fate["exchanges"] if e["type"] == "technosphere")
        for field, value in original_input.items():
            if field != "recipe":
                assert actual_input[field] == value
    assert sum(x["unit"] == "ton kilometer" for x in inputs) == 1
    assert sum(x["type"] == "biosphere" for r in records[1:] for x in r["exchanges"]) == (
        187 if row["source"]["location"] == "CY" else 295
    )
    assert not any(i.code == "migration.recipe_input_omission" for i in result.last_migration_report.issues)


@pytest.mark.parametrize("row", REVIEW["records"], ids=lambda r: r["source"]["location"])
@pytest.mark.parametrize("version", ["3.6", "3.12"])
def test_country_helpers_refuse_uninspected_profiles(row, version):
    source = BrightwayInventory.from_data(consumer(row["source"], -1), context=contexts("ecoinvent", version))
    before = deepcopy(source.data)
    with pytest.raises(MigrationError):
        source.migrate_background(target_context("2026"), policy=MigrationPolicy.permissive())
    assert source.data == before


@pytest.mark.parametrize("change", ["none", "parent", "frozen_recipe", "freight"])
def test_country_authoring_replay_is_atomic_and_rejects_changed_evidence(change):
    resource = deepcopy(load_uvek_technosphere_resource())
    if change == "parent":
        next(r for r in resource["replace"] if r["id"] == REVIEW["records"][-1]["rule_id"])["confidence"] = 0.123
    elif change == "frozen_recipe":
        resource["recipes"][next(iter(REVIEW["recipe_dependencies"]))]["inputs"][0]["amount"] += 1
    elif change == "freight":
        binding = REVIEW["records"][0]["profiles"][0]["bindings"][0]
        fields = ("name", "reference product", "location", "unit")
        next(r for r in resource["replace"] if [r["source"][f] for f in fields] == binding["source"])[
            "confidence"
        ] = 0.123
    before = deepcopy(resource)
    if change == "none":
        apply_pilot_nitrate_pe_country_review(resource)
        assert resource == before
    else:
        with pytest.raises(ValueError):
            apply_pilot_nitrate_pe_country_review(resource)
        assert resource == before


def test_country_reviews_pin_exact_treatments_and_original_authoring_authorities():
    assert {r["source"]["location"] for r in REVIEW["records"]} == {"PE", "IN", "BR", "CO", "CY", "ZA"}
    assert len(REVIEW["recipe_dependencies"]) == 5
    (br,) = [r for r in REVIEW["records"] if r["source"]["location"] == "BR"]
    (controlled,) = [r for r in br["source_fates"] if r.get("whole_controlled_service")]
    assert controlled["target"]["name"] == "Disposal, polyethylene, 0.4% water, to municipal incineration"
    assert controlled["target_selected_from_prior_full_waste_composition_review"]
    assert controlled["declared_wet_composition_coordinates_matching_prior_review"] == 44
    assert REVIEW["controlled_boundary_correction"]["removed_wrapper_collection_tkm_per_kg_treatment"] == 0.031
    assert REVIEW["controlled_boundary_correction"]["original_source_market_collection_preserved"]
    for row in REVIEW["authorization_evidence"] + REVIEW["historical_binding_authorities"]:
        assert hashlib.sha256((DATA_DIR / row["path"]).read_bytes()).hexdigest() == row["sha256"]
    assert sum(row["affected_binding_records"] for row in REVIEW["historical_binding_authorities"]) == 324
    resource = load_uvek_technosphere_resource()
    for rid, expected in REVIEW["recipe_dependencies"].items():
        assert hashlib.sha256(json.dumps(resource["recipes"][rid], sort_keys=True).encode()).hexdigest() == expected
