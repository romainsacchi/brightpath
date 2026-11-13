"""Public MSW treatment calculations retain fate, compartments and marginal uncertainty."""

import hashlib
import json
import math

import pytest

from brightpath import DATA_DIR


@pytest.fixture(scope="module")
def review():
    return json.loads((DATA_DIR / "export/uvek_msw_public_treatment_calculation.json").read_text())


def test_all_nine_calibrations_keep_nonzero_physical_coordinates(review):
    assert len(review["cases"]) == 9
    assert sum(len(case["coefficients"]) for case in review["cases"]) == 1008
    assert review["method"]["physical_coordinates_omitted"] == 0
    assert review["engine"]["macros_disabled"] is True
    for case in review["cases"]:
        rows = case["coefficients"]
        assert len({row["publisher_uuid"] for row in rows}) == len(rows)
        assert case["calibration"]["source_inventory_modified"] is False
        for key in (
            "rounded_mean_matches",
            "base_variance_matches",
            "total_variance_matches",
            "log_location_matches",
            "registered_biosphere_definitions",
        ):
            assert case["calibration"][key] == len(rows)
        for row in rows:
            assert row["amount"] != 0 and math.isfinite(row["amount"])
            assert row["negative"] == (row["amount"] < 0)
            assert row["base_variance"] == float(format(row["calculated_base_variance"], ".5g"))
            assert row["total_variance"] == row["base_variance"] + case["pedigree_variance_addition"]
            assert row["loc"] == math.log(abs(row["amount"]))
            assert row["uncertainty_type"] == 2
            assert row["public_model_definition"]["categories"] == row["target_biosphere_definition"]["categories"]


def test_landfill_climates_and_management_are_not_collapsed(review):
    cases = {case["case"]: case for case in review["cases"]}
    for climate, infiltration in {"dry": 100, "moist": 300, "wet": 500, "very-wet": 1000}.items():
        dump = cases[f"open-dump-{climate}"]
        landfill = cases[f"unsanitary-{climate}"]
        for case in (dump, landfill):
            p = case["parameters"]
            assert p["mean_annual_precipitation_mm"] - p["actual_evapotranspiration_mm"] == infiltration
            assert p["net_infiltration_mm"] == infiltration
            # Both near-term and long-term groundwater releases are part of the model.
            compartments = {tuple(r["target_biosphere_definition"]["categories"]) for r in case["coefficients"]}
            assert ("water", "ground-") in compartments
            assert ("water", "ground-, long-term") in compartments
        assert dump["parameters"]["management"] == "open dump (d)"
        assert landfill["parameters"]["management"] == "unsanitary landfill (u)"
        assert dump["parameters"]["height_m"] == 7
        assert landfill["parameters"]["height_m"] == 15

        def methane(case):
            return sum(
                r["amount"]
                for r in case["coefficients"]
                if r["target_biosphere_definition"]["name"] == "Methane, fossil"
            )

        assert methane(dump) < methane(landfill)
        assert not dump["public_ancillary_exchanges"]
        assert {r["name"] for r in landfill["public_ancillary_exchanges"]} == {
            "excavation, skid-steer loader",
            "diesel, burned in building machine",
        }


def test_burning_keeps_recovery_and_receiving_compartments(review):
    case = next(c for c in review["cases"] if c["case"] == "open-burning")
    p = case["parameters"]
    assert p["waste_index"] == 97  # The original public legacy-MSW mixture, not the incineration-only mixture.
    assert p["air_compartment_flag"] == "hi" and p["soil_compartment_flag"] == "ind"
    assert p["iron_metal_recovery"] == p["copper_metal_recovery"] == 0.95
    assert p["aluminium_metal_recovery"] == 0
    for row in case["coefficients"]:
        comp = row["target_biosphere_definition"]["categories"]
        if comp[0] == "air":
            assert comp[1] == "urban air close to ground"
        if comp[0] == "soil":
            assert comp[1] == "industrial"
    recovered = {r["name"]: r for r in case["public_ancillary_exchanges"]}
    assert recovered["iron scrap, sorted, pressed"]["amount"] == 0.017098
    assert recovered["copper scrap, sorted, pressed"]["amount"] == 0.00083857
    assert all(r["direction"] == "output" for r in recovered.values())
    assert "not a complete municipal-disposal conversion approval" in review["status"]


def test_only_existing_publisher_synonyms_change_flow_ids(review):
    dependency = review["canonical_identity_dependency"]
    path = DATA_DIR.parents[1] / dependency["path"]
    assert hashlib.sha256(path.read_bytes()).hexdigest() == dependency["sha256"]
    assert dependency["new_proxy_or_mass_scaling"] is False
    allowed = {}
    for row in dependency["profiles"]:
        assert row["source_profile"] == {"family": "ecoinvent", "version": "3.8"}
        assert row["source_cas"] == row["target_cas"]
        assert row["status"] == "verified"
        allowed.update(dict.fromkeys(row["source_flow_ids"], row["target"]["flow_id"]))
    assert len(allowed) == 2  # Existing groundwater potassium and nickel publisher synonyms.
    for case in review["cases"]:
        for row in case["coefficients"]:
            source = row["publisher_uuid"]
            assert row["target_publisher_uuid"] == allowed.get(source, source)
