"""Frozen PE dump coefficients come from five independently recalculated climates."""

import hashlib
import json
import math

from brightpath import DATA_DIR


def test_public_pe_dump_calibration_keeps_climates_and_every_nonzero_coordinate():
    review = json.loads((DATA_DIR / "export/uvek_pe_public_open_dump_calculation.json").read_text())
    assert review["origin"].startswith("Recalculated public Doka 2017 workbooks")
    assert review["engine"]["macros_disabled"] is True
    assert review["method"]["repository_cached_coordinates_refreshed_per_model"] == 6683
    counts = {row["climate"]: len(row["coefficients"]) for row in review["cases"]}
    assert counts == {"wet": 108, "dry": 108, "hyperarid": 101, "moist": 108, "very-wet": 108}
    infiltration = {row["climate"]: row["parameters"]["net_infiltration_mm"] for row in review["cases"]}
    assert infiltration == {"wet": 500, "dry": 100, "hyperarid": -250, "moist": 300, "very-wet": 1000}
    for row in review["cases"]:
        assert row["parameters"]["management"] == "open dump (d)"
        assert row["parameters"]["dump_height_m"] == 7
        assert row["parameters"]["operation_years"] == 15
        assert row["calibration"]["source_inventory_modified"] is False
        for key in (
            "rounded_mean_matches",
            "base_variance_matches",
            "total_variance_matches",
            "log_location_matches",
            "registered_biosphere_definitions",
        ):
            assert row["calibration"][key] == len(row["coefficients"])
        assert len({x["publisher_uuid"] for x in row["coefficients"]}) == len(row["coefficients"])


def test_public_pe_dump_uncertainty_rounds_the_base_before_adding_pedigree():
    review = json.loads((DATA_DIR / "export/uvek_pe_public_open_dump_calculation.json").read_text())
    for case in review["cases"]:
        for row in case["coefficients"]:
            assert row["amount"] != 0 and math.isfinite(row["amount"])
            assert row["base_variance"] == float(format(row["calculated_base_variance"], ".5g"))
            assert row["total_variance"] == row["base_variance"] + 0.008
            assert row["loc"] == math.log(abs(row["amount"]))
            assert row["uncertainty_type"] == 2
            assert row["public_model_definition"]["categories"] == row["target_biosphere_definition"]["categories"]


def test_public_pe_dump_reuses_only_the_existing_same_cas_nickel_synonym():
    review = json.loads((DATA_DIR / "export/uvek_pe_public_open_dump_calculation.json").read_text())
    dependency = review["canonical_identity_dependency"]
    path = DATA_DIR.parents[1] / dependency["path"]
    assert hashlib.sha256(path.read_bytes()).hexdigest() == dependency["sha256"]
    assert dependency["new_proxy_or_mass_scaling"] is False
    assert dependency["source_cas"] == dependency["target_cas"] == "014701-22-5"
    assert dependency["source_profile"] == {"family": "ecoinvent", "version": "3.8"}
    for case in review["cases"]:
        renamed = [r for r in case["coefficients"] if r["publisher_uuid"] != r["target_publisher_uuid"]]
        assert len(renamed) == (0 if case["climate"] == "hyperarid" else 1)
        for row in renamed:
            assert row["publisher_uuid"] == dependency["source_publisher_uuid"]
            assert row["target_publisher_uuid"] == dependency["target_publisher_uuid"]
            assert row["target_biosphere_definition"]["name"] == "Nickel II"
            assert row["target_biosphere_definition"]["unit"] == "kilogram"
