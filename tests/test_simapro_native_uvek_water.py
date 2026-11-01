"""Candidate integration regressions; run only after coordinated codec unfreeze.

These exercise actual CSV writing and BP readback, not a standalone inverse.
All quantities are synthetic. No native background vectors or user DBs.
"""

from copy import deepcopy

import pytest

from brightpath import BackgroundProfile, BiosphereProfile, InventoryFormat
from brightpath.background import PackageCatalogProvider
from brightpath.formats.simapro_csv import load_simapro_csv, normalize_simapro_import_data, write_simapro_csv
from brightpath.models import InventoryDocument

NATIVE = BiosphereProfile("uvek", "2026")
PROFILE = BackgroundProfile("uvek", "2026", "cutoff")


def document(background=PROFILE, biosphere=NATIVE, *, formula=False, normal=False, amount=0.75):
    native = biosphere == NATIVE
    process = {
        "name": "Synthetic Water unit fixture",
        "reference product": "fixture",
        "location": "CH",
        "unit": "kilogram",
    }
    flow = {
        "name": "Water",
        "type": "biosphere",
        "amount": amount,
        "unit": "kilogram" if native else "cubic meter",
        "categories": ("emissions to air", "unspecified") if native else ("air",),
        "comment": "Synthetic Water quantity; no native source coefficients.",
    }
    if formula:
        flow["formula"] = "dose"
    if normal:
        flow.update({"uncertainty type": 3, "loc": amount, "scale": 0.125})
    return InventoryDocument(
        data=[
            {
                **process,
                "comment": "Synthetic native unit regression.",
                "parameters": [{"name": "dose", "amount": amount}],
                "exchanges": [
                    {**process, "type": "production", "amount": 1, "simapro category": "Materials/Test"},
                    flow,
                ],
            }
        ],
        background_profile=background,
        biosphere_profile=biosphere,
        inventory_format=InventoryFormat.SIMAPRO_CSV,
    )


def check_roundtrip(source, tmp_path):
    before = deepcopy(source.data)
    path, _ = write_simapro_csv(source, tmp_path / "water.csv")
    result = load_simapro_csv(
        path,
        background_profile=source.background_profile,
        biosphere_profile=source.biosphere_profile,
        catalog_provider=PackageCatalogProvider(),
    )
    expected = before[0]["exchanges"][1]
    flow = next(e for e in result.data[0]["exchanges"] if e["type"] == "biosphere")
    for key in ("name", "unit", "categories", "amount", "formula", "uncertainty type", "loc", "scale", "comment"):
        if key in expected:
            assert flow[key] == expected[key], key
    assert not any(k.startswith("_brightpath") for k in flow)
    assert source.data == before


@pytest.mark.parametrize("amount", [0.75, -0.75])
@pytest.mark.parametrize("mode", ["scalar", "formula", "normal"])
def test_actual_native_water_mass_csv_readback_preserves_signed_quantity_and_metadata(tmp_path, amount, mode):
    check_roundtrip(document(amount=amount, formula=mode == "formula", normal=mode == "normal"), tmp_path)


@pytest.mark.parametrize(
    "background",
    [PROFILE, BackgroundProfile("uvek", "2025", "cutoff"), BackgroundProfile("ecoinvent", "3.10", "cutoff")],
)
@pytest.mark.parametrize("formula", [False, True])
def test_declared_ei_biosphere_keeps_existing_volume_wire_conversion(tmp_path, background, formula):
    # Biosphere profile decides the unit contract, independently of tech family.
    check_roundtrip(document(background, BiosphereProfile("ecoinvent", "3.10"), formula=formula), tmp_path)


def test_native_unit_mismatch_is_still_refused_and_input_not_mutated():
    data = document().data
    data[0]["simapro name"] = "Synthetic Water unit fixture/CH U"
    data[0]["exchanges"][0]["simapro name"] = data[0]["simapro name"]
    flow = data[0]["exchanges"][1]
    flow.update(
        {
            "simapro name": "Water",
            "categories": ("air",),
            "unit": "cubic meter",
            "_brightpath_native_biosphere_wire": (("air",), "kg"),
        }
    )
    before = deepcopy(data)
    with pytest.raises(ValueError, match="Native UVEK SimaPro biosphere unit changed during import"):
        normalize_simapro_import_data(
            data,
            background_profile=PROFILE,
            biosphere_profile=NATIVE,
            database_name="test",
            catalog_provider=PackageCatalogProvider(),
            biosphere_correspondence={},
            version_mapping={},
        )
    assert data == before
