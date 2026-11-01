"""Restore native wire identities only within the exact UVEK 2026 context."""

import csv
from copy import deepcopy

import pytest

from brightpath import BackgroundProfile, BiosphereProfile, InventoryFormat
from brightpath.background import BiosphereCatalog, InMemoryCatalogProvider, PackageCatalogProvider
from brightpath.formats.simapro_csv import load_simapro_csv, normalize_simapro_import_data, write_simapro_csv
from brightpath.models import InventoryDocument

PROFILE = BackgroundProfile("uvek", "2026", "cutoff")
NATIVE = BiosphereProfile("uvek", "2026")
NAME = "Carbon dioxide, biogenic"


def document():
    process = {"name": "Native reader fixture", "reference product": "product", "location": "CH", "unit": "kilogram"}
    return InventoryDocument(
        data=[
            {
                **process,
                "comment": "Synthetic native scalar; unchanged source.",
                "parameters": [{"name": "dose", "amount": -0.75}],
                "exchanges": [
                    {**process, "type": "production", "amount": 1, "simapro category": "Materials/Test"},
                    {
                        "type": "biosphere",
                        "name": NAME,
                        "categories": ("emissions to air", "unspecified"),
                        "unit": "kilogram",
                        "amount": -0.75,
                        "formula": "dose",
                        "uncertainty type": 3,
                        "loc": -0.75,
                        "scale": 0.125,
                        "comment": "Original flow provenance.",
                    },
                ],
            }
        ],
        background_profile=PROFILE,
        biosphere_profile=NATIVE,
        inventory_format=InventoryFormat.SIMAPRO_CSV,
    )


@pytest.mark.parametrize("compartment", ["unspecified", "high. pop.", "low. pop."])
@pytest.mark.parametrize("formula", [False, True])
def test_scalar_reader_restores_literal_native_name_and_exact_air_compartment(tmp_path, compartment, formula):
    source = document()
    data = source.data
    if formula:
        # Formula rows have no scalar distribution in the existing CSV reader.
        for key in ("uncertainty type", "loc", "scale"):
            data[0]["exchanges"][1].pop(key)
    else:
        data[0]["exchanges"][1].pop("formula")
    source = source.replace(data=data)
    before = source.data
    path, _ = write_simapro_csv(source, tmp_path / "scalar.csv")
    # Native high/low air labels are syntax fixtures for the reader; this does
    # not change the writer's separate subcompartment support boundary.
    rows = list(csv.reader(path.open(encoding="latin-1", newline=""), delimiter=";"))
    for row in rows:
        if row and row[0] == NAME:
            row[1] = "" if compartment == "unspecified" else compartment
    with path.open("w", encoding="latin-1", newline="") as handle:
        csv.writer(handle, delimiter=";").writerows(rows)
    provider = PackageCatalogProvider()
    result = load_simapro_csv(path, background_profile=PROFILE, biosphere_profile=NATIVE, catalog_provider=provider)
    flow = next(e for e in result.data[0]["exchanges"] if e["type"] == "biosphere")
    identity = (flow["name"], tuple(flow["categories"]), flow["unit"])
    assert identity == (NAME, ("emissions to air", compartment), "kilogram")
    assert identity in provider.load_biosphere(NATIVE).identities
    for key in ("amount", "formula", "uncertainty type", "loc", "scale", "comment"):
        if key in before[0]["exchanges"][1]:
            assert flow[key] == before[0]["exchanges"][1][key]
    assert not any(key.startswith("_brightpath_native") for key in flow)
    assert source.data == before


def raw():
    data = document().data
    data[0]["simapro name"] = "Native reader fixture/CH U"
    data[0]["exchanges"][0]["simapro name"] = data[0]["simapro name"]
    data[0]["exchanges"][1].update(
        name="Carbon dioxide, non-fossil", **{"simapro name": NAME, "categories": ("air",), "unit": "kg"}
    )
    return data


def normalize(data, identities, *, background=PROFILE, biosphere=NATIVE):
    provider = InMemoryCatalogProvider(biosphere=[BiosphereCatalog(biosphere, identities)])
    return normalize_simapro_import_data(
        data,
        background_profile=background,
        biosphere_profile=biosphere,
        database_name="test",
        catalog_provider=provider,
        biosphere_correspondence={},
        version_mapping={},
    )


def test_native_ambiguous_wire_identity_is_an_error_and_input_is_unchanged():
    data = raw()
    before = deepcopy(data)
    with pytest.raises(ValueError, match="Ambiguous native UVEK SimaPro biosphere identity"):
        normalize(
            data, [(NAME, ("emissions to air", "unspecified"), "kilogram"), (NAME, ("emissions to air",), "kilogram")]
        )
    assert data == before


@pytest.mark.parametrize("mutation", ["name", "compartment", "unit"])
def test_no_fallback_to_a_parsed_alias_other_compartment_or_other_unit(mutation):
    data = raw()
    flow = data[0]["exchanges"][1]
    if mutation == "name":
        flow["simapro name"] = "Missing native label"
    elif mutation == "compartment":
        flow["categories"] = ("water",)
    else:
        flow["unit"] = "cubic meter"
    before = deepcopy(data)
    identities = [(NAME, ("emissions to air", "unspecified"), "kilogram")]
    with pytest.raises(ValueError, match="No exact native UVEK SimaPro biosphere identity"):
        normalize(data, identities)
    assert data == before


@pytest.mark.parametrize(
    "background,biosphere",
    [
        (PROFILE, BiosphereProfile("ecoinvent", "3.10")),
        (BackgroundProfile("uvek", "2025", "cutoff"), BiosphereProfile("ecoinvent", "3.10")),
        (BackgroundProfile("ecoinvent", "3.12", "cutoff"), BiosphereProfile("ecoinvent", "3.10")),
    ],
)
def test_explicit_biosphere_context_is_not_inferred_from_technosphere(background, biosphere):
    data = raw()
    result = normalize(
        data, [("Carbon dioxide, non-fossil", ("air",), "kilogram")], background=background, biosphere=biosphere
    )
    assert result[0]["exchanges"][1]["name"] == "Carbon dioxide, non-fossil"
    assert result[0]["exchanges"][1]["categories"] == ("air",)


def test_normalization_is_idempotent_without_mutating_numeric_or_unknown_fields():
    data = raw()
    data[0]["exchanges"][1]["custom scalar"] = 13
    identity = (NAME, ("emissions to air", "unspecified"), "kilogram")
    first = normalize(data, [identity])
    assert normalize(first, [identity]) == first
    for key in ("amount", "formula", "uncertainty type", "loc", "scale", "comment", "custom scalar"):
        assert first[0]["exchanges"][1][key] == data[0]["exchanges"][1][key]


@pytest.mark.parametrize(
    "name,unit",
    [
        ("Occupation, industrial area", "square meter-year"),
        ("Transformation, from pasture, man made", "square meter"),
        ("Transformation, to industrial area", "square meter"),
        ("Transformation, from unspecified", "square meter"),
    ],
)
def test_8406_and_dac_scalar_keep_already_valid_parsed_native_land_identity(tmp_path, name, unit):
    source = document()
    data = source.data
    flow = data[0]["exchanges"][1]
    flow.update(name=name, categories=("resources", "land"), unit=unit)
    flow.pop("formula")
    source = source.replace(data=data)
    before = source.data
    path, _ = write_simapro_csv(source, tmp_path / "native-land.csv")
    provider = PackageCatalogProvider()
    result = load_simapro_csv(path, background_profile=PROFILE, biosphere_profile=NATIVE, catalog_provider=provider)
    actual = next(e for e in result.data[0]["exchanges"] if e["type"] == "biosphere")
    expected_identity = (name, ("resources", "land"), unit)
    assert expected_identity in provider.load_biosphere(NATIVE).identities
    assert (actual["name"], tuple(actual["categories"]), actual["unit"]) == expected_identity
    for key in ("amount", "uncertainty type", "loc", "scale", "comment"):
        assert actual[key] == before[0]["exchanges"][1][key]
    assert source.data == before


def test_ambiguous_parsed_native_identity_cannot_be_overridden_by_unique_raw_label():
    data = raw()
    data[0]["exchanges"][1]["name"] = "Parsed native flow"
    with pytest.raises(ValueError, match="Ambiguous native UVEK SimaPro biosphere identity"):
        normalize(
            data,
            [
                ("Parsed native flow", ("emissions to air", "unspecified"), "kilogram"),
                ("Parsed native flow", ("emissions to air",), "kilogram"),
                (NAME, ("emissions to air", "unspecified"), "kilogram"),
            ],
        )
