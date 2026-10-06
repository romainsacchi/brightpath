"""Native-format flow evidence applies independently of ecoinvent release."""

from copy import deepcopy

import bw2io
import pytest
from bw2io.extractors.simapro_csv import SimaProCSVExtractor
from test_conversion_preflight import _document, _preflight
from test_simapro_inventory import minimal_activity

from brightpath import BackgroundProfile, SimaProInventory
from brightpath.formats.simapro_csv import normalize_simapro_import_data

FLOWS = [
    ("Oxygen", ("natural resource", "in air"), "kilogram", "kg"),
    ("Oxygen", ("air", "unspecified"), "kilogram", "kg"),
    ("Oxygen", ("water", "surface water"), "kilogram", "kg"),
    ("Occupation, traffic area, road network", ("natural resource", "land"), "square meter-year", "m2a"),
    ("Water, turbine use, unspecified natural origin", ("natural resource", "in water"), "cubic meter", "m3"),
    ("Water, turbine use, unspecified natural origin, CH", ("natural resource", "in water"), "cubic meter", "m3"),
    ("Volume occupied, reservoir", ("natural resource", "in water"), "cubic meter-year", "m3y"),
    ("Energy, gross calorific value, in biomass, primary forest", ("natural resource", "biotic"), "megajoule", "MJ"),
    *[
        (name, ("air", "non-urban air or from high stacks"), "kilo Becquerel", "kBq")
        for name in [
            "Radon-222",
            "Radon-220",
            "Carbon-14",
            "Xenon-133",
            "Xenon-135",
            "Noble gases, radioactive, unspecified",
            "Hydrogen-3, Tritium",
        ]
    ],
]


def exchange(name, categories, unit="kilogram", **kwargs):
    return dict(
        type="biosphere",
        name=name,
        categories=categories,
        unit=unit,
        amount=-0.125,
        **{"uncertainty type": 3, "loc": -0.125, "scale": 0.01},
        **kwargs,
    )


@pytest.mark.parametrize("version", ["3.8", "3.9.1", "3.10", "3.12"])
@pytest.mark.parametrize("system_model", ["cutoff", "consequential"])
def test_reviewed_flows_survive_across_profiles(version, system_model, fixed_simapro_clock):
    data = [minimal_activity(extra_exchanges=[exchange(n, c, u) for n, c, u, _ in FLOWS])]
    source = deepcopy(data)
    inv = SimaProInventory.from_data(data, background_profile=BackgroundProfile("ecoinvent", version, system_model))
    result = inv.render()
    assert not result.has_errors, result.issues
    assert not any(i.code == "simapro_exchange_unused" for i in result.issues)
    for name, categories, _unit, target_unit in FLOWS:
        compartment = {
            "unspecified": "",
            "surface water": "river",
            "non-urban air or from high stacks": "low. pop.",
        }.get(categories[1], categories[1])
        rows = [r for r in result.rows if len(r) == 9 and r[:2] == [name, compartment]]
        assert rows
        for row in rows:
            assert row[2] == target_unit
            dist = SimaProCSVExtractor.create_distribution(*row[3:8])
            assert dist["amount"] == pytest.approx(-0.125)
            assert dist["scale"] == pytest.approx(0.01)
    assert data == source and inv.data == source
    assert result.rows == inv.render().rows


@pytest.mark.parametrize("name", ["Waste mass, total, placed in landfill", "Organic carbon, placed in landfill"])
@pytest.mark.parametrize("version", ["3.8", "3.12"])
def test_final_waste_csv_round_trip(name, version, tmp_path):
    profile = BackgroundProfile("ecoinvent", version, "cutoff")
    data = [minimal_activity(extra_exchanges=[exchange(name, ("inventory indicator", "waste"))])]
    before = deepcopy(data)
    inv = SimaProInventory.from_data(data, background_profile=profile)
    result = inv.render()
    assert not result.has_errors, result.issues
    row = result.rows[result.rows.index(["Final waste flows"]) + 1]
    assert row[:4] == [name, "", "kg", "-0.125"]
    path = tmp_path / "final-waste.csv"
    # Test format representation independently of release-specific catalog membership.
    inv.write_csv(path, validate=False)
    importer = bw2io.SimaProCSVImporter(path)
    importer.apply_strategies()
    raw = importer.data
    normalized = normalize_simapro_import_data(
        raw,
        background_profile=profile,
        database_name="test",
        biosphere_flows=[],
        biosphere_correspondence={},
        version_mapping={},
    )
    flow = next(e for e in normalized[0]["exchanges"] if e["type"] == "biosphere")
    assert flow["name"] == name and flow["categories"] == ("inventory indicator", "waste")
    assert flow["amount"] == pytest.approx(-0.125) and flow["scale"] == pytest.approx(0.01)
    again = SimaProInventory.from_data(normalized, background_profile=profile).render()
    assert not again.has_errors, again.issues
    assert next(r for r in again.rows if len(r) == 9 and r[0] == name)[:8] == row[:8]
    assert data == before
    deterministic = deepcopy(data)
    for key in ("uncertainty type", "loc", "scale"):
        deterministic[0]["exchanges"][1].pop(key)
    assert not _preflight(_document(data=deterministic)).has_errors


def test_unsupported_indicator_and_wrong_radionuclide_unit_are_explicit_errors():
    for exc in [
        exchange("Hazardous waste disposed", ("inventory indicator", "waste")),
        exchange("Radon-222", ("air", "unspecified"), "kilogram"),
    ]:
        result = SimaProInventory.from_data(
            [minimal_activity(extra_exchanges=[exc])],
            background_profile=BackgroundProfile("ecoinvent", "3.12", "cutoff"),
        ).render()
        assert result.has_errors and not result.rows
        assert any(i.code == "simapro_biosphere_unresolved" for i in result.issues)


def test_native_unknown_final_waste_is_preserved_but_not_inferred():
    raw = [
        minimal_activity(
            extra_exchanges=[
                dict(
                    type="technosphere",
                    name="Native custom indicator",
                    categories=("Final waste flows", "custom"),
                    unit="kilogram",
                    amount=2,
                )
            ]
        )
    ]
    # Use the native importer identity and metadata conventions.
    raw[0]["simapro metadata"] = {"Category type": "material"}
    raw[0]["exchanges"][0]["name"] = "Test product {GLO}| test process | Cut-off, U"
    raw[0]["exchanges"][0]["categories"] = ("Test",)
    normalized = normalize_simapro_import_data(
        raw,
        background_profile=BackgroundProfile("ecoinvent", "3.12", "cutoff"),
        database_name="test",
        biosphere_flows=[],
        biosphere_correspondence={},
        version_mapping={},
    )
    result = SimaProInventory.from_data(
        normalized, background_profile=BackgroundProfile("ecoinvent", "3.12", "cutoff")
    ).render()
    assert not result.has_errors, result.issues
    assert next(r for r in result.rows if len(r) == 9 and r[0] == "Native custom indicator")[:4] == [
        "Native custom indicator",
        "custom",
        "kg",
        "2",
    ]
