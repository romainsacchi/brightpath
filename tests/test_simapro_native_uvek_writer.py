"""Native air and long-term groundwater wire tokens preserve exact flow identities."""

import csv
from copy import deepcopy

import pytest
from test_simapro_native_uvek_reader import NATIVE, PROFILE, document

from brightpath import BackgroundProfile, BiosphereProfile
from brightpath.background import PackageCatalogProvider
from brightpath.formats.simapro_csv import load_simapro_csv, render_simapro_rows, write_simapro_csv


@pytest.mark.parametrize("name", ["Carbon dioxide, fossil", "Carbon dioxide, biogenic", "Sulfur dioxide"])
@pytest.mark.parametrize("compartment", ["high. pop.", "low. pop."])
@pytest.mark.parametrize("formula", [False, True])
def test_native_air_wire_token_roundtrip_preserves_exact_compartment_and_values(tmp_path, name, compartment, formula):
    source = document()
    data = source.data
    flow = data[0]["exchanges"][1]
    flow.update(name=name, categories=("emissions to air", compartment))
    if formula:
        for key in ("uncertainty type", "loc", "scale"):
            flow.pop(key)
    else:
        flow.pop("formula")
    source = source.replace(data=data)
    before = deepcopy(data)
    identity = (name, ("emissions to air", compartment), "kilogram")
    provider = PackageCatalogProvider()
    assert identity in provider.load_biosphere(NATIVE).identities
    for iteration in range(2):
        path, result = write_simapro_csv(source, tmp_path / f"native-air-{iteration}.csv")
        assert not result.has_errors
        rows = list(csv.reader(path.open(encoding="latin-1", newline=""), delimiter=";"))
        row = next(r for r in rows if r and r[0] == name)
        assert row[1:3] == [compartment, "kg"]
        restored = load_simapro_csv(
            path, background_profile=PROFILE, biosphere_profile=NATIVE, catalog_provider=provider
        )
        actual = next(e for e in restored.data[0]["exchanges"] if e["type"] == "biosphere")
        assert (actual["name"], tuple(actual["categories"]), actual["unit"]) == identity
        for key in ("amount", "formula", "uncertainty type", "loc", "scale", "comment"):
            if key in before[0]["exchanges"][1]:
                assert actual[key] == before[0]["exchanges"][1][key]
        assert source.data == before
    assert data == before


@pytest.mark.parametrize(
    "biosphere,background",
    [
        (BiosphereProfile("ecoinvent", "3.10"), PROFILE),
        (BiosphereProfile("ecoinvent", "3.10"), BackgroundProfile("uvek", "2025", "cutoff")),
        (BiosphereProfile("ecoinvent", "3.12"), BackgroundProfile("ecoinvent", "3.12", "cutoff")),
    ],
)
def test_native_token_passthrough_requires_explicit_native_biosphere(biosphere, background):
    source = document()
    data = source.data
    data[0]["exchanges"][1].update(name="Carbon dioxide, fossil", categories=("air", "low. pop."))
    source = source.replace(data=data, background_profile=background, biosphere_profile=biosphere)
    result = render_simapro_rows(source)
    assert result.has_errors
    assert any("subcompartment mapping" in issue.message for issue in result.issues)


@pytest.mark.parametrize(
    "name,compartment,unit",
    [
        ("Not a native flow", "low. pop.", "kilogram"),
        ("Carbon dioxide, fossil", "unreviewed air class", "kilogram"),
        ("Carbon dioxide, fossil", "low. pop.", "cubic meter"),
    ],
)
def test_unknown_native_key_or_token_is_not_accepted(name, compartment, unit):
    source = document()
    data = source.data
    data[0]["exchanges"][1].update(name=name, categories=("emissions to air", compartment), unit=unit)
    result = render_simapro_rows(source.replace(data=data))
    assert result.has_errors and not result.rows


@pytest.mark.parametrize(
    "root,name,token",
    [
        ("air", "Carbon dioxide, fossil", "low. pop."),
        ("water", "Copper", "groundwater, long-term"),
    ],
)
def test_ambiguous_native_wire_key_is_rejected(monkeypatch, root, name, token):
    from brightpath.profiles import biosphere_categories

    native_root = "emissions to " + root
    monkeypatch.setattr(
        biosphere_categories,
        "_native_identities",
        lambda: frozenset(
            {
                (name, (native_root, token), "kilogram"),
                (name, (root, token), "kilogram"),
            }
        ),
    )
    source = document()
    data = source.data
    data[0]["exchanges"][1].update(name=name, categories=(native_root, token))
    result = render_simapro_rows(source.replace(data=data))
    assert result.has_errors and not result.rows


def test_native_unspecified_air_uses_existing_blank_token_without_new_merging():
    source = document()
    result = render_simapro_rows(source)
    assert not result.has_errors
    row = next(r for r in result.rows if r and r[0] == "Carbon dioxide, biogenic")
    assert row[1] == ""


@pytest.mark.parametrize("amount", [-0.75, 0, 0.75])
@pytest.mark.parametrize("formula", [False, True])
def test_native_long_term_groundwater_roundtrip_keeps_exact_identity_and_signed_values(tmp_path, amount, formula):
    source = document()
    data = source.data
    data[0]["parameters"] = [{"name": "dose", "amount": amount}]
    flow = data[0]["exchanges"][1]
    flow.update(name="Copper", categories=("emissions to water", "groundwater, long-term"), amount=amount, loc=amount)
    if formula:
        for field in ("uncertainty type", "loc", "scale"):
            flow.pop(field)
    else:
        flow.pop("formula")
    source = source.replace(data=data)
    before = deepcopy(data)
    provider = PackageCatalogProvider()
    identity = ("Copper", ("emissions to water", "groundwater, long-term"), "kilogram")
    assert identity in provider.load_biosphere(NATIVE).identities
    path, report = write_simapro_csv(source, tmp_path / "native-groundwater.csv")
    assert not report.has_errors
    rows = list(csv.reader(path.open(encoding="latin-1", newline=""), delimiter=";"))
    row = next(r for r in rows if r and r[0] == "Copper")
    assert row[1:3] == ["groundwater, long-term", "kg"]
    restored = load_simapro_csv(path, background_profile=PROFILE, biosphere_profile=NATIVE, catalog_provider=provider)
    actual = next(e for e in restored.data[0]["exchanges"] if e["type"] == "biosphere")
    assert (actual["name"], tuple(actual["categories"]), actual["unit"]) == identity
    for field in ("amount", "formula", "uncertainty type", "loc", "scale", "comment"):
        if field in before[0]["exchanges"][1]:
            assert actual[field] == before[0]["exchanges"][1][field]
    assert source.data == before


@pytest.mark.parametrize(
    "name,compartment,unit",
    [
        ("Unknown copper identity", "groundwater, long-term", "kilogram"),
        ("Copper", "invented long-term water", "kilogram"),
        ("Copper", "groundwater, long-term", "cubic meter"),
    ],
)
def test_native_water_passthrough_requires_exact_flow_unit_and_compartment(name, compartment, unit):
    source = document()
    data = source.data
    data[0]["exchanges"][1].update(name=name, categories=("emissions to water", compartment), unit=unit)
    result = render_simapro_rows(source.replace(data=data))
    assert result.has_errors and not result.rows


@pytest.mark.parametrize("version", ["3.10", "3.12"])
def test_native_water_token_is_not_accepted_in_a_canonical_biosphere(version):
    source = document()
    data = source.data
    data[0]["exchanges"][1].update(name="Copper", categories=("water", "groundwater, long-term"))
    result = render_simapro_rows(source.replace(data=data, biosphere_profile=BiosphereProfile("ecoinvent", version)))
    assert result.has_errors and not result.rows


def test_every_native_long_term_groundwater_identity_keeps_its_name_dimension_and_signed_amount(tmp_path):
    provider = PackageCatalogProvider()
    identities = sorted(
        identity
        for identity in provider.load_biosphere(NATIVE).identities
        if tuple(identity[1]) == ("emissions to water", "groundwater, long-term")
    )
    assert len(identities) == 47
    assert {identity[2] for identity in identities} == {"kilogram", "megajoule"}
    source = document()
    data = source.data
    data[0].pop("parameters")
    data[0]["exchanges"] = data[0]["exchanges"][:1]
    for index, (name, categories, unit) in enumerate(identities):
        amount = -0.125 if index % 2 else 0.125
        data[0]["exchanges"].append(
            {
                "name": name,
                "categories": categories,
                "unit": unit,
                "type": "biosphere",
                "amount": amount,
                "uncertainty type": 3,
                "loc": amount,
                "scale": 0.03125,
            }
        )
    source = source.replace(data=data)
    before = deepcopy(data)
    path, report = write_simapro_csv(source, tmp_path / "all-native-long-term.csv")
    assert not report.has_errors
    restored = load_simapro_csv(path, background_profile=PROFILE, biosphere_profile=NATIVE, catalog_provider=provider)

    def values(exchange):
        return (
            exchange["name"],
            tuple(exchange["categories"]),
            exchange["unit"],
            exchange["amount"],
            exchange["uncertainty type"],
            exchange["loc"],
            exchange["scale"],
        )

    assert sorted(values(e) for e in restored.data[0]["exchanges"] if e["type"] == "biosphere") == sorted(
        values(e) for e in before[0]["exchanges"] if e["type"] == "biosphere"
    )
    assert restored.context.background == source.context.background
    assert source.data == before
