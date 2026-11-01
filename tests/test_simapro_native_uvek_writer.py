"""Native air wire tokens are syntax, never a change to flow identity."""

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


def test_ambiguous_native_wire_key_is_rejected(monkeypatch):
    from brightpath.profiles import biosphere_categories

    name = "Carbon dioxide, fossil"
    monkeypatch.setattr(
        biosphere_categories,
        "_native_identities",
        lambda: frozenset(
            {
                (name, ("emissions to air", "low. pop."), "kilogram"),
                (name, ("air", "low. pop."), "kilogram"),
            }
        ),
    )
    source = document()
    data = source.data
    data[0]["exchanges"][1].update(name=name, categories=("emissions to air", "low. pop."))
    result = render_simapro_rows(source.replace(data=data))
    assert result.has_errors and not result.rows


def test_native_unspecified_air_uses_existing_blank_token_without_new_merging():
    source = document()
    result = render_simapro_rows(source)
    assert not result.has_errors
    row = next(r for r in result.rows if r and r[0] == "Carbon dioxide, biogenic")
    assert row[1] == ""
