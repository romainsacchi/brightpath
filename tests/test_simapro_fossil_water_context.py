"""Synthetic context regressions; canonical IDs are not vendor-library evidence."""

from copy import deepcopy

import pytest

from brightpath.background.catalogs import BiosphereCatalog, InMemoryCatalogProvider, PackageCatalogProvider
from brightpath.core.context import BiosphereProfile
from brightpath.formats.simapro_csv import load_simapro_csv, normalize_simapro_import_data, write_simapro_csv
from brightpath.models import BackgroundProfile, InventoryDocument, InventoryFormat

CANONICAL = ("Water", ("water", "fossil well"), "cubic meter")
WIRE = ("Water", ("water", "fossilwater"), "cubic meter")
FOSSIL_ID = "2256a142-8242-4b4f-b9aa-a167803989ca"
BLANK_ID = "2404b41a-2eed-4e9d-8ab6-783946fdf5d6"
VERSIONS = ("3.6", "3.7", "3.8", "3.9", "3.9.1", "3.10", "3.10.1", "3.11", "3.12")


@pytest.fixture(scope="module")
def provider():
    return PackageCatalogProvider()


def document(version, exchange):
    activity = {
        "name": "Synthetic fossil water context",
        "reference product": "synthetic output",
        "location": "GLO",
        "unit": "kilogram",
        "comment": "Synthetic only",
    }
    return InventoryDocument(
        data=[
            {
                **activity,
                "exchanges": [
                    {**activity, "type": "production", "amount": 1.0, "simapro category": "Materials/Other"},
                    exchange,
                ],
            }
        ],
        background_profile=BackgroundProfile("ecoinvent", version, "cutoff"),
        inventory_format=InventoryFormat.BRIGHTWAY_EXCEL,
    )


def fossil(**kw):
    return {
        "name": "Water",
        "type": "biosphere",
        "categories": ("water", "fossil well"),
        "unit": "cubic meter",
        "amount": 0.125,
        **kw,
    }


@pytest.mark.parametrize("version", VERSIONS)
def test_exact_witness_and_actual_csv_roundtrip_across_declared_catalogs(tmp_path, version, provider):
    catalog = provider.load_biosphere(BiosphereProfile("ecoinvent", version))
    assert catalog.references[CANONICAL] == FOSSIL_ID
    assert catalog.references[("Water", ("water",), "cubic meter")] == BLANK_ID
    source = document(version, fossil())
    before = source.data
    path, _ = write_simapro_csv(source, tmp_path / "fossil.csv")
    assert "Water;fossilwater;kg;125;" in path.read_text(encoding="latin-1")
    for _ in range(2):
        result = load_simapro_csv(path, background_profile=source.background_profile, catalog_provider=provider)
        exc = result.data[0]["exchanges"][1]
        assert (exc["name"], tuple(exc["categories"]), exc["unit"]) == CANONICAL
        assert exc["amount"] == 0.125
        assert catalog.references[(exc["name"], tuple(exc["categories"]), exc["unit"])] == FOSSIL_ID
        write_simapro_csv(result, path)
    assert source.data == before


@pytest.mark.parametrize(
    "amount,kind,metadata",
    [
        (0.125, 2, {"loc": -2.0794415416798357, "scale": 0.25}),
        (-0.125, 2, {"loc": -2.0794415416798357, "scale": 0.25, "negative": True}),
        (0.125, 5, {"loc": 0.125, "minimum": 0.1, "maximum": 0.15}),
    ],
)
def test_existing_water_quantity_uncertainty_and_formula_convention_unchanged(
    tmp_path, provider, amount, kind, metadata
):
    expression = "1/8" if amount > 0 else "-1/8"
    source = document("3.12", fossil(amount=amount, **{"uncertainty type": kind, "formula": expression}, **metadata))
    before = source.data
    path, _ = write_simapro_csv(source, tmp_path / "formula.csv")
    result = load_simapro_csv(path, background_profile=source.background_profile, catalog_provider=provider)
    exc = result.data[0]["exchanges"][1]
    assert tuple(exc["categories"]) == CANONICAL[1]
    assert exc["amount"] == pytest.approx(amount)
    assert exc["unit"] == "cubic meter"
    assert exc["uncertainty type"] == kind
    for key, value in metadata.items():
        assert exc[key] == pytest.approx(value)
    assert exc.get("formula")
    assert source.data == before


@pytest.mark.parametrize(
    "categories,token,expected_id",
    [(("water",), "", BLANK_ID), (("water", "ground-"), "groundwater", "51254820-3456-4373-b7b4-056cf7b16e01")],
)
def test_other_water_emission_contexts_stay_distinct(tmp_path, provider, categories, token, expected_id):
    source = document("3.12", fossil(categories=categories))
    path, _ = write_simapro_csv(source, tmp_path / "control.csv")
    assert f"Water;{token};kg;125;" in path.read_text(encoding="latin-1")
    result = load_simapro_csv(path, background_profile=source.background_profile, catalog_provider=provider)
    exc = result.data[0]["exchanges"][1]
    assert tuple(exc["categories"]) == categories
    assert (
        provider.load_biosphere(BiosphereProfile("ecoinvent", "3.12")).references[
            (exc["name"], tuple(exc["categories"]), exc["unit"])
        ]
        == expected_id
    )


def test_resource_fossil_well_still_has_blank_subcompartment(tmp_path):
    source = document(
        "3.12", fossil(name="Water, unspecified natural origin", categories=("natural resource", "fossil well"))
    )
    path, _ = write_simapro_csv(source, tmp_path / "resource.csv")
    body = path.read_text(encoding="latin-1").split("Resources\n", 1)[1]
    row = body.splitlines()[0].split(";")
    assert row[1] == ""
    assert row[2] == "m3"
    assert float(row[3]) == 0.125


def normalize(exchange, identities, references, family="ecoinvent"):
    profile = BiosphereProfile(family, "3.12")
    catalog = BiosphereCatalog(profile, identities, references=references)
    source = document("3.12", exchange).data
    before = deepcopy(source)
    result = normalize_simapro_import_data(
        source,
        background_profile=BackgroundProfile("ecoinvent", "3.12", "cutoff"),
        biosphere_profile=profile,
        database_name="synthetic",
        catalog_provider=InMemoryCatalogProvider(biosphere=[catalog]),
        biosphere_correspondence={},
        version_mapping={},
    )
    assert source == before
    return result[0]["exchanges"][1]


@pytest.mark.parametrize(
    "identities,references",
    [
        ([], {}),
        ([CANONICAL], {}),
        ([CANONICAL], {CANONICAL: BLANK_ID}),
        ([CANONICAL, ("Water", CANONICAL[1], "m3")], {CANONICAL: FOSSIL_ID, ("Water", CANONICAL[1], "m3"): FOSSIL_ID}),
    ],
)
def test_reader_refuses_missing_unverified_and_ambiguous_catalog_witness(identities, references):
    with pytest.raises(ValueError, match="fossil-water"):
        normalize(fossil(categories=WIRE[1]), identities, references)


@pytest.mark.parametrize(
    "changes",
    [
        {"unit": "kilogram"},
        {"name": "Other"},
        {"flow_id": BLANK_ID},
        {"code": BLANK_ID},
        {"input": ("biosphere", BLANK_ID)},
    ],
)
def test_reader_no_unit_density_or_recorded_uuid_fallback(changes):
    with pytest.raises(ValueError, match="fossil-water"):
        normalize(fossil(categories=WIRE[1], **changes), [CANONICAL], {CANONICAL: FOSSIL_ID})


def test_reader_preserves_already_valid_distinct_native_wire_identity():
    result = normalize(fossil(categories=WIRE[1]), [CANONICAL, WIRE], {CANONICAL: FOSSIL_ID, WIRE: BLANK_ID})
    assert (result["name"], tuple(result["categories"]), result["unit"]) == WIRE


def test_reader_does_not_apply_ecoinvent_context_inverse_to_unreviewed_uvek_catalog():
    with pytest.raises(ValueError, match="ecoinvent biosphere catalog"):
        normalize(fossil(categories=WIRE[1]), [CANONICAL], {CANONICAL: FOSSIL_ID}, family="uvek")


def test_reader_requires_uuid_metadata_not_just_name_category_set():
    source = document("3.12", fossil(categories=WIRE[1])).data
    with pytest.raises(ValueError, match="exact declared ecoinvent biosphere catalog"):
        normalize_simapro_import_data(
            source,
            background_profile=BackgroundProfile("ecoinvent", "3.12", "cutoff"),
            database_name="synthetic",
            biosphere_flows={("Water", "water", "fossil well")},
            biosphere_correspondence={},
            version_mapping={},
        )


def test_writer_refuses_unverified_catalog_reference(tmp_path, monkeypatch):
    import brightpath.formats.simapro_csv as module

    catalog = BiosphereCatalog(BiosphereProfile("ecoinvent", "3.12"), [CANONICAL], references={CANONICAL: BLANK_ID})

    class WrongCatalog:
        def load_biosphere(self, profile):
            assert profile == catalog.profile
            return catalog

    monkeypatch.setattr(module, "PackageCatalogProvider", WrongCatalog)
    with pytest.raises(ValueError, match="unverified exact fossil-water"):
        write_simapro_csv(document("3.12", fossil()), tmp_path / "unverified.csv")


def test_writer_uses_independent_declared_biosphere_axis(tmp_path, provider):
    original = document("3.12", fossil())
    mixed = InventoryDocument(
        data=original.data,
        background_profile=original.background_profile,
        biosphere_profile=BiosphereProfile("ecoinvent", "3.6"),
        inventory_format=InventoryFormat.BRIGHTWAY_EXCEL,
    )
    path, _ = write_simapro_csv(mixed, tmp_path / "paired.csv")
    assert "Water;fossilwater;kg;125;" in path.read_text(encoding="latin-1")
    result = load_simapro_csv(
        path,
        background_profile=original.background_profile,
        biosphere_profile=BiosphereProfile("ecoinvent", "3.6"),
        catalog_provider=provider,
    )
    assert tuple(result.data[0]["exchanges"][1]["categories"]) == CANONICAL[1]
    assert result.context.background.biosphere == BiosphereProfile("ecoinvent", "3.6")


def test_public94_exact_water_row_agrees_with_existing_csv_mass_convention(tmp_path, provider):
    # Specific exact (=) row, not the generic README's approximate (~) context table.
    # Pinned public primary CSV: brightway-lca/simapro_ecoinvent_elementary_flows,
    # f81c05f0aa6724dd3db07a59cad155b8d2d0f7c1,
    # Mapping/Output/Mapped_files/SimaProv94-ecoinventEFv3.7.csv.
    # Chris Mutel automated match,2023-01-09; no independent verifier is recorded.
    import csv

    mapping = {
        "SourceListName": "SimaPro9.4",
        "SourceFlowName": "Water",
        "SourceFlowUUID": "E0F21491-61BA-44CF-B0A1-0A3670B63F8D",
        "SourceFlowContext": "Emissions to water/fossilwater",
        "SourceUnit": "kg",
        "MatchCondition": "=",
        "ConversionFactor": "0.001",
        "TargetListName": "ecoinventEFv3.7",
        "TargetFlowUUID": "2256a142-8242-4b4f-b9aa-a167803989ca",
        "TargetFlowContext": "water/fossil well",
        "TargetUnit": "m3",
    }
    source = document("3.7", fossil())
    path, _ = write_simapro_csv(source, tmp_path / "public94-correspondence.csv")
    with path.open(encoding="latin-1", newline="") as stream:
        rows = list(csv.reader(stream, delimiter=";"))
    emitted = next(row for row in rows if row[:2] == ["Water", "fossilwater"])
    assert emitted[0] == mapping["SourceFlowName"]
    assert emitted[2] == mapping["SourceUnit"]
    assert float(emitted[3]) * float(mapping["ConversionFactor"]) == 0.125
    catalog = provider.load_biosphere(BiosphereProfile("ecoinvent", "3.7"))
    assert catalog.references[CANONICAL] == mapping["TargetFlowUUID"]
    result = load_simapro_csv(path, background_profile=source.background_profile, catalog_provider=provider)
    exchange = result.data[0]["exchanges"][1]
    assert exchange["amount"] == 0.125
    assert tuple(exchange["categories"]) == tuple(mapping["TargetFlowContext"].split("/"))


def test_uvek2025_technosphere_with_declared_ei310_biosphere_preserves_fossil_water(tmp_path, provider):
    # CLIC's canonical UVEK2025 pairing uses EI3.10 elementary flows. The
    # format-context witness belongs to that biosphere, not the tech family.
    background = BackgroundProfile("uvek", "2025", "cutoff")
    biosphere = BiosphereProfile("ecoinvent", "3.10")
    source = InventoryDocument(
        data=document("3.10", fossil()).data,
        background_profile=background,
        biosphere_profile=biosphere,
        inventory_format=InventoryFormat.BRIGHTWAY_EXCEL,
    )
    before = source.data
    path, _ = write_simapro_csv(source, tmp_path / "uvek2025-ei310.csv")
    assert "Water;fossilwater;kg;125;" in path.read_text(encoding="latin-1")
    result = load_simapro_csv(
        path,
        background_profile=background,
        biosphere_profile=biosphere,
        catalog_provider=provider,
    )
    exchange = result.data[0]["exchanges"][1]
    assert (exchange["name"], tuple(exchange["categories"]), exchange["unit"]) == CANONICAL
    assert exchange["amount"] == 0.125
    assert provider.load_biosphere(biosphere).references[CANONICAL] == FOSSIL_ID
    assert result.context.background.technosphere == background.to_technosphere_profile()
    assert result.context.background.biosphere == biosphere
    assert source.data == before
