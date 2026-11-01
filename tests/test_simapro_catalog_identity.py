"""Reader regressions using synthetic inventories and exact catalog metadata."""

from copy import deepcopy

import pytest

from brightpath.background import BiosphereCatalog, InMemoryCatalogProvider, TechnosphereCatalog
from brightpath.background.catalogs import PackageCatalogProvider
from brightpath.core import BiosphereProfile
from brightpath.exceptions import SimaProSerializationError
from brightpath.formats.simapro_csv import (
    load_simapro_csv,
    normalize_simapro_import_data,
    render_simapro_rows,
    write_simapro_csv,
)
from brightpath.models import BackgroundProfile, InventoryDocument, InventoryFormat

FIELDS = ("name", "reference product", "location", "unit")


def document(profile, exchange):
    activity = {
        "name": "Synthetic identity fixture",
        "reference product": "synthetic output",
        "location": "GLO",
        "unit": "kilogram",
        "comment": "Synthetic reader regression; no background inventory contents.",
    }
    return InventoryDocument(
        data=[
            {
                **activity,
                "exchanges": [
                    {**activity, "type": "production", "amount": 1, "simapro category": "Materials/Other"},
                    {**exchange, "amount": 2.5, "comment": "Synthetic quantity"},
                ],
            }
        ],
        background_profile=profile,
        inventory_format=InventoryFormat.SIMAPRO_CSV,
    )


@pytest.mark.parametrize(
    "version,name,categories,unit,flow_id",
    [
        ("3.9.1", "Sodium", ("water",), "kilogram", "723f56f9-c68e-5692-a969-0fb2a70bce20"),
        ("3.9.1", "Sodium I", ("water",), "kilogram", "5ac961f0-8e53-42ff-b13f-1a7fb9133868"),
        (
            "3.7",
            "Gas, natural, in ground",
            ("natural resource", "in ground"),
            "cubic meter",
            "7c337428-fb1b-45c7-bbb2-2ee4d29e17ba",
        ),
        (
            "3.12",
            "Gas, natural",
            ("natural resource", "in ground"),
            "standard cubic meter",
            "7c337428-fb1b-45c7-bbb2-2ee4d29e17ba",
        ),
    ],
)
def test_csv_reader_preserves_exact_catalog_biosphere_identity(tmp_path, version, name, categories, unit, flow_id):
    profile = BackgroundProfile("ecoinvent", version, "cutoff")
    provider = PackageCatalogProvider()
    source = document(profile, {"type": "biosphere", "name": name, "categories": categories, "unit": unit})
    before = deepcopy(source.data)
    destination = tmp_path / "synthetic.csv"
    if version == "3.9.1" and name == "Sodium":
        catalog = provider.load_biosphere(BiosphereProfile("ecoinvent", version))
        assert catalog.references[(name, categories, unit)] == flow_id
        rendered = render_simapro_rows(source)
        assert not rendered.rows
        issue = next(i for i in rendered.issues if i.code == "simapro_biosphere_identity_ambiguous")
        assert flow_id in issue.message
        assert "5ac961f0-8e53-42ff-b13f-1a7fb9133868" in issue.message
        with pytest.raises(SimaProSerializationError, match="cannot preserve the distinction"):
            write_simapro_csv(source, destination)
        assert not destination.exists()
        assert source.data == before
        return
    path, _ = write_simapro_csv(source, destination)
    for _ in range(2):
        restored = load_simapro_csv(path, background_profile=profile, catalog_provider=provider)
        exchange = restored.data[0]["exchanges"][1]
        assert (exchange["name"], tuple(exchange["categories"]), exchange["unit"]) == (name, categories, unit)
        catalog = provider.load_biosphere(BiosphereProfile("ecoinvent", version))
        assert catalog.references[(name, categories, unit)] == flow_id
        assert exchange["amount"] == 2.5
        assert exchange["comment"] == "Synthetic quantity"
        write_simapro_csv(restored, path)
    assert source.data == before


def test_csv_reader_restores_mixed_case_product_only_from_exact_catalog(tmp_path):
    provider = PackageCatalogProvider()
    profile = BackgroundProfile("ecoinvent", "3.12", "cutoff")
    supplier = {
        "type": "technosphere",
        "name": "market for NOx retained, by selective catalytic reduction",
        "reference product": "NOx retained, by selective catalytic reduction",
        "location": "GLO",
        "unit": "kilogram",
    }
    source = document(profile, supplier)
    path, _ = write_simapro_csv(source, tmp_path / "synthetic.csv")
    restored = load_simapro_csv(path, background_profile=profile, catalog_provider=provider)
    exchange = restored.data[0]["exchanges"][1]
    expected = tuple(supplier[field] for field in FIELDS)
    assert tuple(exchange[field] for field in FIELDS) == expected
    assert exchange["product"] == supplier["reference product"]
    assert provider.load_technosphere(profile.to_technosphere_profile()).references[expected] == (
        "e3878b9d-8803-5663-8b20-ee557afe295f",
        "c9641dad-17a0-4d81-a9ee-85890945c39d",
    )


def normalize(exchange, biosphere, technosphere=()):
    profile = BackgroundProfile("ecoinvent", "3.12", "cutoff")
    raw = document(profile, exchange).data
    before = deepcopy(raw)
    provider = InMemoryCatalogProvider(
        biosphere=[BiosphereCatalog(BiosphereProfile("ecoinvent", "3.12"), biosphere)],
        technosphere=[TechnosphereCatalog(profile.to_technosphere_profile(), technosphere)],
    )
    result = normalize_simapro_import_data(
        raw,
        background_profile=profile,
        database_name="synthetic",
        catalog_provider=provider,
        biosphere_correspondence={},
        version_mapping={},
    )
    assert raw == before
    return result[0]["exchanges"][1]


@pytest.mark.parametrize("version", ["3.6", "3.7", "3.8"])
def test_sodium_wire_collision_does_not_imply_native_uuid_equivalence(tmp_path, version):
    profile = BackgroundProfile("ecoinvent", version, "cutoff")
    catalog = PackageCatalogProvider().load_biosphere(BiosphereProfile("ecoinvent", version))
    ion = "Sodium, ion" if version in {"3.6", "3.7", "3.8"} else "Sodium I"
    assert catalog.references[("Sodium", ("water",), "kilogram")] == "723f56f9-c68e-5692-a969-0fb2a70bce20"
    assert catalog.references[(ion, ("water",), "kilogram")] == "5ac961f0-8e53-42ff-b13f-1a7fb9133868"
    generic = document(profile, {"type": "biosphere", "name": "Sodium", "categories": ("water",), "unit": "kilogram"})
    before = deepcopy(generic.data)
    destination = tmp_path / "generic.csv"
    rendered = render_simapro_rows(generic)
    assert not rendered.rows
    issue = next(i for i in rendered.issues if i.code == "simapro_biosphere_identity_ambiguous")
    assert "723f56f9-c68e-5692-a969-0fb2a70bce20" in issue.message
    assert "5ac961f0-8e53-42ff-b13f-1a7fb9133868" in issue.message
    with pytest.raises(SimaProSerializationError, match="cannot preserve the distinction"):
        write_simapro_csv(generic, destination)
    assert not destination.exists()
    assert generic.data == before

    ionic = document(profile, {"type": "biosphere", "name": ion, "categories": ("water",), "unit": "kilogram"})
    ionic_before = deepcopy(ionic.data)
    path, result = write_simapro_csv(ionic, tmp_path / "ionic.csv")
    assert not result.has_errors
    literal = next(row for row in path.read_text(encoding="latin-1").splitlines() if row.startswith("Sodium;"))
    assert literal.split(";")[:3] == ["Sodium", "", "kg"]
    restored = load_simapro_csv(path, background_profile=profile, catalog_provider=PackageCatalogProvider())
    exchange = next(e for e in restored.data[0]["exchanges"] if e["type"] == "biosphere")
    identity = exchange["name"], tuple(exchange["categories"]), exchange["unit"]
    assert identity == (ion, ("water",), "kilogram")
    assert catalog.references[identity] == "5ac961f0-8e53-42ff-b13f-1a7fb9133868"
    assert exchange["amount"] == 2.5
    assert ionic.data == ionic_before


def test_biosphere_unit_restoration_preserves_existing_name_aliases():
    exchange = normalize(
        {
            "type": "biosphere",
            "name": "Transformation, from unspecified",
            "simapro name": "Transformation, from unknown",
            "categories": ("natural resource", "land"),
            "unit": "square meter",
        },
        {
            (name, ("natural resource", "land"), "square meter")
            for name in ("Transformation, from unspecified", "Transformation, from unknown")
        },
    )
    assert exchange["name"] == "Transformation, from unspecified"


def test_unique_catalog_match_preserves_explicit_resource_compartment():
    name = "Water, unspecified natural origin"
    exchange = normalize(
        {
            "type": "biosphere",
            "name": name,
            "simapro name": name + "/m3",
            "categories": ("natural resource", "in ground"),
            "unit": "cubic meter",
        },
        {(name, ("natural resource", subcategory), "cubic meter") for subcategory in ("in ground", "in water")},
    )
    assert exchange["categories"] == ("natural resource", "in ground")
    assert exchange["amount"] == 2.5


def test_biosphere_unit_recovery_requires_unique_name_compartment_and_wire_unit():
    identities = {("Gas, natural", ("natural resource", "in ground"), "standard cubic meter")}
    raw = {"type": "biosphere", "name": "Gas, natural", "categories": ("natural resource", "in ground")}
    assert normalize({**raw, "unit": "cubic meter"}, identities)["unit"] == "standard cubic meter"
    assert normalize({**raw, "unit": "kilogram"}, identities)["unit"] == "kilogram"
    assert normalize({**raw, "unit": "cubic meter", "categories": ("air",)}, identities)["unit"] == "cubic meter"
    identities.add(("Gas, natural", ("natural resource", "in ground"), "cubic meter"))
    with pytest.raises(ValueError, match="Ambiguous exact-catalog SimaPro biosphere unit"):
        normalize({**raw, "unit": "cubic meter"}, identities)


def test_reader_rejects_technosphere_wire_collision_instead_of_selecting_by_case():
    raw = {"type": "technosphere", "name": "NOx {GLO}| market for NOx | Cut-off, U", "unit": "kilogram"}
    identities = {("market for NOx", product, "GLO", "kilogram") for product in ("NOx", "nOx")}
    with pytest.raises(ValueError, match="Ambiguous exact-catalog SimaPro technosphere identity"):
        normalize(raw, (), identities)


def test_absent_technosphere_catalog_identity_does_not_invent_capitalization():
    raw = {"type": "technosphere", "name": "NOx {GLO}| market for NOx | Cut-off, U", "unit": "kilogram"}
    exchange = normalize(raw, ())
    assert exchange["reference product"] == "nOx"
