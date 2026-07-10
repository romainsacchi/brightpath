import pytest

import brightpath.migrations as migrations
from brightpath import BackgroundProfile, BrightwayInventory, InventoryFormat, SimaProInventory
from brightpath.core import (
    BackgroundContext,
    BiosphereProfile,
    FormatProfile,
    InventoryContext,
    TechnosphereProfile,
)
from brightpath.formats.brightway_delimited import load_brightway_delimited, write_brightway_delimited
from brightpath.formats.brightway_excel import load_brightway_excel, write_brightway_excel
from brightpath.migrations import engine as migration_engine
from brightpath.models import InventoryDocument


def context(format_id: str, *, technosphere_version: str = "3.10", biosphere_version: str = "3.10"):
    return InventoryContext(
        format=FormatProfile(format_id),
        background=BackgroundContext(
            technosphere=TechnosphereProfile("ecoinvent", technosphere_version, "cutoff"),
            biosphere=BiosphereProfile("ecoinvent", biosphere_version),
        ),
    )


def inventory_data():
    return [
        {
            "name": "foreground service",
            "reference product": "service",
            "location": "GLO",
            "unit": "unit",
            "exchanges": [
                {
                    "name": "foreground service",
                    "reference product": "service",
                    "location": "GLO",
                    "unit": "unit",
                    "amount": 1.0,
                    "type": "production",
                }
            ],
        }
    ]


def document(format_id: str) -> InventoryDocument:
    return InventoryDocument(data=inventory_data(), context=context(format_id))


def test_legacy_migration_engine_is_not_exported_from_public_package():
    assert not hasattr(migrations, "migrate_inventory")
    assert callable(migration_engine.migrate_inventory)


@pytest.mark.parametrize(
    ("facade", "wrong_format", "expected_format"),
    [
        (BrightwayInventory, "brightway_csv", "brightway_excel"),
        (BrightwayInventory, "simapro_csv", "brightway_excel"),
        (SimaProInventory, "brightway_excel", "simapro_csv"),
    ],
)
def test_facades_reject_documents_and_from_data_contexts_for_other_formats(facade, wrong_format, expected_format):
    wrong_context = context(wrong_format)

    with pytest.raises(ValueError, match=expected_format):
        facade(InventoryDocument(data=inventory_data(), context=wrong_context))
    with pytest.raises(ValueError, match=expected_format):
        facade.from_data(inventory_data(), context=wrong_context)


@pytest.mark.parametrize("facade", [BrightwayInventory, SimaProInventory])
def test_facade_constructor_requires_an_inventory_document(facade):
    with pytest.raises(TypeError, match="InventoryDocument"):
        facade(object())


def test_inventory_format_preserves_known_enums_and_custom_identifiers():
    known = document("brightway_csv")
    custom = document("vendor_json")

    assert known.inventory_format is InventoryFormat.BRIGHTWAY_CSV
    assert custom.inventory_format == "vendor_json"
    assert type(custom.inventory_format) is str


@pytest.mark.parametrize(
    ("legacy_kwargs", "message"),
    [
        ({"background_profile": BackgroundProfile("ecoinvent", "3.9", "cutoff")}, "context.technosphere"),
        ({"biosphere_profile": BiosphereProfile("ecoinvent", "3.9")}, "context.biosphere"),
    ],
)
def test_brightway_excel_reader_rejects_legacy_profiles_that_conflict_with_context(tmp_path, legacy_kwargs, message):
    source_context = context("brightway_excel")
    path = write_brightway_excel(InventoryDocument(data=inventory_data(), context=source_context), tmp_path / "source")

    with pytest.raises(ValueError, match=message):
        load_brightway_excel(path, context=source_context, **legacy_kwargs)


def test_brightway_excel_reader_accepts_matching_legacy_profiles_with_context(tmp_path):
    source_context = context("brightway_excel")
    path = write_brightway_excel(InventoryDocument(data=inventory_data(), context=source_context), tmp_path / "source")

    loaded = load_brightway_excel(
        path,
        context=source_context,
        background_profile=BackgroundProfile("ecoinvent", "3.10", "cutoff"),
        biosphere_profile=BiosphereProfile("ecoinvent", "3.10"),
    )

    assert loaded.context == source_context


@pytest.mark.parametrize(
    ("format_id", "suffix"),
    [("brightway_csv", ".csv"), ("brightway_tsv", ".tsv")],
)
@pytest.mark.parametrize(
    ("legacy_kwargs", "message"),
    [
        ({"background_profile": BackgroundProfile("ecoinvent", "3.9", "cutoff")}, "context.technosphere"),
        ({"biosphere_profile": BiosphereProfile("ecoinvent", "3.9")}, "context.biosphere"),
    ],
)
def test_brightway_delimited_reader_rejects_legacy_profiles_that_conflict_with_context(
    tmp_path, format_id, suffix, legacy_kwargs, message
):
    source_context = context(format_id)
    path = write_brightway_delimited(
        InventoryDocument(data=inventory_data(), context=source_context),
        tmp_path / f"source{suffix}",
    )

    with pytest.raises(ValueError, match=message):
        load_brightway_delimited(path, context=source_context, **legacy_kwargs)
