from copy import deepcopy

import pytest

from brightpath import BrightwayInventory, SimaProInventory
from brightpath.background import (
    BiosphereCatalog,
    InMemoryCatalogProvider,
    TechnosphereCatalog,
)
from brightpath.core.context import (
    BackgroundContext,
    BiosphereProfile,
    FormatProfile,
    InventoryContext,
    TechnosphereProfile,
)
from brightpath.core.policies import MigrationPolicy
from brightpath.core.reports import OperationKind, OperationReport
from brightpath.exceptions import MigrationError
from brightpath.migrations.engine import _canonical_unit
from brightpath.migrations.resources import load_biosphere_resources, load_technosphere_resources
from brightpath.models import BackgroundProfile, InventoryFormat


@pytest.fixture(params=[BrightwayInventory, SimaProInventory], ids=["brightway", "simapro"])
def facade_type(request):
    return request.param


def background(technosphere_version: str, biosphere_version: str | None = None) -> BackgroundContext:
    return BackgroundContext(
        technosphere=TechnosphereProfile("ecoinvent", technosphere_version, "cutoff"),
        biosphere=BiosphereProfile("ecoinvent", biosphere_version or technosphere_version),
    )


def format_profile(facade_type) -> FormatProfile:
    format_id = "brightway_excel" if facade_type is BrightwayInventory else "simapro_csv"
    return FormatProfile(format_id)


def inventory(facade_type, context: BackgroundContext, *exchanges):
    return facade_type.from_data(
        [
            {
                "name": "foreground process",
                "reference product": "foreground product",
                "location": "CH",
                "unit": "unit",
                "exchanges": list(exchanges),
            }
        ],
        context=InventoryContext(format=format_profile(facade_type), background=context),
    )


def technosphere_exchange(identity: tuple[str, str, str, str]) -> dict:
    name, product, location, unit = identity
    return {
        "name": name,
        "reference product": product,
        "location": location,
        "unit": unit,
        "amount": 2.0,
        "type": "technosphere",
    }


def biosphere_exchange(specification: dict) -> dict:
    return {
        "name": specification["name"],
        "categories": tuple(specification["categories"]),
        "unit": specification["unit"],
        "amount": 1.0,
        "type": "biosphere",
    }


def technosphere_identity(specification: dict) -> tuple[str, str, str, str]:
    return (
        specification["name"],
        specification["reference product"],
        specification["location"],
        specification["unit"],
    )


def safe_replacement(source_version: str = "3.10", target_version: str = "3.11") -> dict:
    resource = load_technosphere_resources("cutoff")[(source_version, target_version)]
    return next(
        rule
        for rule in resource["replace"]
        if _canonical_unit(rule["source"].get("unit")) == _canonical_unit(rule["target"].get("unit"))
    )


def technosphere_provider(
    source: BackgroundContext,
    target: BackgroundContext,
    source_identity: tuple[str, str, str, str],
    target_identity: tuple[str, str, str, str],
) -> InMemoryCatalogProvider:
    return InMemoryCatalogProvider(
        technosphere=[
            TechnosphereCatalog(source.technosphere, {source_identity}),
            TechnosphereCatalog(target.technosphere, {target_identity}),
        ]
    )


def test_forward_migration_commits_report_and_preserves_facade_format(facade_type):
    source_context = background("3.10", "3.10")
    target_context = background("3.11", "3.10")
    rule = safe_replacement()
    source_identity = technosphere_identity(rule["source"])
    target_identity = technosphere_identity(rule["target"])
    source = inventory(facade_type, source_context, technosphere_exchange(source_identity))
    original_data = deepcopy(source.data)
    provider = technosphere_provider(source_context, target_context, source_identity, target_identity)

    migrated = source.migrate_background(
        target_context,
        catalog_provider=provider,
    )
    repeated = migrated.migrate_background(target_context, catalog_provider=provider)

    assert isinstance(migrated, facade_type)
    assert migrated.context.background == target_context
    assert migrated.context.format == source.context.format
    assert migrated.inventory_format is source.inventory_format
    assert migrated.data[0]["exchanges"][0]["name"] == target_identity[0]
    assert source.data == original_data
    assert source.context.background == source_context
    assert source.migration_reports == ()
    assert len(migrated.migration_reports) == 1
    assert isinstance(migrated.last_migration_report, OperationReport)
    assert migrated.last_migration_report.operation is OperationKind.MIGRATE
    assert migrated.last_migration_report.metadata["committed"] is True
    assert len(repeated.migration_reports) == 2
    assert repeated.migration_reports[0] is migrated.last_migration_report


def test_strict_invalid_target_raises_with_rollback_report(facade_type):
    source_context = background("3.10", "3.10")
    target_context = background("3.11", "3.10")
    rule = safe_replacement()
    source_identity = technosphere_identity(rule["source"])
    source = inventory(facade_type, source_context, technosphere_exchange(source_identity))
    original_data = deepcopy(source.data)
    provider = InMemoryCatalogProvider(
        technosphere=[
            TechnosphereCatalog(source_context.technosphere, {source_identity}),
            TechnosphereCatalog(target_context.technosphere, set()),
        ]
    )

    with pytest.raises(MigrationError) as raised:
        source.migrate_background(target_context, catalog_provider=provider)

    assert raised.value.report is not None
    assert raised.value.report.has_errors
    assert raised.value.report.metadata["committed"] is False
    assert source.data == original_data
    assert source.context.background == source_context
    assert source.migration_reports == ()


def test_reverse_migration_requires_permissive_policy(facade_type):
    source_context = background("3.11", "3.10")
    target_context = background("3.10", "3.10")
    rule = safe_replacement()
    source_identity = technosphere_identity(rule["target"])
    target_identity = technosphere_identity(rule["source"])
    source = inventory(facade_type, source_context, technosphere_exchange(source_identity))
    provider = technosphere_provider(source_context, target_context, source_identity, target_identity)

    with pytest.raises(MigrationError) as strict_error:
        source.migrate_background(target_context, catalog_provider=provider)

    migrated = source.migrate_background(
        target_context.technosphere,
        biosphere_profile=target_context.biosphere,
        policy=MigrationPolicy.permissive(),
        catalog_provider=provider,
    )

    assert "migration.inferred_reverse" in {issue.code for issue in strict_error.value.report.issues}
    assert migrated.data[0]["exchanges"][0]["name"] == target_identity[0]
    assert migrated.last_migration_report.lossy
    assert migrated.context.background == target_context


def test_biosphere_migrates_independently_of_technosphere(facade_type):
    source_context = background("3.10", "3.10")
    target_context = background("3.10", "3.11")
    rule = load_biosphere_resources()[("3.10", "3.11")]["replace"][0]
    categories = tuple(rule["source"]["categories"])
    unit = rule["source"]["unit"]
    source_identity = (rule["source"]["name"], categories, unit)
    target_identity = (rule["target"]["name"], categories, rule["target"].get("unit", unit))
    provider = InMemoryCatalogProvider(
        biosphere=[
            BiosphereCatalog(source_context.biosphere, {source_identity}),
            BiosphereCatalog(target_context.biosphere, {target_identity}),
        ]
    )
    source = inventory(facade_type, source_context, biosphere_exchange(rule["source"]))

    migrated = source.migrate_background(target_context, catalog_provider=provider)

    assert migrated.context.background.technosphere == source_context.technosphere
    assert migrated.context.background.biosphere == target_context.biosphere
    assert migrated.data[0]["exchanges"][0]["name"] == rule["target"]["name"]
    assert migrated.inventory_format in {InventoryFormat.BRIGHTWAY_EXCEL, InventoryFormat.SIMAPRO_CSV}


def test_legacy_technosphere_target_preserves_exact_existing_biosphere(facade_type):
    source_context = background("3.10", "3.9")
    target_context = background("3.11", "3.9")
    rule = safe_replacement()
    source_identity = technosphere_identity(rule["source"])
    target_identity = technosphere_identity(rule["target"])
    source = inventory(facade_type, source_context, technosphere_exchange(source_identity))

    migrated = source.migrate_background(
        BackgroundProfile("ecoinvent", "3.11", "cutoff"),
        catalog_provider=technosphere_provider(source_context, target_context, source_identity, target_identity),
    )

    assert migrated.context.background == target_context


def test_legacy_uvek_target_uses_documented_ecoinvent_310_biosphere(facade_type):
    source_context = background("3.11", "3.11")
    source = inventory(facade_type, source_context)

    migrated = source.migrate_background(
        BackgroundProfile("uvek", "2025", "cutoff"),
        policy=MigrationPolicy(validate_source=False, validate_target=False),
        catalog_provider=InMemoryCatalogProvider(),
    )

    assert migrated.context.background == BackgroundContext(
        technosphere=TechnosphereProfile("uvek", "2025", "cutoff"),
        biosphere=BiosphereProfile("ecoinvent", "3.10"),
    )


def test_complete_target_rejects_redundant_biosphere_argument(facade_type):
    context = background("3.10")
    source = inventory(facade_type, context)

    with pytest.raises(TypeError, match="complete BackgroundContext"):
        source.migrate_background(
            context,
            biosphere_profile=context.biosphere,
            catalog_provider=InMemoryCatalogProvider(),
        )
