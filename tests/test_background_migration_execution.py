from copy import deepcopy

import pytest

from brightpath.background.catalogs import (
    BiosphereCatalog,
    InMemoryCatalogProvider,
    TechnosphereCatalog,
)
from brightpath.background.execution import execute_background_migration
from brightpath.core.context import (
    BackgroundContext,
    BiosphereProfile,
    FormatProfile,
    InventoryContext,
    TechnosphereProfile,
)
from brightpath.core.policies import MigrationPolicy
from brightpath.core.reports import OperationKind, StageKind
from brightpath.migrations.engine import _canonical_unit
from brightpath.migrations.resources import load_biosphere_resources, load_technosphere_resources
from brightpath.models import InventoryDocument


def background(technosphere_version, biosphere_version=None):
    return BackgroundContext(
        technosphere=TechnosphereProfile("ecoinvent", technosphere_version, "cutoff"),
        biosphere=BiosphereProfile("ecoinvent", biosphere_version or technosphere_version),
    )


def document(context, *exchanges):
    return InventoryDocument(
        data=[
            {
                "name": "foreground process",
                "reference product": "foreground product",
                "location": "CH",
                "unit": "unit",
                "exchanges": list(exchanges),
            }
        ],
        context=InventoryContext(
            format=FormatProfile("brightway_excel", dialect="bw2io"),
            background=context,
        ),
    )


def technosphere_exchange(identity, amount=1.0):
    name, product, location, unit = identity
    return {
        "name": name,
        "reference product": product,
        "location": location,
        "unit": unit,
        "amount": amount,
        "type": "technosphere",
    }


def technosphere_identity(specification):
    return (
        specification["name"],
        specification["reference product"],
        specification["location"],
        specification["unit"],
    )


def biosphere_exchange(specification, *, categories=("air", "urban air close to ground"), amount=1.0):
    return {
        "name": specification["name"],
        "uuid": specification["uuid"],
        "categories": categories,
        "unit": specification.get("unit", "kg"),
        "amount": amount,
        "type": "biosphere",
    }


def first_safe_replacement(source_version="3.10", target_version="3.11"):
    resource = load_technosphere_resources("cutoff")[(source_version, target_version)]
    return next(
        rule
        for rule in resource["replace"]
        if _canonical_unit(rule["source"].get("unit")) == _canonical_unit(rule["target"].get("unit"))
    )


def provider_for_technosphere(source, target, source_identity, target_identity):
    return InMemoryCatalogProvider(
        technosphere=[
            TechnosphereCatalog(source.technosphere, {source_identity}),
            TechnosphereCatalog(target.technosphere, {target_identity}),
        ]
    )


def test_successful_forward_technosphere_migration_preserves_format_and_exact_context():
    source = background("3.10.1", "3.10.1")
    target = background("3.11.4", "3.10.1")
    rule = first_safe_replacement()
    source_identity = technosphere_identity(rule["source"])
    target_identity = technosphere_identity(rule["target"])
    original = document(source, technosphere_exchange(source_identity, amount=2.0))
    original_data = original.data

    result = execute_background_migration(
        original,
        target,
        provider_for_technosphere(source, target, source_identity, target_identity),
    )

    assert result.succeeded
    assert result.changed
    assert result.report.operation is OperationKind.MIGRATE
    assert result.report.metadata["committed"] is True
    assert result.value.context.background == target
    assert result.value.context.background.technosphere.version == "3.11.4"
    assert result.value.context.format is original.context.format
    assert result.value.data[0]["exchanges"][0]["name"] == target_identity[0]
    assert result.value.data[0]["exchanges"][0]["amount"] == 2.0
    assert original.data == original_data
    assert original.context.background == source
    assert [stage.stage for stage in result.report.stages] == [
        StageKind.BACKGROUND_VALIDATION,
        StageKind.MIGRATION_PLANNING,
        StageKind.BACKGROUND_MIGRATION,
        StageKind.BACKGROUND_VALIDATION,
    ]


def test_invalid_source_aborts_before_planning_and_returns_original_document():
    source = background("3.10", "3.10")
    target = background("3.11", "3.10")
    rule = first_safe_replacement()
    source_identity = technosphere_identity(rule["source"])
    original = document(source, technosphere_exchange(source_identity))
    provider = InMemoryCatalogProvider(
        technosphere=[
            TechnosphereCatalog(source.technosphere, set()),
            TechnosphereCatalog(target.technosphere, {technosphere_identity(rule["target"])}),
        ]
    )

    result = execute_background_migration(original, target, provider)

    assert not result.succeeded
    assert not result.changed
    assert result.value is original
    assert result.value.context.background == source
    assert [stage.stage for stage in result.report.stages] == [StageKind.BACKGROUND_VALIDATION]
    assert "background.technosphere_link_unresolved" in {issue.code for issue in result.report.issues}


def test_invalid_target_rolls_back_strict_application_and_reports_coverage():
    source = background("3.10", "3.10")
    target = background("3.11", "3.10")
    rule = first_safe_replacement()
    source_identity = technosphere_identity(rule["source"])
    original = document(source, technosphere_exchange(source_identity))
    provider = InMemoryCatalogProvider(
        technosphere=[
            TechnosphereCatalog(source.technosphere, {source_identity}),
            TechnosphereCatalog(target.technosphere, set()),
        ]
    )

    result = execute_background_migration(original, target, provider)

    assert not result.succeeded
    assert not result.changed
    assert result.value is original
    assert result.value.data[0]["exchanges"][0]["name"] == source_identity[0]
    assert result.report.metadata["committed"] is False
    assert result.report.stages[-2].metrics["rolled_back"] is True
    assert "migration.target_coverage_below_minimum" in {issue.code for issue in result.report.issues}


def test_permissive_target_validation_commits_with_warnings():
    source = background("3.10", "3.10")
    target = background("3.11", "3.10")
    rule = first_safe_replacement()
    source_identity = technosphere_identity(rule["source"])
    original = document(source, technosphere_exchange(source_identity))
    provider = InMemoryCatalogProvider(
        technosphere=[
            TechnosphereCatalog(source.technosphere, {source_identity}),
            TechnosphereCatalog(target.technosphere, set()),
        ]
    )

    result = execute_background_migration(original, target, provider, MigrationPolicy.permissive())

    assert result.succeeded
    assert result.value.context.background == target
    assert {issue.severity.value for issue in result.report.stages[-1].issues} == {"warning"}


def test_same_context_is_a_valid_noop():
    context = background("3.10.1", "3.10.1")
    original = document(context)

    result = execute_background_migration(original, context, InMemoryCatalogProvider())

    assert result.succeeded
    assert not result.changed
    assert not result.lossy
    assert result.value is original
    assert result.value.context.background == context
    assert result.report.stages[1].metrics["technosphere"]["steps"] == 0
    assert result.report.stages[2].metrics["steps"] == ()


def test_strict_reverse_is_rejected_before_application():
    source = background("3.11", "3.11")
    target = background("3.10", "3.11")
    rule = first_safe_replacement()
    source_identity = technosphere_identity(rule["target"])
    target_identity = technosphere_identity(rule["source"])
    original = document(source, technosphere_exchange(source_identity))

    result = execute_background_migration(
        original,
        target,
        provider_for_technosphere(source, target, source_identity, target_identity),
    )

    assert not result.succeeded
    assert result.value is original
    assert result.value.context.background == source
    assert StageKind.BACKGROUND_MIGRATION not in {stage.stage for stage in result.report.stages}
    assert "migration.inferred_reverse" in {issue.code for issue in result.report.issues}


def test_permissive_reverse_applies_replacement_and_records_loss():
    source = background("3.11", "3.11")
    target = background("3.10", "3.11")
    rule = first_safe_replacement()
    source_identity = technosphere_identity(rule["target"])
    target_identity = technosphere_identity(rule["source"])
    original = document(source, technosphere_exchange(source_identity))

    result = execute_background_migration(
        original,
        target,
        provider_for_technosphere(source, target, source_identity, target_identity),
        MigrationPolicy.permissive(),
    )

    assert result.succeeded
    assert result.changed
    assert result.lossy
    assert result.value.context.background == target
    assert result.value.data[0]["exchanges"][0]["name"] == target_identity[0]
    assert "migration.reverse_disaggregation" in {loss.code for loss in result.report.losses}


def test_forward_disaggregation_and_reverse_aggregation_execute_transactionally():
    resource = load_technosphere_resources("cutoff")[("3.6", "3.7")]
    rule = next(
        rule
        for rule in resource["disaggregate"]
        if rule["source"]["name"] == "ammonia production, partial oxidation, liquid"
    )
    original_identity = technosphere_identity(rule["source"])
    target_identities = {technosphere_identity(target) for target in rule["targets"]}
    forward_source = background("3.6", "3.6")
    forward_target = background("3.7", "3.6")
    forward_provider = InMemoryCatalogProvider(
        technosphere=[
            TechnosphereCatalog(forward_source.technosphere, {original_identity}),
            TechnosphereCatalog(forward_target.technosphere, target_identities),
        ]
    )
    original = document(forward_source, technosphere_exchange(original_identity, amount=10.0))

    forward = execute_background_migration(original, forward_target, forward_provider)

    assert forward.succeeded
    forward_exchanges = forward.value.data[0]["exchanges"]
    assert len(forward_exchanges) == 2
    assert sum(exchange["amount"] for exchange in forward_exchanges) == pytest.approx(10.0)
    assert {
        (exchange["name"], exchange["reference product"], exchange["location"], exchange["unit"])
        for exchange in forward_exchanges
    } == target_identities

    reverse_source = background("3.7", "3.7")
    reverse_target = background("3.6", "3.7")
    reverse_provider = InMemoryCatalogProvider(
        technosphere=[
            TechnosphereCatalog(reverse_source.technosphere, target_identities),
            TechnosphereCatalog(reverse_target.technosphere, {original_identity}),
        ]
    )
    reverse_input = document(reverse_source, *forward_exchanges)

    reverse = execute_background_migration(
        reverse_input,
        reverse_target,
        reverse_provider,
        MigrationPolicy.permissive(),
    )

    assert reverse.succeeded
    assert len(reverse.value.data[0]["exchanges"]) == 1
    reconstructed = reverse.value.data[0]["exchanges"][0]
    assert reconstructed["reference product"] == original_identity[1]
    assert reconstructed["location"] == original_identity[2]
    assert reconstructed["amount"] == pytest.approx(10.0)
    assert "migration.reverse_aggregation" in {loss.code for loss in reverse.report.losses}
    assert original.data[0]["exchanges"][0]["amount"] == 10.0


def test_biosphere_replacement_runs_independently_of_technosphere():
    source = background("3.10", "3.10")
    target = background("3.10", "3.11")
    rule = load_biosphere_resources()[("3.10", "3.11")]["replace"][0]
    categories = ("air", "urban air close to ground")
    source_identity = (rule["source"]["name"], categories, "kg")
    target_identity = (rule["target"]["name"], categories, "kg")
    original = document(source, biosphere_exchange(rule["source"], categories=categories))
    provider = InMemoryCatalogProvider(
        biosphere=[
            BiosphereCatalog(source.biosphere, {source_identity}),
            BiosphereCatalog(target.biosphere, {target_identity}),
        ]
    )

    result = execute_background_migration(original, target, provider)

    assert result.succeeded
    assert result.value.context.background == target
    exchange = result.value.data[0]["exchanges"][0]
    assert exchange["name"] == rule["target"]["name"]
    assert exchange["uuid"] == rule["target"]["uuid"]
    migration_stage = next(stage for stage in result.report.stages if stage.stage is StageKind.BACKGROUND_MIGRATION)
    assert migration_stage.metrics["steps"][0]["axis"] == "biosphere"
    assert migration_stage.metrics["steps"][0]["counts"]["biosphere_replacements"] == 1


def test_permissive_forward_biosphere_deletion_is_applied_and_reported_as_loss():
    source = background("3.5", "3.5")
    target = BackgroundContext(
        technosphere=source.technosphere,
        biosphere=BiosphereProfile("ecoinvent", "3.6"),
    )
    rule = load_biosphere_resources()[("3.5", "3.6")]["delete"][0]
    categories = ("air",)
    source_identity = (rule["source"]["name"], categories, "kg")
    original = document(source, biosphere_exchange(rule["source"], categories=categories))
    provider = InMemoryCatalogProvider(biosphere=[BiosphereCatalog(source.biosphere, {source_identity})])

    result = execute_background_migration(original, target, provider, MigrationPolicy.permissive())

    assert result.succeeded
    assert result.changed
    assert result.lossy
    assert result.value.data[0]["exchanges"] == []
    assert result.value.context.background == target
    migration_stage = next(stage for stage in result.report.stages if stage.stage is StageKind.BACKGROUND_MIGRATION)
    assert migration_stage.metrics["steps"][0]["counts"]["biosphere_deletions"] == 1
    assert "migration.biosphere_deletion" in {loss.code for loss in result.report.losses}


def test_unsafe_unit_change_never_fakes_target_unit_or_amount():
    source = background("3.6", "3.6")
    target = background("3.7", "3.6")
    resource = load_technosphere_resources("cutoff")[("3.6", "3.7")]
    rule = next(
        rule
        for rule in resource["replace"]
        if rule["source"].get("unit") == "MJ" and rule["target"].get("unit") == "m3"
    )
    source_identity = technosphere_identity(rule["source"])
    target_identity = technosphere_identity(rule["target"])
    original = document(source, technosphere_exchange(source_identity, amount=12.5))
    original_data = deepcopy(original.data)
    provider = provider_for_technosphere(source, target, source_identity, target_identity)

    strict = execute_background_migration(original, target, provider)

    assert not strict.succeeded
    assert strict.value is original
    assert strict.value.data == original_data
    assert "migration.unit_change_without_factor" in {issue.code for issue in strict.report.issues}

    permissive = execute_background_migration(original, target, provider, MigrationPolicy.permissive())

    assert permissive.succeeded
    assert permissive.value.context.background == target
    exchange = permissive.value.data[0]["exchanges"][0]
    assert exchange["name"] == source_identity[0]
    assert exchange["unit"] == "MJ"
    assert exchange["amount"] == 12.5
    assert original.data == original_data
    assert "migration.rule_skipped_unsafe_unit_change" in {loss.code for loss in permissive.report.losses}


def test_disabled_validation_still_preserves_source_data_and_exact_target_context():
    source = background("3.10.1", "3.10.1")
    target = background("3.11.7", "3.10.1")
    rule = first_safe_replacement()
    source_identity = technosphere_identity(rule["source"])
    original = document(source, technosphere_exchange(source_identity))
    original_data = deepcopy(original.data)
    policy = MigrationPolicy(validate_source=False, validate_target=False)

    result = execute_background_migration(original, target, InMemoryCatalogProvider(), policy)

    assert result.succeeded
    assert result.value.context.background == target
    assert result.value.context.format == original.context.format
    assert original.data == original_data
    assert original.context.background == source
