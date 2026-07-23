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
from brightpath.core.policies import MigrationPolicy, PolicyAction
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


def biosphere_exchange(specification, *, categories=None, amount=1.0, include_uuid=True):
    exchange = {
        "name": specification["name"],
        "categories": tuple(specification["categories"]) if categories is None else categories,
        "unit": specification["unit"],
        "amount": amount,
        "type": "biosphere",
    }
    if include_uuid:
        exchange["uuid"] = specification["uuid"]
    return exchange


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

    assert result.succeeded, result.report.to_dict()
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


def test_reverse_biosphere_migration_falls_back_to_unique_child_compartment():
    source = background("3.9")
    target = background("3.8")
    source_identity = ("Sulfuric acid", ("water",), "kilogram")
    target_identity = ("Sulfuric acid", ("water", "surface water"), "kilogram")
    original = document(
        source,
        {
            "name": source_identity[0],
            "categories": source_identity[1],
            "unit": source_identity[2],
            "amount": 1.0,
            "type": "biosphere",
        },
    )
    provider = InMemoryCatalogProvider(
        biosphere=[
            BiosphereCatalog(source.biosphere, {source_identity}),
            BiosphereCatalog(target.biosphere, {target_identity}),
        ]
    )

    result = execute_background_migration(
        original,
        target,
        provider,
        MigrationPolicy(
            on_inferred_reverse=PolicyAction.WARN,
            on_information_loss=PolicyAction.WARN,
        ),
    )

    assert result.succeeded, result.report.to_dict()
    assert result.value.data[0]["exchanges"][0]["categories"] == list(target_identity[1])
    assert "migration.biosphere_parent_compartment_fallback" in {
        issue.code for issue in result.report.issues
    }


def test_reverse_preference_resolves_polystyrene_fae_without_ambiguity():
    resource = load_technosphere_resources("cutoff")[("3.9", "3.10")]
    rule = next(
        candidate
        for candidate in resource["replace"]
        if candidate.get("reverse_preferred") is True
        and candidate["target"]["name"] == "treatment of waste polystyrene, municipal incineration FAE"
    )
    source = background("3.10", "3.10")
    target = background("3.9", "3.9")
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
    assert result.value.data[0]["exchanges"][0]["name"] == target_identity[0]
    assert "migration.replacement_ambiguous" not in {issue.code for issue in result.report.stages[-1].issues}


def test_reverse_preference_resolves_row_unpolluted_wastewater_without_ambiguity():
    resource = load_technosphere_resources("cutoff")[("3.8", "3.9")]
    rule = next(
        candidate
        for candidate in resource["replace"]
        if candidate.get("reverse_preferred") is True
        and candidate["source"]["name"] == "treatment of wastewater, unpolluted, capacity 5E9l/year"
        and candidate["source"]["location"] == "RoW"
    )
    source = background("3.9", "3.9")
    target = background("3.8", "3.8")
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
    assert result.value.data[0]["exchanges"][0]["name"] == target_identity[0]
    assert "migration.replacement_ambiguous" not in {issue.code for issue in result.report.stages[-1].issues}


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


def test_graphite_anode_market_is_disaggregated_from_38_to_39():
    resource = load_technosphere_resources("cutoff")[("3.8", "3.9")]
    rule = next(
        rule
        for rule in resource["disaggregate"]
        if rule["source"]["name"] == "market for anode, graphite, for lithium-ion battery"
    )
    source_identity = technosphere_identity(rule["source"])
    target_identities = {technosphere_identity(target) for target in rule["targets"]}
    source = background("3.8")
    target = background("3.9")
    original = document(source, technosphere_exchange(source_identity, amount=10.0))
    provider = InMemoryCatalogProvider(
        technosphere=[
            TechnosphereCatalog(source.technosphere, {source_identity}),
            TechnosphereCatalog(target.technosphere, target_identities),
        ]
    )

    result = execute_background_migration(original, target, provider)

    assert result.succeeded
    exchanges = result.value.data[0]["exchanges"]
    assert {technosphere_identity(exchange) for exchange in exchanges} == target_identities
    assert sum(exchange["amount"] for exchange in exchanges) == pytest.approx(10.0)
    assert {exchange["location"] for exchange in exchanges} == {"CN", "RoW"}


def test_biosphere_replacement_runs_independently_of_technosphere():
    source = background("3.10", "3.10")
    target = background("3.10", "3.11")
    rule = load_biosphere_resources()[("3.10", "3.11")]["replace"][0]
    categories = tuple(rule["source"]["categories"])
    unit = rule["source"]["unit"]
    source_identity = (rule["source"]["name"], categories, unit)
    target_identity = (rule["target"]["name"], categories, unit)
    original = document(source, biosphere_exchange(rule["source"], include_uuid=False))
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
    source_identity = (
        rule["source"]["name"],
        tuple(rule["source"]["categories"]),
        rule["source"]["unit"],
    )
    original = document(source, biosphere_exchange(rule["source"], include_uuid=False))
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


def test_strict_biosphere_deletion_fails_only_when_the_inventory_matches_the_rule():
    source = background("3.5", "3.5")
    target = BackgroundContext(
        technosphere=source.technosphere,
        biosphere=BiosphereProfile("ecoinvent", "3.6"),
    )
    rule = load_biosphere_resources()[("3.5", "3.6")]["delete"][0]
    categories = tuple(rule["source"]["categories"])
    unit = rule["source"]["unit"]
    original = document(source, biosphere_exchange(rule["source"], include_uuid=False))
    provider = InMemoryCatalogProvider(
        biosphere=[BiosphereCatalog(source.biosphere, {(rule["source"]["name"], categories, unit)})]
    )

    result = execute_background_migration(original, target, provider)

    assert not result.succeeded
    assert "migration.biosphere_deletion" in {issue.code for issue in result.report.issues}


def test_strict_biosphere_deletion_rule_allows_unaffected_inventory():
    source = background("3.5", "3.5")
    target = BackgroundContext(
        technosphere=source.technosphere,
        biosphere=BiosphereProfile("ecoinvent", "3.6"),
    )
    original = document(source)
    provider = InMemoryCatalogProvider()

    result = execute_background_migration(original, target, provider)

    assert result.succeeded


def test_reverse_biosphere_deletion_route_warns_without_blocking():
    source = background("3.6", "3.6")
    target = BackgroundContext(
        technosphere=source.technosphere,
        biosphere=BiosphereProfile("ecoinvent", "3.5"),
    )

    result = execute_background_migration(
        document(source),
        target,
        InMemoryCatalogProvider(),
        MigrationPolicy(on_inferred_reverse=PolicyAction.WARN),
    )

    assert result.succeeded
    notices = [issue for issue in result.report.issues if issue.code == "migration.biosphere_reverse_route_notice"]
    assert len(notices) == 1
    assert notices[0].severity.value == "warning"


def test_reverse_biosphere_rename_uses_complete_non_uuid_identity():
    source = background("3.10", "3.10")
    target = background("3.8", "3.8")
    resource = load_biosphere_resources()[("3.8", "3.9")]
    rule = next(
        rule
        for rule in resource["replace"]
        if rule["source"]["name"] == "Particulates, < 2.5 um"
        and rule["source"]["categories"] == ["air", "urban air close to ground"]
    )
    original_identity = (
        rule["target"]["name"],
        tuple(rule["source"]["categories"]),
        rule["source"]["unit"],
    )
    target_identity = (
        rule["source"]["name"],
        tuple(rule["source"]["categories"]),
        rule["source"]["unit"],
    )
    original = document(
        source,
        {
            "name": original_identity[0],
            "categories": original_identity[1],
            "unit": original_identity[2],
            "amount": 1.0,
            "type": "biosphere",
        },
    )
    provider = InMemoryCatalogProvider(
        biosphere=[
            BiosphereCatalog(source.biosphere, {original_identity}),
            BiosphereCatalog(target.biosphere, {target_identity}),
        ]
    )

    result = execute_background_migration(
        original,
        target,
        provider,
        MigrationPolicy(
            on_inferred_reverse=PolicyAction.WARN,
            on_information_loss=PolicyAction.WARN,
        ),
    )

    assert result.succeeded, result.report.to_dict()
    exchange = result.value.data[0]["exchanges"][0]
    assert (exchange["name"], tuple(exchange["categories"]), exchange["unit"]) == target_identity


def test_unrepresentable_biosphere_unit_change_is_removed_with_a_warning():
    source = background("3.9", "3.9")
    target = background("3.10", "3.10")
    rule = next(
        rule
        for rule in load_biosphere_resources()[("3.9", "3.10")]["replace"]
        if rule["source"]["name"] == "Manganese-55"
    )
    source_identity = (
        rule["source"]["name"],
        tuple(rule["source"]["categories"]),
        rule["source"]["unit"],
    )
    original = document(source, biosphere_exchange(rule["source"], include_uuid=False))
    provider = InMemoryCatalogProvider(
        biosphere=[
            BiosphereCatalog(source.biosphere, {source_identity}),
            BiosphereCatalog(target.biosphere, set()),
        ]
    )

    result = execute_background_migration(original, target, provider)

    assert result.succeeded
    assert result.value.data[0]["exchanges"] == []
    assert "migration.biosphere_exchange_removed_unsafe_unit" in {
        issue.code for issue in result.report.issues
    }
    assert "migration.biosphere_exchange_removed_unsafe_unit" in {
        loss.code for loss in result.report.losses
    }


@pytest.mark.parametrize("reverse", [False, True])
def test_standard_cubic_meter_and_sm3_biosphere_units_use_a_factor_of_one(reverse):
    resource = load_biosphere_resources()[("3.8", "3.9")]
    rule = next(
        rule
        for rule in resource["replace"]
        if rule["source"]["name"] == "Gas, natural, in ground"
    )
    source = background("3.9", "3.9") if reverse else background("3.8", "3.8")
    target = background("3.8", "3.8") if reverse else background("3.9", "3.9")
    source_specification = rule["target"] if reverse else rule["source"]
    target_specification = rule["source"] if reverse else rule["target"]
    source_identity = (
        source_specification["name"],
        tuple(rule["source"]["categories"]),
        _canonical_unit(source_specification["unit"]),
    )
    target_identity = (
        target_specification["name"],
        tuple(rule["source"]["categories"]),
        _canonical_unit(target_specification["unit"]),
    )
    original = document(
        source,
        {
            "name": source_identity[0],
            "categories": source_identity[1],
            "unit": source_identity[2],
            "amount": 2.5,
            "type": "biosphere",
        },
    )
    provider = InMemoryCatalogProvider(
        biosphere=[
            BiosphereCatalog(source.biosphere, {source_identity}),
            BiosphereCatalog(target.biosphere, {target_identity}),
        ]
    )

    result = execute_background_migration(
        original,
        target,
        provider,
        (
            MigrationPolicy(
                on_inferred_reverse=PolicyAction.WARN,
                on_information_loss=PolicyAction.WARN,
            )
            if reverse
            else MigrationPolicy()
        ),
    )

    assert result.succeeded, result.report.to_dict()
    exchange = result.value.data[0]["exchanges"][0]
    assert (exchange["name"], tuple(exchange["categories"]), exchange["unit"]) == target_identity
    assert exchange["amount"] == 2.5


def test_uuid_less_nitrogen_oxides_uses_its_air_compartment():
    source = background("3.9", "3.9")
    target = background("3.10", "3.10")
    original = document(
        source,
        {
            "name": "Nitrogen oxides",
            "categories": ["air"],
            "unit": "kilogram",
            "amount": 1.0,
            "type": "biosphere",
        },
    )
    identity = ("Nitrogen oxides", ("air",), "kilogram")
    provider = InMemoryCatalogProvider(
        biosphere=[
            BiosphereCatalog(source.biosphere, {identity}),
            BiosphereCatalog(target.biosphere, {identity}),
        ]
    )

    result = execute_background_migration(original, target, provider)

    assert result.succeeded
    exchange = result.value.data[0]["exchanges"][0]
    assert exchange["uuid"] == "c1b91234-6f24-417b-8309-46111d09c457"
    assert exchange["categories"] == ["air"]


def test_uuid_less_biosphere_exchange_matches_unique_source_tuple():
    source = background("3.7", "3.7")
    target = background("3.8", "3.8")
    identity = ("Sulfur dioxide", ("air",), "kilogram")
    original = document(
        source,
        {
            "name": "Sulfur dioxide",
            "categories": ["air"],
            "unit": "kilogram",
            "amount": 0.0003,
            "type": "biosphere",
        },
    )
    provider = InMemoryCatalogProvider(
        biosphere=[
            BiosphereCatalog(source.biosphere, {identity}),
            BiosphereCatalog(target.biosphere, {identity}),
        ]
    )

    result = execute_background_migration(original, target, provider)

    assert result.succeeded
    exchange = result.value.data[0]["exchanges"][0]
    rule = next(
        rule
        for rule in load_biosphere_resources()[("3.7", "3.8")]["replace"]
        if (
            rule["source"]["name"],
            tuple(rule["source"]["categories"]),
            rule["source"]["unit"],
        )
        == identity
    )
    assert exchange["name"] == "Sulfur dioxide"
    assert exchange["categories"] == ["air"]
    assert exchange["uuid"] == rule["target"]["uuid"]
    assert "migration.biosphere_replacement_ambiguous" not in {issue.code for issue in result.report.issues}


def test_uuid_less_biosphere_exchange_matches_source_tuples_across_intermediate_steps():
    source = background("3.7", "3.7")
    target = background("3.10", "3.10")
    intermediate_identity = (
        "Ethane, 1,1-difluoro-, HFC-152a",
        ("air", "urban air close to ground"),
        "kilogram",
    )
    target_identity = (
        "1,1-Difluoroethane",
        intermediate_identity[1],
        intermediate_identity[2],
    )
    original = document(
        source,
        {
            "name": intermediate_identity[0],
            "categories": list(intermediate_identity[1]),
            "unit": intermediate_identity[2],
            "amount": 0.013583,
            "type": "biosphere",
        },
    )
    provider = InMemoryCatalogProvider(
        biosphere=[
            BiosphereCatalog(source.biosphere, {intermediate_identity}),
            BiosphereCatalog(BiosphereProfile("ecoinvent", "3.8"), {intermediate_identity}),
            BiosphereCatalog(BiosphereProfile("ecoinvent", "3.9"), {intermediate_identity}),
            BiosphereCatalog(target.biosphere, {target_identity}),
        ]
    )

    result = execute_background_migration(original, target, provider)

    assert result.succeeded
    exchange = result.value.data[0]["exchanges"][0]
    final_rule = next(
        rule
        for rule in load_biosphere_resources()[("3.9", "3.10")]["replace"]
        if (
            rule["source"]["name"],
            tuple(rule["source"]["categories"]),
            rule["source"]["unit"],
        )
        == intermediate_identity
        and rule["target"]["name"] == target_identity[0]
    )
    assert exchange["name"] == target_identity[0]
    assert exchange["categories"] == list(target_identity[1])
    assert exchange["uuid"] == final_rule["target"]["uuid"]
    assert "migration.biosphere_replacement_ambiguous" not in {issue.code for issue in result.report.issues}


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
