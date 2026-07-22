from dataclasses import FrozenInstanceError

import pytest

from brightpath.background.migration import (
    MigrationAxis,
    plan_background_migration,
)
from brightpath.core.context import BackgroundContext, BiosphereProfile, TechnosphereProfile
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.core.reports import Severity, StageKind


def background(
    technosphere_version="3.10",
    biosphere_version=None,
    *,
    family="ecoinvent",
    biosphere_family=None,
    system_model="cutoff",
):
    return BackgroundContext(
        technosphere=TechnosphereProfile(family, technosphere_version, system_model),
        biosphere=BiosphereProfile(biosphere_family or family, biosphere_version or technosphere_version),
    )


def issue_codes(plan):
    return {issue.code for issue in plan.report.issues}


def test_exact_patch_versions_are_preserved_while_resource_series_are_resolved():
    source = background("3.10.1", "3.10.1")
    target = background("3.11", "3.11")

    plan = plan_background_migration(source, target)

    assert plan.source is source
    assert plan.target is target
    assert plan.source.technosphere.version == "3.10.1"
    assert plan.source.biosphere.version == "3.10.1"
    assert plan.source_technosphere_resolution.migration_series == "3.10"
    assert plan.source_biosphere_resolution.migration_series == "3.10"
    assert plan.report.metrics["technosphere"]["source"]["exact_version"] == "3.10.1"
    assert plan.report.metrics["technosphere"]["source"]["migration_series"] == "3.10"
    assert [(step.source_version, step.target_version) for step in plan.technosphere_steps] == [("3.10", "3.11")]
    assert [(step.source_version, step.target_version) for step in plan.biosphere_steps] == [("3.10", "3.11")]
    assert plan.executable


def test_different_exact_versions_in_one_series_are_not_treated_as_a_noop():
    source = background("3.10.1", "3.10")
    target = background("3.10.2", "3.10")

    plan = plan_background_migration(source, target)

    assert plan.requires_migration
    assert not plan.changed
    assert not plan.executable
    assert "migration.technosphere_exact_version_route_unavailable" in issue_codes(plan)


def test_technosphere_and_biosphere_axes_are_planned_independently():
    source = background("3.10", "3.10")
    technosphere_target = background("3.11", "3.10")
    biosphere_target = background("3.10", "3.11")

    technosphere_plan = plan_background_migration(source, technosphere_target)
    biosphere_plan = plan_background_migration(source, biosphere_target)

    assert len(technosphere_plan.technosphere_steps) == 1
    assert technosphere_plan.biosphere_steps == ()
    assert biosphere_plan.technosphere_steps == ()
    assert len(biosphere_plan.biosphere_steps) == 1
    assert technosphere_plan.technosphere_steps[0].axis is MigrationAxis.TECHNOSPHERE
    assert biosphere_plan.biosphere_steps[0].axis is MigrationAxis.BIOSPHERE


def test_forward_plan_exposes_directions_and_stable_resource_names():
    source = background("3.6", "3.6")
    target = background("3.8", "3.6")

    plan = plan_background_migration(source, target)

    assert [step.direction for step in plan.technosphere_steps] == ["forward", "forward"]
    assert [step.resource_name for step in plan.technosphere_steps] == [
        "ecoinvent-3.6-cutoff-ecoinvent-3.7-cutoff",
        "ecoinvent-3.7-cutoff-ecoinvent-3.8-cutoff",
    ]
    assert all(step.disaggregation_rules > 0 for step in plan.technosphere_steps)
    assert plan.executable


def test_strict_reverse_plan_records_policy_errors_and_disaggregation_loss():
    source = background("3.7", "3.7")
    target = background("3.6", "3.7")

    plan = plan_background_migration(source, target)

    assert [step.direction for step in plan.technosphere_steps] == ["backward"]
    assert plan.technosphere_steps[0].inferred_reverse
    assert not plan.executable
    assert "migration.inferred_reverse" in issue_codes(plan)
    assert "migration.reverse_disaggregation_loss" in issue_codes(plan)
    assert {loss.code for loss in plan.report.losses} == {"migration.reverse_disaggregation"}
    assert all(issue.severity is Severity.ERROR for issue in plan.report.issues)


def test_permissive_reverse_plan_warns_but_remains_executable():
    source = background("3.7", "3.7")
    target = background("3.6", "3.7")

    plan = plan_background_migration(source, target, policy=MigrationPolicy.permissive())

    assert plan.executable
    assert plan.succeeded
    assert plan.report.lossy
    assert {issue.severity for issue in plan.report.issues} == {Severity.WARNING}


def test_allow_policy_records_reverse_as_information():
    source = background("3.7", "3.7")
    target = background("3.6", "3.7")
    policy = MigrationPolicy(
        on_inferred_reverse=PolicyAction.ALLOW,
        on_information_loss=PolicyAction.ALLOW,
    )

    plan = plan_background_migration(source, target, policy=policy)

    assert plan.executable
    assert {issue.severity for issue in plan.report.issues} == {Severity.INFO}


def test_reverse_deletion_rule_does_not_make_a_data_free_plan_fail():
    source = background("3.6", "3.6")
    target = background("3.6", "3.5")

    plan = plan_background_migration(source, target, policy=MigrationPolicy.permissive())

    assert [step.direction for step in plan.biosphere_steps] == ["backward"]
    assert plan.biosphere_steps[0].deletion_rules == 3
    assert plan.executable
    assert "migration.reverse_deletion" not in {loss.code for loss in plan.report.losses}
    assert "migration.reverse_deletion_loss" not in issue_codes(plan)


def test_311_to_312_biosphere_resource_is_planned_by_exact_edge():
    source = background("3.12", "3.11")
    target = background("3.12", "3.12")

    plan = plan_background_migration(source, target)

    assert plan.technosphere_steps == ()
    assert len(plan.biosphere_steps) == 1
    step = plan.biosphere_steps[0]
    assert (step.source_version, step.target_version) == ("3.11", "3.12")
    assert step.resource_name == "ecoinvent-3.11-biosphere-ecoinvent-3.12-biosphere"
    assert step.replacement_rules == 12
    assert plan.report.issues == ()
    assert plan.executable


@pytest.mark.parametrize(
    ("source", "target"),
    [
        (background(), background("2025", "2025", family="uvek")),
        (background("3.10", "3.10"), background("3.11", "3.10", system_model="consequential")),
        (
            background("3.10", "3.10", system_model="consequential"),
            background("3.11", "3.10", system_model="consequential"),
        ),
        (background("2024", "2025", family="uvek"), background("2025", "2025", family="uvek")),
        (background(family="custom"), background("2", "1", family="custom")),
    ],
)
def test_unsupported_family_system_model_and_uvek_routes_return_error_plans(source, target):
    plan = plan_background_migration(source, target)

    assert plan.report.stage is StageKind.MIGRATION_PLANNING
    assert plan.report.has_errors
    assert not plan.executable


@pytest.mark.parametrize(
    "same_context",
    [
        background("3.10", "3.10", system_model="consequential"),
        background("2025", "2025", family="uvek"),
    ],
)
def test_supported_noop_contexts_require_no_route(same_context):
    plan = plan_background_migration(same_context, same_context)

    assert not plan.requires_migration
    assert not plan.changed
    assert plan.steps == ()
    assert plan.report.issues == ()
    assert plan.report.losses == ()
    assert plan.executable
    with pytest.raises(FrozenInstanceError):
        plan.source = background("3.11")


@pytest.mark.parametrize("field", ["source", "target", "policy"])
def test_invalid_argument_types_raise(field):
    source = background()
    target = background("3.11")
    arguments = {"source": source, "target": target, "policy": MigrationPolicy.strict()}
    arguments[field] = object()

    with pytest.raises(TypeError, match=field):
        plan_background_migration(**arguments)
