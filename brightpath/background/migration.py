"""Pure, policy-aware planning for background database migrations.

This module resolves packaged migration resources without reading reference
catalogs or transforming inventory data.  The resulting plan keeps the exact
source and target profiles while recording the coarser migration series used
to find resource edges.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Iterable

from brightpath.core.context import BackgroundContext, BiosphereProfile, TechnosphereProfile, VersionResolution
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.core.reports import Issue, Loss, Severity, StageKind, StageReport
from brightpath.exceptions import MigrationUnavailableError
from brightpath.migrations.engine import resolve_migration_route
from brightpath.migrations.resources import (
    load_biosphere_resources,
    load_technosphere_resources,
    load_uvek_biosphere_resource,
    load_uvek_technosphere_resource,
)

_STRICT_MIGRATION_POLICY = MigrationPolicy.strict()


class MigrationAxis(str, Enum):
    """Independent background component addressed by a route step."""

    TECHNOSPHERE = "technosphere"
    BIOSPHERE = "biosphere"


@dataclass(frozen=True)
class MigrationRouteStep:
    """One packaged resource edge in a background migration plan.

    :param axis: Background component affected by this step.
    :param source_version: Source migration series for this directed step.
    :param target_version: Target migration series for this directed step.
    :param direction: ``"forward"`` or ``"backward"`` relative to the
        packaged resource.
    :param resource_name: Stable resource name, never a machine-specific
        absolute path.
    :param replacement_rules: Number of replacement rules in the resource.
    :param disaggregation_rules: Number of one-to-many rules in the resource.
    :param deletion_rules: Number of deletion rules in the resource.
    """

    axis: MigrationAxis
    source_version: str
    target_version: str
    direction: str
    resource_name: str
    replacement_rules: int = 0
    disaggregation_rules: int = 0
    deletion_rules: int = 0

    def __post_init__(self) -> None:
        object.__setattr__(self, "axis", MigrationAxis(self.axis))
        if self.direction not in {"forward", "backward"}:
            raise ValueError("direction must be 'forward' or 'backward'.")
        for field_name in ("source_version", "target_version", "resource_name"):
            if not str(getattr(self, field_name)).strip():
                raise ValueError(f"{field_name} must not be empty.")
        for field_name in ("replacement_rules", "disaggregation_rules", "deletion_rules"):
            value = getattr(self, field_name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValueError(f"{field_name} must be a non-negative integer.")

    @property
    def inferred_reverse(self) -> bool:
        """Whether this step reverses a forward-only packaged resource."""

        return self.direction == "backward"


@dataclass(frozen=True)
class MigrationPlan:
    """Immutable plan for independent technosphere and biosphere migration.

    Route or capability failures are represented in :attr:`report`, making
    every valid planning request inspectable for dry-run workflows.
    """

    source: BackgroundContext
    target: BackgroundContext
    source_technosphere_resolution: VersionResolution
    target_technosphere_resolution: VersionResolution
    source_biosphere_resolution: VersionResolution
    target_biosphere_resolution: VersionResolution
    technosphere_steps: tuple[MigrationRouteStep, ...] = ()
    biosphere_steps: tuple[MigrationRouteStep, ...] = ()
    report: StageReport = StageReport(StageKind.MIGRATION_PLANNING)

    def __post_init__(self) -> None:
        if not isinstance(self.source, BackgroundContext):
            raise TypeError("source must be a BackgroundContext.")
        if not isinstance(self.target, BackgroundContext):
            raise TypeError("target must be a BackgroundContext.")
        for field_name in (
            "source_technosphere_resolution",
            "target_technosphere_resolution",
            "source_biosphere_resolution",
            "target_biosphere_resolution",
        ):
            if not isinstance(getattr(self, field_name), VersionResolution):
                raise TypeError(f"{field_name} must be a VersionResolution.")
        object.__setattr__(self, "technosphere_steps", tuple(self.technosphere_steps))
        object.__setattr__(self, "biosphere_steps", tuple(self.biosphere_steps))
        if any(step.axis is not MigrationAxis.TECHNOSPHERE for step in self.technosphere_steps):
            raise ValueError("technosphere_steps must contain only technosphere route steps.")
        if any(step.axis is not MigrationAxis.BIOSPHERE for step in self.biosphere_steps):
            raise ValueError("biosphere_steps must contain only biosphere route steps.")
        if self.report.stage is not StageKind.MIGRATION_PLANNING:
            raise ValueError("Migration plan report must use the migration_planning stage.")

    @property
    def steps(self) -> tuple[MigrationRouteStep, ...]:
        """Return technosphere steps followed by biosphere steps."""

        return self.technosphere_steps + self.biosphere_steps

    @property
    def requires_migration(self) -> bool:
        """Whether at least one exact background component differs."""

        return self.source != self.target

    @property
    def changed(self) -> bool:
        """Whether the plan contains at least one executable resource step."""

        return bool(self.steps)

    @property
    def executable(self) -> bool:
        """Whether policy and route checks permit execution of this plan."""

        return not self.report.has_errors

    @property
    def succeeded(self) -> bool:
        """Alias for :attr:`executable` used by generic operation callers."""

        return self.executable


def plan_background_migration(
    source: BackgroundContext,
    target: BackgroundContext,
    policy: MigrationPolicy = _STRICT_MIGRATION_POLICY,
) -> MigrationPlan:
    """Plan a background migration without reading catalogs or changing data.

    Exact profile versions remain on ``source`` and ``target``.  Route lookup
    uses the recorded :class:`~brightpath.core.context.VersionResolution`
    values, so a patch version such as ecoinvent ``3.10.1`` can use the
    ``3.10`` migration-resource series without being rewritten in the plan.

    Unsupported routes and policy failures are returned as structured error
    issues.  Only invalid argument types raise immediately.

    :param source: Exact source background context.
    :param target: Exact target background context.
    :param policy: Explicit handling of reverse and lossy operations.
    :return: An immutable, inspectable migration plan.
    """

    if not isinstance(source, BackgroundContext):
        raise TypeError("source must be a BackgroundContext.")
    if not isinstance(target, BackgroundContext):
        raise TypeError("target must be a BackgroundContext.")
    if not isinstance(policy, MigrationPolicy):
        raise TypeError("policy must be a MigrationPolicy.")

    source_technosphere_resolution = source.technosphere.resolve_migration_series()
    target_technosphere_resolution = target.technosphere.resolve_migration_series()
    source_biosphere_resolution = source.biosphere.resolve_migration_series()
    target_biosphere_resolution = target.biosphere.resolve_migration_series()

    issues: list[Issue] = []
    losses: list[Loss] = []
    technosphere_steps = _plan_technosphere(
        source,
        target,
        source_technosphere_resolution,
        target_technosphere_resolution,
        policy,
        issues,
        losses,
    )
    biosphere_steps = _plan_biosphere(
        source,
        target,
        source_biosphere_resolution,
        target_biosphere_resolution,
        policy,
        issues,
        losses,
    )
    report = StageReport(
        stage=StageKind.MIGRATION_PLANNING,
        label="background migration plan",
        issues=tuple(issues),
        losses=tuple(losses),
        metrics={
            "policy": policy.to_dict(),
            "technosphere": {
                "source": _resolution_metrics(source_technosphere_resolution),
                "target": _resolution_metrics(target_technosphere_resolution),
                "steps": len(technosphere_steps),
            },
            "biosphere": {
                "source": _resolution_metrics(source_biosphere_resolution),
                "target": _resolution_metrics(target_biosphere_resolution),
                "steps": len(biosphere_steps),
            },
        },
    )
    return MigrationPlan(
        source=source,
        target=target,
        source_technosphere_resolution=source_technosphere_resolution,
        target_technosphere_resolution=target_technosphere_resolution,
        source_biosphere_resolution=source_biosphere_resolution,
        target_biosphere_resolution=target_biosphere_resolution,
        technosphere_steps=technosphere_steps,
        biosphere_steps=biosphere_steps,
        report=report,
    )


def _plan_technosphere(
    source: BackgroundContext,
    target: BackgroundContext,
    source_resolution: VersionResolution,
    target_resolution: VersionResolution,
    policy: MigrationPolicy,
    issues: list[Issue],
    losses: list[Loss],
) -> tuple[MigrationRouteStep, ...]:
    source_profile = source.technosphere
    target_profile = target.technosphere
    axis = MigrationAxis.TECHNOSPHERE
    if not _families_supported(axis, source_profile.family, target_profile.family, issues):
        return ()
    if source_profile == target_profile:
        return ()
    if source_profile.family != target_profile.family:
        if source_profile.family == "ecoinvent" and target_profile.family == "uvek":
            return _plan_uvek_technosphere(
                source_profile,
                target_profile,
                source_resolution,
                target_resolution,
                policy,
                issues,
                losses,
            )
        _unavailable_issue(
            issues,
            axis,
            "cross_family",
            f"Technosphere migration from {source_profile.family} to {target_profile.family} is not available.",
        )
        return ()
    if source_profile.system_model != target_profile.system_model:
        _unavailable_issue(
            issues,
            axis,
            "cross_system_model",
            "Migration between ecoinvent system models is not supported.",
        )
        return ()
    if source_profile.family == "uvek":
        _unavailable_issue(
            issues,
            axis,
            "uvek_route",
            f"No UVEK technosphere migration route is available from {source_profile.version} "
            f"to {target_profile.version}.",
        )
        return ()
    if source_profile.system_model != "cutoff":
        _unavailable_issue(
            issues,
            axis,
            "system_model",
            f"No ecoinvent {source_profile.system_model} technosphere migration resources are packaged.",
        )
        return ()
    if source_resolution.migration_series == target_resolution.migration_series:
        _same_series_issue(issues, axis, source_resolution, target_resolution)
        return ()

    resources = load_technosphere_resources(source_profile.system_model)
    return _resolve_steps(
        axis,
        source_resolution.migration_series,
        target_resolution.migration_series,
        resources,
        policy,
        issues,
        losses,
    )


def _plan_biosphere(
    source: BackgroundContext,
    target: BackgroundContext,
    source_resolution: VersionResolution,
    target_resolution: VersionResolution,
    policy: MigrationPolicy,
    issues: list[Issue],
    losses: list[Loss],
) -> tuple[MigrationRouteStep, ...]:
    source_profile = source.biosphere
    target_profile = target.biosphere
    axis = MigrationAxis.BIOSPHERE
    if not _families_supported(axis, source_profile.family, target_profile.family, issues):
        return ()
    if source_profile == target_profile:
        return ()
    if (
        source.technosphere.family == "ecoinvent"
        and target.technosphere.family == "uvek"
        and source_profile.family == "ecoinvent"
        and target_profile == BiosphereProfile("ecoinvent", "3.10")
    ):
        direct = _plan_uvek_biosphere(
            source_resolution,
            target_resolution,
            policy,
            issues,
            losses,
        )
        if direct is not None:
            return direct
    if source_profile.family != target_profile.family:
        _unavailable_issue(
            issues,
            axis,
            "cross_family",
            f"Biosphere migration from {source_profile.family} to {target_profile.family} is not available.",
        )
        return ()
    if source_profile.family == "uvek":
        _unavailable_issue(
            issues,
            axis,
            "uvek_route",
            f"No UVEK biosphere migration route is available from {source_profile.version} "
            f"to {target_profile.version}.",
        )
        return ()
    if source_resolution.migration_series == target_resolution.migration_series:
        _same_series_issue(issues, axis, source_resolution, target_resolution)
        return ()

    resources = load_biosphere_resources()
    try:
        return _resolve_steps(
            axis,
            source_resolution.migration_series,
            target_resolution.migration_series,
            resources,
            policy,
            issues,
            losses,
        )
    except _BiosphereRouteGap:
        return _resolve_biosphere_gap(
            source_resolution.migration_series,
            target_resolution.migration_series,
            resources,
            policy,
            issues,
            losses,
        )


class _BiosphereRouteGap(Exception):
    """Internal signal allowing the planner to identify missing bio edges."""


def _resolve_steps(
    axis: MigrationAxis,
    source_series: str,
    target_series: str,
    resources: dict[tuple[str, str], dict],
    policy: MigrationPolicy,
    issues: list[Issue],
    losses: list[Loss],
) -> tuple[MigrationRouteStep, ...]:
    try:
        route = resolve_migration_route(source_series, target_series, resources)
    except MigrationUnavailableError as error:
        if axis is MigrationAxis.BIOSPHERE:
            raise _BiosphereRouteGap from error
        _unavailable_issue(issues, axis, "route", str(error))
        return ()
    return _materialize_steps(axis, route, resources, policy, issues, losses)


def _resolve_biosphere_gap(
    source_series: str,
    target_series: str,
    resources: dict[tuple[str, str], dict],
    policy: MigrationPolicy,
    issues: list[Issue],
    losses: list[Loss],
) -> tuple[MigrationRouteStep, ...]:
    """Use the ecoinvent release backbone to identify absent bio resources."""

    try:
        expected_route = resolve_migration_route(
            source_series,
            target_series,
            load_technosphere_resources("cutoff"),
        )
    except MigrationUnavailableError as error:
        _unavailable_issue(issues, MigrationAxis.BIOSPHERE, "route", str(error))
        return ()

    available_route = []
    for current, neighbor, direction in expected_route:
        forward_pair = (current, neighbor) if direction == "forward" else (neighbor, current)
        if forward_pair not in resources:
            _unavailable_issue(
                issues,
                MigrationAxis.BIOSPHERE,
                "resource_missing",
                "No biosphere migration resource is packaged for ecoinvent "
                f"{forward_pair[0]} to {forward_pair[1]} ({direction} plan step).",
                details={
                    "source_series": forward_pair[0],
                    "target_series": forward_pair[1],
                    "direction": direction,
                },
            )
            continue
        available_route.append((current, neighbor, direction))
    return _materialize_steps(
        MigrationAxis.BIOSPHERE,
        available_route,
        resources,
        policy,
        issues,
        losses,
    )


def _plan_uvek_technosphere(
    source_profile: TechnosphereProfile,
    target_profile: TechnosphereProfile,
    source_resolution: VersionResolution,
    target_resolution: VersionResolution,
    policy: MigrationPolicy,
    issues: list[Issue],
    losses: list[Loss],
) -> tuple[MigrationRouteStep, ...]:
    resource = load_uvek_technosphere_resource()
    source_specification = resource["source_profile"]
    target_specification = resource["target_profile"]
    supported = (
        source_profile.family == source_specification["family"]
        and target_profile.family == target_specification["family"]
        and source_profile.system_model in source_specification["system_models"]
        and source_resolution.migration_series in source_specification["versions"]
        and target_profile.version == target_specification["version"]
        and target_profile.system_model == target_specification["system_model"]
    )
    if not supported:
        _unavailable_issue(
            issues,
            MigrationAxis.TECHNOSPHERE,
            "uvek_route",
            f"No heuristic ecoinvent-to-UVEK route is available from {source_profile.label()} "
            f"to {target_profile.label()}.",
        )
        return ()
    _heuristic_mapping_finding(MigrationAxis.TECHNOSPHERE, resource, issues, losses)
    return _materialize_steps(
        MigrationAxis.TECHNOSPHERE,
        ((source_resolution.migration_series, target_resolution.migration_series, "forward"),),
        {(source_resolution.migration_series, target_resolution.migration_series): resource},
        policy,
        issues,
        losses,
    )


def _plan_uvek_biosphere(
    source_resolution: VersionResolution,
    target_resolution: VersionResolution,
    policy: MigrationPolicy,
    issues: list[Issue],
    losses: list[Loss],
) -> tuple[MigrationRouteStep, ...] | None:
    resource = load_uvek_biosphere_resource()
    if source_resolution.migration_series not in resource["source_profile"]["versions"]:
        return None
    _heuristic_mapping_finding(MigrationAxis.BIOSPHERE, resource, issues, losses)
    return _materialize_steps(
        MigrationAxis.BIOSPHERE,
        ((source_resolution.migration_series, target_resolution.migration_series, "forward"),),
        {(source_resolution.migration_series, target_resolution.migration_series): resource},
        policy,
        issues,
        losses,
    )


def _heuristic_mapping_finding(
    axis: MigrationAxis,
    resource: dict,
    issues: list[Issue],
    losses: list[Loss],
) -> None:
    path = f"background.{axis.value}"
    message = (
        f"{axis.value.capitalize()} migration uses heuristic compatibility resource "
        f"{_resource_name(resource)!r}; mapped targets are not scientific equivalence claims."
    )
    details = {
        "axis": axis.value,
        "resource": _resource_name(resource),
        "quality": resource.get("quality"),
        "coverage": resource.get("coverage", {}),
    }
    issues.append(
        Issue(
            severity=Severity.WARNING,
            code="migration.heuristic_mapping",
            message=message,
            stage=StageKind.MIGRATION_PLANNING,
            path=path,
            details=details,
            suggested_fix="Review low-confidence mappings before using converted results for assessment.",
        )
    )
    losses.append(
        Loss(
            code="migration.heuristic_mapping",
            message=message,
            stage=StageKind.MIGRATION_PLANNING,
            path=path,
            recoverable=True,
            details=details,
        )
    )


def _materialize_steps(
    axis: MigrationAxis,
    route: Iterable[tuple[str, str, str]],
    resources: dict[tuple[str, str], dict],
    policy: MigrationPolicy,
    issues: list[Issue],
    losses: list[Loss],
) -> tuple[MigrationRouteStep, ...]:
    steps = []
    for step_index, (current, neighbor, direction) in enumerate(route):
        forward_pair = (current, neighbor) if direction == "forward" else (neighbor, current)
        resource = resources[forward_pair]
        step = MigrationRouteStep(
            axis=axis,
            source_version=current,
            target_version=neighbor,
            direction=direction,
            resource_name=_resource_name(resource),
            replacement_rules=len(resource.get("replace", ())),
            disaggregation_rules=len(resource.get("disaggregate", ())),
            deletion_rules=len(resource.get("delete", ())),
        )
        steps.append(step)
        path = f"background.{axis.value}.steps[{step_index}]"
        if step.inferred_reverse:
            _policy_issue(
                issues,
                policy.on_inferred_reverse,
                "migration.inferred_reverse",
                f"{axis.value.capitalize()} step {step.resource_name!r} uses an inferred reverse route.",
                path=path,
                details={"axis": axis.value, "resource": step.resource_name},
            )
        if step.inferred_reverse and step.disaggregation_rules:
            loss = Loss(
                code="migration.reverse_disaggregation",
                message=(
                    f"Reversing {step.disaggregation_rules} disaggregation rules can require lossy "
                    "aggregation and cannot reconstruct exchange metadata exactly."
                ),
                stage=StageKind.MIGRATION_PLANNING,
                path=path,
                details={
                    "axis": axis.value,
                    "resource": step.resource_name,
                    "rule_count": step.disaggregation_rules,
                    "policy_action": policy.on_information_loss.value,
                },
            )
            losses.append(loss)
            _policy_issue(
                issues,
                policy.on_information_loss,
                "migration.reverse_disaggregation_loss",
                loss.message,
                path=path,
                details=loss.details,
            )
        if step.deletion_rules:
            reverse = step.inferred_reverse
            loss = Loss(
                code="migration.reverse_deletion" if reverse else "migration.deletion",
                message=(
                    f"The resource contains {step.deletion_rules} deletion rules; "
                    + (
                        "a reverse migration cannot reconstruct the deleted flows."
                        if reverse
                        else "applying them removes matching flows."
                    )
                ),
                stage=StageKind.MIGRATION_PLANNING,
                path=path,
                details={
                    "axis": axis.value,
                    "resource": step.resource_name,
                    "rule_count": step.deletion_rules,
                    "policy_action": policy.on_deletion.value,
                },
            )
            losses.append(loss)
            _policy_issue(
                issues,
                policy.on_deletion,
                "migration.reverse_deletion_loss" if reverse else "migration.deletion_loss",
                loss.message,
                path=path,
                details=loss.details,
            )
    return tuple(steps)


def _families_supported(
    axis: MigrationAxis,
    source_family: str,
    target_family: str,
    issues: list[Issue],
) -> bool:
    supported = {"ecoinvent", "uvek"}
    valid = True
    for role, family in (("source", source_family), ("target", target_family)):
        if family not in supported:
            _unavailable_issue(
                issues,
                axis,
                "family_unsupported",
                f"Unsupported {role} {axis.value} background family: {family!r}.",
                details={"role": role, "family": family},
            )
            valid = False
    return valid


def _same_series_issue(
    issues: list[Issue],
    axis: MigrationAxis,
    source: VersionResolution,
    target: VersionResolution,
) -> None:
    _unavailable_issue(
        issues,
        axis,
        "exact_version_route",
        f"{axis.value.capitalize()} versions {source.exact_version} and {target.exact_version} resolve "
        f"to the same migration series {source.migration_series}; no resource establishes their equivalence.",
        details={
            "source_exact_version": source.exact_version,
            "target_exact_version": target.exact_version,
            "migration_series": source.migration_series,
        },
    )


def _unavailable_issue(
    issues: list[Issue],
    axis: MigrationAxis,
    suffix: str,
    message: str,
    *,
    details: dict | None = None,
) -> None:
    issues.append(
        Issue(
            severity=Severity.ERROR,
            code=f"migration.{axis.value}_{suffix}_unavailable",
            message=message,
            stage=StageKind.MIGRATION_PLANNING,
            path=f"background.{axis.value}",
            details=details or {"axis": axis.value},
        )
    )


def _policy_issue(
    issues: list[Issue],
    action: PolicyAction,
    code: str,
    message: str,
    *,
    path: str,
    details: dict,
) -> None:
    issues.append(
        Issue(
            severity=_severity_for_action(action),
            code=code,
            message=message,
            stage=StageKind.MIGRATION_PLANNING,
            path=path,
            details=details,
        )
    )


def _severity_for_action(action: PolicyAction) -> Severity:
    return {
        PolicyAction.ERROR: Severity.ERROR,
        PolicyAction.WARN: Severity.WARNING,
        PolicyAction.ALLOW: Severity.INFO,
    }[action]


def _resource_name(resource: dict) -> str:
    name = str(resource.get("name") or "").strip()
    if name:
        return name
    path = str(resource.get("_path") or "").strip()
    return Path(path).name or "unnamed-migration-resource"


def _resolution_metrics(resolution: VersionResolution) -> dict[str, str | bool]:
    return {
        "family": resolution.family,
        "exact_version": resolution.exact_version,
        "migration_series": resolution.migration_series,
        "strategy": resolution.strategy,
        "changed": resolution.changed,
    }
