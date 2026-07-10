"""Transactional execution of policy-aware background migrations."""

from __future__ import annotations

import math
from collections.abc import Iterable, Mapping, Sequence
from copy import deepcopy
from typing import Any

from brightpath.background.catalogs import CatalogProvider, TechnosphereIdentity
from brightpath.background.migration import (
    MigrationAxis,
    MigrationPlan,
    MigrationRouteStep,
    plan_background_migration,
)
from brightpath.background.validation import validate_background_links
from brightpath.core.context import BackgroundContext, InventoryContext
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.core.reports import (
    Change,
    Issue,
    Loss,
    OperationKind,
    OperationReport,
    OperationResult,
    Severity,
    StageKind,
    StageReport,
)
from brightpath.migrations.engine import (
    _apply_aggregation,
    _apply_biosphere_rules,
    _apply_disaggregation,
    _apply_replacements,
    _biosphere_matches,
    _canonical_unit,
    _delete_biosphere_exchanges,
    _technosphere_matches,
)
from brightpath.migrations.models import MigrationStepReport
from brightpath.migrations.resources import load_biosphere_resources, load_technosphere_resources
from brightpath.models import InventoryDocument

_STRICT_POLICY = MigrationPolicy.strict()
_TECHNOSPHERE_TYPES = {"production", "technosphere", "substitution"}
_FACTOR_KEYS = (
    "conversion_factor",
    "conversion factor",
    "amount_factor",
    "amount factor",
)


def execute_background_migration(
    document: InventoryDocument,
    target: BackgroundContext,
    provider: CatalogProvider,
    policy: MigrationPolicy = _STRICT_POLICY,
    foreground_technosphere_targets: Iterable[TechnosphereIdentity] = (),
) -> OperationResult[InventoryDocument]:
    """Validate, plan, execute, and verify one background migration.

    Execution is transactional. All rule application happens on copied legacy
    dictionaries and the source document is returned unchanged when an
    error-policy condition is encountered. The document's software-format
    profile is never changed by this operation.

    :param document: Source canonical inventory document.
    :param target: Exact target technosphere and biosphere context.
    :param provider: Injected source of exact validation catalogs.
    :param policy: Explicit migration and validation decisions.
    :param foreground_technosphere_targets: Additional foreground identities
        that should not be resolved against a background catalog.
    :return: The committed document, or the original document on failure,
        paired with an immutable operation report.
    """

    _validate_arguments(document, target, provider, policy)
    foreground_targets = _foreground_targets(foreground_technosphere_targets)
    source = document.context.background
    stages: list[StageReport] = []

    if policy.validate_source:
        source_validation = _validation_with_policy(
            validate_background_links(
                document.data,
                source,
                provider,
                foreground_technosphere_targets=foreground_targets,
            ),
            role="source",
            policy=policy,
        )
        stages.append(source_validation)
        if source_validation.has_errors:
            return _result(document, source, target, policy, stages, committed=False)

    plan = plan_background_migration(source, target, policy)
    stages.append(plan.report)
    if not plan.executable:
        return _result(document, source, target, policy, stages, committed=False)

    working_data = document.data
    migration_stage = _execute_plan(working_data, plan, policy)
    stages.append(migration_stage)
    if migration_stage.has_errors:
        stages[-1] = _rolled_back_stage(migration_stage, "migration application failed")
        return _result(document, source, target, policy, stages, committed=False)

    candidate = document
    if source != target or working_data != document.data:
        candidate = document.replace(
            data=working_data,
            context=InventoryContext(format=document.context.format, background=target),
        )

    if policy.validate_target:
        target_validation = _validation_with_policy(
            validate_background_links(
                candidate.data,
                target,
                provider,
                foreground_technosphere_targets=foreground_targets,
            ),
            role="target",
            policy=policy,
        )
        target_validation = _enforce_minimum_coverage(target_validation, policy)
        stages.append(target_validation)
        if target_validation.has_errors:
            stages[-2] = _rolled_back_stage(migration_stage, "target validation failed")
            return _result(document, source, target, policy, stages, committed=False)

    return _result(candidate, source, target, policy, stages, committed=True)


def _validate_arguments(
    document: object,
    target: object,
    provider: object,
    policy: object,
) -> None:
    if not isinstance(document, InventoryDocument):
        raise TypeError("document must be an InventoryDocument.")
    if not isinstance(target, BackgroundContext):
        raise TypeError("target must be a BackgroundContext.")
    if not isinstance(provider, CatalogProvider):
        raise TypeError("provider must implement CatalogProvider.")
    if not isinstance(policy, MigrationPolicy):
        raise TypeError("policy must be a MigrationPolicy.")


def _foreground_targets(targets: Iterable[TechnosphereIdentity]) -> tuple[TechnosphereIdentity, ...]:
    if isinstance(targets, (str, bytes)):
        raise TypeError("foreground_technosphere_targets must contain four-field identities.")
    try:
        iterator = iter(targets)
    except TypeError as error:
        raise TypeError("foreground_technosphere_targets must be an iterable of four-field identities.") from error
    normalized = []
    for target in iterator:
        if isinstance(target, (str, bytes)) or not isinstance(target, Sequence) or len(target) != 4:
            raise TypeError("foreground_technosphere_targets must contain four-field identities.")
        normalized.append(tuple(str(part or "") for part in target))
    return tuple(normalized)


def _validation_with_policy(report: StageReport, *, role: str, policy: MigrationPolicy) -> StageReport:
    remapped = []
    for issue in report.issues:
        if issue.code.endswith("_link_unresolved"):
            action = policy.on_unresolved_link
        elif role == "target":
            action = policy.on_invalid_target
        else:
            action = policy.on_invalid_source
        remapped.append(_replace_issue_severity(issue, action))
    metrics = _thaw(report.metrics)
    metrics["role"] = role
    metrics["policy_action"] = policy.on_invalid_target.value if role == "target" else policy.on_invalid_source.value
    return StageReport(
        stage=report.stage,
        label=f"{role} {report.label}".strip(),
        issues=tuple(remapped),
        changes=report.changes,
        losses=report.losses,
        metrics=metrics,
    )


def _enforce_minimum_coverage(report: StageReport, policy: MigrationPolicy) -> StageReport:
    below = {}
    for axis in ("technosphere", "biosphere"):
        axis_metrics = report.metrics.get(axis, {})
        coverage = float(axis_metrics.get("coverage", 1.0))
        if coverage < policy.minimum_coverage:
            below[axis] = coverage
    if not below:
        return report

    issue = Issue(
        severity=_severity(policy.on_invalid_target),
        code="migration.target_coverage_below_minimum",
        message=("Target background-link coverage is below the configured minimum " f"of {policy.minimum_coverage:g}."),
        stage=StageKind.BACKGROUND_VALIDATION,
        path="background",
        details={"coverage": below, "minimum_coverage": policy.minimum_coverage},
        suggested_fix="Install the exact target catalogs or correct unresolved target links.",
    )
    metrics = _thaw(report.metrics)
    metrics["minimum_coverage"] = policy.minimum_coverage
    metrics["coverage_satisfied"] = False
    return StageReport(
        stage=report.stage,
        label=report.label,
        issues=(*report.issues, issue),
        changes=report.changes,
        losses=report.losses,
        metrics=metrics,
    )


def _execute_plan(data: list[dict], plan: MigrationPlan, policy: MigrationPolicy) -> StageReport:
    issues: list[Issue] = []
    changes: list[Change] = []
    losses: list[Loss] = []
    step_metrics = []

    for step_index, step in enumerate(plan.technosphere_steps):
        resource = _resource_for_step(step, load_technosphere_resources(plan.source.technosphere.system_model))
        step_report, step_losses = _apply_technosphere_step(data, resource, step, policy, step_index)
        translated = _translate_legacy_issues(step_report, policy, step.axis, step_index)
        issues.extend(translated)
        losses.extend(step_losses)
        losses.extend(_runtime_losses(step_report, step.axis, step_index))
        counts = _step_counts(step_report)
        step_metrics.append(_step_metrics(step, counts))
        changes.append(_step_change(step, counts, step_index))
        if any(issue.severity is Severity.ERROR for issue in translated):
            return _migration_stage(issues, (), losses, step_metrics, rolled_back=True)

    offset = len(plan.technosphere_steps)
    for axis_index, step in enumerate(plan.biosphere_steps):
        step_index = offset + axis_index
        resource = _resource_for_step(step, load_biosphere_resources())
        step_report, step_losses = _apply_biosphere_step(data, resource, step, policy, step_index)
        translated = _translate_legacy_issues(step_report, policy, step.axis, step_index)
        issues.extend(translated)
        losses.extend(step_losses)
        losses.extend(_runtime_losses(step_report, step.axis, step_index))
        counts = _step_counts(step_report)
        step_metrics.append(_step_metrics(step, counts))
        changes.append(_step_change(step, counts, step_index))
        if any(issue.severity is Severity.ERROR for issue in translated):
            return _migration_stage(issues, (), losses, step_metrics, rolled_back=True)

    return _migration_stage(issues, changes, losses, step_metrics, rolled_back=False)


def _resource_for_step(step: MigrationRouteStep, resources: Mapping[tuple[str, str], dict]) -> dict:
    pair = (
        (step.source_version, step.target_version)
        if step.direction == "forward"
        else (step.target_version, step.source_version)
    )
    return deepcopy(resources[pair])


def _apply_technosphere_step(
    data: list[dict],
    resource: dict,
    step: MigrationRouteStep,
    policy: MigrationPolicy,
    step_index: int,
) -> tuple[MigrationStepReport, list[Loss]]:
    report = _legacy_step_report(step)
    losses: list[Loss] = []

    if step.direction == "forward":
        replacements, factored_replacements, findings = _prepare_technosphere_replacements(
            data, resource.get("replace", []), step, policy, report, step_index
        )
        losses.extend(findings)
        if any(issue.severity == "error" for issue in report.issues):
            return report, losses
        _apply_replacements(data, replacements, "forward", report)
        _apply_factored_replacements(data, factored_replacements, "forward", report)

        disaggregations, factored_disaggregations, findings = _prepare_technosphere_disaggregations(
            data, resource.get("disaggregate", []), step, policy, report, step_index
        )
        losses.extend(findings)
        if any(issue.severity == "error" for issue in report.issues):
            return report, losses
        _apply_disaggregation(data, disaggregations, report)
        _apply_factored_disaggregation(data, factored_disaggregations, report)
    else:
        disaggregations, factored_disaggregations, findings = _prepare_technosphere_disaggregations(
            data, resource.get("disaggregate", []), step, policy, report, step_index
        )
        losses.extend(findings)
        if any(issue.severity == "error" for issue in report.issues):
            return report, losses
        _apply_aggregation(data, disaggregations, report)
        _apply_factored_aggregation(data, factored_disaggregations, report)

        replacements, factored_replacements, findings = _prepare_technosphere_replacements(
            data, resource.get("replace", []), step, policy, report, step_index
        )
        losses.extend(findings)
        if any(issue.severity == "error" for issue in report.issues):
            return report, losses
        _apply_replacements(data, replacements, "backward", report)
        _apply_factored_replacements(data, factored_replacements, "backward", report)
    return report, losses


def _apply_biosphere_step(
    data: list[dict],
    resource: dict,
    step: MigrationRouteStep,
    policy: MigrationPolicy,
    step_index: int,
) -> tuple[MigrationStepReport, list[Loss]]:
    report = _legacy_step_report(step)
    if step.direction == "forward":
        _delete_biosphere_exchanges(data, resource.get("delete", []), report)
    prepared = deepcopy(resource)
    if step.direction == "forward":
        prepared["delete"] = []
    replacements, factored_replacements, losses = _prepare_biosphere_replacements(
        data, resource.get("replace", []), step, policy, report, step_index
    )
    if any(issue.severity == "error" for issue in report.issues):
        return report, losses
    prepared["replace"] = replacements
    _apply_biosphere_rules(data, prepared, step.direction, report)
    _apply_factored_biosphere_replacements(data, factored_replacements, step.direction, report)
    return report, losses


def _prepare_technosphere_replacements(
    data: list[dict],
    rules: Sequence[dict],
    step: MigrationRouteStep,
    policy: MigrationPolicy,
    report: MigrationStepReport,
    step_index: int,
) -> tuple[list[dict], list[dict], list[Loss]]:
    match_side = "source" if step.direction == "forward" else "target"
    replacement_side = "target" if step.direction == "forward" else "source"
    safe_replacements = []
    factored_replacements = []
    losses: list[Loss] = []

    for rule in rules:
        matches = _matching_technosphere_entities(data, rule[match_side])
        unit_change = _unit_changes(rule[match_side], rule[replacement_side])
        if not matches or not unit_change:
            safe_replacements.append(rule)
            continue
        factor = _conversion_factor(rule, rule[replacement_side], reverse=step.direction == "backward")
        if factor is not None:
            factored_replacements.append(rule)
            continue
        _unsafe_unit_findings(matches, rule, step, policy, report, losses, step_index)
    return safe_replacements, factored_replacements, losses


def _prepare_technosphere_disaggregations(
    data: list[dict],
    rules: Sequence[dict],
    step: MigrationRouteStep,
    policy: MigrationPolicy,
    report: MigrationStepReport,
    step_index: int,
) -> tuple[list[dict], list[dict], list[Loss]]:
    safe_disaggregations = []
    factored_disaggregations = []
    losses: list[Loss] = []
    for rule in rules:
        matches = _matching_disaggregation_entities(data, rule, step.direction)
        targets = rule.get("targets", [])
        changed_targets = [target for target in targets if _unit_changes(rule["source"], target)]
        if not matches or not changed_targets:
            safe_disaggregations.append(rule)
            continue
        factors = [_conversion_factor(rule, target, reverse=step.direction == "backward") for target in changed_targets]
        if all(factor is not None for factor in factors):
            factored_disaggregations.append(rule)
            continue
        _unsafe_unit_findings(matches, rule, step, policy, report, losses, step_index)
    return safe_disaggregations, factored_disaggregations, losses


def _prepare_biosphere_replacements(
    data: list[dict],
    rules: Sequence[dict],
    step: MigrationRouteStep,
    policy: MigrationPolicy,
    report: MigrationStepReport,
    step_index: int,
) -> tuple[list[dict], list[dict], list[Loss]]:
    match_side = "source" if step.direction == "forward" else "target"
    replacement_side = "target" if step.direction == "forward" else "source"
    safe = []
    factored = []
    losses: list[Loss] = []
    for rule in rules:
        matches = _matching_biosphere_entities(data, rule[match_side])
        if not matches or not _unit_changes(rule[match_side], rule[replacement_side]):
            safe.append(rule)
            continue
        factor = _conversion_factor(rule, rule[replacement_side], reverse=step.direction == "backward")
        if factor is not None:
            factored.append(rule)
            continue
        _unsafe_unit_findings(matches, rule, step, policy, report, losses, step_index)
    return safe, factored, losses


def _unsafe_unit_findings(
    paths: Sequence[str],
    rule: dict,
    step: MigrationRouteStep,
    policy: MigrationPolicy,
    report: MigrationStepReport,
    losses: list[Loss],
    step_index: int,
) -> None:
    action = policy.on_unit_change_without_factor
    source_unit, target_unit = _rule_units(rule, step.direction)
    for path in paths:
        message = (
            f"Migration rule at {path} changes unit from {source_unit!r} to {target_unit!r} "
            "without an explicit numeric conversion factor; the unsafe rule was not applied."
        )
        report.issues.append(
            _legacy_issue(
                severity=_severity(action).value,
                code="migration_unit_change_without_factor",
                message=message,
                path=path,
            )
        )
        losses.append(
            Loss(
                code="migration.rule_skipped_unsafe_unit_change",
                message=message,
                stage=StageKind.BACKGROUND_MIGRATION,
                path=path,
                recoverable=True,
                details={
                    "axis": step.axis.value,
                    "resource": step.resource_name,
                    "step_index": step_index,
                    "source_unit": source_unit,
                    "target_unit": target_unit,
                    "policy_action": action.value,
                },
            )
        )


def _matching_technosphere_entities(data: list[dict], specification: dict) -> list[str]:
    matches = []
    for dataset_index, dataset in enumerate(data):
        if _technosphere_matches(dataset, specification):
            matches.append(f"datasets[{dataset_index}]")
        for exchange_index, exchange in enumerate(dataset.get("exchanges", [])):
            if exchange.get("type") in _TECHNOSPHERE_TYPES and _technosphere_matches(exchange, specification):
                matches.append(f"datasets[{dataset_index}].exchanges[{exchange_index}]")
    return matches


def _matching_disaggregation_entities(data: list[dict], rule: dict, direction: str) -> list[str]:
    matches = []
    specifications = [rule["source"]] if direction == "forward" else rule.get("targets", [])
    for dataset_index, dataset in enumerate(data):
        for exchange_index, exchange in enumerate(dataset.get("exchanges", [])):
            if exchange.get("type") != "technosphere":
                continue
            if any(_technosphere_matches(exchange, specification) for specification in specifications):
                matches.append(f"datasets[{dataset_index}].exchanges[{exchange_index}]")
    return matches


def _matching_biosphere_entities(data: list[dict], specification: dict) -> list[str]:
    matches = []
    for dataset_index, dataset in enumerate(data):
        for exchange_index, exchange in enumerate(dataset.get("exchanges", [])):
            if exchange.get("type") == "biosphere" and _biosphere_matches(exchange, specification):
                matches.append(f"datasets[{dataset_index}].exchanges[{exchange_index}]")
    return matches


def _apply_factored_replacements(
    data: list[dict], rules: list[dict], direction: str, report: MigrationStepReport
) -> None:
    match_side = "source" if direction == "forward" else "target"
    replacement_side = "target" if direction == "forward" else "source"
    reverse = direction == "backward"
    for dataset in data:
        for entity, is_dataset in [(dataset, True), *[(exchange, False) for exchange in dataset.get("exchanges", [])]]:
            if not is_dataset and entity.get("type") not in _TECHNOSPHERE_TYPES:
                continue
            matches = [rule for rule in rules if _technosphere_matches(entity, rule[match_side])]
            if not matches:
                continue
            rule = matches[-1]
            factor = _conversion_factor(rule, rule[replacement_side], reverse=reverse)
            _apply_amount_factor(entity, factor)
            _apply_technosphere_target_with_unit(entity, rule[replacement_side])
            if is_dataset:
                report.dataset_replacements += 1
            else:
                report.technosphere_replacements += 1


def _apply_factored_disaggregation(data: list[dict], rules: list[dict], report: MigrationStepReport) -> None:
    for dataset in data:
        converted = []
        for exchange in dataset.get("exchanges", []):
            if exchange.get("type") != "technosphere":
                converted.append(exchange)
                continue
            matching = next((rule for rule in reversed(rules) if _technosphere_matches(exchange, rule["source"])), None)
            if matching is None:
                converted.append(exchange)
                continue
            try:
                amount = float(exchange["amount"])
                allocations = [float(target.get("allocation", 1.0)) for target in matching["targets"]]
            except (KeyError, TypeError, ValueError):
                converted.append(exchange)
                report.issues.append(
                    _legacy_issue(
                        "error", "migration_disaggregation_invalid", "Invalid factored disaggregation amount."
                    )
                )
                continue
            for target, allocation in zip(matching["targets"], allocations, strict=True):
                migrated = deepcopy(exchange)
                factor = _conversion_factor(matching, target, reverse=False) or 1.0
                migrated["amount"] = amount * allocation * factor
                _apply_technosphere_target_with_unit(migrated, target)
                converted.append(migrated)
            report.technosphere_disaggregations += 1
        dataset["exchanges"] = converted


def _apply_factored_aggregation(data: list[dict], rules: list[dict], report: MigrationStepReport) -> None:
    for dataset in data:
        exchanges = dataset.get("exchanges", [])
        consumed: set[int] = set()
        replacements: dict[int, dict] = {}
        for rule in rules:
            matching = [
                index
                for index, exchange in enumerate(exchanges)
                if index not in consumed
                and exchange.get("type") == "technosphere"
                and any(_technosphere_matches(exchange, target) for target in rule.get("targets", []))
            ]
            if not matching:
                continue
            amount = 0.0
            valid = True
            for index in matching:
                exchange = exchanges[index]
                target = next(target for target in rule["targets"] if _technosphere_matches(exchange, target))
                factor = _conversion_factor(rule, target, reverse=False) or 1.0
                try:
                    amount += float(exchange["amount"]) / factor
                except (KeyError, TypeError, ValueError, ZeroDivisionError):
                    valid = False
                    break
            if not valid:
                report.issues.append(
                    _legacy_issue("error", "migration_aggregation_invalid", "Invalid factored aggregation amount.")
                )
                continue
            migrated = deepcopy(exchanges[matching[0]])
            migrated["amount"] = amount
            _apply_technosphere_target_with_unit(migrated, rule["source"])
            replacements[matching[0]] = migrated
            consumed.update(matching)
            report.technosphere_aggregations += 1
            report.issues.append(
                _legacy_issue(
                    "warning",
                    "migration_reverse_aggregation_lossy",
                    "Reverse migration aggregated exchanges whose individual metadata cannot be reconstructed exactly.",
                )
            )
        dataset["exchanges"] = [
            replacements[index] if index in replacements else exchange
            for index, exchange in enumerate(exchanges)
            if index not in consumed or index in replacements
        ]


def _apply_factored_biosphere_replacements(
    data: list[dict], rules: list[dict], direction: str, report: MigrationStepReport
) -> None:
    match_side = "source" if direction == "forward" else "target"
    replacement_side = "target" if direction == "forward" else "source"
    reverse = direction == "backward"
    for dataset in data:
        for exchange in dataset.get("exchanges", []):
            if exchange.get("type") != "biosphere":
                continue
            matches = [rule for rule in rules if _biosphere_matches(exchange, rule[match_side])]
            if not matches:
                continue
            rule = matches[0]
            factor = _conversion_factor(rule, rule[replacement_side], reverse=reverse)
            _apply_amount_factor(exchange, factor)
            _apply_biosphere_target_with_unit(exchange, rule[replacement_side])
            report.biosphere_replacements += 1


def _apply_technosphere_target_with_unit(entity: dict, target: dict) -> None:
    for field, value in target.items():
        if field in {"allocation", "comment", *_FACTOR_KEYS}:
            continue
        entity[field] = deepcopy(value)
    if "reference product" in target:
        entity["product"] = target["reference product"]
    if "unit" in target:
        entity["unit"] = _canonical_unit(target["unit"])
    entity.pop("input", None)


def _apply_biosphere_target_with_unit(exchange: dict, target: dict) -> None:
    for field, value in target.items():
        if field in {"allocation", "comment", *_FACTOR_KEYS}:
            continue
        exchange[field] = _canonical_unit(value) if field == "unit" else deepcopy(value)
    exchange.pop("input", None)


def _apply_amount_factor(entity: dict, factor: float | None) -> None:
    if factor is not None and "amount" in entity:
        entity["amount"] = float(entity["amount"]) * factor


def _conversion_factor(rule: Mapping, target: Mapping, *, reverse: bool) -> float | None:
    raw: Any = None
    nested = tuple(candidate for key in ("target", "source") if isinstance((candidate := rule.get(key)), Mapping))
    for container in (target, rule, *nested):
        for key in _FACTOR_KEYS:
            if key in container:
                raw = container[key]
                break
        if raw is not None:
            break
    if isinstance(raw, bool) or not isinstance(raw, (int, float)):
        return None
    factor = float(raw)
    if not math.isfinite(factor) or factor == 0:
        return None
    return 1.0 / factor if reverse else factor


def _unit_changes(source: Mapping, target: Mapping) -> bool:
    source_unit = source.get("unit")
    target_unit = target.get("unit")
    return bool(source_unit and target_unit and _canonical_unit(source_unit) != _canonical_unit(target_unit))


def _rule_units(rule: Mapping, direction: str) -> tuple[str, str]:
    if "target" in rule:
        source = rule["source"] if direction == "forward" else rule["target"]
        target = rule["target"] if direction == "forward" else rule["source"]
        return str(source.get("unit") or ""), str(target.get("unit") or "")
    targets = rule.get("targets", [])
    if direction == "forward":
        return str(rule["source"].get("unit") or ""), ", ".join(
            sorted({str(target.get("unit") or "") for target in targets})
        )
    return ", ".join(sorted({str(target.get("unit") or "") for target in targets})), str(
        rule["source"].get("unit") or ""
    )


def _legacy_step_report(step: MigrationRouteStep) -> MigrationStepReport:
    return MigrationStepReport(
        source_version=step.source_version,
        target_version=step.target_version,
        direction=step.direction,
        resource_name=step.resource_name,
    )


def _legacy_issue(severity: str, code: str, message: str, path: str = ""):
    from brightpath.models import Issue as LegacyIssue

    return LegacyIssue(severity=severity, code=code, message=message, path=path)


def _translate_legacy_issues(
    report: MigrationStepReport,
    policy: MigrationPolicy,
    axis: MigrationAxis,
    step_index: int,
) -> list[Issue]:
    translated = []
    for legacy in report.issues:
        action = _action_for_legacy_issue(legacy.code, policy)
        severity = _severity(action) if action is not None else Severity(legacy.severity)
        translated.append(
            Issue(
                severity=severity,
                code=_canonical_legacy_code(legacy.code),
                message=legacy.message,
                stage=StageKind.BACKGROUND_MIGRATION,
                path=legacy.path,
                details={"axis": axis.value, "step_index": step_index, "resource": report.resource_name},
                suggested_fix=legacy.suggested_fix,
            )
        )
    return translated


def _action_for_legacy_issue(code: str, policy: MigrationPolicy) -> PolicyAction | None:
    if "unit_change" in code:
        return policy.on_unit_change_without_factor
    if "ambiguous" in code:
        return policy.on_ambiguous_rule
    if "deletion" in code:
        return policy.on_deletion
    if "aggregation" in code or "allocation_not_unity" in code:
        return policy.on_information_loss
    return None


def _canonical_legacy_code(code: str) -> str:
    return code if code.startswith("migration.") else code.replace("migration_", "migration.", 1)


def _runtime_losses(report: MigrationStepReport, axis: MigrationAxis, step_index: int) -> list[Loss]:
    losses = []
    if report.technosphere_aggregations:
        losses.append(
            Loss(
                code="migration.reverse_aggregation",
                message="Reverse aggregation cannot reconstruct individual exchange metadata exactly.",
                stage=StageKind.BACKGROUND_MIGRATION,
                details={
                    "axis": axis.value,
                    "step_index": step_index,
                    "count": report.technosphere_aggregations,
                },
            )
        )
    if report.biosphere_deletions:
        losses.append(
            Loss(
                code="migration.biosphere_deletion",
                message="Matched biosphere exchanges were deleted by the migration resource.",
                stage=StageKind.BACKGROUND_MIGRATION,
                details={"axis": axis.value, "step_index": step_index, "count": report.biosphere_deletions},
            )
        )
    return losses


def _step_counts(report: MigrationStepReport) -> dict[str, int]:
    return {
        "dataset_replacements": report.dataset_replacements,
        "technosphere_replacements": report.technosphere_replacements,
        "technosphere_disaggregations": report.technosphere_disaggregations,
        "technosphere_aggregations": report.technosphere_aggregations,
        "biosphere_replacements": report.biosphere_replacements,
        "biosphere_deletions": report.biosphere_deletions,
    }


def _step_metrics(step: MigrationRouteStep, counts: dict[str, int]) -> dict[str, Any]:
    return {
        "axis": step.axis.value,
        "source_version": step.source_version,
        "target_version": step.target_version,
        "direction": step.direction,
        "resource": step.resource_name,
        "counts": counts,
    }


def _step_change(step: MigrationRouteStep, counts: dict[str, int], step_index: int) -> Change:
    transformed = sum(counts.values())
    return Change(
        code=f"migration.{step.axis.value}_step_applied",
        message=(
            f"Applied {step.axis.value} migration resource {step.resource_name!r} from "
            f"{step.source_version} to {step.target_version}; transformed {transformed} inventory item(s)."
        ),
        stage=StageKind.BACKGROUND_MIGRATION,
        path=f"background.{step.axis.value}.steps[{step_index}]",
        before=step.source_version,
        after=step.target_version,
        details={"direction": step.direction, "resource": step.resource_name, "counts": counts},
    )


def _migration_stage(
    issues: Sequence[Issue],
    changes: Sequence[Change],
    losses: Sequence[Loss],
    steps: Sequence[dict[str, Any]],
    *,
    rolled_back: bool,
) -> StageReport:
    return StageReport(
        stage=StageKind.BACKGROUND_MIGRATION,
        label="background migration application",
        issues=tuple(issues),
        changes=tuple(changes),
        losses=tuple(losses),
        metrics={"steps": list(steps), "rolled_back": rolled_back},
    )


def _rolled_back_stage(report: StageReport, reason: str) -> StageReport:
    metrics = _thaw(report.metrics)
    metrics["rolled_back"] = True
    metrics["rollback_reason"] = reason
    return StageReport(
        stage=report.stage,
        label=report.label,
        issues=report.issues,
        changes=(),
        losses=report.losses,
        metrics=metrics,
    )


def _replace_issue_severity(issue: Issue, action: PolicyAction) -> Issue:
    return Issue(
        severity=_severity(action),
        code=issue.code,
        message=issue.message,
        stage=issue.stage,
        path=issue.path,
        details=issue.details,
        suggested_fix=issue.suggested_fix,
    )


def _severity(action: PolicyAction) -> Severity:
    return {
        PolicyAction.ERROR: Severity.ERROR,
        PolicyAction.WARN: Severity.WARNING,
        PolicyAction.ALLOW: Severity.INFO,
    }[action]


def _result(
    value: InventoryDocument,
    source: BackgroundContext,
    target: BackgroundContext,
    policy: MigrationPolicy,
    stages: Sequence[StageReport],
    *,
    committed: bool,
) -> OperationResult[InventoryDocument]:
    return OperationResult(
        value=value,
        report=OperationReport(
            operation=OperationKind.MIGRATE,
            stages=tuple(stages),
            metadata={
                "source": _context_details(source),
                "target": _context_details(target),
                "policy": policy.to_dict(),
                "committed": committed,
            },
        ),
    )


def _context_details(context: BackgroundContext) -> dict[str, dict[str, str]]:
    return {
        "technosphere": {
            "family": context.technosphere.family,
            "version": context.technosphere.version,
            "system_model": context.technosphere.system_model,
        },
        "biosphere": {"family": context.biosphere.family, "version": context.biosphere.version},
    }


def _thaw(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {key: _thaw(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_thaw(item) for item in value]
    return value
