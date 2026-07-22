from __future__ import annotations

from collections import defaultdict, deque
from copy import deepcopy
from math import isclose

from brightpath.core.context import resolve_migration_series
from brightpath.exceptions import MigrationError, MigrationUnavailableError
from brightpath.models import BackgroundProfile, InventoryDocument, Issue
from brightpath.units import normalize_unit

from .models import MigrationReport, MigrationStepReport
from .resources import load_biosphere_resources, load_technosphere_resources

_IGNORED_TARGET_FIELDS = {"allocation", "comment", "unit"}
_TECHNOSPHERE_TYPES = {"production", "technosphere", "substitution"}


def resolve_migration_route(
    source_version: str,
    target_version: str,
    available: dict[tuple[str, str], dict],
) -> list[tuple[str, str, str]]:
    """Return the shortest deterministic route through forward and reverse edges."""

    if source_version == target_version:
        return []

    graph: dict[str, list[tuple[str, str]]] = defaultdict(list)
    for source, target in sorted(available):
        graph[source].append((target, "forward"))
        graph[target].append((source, "backward"))
    for neighbors in graph.values():
        neighbors.sort(key=lambda item: (_version_key(item[0]), item[1]))

    if source_version not in graph or target_version not in graph:
        known = ", ".join(sorted(graph, key=_version_key)) or "none"
        raise MigrationUnavailableError(
            f"No migration route from ecoinvent {source_version} to {target_version}; " f"known versions: {known}."
        )

    queue = deque([(source_version, [])])
    visited = {source_version}
    while queue:
        current, route = queue.popleft()
        for neighbor, direction in graph[current]:
            if neighbor in visited:
                continue
            next_route = [*route, (current, neighbor, direction)]
            if neighbor == target_version:
                return next_route
            visited.add(neighbor)
            queue.append((neighbor, next_route))

    raise MigrationUnavailableError(f"No migration route from ecoinvent {source_version} to {target_version}.")


def migrate_inventory(
    document: InventoryDocument,
    target_profile: BackgroundProfile,
) -> tuple[InventoryDocument, MigrationReport]:
    """Migrate background identities and return a new document plus an audit report."""

    source = document.background_profile.normalized()
    target = target_profile.normalized()
    _validate_profiles(source, target)

    report = MigrationReport(source_profile=source, target_profile=target)
    if source == target:
        migrated = document.replace(
            background_profile=target,
            migration_reports=(*document.migration_reports, report),
        )
        return migrated, report

    if source.family != target.family:
        raise MigrationUnavailableError(
            "The legacy migration engine does not execute cross-family routes; use the transactional "
            "background migration service for ecoinvent-to-UVEK conversion."
        )
    if source.family == "uvek":
        raise MigrationUnavailableError(
            f"No UVEK migration route is available from {source.version} to {target.version}."
        )
    if source.system_model != target.system_model:
        raise MigrationUnavailableError("Migration between ecoinvent system models is not supported.")
    if source.system_model != "cutoff":
        raise MigrationUnavailableError("Only ecoinvent cut-off migration resources are currently packaged.")

    resources = load_technosphere_resources(source.system_model)
    source_series = resolve_migration_series(source.family, source.version).migration_series
    target_series = resolve_migration_series(target.family, target.version).migration_series
    route = resolve_migration_route(source_series, target_series, resources)
    biosphere_resources = load_biosphere_resources()
    data = document.data

    for current, neighbor, direction in route:
        forward_pair = (current, neighbor) if direction == "forward" else (neighbor, current)
        resource = resources[forward_pair]
        step = MigrationStepReport(
            source_version=current,
            target_version=neighbor,
            direction=direction,
            resource_name=str(resource.get("name") or resource.get("_path") or ""),
        )

        if direction == "forward":
            _apply_replacements(data, resource.get("replace", []), "forward", step)
            _apply_disaggregation(data, resource.get("disaggregate", []), step)
        else:
            # Reverse the forward operation order: disaggregation is inverted before
            # replacements so target identities are still available for aggregation.
            _apply_aggregation(data, resource.get("disaggregate", []), step)
            _apply_replacements(data, resource.get("replace", []), "backward", step)

        biosphere_resource = biosphere_resources.get(forward_pair)
        if biosphere_resource is None:
            step.issues.append(
                Issue(
                    severity="warning",
                    code="biosphere_migration_missing",
                    message=(
                        "No biosphere migration resource is packaged for ecoinvent "
                        f"{forward_pair[0]} to {forward_pair[1]}; biosphere exchanges were "
                        "left unchanged for this step."
                    ),
                )
            )
        else:
            _apply_biosphere_rules(data, biosphere_resource, direction, step)
        report.steps.append(step)

    migrated = document.replace(
        data=data,
        background_profile=target,
        migration_reports=(*document.migration_reports, report),
    )
    return migrated, report


def _validate_profiles(source: BackgroundProfile, target: BackgroundProfile) -> None:
    for label, profile in (("source", source), ("target", target)):
        if not profile.is_complete:
            raise MigrationError(f"The {label} background profile must define family, version, and system model.")
        if profile.family not in {"ecoinvent", "uvek"}:
            raise MigrationUnavailableError(f"Unsupported {label} background family: {profile.family!r}.")


def _apply_replacements(
    data: list[dict],
    rules: list[dict],
    direction: str,
    report: MigrationStepReport,
) -> None:
    match_side = "source" if direction == "forward" else "target"
    replacement_side = "target" if direction == "forward" else "source"
    index = _build_rule_index(rules, match_side)

    for activity_index, activity in enumerate(data):
        rule = _select_technosphere_rule(
            activity,
            index.get(_identity(activity), []),
            match_side,
            report,
            path=f"activity[{activity_index}]",
        )
        if rule is not None:
            _record_unit_change(
                rule[match_side],
                rule[replacement_side],
                report,
                path=f"activity[{activity_index}]",
            )
            _apply_technosphere_target(activity, rule[replacement_side])
            report.dataset_replacements += 1

        for exchange_index, exchange in enumerate(activity.get("exchanges", [])):
            if exchange.get("type") not in _TECHNOSPHERE_TYPES:
                continue
            path = f"activity[{activity_index}].exchanges[{exchange_index}]"
            rule = _select_technosphere_rule(
                exchange,
                index.get(_identity(exchange), []),
                match_side,
                report,
                path=path,
            )
            if rule is None:
                continue
            _record_unit_change(
                rule[match_side],
                rule[replacement_side],
                report,
                path=path,
            )
            _apply_technosphere_target(exchange, rule[replacement_side])
            report.technosphere_replacements += 1


def _apply_disaggregation(
    data: list[dict],
    rules: list[dict],
    report: MigrationStepReport,
) -> None:
    index = _build_rule_index(rules, "source")
    for activity_index, activity in enumerate(data):
        converted = []
        for exchange_index, exchange in enumerate(activity.get("exchanges", [])):
            if exchange.get("type") != "technosphere":
                converted.append(exchange)
                continue

            path = f"activity[{activity_index}].exchanges[{exchange_index}]"
            rule = _select_technosphere_rule(
                exchange,
                index.get(_identity(exchange), []),
                "source",
                report,
                path=path,
            )
            if rule is None:
                converted.append(exchange)
                continue

            try:
                allocations = [float(target.get("allocation", 1.0)) for target in rule["targets"]]
                original_amount = float(exchange["amount"])
            except (KeyError, TypeError, ValueError) as exc:
                report.issues.append(
                    Issue(
                        severity="error",
                        code="migration_disaggregation_invalid",
                        message=f"Could not disaggregate exchange at {path}: {exc}.",
                        path=path,
                    )
                )
                converted.append(exchange)
                continue

            if not isclose(sum(allocations), 1.0, rel_tol=1e-9, abs_tol=1e-9):
                report.issues.append(
                    Issue(
                        severity="warning",
                        code="migration_allocation_not_unity",
                        message=(
                            f"Disaggregation allocations at {path} sum to {sum(allocations):g}, "
                            "so the total exchange amount changes."
                        ),
                        path=path,
                    )
                )

            for target, allocation in zip(rule["targets"], allocations, strict=True):
                new_exchange = deepcopy(exchange)
                _record_unit_change(rule["source"], target, report, path=path)
                _apply_technosphere_target(new_exchange, target)
                new_exchange["amount"] = original_amount * allocation
                converted.append(new_exchange)
            report.technosphere_disaggregations += 1
        activity["exchanges"] = converted


def _apply_aggregation(
    data: list[dict],
    rules: list[dict],
    report: MigrationStepReport,
) -> None:
    for activity_index, activity in enumerate(data):
        exchanges = activity.get("exchanges", [])
        consumed: set[int] = set()
        replacements: dict[int, dict] = {}

        for rule in rules:
            targets = rule.get("targets", [])
            matching = [
                index
                for index, exchange in enumerate(exchanges)
                if index not in consumed
                and exchange.get("type") == "technosphere"
                and any(_technosphere_matches(exchange, target) for target in targets)
            ]
            if not matching:
                continue

            path = f"activity[{activity_index}].exchanges[{matching[0]}]"
            try:
                amount = sum(float(exchanges[index]["amount"]) for index in matching)
            except (KeyError, TypeError, ValueError) as exc:
                report.issues.append(
                    Issue(
                        severity="error",
                        code="migration_aggregation_invalid",
                        message=f"Could not aggregate exchanges at {path}: {exc}.",
                        path=path,
                    )
                )
                continue

            missing_targets = [
                target
                for target in targets
                if not any(_technosphere_matches(exchanges[index], target) for index in matching)
            ]
            if missing_targets:
                report.issues.append(
                    Issue(
                        severity="warning",
                        code="migration_reverse_aggregation_partial",
                        message=(
                            f"Reverse aggregation at {path} found only part of the forward "
                            "disaggregation targets; the reconstructed amount may be incomplete."
                        ),
                        path=path,
                    )
                )

            new_exchange = deepcopy(exchanges[matching[0]])
            first_target = next(
                (target for target in targets if _technosphere_matches(new_exchange, target)),
                targets[0],
            )
            _record_unit_change(first_target, rule["source"], report, path=path)
            _apply_technosphere_target(new_exchange, rule["source"])
            new_exchange["amount"] = amount
            replacements[matching[0]] = new_exchange
            consumed.update(matching)
            report.technosphere_aggregations += 1
            report.issues.append(
                Issue(
                    severity="warning",
                    code="migration_reverse_aggregation_lossy",
                    message=(
                        f"Reverse migration aggregated {len(matching)} exchanges at {path}; "
                        "their individual metadata cannot be reconstructed exactly."
                    ),
                    path=path,
                )
            )

        activity["exchanges"] = [
            replacements[index] if index in replacements else exchange
            for index, exchange in enumerate(exchanges)
            if index not in consumed or index in replacements
        ]


def _apply_biosphere_rules(
    data: list[dict],
    resource: dict,
    direction: str,
    report: MigrationStepReport,
    *,
    target_biosphere_identities=frozenset(),
) -> None:
    if direction == "forward":
        _delete_biosphere_exchanges(data, resource.get("delete", []), report)
        match_side, replacement_side = "source", "target"
    else:
        match_side, replacement_side = "target", "source"
        if resource.get("delete"):
            report.issues.append(
                Issue(
                    severity="warning",
                    code="migration_biosphere_deletion_irreversible",
                    message=(
                        f"{len(resource['delete'])} biosphere deletion rules in "
                        f"{resource.get('name', 'this migration')} cannot be reversed."
                    ),
                )
            )

    rules_by_name: dict[str, list[dict]] = defaultdict(list)
    rules_without_name: list[dict] = []
    for rule in resource.get("replace", []):
        name = str(rule[match_side].get("name") or "")
        if name:
            rules_by_name[name].append(rule)
        else:
            rules_without_name.append(rule)

    for activity_index, activity in enumerate(data):
        for exchange_index, exchange in enumerate(activity.get("exchanges", [])):
            if exchange.get("type") != "biosphere":
                continue
            path = f"activity[{activity_index}].exchanges[{exchange_index}]"
            candidates = [
                *rules_by_name.get(str(exchange.get("name") or ""), []),
                *rules_without_name,
            ]
            matches = [rule for rule in candidates if _biosphere_matches(exchange, rule[match_side])]
            if not matches:
                continue
            if len(matches) > 1:
                # CLIC-style inventories identify elementary flows by name,
                # emission category, and unit. When that exact identity is
                # already valid in the target catalog, no UUID-specific
                # migration rule is needed (and choosing one would be wrong).
                if _biosphere_catalog_identity(exchange) in target_biosphere_identities:
                    continue
                target = _shared_catalog_target(
                    exchange,
                    matches,
                    replacement_side,
                    target_biosphere_identities,
                )
                if target is not None:
                    _record_unit_change(matches[0][match_side], target, report, path=path)
                    _apply_biosphere_target(exchange, target)
                    report.biosphere_replacements += 1
                    continue
                report.issues.append(
                    Issue(
                        severity="warning",
                        code="migration_biosphere_replacement_ambiguous",
                        message=(
                            f"Multiple biosphere migration rules match {path}; the first "
                            "packaged rule was applied deterministically."
                        ),
                        path=path,
                    )
                )
            rule = matches[0]
            _record_unit_change(
                rule[match_side],
                rule[replacement_side],
                report,
                path=path,
            )
            _apply_biosphere_target(exchange, rule[replacement_side])
            report.biosphere_replacements += 1


def _biosphere_catalog_identity(exchange: dict) -> tuple[str, tuple[str, ...], str]:
    raw_categories = exchange.get("categories", ())
    if isinstance(raw_categories, str):
        raw_categories = (raw_categories,)
    return (
        str(exchange.get("name") or ""),
        tuple(str(value) for value in raw_categories),
        str(exchange.get("unit") or ""),
    )


def _shared_catalog_target(
    exchange: dict,
    matches: list[dict],
    replacement_side: str,
    target_biosphere_identities,
) -> dict | None:
    """Return a UUID-free target when every candidate has one catalog identity."""

    targets = []
    for rule in matches:
        target = {key: value for key, value in rule[replacement_side].items() if key != "uuid"}
        candidate = dict(exchange)
        candidate.update(target)
        if _biosphere_catalog_identity(candidate) in target_biosphere_identities:
            targets.append(target)
    unique_targets = list({tuple(sorted(target.items())): target for target in targets}.values())
    if len(unique_targets) != 1:
        return None
    return unique_targets[0]


def _delete_biosphere_exchanges(
    data: list[dict],
    rules: list[dict],
    report: MigrationStepReport,
) -> None:
    for activity in data:
        retained = []
        for exchange in activity.get("exchanges", []):
            if exchange.get("type") == "biosphere" and any(
                _biosphere_matches(exchange, rule["source"]) for rule in rules
            ):
                report.biosphere_deletions += 1
            else:
                retained.append(exchange)
        activity["exchanges"] = retained


def _build_rule_index(rules: list[dict], side: str) -> dict[tuple[str, str, str], list[dict]]:
    index: dict[tuple[str, str, str], list[dict]] = defaultdict(list)
    for rule in rules:
        index[_rule_identity(rule[side])].append(rule)
    return index


def _select_technosphere_rule(
    entity: dict,
    candidates: list[dict],
    side: str,
    report: MigrationStepReport,
    *,
    path: str,
) -> dict | None:
    if not candidates:
        return None
    compatible = [rule for rule in candidates if _units_compatible(entity.get("unit"), rule[side].get("unit"))]
    if compatible:
        candidates = compatible
    if len(candidates) > 1:
        report.issues.append(
            Issue(
                severity="warning",
                code="migration_replacement_ambiguous",
                message=(
                    f"Multiple migration rules match {path}; the last packaged rule was " "applied deterministically."
                ),
                path=path,
            )
        )
    return candidates[-1]


def _apply_technosphere_target(entity: dict, target: dict) -> None:
    for field, value in target.items():
        if field in _IGNORED_TARGET_FIELDS:
            continue
        entity[field] = deepcopy(value)
    if "reference product" in target:
        entity["product"] = target["reference product"]
    entity.pop("input", None)


def _apply_biosphere_target(exchange: dict, target: dict) -> None:
    for field, value in target.items():
        if field in {"comment", "allocation"}:
            continue
        if field == "unit":
            exchange[field] = _canonical_unit(value)
        else:
            exchange[field] = deepcopy(value)
    exchange.pop("input", None)


def _record_unit_change(
    source: dict,
    target: dict,
    report: MigrationStepReport,
    *,
    path: str,
) -> None:
    source_unit = source.get("unit")
    target_unit = target.get("unit")
    if not source_unit or not target_unit:
        return
    if _canonical_unit(source_unit) == _canonical_unit(target_unit):
        return
    report.issues.append(
        Issue(
            severity="warning",
            code="migration_unit_changed_without_amount_conversion",
            message=(
                f"Migration rule at {path} changes unit from {source_unit!r} to "
                f"{target_unit!r} without an amount conversion factor; review this exchange."
            ),
            path=path,
        )
    )


def _technosphere_matches(entity: dict, specification: dict) -> bool:
    return _identity(entity) == _rule_identity(specification) and _units_compatible(
        entity.get("unit"), specification.get("unit")
    )


def _biosphere_matches(exchange: dict, specification: dict) -> bool:
    expected_name = specification.get("name")
    if expected_name and exchange.get("name") != expected_name:
        return False
    if not expected_name and specification.get("uuid"):
        actual_uuid = _exchange_uuid(exchange)
        if not actual_uuid or actual_uuid != specification["uuid"]:
            return False
    if not _units_compatible(exchange.get("unit"), specification.get("unit")):
        return False
    expected_categories = specification.get("categories")
    if expected_categories is not None and tuple(str(value) for value in exchange.get("categories", ())) != tuple(
        str(value) for value in expected_categories
    ):
        return False
    if expected_name and expected_categories is not None and specification.get("unit"):
        return True
    actual_uuid = _exchange_uuid(exchange)
    if actual_uuid and specification.get("uuid") and actual_uuid != specification["uuid"]:
        return False
    return bool(expected_name or specification.get("uuid"))


def _exchange_uuid(exchange: dict) -> str:
    if exchange.get("uuid"):
        return str(exchange["uuid"])
    if exchange.get("code"):
        return str(exchange["code"])
    input_key = exchange.get("input")
    if isinstance(input_key, (tuple, list)) and len(input_key) == 2:
        return str(input_key[1])
    return ""


def _identity(entity: dict) -> tuple[str, str, str]:
    return (
        str(entity.get("name") or ""),
        str(entity.get("reference product") or entity.get("product") or ""),
        str(entity.get("location") or ""),
    )


def _rule_identity(specification: dict) -> tuple[str, str, str]:
    return (
        str(specification.get("name") or ""),
        str(specification.get("reference product") or ""),
        str(specification.get("location") or ""),
    )


def _units_compatible(actual, expected) -> bool:
    if not expected or not actual:
        return True
    return _canonical_unit(actual) == _canonical_unit(expected)


def _canonical_unit(value) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        return ""
    try:
        return str(normalize_unit(normalized))
    except (KeyError, TypeError, ValueError):
        return normalized


def _version_key(version: str) -> tuple[int, ...]:
    try:
        return tuple(int(part) for part in version.split("."))
    except ValueError:
        return tuple()
