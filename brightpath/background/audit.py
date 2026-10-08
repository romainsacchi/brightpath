"""Read-only migration coverage probes and resource collision diagnostics."""

from __future__ import annotations

import hashlib
import json
import math
from collections import Counter, defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed
from copy import deepcopy
from dataclasses import asdict
from multiprocessing import get_context

from brightpath import DATA_DIR, __version__
from brightpath.background.catalogs import PackageCatalogProvider
from brightpath.background.execution import execute_background_migration
from brightpath.background.patches import compatibility_resource
from brightpath.background.uvek_export import load_uvek_export_resource, migration_resource_fingerprint
from brightpath.background.validation import validate_background_links
from brightpath.core.context import (
    BackgroundContext,
    BiosphereProfile,
    FormatProfile,
    InventoryContext,
    TechnosphereProfile,
)
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.core.reports import Severity
from brightpath.migrations.engine import _canonical_unit
from brightpath.migrations.resources import load_biosphere_resources, load_technosphere_resources
from brightpath.models import InventoryDocument

AUDIT_POLICY = MigrationPolicy(on_inferred_reverse=PolicyAction.WARN, on_information_loss=PolicyAction.WARN)


def digest(value):
    """Hash stable JSON rather than machine-specific paths or timestamps."""
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False, allow_nan=False).encode()).hexdigest()


def _identity(specification, axis):
    fields = (
        ("name", "reference product", "location", "unit") if axis == "technosphere" else ("name", "categories", "unit")
    )
    result = {field: specification.get(field, "") for field in fields}
    result["unit"] = _canonical_unit(result["unit"])
    return result


def resource_collisions(resources, axis, proxies=()):
    """Inventory overlapping match identities in both directions, without resolving them.

    Biosphere targets inherit omitted source fields, as the executor does. The
    scan intentionally includes UUID-independent matches and cross-verb overlaps.
    A collision is a review candidate, not proof of a runtime failure.
    """
    findings = []
    for (source_version, target_version), resource in sorted(resources.items()):
        for direction in ("forward", "backward"):
            groups = defaultdict(list)
            for verb in ("replace", "disaggregate", "delete"):
                for rule_index, rule in enumerate(resource.get(verb, ())):
                    if direction == "backward" and verb == "delete":
                        continue
                    targets = rule.get("targets", [rule.get("target", {})])
                    matches = [rule["source"]] if direction == "forward" else targets
                    for match in matches:
                        specification = {**rule["source"], **match} if axis == "biosphere" else match
                        key = json.dumps(_identity(specification, axis), sort_keys=True)
                        groups[key].append(
                            {
                                "verb": verb,
                                "rule_index": rule_index,
                                "rule_sha256": digest(rule),
                                "source": _identity(rule["source"], axis),
                                "targets": [
                                    _identity({**rule["source"], **target} if axis == "biosphere" else target, axis)
                                    for target in targets
                                ],
                                "reverse_preferred": rule.get("reverse_preferred") is True,
                            }
                        )
            for key, candidates in sorted(groups.items()):
                if len(candidates) < 2:
                    continue
                matched = json.loads(key)
                proxy = next(
                    (
                        rule
                        for rule in proxies
                        if direction == "backward"
                        and axis == "technosphere"
                        and (rule["source_version"], rule["target_version"]) == (target_version, source_version)
                        and _identity(rule["source"], axis) == matched
                    ),
                    None,
                )
                preferred = [candidate for candidate in candidates if candidate["reverse_preferred"]]
                disposition = "review_required"
                if proxy:
                    disposition = "explicit_proxy"
                elif (
                    direction == "backward"
                    and len(preferred) == 1
                    and all(candidate["verb"] == "replace" for candidate in candidates)
                ):
                    disposition = "explicit_preference"
                findings.append(
                    {
                        "axis": axis,
                        "resource": resource["name"],
                        "direction": direction,
                        "match": matched,
                        "disposition": disposition,
                        "proxy": proxy,
                        "candidates": candidates,
                    }
                )
    return findings


def _document(source, cases):
    data = []
    for case in cases:
        identity = {
            "name": f"Migration audit {case['id']}",
            "reference product": "audit output",
            "location": "GLO",
            "unit": "unit",
        }
        data.append(
            {
                **identity,
                "code": case["id"],
                "exchanges": [
                    {**identity, "type": "production", "amount": 1},
                    {**deepcopy(case["exchange"]), "amount": 2.0, "uncertainty type": 3, "loc": 2.0, "scale": 0.2},
                ],
            }
        )
    return InventoryDocument(data=data, context=InventoryContext(FormatProfile("brightway_excel"), source))


def _check_success(document, result, target, provider):
    if result.value.context != InventoryContext(document.context.format, target):
        raise ValueError("Successful migration has the wrong exact target context.")
    if validate_background_links(result.value.data, target, provider).has_errors:
        raise ValueError("Successful migration contains invalid target links.")
    migrated = {dataset["code"]: dataset for dataset in result.value.data}
    for dataset_index, original in enumerate(document.data):
        dataset = migrated[original["code"]]
        if dataset["exchanges"][0] != original["exchanges"][0]:
            raise ValueError("Migration changed the synthetic production exchange.")
        documented_omission = any(
            loss.code == "migration.biosphere_exchange_removed_unsafe_unit"
            and loss.path == f"datasets[{dataset_index}].exchanges[1]"
            for loss in result.report.losses
        )
        if len(dataset["exchanges"]) < 2 and not documented_omission:
            raise ValueError("Successful migration silently removed the probe exchange.")
    for dataset in result.value.data:
        for exchange in dataset["exchanges"]:
            for field in ("amount", "loc", "scale", "minimum", "maximum"):
                if field in exchange and not math.isfinite(float(exchange[field])):
                    raise ValueError(f"Migration produced a non-finite {field}.")
    repeated = execute_background_migration(result.value, target, provider, AUDIT_POLICY)
    if not repeated.succeeded or repeated.value.data != result.value.data:
        raise ValueError("Repeated same-context migration is not an unchanged successful no-op.")


def audit_cases(cases, source, target, provider, *, batch_size=1):
    """Execute every case, bisecting failed batches to isolate blocked identities.

    Successful batch warning/loss codes are reported at route level, never
    attributed to an individual flow. Exceptions and broken invariants are audit
    errors and cannot be accepted by a baseline.
    """
    if batch_size < 1:
        raise ValueError("batch_size must be positive.")
    cases = sorted(cases, key=lambda case: case["id"])
    if not cases or len({case["id"] for case in cases}) != len(cases):
        raise ValueError("Audit cases must be nonempty and have unique IDs.")
    outcomes = []
    warning_codes, loss_codes = set(), set()

    def run(batch):
        document = _document(source, batch)
        original = document.data
        try:
            result = execute_background_migration(document, target, provider, AUDIT_POLICY)
            if document.data != original or document.context.background != source:
                raise ValueError("Migration mutated its caller-owned source.")
            if not result.succeeded:
                if (
                    result.value.data != original
                    or result.value.context != document.context
                    or not result.report.has_errors
                ):
                    raise ValueError("Failed migration did not roll back with a structured error.")
                if len(batch) > 1:
                    midpoint = len(batch) // 2
                    run(batch[:midpoint])
                    run(batch[midpoint:])
                    return
                outcomes.append(
                    {
                        **batch[0],
                        "status": "blocked",
                        "errors": [
                            issue.to_dict() for issue in result.report.issues if issue.severity is Severity.ERROR
                        ],
                    }
                )
                return
            _check_success(document, result, target, provider)
            warning_codes.update(issue.code for issue in result.report.issues if issue.severity is Severity.WARNING)
            loss_codes.update(loss.code for loss in result.report.losses)
            migrated = {dataset["code"]: dataset for dataset in result.value.data}
            codes = {case["id"] for case in batch}
            helpers = [dataset for dataset in result.value.data if dataset["code"] not in codes]
            for case in batch:
                outcomes.append(
                    {
                        **case,
                        "status": "omitted_with_warning" if len(migrated[case["id"]]["exchanges"]) < 2 else "supported",
                        "output_sha256": digest([migrated[case["id"]], *helpers]),
                    }
                )
        except Exception as error:
            if len(batch) > 1:
                midpoint = len(batch) // 2
                run(batch[:midpoint])
                run(batch[midpoint:])
                return
            outcomes.extend(
                {**case, "status": "audit_error", "error": f"{type(error).__name__}: {error}"} for case in batch
            )

    for offset in range(0, len(cases), batch_size):
        run(cases[offset : offset + batch_size])
    outcomes.sort(key=lambda case: case["id"])
    errors = Counter(issue["code"] for case in outcomes for issue in case.get("errors", ()))
    return {
        "source": asdict(source),
        "target": asdict(target),
        "cases": outcomes,
        "summary": {
            "total": len(outcomes),
            "statuses": dict(sorted(Counter(case["status"] for case in outcomes).items())),
            "error_codes": dict(sorted(errors.items())),
            "warning_codes": sorted(warning_codes),
            "loss_codes": sorted(loss_codes),
            "cases_sha256": digest(outcomes),
        },
    }


def _run_audit_task(task):
    cases, source, target, batch_size = task
    return audit_cases(cases, source, target, PackageCatalogProvider(), batch_size=batch_size)


def run_migration_audit(*, progress=None, workers=1):
    """Audit approved UVEK suppliers and all UVEK source biosphere identities.

    Exact ecoinvent cut-off targets come from packaged catalogs, not a curated
    test list. Unavailable routes remain explicit blocked cases. No proprietary
    inventory coefficients or external database installation are needed.
    """
    if workers < 1:
        raise ValueError("workers must be positive.")
    provider = PackageCatalogProvider()
    resource = load_uvek_export_resource()
    source = BackgroundContext(TechnosphereProfile(**resource["source_profile"]), BiosphereProfile("ecoinvent", "3.10"))
    supplier_cases = [
        {"id": rule["id"], "exchange": {**rule["source"], "type": "technosphere"}} for rule in resource["rules"]
    ]
    biosphere_cases = [
        {
            "id": "biosphere-" + digest([name, categories, unit]),
            "exchange": {"name": name, "categories": list(categories), "unit": unit, "type": "biosphere"},
        }
        for name, categories, unit in sorted(provider.load_biosphere(source.biosphere).identities)
    ]
    targets = [
        profile
        for profile in provider.technosphere_profiles()
        if profile.family == "ecoinvent" and profile.system_model == "cutoff"
    ]
    if not targets:
        raise ValueError("No exact ecoinvent cut-off catalogs were discovered.")
    tasks = {}
    for profile in targets:
        target = BackgroundContext(profile, BiosphereProfile("ecoinvent", profile.version))
        for axis, cases, destination, batch_size in (
            ("uvek_suppliers", supplier_cases, target, 1),
            ("uvek_biosphere", biosphere_cases, BackgroundContext(source.technosphere, target.biosphere), 128),
        ):
            name = f"{axis}/ecoinvent-{profile.version}-cutoff"
            tasks[name] = (cases, source, destination, batch_size)
    routes = {}
    if workers == 1:
        for name, task in tasks.items():
            routes[name] = _run_audit_task(task)
            if progress:
                progress(name, routes[name]["summary"])
    else:
        with ProcessPoolExecutor(max_workers=workers, mp_context=get_context("spawn")) as executor:
            futures = {executor.submit(_run_audit_task, task): name for name, task in tasks.items()}
            for future in as_completed(futures):
                name = futures[future]
                routes[name] = future.result()
                if progress:
                    progress(name, routes[name]["summary"])
    routes = dict(sorted(routes.items()))
    collisions = resource_collisions(
        load_technosphere_resources("cutoff"), "technosphere", compatibility_resource()["reverse_proxies"]
    )
    collisions.extend(resource_collisions(load_biosphere_resources(), "biosphere"))
    baseline = {
        "schema_version": 1,
        "policy": AUDIT_POLICY.to_dict(),
        "resources": {
            relative: hashlib.sha256((DATA_DIR / relative).read_bytes()).hexdigest()
            for relative in ("migrations/RESOURCE_MANIFEST.json", "export/reference_catalogs/RESOURCE_MANIFEST.json")
        },
        "unresolved_uvek_suppliers": len(resource["unresolved"]),
        "routes": {name: result["summary"] for name, result in routes.items()},
        "collisions": {
            "count": len(collisions),
            "dispositions": dict(sorted(Counter(finding["disposition"] for finding in collisions).items())),
            "sha256": digest(collisions),
        },
    }
    return {
        "schema_version": 1,
        "brightpath_version": __version__,
        "migration_fingerprint": migration_resource_fingerprint(),
        "baseline": baseline,
        "routes": routes,
        "collisions": collisions,
    }


def compare_baseline(report, baseline):
    """Require explicit review of changed coverage, outputs, resources or collisions."""
    differences = []
    current = report["baseline"]
    if set(baseline) != set(current):
        differences.append("Baseline schema fields changed.")
    for field in sorted(set(current) - {"routes"}):
        if current[field] != baseline.get(field):
            differences.append(f"Baseline {field} changed.")
    previous_routes = baseline.get("routes", {})
    for route in sorted(set(current["routes"]) | set(previous_routes)):
        if current["routes"].get(route) != previous_routes.get(route):
            differences.append(f"Migration coverage or output changed: {route}.")
    for route, summary in current["routes"].items():
        if summary["statuses"].get("audit_error", 0):
            differences.append(f"Unacceptable audit errors: {route}.")
    return differences
