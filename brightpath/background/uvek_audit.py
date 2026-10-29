"""Independent-axis, exact-profile audits of native UVEK 2026 routes."""

from __future__ import annotations

import hashlib
from collections import Counter
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import asdict
from multiprocessing import get_context

from brightpath import DATA_DIR, __version__
from brightpath.background.audit import AUDIT_POLICY, audit_cases, digest
from brightpath.background.catalogs import PackageCatalogProvider
from brightpath.background.migration import plan_background_migration
from brightpath.background.uvek_export import identity, load_uvek_export_resource, migration_resource_fingerprint
from brightpath.core.context import BackgroundContext, BiosphereProfile, TechnosphereProfile
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.migrations.resources import load_uvek_release_resources

NATIVE_WARNING_POLICY = MigrationPolicy(
    on_inferred_reverse=PolicyAction.WARN,
    on_information_loss=PolicyAction.WARN,
    on_heuristic_mapping=PolicyAction.WARN,
)
POLICIES = {"strict": MigrationPolicy.strict(), "warnings": NATIVE_WARNING_POLICY}


def catalog_cases(provider, profile, axis):
    """Use every exact catalog identity, never inventory amounts or user projects."""
    if axis == "technosphere":
        fields = ("name", "reference product", "location", "unit")
        identities = provider.load_technosphere(profile).identities
    else:
        fields = ("name", "categories", "unit")
        identities = provider.load_biosphere(profile).identities
    return [
        {"id": axis + "-" + digest(key), "exchange": {**dict(zip(fields, key, strict=True)), "type": axis}}
        for key in sorted(identities)
    ]


def uvek_2026_tasks(provider):
    """Discover release bridges and incoming/outgoing cross-family audit tasks.

    Process exports probe the full approved 2025 export cohort reachable through
    the 2026 bridge. All other process routes and every biosphere route probe
    their complete exact source catalogs. Non-approved native export identities
    are recorded separately, not counted as executed or supported probes.
    """
    native_tech = TechnosphereProfile("uvek", "2026", "cutoff")
    native_bio = BiosphereProfile("uvek", "2026")
    legacy_tech = TechnosphereProfile("uvek", "2025", "cutoff")
    bridge = next(
        resource
        for resource in load_uvek_release_resources("technosphere")
        if resource["source_profiles"][0]["version"] == "2026"
    )
    approved_legacy = {identity(rule["source"]) for rule in load_uvek_export_resource()["replace"]}
    approved_native = {
        identity(rule["source"]) for rule in bridge["replace"] if identity(rule["target"]) in approved_legacy
    }
    all_native = catalog_cases(provider, native_tech, "technosphere")
    export_cases = [case for case in all_native if identity(case["exchange"]) in approved_native]
    excluded = [case for case in all_native if identity(case["exchange"]) not in approved_native]
    tasks = {}
    unavailable = []

    def add(axis, source_profile, target_profile, cases=None):
        if axis == "technosphere":
            source = BackgroundContext(source_profile, native_bio)
            target = BackgroundContext(target_profile, native_bio)
        else:
            source = BackgroundContext(native_tech, source_profile)
            target = BackgroundContext(native_tech, target_profile)
        if cases is None:
            cases = catalog_cases(provider, source_profile, axis)
        label = f"{axis}/{source_profile.label()}->{target_profile.label()}"
        for name, policy in POLICIES.items():
            tasks[f"{label}/{name}"] = (cases, source, target, policy)

    add("technosphere", native_tech, legacy_tech, all_native)
    add("technosphere", legacy_tech, native_tech)
    for profile in provider.technosphere_profiles():
        if profile.family != "ecoinvent":
            continue
        add("technosphere", profile, native_tech)
        # Discover outgoing support from the planner; unavailable consequential
        # exports remain explicit route-plan findings below, not supported claims.
        target = BackgroundContext(profile, native_bio)
        plan = plan_background_migration(BackgroundContext(native_tech, native_bio), target, AUDIT_POLICY)
        if plan.executable:
            add("technosphere", native_tech, profile, export_cases)
        else:
            unavailable.append(
                {
                    "source": asdict(native_tech),
                    "target": asdict(profile),
                    "issues": [issue.to_dict() for issue in plan.report.issues],
                }
            )
    biospheres = sorted(
        {
            BiosphereProfile("ecoinvent", profile.version)
            for profile in provider.technosphere_profiles()
            if profile.family == "ecoinvent"
        },
        key=lambda profile: profile.version,
    )
    for profile in biospheres:
        add("biosphere", native_bio, profile)
        add("biosphere", profile, native_bio)
    scope = {
        "native_process_identities": len(all_native),
        "approved_export_cohort": len(export_cases),
        "outside_export_cohort": len(excluded),
        "outside_export_cohort_sha256": digest(excluded),
        "unavailable_process_routes": unavailable,
        "process_export_scope": "Approved directional mappings only; other native suppliers remain unapproved.",
        "other_scope": "Complete exact source catalogs for release bridges, incoming process routes and biosphere routes.",
    }
    return tasks, scope


def _run_task(task):
    cases, source, target, policy = task
    result = audit_cases(cases, source, target, PackageCatalogProvider(), batch_size=128, policy=policy)
    result["policy"] = policy.to_dict()
    plan = plan_background_migration(source, target, policy)
    result["plan"] = {
        "executable": plan.executable,
        "technosphere_steps": [asdict(step) for step in plan.technosphere_steps],
        "biosphere_steps": [asdict(step) for step in plan.biosphere_steps],
        "report": plan.report.to_dict(),
    }
    return result


def run_uvek_2026_audit(*, progress=None, workers=1, route_names=None):
    """Execute discovered native routes under strict and explicit warning policies.

    ``route_names`` selects exact task names for iteration. Such reports are
    marked partial, and must not replace a complete release baseline.
    """
    if workers < 1:
        raise ValueError("workers must be positive.")
    tasks, scope = uvek_2026_tasks(PackageCatalogProvider())
    discovered = set(tasks)
    if route_names is not None:
        selected = set(route_names)
        if not selected or selected - discovered:
            raise ValueError("Selected audit routes must be nonempty discovered task names.")
        tasks = {name: task for name, task in tasks.items() if name in selected}
    routes = {}
    if workers == 1:
        for name, task in tasks.items():
            routes[name] = _run_task(task)
            if progress:
                progress(name, routes[name]["summary"])
    else:
        with ProcessPoolExecutor(max_workers=workers, mp_context=get_context("spawn")) as executor:
            futures = {executor.submit(_run_task, task): name for name, task in tasks.items()}
            for future in as_completed(futures):
                name = futures[future]
                routes[name] = future.result()
                if progress:
                    progress(name, routes[name]["summary"])
    routes = dict(sorted(routes.items()))
    baseline = {
        "schema_version": 1,
        "complete": set(routes) == discovered,
        "scope": scope,
        "policy": {name: policy.to_dict() for name, policy in POLICIES.items()},
        "resources": {
            relative: hashlib.sha256((DATA_DIR / relative).read_bytes()).hexdigest()
            for relative in (
                "migrations/RESOURCE_MANIFEST.json",
                "export/reference_catalogs/RESOURCE_MANIFEST.json",
                "schemas/migration-v3.schema.json",
            )
        },
        "routes": {name: result["summary"] for name, result in routes.items()},
    }
    return {
        "schema_version": 1,
        "brightpath_version": __version__,
        "migration_fingerprint": migration_resource_fingerprint(),
        "baseline": baseline,
        "routes": routes,
        "totals": dict(
            sorted(Counter(case["status"] for result in routes.values() for case in result["cases"]).items())
        ),
    }
