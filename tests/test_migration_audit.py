import json
import runpy
from copy import deepcopy
from dataclasses import replace
from pathlib import Path

import pytest

from brightpath.background import (
    BiosphereCatalog,
    InMemoryCatalogProvider,
    PackageCatalogProvider,
    TechnosphereCatalog,
    audit,
)
from brightpath.background import execution as execution_module
from brightpath.background.patches import compatibility_resource
from brightpath.background.uvek_export import load_uvek_export_resource
from brightpath.core.context import BackgroundContext, BiosphereProfile, TechnosphereProfile
from brightpath.migrations.resources import load_biosphere_resources


def context(version):
    return BackgroundContext(
        TechnosphereProfile("ecoinvent", version, "cutoff"), BiosphereProfile("ecoinvent", version)
    )


def case(name):
    return {"id": name, "exchange": {"name": name, "categories": ["air"], "unit": "kilogram", "type": "biosphere"}}


@pytest.fixture
def provider():
    return InMemoryCatalogProvider(
        technosphere=[TechnosphereCatalog(context(version).technosphere, ()) for version in ("3.11", "3.12")],
        biosphere=[
            BiosphereCatalog(context(version).biosphere, {(name, ("air",), "kilogram") for name in names})
            for version, names in (("3.11", ("retained", "removed")), ("3.12", ("retained",)))
        ],
    )


def test_batched_audit_isolates_failures_and_preserves_inputs(provider):
    cases = [case("retained"), case("removed")]
    original = deepcopy(cases)
    result = audit.audit_cases(cases, context("3.11"), context("3.12"), provider, batch_size=128)
    statuses = {entry["id"]: entry["status"] for entry in result["cases"]}
    assert statuses == {"removed": "blocked", "retained": "supported"}
    assert result["summary"]["total"] == 2
    assert result["summary"]["error_codes"]
    assert cases == original
    assert result == audit.audit_cases(
        list(reversed(cases)), context("3.11"), context("3.12"), provider, batch_size=128
    )


@pytest.mark.parametrize(
    "cases,batch_size", [([], 1), ([case("retained"), case("retained")], 1), ([case("retained")], 0)]
)
def test_empty_duplicate_or_invalid_batch_is_rejected(cases, batch_size, provider):
    with pytest.raises(ValueError):
        audit.audit_cases(cases, context("3.11"), context("3.12"), provider, batch_size=batch_size)


@pytest.mark.parametrize(
    "failure", ["exception", "wrong_context", "changed_production", "dropped_exchange", "rollback"]
)
def test_engine_errors_are_never_classified_as_expected_blockers(monkeypatch, provider, failure):
    execute = audit.execute_background_migration

    def broken(document, target, catalogs, policy):
        if failure == "exception":
            raise RuntimeError("engine failure")
        result = execute(document, target, catalogs, policy)
        if failure == "wrong_context":
            return replace(result, value=result.value.replace(context=document.context))
        data = result.value.data
        if failure == "changed_production":
            data[0]["exchanges"][0]["amount"] = 7
        elif failure == "dropped_exchange":
            data[0]["exchanges"] = data[0]["exchanges"][:1]
        else:
            data[0]["exchanges"][1]["amount"] = 17
        return replace(result, value=result.value.replace(data=data))

    monkeypatch.setattr(audit, "execute_background_migration", broken)
    result = audit.audit_cases(
        [case("removed" if failure == "rollback" else "retained")], context("3.11"), context("3.12"), provider
    )
    assert result["summary"]["statuses"] == {"audit_error": 1}


def test_aluminium_regression_is_detected_without_loosening_ambiguity_policy(monkeypatch):
    source = BackgroundContext(TechnosphereProfile("uvek", "2025", "cutoff"), BiosphereProfile("ecoinvent", "3.10"))
    rule = next(
        rule
        for rule in load_uvek_export_resource()["rules"]
        if rule["source"]["name"] == "Aluminium, primary, at plant" and rule["source"]["location"] == "RER"
    )
    cases = [{"id": rule["id"], "exchange": {**rule["source"], "type": "technosphere"}}]
    provider = PackageCatalogProvider()
    fixed = audit.audit_cases(cases, source, context("3.11"), provider)
    assert fixed["summary"]["statuses"] == {"supported": 1}
    assert "migration.reverse_proxy" in fixed["summary"]["loss_codes"]
    resource = deepcopy(compatibility_resource())
    resource["reverse_proxies"] = [
        rule for rule in resource["reverse_proxies"] if "aluminium" not in rule["source"]["name"]
    ]
    monkeypatch.setattr(execution_module, "compatibility_resource", lambda: resource)
    unfixed = audit.audit_cases(cases, source, context("3.11"), provider)
    assert unfixed["summary"]["statuses"] == {"blocked": 1}
    assert "migration.replacement_ambiguous" in unfixed["summary"]["error_codes"]


def test_recipe_helpers_are_included_in_audited_output(monkeypatch):
    source = BackgroundContext(TechnosphereProfile("uvek", "2025", "cutoff"), BiosphereProfile("ecoinvent", "3.10"))
    rule = next(rule for rule in load_uvek_export_resource()["rules"] if "recipe" in rule)
    fingerprints = []
    original_digest = audit.digest

    def capture(value):
        if isinstance(value, list) and value and isinstance(value[0], dict) and "exchanges" in value[0]:
            fingerprints.append(deepcopy(value))
        return original_digest(value)

    monkeypatch.setattr(audit, "digest", capture)
    result = audit.audit_cases(
        [{"id": rule["id"], "exchange": {**rule["source"], "type": "technosphere"}}],
        source,
        context("3.12"),
        PackageCatalogProvider(),
    )
    assert result["summary"]["statuses"] == {"supported": 1}
    assert "migration.documented_proxy" in result["summary"]["loss_codes"]
    assert len(fingerprints) == 1
    assert len(fingerprints[0]) > 1
    assert all(dataset.get("brightpath_conversion") for dataset in fingerprints[0][1:])


def test_collisions_include_forward_cross_verb_and_reverse_many_to_one():
    old = {"name": "production", "reference product": "metal", "location": "RER", "unit": "kg"}
    imported = {**old, "name": "import"}
    new = {**old, "location": "Europe", "unit": "kilogram"}
    resource = {
        "name": "merge",
        "replace": [{"source": old, "target": new}, {"source": imported, "target": new}],
        "disaggregate": [{"source": old, "targets": [new]}],
    }
    findings = audit.resource_collisions({("3.11", "3.12"): resource}, "technosphere")
    assert {finding["direction"] for finding in findings} == {"forward", "backward"}
    assert all(finding["disposition"] == "review_required" for finding in findings)
    reverse = next(finding for finding in findings if finding["direction"] == "backward")
    assert len(reverse["candidates"]) == 3
    assert reverse["match"]["unit"] == "kilogram"


def test_collisions_report_explicit_preferences_and_proxies():
    old = {"name": "production", "reference product": "metal", "location": "RER", "unit": "kilogram"}
    new = {**old, "location": "Europe"}
    resource = {
        "name": "merge",
        "replace": [
            {"source": old, "target": new, "reverse_preferred": True},
            {"source": {**old, "name": "import"}, "target": new},
        ],
    }
    resources = {("3.11", "3.12"): resource}
    assert audit.resource_collisions(resources, "technosphere")[0]["disposition"] == "explicit_preference"
    proxy = {
        "source_version": "3.12",
        "target_version": "3.11",
        "source": new,
        "target": old,
        "rationale": "Retain production",
        "evidence": "review",
    }
    assert audit.resource_collisions(resources, "technosphere", [proxy])[0]["disposition"] == "explicit_proxy"


def test_biosphere_collision_targets_inherit_compartments_and_units():
    source = {"name": "old", "categories": ["air"], "unit": "kilogram", "uuid": "old-id"}
    resource = {
        "name": "merge",
        "replace": [
            {"source": source, "target": {"name": "new", "uuid": "new-id"}},
            {"source": {**source, "name": "other", "uuid": "other-id"}, "target": {"name": "new", "uuid": "new-id"}},
        ],
    }
    finding = audit.resource_collisions({("3.11", "3.12"): resource}, "biosphere")[0]
    assert finding["match"] == {"name": "new", "categories": ["air"], "unit": "kilogram"}
    assert finding["direction"] == "backward"


@pytest.mark.parametrize("change", ["output", "new_route", "removed_route", "collision", "policy", "resources"])
def test_gate_rejects_any_unreviewed_coverage_or_output_change(change):
    baseline = {
        "schema_version": 1,
        "routes": {"route": {"statuses": {"supported": 1}, "cases_sha256": "before"}},
        "collisions": {"sha256": "before"},
        "policy": {},
        "resources": {},
    }
    current = deepcopy(baseline)
    if change == "output":
        current["routes"]["route"]["cases_sha256"] = "after"
    elif change == "new_route":
        current["routes"]["new"] = current["routes"]["route"]
    elif change == "removed_route":
        current["routes"].clear()
    elif change == "collision":
        current["collisions"]["sha256"] = "after"
    else:
        current[change]["changed"] = True
    assert audit.compare_baseline({"baseline": baseline}, baseline) == []
    assert audit.compare_baseline({"baseline": current}, baseline)


def test_audit_errors_cannot_be_blessed_in_a_baseline():
    baseline = {"schema_version": 1, "routes": {"route": {"statuses": {"audit_error": 1}}}}
    assert audit.compare_baseline({"baseline": baseline}, baseline) == ["Unacceptable audit errors: route."]


def test_documented_biosphere_omission_is_distinct_from_supported_or_silent_loss():
    source, target = context("3.9"), context("3.10")
    rule = next(
        rule
        for rule in load_biosphere_resources()[("3.9", "3.10")]["replace"]
        if rule["source"]["name"] == "Manganese-55"
    )
    flow = {field: rule["source"][field] for field in ("name", "categories", "unit")}
    provider = InMemoryCatalogProvider(
        technosphere=[TechnosphereCatalog(profile.technosphere, ()) for profile in (source, target)],
        biosphere=[
            BiosphereCatalog(source.biosphere, {(flow["name"], tuple(flow["categories"]), flow["unit"])}),
            BiosphereCatalog(target.biosphere, ()),
        ],
    )
    result = audit.audit_cases(
        [{"id": "omission", "exchange": {**flow, "type": "biosphere"}}], source, target, provider
    )
    assert result["summary"]["statuses"] == {"omitted_with_warning": 1}
    assert "migration.biosphere_exchange_removed_unsafe_unit" in result["summary"]["loss_codes"]


@pytest.mark.parametrize("baseline_matches", [True, False, None])
def test_cli_writes_diagnostics_and_enforces_exit_status(tmp_path, monkeypatch, baseline_matches):
    main = runpy.run_path(str(Path(__file__).resolve().parents[1] / "scripts/audit_migration_coverage.py"))["main"]
    baseline = {"schema_version": 1, "routes": {"example": {"statuses": {"supported": 1}, "cases_sha256": "original"}}}
    report = {"baseline": baseline, "routes": {}}
    monkeypatch.setitem(main.__globals__, "run_migration_audit", lambda **kwargs: deepcopy(report))
    output = tmp_path / "nested" / "audit.json"
    arguments = ["--output", str(output)]
    if baseline_matches is not None:
        expected = deepcopy(baseline)
        if not baseline_matches:
            expected["routes"]["example"]["cases_sha256"] = "changed"
        baseline_path = tmp_path / "baseline.json"
        baseline_path.write_text(json.dumps(expected), encoding="utf-8")
        arguments.extend(["--baseline", str(baseline_path)])
    assert main(arguments) == (0 if baseline_matches else 1)
    saved = json.loads(output.read_text(encoding="utf-8"))
    assert saved["gate"]["passed"] is bool(baseline_matches)
    assert saved["baseline"] == baseline


def test_parallel_worker_uses_the_same_probe_contract(provider, monkeypatch):
    monkeypatch.setattr(audit, "PackageCatalogProvider", lambda: provider)
    task = ([case("retained")], context("3.11"), context("3.12"), 128)
    assert audit._run_audit_task(task) == audit.audit_cases(*task[:3], provider, batch_size=task[3])


def test_full_audit_discovers_all_targets_and_does_not_sample_suppliers_or_flows(monkeypatch):
    observed = []

    def record(cases, source, target, provider, *, batch_size):
        observed.append((cases, source, target))
        return {"summary": {"statuses": {"supported": len(cases)}}}

    monkeypatch.setattr(audit, "audit_cases", record)
    report = audit.run_migration_audit()
    provider = PackageCatalogProvider()
    profiles = [
        profile
        for profile in provider.technosphere_profiles()
        if profile.family == "ecoinvent" and profile.system_model == "cutoff"
    ]
    rules = load_uvek_export_resource()["rules"]
    assert len(observed) == len(profiles) * 2 == len(report["routes"])
    for cases, source, target in observed:
        if target.technosphere.family == "ecoinvent":
            assert {entry["id"] for entry in cases} == {rule["id"] for rule in rules}
        else:
            assert len(cases) == len(provider.load_biosphere(source.biosphere).identities)
