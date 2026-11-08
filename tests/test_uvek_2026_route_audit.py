"""Native-route audits must expose scope, policy and exact catalog identities."""

import json
import runpy
from pathlib import Path

import pytest

from brightpath.background import PackageCatalogProvider, uvek_audit
from brightpath.background.audit import digest
from brightpath.core.policies import MigrationPolicy, PolicyAction


@pytest.fixture(scope="module")
def discovery():
    return uvek_audit.uvek_2026_tasks(PackageCatalogProvider())


def test_all_supported_routes_have_strict_and_warning_tasks_with_independent_axes(discovery):
    tasks, scope = discovery
    assert scope["native_process_identities"] == 11947
    # The cohort includes 1,517 replacements and the separately reviewed omission.
    assert scope["approved_export_cohort"] == 1518
    assert scope["outside_export_cohort"] == 10429
    # The exact 3.10.1 consequential catalog is visible, with both cross-model directions refused.
    consequential = [
        p
        for p in PackageCatalogProvider().technosphere_profiles()
        if p.family == "ecoinvent" and p.system_model == "consequential"
    ]
    native = ("uvek", "2026", "cutoff")
    expected_refusals = {
        pair
        for p in consequential
        for pair in ((native, (p.family, p.version, p.system_model)), ((p.family, p.version, p.system_model), native))
    }
    observed_refusals = {
        tuple(
            tuple(route[role][field] for field in ("family", "version", "system_model"))
            for role in ("source", "target")
        )
        for route in scope["unavailable_process_routes"]
    }
    assert len(scope["unavailable_process_routes"]) == len(expected_refusals) == 16
    assert observed_refusals == expected_refusals
    assert all(
        route["source"]["system_model"] != route["target"]["system_model"]
        for route in scope["unavailable_process_routes"]
    )
    assert len(tasks) == 76
    provider = PackageCatalogProvider()
    for name, (cases, source, target, policy) in tasks.items():
        axis = name.split("/")[0]
        assert (source.biosphere == target.biosphere) == (axis == "technosphere")
        assert (source.technosphere == target.technosphere) == (axis == "biosphere")
        assert policy == (MigrationPolicy.strict() if name.endswith("/strict") else uvek_audit.NATIVE_WARNING_POLICY)
        assert cases and len({row["id"] for row in cases}) == len(cases)
        if source.technosphere.family == "uvek" and target.technosphere.family == "ecoinvent":
            assert target.technosphere.system_model == "cutoff"
            assert len(cases) == 1518
        else:
            profile = source.technosphere if axis == "technosphere" else source.biosphere
            expected = uvek_audit.catalog_cases(provider, profile, axis)
            assert cases == expected


def test_consequential_profiles_are_unavailable_and_exact_cutoff_patch_routes_remain_visible(discovery):
    tasks, scope = discovery
    sources = {
        source.technosphere
        for name, (_cases, source, _target, _policy) in tasks.items()
        if name.startswith("technosphere/") and source.technosphere.family == "ecoinvent"
    }
    available = {
        profile
        for profile in PackageCatalogProvider().technosphere_profiles()
        if profile.family == "ecoinvent" and profile.system_model == "cutoff"
    }
    assert sources == available
    assert {profile.version for profile in sources} >= {"3.9.1", "3.10.1"}
    assert all(profile.system_model == "cutoff" for profile in sources)
    assert any(route["source"]["system_model"] == "consequential" for route in scope["unavailable_process_routes"])


def test_report_determinism_and_partial_scope_are_explicit(monkeypatch, discovery):
    tasks, scope = discovery
    names = sorted(tasks)[:2]
    monkeypatch.setattr(uvek_audit, "uvek_2026_tasks", lambda _provider: (tasks, scope))

    def run(task):
        cases, _source, _target, policy = task
        return {
            "cases": [{"id": case["id"], "status": "blocked"} for case in cases],
            "summary": {"statuses": {"blocked": len(cases)}, "cases_sha256": digest(cases)},
            "policy": policy.to_dict(),
        }

    monkeypatch.setattr(uvek_audit, "_run_task", run)
    report = uvek_audit.run_uvek_2026_audit(route_names=names)
    assert not report["baseline"]["complete"]
    assert report == uvek_audit.run_uvek_2026_audit(route_names=list(reversed(names)))
    assert set(report["routes"]) == set(names)
    assert report["totals"]["blocked"] == sum(len(tasks[name][0]) for name in names)
    for invalid in ([], ["unknown route"]):
        with pytest.raises(ValueError):
            uvek_audit.run_uvek_2026_audit(route_names=invalid)
    with pytest.raises(ValueError):
        uvek_audit.run_uvek_2026_audit(workers=0)
    fingerprints = iter(("before", "after"))
    monkeypatch.setattr(uvek_audit, "migration_resource_fingerprint", lambda: next(fingerprints))
    with pytest.raises(RuntimeError, match="changed during the native-route audit"):
        uvek_audit.run_uvek_2026_audit(route_names=names)


def test_runtime_task_honors_the_selected_information_loss_policy(discovery):
    from brightpath.migrations.resources import load_uvek_release_resources

    tasks, _scope = discovery
    resource = next(
        row for row in load_uvek_release_resources("biosphere") if row["source_profiles"][0]["family"] == "uvek"
    )
    rule = next(row for row in resource["replace"] if row.get("biosphere_information_loss"))
    for policy in (MigrationPolicy.strict(), MigrationPolicy(on_information_loss=PolicyAction.WARN)):
        _cases, source, target, _policy = next(
            task
            for name, task in tasks.items()
            if name.startswith("biosphere/")
            and task[1].biosphere.family == "uvek"
            and task[2].biosphere.version == "3.10"
        )
        case = {"id": "coarsening", "exchange": {**rule["source"], "type": "biosphere"}}
        result = uvek_audit._run_task(([case], source, target, policy))
        expected = "blocked" if policy.on_information_loss is PolicyAction.ERROR else "supported"
        assert result["summary"]["statuses"] == {expected: 1}
        assert result["policy"] == policy.to_dict()
        assert result["plan"]["biosphere_steps"]
        assert result["plan"]["technosphere_steps"] == []


def test_native_route_audit_includes_and_accepts_the_reviewed_biowaste_omission(discovery):
    tasks, _scope = discovery
    for name, (cases, source, target, policy) in tasks.items():
        if (
            not name.startswith("technosphere/")
            or target.technosphere.family != "ecoinvent"
            or target.technosphere.version != "3.12"
        ):
            continue
        case = next(case for case in cases if case["exchange"]["name"] == "Biowaste, at collection point")
        result = uvek_audit._run_task(([case], source, target, policy))
        assert result["summary"]["statuses"] == {"omitted_with_warning": 1}
        assert result["cases"][0]["status"] == "omitted_with_warning"


def test_partial_audit_cannot_be_accepted_even_against_a_matching_baseline(tmp_path, monkeypatch):
    main = runpy.run_path(str(Path(__file__).resolve().parents[1] / "scripts/audit_uvek_2026_routes.py"))["main"]
    baseline = {"complete": False, "routes": {}}
    monkeypatch.setitem(main.__globals__, "run_uvek_2026_audit", lambda **_kwargs: {"baseline": baseline})
    baseline_path = tmp_path / "baseline.json"
    baseline_path.write_text(json.dumps(baseline))
    output = tmp_path / "report.json"
    assert main(["--baseline", str(baseline_path), "--output", str(output)]) == 1
    assert (
        "Partial native-route audit cannot pass the release gate."
        in json.loads(output.read_text())["gate"]["differences"]
    )
