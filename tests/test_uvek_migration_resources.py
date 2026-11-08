import json
import math

import pytest

from brightpath import DATA_DIR
from brightpath.background import BiosphereCatalog, InMemoryCatalogProvider, TechnosphereCatalog
from brightpath.background.execution import execute_background_migration
from brightpath.background.migration import plan_background_migration
from brightpath.background.reference_signs import select_profile_sign_rules
from brightpath.core import (
    BackgroundContext,
    BiosphereProfile,
    FormatProfile,
    InventoryContext,
    MigrationPolicy,
    TechnosphereProfile,
)
from brightpath.core.policies import PolicyAction
from brightpath.migrations.canonical_biosphere import select_canonical_biosphere_rules
from brightpath.migrations.engine import _canonical_unit
from brightpath.migrations.resources import load_uvek_biosphere_resource, load_uvek_technosphere_resource
from brightpath.models import InventoryDocument

CATALOG_DIRECTORY = DATA_DIR / "export" / "reference_catalogs"
VERSIONS = ("3.6", "3.7", "3.8", "3.9", "3.10", "3.11", "3.12")
SYSTEM_MODELS = ("cutoff", "consequential")
HEURISTIC_POLICY = MigrationPolicy(on_heuristic_mapping=PolicyAction.WARN)


def _catalog(version: str, system_model: str = "cutoff") -> dict:
    path = CATALOG_DIRECTORY / f"ecoinvent__{version}__{system_model}.json"
    return json.loads(path.read_text(encoding="utf-8"))


def _technosphere_identity(row: dict) -> tuple[str, str, str, str]:
    return row["name"], row["reference_product"], row["location"], row["unit"]


def _technosphere_rule_identity(row: dict) -> tuple[str, str, str, str]:
    return row["name"], row["reference product"], row["location"], row["unit"]


def _biosphere_identity(row: dict) -> tuple[str, tuple[str, ...], str]:
    return row["name"], tuple(row["categories"]), row["unit"]


def test_uvek_resources_cover_every_packaged_ecoinvent_identity_with_targets_or_omissions():
    technosphere = load_uvek_technosphere_resource()
    biosphere = load_uvek_biosphere_resource()
    source_technosphere = set()
    source_biosphere = set()
    for version in VERSIONS:
        for system_model in SYSTEM_MODELS:
            catalog = _catalog(version, system_model)
            if system_model == "cutoff":
                source_technosphere.update(_technosphere_identity(row) for row in catalog["technosphere"])
            source_biosphere.update(_biosphere_identity(row) for row in catalog["biosphere"])

    uvek = json.loads((CATALOG_DIRECTORY / "uvek__2025__cutoff.json").read_text(encoding="utf-8"))
    target_technosphere = {_technosphere_identity(row) for row in uvek["technosphere"]}
    target_biosphere = {_biosphere_identity(row) for row in _catalog("3.10")["biosphere"]}
    mapped_technosphere_sources = {_technosphere_rule_identity(rule["source"]) for rule in technosphere["replace"]}
    mapped_technosphere_targets = {
        _technosphere_rule_identity(rule["target"])
        for rule in technosphere["replace"]
        if "recipe_ref" not in rule["target"]
    }
    omitted_sources = {_technosphere_rule_identity(rule["source"]) for rule in technosphere["delete"]}
    mapped_biosphere_sources = {_biosphere_identity(rule["source"]) for rule in biosphere["replace"]}
    unresolved_biosphere_sources = {_biosphere_identity(rule["source"]) for rule in biosphere["unresolved"]}
    mapped_biosphere_targets = {_biosphere_identity(rule["target"]) for rule in biosphere["replace"]}

    assert mapped_technosphere_sources | omitted_sources == source_technosphere
    assert mapped_technosphere_sources.isdisjoint(omitted_sources)
    assert len(omitted_sources) == 257 and all(rule["approved_omission"] for rule in technosphere["delete"])
    assert mapped_technosphere_targets <= target_technosphere
    assert mapped_biosphere_sources | unresolved_biosphere_sources == source_biosphere
    assert mapped_biosphere_sources.isdisjoint(unresolved_biosphere_sources)
    assert len(mapped_biosphere_sources) == 5390
    assert len(unresolved_biosphere_sources) == 6080
    assert mapped_biosphere_targets <= target_biosphere
    assert all(0 <= rule["confidence"] <= 1 for rule in (*technosphere["replace"], *biosphere["replace"]))
    assert all(
        "recipe_ref" in rule["target"]
        or _canonical_unit(rule["source"]["unit"]) == _canonical_unit(rule["target"]["unit"])
        or (
            isinstance(rule.get("conversion_factor"), (int, float))
            and not isinstance(rule.get("conversion_factor"), bool)
            and math.isfinite(float(rule["conversion_factor"]))
            and rule["conversion_factor"] != 0
        )
        for rule in technosphere["replace"]
    )


def test_uvek_plan_and_execution_migrate_both_axes_transactionally():
    source = BackgroundContext(
        technosphere=TechnosphereProfile("ecoinvent", "3.11", "cutoff"),
        biosphere=BiosphereProfile("ecoinvent", "3.11"),
    )
    target = BackgroundContext(
        technosphere=TechnosphereProfile("uvek", "2025", "cutoff"),
        biosphere=BiosphereProfile("ecoinvent", "3.10"),
    )
    source_technosphere = {_technosphere_identity(row) for row in _catalog("3.11")["technosphere"]}
    source_biosphere = {_biosphere_identity(row) for row in _catalog("3.11")["biosphere"]}
    technosphere_rule = next(
        rule
        for rule in load_uvek_technosphere_resource()["replace"]
        if _technosphere_rule_identity(rule["source"]) in source_technosphere
        and _canonical_unit(rule["source"]["unit"]) == _canonical_unit(rule["target"]["unit"])
    )
    biosphere_rule = next(
        rule
        for rule in select_canonical_biosphere_rules(load_uvek_biosphere_resource(), "3.11")["replace"]
        if _biosphere_identity(rule["source"]) in source_biosphere and rule["source"] != rule["target"]
    )
    source_technosphere_identity = _technosphere_rule_identity(technosphere_rule["source"])
    target_technosphere_identity = _technosphere_rule_identity(technosphere_rule["target"])
    source_biosphere_identity = _biosphere_identity(biosphere_rule["source"])
    target_biosphere_identity = _biosphere_identity(biosphere_rule["target"])
    document = InventoryDocument(
        data=[
            {
                "name": "foreground process",
                "reference product": "foreground product",
                "location": "CH",
                "unit": "unit",
                "exchanges": [
                    {
                        "name": source_technosphere_identity[0],
                        "reference product": source_technosphere_identity[1],
                        "location": source_technosphere_identity[2],
                        "unit": source_technosphere_identity[3],
                        "amount": 2.0,
                        "type": "technosphere",
                    },
                    {
                        "name": source_biosphere_identity[0],
                        "categories": list(source_biosphere_identity[1]),
                        "unit": source_biosphere_identity[2],
                        "amount": 1.0,
                        "type": "biosphere",
                    },
                ],
            }
        ],
        context=InventoryContext(format=FormatProfile("brightway_excel"), background=source),
    )
    provider = InMemoryCatalogProvider(
        technosphere=[
            TechnosphereCatalog(source.technosphere, {source_technosphere_identity}),
            TechnosphereCatalog(target.technosphere, {target_technosphere_identity}),
        ],
        biosphere=[
            BiosphereCatalog(source.biosphere, {source_biosphere_identity}),
            BiosphereCatalog(target.biosphere, {target_biosphere_identity}),
        ],
    )

    plan = plan_background_migration(source, target, HEURISTIC_POLICY)
    result = execute_background_migration(document, target, provider, HEURISTIC_POLICY)

    assert plan.executable
    assert [(step.axis.value, step.direction) for step in plan.steps] == [
        ("technosphere", "forward"),
        ("biosphere", "forward"),
    ]
    assert {issue.code for issue in plan.report.issues} == {"migration.heuristic_mapping"}
    assert result.succeeded
    assert result.lossy
    assert result.value.context.background == target
    assert _technosphere_rule_identity(result.value.data[0]["exchanges"][0]) == target_technosphere_identity
    assert _biosphere_identity(result.value.data[0]["exchanges"][1]) == target_biosphere_identity
    assert document.context.background == source


def test_every_executable_forward_rule_runs_from_a_real_supported_source_catalog():
    """Exercise approved dispositions and account explicitly for blocked sign reviews."""
    from copy import deepcopy

    from brightpath.background import PackageCatalogProvider

    resource = load_uvek_technosphere_resource()
    provider = PackageCatalogProvider()
    blocked = [
        rule
        for rule in resource["replace"]
        if rule.get("reference_flow_sign_review", {}).get("status") == "unresolved"
        and not (
            rule["reference_flow_sign_review"].get("profiles")
            and all(entry["review"]["status"] == "verified" for entry in rule["reference_flow_sign_review"]["profiles"])
        )
    ]
    assert len(blocked) == 208
    blocked_ids = {rule["id"] for rule in blocked}
    assert not {"bp-7e9a1ca32be10996", "bp-3a513f3458239f8b"} & blocked_ids
    remaining = {
        _technosphere_rule_identity(rule["source"]): rule
        for rule in resource["replace"]
        if rule["id"] not in blocked_ids
    }
    profiles = sorted(
        (TechnosphereProfile(**profile) for profile in resource["source_profiles"]),
        key=lambda profile: (tuple(int(part) for part in profile.version.split(".")), profile.system_model == "cutoff"),
        reverse=True,
    )
    target = BackgroundContext(TechnosphereProfile("uvek", "2025", "cutoff"), BiosphereProfile("ecoinvent", "3.10"))
    target_biosphere = provider.load_biosphere(target.biosphere).identities
    checked = 0
    for profile in profiles:
        available = provider.load_technosphere(profile).identities
        candidates = [remaining[key] for key in sorted(available & remaining.keys())]
        selected_resource = select_profile_sign_rules({"replace": candidates}, profile)
        # A declared patch context does not approve other rules' missing exact
        # decisions. Leave those sources for an inspected base context below;
        # the final remaining/checked assertions still require every rule.
        selected = [
            r
            for r in selected_resource["replace"]
            if r.get("reference_flow_sign_review", {}).get("status") != "unresolved"
        ]
        eligible = {_technosphere_rule_identity(r["source"]) for r in [*selected, *selected_resource["delete"]]}
        inputs = [remaining.pop(key) for key in sorted(eligible)]
        if not inputs:
            continue
        source = BackgroundContext(profile, BiosphereProfile("ecoinvent", profile.version))
        name, categories, unit = min(provider.load_biosphere(source.biosphere).identities & target_biosphere)
        output = {
            "name": f"Schema test {profile.version} {profile.system_model}",
            "reference product": "schema test output",
            "location": "GLO",
            "unit": "unit",
        }
        data = [
            {
                **output,
                "exchanges": [
                    {**output, "type": "production", "amount": 1},
                    *[
                        {
                            **rule["source"],
                            "type": "technosphere",
                            "amount": 2,
                            "uncertainty type": 3,
                            "loc": 2,
                            "scale": 0.1,
                            "schema_rule_id": rule["id"],
                        }
                        for rule in inputs
                    ],
                    {"name": name, "categories": list(categories), "unit": unit, "type": "biosphere", "amount": 1},
                ],
            }
        ]
        original = deepcopy(data)
        document = InventoryDocument(data=data, context=InventoryContext(FormatProfile("brightway_excel"), source))
        result = execute_background_migration(
            document,
            target,
            provider,
            MigrationPolicy(on_heuristic_mapping=PolicyAction.WARN, on_information_loss=PolicyAction.WARN),
        )
        assert result.succeeded, (profile, [issue.to_dict() for issue in result.report.issues])
        assert data == original == document.data
        assert result.value.context.background == target
        assert result.value.data[0]["exchanges"][0] == original[0]["exchanges"][0]
        converted = result.value.data[0]["exchanges"][1:-1]
        assert len(converted) == len(selected)
        omissions = [loss for loss in result.report.losses if loss.code == "migration.technosphere_omission"]
        assert len(omissions) == len(selected_resource["delete"])
        for exchange, rule in zip(converted, selected, strict=True):
            if "recipe_ref" in rule["target"]:
                helper = next(h for h in result.value.data[1:] if h["code"] == exchange["input"][1])
                assert helper["brightpath_conversion"]["recipe_id"] == rule["target"]["recipe_ref"]
                assert _technosphere_rule_identity(exchange) == _technosphere_rule_identity(helper)
            else:
                assert _technosphere_rule_identity(exchange)[:3] == _technosphere_rule_identity(rule["target"])[:3]
                assert _canonical_unit(exchange["unit"]) == _canonical_unit(rule["target"]["unit"])
            assert exchange["schema_rule_id"] == rule["id"]
            factor = rule.get("conversion_factor", 1)
            assert exchange["amount"] == pytest.approx(2 * factor)
            assert exchange["loc"] == pytest.approx(2 * factor)
            assert exchange["scale"] == pytest.approx(0.1 * abs(factor))
        assert result.report.stages[-1].metrics["technosphere"]["coverage"] == 1
        assert result.report.stages[-1].metrics["biosphere"]["coverage"] == 1
        checked += len(inputs)
    assert not remaining
    assert checked == 30267
    assert checked + len(blocked) + len(resource["delete"]) == resource["coverage"]["mapped_identities"] == 30732


def _factored_document(uncertainty):
    from brightpath.background import PackageCatalogProvider

    source = BackgroundContext(
        TechnosphereProfile("ecoinvent", "3.12", "cutoff"), BiosphereProfile("ecoinvent", "3.12")
    )
    target = BackgroundContext(TechnosphereProfile("uvek", "2025", "cutoff"), BiosphereProfile("ecoinvent", "3.10"))
    provider = PackageCatalogProvider()
    catalog = provider.load_technosphere(source.technosphere).identities
    rule = next(
        rule
        for rule in load_uvek_technosphere_resource()["replace"]
        if rule.get("conversion_factor", 1) != 1 and _technosphere_rule_identity(rule["source"]) in catalog
    )
    data = [
        {
            "name": "Synthetic foreground",
            "reference product": "service",
            "location": "GLO",
            "unit": "unit",
            "exchanges": [{**rule["source"], "type": "technosphere", "amount": 2, **uncertainty}],
        }
    ]
    return (
        InventoryDocument(data=data, context=InventoryContext(FormatProfile("brightway_excel"), source)),
        target,
        provider,
        float(rule["conversion_factor"]),
    )


@pytest.mark.parametrize("distribution", [0, 1, 2, 3, 4])
def test_forward_unit_factors_scale_uncertainty_and_formulas_consistently(distribution):
    uncertainty = {"uncertainty type": distribution, "minimum": 1, "maximum": 3, "formula": "input_amount"}
    if distribution != 4:
        uncertainty["loc"] = math.log(2) if distribution == 2 else 2
    if distribution in (2, 3):
        uncertainty["scale"] = 0.2
    original, target, provider, factor = _factored_document(uncertainty)
    before = original.data
    result = execute_background_migration(original, target, provider, HEURISTIC_POLICY)
    assert result.succeeded
    assert original.data == before
    exchange = result.value.data[0]["exchanges"][0]
    assert exchange["amount"] == pytest.approx(2 * factor)
    assert exchange["minimum"] == pytest.approx(min(factor, 3 * factor))
    assert exchange["maximum"] == pytest.approx(max(factor, 3 * factor))
    assert exchange["formula"] == f"(input_amount) * ({factor!r})"
    if "loc" in uncertainty:
        expected = math.log(2) + math.log(abs(factor)) if distribution == 2 else 2 * factor
        assert exchange["loc"] == pytest.approx(expected)
    if distribution in (2, 3):
        assert exchange["scale"] == pytest.approx(0.2 if distribution == 2 else 0.2 * abs(factor))


@pytest.mark.parametrize("policy", [HEURISTIC_POLICY, MigrationPolicy.permissive()])
@pytest.mark.parametrize("uncertainty", [{"uncertainty type": 8}, {"uncertainty type": 3, "loc": "invalid"}])
def test_unsafe_forward_uncertainty_scaling_rolls_back_under_every_policy(policy, uncertainty):
    original, target, provider, _ = _factored_document(uncertainty)
    before = original.data
    result = execute_background_migration(original, target, provider, policy)
    assert not result.succeeded
    assert result.value is original
    assert original.data == before
    # Planning losses remain visible; failed execution must commit no changes.
    assert not result.report.changes
    assert result.report.metadata["committed"] is False
    assert "migration.amount_scaling_invalid" in {issue.code for issue in result.report.issues}
