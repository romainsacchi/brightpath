"""Private reference-aware engine regressions; synthetic values, no format exports."""

import json
import math
from copy import deepcopy
from pathlib import Path

import pytest

from brightpath.background.catalogs import BiosphereCatalog, CatalogProvider, TechnosphereCatalog
from brightpath.background.execution import execute_background_migration
from brightpath.core.context import (
    BackgroundContext,
    BiosphereProfile,
    FormatProfile,
    InventoryContext,
    TechnosphereProfile,
)
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.migrations.engine import _apply_biosphere_rules, _same_uuid_reverse_biosphere_target
from brightpath.migrations.models import MigrationStepReport
from brightpath.models import InventoryDocument

FIXTURES = json.loads((Path(__file__).parent / "fixtures/reverse_biosphere_uuid_rules.json").read_text())
SODIUM = next(f for f in FIXTURES if f["source_identity"][0] == "Sodium I")
A = "723f56f9-c68e-5692-a969-0fb2a70bce20"
B = "5ac961f0-8e53-42ff-b13f-1a7fb9133868"


def identity(row):
    return row["name"], tuple(row["categories"]), row["unit"]


def fixture_values(fixture=SODIUM):
    n, c, u = fixture["source_identity"]
    exchange = {
        "type": "biosphere",
        "name": n,
        "categories": c,
        "unit": u,
        "amount": -2.5,
        "uncertainty type": 2,
        "loc": math.log(4.0),
        "scale": 0.3,
        "negative": True,
        "formula": "dose / 3",
        "comment": "Synthetic quantity fixture",
    }
    source = {identity(exchange): fixture["source_UUID"]}
    target = {identity(fixture["retained_destination"]): fixture["source_UUID"]}
    old = fixture["old_destination_identity"]
    old_key = old[0], tuple(old[1]), old[2]
    if old_key not in target and fixture["old_destination_UUID"]:
        target[old_key] = fixture["old_destination_UUID"]
    return exchange, source, target


def apply(exchange, rule, source, target, direction="backward"):
    data = [{"exchanges": [deepcopy(exchange)]}]
    report = MigrationStepReport("new", "old", direction, "private-unit-test")
    _apply_biosphere_rules(
        data,
        {"replace": [rule]},
        direction,
        report,
        target_biosphere_identities=frozenset(target),
        source_biosphere_references=source,
        target_biosphere_references=target,
    )
    return data[0]["exchanges"][0], report


@pytest.mark.parametrize("fixture", FIXTURES, ids=lambda f: f["source_identity"][0])
def test_all_seven_actual_rule_families_preserve_native_uuid_and_science(fixture):
    exchange, source, target = fixture_values(fixture)
    before = deepcopy(exchange)
    after, report = apply(exchange, fixture["rule"], source, target)
    for field, expected in fixture["retained_destination"].items():
        assert after[field] == expected
    for field in ("amount", "uncertainty type", "loc", "scale", "negative", "formula", "comment"):
        assert after[field] == before[field]
    assert exchange == before
    assert [i.code for i in report.issues] == ["migration_biosphere_source_uuid_preserved"]


@pytest.mark.parametrize("native_name", ["Sodium I", "Sodium, ion"])
def test_exact_target_metadata_name_may_differ(native_name):
    exchange, source, _ = fixture_values()
    target = {(native_name, ("water",), "kilogram"): B}
    after, _ = apply(exchange, SODIUM["rule"], source, target)
    assert after["name"] == native_name and after["uuid"] == B


@pytest.mark.parametrize("field", ["conversion_factor", "conversion factor", "amount_factor", "amount factor"])
@pytest.mark.parametrize("value", [2, 0.5, -1, 0, float("nan"), float("inf"), True])
def test_non1_or_invalid_factors_never_qualify(field, value):
    exchange, source, target = fixture_values()
    rule = deepcopy(SODIUM["rule"])
    rule[field] = value
    assert _same_uuid_reverse_biosphere_target(exchange, rule, source, target) is None


@pytest.mark.parametrize("container", ["rule", "source", "target"])
def test_explicit_factor_one_is_supported(container):
    exchange, source, target = fixture_values()
    rule = deepcopy(SODIUM["rule"])
    (rule if container == "rule" else rule[container])["conversion_factor"] = 1.0
    after, _ = apply(exchange, rule, source, target)
    assert after["uuid"] == B


@pytest.mark.parametrize(
    "problem",
    [
        "missing_source",
        "missing_target",
        "different_uuid",
        "different_categories",
        "different_unit",
        "ambiguous_target",
        "explicit_uuid_conflict",
    ],
)
def test_missing_conflicting_and_inexact_references_do_not_trigger_guard(problem):
    exchange, source, target = fixture_values()
    if problem == "missing_source":
        source = {}
    elif problem == "missing_target":
        target = {}
    elif problem == "different_uuid":
        target = {("Sodium, ion", ("water",), "kilogram"): A}
    elif problem == "different_categories":
        target = {("Sodium, ion", ("water", "ground-"), "kilogram"): B}
    elif problem == "different_unit":
        target = {("Sodium, ion", ("water",), "gram"): B}
    elif problem == "ambiguous_target":
        target[("Another name", ("water",), "kilogram")] = B
    else:
        exchange["uuid"] = A
    assert _same_uuid_reverse_biosphere_target(exchange, SODIUM["rule"], source, target) is None


def test_forward_rule_and_quantities_remain_identical_with_or_without_bindings():
    rule = deepcopy(SODIUM["rule"])
    exchange = {
        **rule["source"],
        "type": "biosphere",
        "amount": 1.7,
        "formula": "dose",
        "uncertainty type": 3,
        "loc": 1.7,
        "scale": 0.3,
    }
    old, _ = apply(exchange, rule, {}, {}, direction="forward")
    new, report = apply(exchange, rule, {identity(exchange): A}, {("Sodium I", ("water",), "kilogram"): B}, "forward")
    assert new == old and new["uuid"] == B
    assert not report.issues


def test_ambiguous_rule_path_is_unchanged():
    exchange, source, target = fixture_values()
    rules = {"replace": [deepcopy(SODIUM["rule"]), deepcopy(SODIUM["rule"])]}
    outcomes = []
    for source_refs, target_refs in [({}, {}), (source, target)]:
        data = [{"exchanges": [deepcopy(exchange)]}]
        report = MigrationStepReport("3.10", "3.9", "backward", "ambiguous-fixture")
        _apply_biosphere_rules(
            data,
            rules,
            "backward",
            report,
            target_biosphere_identities=frozenset(target),
            source_biosphere_references=source_refs,
            target_biosphere_references=target_refs,
        )
        outcomes.append((data, report))
    assert outcomes[0] == outcomes[1]


def test_existing_correct_same_uuid_rename_keeps_legacy_path():
    exchange, source, _ = fixture_values()
    rule = {
        "source": {"name": "Sodium, ion", "categories": ["water"], "unit": "kilogram", "uuid": B},
        "target": {"name": "Sodium I", "uuid": B},
    }
    target = {("Sodium, ion", ("water",), "kilogram"): B}
    assert _same_uuid_reverse_biosphere_target(exchange, rule, source, target) is None
    old, _ = apply(exchange, rule, {}, {})
    new, report = apply(exchange, rule, source, target)
    assert new == old and new["name"] == "Sodium, ion"
    assert not report.issues


def test_triangular_bounds_zero_nominal_and_formula_are_unchanged():
    exchange, source, target = fixture_values()
    for field in ("negative", "scale"):
        exchange.pop(field)
    exchange.update(amount=0, loc=0.1, minimum=0, maximum=0.3, **{"uncertainty type": 5})
    after, _ = apply(exchange, SODIUM["rule"], source, target)
    for field in ("amount", "loc", "minimum", "maximum", "formula", "uncertainty type"):
        assert after[field] == exchange[field]


@pytest.mark.parametrize("target_version", ["3.9", "3.8", "3.7", "3.6"])
def test_public_background_execution_uses_provider_bindings_without_mutating_caller(target_version):
    exchange, _source, _target = fixture_values()
    exchange.update(amount=2.5, negative=False, loc=math.log(2.5), formula="dose")
    activity = {
        "name": "Private consumer",
        "reference product": "Private product",
        "location": "GLO",
        "unit": "kilogram",
        "comment": "Synthetic public-pipeline fixture",
        "parameters": [{"name": "dose", "amount": 2.5}],
        "exchanges": [
            {
                "name": "Private consumer",
                "reference product": "Private product",
                "location": "GLO",
                "unit": "kilogram",
                "type": "production",
                "amount": 1,
            },
            exchange,
        ],
    }
    before = deepcopy(activity)

    class Provider(CatalogProvider):
        def technosphere_profiles(self):
            return frozenset({TechnosphereProfile("ecoinvent", "3.10", "cutoff")})

        def biosphere_profiles(self):
            return frozenset(BiosphereProfile("ecoinvent", v) for v in ("3.10", "3.9", "3.8", "3.7", "3.6"))

        def load_technosphere(self, profile):
            return TechnosphereCatalog(profile=profile, identities=frozenset())

        def load_biosphere(self, profile):
            name = "Sodium I" if profile.version in {"3.10", "3.9", "3.9.1"} else "Sodium, ion"
            refs = {(name, ("water",), "kilogram"): B}
            if profile.version != "3.10":
                refs[("Sodium", ("water",), "kilogram")] = A
            return BiosphereCatalog(profile=profile, identities=frozenset(refs), references=refs)

    source_context = BackgroundContext(
        TechnosphereProfile("ecoinvent", "3.10", "cutoff"), BiosphereProfile("ecoinvent", "3.10")
    )
    target_context = BackgroundContext(source_context.technosphere, BiosphereProfile("ecoinvent", target_version))
    document = InventoryDocument(
        data=[activity], context=InventoryContext(FormatProfile("brightway_excel"), source_context)
    )
    result = execute_background_migration(
        document, target_context, Provider(), MigrationPolicy(on_inferred_reverse=PolicyAction.WARN)
    )
    assert not result.report.has_errors
    after = result.value.data[0]["exchanges"][1]
    assert after["name"] == ("Sodium I" if target_version == "3.9" else "Sodium, ion") and after["uuid"] == B
    for field in ("amount", "loc", "scale", "formula", "negative"):
        assert after[field] == exchange[field]
    assert activity == before and document.data == [before]
    assert any("source_uuid_preserved" in issue.code for issue in result.report.issues)
