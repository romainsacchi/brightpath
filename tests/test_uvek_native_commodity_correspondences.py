"""Preserve commodity identity, source uncertainty and explicit UVEK consent."""

import json
import math
from collections import defaultdict

import pytest

from brightpath import DATA_DIR, BackgroundProfile
from brightpath.background import PackageCatalogProvider
from brightpath.background.execution import execute_background_migration
from brightpath.background.migration import plan_background_migration
from brightpath.core import (
    BackgroundContext,
    BiosphereProfile,
    FormatProfile,
    InventoryContext,
    MigrationPolicy,
    TechnosphereProfile,
)
from brightpath.core.policies import PolicyAction
from brightpath.migrations.resources import load_uvek_technosphere_resource
from brightpath.models import InventoryDocument
from brightpath.profiles import simapro

IDS = ["bp-be4b125b435b18ca", "bp-834f96388a124aa3", "bp-dbd744f486420f56"]
EXPECTED = {
    "bp-be4b125b435b18ca": {
        "name": "Sheet rolling, copper",
        "reference product": "Sheet rolling, copper",
        "location": "RER",
        "unit": "kilogram",
    },
    "bp-834f96388a124aa3": {
        "name": "Sheet rolling, steel",
        "reference product": "Sheet rolling, steel",
        "location": "RER",
        "unit": "kilogram",
    },
    "bp-dbd744f486420f56": {
        "name": "Silica sand, at plant",
        "reference product": "Silica sand, at plant",
        "location": "DE",
        "unit": "kilogram",
    },
}
SOURCE = BackgroundContext(TechnosphereProfile("ecoinvent", "3.12", "cutoff"), BiosphereProfile("ecoinvent", "3.12"))
TARGETS = {
    "2025": BackgroundContext(TechnosphereProfile("uvek", "2025", "cutoff"), BiosphereProfile("ecoinvent", "3.10")),
    "2026": BackgroundContext(TechnosphereProfile("uvek", "2026", "cutoff"), BiosphereProfile("uvek", "2026")),
}


@pytest.fixture(scope="module")
def catalog_provider():
    return PackageCatalogProvider()


@pytest.fixture(scope="module")
def candidate_resource():
    # Exercise the normal manifest-backed production loader.
    resource = load_uvek_technosphere_resource()
    assert {r["id"]: r["target"] for r in resource["replace"] if r["id"] in IDS} == EXPECTED
    before = json.dumps(resource, sort_keys=True)
    yield resource
    assert json.dumps(resource, sort_keys=True) == before


@pytest.fixture
def candidate(candidate_resource):
    return candidate_resource


def document(rule):
    process = {
        "name": "Private supplier correction regression",
        "reference product": "Synthetic service",
        "location": "GLO",
        "unit": "unit",
    }
    exchange = {
        **rule["source"],
        "type": "technosphere",
        "amount": -2.5,
        "formula": "dose",
        "uncertainty type": 2,
        "loc": math.log(1.5),
        "scale": 0.3,
        "negative": True,
        "minimum": -4.0,
        "maximum": -1.0,
        "custom provenance": "Private scalar fixture only.",
    }
    return InventoryDocument(
        data=[
            {
                **process,
                "comment": "Synthetic migration fixture, never published or serialized.",
                "parameters": [{"name": "dose", "amount": -2.5}],
                "exchanges": [{**process, "type": "production", "amount": 1.0}, exchange],
            }
        ],
        context=InventoryContext(FormatProfile("brightway_excel"), SOURCE),
    )


@pytest.mark.parametrize("rule_id", IDS)
@pytest.mark.parametrize("version", ["2025", "2026"])
def test_consent_selects_exact_supplier_preserving_type2_and_original_profile(
    candidate, catalog_provider, rule_id, version
):
    rule = next(r for r in candidate["replace"] if r["id"] == rule_id)
    original = document(rule)
    before, context = original.data, original.context
    policy = MigrationPolicy(on_heuristic_mapping=PolicyAction.WARN)
    result = execute_background_migration(original, TARGETS[version], catalog_provider, policy)
    assert result.succeeded
    assert result.lossy  # Market/regional-producer limits remain explicit.
    assert result.value.context.background == TARGETS[version]
    assert original.context == context and original.context.background == SOURCE
    assert original.data == before
    exchange = result.value.data[0]["exchanges"][1]
    assert {k: exchange[k] for k in ["name", "reference product", "location", "unit"]} == rule["target"]
    assert all(
        exchange[k] == value
        for k, value in before[0]["exchanges"][1].items()
        if k not in ["name", "reference product", "location", "unit"]
    )
    assert result.value.data[0]["parameters"] == before[0]["parameters"]
    assert "migration.heuristic_mapping" in {issue.code for issue in result.report.issues}


@pytest.mark.parametrize("rule_id", IDS)
@pytest.mark.parametrize("version", ["2025", "2026"])
def test_without_consent_refuses_and_commits_no_changes(candidate, catalog_provider, rule_id, version):
    rule = next(r for r in candidate["replace"] if r["id"] == rule_id)
    original = document(rule)
    before, context = original.data, original.context
    strict = MigrationPolicy.strict()
    plan = plan_background_migration(SOURCE, TARGETS[version], strict)
    assert not plan.executable
    result = execute_background_migration(original, TARGETS[version], catalog_provider, strict)
    assert not result.succeeded and result.value is original
    assert original.data == before and original.context == context
    assert "migration.heuristic_mapping" in {issue.code for issue in result.report.issues}


@pytest.mark.parametrize("rule_id", IDS)
def test_native2025_metadata_has_exactly_one_distinct_provider_product_pair(candidate_resource, rule_id):
    target = next(r["target"] for r in candidate_resource["replace"] if r["id"] == rule_id)
    references = json.loads(
        (DATA_DIR / "export/openlca_references/uvek__2025__cutoff__ecoinvent__3.10.json").read_text()
    )
    pairs = []
    for chosen_id in IDS:
        chosen = next(r["target"] for r in candidate_resource["replace"] if r["id"] == chosen_id)
        matches = [
            r
            for r in references["technosphere"]
            if (r["name"], r["reference_product"], r["location"], r["unit"])
            == tuple(chosen[k] for k in ["name", "reference product", "location", "unit"])
        ]
        assert len(matches) == 1
        row = matches[0]
        assert row["unit_name"] == "kg" and row["flow_property_name"] == "Mass"
        pairs.append((row["process_id"], row["flow_id"]))
    assert len(set(pairs)) == len({p[0] for p in pairs}) == len({p[1] for p in pairs}) == 3
    assert target["unit"] == "kilogram"


def test_display_table_has_no_native_wire_collision(candidate_resource, catalog_provider):
    native = catalog_provider.load_technosphere(TARGETS["2025"].technosphere)
    groups = defaultdict(set)
    for name, product, location, unit in native.identities:
        wire = simapro.format_simapro_technosphere_name(
            name=name,
            reference_product=product,
            location=location,
            unit=unit,
            profile=BackgroundProfile("uvek", "2025", "cutoff"),
        )
        groups[(wire.casefold(), simapro._canonical_unit(unit))].add((name, product, location, unit))
    assert not [values for values in groups.values() if len(values) > 1]
    for rule in candidate_resource["replace"]:
        if rule["id"] not in IDS:
            continue
        target = rule["target"]
        wire = simapro.format_simapro_technosphere_name(
            **dict(
                name=target["name"],
                reference_product=target["reference product"],
                location=target["location"],
                unit=target["unit"],
                profile=BackgroundProfile("uvek", "2025", "cutoff"),
            )
        )
        assert simapro.parse_simapro_technosphere_name(
            wire,
            unit="kg",
            profile=BackgroundProfile("uvek", "2025", "cutoff"),
            technosphere_identities=native.identities,
        ) == (target["name"], target["reference product"], target["location"])


def test_resource_retains_explicit_heuristic_boundaries(candidate_resource):
    assert candidate_resource["quality"] == "heuristic"
    assert candidate_resource["reverse_policy"] == "explicit_only"
    assert candidate_resource["target_profile"] == {"family": "uvek", "version": "2025", "system_model": "cutoff"}
    assert candidate_resource["source_profiles"] == [
        {"family": "ecoinvent", "version": version, "system_model": system}
        for version in ["3.6", "3.7", "3.8", "3.9", "3.10", "3.11", "3.12"]
        for system in ["cutoff", "consequential"]
    ]


def test_real_current_native_product_ambiguity_remains_refused():
    identities = frozenset(
        {
            ("Sheet rolling, steel", "Service A", "RER", "kilogram"),
            ("Sheet rolling, steel", "Service B", "RER", "kilogram"),
        }
    )
    with pytest.raises(ValueError, match="Ambiguous UVEK reference product"):
        simapro.parse_simapro_technosphere_name(
            "Sheet rolling, steel/RER U",
            unit="kg",
            profile=BackgroundProfile("uvek", "2025", "cutoff"),
            technosphere_identities=identities,
        )
