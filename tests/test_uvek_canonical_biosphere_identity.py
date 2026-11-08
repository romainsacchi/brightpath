"""Publisher identity regressions for separately listed foreground emissions."""

import json
from copy import deepcopy
from functools import lru_cache

import pytest

from brightpath import DATA_DIR
from brightpath.background import PackageCatalogProvider
from brightpath.background.execution import execute_background_migration
from brightpath.background import plan_background_migration
from brightpath.background import migration as planning
from brightpath.core import (
    BackgroundContext,
    BiosphereProfile,
    FormatProfile,
    InventoryContext,
    MigrationPolicy,
    TechnosphereProfile,
)
from brightpath.exceptions import MigrationError
from brightpath.migrations.canonical_biosphere import (
    MARKER,
    REVIEW,
    select_canonical_biosphere_rules,
    validate_canonical_biosphere_resource,
)
from brightpath.migrations.engine import _apply_biosphere_rules
from brightpath.migrations.models import MigrationStepReport
from brightpath.migrations.resources import load_uvek_biosphere_resource
from brightpath.models import InventoryDocument
from brightpath.units import normalize_unit

VERSIONS = ("3.6", "3.7", "3.8", "3.9", "3.10", "3.11", "3.12")


@pytest.mark.parametrize("version", ["3.6", "3.7", "3.8", "3.9", "3.11", "3.12"])
def test_verified_biosphere_plan_is_distinct_from_heuristic_supplier_consent(version):
    source = BackgroundContext(
        TechnosphereProfile("ecoinvent", version, "cutoff"), BiosphereProfile("ecoinvent", version)
    )
    target = BackgroundContext(TechnosphereProfile("uvek", "2025", "cutoff"), BiosphereProfile("ecoinvent", "3.10"))
    plan = plan_background_migration(source, target)
    identities = [i for i in plan.report.issues if i.code == "migration.canonical_biosphere_identity"]
    assert len(identities) == 1
    assert identities[0].details["source_version"] == version
    assert identities[0].details["coverage"]["unresolved_identities"] > 0
    assert not any(
        i.code == "migration.heuristic_mapping" and i.path == "background.biosphere" for i in plan.report.issues
    )
    assert any(
        i.code == "migration.heuristic_mapping" and i.path == "background.technosphere" for i in plan.report.issues
    )
    assert not plan.executable  # Native supplier mappings still require independent consent.


def test_unreviewed_biosphere_resource_still_requires_heuristic_consent(monkeypatch):
    resource = {k: v for k, v in load_uvek_biosphere_resource().items() if k != MARKER}
    monkeypatch.setattr(planning, "load_uvek_biosphere_resource", lambda: resource)
    issues, losses = [], []
    planning._plan_uvek_biosphere(
        BiosphereProfile("ecoinvent", "3.6").resolve_migration_series(),
        BiosphereProfile("ecoinvent", "3.10").resolve_migration_series(),
        MigrationPolicy.strict(),
        issues,
        losses,
    )
    assert [i.code for i in issues] == ["migration.heuristic_mapping"]
    assert issues[0].severity.value == "error"
    assert [loss.code for loss in losses] == ["migration.heuristic_mapping"]


@lru_cache
def catalog(version):
    return json.loads((DATA_DIR / "export" / "reference_catalogs" / f"ecoinvent__{version}__cutoff.json").read_text())[
        "biosphere"
    ]


def key(row):
    return row["name"], tuple(row["categories"]), normalize_unit(row["unit"])


def exchange(row, amount=0.25, **extra):
    return {
        **{k: deepcopy(row[k]) for k in ("name", "categories", "unit")},
        "amount": amount,
        "type": "biosphere",
        **extra,
    }


def report(version, direction="forward"):
    return MigrationStepReport(version, "3.10", direction, "canonical-publisher-identity")


def execute(version, exchanges):
    source = BackgroundContext(
        TechnosphereProfile("ecoinvent", version, "cutoff"), BiosphereProfile("ecoinvent", version)
    )
    target = BackgroundContext(TechnosphereProfile("uvek", "2025", "cutoff"), BiosphereProfile("ecoinvent", "3.10"))
    output = {
        "name": "separate foreground emissions",
        "reference product": "service",
        "location": "GLO",
        "unit": "unit",
    }
    data = [
        {
            **output,
            "opaque caller metadata": {"keep": True},
            "exchanges": [
                {**output, "type": "production", "amount": 1},
                *exchanges,
            ],
        }
    ]
    original = deepcopy(data)
    document = InventoryDocument(data=data, context=InventoryContext(FormatProfile("brightway_excel"), source))
    result = execute_background_migration(document, target, PackageCatalogProvider(), MigrationPolicy.permissive())
    assert data == original
    assert document.context.background == source
    return result, original


@pytest.mark.parametrize("version", ("3.6", "3.7", "3.8"))
@pytest.mark.parametrize("amount", (-0.25, 0, 0.25))
@pytest.mark.parametrize(
    "uncertainty",
    (
        {"uncertainty type": 2, "loc": -1.3862943611198906, "scale": 0.2, "negative": True},
        {"uncertainty type": 3, "loc": -0.25, "scale": 0.1, "minimum": -1, "maximum": 1},
    ),
)
def test_old_nickel_air_retains_mass_context_and_caller_metadata(version, amount, uncertainty):
    source = next(
        r
        for r in catalog(version)
        if r["name"] == "Nickel" and r["categories"] == ["air", "non-urban air or from high stacks"]
    )
    target = next(r for r in catalog("3.10") if r["flow_id"] == source["flow_id"])
    bio = exchange(
        source,
        amount,
        **uncertainty,
        formula="dose",
        flow_id=source["flow_id"],
        flow=source["flow_id"],
        uuid=source["flow_id"],
        input=("old-biosphere", source["flow_id"]),
        caller_metadata={"measurement": [1, 2]},
    )
    result, original = execute(version, [bio])
    assert result.succeeded, result.report
    actual = result.value.data[0]["exchanges"][1]
    assert key(actual) == key(target)
    assert actual["categories"][0] == "air"
    for field in ("amount", "formula", "caller_metadata", *uncertainty):
        assert actual[field] == bio[field]
    assert all(actual[f] == target["flow_id"] for f in ("flow_id", "flow", "uuid"))
    assert "input" not in actual
    assert result.value.data[0]["opaque caller metadata"] == original[0]["opaque caller metadata"]


@pytest.mark.parametrize("name", ("Nickel, ion", "Potassium, ion"))
@pytest.mark.parametrize("version", ("3.6", "3.7", "3.8"))
def test_reviewed_groundwater_synonyms_update_publisher_ids_without_rescaling(name, version):
    source = next(r for r in catalog(version) if r["name"] == name and r["categories"] == ["water", "ground-"])
    resource = select_canonical_biosphere_rules(load_uvek_biosphere_resource(), version)
    rule = next(r for r in resource["replace"] if key(r["source"]) == key(source))
    assert rule[REVIEW]["profiles"][0]["identity_kind"] == "reviewed_publisher_synonym"
    assert rule["target"]["flow_id"] != source["flow_id"]
    data = [
        {
            "exchanges": [
                exchange(
                    source,
                    -1.25,
                    flow_id=source["flow_id"],
                    flow=source["flow_id"],
                    uuid=source["flow_id"],
                    formula="-dose",
                )
            ]
        }
    ]
    _apply_biosphere_rules(data, resource, "forward", report(version))
    actual = data[0]["exchanges"][0]
    assert key(actual) == key(rule["target"])
    assert actual["amount"] == -1.25 and actual["formula"] == "-dose"
    assert all(actual[f] == rule["target"]["flow_id"] for f in ("flow_id", "flow", "uuid"))


@pytest.mark.parametrize("version", ("3.6", "3.8", "3.10", "3.12"))
@pytest.mark.parametrize("context", ("ground-", "ground-, long-term", "ocean", "surface water"))
def test_manganese_label_uses_source_release_not_default_target(version, context):
    source = next(r for r in catalog(version) if r["name"] == "Manganese" and r["categories"] == ["water", context])
    target = next(r for r in catalog("3.10") if r["flow_id"] == source["flow_id"])
    data = [{"exchanges": [exchange(source, -0.125, uuid="opaque caller metadata")]}]
    step = report(version)
    _apply_biosphere_rules(data, load_uvek_biosphere_resource(), "forward", step)
    assert not step.issues
    assert key(data[0]["exchanges"][0]) == key(target)
    assert data[0]["exchanges"][0]["flow_id"] == target["flow_id"]
    assert data[0]["exchanges"][0]["uuid"] == "opaque caller metadata"
    assert data[0]["exchanges"][0]["amount"] == -0.125


@pytest.mark.parametrize("version", VERSIONS)
def test_every_verified_profile_retains_publisher_identity_unit_and_full_context(version):
    original = load_uvek_biosphere_resource()
    selected = select_canonical_biosphere_rules(original, version)
    validate_canonical_biosphere_resource(selected)
    sources = {key(r): r for r in catalog(version)}
    targets = {r["flow_id"]: r for r in catalog("3.10")}
    data = [{"exchanges": [exchange(r["source"], -0.0125) for r in selected["replace"]]}]
    step = report(version)
    _apply_biosphere_rules(data, original, "forward", step)
    assert not step.issues
    assert step.biosphere_replacements == len(selected["replace"])
    for rule, actual in zip(selected["replace"], data[0]["exchanges"][0:], strict=True):
        target = targets[actual["flow_id"]]
        assert key(actual) == key(target)
        assert actual["amount"] == -0.0125
        assert key(target)[1:] == key(sources[key(rule["source"])])[1:]
        decision = rule[REVIEW]["profiles"][0]
        if decision["identity_kind"] == "publisher_uuid":
            assert sources[key(rule["source"])]["flow_id"] == target["flow_id"]
        else:
            assert decision["source_cas"] == decision["target_cas"]


@pytest.mark.parametrize(
    "kind", ("radioactive manganese", "gas volume", "fish context", "unknown chemical", "unknown context")
)
def test_unsupported_physical_flow_rolls_back_even_permissively(kind):
    version = "3.9"
    if kind == "radioactive manganese":
        source = next(
            r for r in catalog(version) if r["name"] == "Manganese-55" and r["categories"] == ["water", "surface water"]
        )
    elif kind in ("gas volume", "fish context"):
        version = "3.6"
        resource = select_canonical_biosphere_rules(load_uvek_biosphere_resource(), version)
        candidates = [
            r["source"]
            for r in resource["unresolved"]
            if "different physical unit or receiving context" in r["profiles"][0]["reason"]
        ]
        source = next(
            r for r in candidates if (r["unit"] == "cubic meter" if kind == "gas volume" else "Fish" in r["name"])
        )
    else:
        source = {"name": "invented pollutant", "categories": ["air", "unknown context"], "unit": "kilogram"}
        if kind == "unknown context":
            source["name"] = "Carbon dioxide, fossil"
    good = next(
        r
        for r in catalog(version)
        if r["name"] == "Carbon dioxide, fossil" and r["categories"] == ["air", "urban air close to ground"]
    )
    result, original = execute(version, [exchange(good), exchange(source, -2, formula="-waste", flow_id="caller-id")])
    assert not result.succeeded
    assert any(i.code == "migration.biosphere_identity_unresolved" for i in result.report.issues)
    assert result.value is None or result.value.data == original


def test_alias_units_and_idempotent_selection_do_not_mutate_resources():
    original = load_uvek_biosphere_resource()
    frozen = json.dumps(original, sort_keys=True)
    selected = select_canonical_biosphere_rules(original, "3.6")
    assert select_canonical_biosphere_rules(selected, "3.6") == selected
    source = next(r for r in catalog("3.6") if r["name"] == "Nickel" and r["categories"][0] == "air")
    data = [{"exchanges": [exchange(source, -1, unit="kg")]}]
    step = report("3.6")
    _apply_biosphere_rules(data, original, "forward", step)
    assert not step.issues and data[0]["exchanges"][0]["unit"] == "kilogram"
    assert json.dumps(original, sort_keys=True) == frozen


@pytest.mark.parametrize(
    "change", ("unit", "context", "uuid", "cas", "duplicate profile", "default target", "unreviewed chemical")
)
def test_schema_rejects_changed_physical_correspondence(change):
    resource = deepcopy(load_uvek_biosphere_resource())
    rule = next(
        r
        for r in resource["replace"]
        if any(p.get("identity_kind") == "reviewed_publisher_synonym" for p in r[REVIEW]["profiles"])
    )
    decision = rule[REVIEW]["profiles"][0]
    if change == "unit":
        decision["target"]["unit"] = "gram"
    elif change == "context":
        decision["target"]["categories"] = ["natural resource", "in ground"]
    elif change == "uuid":
        decision["identity_kind"] = "publisher_uuid"
    elif change == "cas":
        decision["target_cas"] = "different chemical"
    elif change == "duplicate profile":
        rule[REVIEW]["profiles"].append(deepcopy(decision))
    elif change == "default target":
        rule["target"] = {**rule["target"], "flow_id": "uninspected target"}
    else:
        decision["identity_kind"] = "fuzzy chemical name"
    with pytest.raises(MigrationError):
        validate_canonical_biosphere_resource(resource)
    if change in {"unit", "context", "uuid", "cas", "duplicate profile", "unreviewed chemical"}:
        with pytest.raises(MigrationError):
            select_canonical_biosphere_rules(resource, decision["source_profile"]["version"])


def test_inferred_inverse_is_not_authorized_and_cannot_mutate_input():
    source = catalog("3.10")[0]
    data = [{"exchanges": [exchange(source)]}]
    original = deepcopy(data)
    step = report("3.10", "backward")
    _apply_biosphere_rules(data, load_uvek_biosphere_resource(), "backward", step)
    assert data == original
    assert [i.code for i in step.issues] == ["migration_biosphere_identity_direction_unavailable"]
