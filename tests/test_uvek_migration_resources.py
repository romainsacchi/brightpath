import json
import math

from brightpath import DATA_DIR
from brightpath.background import BiosphereCatalog, InMemoryCatalogProvider, TechnosphereCatalog
from brightpath.background.execution import execute_background_migration
from brightpath.background.migration import plan_background_migration
from brightpath.core import BackgroundContext, BiosphereProfile, FormatProfile, InventoryContext, TechnosphereProfile
from brightpath.migrations.engine import _canonical_unit
from brightpath.migrations.resources import load_uvek_biosphere_resource, load_uvek_technosphere_resource
from brightpath.models import InventoryDocument

CATALOG_DIRECTORY = DATA_DIR / "export" / "reference_catalogs"
VERSIONS = ("3.6", "3.7", "3.8", "3.9", "3.10", "3.11", "3.12")
SYSTEM_MODELS = ("cutoff", "consequential")


def _catalog(version: str, system_model: str = "cutoff") -> dict:
    path = CATALOG_DIRECTORY / f"ecoinvent__{version}__{system_model}.json"
    return json.loads(path.read_text(encoding="utf-8"))


def _technosphere_identity(row: dict) -> tuple[str, str, str, str]:
    return row["name"], row["reference_product"], row["location"], row["unit"]


def _technosphere_rule_identity(row: dict) -> tuple[str, str, str, str]:
    return row["name"], row["reference product"], row["location"], row["unit"]


def _biosphere_identity(row: dict) -> tuple[str, tuple[str, ...], str]:
    return row["name"], tuple(row["categories"]), row["unit"]


def test_uvek_resources_cover_every_packaged_ecoinvent_identity_with_existing_targets():
    technosphere = load_uvek_technosphere_resource()
    biosphere = load_uvek_biosphere_resource()
    source_technosphere = set()
    source_biosphere = set()
    for version in VERSIONS:
        for system_model in SYSTEM_MODELS:
            catalog = _catalog(version, system_model)
            source_technosphere.update(_technosphere_identity(row) for row in catalog["technosphere"])
            source_biosphere.update(_biosphere_identity(row) for row in catalog["biosphere"])

    uvek = json.loads((CATALOG_DIRECTORY / "uvek__2025__cutoff.json").read_text(encoding="utf-8"))
    target_technosphere = {_technosphere_identity(row) for row in uvek["technosphere"]}
    target_biosphere = {_biosphere_identity(row) for row in _catalog("3.10")["biosphere"]}
    mapped_technosphere_sources = {_technosphere_rule_identity(rule["source"]) for rule in technosphere["replace"]}
    mapped_technosphere_targets = {_technosphere_rule_identity(rule["target"]) for rule in technosphere["replace"]}
    mapped_biosphere_sources = {_biosphere_identity(rule["source"]) for rule in biosphere["replace"]}
    mapped_biosphere_targets = {_biosphere_identity(rule["target"]) for rule in biosphere["replace"]}

    assert mapped_technosphere_sources == source_technosphere
    assert mapped_technosphere_targets <= target_technosphere
    assert mapped_biosphere_sources == source_biosphere
    assert mapped_biosphere_targets <= target_biosphere
    assert all(0 <= rule["confidence"] <= 1 for rule in (*technosphere["replace"], *biosphere["replace"]))
    assert all(
        _canonical_unit(rule["source"]["unit"]) == _canonical_unit(rule["target"]["unit"])
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
        for rule in load_uvek_biosphere_resource()["replace"]
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

    plan = plan_background_migration(source, target)
    result = execute_background_migration(document, target, provider)

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
