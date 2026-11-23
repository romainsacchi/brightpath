"""ecoinvent 3.12 -> 3.13 cut-off and biosphere migration resources."""

import json
import math

import pytest

from brightpath import DATA_DIR, BrightwayInventory
from brightpath.background import PackageCatalogProvider
from brightpath.core.context import (
    BackgroundContext,
    BiosphereProfile,
    FormatProfile,
    InventoryContext,
    TechnosphereProfile,
)
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_biosphere_resources, load_technosphere_resources
from brightpath.units import normalize_unit


def context(version: str) -> BackgroundContext:
    return BackgroundContext(TechnosphereProfile("ecoinvent", version, "cutoff"), BiosphereProfile("ecoinvent", version))


def inventory(version: str, *exchanges: dict) -> BrightwayInventory:
    return BrightwayInventory.from_data(
        [
            {
                "name": "synthetic foreground process",
                "reference product": "synthetic product",
                "location": "CH",
                "unit": "unit",
                "comment": "synthetic",
                "exchanges": [
                    {
                        "name": "synthetic foreground process",
                        "reference product": "synthetic product",
                        "location": "CH",
                        "unit": "unit",
                        "amount": 1.0,
                        "type": "production",
                    },
                    *exchanges,
                ],
            }
        ],
        context=InventoryContext(format=FormatProfile("brightway_excel"), background=context(version)),
    )


def technosphere(name, product, location, unit, amount=1.0):
    return {
        "name": name,
        "reference product": product,
        "location": location,
        "unit": unit,
        "amount": amount,
        "type": "technosphere",
    }


def biosphere(name, categories, unit="kilogram", amount=1.0):
    return {"name": name, "categories": tuple(categories), "unit": unit, "amount": amount, "type": "biosphere"}


def identity(side: dict) -> tuple[str, str, str, str]:
    return side["name"], side["reference product"], side["location"], normalize_unit(side["unit"])


def test_every_rule_identity_exists_in_the_exact_catalogs():
    provider = PackageCatalogProvider()
    source = provider.load_technosphere(TechnosphereProfile("ecoinvent", "3.12", "cutoff")).identities
    target = provider.load_technosphere(TechnosphereProfile("ecoinvent", "3.13", "cutoff")).identities
    resource = load_technosphere_resources("cutoff")[("3.12", "3.13")]
    for rule in resource["replace"]:
        assert identity(rule["source"]) in source
        assert identity(rule["target"]) in target
    for rule in resource["disaggregate"]:
        assert identity(rule["source"]) in source
        assert all(identity(item) in target for item in rule["targets"])
        assert math.isclose(sum(item["allocation"] for item in rule["targets"]), 1.0, abs_tol=1e-9)

    bio_source = provider.load_biosphere(BiosphereProfile("ecoinvent", "3.12")).references
    bio_target = provider.load_biosphere(BiosphereProfile("ecoinvent", "3.13")).references
    resource = load_biosphere_resources()[("3.12", "3.13")]
    for operation in ("replace", "delete"):
        for rule in resource[operation]:
            key = (rule["source"]["name"], tuple(rule["source"]["categories"]), rule["source"]["unit"])
            assert bio_source[key] == rule["source"]["uuid"]
            if operation == "replace":
                target = rule["target"]
                key = (
                    target.get("name", key[0]),
                    tuple(target.get("categories", key[1])),
                    target.get("unit", key[2]),
                )
                assert bio_target[key] == target["uuid"]


def test_catalog_manifest_counts_for_313():
    manifest = json.loads((DATA_DIR / "export/reference_catalogs/RESOURCE_MANIFEST.json").read_text(encoding="utf-8"))
    entry = next(item for item in manifest["resources"] if item["file"] == "ecoinvent__3.13__cutoff.json")
    assert entry["technosphere_identities"] == entry["technosphere_uuid_references"] == 27400
    assert entry["biosphere_identities"] == entry["biosphere_uuid_references"] == 9893


def test_strict_migration_replaces_disaggregates_and_renames():
    source = inventory(
        "3.12",
        technosphere("calcined petroleum coke production", "calcined petroleum coke", "CN", "kilogram", 2.0),
        technosphere("market for seed-cotton", "seed-cotton", "GLO", "kilogram", 10.0),
        biosphere("2-Propen-1-ol", ("air",), amount=0.5),
        biosphere("2-Propen-1-ol", ("water", "surface water"), amount=0.25),
        biosphere("Carbon dioxide, fossil", ("air",), amount=7.0),
    )
    migrated = source.migrate_background(context("3.13"))
    exchanges = migrated.data[0]["exchanges"][1:]

    coke = [item for item in exchanges if item.get("reference product") == "calcined petroleum coke"]
    assert [item["name"] for item in coke] == ["calcined petroleum coke production, vertical shaft"]
    cotton = [item for item in exchanges if item.get("name") == "market for seed-cotton"]
    assert len(cotton) == 7
    assert math.isclose(sum(item["amount"] for item in cotton), 10.0)
    flows = {(item["name"], tuple(item["categories"])) for item in exchanges if item["type"] == "biosphere"}
    assert flows == {
        ("Allyl alcohol", ("air",)),
        ("Allyl alcohol", ("water",)),
        ("Carbon dioxide, fossil", ("air",)),
    }
    assert not migrated.validate().has_errors


def test_biosphere_deletion_requires_explicit_policy():
    source = inventory("3.12", biosphere("2-Propen-1-ol", ("soil", "agricultural"), amount=0.01))
    with pytest.raises(MigrationError):
        source.migrate_background(context("3.13"))
    migrated = source.migrate_background(context("3.13"), policy=MigrationPolicy(on_deletion=PolicyAction.WARN))
    assert [item["type"] for item in migrated.data[0]["exchanges"]] == ["production"]


def test_chained_route_from_311_to_313():
    source = inventory(
        "3.11", technosphere("4-methyl-2-pentanone production", "4-methyl-2-pentanone", "RER", "kilogram")
    )
    migrated = source.migrate_background(context("3.13"))
    assert migrated.data[0]["exchanges"][1]["name"] == (
        "methyl isobutyl ketone production, one-step process, from acetone and hydrogen"
    )
