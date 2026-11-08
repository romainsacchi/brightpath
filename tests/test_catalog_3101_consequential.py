"""Exact native patch references must not imply a cross-system-model migration route."""

import hashlib
import json
from copy import deepcopy
from pathlib import Path
from uuid import UUID

import pytest

from brightpath import (
    DATA_DIR,
    BackgroundContext,
    BiosphereProfile,
    BrightwayInventory,
    FormatProfile,
    InventoryContext,
    TechnosphereProfile,
)
from brightpath.background import migration_resource_fingerprint
from brightpath.background.catalogs import PackageCatalogProvider
from brightpath.background.migration import plan_background_migration
from brightpath.background.validation import validate_background_links
from brightpath.core.policies import MigrationPolicy

PROFILE = TechnosphereProfile("ecoinvent", "3.10.1", "consequential")
CATALOG_SHA256 = "7a62d5958352ec3cb4fd7a09213460995320361b2bc3452416cb85eaebd6d91d"


def test_exact_native_catalog_has_complete_patch_uuid_references():
    provider = PackageCatalogProvider()
    catalog = provider.load_technosphere(PROFILE)
    path = DATA_DIR / "export/reference_catalogs/ecoinvent__3.10.1__consequential.json"
    assert hashlib.sha256(path.read_bytes()).hexdigest() == catalog.digest == CATALOG_SHA256
    assert catalog.profile == PROFILE and catalog.schema_version == 2
    assert len(catalog.identities) == len(catalog.references) == 20772
    for activity_id, flow_id in catalog.references.values():
        UUID(activity_id)
        UUID(flow_id)
    source = ("1,1-difluoroethane production", "1,1-difluoroethane", "RoW", "kilogram")
    assert catalog.references[source] == (
        "5d7b456a-7dcc-55f8-8ae2-e8bf341b75aa",
        "807906d0-f3cb-4a7c-a528-ae497a61bf12",
    )
    base = provider.load_technosphere(TechnosphereProfile("ecoinvent", "3.10", "consequential"))
    assert base.identities == catalog.identities
    assert base.references[source][0] != catalog.references[source][0]
    assert PROFILE in provider.technosphere_profiles()


def test_patch_biosphere_shards_preserve_every_original_identity_and_uuid():
    provider = PackageCatalogProvider()
    catalog = provider.load_biosphere(BiosphereProfile("ecoinvent", "3.10.1"))
    original = json.loads((DATA_DIR / "export/reference_catalogs/ecoinvent__3.10.1__cutoff.json").read_text())
    expected = {(row["name"], tuple(row["categories"]), row["unit"]): row["flow_id"] for row in original["biosphere"]}
    assert len(catalog.identities) == len(catalog.references) == 4362
    assert dict(catalog.references) == expected


@pytest.mark.parametrize("amount", (-2, 0, 2))
def test_native_patch_validation_keeps_exact_profile_and_signed_caller_data(amount):
    supplier = {
        "name": "1,1-difluoroethane production",
        "reference product": "1,1-difluoroethane",
        "location": "RoW",
        "unit": "kilogram",
    }
    owner = {"name": "Synthetic exact-patch consumer", "reference product": "service", "location": "CH", "unit": "unit"}
    data = [
        {
            **owner,
            "exchanges": [
                {**owner, "type": "production", "amount": 1},
                {
                    **supplier,
                    "type": "technosphere",
                    "amount": amount,
                    "formula": "dose",
                    "caller_metadata": {"keep": [1, 2]},
                },
            ],
        }
    ]
    before = deepcopy(data)
    background = BackgroundContext(PROFILE, BiosphereProfile("ecoinvent", "3.10.1"))
    assert not validate_background_links(data, background, PackageCatalogProvider()).has_errors
    inventory = BrightwayInventory.from_data(
        data, context=InventoryContext(FormatProfile("brightway_excel"), background)
    )
    result = inventory.migrate_background(background, policy=MigrationPolicy.strict())
    assert result.context.background == background
    assert result.data == inventory.data == data == before


@pytest.mark.parametrize(
    "source",
    [("uvek", "2025"), *[("ecoinvent", version) for version in ("3.6", "3.7", "3.8", "3.9", "3.10", "3.11", "3.12")]],
)
@pytest.mark.parametrize("target_version", ("3.8", "3.9", "3.10", "3.10.1", "3.11", "3.12"))
@pytest.mark.parametrize("policy", (MigrationPolicy.strict(), MigrationPolicy.permissive()))
def test_catalog_availability_never_enables_either_prohibited_cross_model_route(source, target_version, policy):
    family, version = source
    original = BackgroundContext(
        TechnosphereProfile(family, version, "cutoff"),
        BiosphereProfile("ecoinvent", "3.10" if family == "uvek" else version),
    )
    target = BackgroundContext(
        TechnosphereProfile("ecoinvent", target_version, "consequential"), BiosphereProfile("ecoinvent", target_version)
    )
    plan = plan_background_migration(original, target, policy)
    assert not plan.executable and plan.technosphere_steps == ()
    assert plan.source is original and plan.target is target
    assert "migration.technosphere_cross_system_model_unavailable" in {i.code for i in plan.report.issues}


def test_new_catalog_manifest_entry_invalidates_cached_migration_fingerprints_without_writes(monkeypatch):
    manifest_path = DATA_DIR / "export/reference_catalogs/RESOURCE_MANIFEST.json"
    manifest = json.loads(manifest_path.read_bytes())
    original_bytes = manifest_path.read_bytes()
    previous = deepcopy(manifest)
    previous["resources"] = [r for r in previous["resources"] if r["file"] != "ecoinvent__3.10.1__consequential.json"]
    assert len(previous["resources"]) + 1 == len(manifest["resources"])
    previous_bytes = (json.dumps(previous, indent=2, sort_keys=True) + "\n").encode()
    current = migration_resource_fingerprint()
    original_read = Path.read_bytes
    with monkeypatch.context() as context:
        context.setattr(
            Path, "read_bytes", lambda path: previous_bytes if path == manifest_path else original_read(path)
        )
        assert migration_resource_fingerprint() != current
    assert manifest_path.read_bytes() == original_bytes
    assert migration_resource_fingerprint() == current
