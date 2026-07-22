import pytest

from brightpath import DATA_DIR, BackgroundProfile, BrightwayInventory
from brightpath.background import PackageCatalogProvider
from brightpath.core import BackgroundContext, BiosphereProfile, MigrationPolicy, TechnosphereProfile
from brightpath.exceptions import MigrationError
from brightpath.migrations import available_ecoinvent_versions, resolve_migration_route
from brightpath.migrations.resources import load_technosphere_resources


def profile(version):
    return BackgroundProfile("ecoinvent", version, "cutoff")


def inventory_with_exchange(exchange, version="3.6"):
    return BrightwayInventory.from_data(
        [
            {
                "name": "foreground service",
                "reference product": "service",
                "location": "GLO",
                "unit": "unit",
                "exchanges": [
                    {
                        "name": "foreground service",
                        "reference product": "service",
                        "location": "GLO",
                        "unit": "unit",
                        "amount": 1.0,
                        "type": "production",
                    },
                    exchange,
                ],
            }
        ],
        background_profile=profile(version),
    )


def test_packaged_migrations_cover_forward_and_reverse_routes():
    resources = load_technosphere_resources("cutoff")

    assert available_ecoinvent_versions() == (
        "3.5",
        "3.6",
        "3.7",
        "3.8",
        "3.9",
        "3.10",
        "3.11",
        "3.12",
    )
    assert [direction for _, _, direction in resolve_migration_route("3.6", "3.12", resources)] == ["forward"] * 6
    assert [direction for _, _, direction in resolve_migration_route("3.12", "3.6", resources)] == ["backward"] * 6


def test_forward_and_backward_replacement_are_non_mutating():
    source = inventory_with_exchange(
        {
            "name": "aluminium production, primary, ingot",
            "reference product": "aluminium, primary, ingot",
            "location": "RNA",
            "unit": "kilogram",
            "amount": 2.0,
            "type": "technosphere",
        }
    )

    forward = source.migrate_background(profile("3.7"))
    backward = forward.migrate_background(
        profile("3.6"),
        policy=MigrationPolicy.permissive(),
    )

    assert source.data[0]["exchanges"][1]["location"] == "RNA"
    assert forward.data[0]["exchanges"][1]["location"] == "CA"
    assert backward.data[0]["exchanges"][1]["location"] == "RNA"
    assert "migration.technosphere_step_applied" in {change.code for change in forward.last_migration_report.changes}
    assert "migration.technosphere_step_applied" in {change.code for change in backward.last_migration_report.changes}


def test_reverse_migration_aggregates_forward_disaggregation():
    source = inventory_with_exchange(
        {
            "name": "ammonia production, partial oxidation, liquid",
            "reference product": "ammonia, liquid",
            "location": "RoW",
            "unit": "kilogram",
            "amount": 10.0,
            "type": "technosphere",
        }
    )

    forward = source.migrate_background(profile("3.7"))
    backward = forward.migrate_background(
        profile("3.6"),
        policy=MigrationPolicy.permissive(),
    )
    forward_exchanges = forward.data[0]["exchanges"][1:]
    reconstructed = backward.data[0]["exchanges"][1]

    assert len(forward_exchanges) == 2
    assert sum(exchange["amount"] for exchange in forward_exchanges) == pytest.approx(10.0)
    assert reconstructed["reference product"] == "ammonia, liquid"
    assert reconstructed["location"] == "RoW"
    assert reconstructed["amount"] == pytest.approx(10.0)
    assert "migration.reverse_aggregation" in {loss.code for loss in backward.last_migration_report.losses}


def test_full_311_to_312_migration_applies_biosphere_renames():
    source = inventory_with_exchange(
        {
            "name": "4-Methyl-2-pentanone",
            "categories": ("air",),
            "unit": "kilogram",
            "amount": 1.0,
            "type": "biosphere",
        },
        version="3.11",
    )

    target = BackgroundContext(
        technosphere=TechnosphereProfile("ecoinvent", "3.12", "cutoff"),
        biosphere=BiosphereProfile("ecoinvent", "3.12"),
    )

    migrated = source.migrate_background(target)

    assert migrated.data[0]["exchanges"][1]["name"] == "Methyl isobutyl ketone"
    assert migrated.background_profile == BackgroundProfile("ecoinvent", "3.12", "cutoff")
    assert migrated.biosphere_profile == BiosphereProfile("ecoinvent", "3.12")
    assert source.data[0]["exchanges"][1]["name"] == "4-Methyl-2-pentanone"


def test_311_to_312_biosphere_resource_covers_every_obsolete_catalog_identity():
    provider = PackageCatalogProvider()
    source_profile = BiosphereProfile("ecoinvent", "3.11")
    target_profile = BiosphereProfile("ecoinvent", "3.12")
    source_catalog = provider.load_biosphere(source_profile).identities
    target_catalog = provider.load_biosphere(target_profile).identities
    obsolete = source_catalog - target_catalog
    source = BrightwayInventory.from_data(
        [
            {
                "name": "foreground service",
                "reference product": "service",
                "location": "GLO",
                "unit": "unit",
                "exchanges": [
                    {
                        "name": "foreground service",
                        "reference product": "service",
                        "location": "GLO",
                        "unit": "unit",
                        "amount": 1.0,
                        "type": "production",
                    },
                    *(
                        {
                            "name": name,
                            "categories": categories,
                            "unit": unit,
                            "amount": 1.0,
                            "type": "biosphere",
                        }
                        for name, categories, unit in sorted(obsolete)
                    ),
                ],
            }
        ],
        background_profile=profile("3.11"),
    )
    target = BackgroundContext(
        technosphere=TechnosphereProfile("ecoinvent", "3.11", "cutoff"),
        biosphere=target_profile,
    )

    migrated = source.migrate_background(target)
    migrated_identities = {
        (exchange["name"], tuple(exchange["categories"]), exchange["unit"])
        for exchange in migrated.data[0]["exchanges"]
        if exchange["type"] == "biosphere"
    }

    assert len(obsolete) == 12
    assert migrated_identities <= target_catalog
    assert {
        "Ferrocyanide",
        "Methyl isobutyl ketone",
        "tert-Butanol",
    } <= {name for name, _categories, _unit in migrated_identities}
    assert {
        (exchange["name"], tuple(exchange["categories"]), exchange["unit"])
        for exchange in source.data[0]["exchanges"]
        if exchange["type"] == "biosphere"
    } == obsolete


def test_cross_family_migration_resource_is_active():
    resource = DATA_DIR / "migrations" / "uvek" / "ecoinvent-to-uvek-2025.json"

    assert resource.is_file()


def test_consequential_migration_is_rejected_until_rules_exist():
    source = BrightwayInventory.from_data(
        [],
        background_profile=BackgroundProfile("ecoinvent", "3.10", "consequential"),
    )

    with pytest.raises(MigrationError, match="consequential"):
        source.migrate_background(BackgroundProfile("ecoinvent", "3.11", "consequential"))


@pytest.mark.parametrize(
    "same_profile",
    [
        BackgroundProfile("ecoinvent", "3.10", "consequential"),
        BackgroundProfile("uvek", "2025", "cutoff"),
    ],
)
def test_same_profile_migration_is_a_supported_noop(same_profile):
    source = BrightwayInventory.from_data([], background_profile=same_profile)

    migrated = source.migrate_background(same_profile)

    assert migrated.background_profile == same_profile.normalized()
    assert migrated.last_migration_report.stages
    assert not migrated.last_migration_report.changed
