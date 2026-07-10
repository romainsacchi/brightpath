import pytest

from brightpath import DATA_DIR, BackgroundProfile, BrightwayInventory
from brightpath.exceptions import MigrationUnavailableError
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

    forward = source.migrate_background(profile("3.7"), validate_target=False)
    backward = forward.migrate_background(profile("3.6"), validate_target=False)

    assert source.data[0]["exchanges"][1]["location"] == "RNA"
    assert forward.data[0]["exchanges"][1]["location"] == "CA"
    assert backward.data[0]["exchanges"][1]["location"] == "RNA"
    assert forward.last_migration_report.steps[0].technosphere_replacements == 1
    assert backward.last_migration_report.steps[0].technosphere_replacements == 1


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

    forward = source.migrate_background(profile("3.7"), validate_target=False)
    backward = forward.migrate_background(profile("3.6"), validate_target=False)
    forward_exchanges = forward.data[0]["exchanges"][1:]
    reconstructed = backward.data[0]["exchanges"][1]

    assert len(forward_exchanges) == 2
    assert sum(exchange["amount"] for exchange in forward_exchanges) == pytest.approx(10.0)
    assert reconstructed["reference product"] == "ammonia, liquid"
    assert reconstructed["location"] == "RoW"
    assert reconstructed["amount"] == pytest.approx(10.0)
    assert "migration_reverse_aggregation_lossy" in {issue.code for issue in backward.last_migration_report.all_issues}


def test_missing_biosphere_step_is_reported():
    source = inventory_with_exchange(
        {
            "name": "market for electricity, low voltage",
            "reference product": "electricity, low voltage",
            "location": "CH",
            "unit": "kilowatt hour",
            "amount": 1.0,
            "type": "technosphere",
        },
        version="3.11",
    )

    migrated = source.migrate_background(profile("3.12"), validate_target=False)

    assert "biosphere_migration_missing" in {issue.code for issue in migrated.last_migration_report.all_issues}


def test_cross_family_migration_uses_explicit_placeholder_failure():
    placeholder = DATA_DIR / "migrations" / "uvek" / "ecoinvent-to-uvek-2025-placeholder.json"
    source = inventory_with_exchange(
        {
            "name": "market for electricity, low voltage",
            "reference product": "electricity, low voltage",
            "location": "CH",
            "unit": "kilowatt hour",
            "amount": 1.0,
            "type": "technosphere",
        }
    )

    assert placeholder.is_file()
    with pytest.raises(MigrationUnavailableError, match="placeholder"):
        source.migrate_background(BackgroundProfile("uvek", "2025", "cutoff"))


def test_consequential_migration_is_rejected_until_rules_exist():
    source = BrightwayInventory.from_data(
        [],
        background_profile=BackgroundProfile("ecoinvent", "3.10", "consequential"),
    )

    with pytest.raises(MigrationUnavailableError, match="cut-off"):
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

    migrated = source.migrate_background(same_profile, validate_target=False)

    assert migrated.background_profile == same_profile.normalized()
    assert migrated.last_migration_report.steps == []
    assert not migrated.last_migration_report.changed
