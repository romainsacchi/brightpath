from copy import deepcopy
from dataclasses import FrozenInstanceError

import pytest

from brightpath.core import (
    CANONICAL_SCHEMA_VERSION,
    BackgroundContext,
    BiosphereProfile,
    CanonicalInventory,
    ContextHint,
    FormatProfile,
    InventoryContext,
    TechnosphereProfile,
    resolve_migration_series,
)


def inventory_context():
    return InventoryContext(
        format=FormatProfile("Brightway_Excel", format_version="2.0", dialect="BW2IO"),
        background=BackgroundContext(
            technosphere=TechnosphereProfile("ecoinvent", "3.10.1", "cut-off"),
            biosphere=BiosphereProfile("ecoinvent", "3.10.1"),
        ),
    )


def legacy_inventory():
    return [
        {
            "name": "waste treatment service",
            "reference product": "waste treatment service",
            "location": "CH",
            "unit": "kilogram",
            "code": "foreground-code",
            "formula": "mass * lifetime",
            "custom dataset field": {"nested": [1, {"tuple": (2, 3)}]},
            "parameters": [
                {
                    "name": "mass",
                    "amount": 4.0,
                    "formula": "density * volume",
                    "group": "foreground",
                    "provenance": {"source": "engineering model", "pages": [4, 5]},
                }
            ],
            "exchanges": [
                {
                    "name": "waste treatment service",
                    "reference product": "waste treatment service",
                    "location": "CH",
                    "unit": "kilogram",
                    "type": "production",
                    "amount": 1.0,
                    "formula": "reference_amount",
                    "waste product": True,
                    "custom production metadata": {"allocation": ["mass", "economic"]},
                },
                {
                    "name": "avoided material",
                    "reference product": "material",
                    "location": "RER",
                    "unit": "kilogram",
                    "type": "substitution",
                    "amount": -0.25,
                    "formula": "-recovery_rate",
                    "uncertainty type": 2,
                    "loc": -1.4,
                    "scale": 0.2,
                    "minimum": -1.0,
                    "maximum": 0.0,
                    "pedigree": {"reliability": 2, "correlation": [1, 2, 3]},
                },
                {
                    "name": "Carbon dioxide, fossil",
                    "categories": ("air", "urban air close to ground"),
                    "unit": "kilogram",
                    "type": "biosphere",
                    "amount": 0.1,
                    "input": ("biosphere3", "flow-code"),
                },
            ],
        }
    ]


def test_context_preserves_exact_versions_and_separates_migration_series():
    context = inventory_context()

    assert context.format.format_id == "brightway_excel"
    assert context.format.id == "brightway_excel"
    assert context.format.format_version == "2.0"
    assert context.format.dialect == "bw2io"
    assert context.background.technosphere.version == "3.10.1"
    assert context.background.biosphere.version == "3.10.1"

    resolution = context.background.technosphere.resolve_migration_series()

    assert resolution.exact_version == "3.10.1"
    assert resolution.migration_series == "3.10"
    assert resolution.changed
    assert context.background.technosphere.version == "3.10.1"


def test_version_resolution_keeps_non_ecoinvent_versions_exact():
    resolution = resolve_migration_series("BAFU", "2025.0")

    assert resolution.family == "uvek"
    assert resolution.exact_version == "2025.0"
    assert resolution.migration_series == "2025.0"
    assert not resolution.changed


def test_background_context_normalizes_bafu_only_at_profile_boundary():
    background = BackgroundContext(
        technosphere=TechnosphereProfile(" BAFU ", "2025", "cut off"),
        # UVEK currently uses an ecoinvent biosphere; the axes must not be coupled.
        biosphere=BiosphereProfile("ecoinvent", "3.10"),
    )

    assert background.technosphere == TechnosphereProfile("uvek", "2025", "cutoff")
    assert background.biosphere.family == "ecoinvent"


def test_context_hint_can_be_partial_and_never_invents_missing_context():
    context = inventory_context()
    hint = ContextHint(format=context.format, technosphere=context.background.technosphere)

    assert not hint.is_complete
    assert hint.background is None
    assert hint.technosphere.version == "3.10.1"
    with pytest.raises(ValueError, match="missing background"):
        hint.require_complete()

    complete = ContextHint.from_context(context)
    assert complete.is_complete
    assert complete.require_complete() == context


def test_context_value_objects_reject_empty_required_axes_and_are_frozen():
    with pytest.raises(ValueError, match="format_id"):
        FormatProfile("")
    with pytest.raises(ValueError, match="version"):
        BiosphereProfile("ecoinvent", "")

    profile = TechnosphereProfile("ecoinvent", "3.10.1", "cutoff")
    with pytest.raises(FrozenInstanceError):
        profile.version = "3.10"


def test_legacy_bridge_is_lossless_typed_and_does_not_mutate_source():
    source = legacy_inventory()
    source_snapshot = deepcopy(source)
    metadata = {"owner": {"name": "BrightPath", "roles": ["author"]}}
    database_parameters = [{"name": "database lifetime", "amount": 20, "scope": {"database": "foreground"}}]
    project_parameters = [{"name": "scenario", "formula": "base + delta", "custom": (1, 2)}]
    extensions = {
        "brightway": {"database metadata": {"depends": ["ecoinvent-3.10.1-cutoff"]}},
        "simapro": {"process category": "Materials/Other"},
    }

    inventory = CanonicalInventory.from_legacy_dicts(
        source,
        context=inventory_context(),
        database_name="foreground",
        metadata=metadata,
        database_parameters=database_parameters,
        project_parameters=project_parameters,
        extensions=extensions,
        source_namespace="brightway",
    )

    assert source == source_snapshot
    assert inventory.schema_version == CANONICAL_SCHEMA_VERSION
    assert inventory.datasets[0].identity.reference_product == "waste treatment service"
    assert inventory.datasets[0].parameters[0].formula == "density * volume"
    assert inventory.datasets[0].exchanges[1].exchange_type == "substitution"
    assert inventory.datasets[0].exchanges[1].formula == "-recovery_rate"
    assert inventory.datasets[0].exchanges[1].uncertainty.uncertainty_type == 2
    assert inventory.datasets[0].exchanges[2].identity.categories == (
        "air",
        "urban air close to ground",
    )
    assert inventory.datasets[0].extensions["brightway"]["custom dataset field"] == {"nested": [1, {"tuple": (2, 3)}]}
    assert inventory.datasets[0].exchanges[0].extensions["brightway"]["waste product"] is True
    assert inventory.datasets[0].exchanges[1].extensions["brightway"]["pedigree"]["correlation"] == [1, 2, 3]

    components = inventory.to_legacy_components()

    assert components == {
        "data": source_snapshot,
        "database_name": "foreground",
        "metadata": metadata,
        "database_parameters": database_parameters,
        "project_parameters": project_parameters,
        "extensions": extensions,
    }


def test_legacy_bridge_copies_inputs_and_returns_fresh_nested_values():
    source = legacy_inventory()
    inventory = CanonicalInventory.from_legacy_dicts(
        source,
        context=inventory_context(),
        source_namespace="brightway",
    )

    source[0]["custom dataset field"]["nested"][0] = 99
    first_output = inventory.to_legacy_dicts()
    first_output[0]["custom dataset field"]["nested"][0] = 88
    extension = inventory.datasets[0].extensions["brightway"]
    extension["custom dataset field"]["nested"][0] = 77

    second_output = inventory.to_legacy_dicts()

    assert second_output[0]["custom dataset field"]["nested"][0] == 1
    with pytest.raises(TypeError):
        inventory.datasets[0].extensions["brightway"] = {}


def test_empty_legacy_mappings_round_trip_exactly():
    inventory = CanonicalInventory.from_legacy_dicts([{}], context=inventory_context())

    assert inventory.to_legacy_dicts() == [{}]


def test_invalid_legacy_shapes_fail_with_a_precise_path():
    with pytest.raises(TypeError, match=r"data\[0\]"):
        CanonicalInventory.from_legacy_dicts(["not a mapping"], context=inventory_context())
    with pytest.raises(TypeError, match="dataset.exchanges"):
        CanonicalInventory.from_legacy_dicts(
            [{"name": "broken", "exchanges": {"not": "a list"}}],
            context=inventory_context(),
        )
