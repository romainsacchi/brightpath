"""Slag compartments retain their own capacity, operation and positive coordinates."""

from copy import deepcopy

import pytest
from test_uvek_reference_signs import FIELDS, consumer, contexts
from test_uvek_refrigerant_disposal import target_context

from brightpath import BrightwayInventory, InventoryContext, TechnosphereProfile
from brightpath.background.reference_signs import select_profile_sign_rules
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_release_resources, load_uvek_technosphere_resource
from scripts.review_uvek_slag_landfill_components import (
    APPROVAL,
    SOURCES,
    approved_component_rule,
    validate_inventories,
)

POLICY = MigrationPolicy(on_heuristic_mapping=PolicyAction.WARN, on_information_loss=PolicyAction.WARN)
VERSIONS = ("3.6", "3.7", "3.8", "3.9", "3.9.1", "3.10", "3.10.1", "3.11", "3.12")


def batch(amount):
    data = None
    for source in SOURCES:
        for kind, uncertainty in (("technosphere", 2), ("substitution", 3)):
            part = consumer(dict(zip(FIELDS, source, strict=True)), amount, uncertainty, kind)
            part[0]["exchanges"][1]["caller_metadata"] = {"keep": [1, 2]}
            if data is None:
                data = part
            else:
                data[0]["exchanges"].append(part[0]["exchanges"][1])
    data[0]["exchanges"].append(
        {
            "name": "Carbon dioxide, fossil",
            "categories": ["air"],
            "unit": "kilogram",
            "type": "biosphere",
            "amount": 0.5,
        }
    )
    return data


def release_target(supplier, release):
    if release == "2025":
        return supplier
    bridge = next(
        r for r in load_uvek_release_resources("technosphere") if r["source_profiles"][0]["version"] == "2025"
    )
    return next(r["target"] for r in bridge["replace"] if r["source"] == supplier)


@pytest.mark.parametrize("version", VERSIONS)
@pytest.mark.parametrize("release", ("2025", "2026", "native2026"))
@pytest.mark.parametrize("amount", (-10, 0, 10))
def test_operation_and_full_facility_preserve_signs_metadata_and_caller_emissions(version, release, amount):
    data = batch(amount)
    original = deepcopy(data)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", version))
    result = inventory.migrate_background(target_context(release), policy=POLICY)
    assert len(result.data) == 1
    assert result.data[0]["exchanges"][0] == original[0]["exchanges"][0]
    assert result.data[0]["exchanges"][-1]["amount"] == 0.5
    for before, after in zip(original[0]["exchanges"][1:-1], result.data[0]["exchanges"][1:-1], strict=True):
        expected = SOURCES[tuple(before[f] for f in FIELDS)]["target"]
        assert {f: after[f] for f in FIELDS} == release_target(expected, release)
        for field in (
            "amount",
            "type",
            "formula",
            "uncertainty type",
            "loc",
            "scale",
            "negative",
            "minimum",
            "maximum",
            "caller_metadata",
        ):
            assert after.get(field) == before.get(field)
    warnings = [i for i in result.last_migration_report.issues if i.code == "migration.technosphere_information_loss"]
    assert len(warnings) == 18
    assert all(
        "375,000 m3" in i.message and "factor +1" in i.message and "water-to-water" in i.message for i in warnings
    )
    assert inventory.data == data == original


@pytest.mark.parametrize("release", ("2025", "2026", "native2026"))
@pytest.mark.parametrize("amount", (-2, 0, 2))
def test_existing_reverse_links_match_independently_inspected_construction_and_operation(release, amount):
    data = None
    for kind in ("construction", "operation"):
        row = next(r for r in APPROVAL["rules"] if r["kind"] == kind)
        part = consumer(release_target(row["target"], release), amount, 3)
        if data is None:
            data = part
        else:
            data[0]["exchanges"].append(part[0]["exchanges"][1])
    original = deepcopy(data)
    inventory = BrightwayInventory.from_data(
        data,
        context=InventoryContext(
            contexts("uvek", target_context(release).technosphere.version).format, target_context(release)
        ),
    )
    result = inventory.migrate_background(
        contexts("ecoinvent", "3.12").background,
        policy=MigrationPolicy(
            on_heuristic_mapping=PolicyAction.WARN,
            on_information_loss=PolicyAction.WARN,
            on_inferred_reverse=PolicyAction.WARN,
        ),
    )
    assert len(result.data) == 1
    for kind, before, after in zip(
        ("construction", "operation"), data[0]["exchanges"][1:], result.data[0]["exchanges"][1:], strict=True
    ):
        assert {f: after[f] for f in FIELDS} == APPROVAL["reverse_validation"][kind + "_target"]
        assert after["amount"] == amount and after["formula"] == before["formula"]
        assert after["loc"] == before["loc"] and after["scale"] == before["scale"]
    assert inventory.data == data == original


def test_loss_requires_acceptance_and_foreground_suppliers_remain_owned():
    data = batch(-1)
    before = deepcopy(data)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.12"))
    with pytest.raises(MigrationError):
        inventory.migrate_background(
            target_context("2025"), policy=MigrationPolicy(on_heuristic_mapping=PolicyAction.WARN)
        )
    assert inventory.data == before
    for source in SOURCES:
        record = dict(zip(FIELDS, source, strict=True))
        data.append({**record, "exchanges": [{**record, "type": "production", "amount": 1}]})
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.12")).migrate_background(
        target_context("2025"), policy=POLICY
    )
    assert result.data == data


@pytest.mark.parametrize("release", ("2025", "2026", "native2026"))
def test_slag_component_review_does_not_enable_consequential_conversion(release):
    data = batch(1)
    original = deepcopy(data)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.12", "consequential"))
    with pytest.raises(MigrationError, match="system models"):
        inventory.migrate_background(target_context(release), policy=MigrationPolicy.permissive())
    assert inventory.data == original


@pytest.mark.parametrize("version", ("3.13",))
def test_uninspected_patch_and_future_profiles_cannot_inherit_component_reviews(version):
    selected = select_profile_sign_rules(
        load_uvek_technosphere_resource(), TechnosphereProfile("ecoinvent", version, "cutoff")
    )
    chosen = [r for r in selected["replace"] if tuple(r["source"][f] for f in FIELDS) in SOURCES]
    assert len(chosen) == 9 and all(r["reference_flow_sign_review"]["status"] == "unresolved" for r in chosen)


def observed_evidence():
    records, observations, native = {}, {"ecoinvent": {}, "uvek": {}}, {}
    for row in APPROVAL["inventories"]:
        key = tuple(row["source"][f] for f in FIELDS)
        records.setdefault(key, []).append({**row, "source": list(key)})
        family = row["label"].split("-")[0]
        observations[family].setdefault(key, set()).add((row["label"], 1))
        if family == "uvek":
            waste = key[3] == "kilogram"
            native[key] = {
                "native_flow_type": "WASTE_FLOW" if waste else "PRODUCT_FLOW",
                "native_reference_is_input": waste,
            }
    return records, observations, native


@pytest.mark.parametrize(
    "mutation",
    (
        "inventory",
        "comment",
        "product_information",
        "missing",
        "duplicate",
        "source_sign",
        "target_sign",
        "native_role",
        "extra_profile",
    ),
)
def test_inventory_reference_native_role_or_exact_profile_drift_requires_new_review(mutation):
    records, observations, native = observed_evidence()
    validate_inventories(records, observations, native)
    source = next(iter(SOURCES))
    target = tuple(SOURCES[source]["target"][f] for f in FIELDS)
    if mutation in ("inventory", "comment", "product_information"):
        key = {
            "inventory": "inventory_sha256",
            "comment": "activity_comment_sha256",
            "product_information": "product_information_sha256",
        }[mutation]
        records[source][0][key] = "0" * 64
    elif mutation == "missing":
        records[source].pop()
    elif mutation == "duplicate":
        records[source].append(deepcopy(records[source][0]))
    elif mutation == "source_sign":
        observations["ecoinvent"][source] = {(label, -1) for label, _sign in observations["ecoinvent"][source]}
    elif mutation == "target_sign":
        observations["uvek"][target] = {("uvek-2025-cutoff", -1)}
    elif mutation == "extra_profile":
        observations["ecoinvent"][source].add(("ecoinvent-3.13-cutoff", 1))
    else:
        native[target]["native_reference_is_input"] = not native[target]["native_reference_is_input"]
    with pytest.raises(ValueError, match="repeat the scientific review"):
        validate_inventories(records, observations, native)


@pytest.mark.parametrize("mutation", ("target", "factor"))
def test_component_projection_preserves_owned_rule_and_rejects_unreviewed_changes(mutation):
    row = next(iter(SOURCES.values()))
    rule = {"source": deepcopy(row["source"]), "target": deepcopy(row["previous_target"])}
    saved = deepcopy(rule)
    result = approved_component_rule(rule)
    assert result["target"] == row["target"] and rule == saved
    if mutation == "target":
        rule["target"]["name"] = "Another landfill"
    else:
        rule["conversion_factor"] = -1
    with pytest.raises(ValueError, match="repeat the scientific review"):
        approved_component_rule(rule)
