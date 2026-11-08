"""Independent reverse combustion boundaries and complete gas conversion routes."""

from copy import deepcopy

import pytest
from test_uvek_reference_signs import consumer, contexts
from test_uvek_refrigerant_disposal import target_context

from brightpath import BrightwayInventory, FormatProfile, InventoryContext
from brightpath.background.uvek_export import load_uvek_export_resource
from brightpath.core.policies import MigrationPolicy
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_release_resources
from scripts.generate_uvek_export import legacy_export_projection
from scripts.review_uvek_reverse_gas import (
    APPROVAL,
    FIELDS,
    METHOD,
    additions,
    apply_reverse_gas,
    identity,
    legacy_projection,
    validate_reverse_gas_inventories,
)

POLICY = MigrationPolicy.permissive()


def batch(amount):
    data = None
    for rule in APPROVAL["rules"]:
        for kind, uncertainty in (("technosphere", 2), ("substitution", 3)):
            row = consumer(rule["source"], amount, uncertainty, kind)
            row[0]["exchanges"][1]["caller_metadata"] = {"retain": [1, 2]}
            if data is None:
                data = row
            else:
                data[0]["exchanges"].append(row[0]["exchanges"][1])
    return data


def native_data(kind, data):
    if kind == "2025":
        return data
    bridge = next(
        r for r in load_uvek_release_resources("technosphere") if r["source_profiles"][0]["version"] == "2025"
    )
    mapping = {identity(r["source"]): r["target"] for r in bridge["replace"]}
    for exchange in data[0]["exchanges"][1:]:
        exchange.update(mapping[identity(exchange)])
    return data


@pytest.mark.parametrize("kind", ["2025", "2026", "native2026"])
@pytest.mark.parametrize("amount", [-10, 0, 10])
def test_reverse_gas_profiles_preserve_signed_quantity_uncertainty_formula_and_full_changes(kind, amount):
    data = native_data(kind, batch(amount))
    before = deepcopy(data)
    # Native source profiles are explicit; no background migration occurs at load time.
    inventory = BrightwayInventory.from_data(
        data, context=InventoryContext(FormatProfile("brightway_excel"), target_context(kind))
    )
    result = inventory.migrate_background(contexts("ecoinvent", "3.12").background, policy=POLICY)
    assert len(result.data) == 3
    assert result.data[0]["exchanges"][0] == before[0]["exchanges"][0]
    helpers = {identity(d): d for d in result.data[1:]}
    changes = {c.path: c for c in result.last_migration_report.changes if c.code == "migration.uvek_supplier"}
    assert len(changes) == 2 * len(APPROVAL["rules"]) == 84
    for index, (old, new) in enumerate(
        zip(before[0]["exchanges"][1:], result.data[0]["exchanges"][1:], strict=True), 1
    ):
        row = APPROVAL["rules"][(index - 1) // 2]
        if row["kind"] == "matching_service_proxy":
            assert new["amount"] == -old["amount"] and new["type"] == old["type"]
            assert new["formula"] == "(dose) * (-1.0)" and new["caller_metadata"] == old["caller_metadata"]
            assert new["scale"] == old["scale"]
            if old["uncertainty type"] == 2:
                assert new["loc"] == old["loc"] and new["negative"] is (amount >= 0)
            else:
                assert new["loc"] == -amount
                assert (new["minimum"], new["maximum"]) == (-amount - 1, -amount + 1)
        for field in (
            "amount",
            "type",
            "uncertainty type",
            "formula",
            "loc",
            "scale",
            "negative",
            "minimum",
            "maximum",
            "caller_metadata",
        ):
            if row["kind"] != "matching_service_proxy":
                assert new.get(field) == old.get(field)
        recorded = changes[f"datasets[0].exchanges[{index}]"].to_dict()["before"]
        # The release bridge additionally normalizes the product alias.
        assert {k: recorded[k] for k in old} == old
        if row["kind"] == "reconstruction":
            helper = helpers[identity(new)]
            assert helper["exchanges"][0]["type"] == "production" and helper["exchanges"][0]["amount"] == 1
            observed = [{k: e[k] for k in (*FIELDS, "amount", "type")} for e in helper["exchanges"][1:]]
            assert observed == row["inputs"]
            assert helper["location"] == "DE" and helper["unit"] == "megajoule"
        else:
            assert identity(new) == identity(row["target"])
    warnings = [i for i in result.last_migration_report.issues if i.code == "migration.documented_proxy"]
    assert len(warnings) == 84
    assert any("RER-to-DE" in i.message and "retain coke-oven-gas supply" in i.message for i in warnings)
    assert inventory.data == data == before
    assert result.migrate_background(result.context.background).data == result.data


def test_reverse_reconstruction_requires_proxy_policy_and_protects_foreground():
    data = batch(-1)
    original = deepcopy(data)
    inventory = BrightwayInventory.from_data(data, context=contexts("uvek", "2025"))
    with pytest.raises(MigrationError):
        inventory.migrate_background(contexts("ecoinvent", "3.12").background)
    assert inventory.data == original
    for row in APPROVAL["rules"]:
        source = row["source"]
        data.append({**source, "exchanges": [{**source, "amount": 1, "type": "production"}]})
    inventory = BrightwayInventory.from_data(data, context=contexts("uvek", "2025"))
    assert inventory.migrate_background(contexts("ecoinvent", "3.12").background).data == data


@pytest.mark.parametrize("change", ["source_hash", "target_hash", "source_profile", "missing_target"])
def test_reverse_inventory_changes_require_independent_review(change):
    inventories = {
        identity(r["source"]): [("uvek-2025-cutoff", r["source_inventory_sha256"])] for r in APPROVAL["rules"]
    }
    inventories.update(
        {
            identity(r["target"]): [("ecoinvent-3.12-cutoff", r["inventory_sha256"])]
            for r in APPROVAL["target_inventories"]
        }
    )
    key = identity(APPROVAL["rules"][0]["source"])
    if change == "source_hash":
        inventories[key] = [("uvek-2025-cutoff", "0" * 64)]
    elif change == "source_profile":
        inventories[key][0] = ("uvek-2026-cutoff", inventories[key][0][1])
    else:
        key = identity(APPROVAL["target_inventories"][0]["target"])
        if change == "missing_target":
            inventories.pop(key)
        else:
            inventories[key] = [("ecoinvent-3.12-cutoff", "0" * 64)]
    with pytest.raises(ValueError, match="repeat the independent review"):
        validate_reverse_gas_inventories(inventories)


@pytest.mark.parametrize("change", ["rule", "recipe", "review", "missing_rule", "unknown_source"])
def test_later_reverse_additions_cannot_weaken_the_publisher_baseline(change):
    resource = deepcopy(load_uvek_export_resource())
    row = next(r for r in resource["replace"] if r.get("mapping_method") == METHOD and "recipe_ref" in r["target"])
    if change == "rule":
        row["conversion_factor"] = -1
    elif change == "recipe":
        resource["recipes"][row["target"]["recipe_ref"]]["inputs"][0]["amount"] = 100
    elif change == "review":
        resource["reviews"][row["review_ref"]]["rationale"] = "Unreviewed"
    elif change == "missing_rule":
        resource["replace"].remove(row)
    else:
        row["source"]["name"] = "Unreviewed"
    with pytest.raises(ValueError):
        legacy_projection(resource)


def test_reverse_additions_keep_all_original_dispositions_and_validate_catalog_dependencies():
    resource = load_uvek_export_resource()
    projected = legacy_export_projection(resource)
    assert len(projected["replace"]) == 1423 and len(projected["recipes"]) == 7
    rules, recipes, _ = additions()
    assert len(rules) == 42 and len(recipes) == 2
    sources = {identity(r["source"]) for r in rules}
    targets = {identity(r["target"]) for r in APPROVAL["target_inventories"]}
    for row in APPROVAL["rules"][:2]:
        assert row["weighted_fuel_mj"] == pytest.approx(1)
    with pytest.raises(ValueError, match="dependency is missing"):
        apply_reverse_gas(
            deepcopy(projected), [], sources, targets - {identity(APPROVAL["target_inventories"][0]["target"])}
        )
