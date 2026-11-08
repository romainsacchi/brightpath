"""Allocated sludge supply keeps freight without acquiring a disposal burden or credit."""

from collections import defaultdict
from copy import deepcopy

import pytest
from test_uvek_reference_signs import consumer, contexts
from test_uvek_refrigerant_disposal import target_context

from brightpath import (
    BackgroundContext,
    BiosphereProfile,
    BrightwayInventory,
    FormatProfile,
    InventoryContext,
    TechnosphereProfile,
)
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from scripts import review_uvek_digester_stock as review

POLICY = MigrationPolicy(on_heuristic_mapping=PolicyAction.WARN, on_information_loss=PolicyAction.WARN)
CASES = [(row, profile) for row in review.SOURCES.values() for profile in row["profiles"]]
STOCK = next(row for row in review.SOURCES.values() if row["kind"] == "stock")


def test_retired_parent_audit_replays_without_requiring_executable_retired_helpers():
    import json

    from brightpath.migrations.resources import load_uvek_technosphere_resource

    resource = load_uvek_technosphere_resource()
    ledger = json.loads((review.ROOT / "brightpath/data/export/uvek_reference_flow_sign_review.json").read_text())[
        "resources"
    ][resource["name"]]
    retired = review.APPROVAL["retired_helper_bodies"]
    assert len(retired) == 7 and not set(retired).intersection(resource["recipes"])
    review.validate_bindings(resource, ledger)
    for row in review.SOURCES.values():
        prior = ledger[row["id"]]
        regenerated = review.forward_decision(
            prior["original_rule"],
            set(map(tuple, prior["source_observations"])),
            prior["binding"],
            prior["original_review"],
            prior,
        )
        assert not set(retired).intersection(regenerated["recipes"])
        assert regenerated[review.MARKER]["base_decision"] == row["base_decision"]


@pytest.mark.parametrize("change", ("resurrect", "audit_body"))
def test_retired_digester_parent_cannot_change_or_return_to_executable_resource(change, monkeypatch):
    import json

    from brightpath.migrations.resources import load_uvek_technosphere_resource

    resource = deepcopy(load_uvek_technosphere_resource())
    ledger = json.loads((review.ROOT / "brightpath/data/export/uvek_reference_flow_sign_review.json").read_text())[
        "resources"
    ][resource["name"]]
    rid = next(iter(review.APPROVAL["retired_helper_bodies"]))
    if change == "resurrect":
        resource["recipes"][rid] = deepcopy(review.APPROVAL["retired_helper_bodies"][rid])
    else:
        changed = deepcopy(review.APPROVAL)
        changed["retired_helper_bodies"][rid]["inputs"][0]["amount"] += 1
        monkeypatch.setattr(review, "APPROVAL", changed)
    with pytest.raises(ValueError, match="retired digester-sludge helper changed or became executable"):
        review.validate_bindings(resource, ledger)


@pytest.mark.parametrize("row,profile", CASES)
@pytest.mark.parametrize("release", ("2025", "2026", "native2026"))
@pytest.mark.parametrize("amount", (-2, 0, 2))
def test_allocated_supply_preserves_quantity_freight_and_caller_state(row, profile, release, amount):
    version = profile["label"].split("-")[1]
    data = consumer(row["source"], amount, 2)
    data[0]["exchanges"][1]["caller_metadata"] = {"preserve": [1, 2]}
    data[0]["exchanges"].append(
        {
            "name": "Carbon dioxide, fossil",
            "categories": ["air"],
            "unit": "kilogram",
            "type": "biosphere",
            "amount": 0.5,
        }
    )
    before = deepcopy(data)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", version))
    result = inventory.migrate_background(target_context(release), policy=POLICY)
    final = result.data[0]["exchanges"][1]
    assert final["amount"] == amount
    assert all(final.get(k) == value for k, value in before[0]["exchanges"][1].items() if k not in review.FIELDS)
    assert inventory.data == data == before
    assert result.data[0]["exchanges"][-1]["amount"] == 0.5
    helpers = {d["code"]: d for d in result.data[1:]}
    stocks = [h for h in helpers.values() if h["brightpath_conversion"].get("zero_burden")]
    assert len(stocks) == 1
    stock = stocks[0]
    assert stock["reference product"] == "digester sludge"
    assert stock["exchanges"] == [{**{f: stock[f] for f in review.FIELDS}, "type": "production", "amount": 1}]
    assert all("incineration" not in e.get("name", "") for h in helpers.values() for e in h["exchanges"])
    assert any(i.code == "migration.zero_burden_supplier" for i in result.last_migration_report.issues)
    if row["kind"] == "market":
        parent = helpers[final["input"][1]]
        retained = [e for e in parent["exchanges"] if e["type"] != "production"]
        expected = profile["recipe"]["inputs"]
        assert len(retained) == len(expected)
        for actual, original in zip(retained, expected, strict=True):
            assert actual["amount"] == original["amount"]
            for field in (
                "uncertainty type",
                "loc",
                "scale",
                "minimum",
                "maximum",
                "negative",
                "pedigree",
                "scale without pedigree",
            ):
                if field in original:
                    assert actual[field] == original[field]
    assert result.migrate_background(target_context(release)).data == result.data


@pytest.mark.parametrize("mutation", ("none", "missing", "duplicate", "biosphere", "negative", "description"))
def test_allocation_boundary_requires_complete_exact_inventory_evidence(mutation):
    records = deepcopy(review.APPROVAL["inventories"])
    source = review.identity(STOCK["source"])
    record = next(r for r in records if review.identity(r["source"]) == source)
    if mutation == "missing":
        records.remove(record)
    elif mutation == "duplicate":
        records.append(deepcopy(record))
    elif mutation == "biosphere":
        record["total_exchanges"] = 2
        record["only_self_production"] = False
    elif mutation == "negative":
        record["reference_amount"] = record["production_amount"] = -1
    elif mutation == "description":
        record["activity_comment_sha256"] = "0" * 64
    observations = defaultdict(lambda: defaultdict(set))
    if mutation == "none":
        review.validate_inventories(records, observations)
        assert len(observations["ecoinvent"][source]) == 9
    else:
        with pytest.raises(ValueError, match="inventories or descriptions changed"):
            review.validate_inventories(records, observations)


def test_review_does_not_authorize_physical_treatment_or_incremental_digestion_proxy():
    assert all(
        row["source"]["name"] in ("digester sludge, Recycled Content cut-off", "market for digester sludge")
        for row in review.SOURCES.values()
    )
    assert review.APPROVAL["authorized_system_model"] == "cutoff"
    assert {p["label"].split("-")[-1] for _, p in CASES} == {"cutoff"}
    assert all(r["source"]["name"] != "treatment of biowaste by anaerobic digestion" for r in review.SOURCES.values())


@pytest.mark.parametrize("release", ("2025", "2026", "native2026"))
def test_native_incineration_cannot_inherit_the_inverse_stock_allocation(release):
    data = consumer(review.APPROVAL["native_role"]["source"], -2, 2)
    before = deepcopy(data)
    inventory = BrightwayInventory.from_data(
        data, context=InventoryContext(FormatProfile("brightway_excel"), target_context(release))
    )
    target = BackgroundContext(
        TechnosphereProfile("ecoinvent", "3.12", "cutoff"), BiosphereProfile("ecoinvent", "3.12")
    )
    with pytest.raises(MigrationError, match="No reviewed directional correspondence"):
        inventory.migrate_background(target, policy=POLICY)
    assert inventory.data == data == before


@pytest.mark.parametrize("native", (False, True))
@pytest.mark.parametrize("format_name", ("brightway", "simapro"))
def test_material_market_exports_retain_stock_freight_and_uncertainty(tmp_path, native, format_name):
    from brightpath import SimaProInventory

    row = next(r for r in review.SOURCES.values() if r["source"]["location"] == "CH")
    data = consumer(row["source"], -2, 3)
    data[0]["parameters"] = [{"name": "dose", "amount": -2}]
    data[0]["comment"] = "Synthetic allocated-sludge supply fixture."
    converted = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.12")).migrate_background(
        target_context("native2026" if native else "2025"), policy=POLICY
    )
    if format_name == "brightway":
        path = converted.write_excel(tmp_path / "allocated.xlsx")
        restored = BrightwayInventory.from_excel(path)
    else:
        categorized = deepcopy(converted.data)
        for dataset in categorized:
            dataset["exchanges"][0]["simapro category"] = "Materials/Allocated stock"
        path = (
            BrightwayInventory.from_data(categorized, context=converted.context)
            .to_simapro()
            .write_csv(tmp_path / "allocated.csv")
        )
        restored = SimaProInventory.from_csv(
            path,
            background_profile=converted.background_profile,
            biosphere_profile=converted.context.background.biosphere,
        ).to_brightway()
    assert not restored.validate().has_errors
    by_identity = {review.identity(d): d for d in restored.data}
    for original in converted.data[1:]:
        final = by_identity[review.identity(original)]
        if original["brightpath_conversion"].get("zero_burden"):
            assert len(final["exchanges"]) == 1
            assert final["exchanges"][0]["type"] == "production" and final["exchanges"][0]["amount"] == 1
        else:
            before = [e for e in original["exchanges"] if e["type"] == "technosphere"]
            after = [e for e in final["exchanges"] if e["type"] == "technosphere"]
            assert len(before) == len(after)
            for old, new in zip(before, after, strict=True):
                assert review.identity(new) == review.identity(old)
                assert new["amount"] == pytest.approx(old["amount"])
                if old.get("uncertainty type") == 2:
                    assert new["uncertainty type"] == 2
                    assert new["scale"] == pytest.approx(old["scale"])
