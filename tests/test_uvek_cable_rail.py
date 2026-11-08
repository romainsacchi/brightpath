"""Inspected cable and rail services retain signs, caller data and complete processing."""

from collections import defaultdict
from copy import deepcopy

import pytest
from test_uvek_reference_signs import consumer, contexts
from test_uvek_refrigerant_disposal import target_context

from brightpath import BrightwayInventory
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from scripts import review_uvek_cable_rail as review

POLICY = MigrationPolicy(
    on_heuristic_mapping=PolicyAction.WARN, on_deletion=PolicyAction.ERROR, on_information_loss=PolicyAction.WARN
)
CASES = [p for row in review.APPROVAL["root_rules"] for p in row["profiles"]]
VERSIONS = sorted({p["label"].split("-")[1] for p in CASES})


def batch(version, amount):
    rows = [p for p in CASES if p["label"] == f"ecoinvent-{version}-cutoff"]
    data = None
    expected = []
    for row in rows:
        for kind, uncertainty in (("technosphere", 2), ("substitution", 3)):
            part = consumer(row["source"], amount, uncertainty, kind)
            part[0]["exchanges"][1]["caller_metadata"] = {"retain": [1, 2]}
            if data is None:
                data = part
            else:
                data[0]["exchanges"].append(part[0]["exchanges"][1])
            expected.append(row)
    data[0]["exchanges"].append(
        {
            "name": "Carbon dioxide, fossil",
            "categories": ["air"],
            "unit": "kilogram",
            "type": "biosphere",
            "amount": 0.5,
        }
    )
    return data, expected


@pytest.mark.parametrize("version", VERSIONS)
@pytest.mark.parametrize("release", ("2025", "2026", "native2026"))
@pytest.mark.parametrize("amount", (-2, 0, 2))
def test_complete_services_preserve_signed_uncertainty_formulas_caller_fields_and_emissions(version, release, amount):
    data, expected = batch(version, amount)
    original = deepcopy(data)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", version))
    result = inventory.migrate_background(target_context(release), policy=POLICY)
    assert len(result.data) > 1
    assert result.data[0]["exchanges"][0] == original[0]["exchanges"][0]
    assert len(result.data[0]["exchanges"]) == len(original[0]["exchanges"])
    for old, new, row in zip(original[0]["exchanges"][1:-1], result.data[0]["exchanges"][1:-1], expected, strict=True):
        factor = row["conversion_factor"]
        assert new["amount"] == amount * factor and new["type"] == old["type"]
        assert new["caller_metadata"] == old["caller_metadata"]
        assert new["formula"] == ("dose" if factor == 1 else "(dose) * (-1.0)")
        assert new["scale"] == old["scale"] == 0.2
        if old["uncertainty type"] == 2:
            assert new["loc"] == old["loc"]
            assert new["negative"] == (old["negative"] if factor == 1 else not old["negative"])
        else:
            assert new["loc"] == amount * factor
            assert new["minimum"] == amount * factor - 1 and new["maximum"] == amount * factor + 1
    emission = result.data[0]["exchanges"][-1]
    assert emission["type"] == "biosphere" and emission["unit"] == "kilogram" and emission["amount"] == 0.5
    if release != "native2026":
        assert emission == original[0]["exchanges"][-1]
    assert not any(r.get("brightpath_conversion", {}).get("zero_burden") for r in result.data[1:])
    assert any("FAE" in i.message or "ash" in i.message for i in result.last_migration_report.issues)
    assert inventory.data == data == original
    assert result.migrate_background(target_context(release)).data == result.data


@pytest.mark.parametrize("version", VERSIONS)
def test_cable_helper_contains_processing_and_wire_incineration_without_foreign_stock_or_collection_boundary(version):
    row = next(
        p for p in CASES if p["label"] == f"ecoinvent-{version}-cutoff" and p["kind"] == "cable-treatment-composition"
    )
    result = BrightwayInventory.from_data(
        consumer(row["source"], -1), context=contexts("ecoinvent", version)
    ).migrate_background(target_context("2025"), policy=POLICY)
    helpers = {review.identity(r): r for r in result.data[1:]}
    root = next(r for r in result.data[1:] if r["brightpath_conversion"]["recipe_id"] == row["target"]["recipe_ref"])
    totals = defaultdict(float)

    def walk(dataset, scale=1):
        for exchange in dataset["exchanges"]:
            if exchange["type"] == "production":
                assert exchange["amount"] == 1
                continue
            child = helpers.get(review.identity(exchange))
            if child is not None:
                walk(child, scale * exchange["amount"])
            else:
                totals[review.identity(exchange)] += scale * exchange["amount"]

    walk(root)
    assert sum(v for k, v in totals.items() if k[0] == "Electricity, medium voltage, at grid") == pytest.approx(
        0.18, abs=0
    )
    assert sum(v for k, v in totals.items() if k[0] == "Manual treatment plant, WEEE scrap") == pytest.approx(
        4e-10, abs=0
    )
    assert sum(
        v for k, v in totals.items() if k[0] == "Disposal, wire plastic, 3.55% water, to municipal incineration"
    ) == pytest.approx(0.34, rel=1e-14, abs=0)
    assert 0 < sum(v for k, v in totals.items() if k[3] == "ton kilometer") < 0.05
    forbidden = {
        "xx Copper, secondary, from cable treatment, at plant",
        "Disposal, plastic conduits, as building waste",
        "Disposal, treatment of cables",
        "xxx Operation, freight train",
    }
    assert not any(k[0] in forbidden for k in totals)


@pytest.mark.parametrize("version", VERSIONS)
def test_local_service_ownership_and_cross_model_refusals(version):
    data, _ = batch(version, -2)
    data[0]["exchanges"] = data[0]["exchanges"][:-1]
    for row in CASES:
        if row["label"] == f"ecoinvent-{version}-cutoff":
            data.append(
                {
                    **row["source"],
                    "exchanges": [{**row["source"], "amount": row["source_reference"], "type": "production"}],
                }
            )
    original = deepcopy(data)
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", version)).migrate_background(
        target_context("2025"), policy=POLICY
    )
    assert result.data == data == original
    with pytest.raises(MigrationError, match="system models"):
        BrightwayInventory.from_data(data, context=contexts("ecoinvent", version, "consequential")).migrate_background(
            target_context("2025"), policy=MigrationPolicy.permissive()
        )


@pytest.mark.parametrize("change", ("none", "missing", "duplicate", "reference", "inventory", "description"))
def test_complete_source_parent_and_native_evidence_is_pinned(change):
    evidence = defaultdict(list)
    for row in review.APPROVAL["inventories"]:
        evidence[review.identity(row["source"])].append(
            {**deepcopy(row), "source": list(review.identity(row["source"]))}
        )
    source = next(iter(evidence))
    if change == "missing":
        evidence[source].pop()
    elif change == "duplicate":
        evidence[source].append(deepcopy(evidence[source][0]))
    elif change == "reference":
        evidence[source][0]["reference_amount"] *= -1
    elif change == "inventory":
        evidence[source][0]["inventory_sha256"] = "0" * 64
    elif change == "description":
        evidence[source][0]["activity_comment_sha256"] = "0" * 64
    if change == "none":
        review.validate_inventories(evidence)
    else:
        with pytest.raises(ValueError, match="repeat the scientific review"):
            review.validate_inventories(evidence)


def test_caller_can_reject_proxy_information_loss_transactionally():
    row = next(p for p in CASES if p["label"] == "ecoinvent-3.12-cutoff" and p["kind"] == "cable-treatment-composition")
    data = consumer(row["source"], -1)
    original = deepcopy(data)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.12"))
    with pytest.raises(MigrationError) as caught:
        inventory.migrate_background(
            target_context("2025"),
            policy=MigrationPolicy(on_heuristic_mapping=PolicyAction.WARN, on_information_loss=PolicyAction.ERROR),
        )
    assert any(i.code == "migration.technosphere_information_loss" for i in caught.value.report.issues)
    assert inventory.data == data == original


@pytest.mark.parametrize("version", VERSIONS)
@pytest.mark.parametrize("release", ("2025", "2026", "native2026"))
@pytest.mark.parametrize("amount", (-2, 0, 2))
def test_every_affected_parent_uses_its_reviewed_helper_and_preserves_caller_data(version, release, amount):
    rows = [p for p in review.APPROVAL["parent_profiles"] if p["label"] == f"ecoinvent-{version}-cutoff"]
    data = consumer(rows[0]["source"], amount)
    data[0]["exchanges"] = data[0]["exchanges"][:1]
    expected = []
    for row in rows:
        for kind, uncertainty in (("technosphere", 2), ("substitution", 3)):
            exchange = consumer(row["source"], amount, uncertainty, kind)[0]["exchanges"][1]
            exchange["caller_metadata"] = {"retain": [1, 2]}
            data[0]["exchanges"].append(exchange)
            expected.append(row)
    data[0]["exchanges"].append(
        {
            "name": "Carbon dioxide, fossil",
            "categories": ["air"],
            "unit": "kilogram",
            "type": "biosphere",
            "amount": 0.5,
        }
    )
    original = deepcopy(data)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", version))
    result = inventory.migrate_background(target_context(release), policy=POLICY)
    helpers = {r["code"]: r for r in result.data[1:]}
    assert result.data[0]["exchanges"][0] == original[0]["exchanges"][0]
    for old, new, row in zip(original[0]["exchanges"][1:-1], result.data[0]["exchanges"][1:-1], expected, strict=True):
        factor = row["conversion_factor"]
        assert factor in (-1, 1)
        assert new["amount"] == amount * factor
        assert new["type"] == old["type"]
        assert new["caller_metadata"] == old["caller_metadata"]
        assert new["formula"] == ("dose" if factor == 1 else "(dose) * (-1.0)")
        assert new["scale"] == old["scale"] == 0.2
        if old["uncertainty type"] == 2:
            assert new["loc"] == old["loc"]
            assert new["negative"] == (old["negative"] if factor == 1 else not old["negative"])
        else:
            assert new["loc"] == amount * factor
            assert new["minimum"] == amount * factor - 1
            assert new["maximum"] == amount * factor + 1
        helper = helpers[new["input"][1]]
        expected_recipe = row["target"]["recipe_ref"]
        from scripts.review_uvek_digester_stock import APPROVAL as DIGESTER_APPROVAL
        from scripts.review_uvek_digester_stock import SOURCES as DIGESTER_SOURCES

        if expected_recipe in DIGESTER_APPROVAL["retired_helper_bodies"]:
            # The cable audit retains historical rail-ancestor rows. The later
            # allocated-stock review deliberately retired their incineration helpers.
            replacement = DIGESTER_SOURCES[review.identity(row["source"])]
            assert replacement["kind"] == "market"
            (approved,) = [p for p in replacement["profiles"] if p["label"] == row["label"]]
            expected_recipe = approved["recipe_id"]
        assert helper["brightpath_conversion"]["recipe_id"] == expected_recipe
        assert not helper["brightpath_conversion"].get("zero_burden")
        production = [e for e in helper["exchanges"] if e["type"] == "production"]
        assert len(production) == 1 and production[0]["amount"] == 1
    emission = result.data[0]["exchanges"][-1]
    assert emission["type"] == "biosphere" and emission["amount"] == 0.5
    if release != "native2026":
        assert emission == original[0]["exchanges"][-1]
    assert inventory.data == data == original
    assert result.migrate_background(target_context(release)).data == result.data


@pytest.mark.parametrize("version", VERSIONS)
def test_repaired_parents_respect_foreground_supplier_ownership(version):
    rows = [p for p in review.APPROVAL["parent_profiles"] if p["label"] == f"ecoinvent-{version}-cutoff"]
    data = consumer(rows[0]["source"], -2)
    data[0]["exchanges"] = data[0]["exchanges"][:1]
    for row in rows:
        data[0]["exchanges"].append(consumer(row["source"], -2)[0]["exchanges"][1])
        data.append(
            {**row["source"], "exchanges": [{**row["source"], "type": "production", "amount": row["source_reference"]}]}
        )
    original = deepcopy(data)
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", version)).migrate_background(
        target_context("2025"), policy=POLICY
    )
    assert result.data == data == original


@pytest.mark.parametrize("version", ("3.9.2", "3.10.2"))
def test_new_helpers_cannot_supply_uninspected_patch_versions(version):
    import json

    from brightpath import TechnosphereProfile
    from brightpath.background.reference_signs import select_profile_sign_rules

    resource = json.loads((review.ROOT / "brightpath/data/migrations/uvek/ecoinvent-to-uvek-2025.json").read_text())
    selected = select_profile_sign_rules(resource, TechnosphereProfile("ecoinvent", version, "cutoff"))
    rules = {review.identity(r["source"]): r for r in selected["replace"]}
    for source in review.SOURCES:
        assert rules[source]["reference_flow_sign_review"]["status"] == "unresolved"
