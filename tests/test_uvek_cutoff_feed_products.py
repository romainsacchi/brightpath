"""Cut-off physical coproduct supply is independent of consequential nutritional omissions."""

import json
from collections import defaultdict
from copy import deepcopy
from pathlib import Path

import pytest
from test_uvek_reference_signs import FIELDS, consumer, contexts
from test_uvek_refrigerant_disposal import target_context
from uvek_review_helpers import historical_forward_resource

from brightpath import BrightwayInventory, SimaProInventory, TechnosphereProfile
from brightpath.background.reference_signs import select_profile_sign_rules
from brightpath.core.policies import MigrationPolicy
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_release_resources, load_uvek_technosphere_resource
from scripts.review_uvek_cutoff_feed_products import (
    APPROVAL,
    MARKETS,
    PRODUCTS,
    REVERSE_PRODUCTS,
    approved_product_rule,
    legacy_reverse_projection,
    reverse_additions,
    reverse_decision,
    validate_inventories,
    validate_market_bindings,
)
from scripts.review_uvek_reference_signs import REVIEW_PATH, apply_sign_review

POLICY = MigrationPolicy.permissive()
VERSIONS = ["3.6", "3.7", "3.8", "3.9", "3.10", "3.11", "3.12"]
ROOT = Path(__file__).resolve().parents[1]


def batch(amount, version="3.12"):
    data = None
    active_products = [
        source
        for source, row in PRODUCTS.items()
        if any(p["label"] == f"ecoinvent-{version}-cutoff" for p in row["profiles"])
    ]
    for source in [*active_products, *MARKETS]:
        for kind, uncertainty in [("technosphere", 2), ("substitution", 3)]:
            part = consumer(dict(zip(FIELDS, source, strict=True)), amount, uncertainty, kind)
            part[0]["exchanges"][1]["caller_metadata"] = {"preserve": [1, 2]}
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


@pytest.mark.parametrize("version", VERSIONS)
@pytest.mark.parametrize("release", ["2025", "native2026"])
@pytest.mark.parametrize("amount", [-10, 0, 10])
def test_physical_coproducts_and_markets_keep_signed_kg_uncertainty_formulas_and_components(version, release, amount):
    data = batch(amount, version)
    before = deepcopy(data)
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", version, "cutoff")).migrate_background(
        target_context(release), policy=POLICY
    )
    helpers = {tuple(d[f] for f in FIELDS): d for d in result.data[1:]}
    assert len(helpers) == 5 and len(result.data) == 6
    bridge = None
    if release != "2025":
        bridge = next(
            r for r in load_uvek_release_resources("technosphere") if r["source_profiles"][0]["version"] == "2025"
        )

    def released(target):
        if bridge is None:
            return target
        return next(r["target"] for r in bridge["replace"] if r["source"] == target)

    for old, new in zip(before[0]["exchanges"][1:-1], result.data[0]["exchanges"][1:-1], strict=True):
        for field in ["amount", "type", "loc", "scale", "minimum", "maximum", "formula", "negative", "caller_metadata"]:
            if field in old:
                assert new[field] == old[field]
        source = tuple(old[f] for f in FIELDS)
        if source in PRODUCTS:
            expected = released(PRODUCTS[source]["target"])
            assert tuple(new[f] for f in FIELDS) == tuple(expected[f] for f in FIELDS)
        else:
            helper = helpers[tuple(new[f] for f in FIELDS)]
            profile = next(p for p in MARKETS[source]["profiles"] if p["label"] == f"ecoinvent-{version}-cutoff")
            expected = [
                (tuple(released({k: e[k] for k in FIELDS})[f] for f in FIELDS), e["amount"]) for e in profile["inputs"]
            ]
            observed = [(tuple(e[f] for f in FIELDS), e["amount"]) for e in helper["exchanges"][1:]]
            assert sorted(observed) == sorted(expected)
            assert sum(q for key, q in observed if key[3] == "kilogram") == pytest.approx(1)
    assert result.data[0]["exchanges"][0] == before[0]["exchanges"][0]
    expected_emission = deepcopy(before[0]["exchanges"][-1])
    if release != "2025":
        biosphere_bridge = json.loads(
            (ROOT / "brightpath/data/migrations/uvek/ecoinvent-3.10-biosphere-to-uvek-2026-biosphere.json").read_text()
        )
        native_target = next(
            r["target"]
            for r in biosphere_bridge["replace"]
            if r["source"] == {k: expected_emission[k] for k in ["name", "categories", "unit"]}
        )
        expected_emission.update(native_target)
    assert result.data[0]["exchanges"][-1] == expected_emission
    assert data == before
    assert not any(i.code == "migration.technosphere_omission" for i in result.last_migration_report.issues)
    assert any("wet-basis" in i.message for i in result.last_migration_report.issues)
    assert result.migrate_background(target_context(release)).data == result.data


def test_historical_recipe_replay_restores_conditional_omission_evidence_from_a_fresh_authoring_resource():
    resource = deepcopy(historical_forward_resource())
    before = deepcopy(resource)
    refs = {
        e["review_ref"]
        for row in MARKETS.values()
        for e in row["original_rule"]["reference_flow_sign_review"]["profiles"]
        if e.get("operation") == "delete"
    }
    assert len(refs) == 5
    for reference in refs:
        resource["reviews"].pop(reference)
    ledger = json.loads((ROOT / REVIEW_PATH).read_text())
    apply_sign_review(resource, ledger)
    assert resource == before


def test_unknown_context_strict_policy_and_foreground_ownership_remain_explicit():
    data = batch(-10)
    before = deepcopy(data)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.12", "cutoff"))
    with pytest.raises(MigrationError):
        inventory.migrate_background(target_context("2025"))
    assert inventory.data == before
    for source in [*PRODUCTS, *MARKETS]:
        identity = dict(zip(FIELDS, source, strict=True))
        data.append({**identity, "exchanges": [{**identity, "type": "production", "amount": 1}]})
    protected = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.12", "cutoff")).migrate_background(
        target_context("2025"), policy=POLICY
    )
    assert protected.data == data
    selected = select_profile_sign_rules(
        load_uvek_technosphere_resource(), TechnosphereProfile("ecoinvent", "3.10.1", "cutoff")
    )
    assert all(
        r["reference_flow_sign_review"]["status"] == "unresolved"
        for r in selected["replace"]
        if tuple(r["source"][f] for f in FIELDS) in {*PRODUCTS, *MARKETS}
    )


def inspection_evidence():
    inventories, observations, native = defaultdict(list), {"uvek": defaultdict(set)}, {}
    for row in APPROVAL["inventories"]:
        source = tuple(row["source"][f] for f in FIELDS)
        inventories[source].append(
            {
                "source": list(source),
                **{
                    k: row[k]
                    for k in ["label", "inventory_sha256", "activity_comment_sha256"]
                    + (["product_information_sha256"] if "product_information_sha256" in row else [])
                },
            }
        )
    for row in APPROVAL["targets"]:
        target = tuple(row["target"][f] for f in FIELDS)
        observations["uvek"][target].add(("uvek-2025-cutoff", 1))
        native[target] = {"native_flow_type": "PRODUCT_FLOW", "native_reference_is_input": False}
    return inventories, observations, native


@pytest.mark.parametrize("change", ["none", "hash", "comment", "product_information", "missing", "duplicate", "native"])
def test_scientific_inventory_comment_and_product_role_guards(change):
    inventories, observations, native = inspection_evidence()
    rows = next(iter(inventories.values()))
    if change == "none":
        validate_inventories(inventories, observations, native)
        return
    if change in ["hash", "comment"]:
        rows[0]["inventory_sha256" if change == "hash" else "activity_comment_sha256"] = "0" * 64
    elif change == "product_information":
        row = next(r for rows in inventories.values() for r in rows if "product_information_sha256" in r)
        row["product_information_sha256"] = "0" * 64
    elif change == "missing":
        rows.pop()
    elif change == "duplicate":
        rows.append(deepcopy(rows[0]))
    else:
        next(iter(native.values()))["native_flow_type"] = "WASTE_FLOW"
    with pytest.raises(ValueError, match="Cut-off coproduct"):
        validate_inventories(inventories, observations, native)


def test_changed_supplier_and_market_component_cannot_inherit_the_review():
    row = next(iter(PRODUCTS.values()))
    rule = deepcopy(row["original_rule"])
    rule["target"]["name"] = "Unreviewed ethanol"
    with pytest.raises(ValueError, match="repeat the independent review"):
        approved_product_rule(rule)
    resource = deepcopy(load_uvek_technosphere_resource())
    rule = next(r for r in resource["replace"] if tuple(r["source"][f] for f in FIELDS) in PRODUCTS)
    rule["confidence"] = -1
    with pytest.raises(ValueError, match="repeat the independent review"):
        validate_market_bindings(resource)


@pytest.mark.parametrize("release", ["2025", "native2026"])
def test_coproduct_market_helpers_survive_excel_and_simapro_exports(release, tmp_path):
    source = dict(zip(FIELDS, next(iter(MARKETS)), strict=True))
    data = consumer(source, -10)
    data[0]["comment"] = "Cut-off coproduct market export regression."
    data[0]["exchanges"][1].pop("formula")
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.12", "cutoff")).migrate_background(
        target_context(release), policy=POLICY
    )
    xlsx = BrightwayInventory.from_excel(result.write_excel(tmp_path / "coproduct.xlsx"))
    categorized = deepcopy(result.data)
    for dataset in categorized:
        dataset["exchanges"][0]["simapro category"] = "Materials/Feed coproducts"
    csv = (
        BrightwayInventory.from_data(categorized, context=result.context)
        .to_simapro()
        .write_csv(tmp_path / "coproduct.csv")
    )
    simapro = SimaProInventory.from_csv(
        csv, background_profile=result.background_profile, biosphere_profile=result.context.background.biosphere
    ).to_brightway()
    for restored in [xlsx, simapro]:
        assert len(restored.data) == 2 and not restored.validate().has_errors
        assert restored.data[0]["exchanges"][1]["amount"] == -10
        assert any("DDGS" in e["name"] for e in restored.data[1]["exchanges"])


@pytest.mark.parametrize("source", list(REVERSE_PRODUCTS))
@pytest.mark.parametrize("release", ["2025", "native2026"])
@pytest.mark.parametrize("amount", [-1, 0, 1])
def test_independently_reviewed_reverse_products_keep_signed_kg_uncertainty_and_formulas(source, release, amount):
    from brightpath import FormatProfile, InventoryContext

    target = dict(zip(FIELDS, source, strict=True))
    if release != "2025":
        bridge = next(
            r for r in load_uvek_release_resources("technosphere") if r["source_profiles"][0]["version"] == "2025"
        )
        target = next(r["target"] for r in bridge["replace"] if r["source"] == target)
    data = consumer(target, amount, 3)
    before = deepcopy(data)
    inventory = BrightwayInventory.from_data(
        data, context=InventoryContext(FormatProfile("brightway_excel"), target_context(release))
    )
    result = inventory.migrate_background(contexts("ecoinvent", "3.12").background, policy=POLICY)
    observed = result.data[0]["exchanges"][1]
    assert tuple(observed[f] for f in FIELDS) == tuple(REVERSE_PRODUCTS[source]["target"][f] for f in FIELDS)
    for field in ["amount", "loc", "scale", "minimum", "maximum", "formula"]:
        assert observed[field] == before[0]["exchanges"][1][field]
    warnings = [i for i in result.last_migration_report.issues if i.code == "migration.documented_proxy"]
    assert len(warnings) == 1 and warnings[0].severity.value == "warning"
    assert "wet-basis" in warnings[0].message and "allocation" in warnings[0].message
    assert any(i.code == "migration.documented_proxy" for i in result.last_migration_report.losses)
    assert inventory.data == data == before


@pytest.mark.parametrize("change", ["target", "sign", "native", "review", "partial"])
def test_reverse_product_boundaries_and_frozen_baseline_projection_remain_strict(change):
    rules, reviews = reverse_additions()
    if change in ["target", "sign", "native"]:
        rule = deepcopy(rules[0])
        observations = {("ecoinvent-3.12-cutoff", 1)}
        native = {"native_flow_type": "PRODUCT_FLOW", "native_reference_is_input": False}
        if change == "target":
            rule["target"]["reference product"] = "ethanol, without water, in 95% solution state"
        elif change == "sign":
            observations = {("ecoinvent-3.12-cutoff", -1)}
        else:
            native["native_flow_type"] = "WASTE_FLOW"
        with pytest.raises(ValueError, match="Independent reverse coproduct"):
            reverse_decision(rule, observations, {1}, native, "synthetic")
    else:
        path = ROOT / "brightpath/data/migrations/uvek/uvek-2025-to-ecoinvent-3.12-cutoff.json"
        resource = json.loads(path.read_text())
        if change == "review":
            resource["reviews"][rules[0]["review_ref"]]["rationale"] = "Changed"
        else:
            resource["replace"] = [r for r in resource["replace"] if r["id"] != rules[0]["id"]]
        with pytest.raises(ValueError, match="Stale independent reverse coproduct"):
            legacy_reverse_projection(resource)


def test_reverse_proxy_policy_and_foreground_ownership_remain_transactional():
    data = None
    for source in REVERSE_PRODUCTS:
        part = consumer(dict(zip(FIELDS, source, strict=True)), 1)
        if data is None:
            data = part
        else:
            data[0]["exchanges"].append(part[0]["exchanges"][1])
    before = deepcopy(data)
    inventory = BrightwayInventory.from_data(data, context=contexts("uvek", "2025"))
    with pytest.raises(MigrationError) as caught:
        inventory.migrate_background(contexts("ecoinvent", "3.12").background)
    assert any(i.code == "migration.documented_proxy" for i in caught.value.report.issues)
    assert inventory.data == before
    for source in REVERSE_PRODUCTS:
        identity = dict(zip(FIELDS, source, strict=True))
        data.append({**identity, "exchanges": [{**identity, "type": "production", "amount": 1}]})
    result = BrightwayInventory.from_data(data, context=contexts("uvek", "2025")).migrate_background(
        contexts("ecoinvent", "3.12").background, policy=POLICY
    )
    assert result.data == data


def test_prior_reverse_batch_is_preserved_as_a_complete_reviewed_prefix():
    rules, reviews = reverse_additions()
    assert len(rules) == 8
    resource = json.loads(
        (ROOT / "brightpath/data/migrations/uvek/uvek-2025-to-ecoinvent-3.12-cutoff.json").read_text()
    )
    previous = deepcopy(resource)
    extra = {r["id"] for r in rules[6:]}
    previous["replace"] = [r for r in previous["replace"] if r["id"] not in extra]
    for rule in rules[6:]:
        previous["reviews"].pop(rule["review_ref"])
    previous["coverage"]["mapped_identities"] -= 2
    previous["coverage"]["unresolved_identities"] += 2
    previous["coverage"]["dispositions"]["proxy"] -= 2
    previous["coverage"]["dispositions"]["unresolved"] += 2
    assert legacy_reverse_projection(previous) == legacy_reverse_projection(resource)
