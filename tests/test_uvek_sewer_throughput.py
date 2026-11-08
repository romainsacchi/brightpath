"""Sewer throughput retains full construction per m3, rather than gas, tailings or pipe length."""

from copy import deepcopy

import pytest
from test_uvek_reference_signs import FIELDS, consumer, contexts
from test_uvek_refrigerant_disposal import target_context

from brightpath import BackgroundContext, BiosphereProfile, BrightwayInventory, InventoryContext, TechnosphereProfile
from brightpath.background.reference_signs import select_profile_sign_rules
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_release_resources, load_uvek_technosphere_resource
from scripts.review_uvek_sewer_throughput import (
    APPROVAL,
    SOURCES,
    VERSIONS,
    validate_bindings,
    validate_inventories,
)

POLICY = MigrationPolicy(on_heuristic_mapping=PolicyAction.WARN, on_information_loss=PolicyAction.WARN)


def batch(amount):
    sources = [dict(zip(FIELDS, s, strict=True)) for s in SOURCES]
    data = consumer(sources[0], amount, 2)
    data[0]["exchanges"][1:] = [
        consumer(source, amount, uncertainty, kind)[0]["exchanges"][1]
        for source in sources
        for kind, uncertainty in (("technosphere", 2), ("substitution", 3))
    ]
    for e in data[0]["exchanges"][1:]:
        e["caller_metadata"] = {"keep": [1, 2]}
    data[0]["exchanges"].append(
        {
            "name": "Carbon dioxide, fossil",
            "categories": ["air"],
            "type": "biosphere",
            "unit": "kilogram",
            "amount": 0.5,
        }
    )
    return data


@pytest.mark.parametrize("version", sorted(VERSIONS))
@pytest.mark.parametrize("release", ("2025", "2026", "native2026"))
@pytest.mark.parametrize("amount", (-2, 0, 2))
def test_full_sewer_construction_per_reported_cubic_metre_keeps_callers_disposal_and_water(version, release, amount):
    data = batch(amount)
    original = deepcopy(data)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", version))
    result = inventory.migrate_background(target_context(release), policy=POLICY)
    assert result.data[0]["exchanges"][0] == original[0]["exchanges"][0]
    assert result.data[0]["exchanges"][-1]["amount"] == 0.5
    helpers = {d["code"]: d for d in result.data[1:]}
    by_identity = {tuple(d[f] for f in FIELDS): d for d in result.data[1:]}
    for before, after in zip(original[0]["exchanges"][1:-1], result.data[0]["exchanges"][1:-1], strict=True):
        for field in ("amount", "type", "formula", "scale", "loc", "negative", "minimum", "maximum", "caller_metadata"):
            assert after.get(field) == before.get(field)
        parent = helpers[after["input"][1]]
        production = [e for e in parent["exchanges"] if e["type"] == "production"]
        assert len(production) == 1 and production[0]["amount"] == 1 and parent["unit"] == "cubic meter"
        if before["name"].startswith("market for"):
            (child,) = [e for e in parent["exchanges"] if e["type"] == "technosphere"]
            assert child["amount"] == 1 and child["unit"] == "cubic meter"
            assert not any(e["type"] == "biosphere" for e in parent["exchanges"])
            parent = by_identity[tuple(child[f] for f in FIELDS)]
        materials = [e for e in parent["exchanges"] if e["type"] == "technosphere"]
        assert len(materials) >= 40 and all(e["amount"] > 0 for e in materials)
        assert any("Polyvinylchloride, emulsion polymerised" in e["name"] for e in materials)
        assert any(e["name"] == "Extrusion, plastic pipes" for e in materials)
        assert any(e["name"] == "Synthetic rubber, at plant" for e in materials)
        assert any(e["name"] == "Cement mortar, at plant" for e in materials)
        assert any(e["name"].startswith("Disposal,") for e in materials)
        flows = [e for e in parent["exchanges"] if e["type"] == "biosphere"]
        assert len(flows) == 2 and all(e["name"] == "Water" and e["amount"] > 0 for e in flows)
        assert {e["unit"] for e in flows} == ({"kilogram"} if release == "native2026" else {"cubic meter"})
        if version == "3.12" and before["name"].endswith("rural"):
            multiplier = 1000 if release == "native2026" else 1
            assert sorted(e["amount"] for e in flows) == pytest.approx(
                [0.000548559954224778 * multiplier, 0.00310850640727374 * multiplier]
            )
            assert sum(e["amount"] for e in materials if e["name"] == "Cast iron, at plant") == 0.00236222462806739
    assert not any(
        e["name"] in {"Natural gas, vented", "Tailings, uranium milling", "Emulsion polymerisation"}
        or e["unit"] == "kilometer"
        for d in result.data[1:]
        for e in d["exchanges"]
    )
    assert result.last_migration_report.losses
    assert inventory.data == data == original


@pytest.mark.parametrize("version", sorted(VERSIONS))
def test_later_native_conversion_scales_each_reported_water_release_once_without_rebuilding_the_sewer(version):
    canonical = BrightwayInventory.from_data(batch(1), context=contexts("ecoinvent", version)).migrate_background(
        target_context("2025"), policy=POLICY
    )
    original = deepcopy(canonical.data)
    native = canonical.migrate_background(target_context("native2026"), policy=POLICY)
    assert len(native.data) == len(original)
    for before, after in zip(original[1:], native.data[1:], strict=True):
        assert before["code"] == after["code"]
        old = [e for e in before["exchanges"] if e["type"] == "biosphere"]
        new = [e for e in after["exchanges"] if e["type"] == "biosphere"]
        assert len(old) == len(new)
        assert sorted(e["amount"] * 1000 for e in old) == pytest.approx(sorted(e["amount"] for e in new))
    assert canonical.data == original


@pytest.mark.parametrize("version", ("3.9.1", "3.10.1"))
def test_patch_polyethylene_submix_is_rebuilt_from_its_actual_country_shares_without_global_rule_changes(version):
    result = BrightwayInventory.from_data(batch(1), context=contexts("ecoinvent", version)).migrate_background(
        target_context("2025"), policy=POLICY
    )
    markets = [
        d
        for d in result.data[1:]
        if d.get("brightpath_conversion", {}).get("recipe_id", "").startswith("sewer-throughput-pe-")
    ]
    assert len(markets) == 2
    regional = next(d for d in markets if d["location"] == "RER")
    quantities = sorted(e["amount"] for e in regional["exchanges"] if e["type"] == "technosphere")
    assert len(quantities) == 2 and sum(quantities) == pytest.approx(1)
    if version == "3.10.1":
        assert quantities == [0.29097708633433217, 0.7090229136656678]
    selected = select_profile_sign_rules(
        load_uvek_technosphere_resource(), TechnosphereProfile("ecoinvent", version, "cutoff")
    )
    assert (
        next(
            r
            for r in selected["replace"]
            if r["source"]["name"] == "market group for waste polyethylene" and r["source"]["location"] == "RER"
        )["reference_flow_sign_review"]["status"]
        == "unresolved"
    )


@pytest.mark.parametrize("version", sorted(VERSIONS))
def test_complete_graph_does_not_infer_the_six_missing_native_reverse_suppliers_or_delete_them(version):
    canonical = BrightwayInventory.from_data(batch(1), context=contexts("ecoinvent", version)).migrate_background(
        target_context("2025"), policy=POLICY
    )
    original = deepcopy(canonical.data)
    with pytest.raises(MigrationError) as caught:
        canonical.migrate_background(
            BackgroundContext(
                TechnosphereProfile("ecoinvent", "3.12", "cutoff"), BiosphereProfile("ecoinvent", "3.10")
            ),
            policy=MigrationPolicy.permissive(),
        )
    unresolved = {
        tuple(i.details["source"][f] for f in FIELDS)
        for i in caught.value.report.issues
        if i.code == "migration.uvek_supplier_unresolved"
    }
    assert unresolved == {tuple(r["source"][f] for f in FIELDS) for r in APPROVAL["unsupported_reverse_suppliers"]}
    assert canonical.data == original


@pytest.mark.parametrize("release", ("2025", "2026", "native2026"))
@pytest.mark.parametrize("amount", (-2, 0, 2))
def test_fifteen_independently_inspected_native_reverse_suppliers_keep_their_coordinates(release, amount):
    suppliers = [r["source"] for r in APPROVAL["reverse_bindings"]]
    if release != "2025":
        bridge = next(
            r for r in load_uvek_release_resources("technosphere") if r["source_profiles"][0]["version"] == "2025"
        )
        suppliers = [next(r["target"] for r in bridge["replace"] if r["source"] == source) for source in suppliers]
    data = consumer(suppliers[0], amount, 3)
    data[0]["exchanges"][1:] = [consumer(s, amount, 3)[0]["exchanges"][1] for s in suppliers]
    inventory = BrightwayInventory.from_data(
        data, context=InventoryContext(contexts("uvek", "2025").format, target_context(release))
    )
    result = inventory.migrate_background(
        BackgroundContext(TechnosphereProfile("ecoinvent", "3.12", "cutoff"), BiosphereProfile("ecoinvent", "3.12")),
        policy=MigrationPolicy(
            on_heuristic_mapping=PolicyAction.WARN,
            on_information_loss=PolicyAction.WARN,
            on_inferred_reverse=PolicyAction.WARN,
        ),
    )
    assert len(result.data) == 1
    for before, after in zip(data[0]["exchanges"][1:], result.data[0]["exchanges"][1:], strict=True):
        for f in ("amount", "unit", "formula", "loc", "scale", "minimum", "maximum"):
            assert before[f] == after[f]
    assert inventory.data == data


def test_loss_refusal_foreground_ownership_and_incompatible_profiles_are_transactional():
    data = batch(1)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.12"))
    with pytest.raises(MigrationError):
        inventory.migrate_background(
            target_context("2025"), policy=MigrationPolicy(on_heuristic_mapping=PolicyAction.WARN)
        )
    assert inventory.data == data
    for source in SOURCES:
        supplier = dict(zip(FIELDS, source, strict=True))
        data.append({**supplier, "exchanges": [{**supplier, "type": "production", "amount": 1}]})
    assert (
        BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.12"))
        .migrate_background(target_context("2025"), policy=POLICY)
        .data
        == data
    )
    for version in ("3.6", "3.10.2", "3.13"):
        selected = select_profile_sign_rules(
            load_uvek_technosphere_resource(), TechnosphereProfile("ecoinvent", version, "cutoff")
        )
        assert all(
            r["reference_flow_sign_review"]["status"] == "unresolved"
            for r in selected["replace"]
            if tuple(r["source"][f] for f in FIELDS) in SOURCES
        )
    inventory = BrightwayInventory.from_data(batch(1), context=contexts("ecoinvent", "3.12", "consequential"))
    with pytest.raises(MigrationError, match="system models"):
        inventory.migrate_background(target_context("native2026"), policy=MigrationPolicy.permissive())
    assert inventory.data == batch(1)


@pytest.mark.parametrize("mutation", ("inventory", "description", "missing", "duplicate", "patch", "native_role"))
def test_inventory_boundary_or_native_role_drift_requires_review(mutation):
    records, observations = {}, {"ecoinvent": {}, "uvek": {}}
    for row in APPROVAL["inventories"]:
        source = tuple(row["source"][f] for f in FIELDS)
        records.setdefault(source, []).append({**deepcopy(row), "source": list(source)})
        observations[row["label"].split("-")[0]].setdefault(source, set()).add((row["label"], row["reference_amount"]))
    native = {tuple(row["source"][f] for f in FIELDS): deepcopy(row) for row in APPROVAL["native_roles"]}
    validate_inventories(records, observations, native)
    source = next(iter(SOURCES))
    if mutation == "inventory":
        records[source][0]["inventory_sha256"] = "0" * 64
    elif mutation == "description":
        records[source][0]["product_information_sha256"] = "0" * 64
    elif mutation == "missing":
        records[source].pop()
    elif mutation == "duplicate":
        records[source].append(deepcopy(records[source][0]))
    elif mutation == "patch":
        observations["ecoinvent"][source].add(("ecoinvent-3.10.2-cutoff", 1))
    else:
        native[next(iter(native))]["native_reference_is_input"] = True
    with pytest.raises(ValueError, match="repeat the scientific review"):
        validate_inventories(records, observations, native)


def test_nested_recipe_and_patch_share_drift_cannot_inherit_approval(monkeypatch):
    resource = deepcopy(load_uvek_technosphere_resource())
    validate_bindings(resource)
    rid = next(iter(APPROVAL["recipe_dependencies"]))
    resource["recipes"][rid]["inputs"][0]["amount"] *= 2
    with pytest.raises(ValueError, match="recipe dependency changed"):
        validate_bindings(resource)
    rid = APPROVAL["local_market_proofs"][0]["recipe_ref"]
    recipe = deepcopy(APPROVAL["local_recipes"][rid])
    recipe["inputs"][0]["amount"] *= 2
    monkeypatch.setitem(APPROVAL["local_recipes"], rid, recipe)
    with pytest.raises(ValueError, match="patch submix changed"):
        validate_bindings(load_uvek_technosphere_resource())
