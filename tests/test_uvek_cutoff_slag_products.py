"""Physical GGBFS supply retains processing, cut-off stock and freight boundaries."""

from collections import defaultdict
from copy import deepcopy

import pytest
from test_uvek_market_compositions import assert_composition_quantities, batch
from test_uvek_reference_signs import consumer, contexts
from test_uvek_refrigerant_disposal import target_context
from test_uvek_reverse_industrial_gas import native_data

from brightpath import BrightwayInventory, FormatProfile, InventoryContext, SimaProInventory
from brightpath.background.uvek_export import load_uvek_export_resource
from brightpath.core.policies import MigrationPolicy
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource
from scripts.generate_uvek_export import legacy_export_projection
from scripts.review_uvek_cutoff_slag_products import (
    APPROVAL,
    FIELDS,
    PRODUCTS,
    apply_reverse_product,
    identity,
    legacy_reverse_projection,
    product_decision,
    product_recipe,
    reverse_addition,
    validate_bindings,
    validate_inventories,
)

POLICY = MigrationPolicy.permissive()
VERSIONS = ["3.6", "3.7", "3.8", "3.9", "3.10", "3.11", "3.12"]


@pytest.mark.parametrize("version", VERSIONS)
@pytest.mark.parametrize("amount", [-10, 0, 10])
def test_physical_slag_profiles_preserve_signs_uncertainty_formula_stock_and_freight(version, amount):
    assert_composition_quantities(
        f"ecoinvent-{version}-cutoff",
        amount,
        definitions=PRODUCTS,
        recipe_factory=product_recipe,
        warning_fragment="Independently inspected physical cut-off ground-granulated",
    )


@pytest.mark.parametrize("version", VERSIONS)
@pytest.mark.parametrize("kind", ["2025", "native2026"])
def test_slag_proxy_uses_processing_without_allocated_pig_iron_but_retains_raw_supply(version, kind):
    data = batch(f"ecoinvent-{version}-cutoff", 1, definitions=PRODUCTS)
    emission = {
        "name": "Carbon dioxide, fossil",
        "categories": ["air"],
        "unit": "kilogram",
        "type": "biosphere",
        "amount": 7,
    }
    data[0]["exchanges"].append(emission)
    before = deepcopy(data)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", version))
    result = inventory.migrate_background(target_context(kind), policy=POLICY)
    assert inventory.data == data == before
    assert result.data[0]["exchanges"][-1]["amount"] == 7
    assert any(d.get("brightpath_conversion", {}).get("zero_burden") for d in result.data[1:])
    for helper in result.data[1:]:
        assert not any(
            e.get("name", "").startswith("Ground granulated blast furnace slag, with burdens")
            for e in helper["exchanges"]
        )
        if helper.get("brightpath_conversion", {}).get("zero_burden"):
            assert len(helper["exchanges"]) == 1 and helper["exchanges"][0]["amount"] == 1
    for source, row in PRODUCTS.items():
        profile = next(p for p in row["profiles"] if p["label"] == f"ecoinvent-{version}-cutoff")
        helper = next(
            d
            for d in result.data[1:]
            if d.get("brightpath_conversion", {}).get("recipe_id") == product_recipe(source, profile)[0]
        )
        assert [e["amount"] for e in helper["exchanges"][1:]] == [1, profile["raw_slag_amount"]]
        assert helper["exchanges"][1]["name"].startswith("Ground granulated blast furnace slag, no burdens")


@pytest.mark.parametrize("version,model", [("3.12.1", "cutoff"), ("3.6.1", "cutoff"), ("3.12", "consequential")])
def test_slag_product_approval_has_no_uninspected_patch_or_consequential_fallback(version, model):
    data = batch("ecoinvent-3.12-cutoff", 1, definitions=PRODUCTS)
    before = deepcopy(data)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", version, model))
    with pytest.raises(MigrationError):
        inventory.migrate_background(target_context("2025"), policy=POLICY)
    assert inventory.data == data == before


def test_slag_product_proxy_requires_loss_policy_and_protects_foreground():
    data = batch("ecoinvent-3.12-cutoff", 1, definitions=PRODUCTS)
    before = deepcopy(data)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.12"))
    with pytest.raises(MigrationError):
        inventory.migrate_background(target_context("2025"))
    assert inventory.data == before
    for source in PRODUCTS:
        record = dict(zip(FIELDS, source, strict=True))
        data.append({**record, "exchanges": [{**record, "type": "production", "amount": 1}]})
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.12")).migrate_background(
        target_context("2025"), policy=POLICY
    )
    assert result.data == data


def evidence():
    inventories, observations, native = defaultdict(list), {"ecoinvent": defaultdict(set), "uvek": defaultdict(set)}, {}
    for row in APPROVAL["inventories"]:
        key = identity(row["source"])
        inventories[key].append({k: v for k, v in row.items() if k != "source"})
        observations[row["label"].split("-")[0]][key].add((row["label"], 1))
        if row["label"].startswith("uvek"):
            native[key] = {"native_flow_type": "PRODUCT_FLOW", "native_reference_is_input": False}
    return inventories, observations, native


def test_native_2026_product_variants_keep_the_independently_inspected_raw_supply_boundary():
    rows = APPROVAL["native_2026_boundary_observations"]
    assert len(rows) == 2
    for row in rows:
        assert row["source_profile"] == {"family": "uvek", "version": "2026", "system_model": "cutoff"}
        assert row["reference"] == [{"amount": 1, "self": True}]
        assert len(row["inventory_sha256"]) == 64
        assert row["raw_slag_supply_amount"] == (0 if "no burdens" in row["source"]["name"] else 1.0014)


@pytest.mark.parametrize(
    "change", ["none", "inventory", "comment", "product_information", "stock_sign", "native", "missing", "duplicate"]
)
def test_slag_product_boundary_requires_all_inventory_metadata_stock_and_native_evidence(change):
    inventories, observations, native = evidence()
    source = next(iter(inventories))
    if change == "none":
        validate_inventories(inventories, observations, native)
        return
    if change in {"inventory", "comment", "product_information"}:
        field = {
            "inventory": "inventory_sha256",
            "comment": "activity_comment_sha256",
            "product_information": "product_information_sha256",
        }[change]
        inventories[source][0][field] = "0" * 64
    elif change == "stock_sign":
        row = next(
            r for r in APPROVAL["inventories"] if r["source"]["name"] == "blast furnace slag, Recycled Content cut-off"
        )
        observations["ecoinvent"][identity(row["source"])].remove((row["label"], 1))
        observations["ecoinvent"][identity(row["source"])].add((row["label"], -1))
    elif change == "native":
        native[identity(APPROVAL["operating_primitive"])]["native_flow_type"] = "WASTE_FLOW"
    elif change == "missing":
        inventories[source].pop()
    else:
        inventories[source].append(deepcopy(inventories[source][0]))
    with pytest.raises(ValueError, match="repeat the independent review"):
        validate_inventories(inventories, observations, native)


@pytest.mark.parametrize("change", ["component", "recipe"])
def test_slag_raw_supply_bindings_cannot_change_silently(change):
    resource = load_uvek_technosphere_resource()
    validate_bindings(resource)
    mutated = deepcopy(resource)
    if change == "recipe":
        key = next(iter(APPROVAL["recipe_dependencies"]))
        mutated["recipes"][key]["rationale"] = "Unreviewed"
    else:
        source = tuple(APPROVAL["rules"][0]["profiles"][0]["bindings"][0]["source"])
        row = next(r for r in mutated["replace"] if identity(r["source"]) == source)
        row["confidence"] = 0
    with pytest.raises(ValueError, match="repeat the scientific review"):
        validate_bindings(mutated)


@pytest.mark.parametrize("change", ["prior_target", "inventory", "factor"])
def test_slag_product_compiler_rejects_changed_prior_correspondence_or_inventory(change):
    row = next(iter(PRODUCTS.values()))
    rule = {"source": row["source"], "target": deepcopy(row["previous_target"])}
    observations = [(p["label"], p["source_sign"]) for p in row["profiles"]]
    inventories = [(p["label"], p["inventory_sha256"]) for p in row["profiles"]]
    if change == "prior_target":
        rule["target"] = deepcopy(APPROVAL["operating_primitive"])
    elif change == "inventory":
        inventories[0] = (inventories[0][0], "0" * 64)
    else:
        rule["conversion_factor"] = 0.001
    with pytest.raises(ValueError, match="repeat the scientific review"):
        product_decision(rule, observations, inventories, "synthetic", None)


@pytest.mark.parametrize("kind", ["2025", "2026", "native2026"])
@pytest.mark.parametrize("amount", [-10, 0, 10])
@pytest.mark.parametrize("exchange_type,uncertainty", [("technosphere", 2), ("substitution", 3)])
def test_reverse_no_burdens_slag_preserves_caller_and_removes_exact_upstream_raw_supply(
    kind, amount, exchange_type, uncertainty
):
    data = native_data(kind, consumer(APPROVAL["reverse"]["source"], amount, uncertainty, exchange_type))
    before = deepcopy(data)
    inventory = BrightwayInventory.from_data(
        data, context=InventoryContext(FormatProfile("brightway_excel"), target_context(kind))
    )
    result = inventory.migrate_background(contexts("ecoinvent", "3.12").background, policy=POLICY)
    assert inventory.data == data == before
    old, new = before[0]["exchanges"][1], result.data[0]["exchanges"][1]
    for field in ("amount", "type", "uncertainty type", "formula", "loc", "scale", "negative", "minimum", "maximum"):
        assert new.get(field) == old.get(field)
    assert len(result.data) == 2
    helper = result.data[1]
    assert helper["exchanges"][0]["amount"] == 1 and helper["location"] == "US"
    assert [{k: e[k] for k in (*FIELDS, "type", "amount")} for e in helper["exchanges"][1:]] == APPROVAL["reverse"][
        "inputs"
    ]
    assert helper["exchanges"][2]["amount"] == -1.0011225250000004
    assert any(
        i.code == "migration.documented_proxy" and "RER-to-US" in i.message for i in result.last_migration_report.issues
    )
    assert result.migrate_background(result.context.background).data == result.data


@pytest.mark.parametrize("change", ["rule", "recipe", "review", "extra_source"])
def test_reverse_slag_extension_cannot_change_the_frozen_publisher_baseline(change):
    resource = deepcopy(load_uvek_export_resource())
    legacy_reverse_projection(resource)
    expected, recipes, _ = reverse_addition()
    rule = next(r for r in resource["replace"] if r["id"] == expected["id"])
    if change == "rule":
        rule["conversion_factor"] = -1
    elif change == "recipe":
        resource["recipes"][next(iter(recipes))]["inputs"][1]["amount"] = -1
    elif change == "review":
        resource["reviews"][rule["review_ref"]]["rationale"] = "Unreviewed"
    else:
        rule["source"]["location"] = "GLO"
    with pytest.raises(ValueError, match="reverse slag|Reverse slag"):
        legacy_reverse_projection(resource)


def test_reverse_slag_extension_preserves_publisher_prefix_and_excludes_with_burdens():
    resource = load_uvek_export_resource()
    original = legacy_export_projection(resource)
    assert len(original["replace"]) == 1423 and len(original["recipes"]) == 7
    source = {**APPROVAL["reverse"]["source"]}
    source["name"] = source["reference product"] = "Ground granulated blast furnace slag, with burdens, at plant"
    inventory = BrightwayInventory.from_data(consumer(source, 1, 2, "technosphere"), context=contexts("uvek", "2025"))
    with pytest.raises(MigrationError):
        inventory.migrate_background(contexts("ecoinvent", "3.12").background, policy=POLICY)


def test_reverse_slag_requires_proxy_policy_and_preserves_a_caller_owned_supplier():
    source = APPROVAL["reverse"]["source"]
    data = consumer(source, 1, 2, "technosphere")
    before = deepcopy(data)
    inventory = BrightwayInventory.from_data(data, context=contexts("uvek", "2025"))
    with pytest.raises(MigrationError):
        inventory.migrate_background(contexts("ecoinvent", "3.12").background)
    assert inventory.data == data == before
    data.append({**source, "exchanges": [{**source, "type": "production", "amount": 1}]})
    inventory = BrightwayInventory.from_data(data, context=contexts("uvek", "2025"))
    result = inventory.migrate_background(contexts("ecoinvent", "3.12").background)
    assert result.data == data


@pytest.mark.parametrize("missing", ["source", "product", "raw_supply"])
def test_reverse_slag_requires_exact_source_and_operating_target_catalogs(missing):
    resource = legacy_reverse_projection(deepcopy(load_uvek_export_resource()))
    before = deepcopy(resource)
    sources = {identity(APPROVAL["reverse"]["source"])}
    targets = {identity(e) for e in APPROVAL["reverse"]["inputs"]}
    if missing == "source":
        sources.clear()
    elif missing == "product":
        targets.remove(identity(APPROVAL["reverse"]["target_product"]))
    else:
        targets.remove(identity(APPROVAL["reverse"]["raw_supply"]))
    with pytest.raises(ValueError, match="missing from its exact catalog"):
        apply_reverse_product(resource, [], sources, targets)
    assert resource == before


@pytest.mark.parametrize("kind", ["2025", "native2026"])
def test_reverse_slag_operating_boundary_round_trips_through_excel_and_simapro(tmp_path, kind):
    data = native_data(kind, consumer(APPROVAL["reverse"]["source"], 1, 2, "technosphere"))
    data[0]["comment"] = "Reverse slag operating-boundary export/reimport fixture."
    data[0]["exchanges"][1].pop("formula")
    migrated = BrightwayInventory.from_data(
        data, context=InventoryContext(FormatProfile("brightway_excel"), target_context(kind))
    ).migrate_background(contexts("ecoinvent", "3.12").background, policy=POLICY)
    excel = BrightwayInventory.from_excel(migrated.write_excel(tmp_path / "slag_reverse.xlsx"))
    categorized = migrated.data
    for dataset in categorized:
        dataset["exchanges"][0]["simapro category"] = "Materials/Slag products"
    path = (
        BrightwayInventory.from_data(categorized, context=migrated.context)
        .to_simapro()
        .write_csv(tmp_path / "slag_reverse.csv")
    )
    simapro = SimaProInventory.from_csv(
        path, background_profile=migrated.background_profile, biosphere_profile=migrated.context.background.biosphere
    ).to_brightway()
    for restored in (excel, simapro):
        assert len(restored.data) == 2 and not restored.validate().has_errors
        inputs = restored.data[1]["exchanges"][1:]
        by_name = {exchange["name"]: exchange["amount"] for exchange in inputs}
        assert by_name["ground granulated blast furnace slag production"] == 1
        assert by_name["market for blast furnace slag"] == pytest.approx(-1.0011225250000004)


@pytest.mark.parametrize("version", ["3.6", "3.12"])
@pytest.mark.parametrize("kind", ["2025", "native2026"])
def test_slag_product_helpers_round_trip_excel_and_simapro(tmp_path, version, kind):
    data = batch(f"ecoinvent-{version}-cutoff", 1, definitions=PRODUCTS)
    data[0]["comment"] = "Physical slag product export and reimport fixture."
    for exchange in data[0]["exchanges"][1:]:
        exchange.pop("formula")
        exchange["type"] = "technosphere"
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", version)).migrate_background(
        target_context(kind), policy=POLICY
    )
    excel = BrightwayInventory.from_excel(result.write_excel(tmp_path / "slag.xlsx"))
    categorized = result.data
    for dataset in categorized:
        dataset["exchanges"][0]["simapro category"] = "Materials/Slag products"
    path = (
        BrightwayInventory.from_data(categorized, context=result.context).to_simapro().write_csv(tmp_path / "slag.csv")
    )
    simapro = SimaProInventory.from_csv(
        path, background_profile=result.background_profile, biosphere_profile=result.context.background.biosphere
    ).to_brightway()
    for restored in (excel, simapro):
        assert len(restored.data) == len(result.data) and not restored.validate().has_errors
        assert not any(
            "Ground granulated blast furnace slag, with burdens" in e.get("name", "")
            for d in restored.data
            for e in d["exchanges"]
        )


@pytest.mark.parametrize("version", ["3.6", "3.12"])
@pytest.mark.parametrize("kind", ["2025", "native2026"])
def test_complete_slag_helper_hierarchy_preserves_existing_reverse_coverage_and_rollback(version, kind):
    forward = BrightwayInventory.from_data(
        batch(f"ecoinvent-{version}-cutoff", 1, definitions=PRODUCTS), context=contexts("ecoinvent", version)
    ).migrate_background(target_context(kind), policy=POLICY)
    before = deepcopy(forward.data)
    result = forward.migrate_background(contexts("ecoinvent", "3.12").background, policy=POLICY)
    # Corrected complete freight services use existing independently reviewed
    # reverse suppliers. The extra helper is the existing slag operating boundary.
    assert result.data[0] == before[0] and len(result.data) == len(before) + 1
    restored = {row["code"]: row for row in result.data if "code" in row}
    assert all(
        row["code"] in restored and row["exchanges"][0] == restored[row["code"]]["exchanges"][0] for row in before[1:]
    )
    assert forward.data == before

    # A valid native supplier without an approved reverse correspondence must
    # still refuse transactionally; the forward freight correction adds no inverse.
    name = "Disposal, raw sewage sludge, to municipal incineration"
    source = dict(zip(FIELDS, (name, name, "CH", "kilogram"), strict=True))
    missing = native_data(kind, consumer(source, 1))[0]["exchanges"][1]
    incomplete = deepcopy(before)
    incomplete[1]["exchanges"].append(missing)
    inventory = BrightwayInventory.from_data(incomplete, context=forward.context)
    with pytest.raises(MigrationError) as caught:
        inventory.migrate_background(contexts("ecoinvent", "3.12").background, policy=POLICY)
    errors = [issue for issue in caught.value.report.issues if issue.severity.value == "error"]
    assert errors and all(issue.code == "migration.uvek_supplier_unresolved" for issue in errors)
    assert inventory.data == incomplete
