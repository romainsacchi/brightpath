"""Known positive copper allocation stocks never acquire physical PV-recycling burdens or credits."""

from collections import defaultdict
from copy import deepcopy

import pytest
from test_uvek_reference_signs import FIELDS, consumer, contexts
from test_uvek_refrigerant_disposal import target_context

from brightpath import BackgroundContext, BiosphereProfile, BrightwayInventory, TechnosphereProfile
from brightpath.background.reference_signs import select_profile_sign_rules
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource
from scripts import review_uvek_copper_stock as review
from scripts.review_uvek_cutoff_stocks import stock_decision, stock_recipe

SOURCE = next(iter(review.SOURCES.values()))["source"]
PREVIOUS = review.APPROVAL["unsupported_native_reverse_source"]
STOCK_RID = stock_recipe(tuple(SOURCE[f] for f in FIELDS), review_path=review.REVIEW_PATH)[0]
POLICY = MigrationPolicy(on_heuristic_mapping=PolicyAction.WARN)


def direct(amount):
    data = consumer(SOURCE, amount, 2)
    data[0]["exchanges"].append(consumer(SOURCE, amount, 3, "substitution")[0]["exchanges"][1])
    for e in data[0]["exchanges"][1:]:
        e["caller_metadata"] = {"retain": [1, 2]}
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


@pytest.mark.parametrize("version", sorted(review.VERSIONS))
@pytest.mark.parametrize("release", ("2025", "2026", "native2026"))
@pytest.mark.parametrize("amount", (-2, 0, 2))
def test_copper_stock_retains_all_signed_quantities_uncertainty_formulas_and_separate_emissions(
    version, release, amount
):
    data = direct(amount)
    original = deepcopy(data)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", version))
    result = inventory.migrate_background(target_context(release), policy=POLICY)
    assert len(result.data) == 2
    stock = result.data[1]
    assert stock["brightpath_conversion"]["recipe_id"] == STOCK_RID
    assert stock["brightpath_conversion"]["zero_burden"] is True
    assert stock["unit"] == "kilogram" and stock["location"] == "GLO"
    assert stock["exchanges"] == [{**{f: stock[f] for f in FIELDS}, "type": "production", "amount": 1}]
    for before, after in zip(original[0]["exchanges"][1:-1], result.data[0]["exchanges"][1:-1], strict=True):
        assert after["amount"] == before["amount"] == amount and after["type"] == before["type"]
        for key in ("formula", "uncertainty type", "loc", "scale", "minimum", "maximum", "negative", "caller_metadata"):
            assert after.get(key) == before.get(key)
        assert after["input"][1] == stock["code"]
    assert result.data[0]["exchanges"][0] == original[0]["exchanges"][0]
    # A native biosphere export can normalize the name while retaining the caller's physical quantity.
    assert result.data[0]["exchanges"][-1]["amount"] == original[0]["exchanges"][-1]["amount"] == 0.5
    assert result.data[0]["exchanges"][-1]["type"] == "biosphere"
    assert sum(i.code == "migration.zero_burden_supplier" for i in result.last_migration_report.issues) == 2
    assert not any(i.code == "migration.technosphere_information_loss" for i in result.last_migration_report.losses)
    assert inventory.data == data == original
    assert result.migrate_background(target_context(release)).data == result.data


def parents(version, amount):
    sources = {
        tuple(e["source"][f] for f in FIELDS): e["source"]
        for e in review.APPROVAL["parent_rule_bindings"]
        if e["label"] == f"ecoinvent-{version}-cutoff"
    }
    data = None
    for source in sources.values():
        part = consumer(source, amount, 3)
        if data is None:
            data = part
        else:
            data[0]["exchanges"].append(part[0]["exchanges"][1])
    return data


@pytest.mark.parametrize("version", ("3.6", "3.7", "3.8", "3.9", "3.10", "3.11", "3.12"))
@pytest.mark.parametrize("amount", (-2, 0, 2))
def test_existing_ship_processing_and_vessel_markets_share_the_correct_empty_stock_without_changing_copper_amounts(
    version, amount
):
    data = parents(version, amount)
    original = deepcopy(data)
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", version)).migrate_background(
        target_context("2025"), policy=MigrationPolicy.permissive()
    )
    by_recipe = {d["brightpath_conversion"]["recipe_id"]: d for d in result.data[1:]}
    stock = by_recipe[STOCK_RID]
    assert stock["exchanges"] == [{**{f: stock[f] for f in FIELDS}, "type": "production", "amount": 1}]
    selected = select_profile_sign_rules(
        load_uvek_technosphere_resource(), TechnosphereProfile("ecoinvent", version, "cutoff")
    )
    rules = {tuple(r["source"][f] for f in FIELDS): r for r in selected["replace"]}
    found = 0
    for occurrence in review.APPROVAL["direct_stock_transformations"]:
        if occurrence["label"] != f"ecoinvent-{version}-cutoff":
            continue
        source = tuple(occurrence["parent"][f] for f in FIELDS)
        helper = by_recipe[rules[source]["target"]["recipe_ref"]]
        inputs = [e for e in helper["exchanges"] if e["type"] != "production"]
        actual = inputs[occurrence["input_index"]]
        assert actual["amount"] == occurrence["signed_source_amount"] == occurrence["previous_input"]["amount"]
        assert tuple(actual[f] for f in FIELDS) == tuple(stock[f] for f in FIELDS)
        assert occurrence["target_input"]["zero_burden_supplier"]["source_profile"]["version"] == version
        found += 1
    assert found == 6
    assert not any(e["name"] == PREVIOUS["name"] for d in result.data for e in d["exchanges"])
    assert any(i.code == "migration.zero_burden_supplier" for i in result.last_migration_report.issues)
    assert data == original


@pytest.mark.parametrize("release", ("2025", "2026", "native2026"))
def test_reverse_of_owned_allocation_stock_preserves_empty_graph_and_all_signed_caller_state(release):
    forward = BrightwayInventory.from_data(direct(-2), context=contexts("ecoinvent", "3.12")).migrate_background(
        target_context(release), policy=POLICY
    )
    original = deepcopy(forward.data)
    result = forward.migrate_background(
        BackgroundContext(TechnosphereProfile("ecoinvent", "3.12", "cutoff"), BiosphereProfile("ecoinvent", "3.10")),
        policy=MigrationPolicy.permissive(),
    )
    assert result.data[0]["exchanges"][1]["amount"] == -2
    assert result.data[0]["exchanges"][2]["amount"] == -2
    assert result.data[1] == original[1]
    assert forward.data == original


@pytest.mark.parametrize("amount", (-1, 1))
def test_actual_nonempty_native_pv_recovery_has_no_inferred_inverse_to_the_empty_stock(amount):
    data = consumer(PREVIOUS, amount, 3)
    inventory = BrightwayInventory.from_data(data, context=contexts("uvek", "2025"))
    with pytest.raises(MigrationError) as caught:
        inventory.migrate_background(contexts("ecoinvent", "3.12").background, policy=MigrationPolicy.permissive())
    assert any(i.code == "migration.uvek_supplier_unresolved" for i in caught.value.report.issues)
    assert inventory.data == data


def evidence():
    records = defaultdict(list)
    observations = {"ecoinvent": defaultdict(set), "uvek": defaultdict(set)}
    for row in review.APPROVAL["inventories"]:
        source = tuple(row["source"][f] for f in FIELDS)
        records[source].append({**deepcopy(row), "source": list(source)})
        observations[row["label"].split("-")[0]][source].add((row["label"], row["reference_amount"]))
    native = {tuple(r["source"][f] for f in FIELDS): deepcopy(r) for r in review.APPROVAL["native_roles"]}
    return records, observations, native


@pytest.mark.parametrize(
    "change",
    ("inventory_sha256", "activity_comment_sha256", "missing", "duplicate", "nonempty", "negative", "role", "patch"),
)
def test_full_allocation_and_dependent_inventory_metadata_or_role_drift_requires_review(change):
    data, observations, native = evidence()
    review.validate_inventories(data, observations, native)
    source = tuple(SOURCE[f] for f in FIELDS)
    if change in ("inventory_sha256", "activity_comment_sha256"):
        data[source][0][change] = "0" * 64
    elif change == "missing":
        data[source].pop()
    elif change == "duplicate":
        data[source].append(deepcopy(data[source][0]))
    elif change == "nonempty":
        data[source][0]["total_exchanges"] = 2
    elif change == "negative":
        data[source][0]["production_amount"] = -1
    elif change == "role":
        native[next(iter(native))]["native_reference_is_input"] = not native[next(iter(native))][
            "native_reference_is_input"
        ]
    else:
        observations["ecoinvent"][source].add(("ecoinvent-3.12.1-cutoff", 1))
    with pytest.raises(ValueError, match="repeat the scientific review"):
        review.validate_inventories(data, observations, native)


def test_new_stock_authoring_parameter_cannot_authorize_an_unlabelled_energy_or_negative_treatment_stage():
    row = next(iter(review.SOURCES.values()))
    source = {**SOURCE, "name": "Unreviewed recovered-energy allocation stock", "unit": "megajoule"}
    forged = {**deepcopy(row), "source": source}
    rule = {"source": source, "target": row["previous_target"]}
    with pytest.raises(ValueError, match="repeat the scientific review"):
        stock_decision(
            rule,
            [(p["label"], 1) for p in row["profiles"]],
            deepcopy(row["profiles"]),
            "synthetic",
            None,
            approval_row=forged,
            review_path=review.REVIEW_PATH,
        )


def test_parent_binding_or_copper_quantity_mutation_cannot_inherit_stock_approval(monkeypatch):
    review.validate_bindings(load_uvek_technosphere_resource())
    changed = deepcopy(review.APPROVAL["direct_stock_transformations"])
    changed[0]["signed_source_amount"] *= -1
    monkeypatch.setitem(review.APPROVAL, "direct_stock_transformations", changed)
    with pytest.raises(ValueError, match="allocation approval changed"):
        review.validate_bindings(load_uvek_technosphere_resource())


def test_uninspected_profiles_models_and_foreground_suppliers_remain_protected():
    for version in ("3.6.1", "3.12.1", "3.13"):
        selected = select_profile_sign_rules(
            load_uvek_technosphere_resource(), TechnosphereProfile("ecoinvent", version, "cutoff")
        )
        rule = next(r for r in selected["replace"] if r["source"] == SOURCE)
        assert rule["reference_flow_sign_review"]["status"] == "unresolved"
    data = direct(-2)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.12", "consequential"))
    with pytest.raises(MigrationError, match="system models"):
        inventory.migrate_background(target_context("2025"), policy=MigrationPolicy.permissive())
    data.append({**SOURCE, "exchanges": [{**SOURCE, "type": "production", "amount": 1}]})
    assert (
        BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.12"))
        .migrate_background(target_context("2025"), policy=POLICY)
        .data
        == data
    )


@pytest.mark.parametrize("version", ("3.9.1", "3.12"))
def test_excel_round_trip_preserves_stock_quantity_and_explicit_production_only_supplier(tmp_path, version):
    data = consumer(SOURCE, -2, 3)
    data[0]["comment"] = "Copper allocation stock export fixture"
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", version)).migrate_background(
        target_context("2025"), policy=POLICY
    )
    path = tmp_path / "copper-stock.xlsx"
    result.write_excel(path)
    restored = BrightwayInventory.from_excel(path, context=result.context)
    assert len(restored.data) == 2
    actual = restored.data[0]["exchanges"][1]
    assert actual["amount"] == -2 and actual["uncertainty type"] == 3
    assert (actual["loc"], actual["scale"]) == (
        result.data[0]["exchanges"][1]["loc"],
        result.data[0]["exchanges"][1]["scale"],
    )
    assert [e["type"] for e in restored.data[1]["exchanges"]] == ["production"]
    assert not restored.validate().has_errors
