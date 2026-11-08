"""Dedicated LCD services retain reference direction and disclose treatment boundaries."""

from collections import Counter
from copy import deepcopy

import pytest
from test_uvek_reference_signs import FIELDS, consumer, contexts
from test_uvek_refrigerant_disposal import target_context

from brightpath import BrightwayInventory, SimaProInventory, TechnosphereProfile
from brightpath.background.reference_signs import select_profile_sign_rules
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_release_resources, load_uvek_technosphere_resource
from scripts.review_uvek_lcd_treatments import APPROVAL, LCD_TREATMENTS, TARGETS, lcd_profiles
from scripts.review_uvek_market_compositions import COMPOSITIONS

POLICY = MigrationPolicy(on_heuristic_mapping=PolicyAction.WARN, on_information_loss=PolicyAction.WARN)
LABELS = [
    label
    for label in sorted({p["label"] for a in LCD_TREATMENTS.values() for p in a["profiles"]})
    if label.endswith("-cutoff")
]


def batch(label, amount):
    data = None
    for source, approval in LCD_TREATMENTS.items():
        if label not in {p["label"] for p in approval["profiles"]}:
            continue
        for kind in ("technosphere", "substitution"):
            for uncertainty in (2, 3):
                row = consumer(dict(zip(FIELDS, source, strict=True)), amount, uncertainty, kind)
                row[0]["exchanges"][1]["caller_metadata"] = {"retain": [1, "LCD"]}
                if data is None:
                    data = row
                else:
                    data[0]["exchanges"].append(row[0]["exchanges"][1])
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


@pytest.mark.parametrize("label", LABELS)
@pytest.mark.parametrize("amount", [-10, 0, 10])
@pytest.mark.parametrize("release", ["2025", "native2026"])
def test_lcd_services_preserve_demand_credits_uncertainty_formulas_and_original_changes(label, amount, release):
    _, version, model = label.split("-")
    data = batch(label, amount)
    before = deepcopy(data)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", version, model))
    result = inventory.migrate_background(target_context(release), policy=POLICY)
    assert len(result.data) == 1 and result.data[0]["exchanges"][0] == before[0]["exchanges"][0]
    emission = result.data[0]["exchanges"][-1]
    assert emission["name"] == "Carbon dioxide, fossil" and emission["amount"] == 0.5
    bridge = (
        next(r for r in load_uvek_release_resources("technosphere") if r["source_profiles"][0]["version"] == "2025")
        if release != "2025"
        else None
    )
    warnings = {
        i.path: i for i in result.last_migration_report.issues if i.code == "migration.technosphere_information_loss"
    }
    losses = {
        loss.path: loss
        for loss in result.last_migration_report.losses
        if loss.code == "migration.technosphere_information_loss"
    }
    assert len(warnings) == len(losses) == len(before[0]["exchanges"]) - 2
    for i, (old, new) in enumerate(
        zip(before[0]["exchanges"][1:-1], result.data[0]["exchanges"][1:-1], strict=True), start=1
    ):
        approval = LCD_TREATMENTS[tuple(old[f] for f in FIELDS)]
        target = dict(zip(FIELDS, approval["target"], strict=True))
        if bridge:
            target = next(r["target"] for r in bridge["replace"] if r["source"] == target)
        assert {f: new[f] for f in FIELDS} == target
        assert new["amount"] == -amount and new["type"] == old["type"]
        assert new["formula"] == "(dose) * (-1.0)" and new["caller_metadata"] == old["caller_metadata"]
        assert new["scale"] == old["scale"]
        if old["uncertainty type"] == 2:
            assert new["loc"] == old["loc"] and new["negative"] is (amount >= 0)
        else:
            assert new["loc"] == -amount and (new["minimum"], new["maximum"]) == (-amount - 1, -amount + 1)
        path = f"datasets[0].exchanges[{i}]"
        assert approval["limitations"] in warnings[path].message
        details = losses[path].to_dict()["details"]
        assert details["source"] == {f: old[f] for f in FIELDS}
        assert details["target"] == dict(zip(FIELDS, approval["target"], strict=True))
    assert inventory.data == data == before
    assert result.migrate_background(target_context(release)).data == result.data


def test_lcd_scope_and_broader_mechanical_boundary_are_explicit():
    assert len(LCD_TREATMENTS) == APPROVAL["source_count"] == 5
    assert sum(len(a["profiles"]) for a in LCD_TREATMENTS.values()) == APPROVAL["source_profile_count"] == 50
    mechanical = next(r for r in APPROVAL["rules"] if r["kind"] == "mechanical")
    assert sum(p["documents_manual_pretreatment"] for p in mechanical["profiles"]) == 8
    assert all(p["separate_manual_treatment_supplier"] is False for p in mechanical["profiles"])
    assert (
        "broader manual-plus-mechanical boundary"
        in LCD_TREATMENTS[tuple(mechanical["source"][f] for f in FIELDS)]["limitations"]
    )
    assert all(row["native_flow_type"] == "WASTE_FLOW" and row["native_reference_is_input"] for row in TARGETS.values())


@pytest.mark.parametrize("version", ["3.9.1", "3.10.1"])
def test_lcd_treatments_never_inherit_patch_profile_approval(version):
    resource = select_profile_sign_rules(
        load_uvek_technosphere_resource(), TechnosphereProfile("ecoinvent", version, "cutoff")
    )
    rules = [r for r in resource["replace"] if tuple(r["source"][f] for f in FIELDS) in LCD_TREATMENTS]
    assert len(rules) == 5 and all(r["reference_flow_sign_review"]["status"] == "unresolved" for r in rules)


def test_lcd_policy_foreground_ownership_and_independent_credit_are_preserved():
    data = batch("ecoinvent-3.12-cutoff", -1)
    original = deepcopy(data)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.12", "cutoff"))
    with pytest.raises(MigrationError) as caught:
        inventory.migrate_background(
            target_context("2025"),
            policy=MigrationPolicy(on_heuristic_mapping=PolicyAction.WARN, on_information_loss=PolicyAction.ERROR),
        )
    assert any(i.code == "migration.technosphere_information_loss" for i in caught.value.report.issues)
    assert inventory.data == data == original
    for source in LCD_TREATMENTS:
        supplier = dict(zip(FIELDS, source, strict=True))
        data.append({**supplier, "exchanges": [{**supplier, "amount": -1, "type": "production"}]})
    local = {
        "name": "Local recovered material",
        "reference product": "local metal",
        "location": "CH",
        "unit": "kilogram",
    }
    credit = {**local, "amount": -2, "type": "substitution", "formula": "metal_credit"}
    data[0]["exchanges"].append(credit)
    data.append({**local, "exchanges": [{**local, "amount": 1, "type": "production"}]})
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.12", "cutoff")).migrate_background(
        target_context("2025"), policy=POLICY
    )
    assert result.data == data and result.data[0]["exchanges"][-1] == credit
    assert not any(i.code == "migration.technosphere_information_loss" for i in result.last_migration_report.issues)


def test_lcd_updates_roll_back_with_unrelated_unreviewed_mapping(unreviewed_uvek_correspondence):
    data = batch("ecoinvent-3.12-cutoff", -1)
    data[0]["exchanges"].append({**unreviewed_uvek_correspondence, "amount": 1, "type": "technosphere"})
    before = deepcopy(data)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.12"))
    with pytest.raises(MigrationError):
        inventory.migrate_background(target_context("2025"), policy=POLICY)
    assert inventory.data == data == before


@pytest.mark.parametrize("mutation", ["source", "target", "missing_profile", "sign"])
def test_lcd_authoring_rejects_stale_or_changed_inventory_evidence(mutation):
    inventories = {s: [(p["label"], p["inventory_sha256"]) for p in a["profiles"]] for s, a in LCD_TREATMENTS.items()}
    inventories.update({t: [("uvek-2025-cutoff", row["inventory_sha256"])] for t, row in TARGETS.items()})
    source = next(iter(LCD_TREATMENTS))
    observations = [(p["label"], -1) for p in LCD_TREATMENTS[source]["profiles"]]
    assert lcd_profiles(source, observations, inventories, [])
    if mutation in ("source", "target"):
        key = source if mutation == "source" else LCD_TREATMENTS[source]["target"]
        inventories[key][0] = (inventories[key][0][0], "0" * 64)
    elif mutation == "missing_profile":
        observations.pop()
    else:
        observations[0] = (observations[0][0], 1)
    with pytest.raises(ValueError, match="LCD treatment inventory or profile changed"):
        lcd_profiles(source, observations, inventories, [])


@pytest.mark.parametrize("target", list(TARGETS))
def test_lcd_reverse_is_independently_unresolved(target):
    data = consumer(dict(zip(FIELDS, target, strict=True)), 1)
    inventory = BrightwayInventory.from_data(data, context=contexts("uvek", "2025"))
    with pytest.raises(MigrationError):
        inventory.migrate_background(contexts("ecoinvent", "3.12").background, policy=MigrationPolicy.permissive())
    assert inventory.data == data


@pytest.mark.parametrize("release", ["2025", "native2026"])
def test_lcd_suppliers_export_and_reimport_through_brightway_and_simapro(tmp_path, release):
    data = batch("ecoinvent-3.9-cutoff", -1)
    data[0]["comment"] = "Synthetic LCD-service export fixture."
    for e in data[0]["exchanges"][1:-1]:
        e.pop("formula")
        e["type"] = "technosphere"
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.9")).migrate_background(
        target_context(release), policy=POLICY
    )
    restored = BrightwayInventory.from_excel(result.write_excel(tmp_path / "lcd.xlsx"))
    categorized = result.data
    categorized[0]["exchanges"][0]["simapro category"] = "Materials/Processing services"
    csv = BrightwayInventory.from_data(categorized, context=result.context).to_simapro().write_csv(tmp_path / "lcd.csv")
    loaded = SimaProInventory.from_csv(
        csv, background_profile=result.background_profile, biosphere_profile=result.context.background.biosphere
    ).to_brightway()
    expected = [(tuple(e[f] for f in FIELDS), e["amount"]) for e in result.data[0]["exchanges"][1:-1]]
    for inventory in (restored, loaded):
        assert len(inventory.data) == 1 and not inventory.validate().has_errors
        observed = [
            (tuple(e[f] for f in FIELDS), e["amount"])
            for e in inventory.data[0]["exchanges"]
            if e["type"] == "technosphere"
        ]
        assert Counter(observed) == Counter(expected)


@pytest.mark.parametrize("label", LABELS)
@pytest.mark.parametrize("amount", [-10, 0, 10])
def test_lcd_market_retains_manual_mechanical_shares_and_all_freight(label, amount):
    from test_uvek_market_compositions import assert_composition_quantities

    source = ("market for used liquid crystal display", "used liquid crystal display", "GLO", "kilogram")
    row = COMPOSITIONS[source]
    p = next(p for p in row["profiles"] if p["label"] == label)
    assert len(p["inputs"]) == 6 and p["source_sign"] == -1
    assert {e["name"]: e["amount"] for e in p["inputs"] if e["name"].startswith("Dismantling, LCD")} == {
        "Dismantling, LCD screen, manually, at plant": 0.23,
        "Dismantling, LCD screen, mechanically, at plant": 0.77,
    }
    assert len([e for e in p["inputs"] if e["unit"] == "ton kilometer"]) == 4
    if label.split("-")[1] in ("3.11", "3.12"):
        assert [(e["name"], e["amount"]) for e in p["inputs"] if e["name"] == "Transport, freight, rail"] == [
            ("Transport, freight, rail", 0.112)
        ]
        assert [(e["name"], e["amount"]) for e in p["inputs"] if e["name"] == "Transport, barge"] == [
            ("Transport, barge", 0.0199)
        ]
    assert_composition_quantities(label, amount, definitions={source: row})
