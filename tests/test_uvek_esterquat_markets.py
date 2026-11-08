"""Esterquat recovery credits retain their observed feedstock mix and freight."""

from collections import defaultdict
from copy import deepcopy

import pytest
from test_uvek_market_compositions import POLICY, assert_composition_quantities, batch
from test_uvek_reference_signs import FIELDS, consumer, contexts
from test_uvek_refrigerant_disposal import target_context

from brightpath import BrightwayInventory, SimaProInventory
from brightpath.exceptions import MigrationError
from scripts.review_uvek_esterquat_markets import APPROVAL, validate_provider_inventories
from scripts.review_uvek_market_compositions import COMPOSITIONS

DEFINITIONS = {s: r for s, r in COMPOSITIONS.items() if s[0] == "market for esterquat"}
LABELS = sorted({p["label"] for row in DEFINITIONS.values() for p in row["profiles"] if p["label"].endswith("-cutoff")})
COCONUT = "xx Esterquat, coconut oil and palm kernel oil, at plant"
TALLOW = "xx Esterquat, tallow, at plant"


@pytest.mark.parametrize("label", LABELS)
@pytest.mark.parametrize("amount", [-5 / 3, 0, 5 / 3])
def test_esterquat_markets_preserve_credit_sign_uncertainty_and_every_supplier(label, amount):
    assert_composition_quantities(label, amount, definitions=DEFINITIONS)
    model = label.split("-")[-1]
    for row in DEFINITIONS.values():
        profile = next(p for p in row["profiles"] if p["label"] == label)
        supplies = defaultdict(float)
        for e in profile["inputs"]:
            if "Esterquat" in e["name"]:
                supplies[e["name"]] += e["amount"]
        assert supplies[COCONUT] == pytest.approx(1 if model == "consequential" else 0.883720930232558)
        assert supplies[TALLOW] == pytest.approx(0 if model == "consequential" else 0.116279069767442)
        assert sum(supplies.values()) == pytest.approx(1)
        assert sum(e["amount"] for e in profile["inputs"] if e["unit"] == "ton kilometer") > 0


@pytest.mark.parametrize("label", LABELS)
def test_native_2026_keeps_esterquat_credits_and_complete_market_helpers(label):
    _, version, model = label.split("-")
    data = batch(label, -5 / 3, definitions=DEFINITIONS)
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", version, model)).migrate_background(
        target_context("native2026"), policy=POLICY
    )
    assert len(result.data) == 3
    assert all(e["amount"] == pytest.approx(-5 / 3) for e in result.data[0]["exchanges"][1:])
    assert all(len(d["exchanges"]) > 1 for d in result.data[1:])
    assert not any(i.code.endswith("link_unresolved") for i in result.last_migration_report.issues)


def provider_evidence():
    inventories, observations, native = defaultdict(list), {"ecoinvent": defaultdict(set), "uvek": defaultdict(set)}, {}
    for row in APPROVAL["provider_inventories"]:
        key = tuple(row["source"][f] for f in FIELDS)
        inventories[key].append((row["label"], row["inventory_sha256"]))
        family = row["label"].split("-")[0]
        observations[family][key].add((row["label"], 1))
        if family == "uvek":
            native[key] = {"native_flow_type": "PRODUCT_FLOW", "native_reference_is_input": False}
    return inventories, observations, native


@pytest.mark.parametrize("change", ["none", "hash", "missing", "duplicate", "sign", "native", "extra"])
def test_provider_inventory_and_native_role_gates_require_the_exact_independent_review(change):
    inventories, observations, native = provider_evidence()
    source = next(iter(inventories))
    label, digest = inventories[source][0]
    if change == "none":
        validate_provider_inventories(inventories, observations, native)
        return
    if change == "hash":
        inventories[source][0] = (label, "0" * 64)
    elif change == "missing":
        inventories[source].pop()
    elif change == "duplicate":
        inventories[source].append((label, digest))
    elif change == "sign":
        observations["ecoinvent"][source].discard((label, 1))
        observations["ecoinvent"][source].add((label, -1))
    elif change == "native":
        next(iter(native.values()))["native_reference_is_input"] = True
    else:
        inventories[("uninspected", "product", "GLO", "kilogram")] = [(label, digest)]
    with pytest.raises(ValueError, match="Esterquat"):
        validate_provider_inventories(inventories, observations, native)


@pytest.mark.parametrize("patch", ["3.9.1", "3.10.1"])
def test_foreground_ownership_strict_policy_and_uninspected_patch_remain_protected(patch):
    data = batch("ecoinvent-3.12-cutoff", -1, definitions=DEFINITIONS)
    before = deepcopy(data)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.12", "cutoff"))
    with pytest.raises(MigrationError):
        inventory.migrate_background(target_context("2025"))
    assert inventory.data == data == before
    for source in DEFINITIONS:
        identity = dict(zip(FIELDS, source, strict=True))
        data.append({**identity, "exchanges": [{**identity, "amount": 1, "type": "production"}]})
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.12", "cutoff")).migrate_background(
        target_context("2025"), policy=POLICY
    )
    assert result.data == data
    with pytest.raises(MigrationError) as exc:
        BrightwayInventory.from_data(before, context=contexts("ecoinvent", patch, "cutoff")).migrate_background(
            target_context("2025"), policy=POLICY
        )
    assert any(i.code == "migration.reference_flow_sign_unresolved" for i in exc.value.report.issues)


@pytest.mark.parametrize("release", ["2025", "native2026"])
def test_esterquat_helpers_and_signed_credit_survive_excel_and_simapro_exports(release, tmp_path):
    source = dict(zip(FIELDS, next(iter(DEFINITIONS)), strict=True))
    data = consumer(source, -5 / 3)
    data[0]["comment"] = "Esterquat market and recovery-credit regression."
    data[0]["exchanges"][1].pop("formula")
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.12", "cutoff")).migrate_background(
        target_context(release), policy=POLICY
    )
    xlsx = BrightwayInventory.from_excel(result.write_excel(tmp_path / "esterquat.xlsx"))
    categorized = deepcopy(result.data)
    for dataset in categorized:
        dataset["exchanges"][0]["simapro category"] = "Materials/Chemicals"
    csv = (
        BrightwayInventory.from_data(categorized, context=result.context)
        .to_simapro()
        .write_csv(tmp_path / "esterquat.csv")
    )
    simapro = SimaProInventory.from_csv(
        csv, background_profile=result.background_profile, biosphere_profile=result.context.background.biosphere
    ).to_brightway()
    for restored in [xlsx, simapro]:
        assert len(restored.data) == 2 and not restored.validate().has_errors
        credit = next(e for e in restored.data[0]["exchanges"] if e["type"] == "technosphere")
        assert credit["amount"] == pytest.approx(-5 / 3)
        assert any("Esterquat, coconut oil" in e["name"] for e in restored.data[1]["exchanges"])
