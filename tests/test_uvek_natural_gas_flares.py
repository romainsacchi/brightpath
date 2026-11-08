"""Modern flaring preserves reference energy/volume, signed demand and explicit emission boundaries."""

import json
import math
from copy import deepcopy
from pathlib import Path

import pytest
from test_uvek_reference_signs import FIELDS, consumer, contexts
from test_uvek_refrigerant_disposal import target_context
from uvek_review_helpers import historical_forward_resource

from brightpath import BrightwayInventory, FormatProfile, InventoryContext, TechnosphereProfile
from brightpath.background.reference_signs import select_profile_sign_rules
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_release_resources
from scripts.review_uvek_natural_gas_flares import (
    APPROVAL,
    FLARES,
    REVERSE,
    TARGET,
    VOLUME_PER_MJ,
    forward_profiles,
    legacy_projection,
    reverse_decision,
    reverse_rule,
    validate_inventories,
)
from scripts.review_uvek_reference_signs import rule_binding

ROOT = Path(__file__).resolve().parents[1]
POLICY = MigrationPolicy.permissive()
LABELS = [
    label
    for label in sorted({p["label"] for r in APPROVAL["rules"] for p in r["profiles"] if p["status"] == "verified"})
    if label.endswith("-cutoff")
]
OLD_LABELS = sorted(
    {
        p["label"]
        for r in APPROVAL["rules"]
        for p in r["profiles"]
        if p["status"] == "unresolved" and p["label"].endswith("-cutoff")
    }
)


def evidence():
    inventories = {}
    for row in APPROVAL["rules"]:
        source = tuple(row["source"][f] for f in FIELDS)
        inventories[source] = [
            {"source": list(source), **{k: p[k] for k in ("label", "inventory_sha256", "activity_comment_sha256")}}
            for p in row["profiles"]
        ]
    target = APPROVAL["target"]
    inventories[TARGET] = [
        {
            "label": "uvek-2025-cutoff",
            "source": list(TARGET),
            **{k: target[k] for k in ("inventory_sha256", "activity_comment_sha256")},
        }
    ]
    return inventories


def batch(amount):
    data = None
    for source in FLARES:
        for kind, uncertainty in (("technosphere", 2), ("substitution", 3)):
            part = consumer(dict(zip(FIELDS, source, strict=True)), amount, uncertainty, kind)
            part[0]["exchanges"][1]["caller_metadata"] = {"retain": [1, 2]}
            if data is None:
                data = part
            else:
                data[0]["exchanges"].append(part[0]["exchanges"][1])
    data[0]["exchanges"].append(
        {
            "name": "Carbon dioxide, fossil",
            "categories": ["air"],
            "unit": "kilogram",
            "amount": 0.5,
            "type": "biosphere",
        }
    )
    return data


def assert_scaled(before, after, factor):
    assert after["amount"] == pytest.approx(before["amount"] * factor)
    assert after["type"] == before["type"] and after.get("caller_metadata") == before.get("caller_metadata")
    assert after["scale"] == pytest.approx(before["scale"] * (1 if before["uncertainty type"] == 2 else abs(factor)))
    if before["uncertainty type"] == 2:
        assert after["loc"] == pytest.approx(before["loc"] + math.log(abs(factor)))
        assert after["negative"] is (before["amount"] >= 0)
    else:
        assert after["loc"] == pytest.approx(before["loc"] * factor)
        assert after["minimum"] == pytest.approx(before["maximum"] * factor)
        assert after["maximum"] == pytest.approx(before["minimum"] * factor)
    assert eval(after["formula"], {"__builtins__": {}}, {"dose": 2}) == pytest.approx(2 * factor)


@pytest.mark.parametrize("label", LABELS)
@pytest.mark.parametrize("release", ["2025", "native2026"])
@pytest.mark.parametrize("amount", [-36, 0, 36])
def test_modern_direct_and_market_flares_scale_signed_energy_uncertainty_and_formulas(label, release, amount):
    _, version, model = label.split("-")
    data = batch(amount)
    before = deepcopy(data)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", version, model))
    result = inventory.migrate_background(target_context(release), policy=POLICY)
    assert len(result.data) == 1 and len(result.data[0]["exchanges"]) == len(before[0]["exchanges"])
    for old, new in zip(before[0]["exchanges"][1:-1], result.data[0]["exchanges"][1:-1], strict=True):
        assert_scaled(old, new, -VOLUME_PER_MJ)
        target = dict(zip(FIELDS, TARGET, strict=True))
        if release != "2025":
            bridge = next(
                r for r in load_uvek_release_resources("technosphere") if r["source_profiles"][0]["version"] == "2025"
            )
            target = next(r["target"] for r in bridge["replace"] if r["source"] == target)
        assert tuple(new[f] for f in FIELDS) == tuple(target[f] for f in FIELDS)
    assert result.data[0]["exchanges"][0] == before[0]["exchanges"][0]
    assert result.data[0]["exchanges"][-1]["amount"] == 0.5
    warnings = [i for i in result.last_migration_report.issues if i.code == "migration.technosphere_information_loss"]
    assert len(warnings) == len(before[0]["exchanges"]) - 2
    assert all("resource withdrawal" in i.message for i in warnings)
    assert not any(i.code == "migration.technosphere_omission" for i in result.last_migration_report.issues)
    assert inventory.data == data == before
    assert result.migrate_background(target_context(release)).data == result.data


@pytest.mark.parametrize("release", ["2025", "native2026"])
@pytest.mark.parametrize("amount", [-1, 0, 1])
def test_independent_reverse_flare_scales_one_cubic_meter_to_36_mj(release, amount):
    source = REVERSE["source"]
    if release != "2025":
        bridge = next(
            r for r in load_uvek_release_resources("technosphere") if r["source_profiles"][0]["version"] == "2025"
        )
        source = next(r["target"] for r in bridge["replace"] if r["source"] == source)
    data = consumer(source, amount, 3)
    before = deepcopy(data)
    source_context = target_context(release)
    inventory = BrightwayInventory.from_data(
        data, context=InventoryContext(FormatProfile("brightway_excel"), source_context)
    )
    result = inventory.migrate_background(contexts("ecoinvent", "3.12").background, policy=POLICY)
    after = result.data[0]["exchanges"][1]
    assert_scaled(before[0]["exchanges"][1], after, -36)
    assert tuple(after[f] for f in FIELDS) == tuple(REVERSE["target"][f] for f in FIELDS)
    assert inventory.data == data == before
    # Actual independently inspected resource quantities agree, including the extraction accounted in the flare.
    source_scale = amount / REVERSE["source_reference_sign"]
    target_scale = after["amount"] / REVERSE["target_reference_sign"]
    assert source_scale * APPROVAL["target"]["gas_resource_m3"] == pytest.approx(target_scale * VOLUME_PER_MJ)


@pytest.mark.parametrize("label", OLD_LABELS)
def test_older_emission_boundaries_use_source_helpers_with_methane_and_nox(label):
    _, version, model = label.split("-")
    data = batch(-36)
    before = deepcopy(data)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", version, model))
    result = inventory.migrate_background(target_context("2025"), policy=POLICY)
    assert len(result.data) > 1
    names = {e["name"] for h in result.data[1:] for e in h["exchanges"] if e["type"] == "biosphere"}
    assert {"Methane, fossil", "Nitrogen oxides"} <= names
    assert not any(i.code == "migration.reference_flow_sign_unresolved" for i in result.last_migration_report.issues)
    assert inventory.data == data == before


def test_source_helpers_supersede_pending_profiles_without_extending_the_native_proxy():
    from scripts.review_uvek_remaining_gas_flares import SOURCES

    resource = historical_forward_resource()
    queue = json.loads((ROOT / "docs/uvek_forward_review_queue.json").read_text())
    pending = {r["id"] for r in queue["rules"]}
    # The original native-proxy approval remains historical evidence with its exact scope.
    assert len(FLARES) == APPROVAL["source_count"] == 2
    assert APPROVAL["approved_source_profile_count"] == 16 and APPROVAL["pending_source_profile_count"] == 12
    found = 0
    for rule in resource["replace"]:
        source = tuple(rule["source"][f] for f in FIELDS)
        if source not in SOURCES:
            continue
        found += 1
        assert rule["id"] not in pending
        entries = rule["reference_flow_sign_review"]["profiles"]
        assert len(entries) == 9
        assert all(p["source_profile"]["system_model"] == "cutoff" for p in entries)
        for p in entries:
            retained = source in FLARES and p["source_profile"]["version"] in {"3.9", "3.10", "3.11", "3.12"}
            assert p["review"]["status"] == "verified"
            assert p["conversion_factor"] == (-1 / 36 if retained else -1)
            if retained:
                assert tuple(p.get("target", rule["target"])[f] for f in FIELDS) == TARGET
            else:
                assert p["target"]["recipe_ref"] in resource["recipes"]
        unknown = select_profile_sign_rules({"replace": [rule]}, TechnosphereProfile("ecoinvent", "3.12.1", "cutoff"))[
            "replace"
        ][0]
        assert unknown["reference_flow_sign_review"]["status"] == "unresolved"
    assert found == 4


def test_strict_information_loss_foreground_ownership_and_unrelated_errors_remain_transactional(
    unreviewed_uvek_correspondence,
):
    data = batch(-36)
    original = deepcopy(data)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.12"))
    for policy in [MigrationPolicy(), MigrationPolicy(on_heuristic_mapping=PolicyAction.WARN)]:
        with pytest.raises(MigrationError):
            inventory.migrate_background(target_context("2025"), policy=policy)
    assert inventory.data == original
    for source in FLARES:
        identity = dict(zip(FIELDS, source, strict=True))
        data.append({**identity, "exchanges": [{**identity, "type": "production", "amount": -1}]})
    owned = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.12")).migrate_background(
        target_context("2025"), policy=POLICY
    )
    assert owned.data == data
    broken = deepcopy(original)
    broken[0]["exchanges"].append({**unreviewed_uvek_correspondence, "type": "technosphere", "amount": 1})
    inv = BrightwayInventory.from_data(broken, context=contexts("ecoinvent", "3.12"))
    with pytest.raises(MigrationError):
        inv.migrate_background(target_context("2025"), policy=POLICY)
    assert inv.data == broken


@pytest.mark.parametrize("change", ["source_hash", "boundary", "target_hash", "profile"])
def test_source_and_target_inventory_or_boundary_changes_cannot_inherit_approval(change):
    inventories = evidence()
    source = next(iter(FLARES))
    if change == "source_hash":
        inventories[source][0]["inventory_sha256"] = "0" * 64
    elif change == "boundary":
        inventories[source][0]["activity_comment_sha256"] = "0" * 64
    elif change == "target_hash":
        inventories[TARGET][0]["inventory_sha256"] = "0" * 64
    else:
        inventories[source][0]["label"] = "ecoinvent-3.12.1-cutoff"
    with pytest.raises(ValueError, match="repeat the independent review"):
        validate_inventories(inventories)


@pytest.mark.parametrize("change", ["factor", "target", "source_profile", "reference_sign", "native_role"])
def test_reverse_correspondence_requires_its_independent_exact_inventory_and_reference_basis(change):
    rule, _ = reverse_rule()
    observations = {("ecoinvent-3.12-cutoff", -1)}
    native = {"native_flow_type": "PRODUCT_FLOW", "native_reference_is_input": False}
    if change == "factor":
        rule["conversion_factor"] = -1
    elif change == "target":
        rule["target"]["name"] = "Natural gas production"
    elif change == "source_profile":
        observations = {("ecoinvent-3.11-cutoff", -1)}
    elif change == "reference_sign":
        observations = {("ecoinvent-3.12-cutoff", 1)}
    else:
        native["native_reference_is_input"] = True
    with pytest.raises(ValueError, match="repeat the independent review"):
        reverse_decision(rule, observations, evidence(), native, {1}, rule_binding(rule))


def test_forward_approval_rejects_missing_or_uninspected_reference_profiles():
    source = next(iter(FLARES))
    observations = [(p["label"], -1) for p in APPROVAL["rules"][0]["profiles"]]
    observations[0] = ("ecoinvent-3.12.1-cutoff", -1)
    with pytest.raises(ValueError, match="repeat the review"):
        forward_profiles(source, observations, evidence(), ["synthetic"])


@pytest.mark.parametrize("change", ["factor", "review", "target"])
def test_later_reverse_addition_cannot_mask_mutations_in_the_frozen_publisher_comparison(change):
    from brightpath.background.uvek_export import load_uvek_export_resource

    resource = deepcopy(load_uvek_export_resource())
    expected, _ = reverse_rule()
    rule = next(r for r in resource["replace"] if r["id"] == expected["id"])
    if change == "factor":
        rule["conversion_factor"] = -1
    elif change == "review":
        resource["reviews"][rule["review_ref"]]["rationale"] = "Unreviewed"
    else:
        rule["target"]["name"] = "Unreviewed"
    with pytest.raises(ValueError, match="Stale independently reviewed"):
        legacy_projection(resource)


@pytest.mark.parametrize("release", ["2025", "native2026"])
def test_flare_volume_and_reference_identity_survive_excel_and_simapro_exports(release, tmp_path):
    from brightpath import SimaProInventory

    source = next(iter(FLARES))
    data = consumer(dict(zip(FIELDS, source, strict=True)), -36)
    data[0]["comment"] = "Sweet-gas flare volume/reference export regression."
    data[0]["exchanges"][1].pop("formula")
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.12")).migrate_background(
        target_context(release), policy=POLICY
    )
    xlsx = BrightwayInventory.from_excel(result.write_excel(tmp_path / "flare.xlsx"))
    categorized = deepcopy(result.data)
    categorized[0]["exchanges"][0]["simapro category"] = "Materials/Processing services"
    path = (
        BrightwayInventory.from_data(categorized, context=result.context).to_simapro().write_csv(tmp_path / "flare.csv")
    )
    simapro = SimaProInventory.from_csv(
        path, background_profile=result.background_profile, biosphere_profile=result.context.background.biosphere
    ).to_brightway()
    expected = result.data[0]["exchanges"][1]
    for restored in [xlsx, simapro]:
        assert len(restored.data) == 1 and not restored.validate().has_errors
        observed = next(e for e in restored.data[0]["exchanges"] if e["type"] == "technosphere")
        assert tuple(observed[f] for f in FIELDS) == tuple(expected[f] for f in FIELDS)
        assert observed["amount"] == pytest.approx(1)
