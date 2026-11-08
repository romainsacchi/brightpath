"""Exact refinery flare service proxies preserve signed quantities and disclose losses."""

from collections import defaultdict
from copy import deepcopy

import pytest
from test_uvek_reference_signs import FIELDS, consumer, contexts
from test_uvek_refrigerant_disposal import target_context

from brightpath import BrightwayInventory, SimaProInventory, TechnosphereProfile
from brightpath.background.reference_signs import select_profile_sign_rules
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_release_resources, load_uvek_technosphere_resource
from scripts.review_uvek_refinery_flare import APPROVAL, FLARES, LIMITATIONS, TARGET, flare_profiles

POLICY = MigrationPolicy(on_heuristic_mapping=PolicyAction.WARN, on_information_loss=PolicyAction.WARN)
LABELS = [label for label in [p["label"] for p in next(iter(FLARES.values()))["profiles"]] if label.endswith("-cutoff")]


def batch(amount):
    rows = []
    for source in FLARES:
        for kind in ("technosphere", "substitution"):
            for uncertainty in (2, 3, 4, 5):
                rows.append(
                    consumer(dict(zip(FIELDS, source, strict=True)), amount, uncertainty, kind)[0]["exchanges"][1]
                )
    data = consumer(dict(zip(FIELDS, next(iter(FLARES)), strict=True)), amount)
    data[0]["exchanges"][1:] = rows + [
        {
            "name": "Carbon dioxide, fossil",
            "categories": ["air"],
            "unit": "kilogram",
            "type": "biosphere",
            "amount": 0.5,
        }
    ]
    return data


@pytest.mark.parametrize("label", LABELS)
@pytest.mark.parametrize("amount", [-10, 0, 10])
@pytest.mark.parametrize("release", ["2025", "native2026"])
def test_refinery_flare_signs_uncertainty_formulas_and_separate_emissions(label, amount, release):
    _, version, model = label.split("-")
    before = batch(amount)
    original = deepcopy(before)
    inventory = BrightwayInventory.from_data(before, context=contexts("ecoinvent", version, model))
    result = inventory.migrate_background(target_context(release), policy=POLICY)
    target = dict(zip(FIELDS, TARGET, strict=True))
    if release != "2025":
        release_resource = next(
            r for r in load_uvek_release_resources("technosphere") if r["source_profiles"][0]["version"] == "2025"
        )
        target = next(r["target"] for r in release_resource["replace"] if r["source"] == target)
    assert len(result.data) == 1
    assert result.data[0]["exchanges"][0] == original[0]["exchanges"][0]
    emission = result.data[0]["exchanges"][-1]
    assert {k: emission[k] for k in ("name", "amount", "unit", "type")} == {
        k: original[0]["exchanges"][-1][k] for k in ("name", "amount", "unit", "type")
    }
    assert emission["categories"] == (["emissions to air", "unspecified"] if release == "native2026" else ["air"])
    for old, new in zip(original[0]["exchanges"][1:-1], result.data[0]["exchanges"][1:-1], strict=True):
        assert {f: new[f] for f in FIELDS} == target
        assert new["amount"] == -amount and new["type"] == old["type"]
        assert new["formula"] == "(dose) * (-1.0)"
        if old["uncertainty type"] == 2:
            assert new["loc"] == old["loc"] and new["negative"] is (amount >= 0)
        else:
            assert new["loc"] == -amount
            assert (new["minimum"], new["maximum"]) == (-amount - 1, -amount + 1)
        if "scale" in old:
            assert new["scale"] == old["scale"]
    warnings = [i for i in result.last_migration_report.issues if i.code == "migration.technosphere_information_loss"]
    losses = [i for i in result.last_migration_report.losses if i.code == "migration.technosphere_information_loss"]
    assert len(warnings) == len(losses) == 16
    assert all(LIMITATIONS in i.message for i in warnings)
    for loss in losses:
        details = loss.to_dict()["details"]
        assert tuple(details["source"][f] for f in FIELDS) in FLARES
        assert tuple(details["target"][f] for f in FIELDS) == TARGET
    assert inventory.data == before == original
    assert result.migrate_background(target_context(release)).data == result.data


@pytest.mark.parametrize("version", ["3.9.1", "3.10.1"])
@pytest.mark.parametrize("model", ["cutoff"])
def test_refinery_flare_unknown_profiles_do_not_inherit_approval(version, model):
    resource = select_profile_sign_rules(
        load_uvek_technosphere_resource(), TechnosphereProfile("ecoinvent", version, model)
    )
    rules = [r for r in resource["replace"] if tuple(r["source"][f] for f in FIELDS) in FLARES]
    assert len(rules) == 2 and all(r["reference_flow_sign_review"]["status"] == "unresolved" for r in rules)
    data = batch(-1)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", version, model))
    with pytest.raises(MigrationError):
        inventory.migrate_background(target_context("2025"), policy=POLICY)
    assert inventory.data == data


def test_refinery_flare_loss_policy_and_foreground_suppliers_remain_explicit():
    data = batch(-1)
    original = deepcopy(data)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.12"))
    with pytest.raises(MigrationError) as caught:
        inventory.migrate_background(
            target_context("2025"),
            policy=MigrationPolicy(on_heuristic_mapping=PolicyAction.WARN, on_information_loss=PolicyAction.ERROR),
        )
    assert any(i.code == "migration.technosphere_information_loss" for i in caught.value.report.issues)
    assert inventory.data == original
    for source in FLARES:
        supplier = dict(zip(FIELDS, source, strict=True))
        data.append({**supplier, "exchanges": [{**supplier, "amount": -1, "type": "production"}]})
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.12")).migrate_background(
        target_context("2025"), policy=POLICY
    )
    assert result.data == data
    assert not any(i.code == "migration.technosphere_information_loss" for i in result.last_migration_report.issues)


@pytest.mark.parametrize("amount", [-10, 0, 10])
@pytest.mark.parametrize("release", ["2025", "native2026"])
def test_refinery_flare_independently_reviewed_reverse_restores_quantity_and_uncertainty(amount, release):
    data = batch(amount)
    original = deepcopy(data)
    forward = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.12")).migrate_background(
        target_context(release), policy=POLICY
    )
    back = forward.migrate_background(contexts("ecoinvent", "3.12").background, policy=MigrationPolicy.permissive())
    assert len(back.data) == 1
    for old, new in zip(original[0]["exchanges"][1:-1], back.data[0]["exchanges"][1:-1], strict=True):
        # Both source market profiles contain only the direct flare service, so
        # the independently reviewed reverse endpoint is the direct treatment.
        assert new["name"] == "treatment of waste refinery gas, burned in flare"
        assert new["reference product"] == "waste refinery gas" and new["unit"] == "megajoule"
        for field in (
            "amount",
            "type",
            "uncertainty type",
            "loc",
            "scale",
            "negative",
            "minimum",
            "maximum",
            "caller_metadata",
        ):
            assert new.get(field) == old.get(field)
        assert new["formula"] == "((dose) * (-1.0)) * (-1.0)"
    warnings = [i for i in back.last_migration_report.issues if i.code == "migration.documented_proxy"]
    assert len(warnings) == 16
    assert all("nine active biosphere exchanges" in i.message for i in warnings)
    assert data == original


@pytest.mark.parametrize("mutation", ["source", "target", "missing_profile", "sign"])
def test_refinery_flare_authoring_rejects_stale_or_incomplete_evidence(mutation):
    inventories = defaultdict(list)
    observations = {}
    for source, row in FLARES.items():
        observations[source] = [(p["label"], -1) for p in row["profiles"]]
        inventories[source] = [(p["label"], p["inventory_sha256"]) for p in row["profiles"]]
    inventories[TARGET] = [("uvek-2025-cutoff", APPROVAL["target"]["inventory_sha256"])]
    source = next(iter(FLARES))
    assert len(flare_profiles(source, observations[source], inventories, [])) == 14
    if mutation in ("source", "target"):
        key = source if mutation == "source" else TARGET
        label, _ = inventories[key][0]
        inventories[key][0] = (label, "0" * 64)
    elif mutation == "missing_profile":
        observations[source].pop()
    else:
        label, _ = observations[source][0]
        observations[source][0] = (label, 1)
    with pytest.raises(ValueError, match="Refinery-flare inventory or profile changed"):
        flare_profiles(source, observations[source], inventories, [])


@pytest.mark.parametrize("model", ["cutoff"])
@pytest.mark.parametrize("release", ["2025", "native2026"])
def test_refinery_flare_links_round_trip_through_both_export_formats(tmp_path, model, release):
    data = batch(-1)
    data[0]["comment"] = "Synthetic refinery flare service export check."
    for e in data[0]["exchanges"][1:-1]:
        e.pop("formula")
        e["type"] = "technosphere"
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.12", model)).migrate_background(
        target_context(release), policy=POLICY
    )
    restored = BrightwayInventory.from_excel(result.write_excel(tmp_path / "flare.xlsx"))
    categorized = result.data
    categorized[0]["exchanges"][0]["simapro category"] = "Materials/Processing services"
    csv = (
        BrightwayInventory.from_data(categorized, context=result.context).to_simapro().write_csv(tmp_path / "flare.csv")
    )
    loaded = SimaProInventory.from_csv(
        csv, background_profile=result.background_profile, biosphere_profile=result.context.background.biosphere
    ).to_brightway()
    expected = [(tuple(e[f] for f in FIELDS), e["amount"]) for e in result.data[0]["exchanges"][1:-1]]
    for inventory in (restored, loaded):
        assert len(inventory.data) == 1 and not inventory.validate().has_errors
        inputs = [e for e in inventory.data[0]["exchanges"] if e["type"] == "technosphere"]
        assert [(tuple(e[f] for f in FIELDS), e["amount"]) for e in inputs] == expected
        assert all(e["amount"] == 1 for e in inputs)
