"""Dedicated regional waste services preserve mass, signs and caller-owned inventories."""

from collections import Counter
from copy import deepcopy

import pytest
from test_uvek_reference_signs import FIELDS, consumer, contexts
from test_uvek_refrigerant_disposal import target_context

from brightpath import BrightwayInventory, SimaProInventory, TechnosphereProfile
from brightpath.background.reference_signs import select_profile_sign_rules
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import (
    load_uvek_release_resources,
    load_uvek_technosphere_resource,
)
from scripts.review_uvek_dedicated_disposals import (
    APPROVAL,
    DISPOSALS,
    TARGETS,
    dedicated_disposal_profiles,
)
from scripts.review_uvek_market_compositions import COMPOSITIONS

POLICY = MigrationPolicy(on_heuristic_mapping=PolicyAction.WARN, on_information_loss=PolicyAction.WARN)
LABELS = [
    label
    for label in sorted({p["label"] for r in DISPOSALS.values() for p in r["profiles"]})
    if label.endswith("-cutoff")
]
FORMOX = next(s for s in DISPOSALS if "Formox" in s[0])
MARKETS = {
    s: r
    for s, r in COMPOSITIONS.items()
    if s[0]
    in {
        "market for dust, alloyed electric arc furnace steel",
        "market for slag, unalloyed electric arc furnace steel",
        "market for used glider from electric scooter",
        "market for used internal combustion engine, passenger car",
        "market for used powertrain from electric scooter",
    }
}


def batch(label, amount):
    result = None
    for source, r in DISPOSALS.items():
        if label not in {p["label"] for p in r["profiles"]}:
            continue
        for kind in ("technosphere", "substitution"):
            for uncertainty in (2, 3):
                row = consumer(dict(zip(FIELDS, source, strict=True)), amount, uncertainty, kind)
                row[0]["exchanges"][1]["caller_metadata"] = {"retain": [1, "dedicated disposal"]}
                if result is None:
                    result = row
                else:
                    result[0]["exchanges"].append(row[0]["exchanges"][1])
    result[0]["comment"] = "Synthetic dedicated-disposal regression consumer."
    result[0]["exchanges"].append(
        {
            "name": "Carbon dioxide, fossil",
            "categories": ["air"],
            "unit": "kilogram",
            "type": "biosphere",
            "amount": 0.5,
        }
    )
    return result


@pytest.mark.parametrize("label", LABELS)
@pytest.mark.parametrize("amount", [-10, 0, 10])
@pytest.mark.parametrize("release", ["2025", "native2026"])
def test_dedicated_services_preserve_mass_credits_uncertainty_and_foreground(label, amount, release):
    _, version, model = label.split("-")
    data = batch(label, amount)
    before = deepcopy(data)
    original = BrightwayInventory.from_data(data, context=contexts("ecoinvent", version, model))
    result = original.migrate_background(target_context(release), policy=POLICY)
    assert len(result.data) == 1 and result.data[0]["exchanges"][0] == before[0]["exchanges"][0]
    assert result.data[0]["exchanges"][-1]["amount"] == 0.5
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
    for index, (old, new) in enumerate(
        zip(before[0]["exchanges"][1:-1], result.data[0]["exchanges"][1:-1], strict=True),
        1,
    ):
        approval = DISPOSALS[tuple(old[f] for f in FIELDS)]
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
            assert new["loc"] == -amount and (new["minimum"], new["maximum"]) == (
                -amount - 1,
                -amount + 1,
            )
        path = f"datasets[0].exchanges[{index}]"
        assert approval["limitations"] in warnings[path].message
        details = losses[path].to_dict()["details"]
        assert details["source"] == {f: old[f] for f in FIELDS}
        assert details["target"] == dict(zip(FIELDS, approval["target"], strict=True))
    assert original.data == data == before
    assert result.migrate_background(target_context(release)).data == result.data


def test_disposal_scope_native_roles_and_waste_boundaries_are_explicit():
    assert len(DISPOSALS) == APPROVAL["source_count"] == 8
    assert sum(len(r["profiles"]) for r in DISPOSALS.values()) == APPROVAL["source_profile_count"] == 38
    assert len(TARGETS) == 8
    assert all(r["native_flow_type"] == "WASTE_FLOW" and r["native_reference_is_input"] for r in TARGETS.values())
    for source, r in DISPOSALS.items():
        assert "wet kg" in r["limitations"] and "No reverse correspondence is inferred" in r["limitations"]
        if "dust" in source[0]:
            assert "105410 ppm" in r["limitations"] and "15.4%" in r["limitations"]
        if "polyethylene terephthalate" in source[0]:
            assert "0.2% water" in r["limitations"] and "'xx' name" in r["limitations"]


@pytest.mark.parametrize("version", ["3.9.1", "3.10.1"])
def test_patch_profiles_never_inherit_disposal_approval(version):
    resource = select_profile_sign_rules(
        load_uvek_technosphere_resource(),
        TechnosphereProfile("ecoinvent", version, "cutoff"),
    )
    rules = [r for r in resource["replace"] if tuple(r["source"][f] for f in FIELDS) in DISPOSALS]
    assert len(rules) == 8 and all(r["reference_flow_sign_review"]["status"] == "unresolved" for r in rules)


def test_strict_loss_policy_and_local_suppliers_remain_authoritative():
    data = batch("ecoinvent-3.6-cutoff", -1)
    before = deepcopy(data)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.6", "cutoff"))
    with pytest.raises(MigrationError) as caught:
        inventory.migrate_background(
            target_context("2025"),
            policy=MigrationPolicy(
                on_heuristic_mapping=PolicyAction.WARN,
                on_information_loss=PolicyAction.ERROR,
            ),
        )
    assert any(i.code == "migration.technosphere_information_loss" for i in caught.value.report.issues)
    assert inventory.data == data == before
    for source in DISPOSALS:
        supplier = dict(zip(FIELDS, source, strict=True))
        data.append(
            {
                **supplier,
                "comment": "Local treatment supplier.",
                "exchanges": [{**supplier, "amount": -1, "type": "production"}],
            }
        )
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.6", "cutoff")).migrate_background(
        target_context("2025"), policy=POLICY
    )
    assert result.data == data
    assert not any(i.code == "migration.technosphere_information_loss" for i in result.last_migration_report.issues)


@pytest.mark.parametrize("mutation", ["source", "target", "missing_profile", "sign"])
@pytest.mark.parametrize("source", [next(iter(DISPOSALS)), FORMOX])
def test_changed_disposal_evidence_cannot_inherit_scientific_approval(mutation, source):
    inventories = {s: [(p["label"], p["inventory_sha256"]) for p in r["profiles"]] for s, r in DISPOSALS.items()}
    inventories.update({t: [("uvek-2025-cutoff", r["inventory_sha256"])] for t, r in TARGETS.items()})
    observations = [(p["label"], -1) for p in DISPOSALS[source]["profiles"]]
    assert dedicated_disposal_profiles(source, observations, inventories, [])
    if mutation in ("source", "target"):
        key = source if mutation == "source" else DISPOSALS[source]["target"]
        inventories[key][0] = (inventories[key][0][0], "0" * 64)
    elif mutation == "missing_profile":
        observations.pop()
    else:
        observations[0] = (observations[0][0], 1)
    with pytest.raises(ValueError, match="repeat the scientific review"):
        dedicated_disposal_profiles(source, observations, inventories, [])


@pytest.mark.parametrize("label", ["ecoinvent-3.6-cutoff", "ecoinvent-3.12-cutoff"])
@pytest.mark.parametrize("release", ["2025", "native2026"])
def test_disposal_endpoints_survive_excel_and_simapro_round_trips(tmp_path, release, label):
    _, version, model = label.split("-")
    data = batch(label, -2)
    for e in data[0]["exchanges"][1:-1]:
        e.pop("formula")
        e["type"] = "technosphere"
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", version, model)).migrate_background(
        target_context(release), policy=POLICY
    )
    xlsx = BrightwayInventory.from_excel(result.write_excel(tmp_path / "disposal.xlsx"))
    categorized = deepcopy(result.data)
    categorized[0]["exchanges"][0]["simapro category"] = "Materials/Processing services"
    csv = (
        BrightwayInventory.from_data(categorized, context=result.context)
        .to_simapro()
        .write_csv(tmp_path / "disposal.csv")
    )
    simapro = SimaProInventory.from_csv(
        csv,
        background_profile=result.background_profile,
        biosphere_profile=result.context.background.biosphere,
    ).to_brightway()
    expected = Counter((tuple(e[f] for f in FIELDS), e["amount"]) for e in result.data[0]["exchanges"][1:-1])
    for restored in (xlsx, simapro):
        assert len(restored.data) == 1 and not restored.validate().has_errors
        observed = Counter(
            (tuple(e[f] for f in FIELDS), e["amount"])
            for e in restored.data[0]["exchanges"]
            if e["type"] == "technosphere"
        )
        assert observed == expected


@pytest.mark.parametrize(
    "label",
    [f"ecoinvent-{v}-{m}" for v in ("3.6", "3.7") for m in ("cutoff",)],
)
@pytest.mark.parametrize("amount", [-10, 0, 10])
def test_new_disposal_markets_retain_all_original_shares_freight_and_credits(label, amount):
    from test_uvek_market_compositions import assert_composition_quantities

    assert len(MARKETS) == 5 and sum(len(r["profiles"]) for r in MARKETS.values()) == 16
    assert_composition_quantities(label, amount, definitions=MARKETS)


@pytest.mark.parametrize("target", list(TARGETS))
def test_reverse_disposals_remain_independently_unsupported(target):
    data = consumer(dict(zip(FIELDS, target, strict=True)), 1)
    before = deepcopy(data)
    original = BrightwayInventory.from_data(data, context=contexts("uvek", "2025"))
    with pytest.raises(MigrationError):
        original.migrate_background(
            contexts("ecoinvent", "3.12").background,
            policy=MigrationPolicy.permissive(),
        )
    assert original.data == data == before


def test_formox_is_a_spent_carrier_landfill_with_exact_modern_profile_scope():
    approval = DISPOSALS[FORMOX]
    assert FORMOX[2] == "RER" and approval["target"][2] == "CH"
    assert {p["label"] for p in approval["profiles"]} == {
        f"ecoinvent-{v}-{m}" for v in ("3.10", "3.11", "3.12") for m in ("cutoff", "consequential")
    }
    assert all(p["active_biosphere_count"] == 24 for p in approval["profiles"])
    assert TARGETS[approval["target"]]["active_biosphere_count"] == 8
    assert "unrecycled matrix" in approval["limitations"]
    assert "steam production" in approval["limitations"] and "Source moisture is unavailable" in approval["limitations"]
    assert [b["source_count"] for b in APPROVAL["review_batches"]] == [7, 1]
    assert [b["source_profile_count"] for b in APPROVAL["review_batches"]] == [32, 6]


@pytest.mark.parametrize("label", [f"ecoinvent-{v}-{m}" for v in ("3.10", "3.11", "3.12") for m in ("cutoff",)])
@pytest.mark.parametrize("amount", [-10, 0, 10])
def test_formox_market_preserves_the_unrounded_carrier_share_and_original_freight(label, amount):
    from test_uvek_market_compositions import assert_composition_quantities

    source = (
        "market for spent Formox catalyst base from formaldehyde production",
        "spent Formox catalyst base from formaldehyde production",
        "RER",
        "kilogram",
    )
    approval = COMPOSITIONS[source]
    profile = next(p for p in approval["profiles"] if p["label"] == label)
    assert len(profile["inputs"]) == 2
    treatment = next(e for e in profile["inputs"] if e["unit"] == "kilogram")
    freight = next(e for e in profile["inputs"] if e["unit"] == "ton kilometer")
    assert treatment["amount"] == 0.9999999998340274
    assert freight["amount"] == 0.077
    assert treatment["name"] == DISPOSALS[FORMOX]["target"][0]
    assert_composition_quantities(label, amount, definitions={source: approval})
