"""The LFP market must supply complete LiFePO4 packs, preserving signs and final delivery."""

import json
import math
from copy import deepcopy

import pytest

from brightpath import (
    DATA_DIR,
    BackgroundContext,
    BiosphereProfile,
    BrightwayInventory,
    FormatProfile,
    InventoryContext,
    TechnosphereProfile,
)
from brightpath.background.rule_approvals import compatibility_rule_fingerprint
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource

REVIEW = json.loads((DATA_DIR / "export/uvek_lfp_battery_312_review.json").read_text())
(ROW,) = REVIEW["records"]


def context(family, version, model="cutoff"):
    return InventoryContext(
        FormatProfile("brightway_excel"),
        BackgroundContext(TechnosphereProfile(family, version, model), BiosphereProfile("ecoinvent", "3.10")),
    )


def consumer(amount, uncertainty=2):
    fg = {"name": "LFP battery user", "reference product": "output", "location": "CH", "unit": "unit"}
    edge = {
        **ROW["original_rule"]["source"],
        "type": "technosphere",
        "amount": amount,
        "formula": "battery_mass",
        "uncertainty type": uncertainty,
        "loc": math.log(abs(amount)) if uncertainty == 2 else amount,
        "scale": 0.2,
        "negative": amount < 0,
        "audit note": "Preserve caller information",
    }
    if uncertainty == 3:
        edge.update(minimum=amount - 1, maximum=amount + 1)
    return [{**fg, "exchanges": [{**fg, "amount": 1, "type": "production"}, edge]}]


def policy(approved=True):
    resource = load_uvek_technosphere_resource()
    rule = next(r for r in resource["replace"] if r["id"] == ROW["rule_id"])
    fingerprint = compatibility_rule_fingerprint(
        resource,
        "replace",
        rule,
        TechnosphereProfile("ecoinvent", "3.12", "cutoff"),
        TechnosphereProfile("uvek", "2025", "cutoff"),
    )
    return MigrationPolicy(
        approved_compatibility_rules=(fingerprint,) if approved else (), on_information_loss=PolicyAction.WARN
    )


@pytest.mark.parametrize("amount", [-2.5, 2.5])
@pytest.mark.parametrize("uncertainty", [2, 3])
@pytest.mark.parametrize("release", ["2025", "2026"])
def test_complete_lfp_packs_keep_signed_mass_statistics_and_delivery(amount, uncertainty, release):
    source = consumer(amount, uncertainty)
    before = deepcopy(source)
    result = BrightwayInventory.from_data(source, context=context("ecoinvent", "3.12")).migrate_background(
        context("uvek", release).background, policy=policy()
    )
    assert source == before and len(result.data) == 2
    edge = result.data[0]["exchanges"][1]
    for key in (
        "amount",
        "formula",
        "uncertainty type",
        "loc",
        "scale",
        "negative",
        "minimum",
        "maximum",
        "audit note",
    ):
        if key in before[0]["exchanges"][1]:
            assert edge[key] == before[0]["exchanges"][1][key]
    (helper,) = result.data[1:]
    assert helper["unit"] == "kilogram"
    assert [e["amount"] for e in helper["exchanges"] if e["type"] == "production"] == [1]
    inputs = [e for e in helper["exchanges"] if e["type"] != "production"]
    assert len(inputs) == 6 and all(e["type"] == "technosphere" for e in inputs)
    packs = [e for e in inputs if e["unit"] == "kilogram"]
    assert len(packs) == 2
    assert all(e["name"] == "Battery, LiIo, rechargeable, prismatic,LiFePO4" for e in packs)
    assert [e["amount"] for e in packs] == [0.7325, 0.2675]
    assert not any("NMC" in e["name"] or "cell" in e["name"].casefold() for e in packs)
    freight = [e for e in inputs if e["unit"] == "ton kilometer"]
    assert [e["amount"] for e in freight] == [0.06231, 0.3196, 1.112, 0.01533]
    for got, want in zip(inputs, REVIEW["recipes"][ROW["recipe_id"]]["inputs"], strict=True):
        for key in ("uncertainty type", "loc", "scale", "negative", "pedigree", "scale without pedigree"):
            if key in want:
                assert got[key] == want[key]


@pytest.mark.parametrize(
    "version,model,approved",
    [
        ("3.9", "cutoff", True),
        ("3.10", "cutoff", True),
        ("3.11", "cutoff", True),
        ("3.12", "consequential", True),
        ("3.12", "cutoff", False),
    ],
)
def test_other_profiles_and_missing_approval_keep_the_source_unchanged(version, model, approved):
    source = consumer(-1)
    before = deepcopy(source)
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(source, context=context("ecoinvent", version, model)).migrate_background(
            context("uvek", "2026").background, policy=policy(approved)
        )
    assert source == before
