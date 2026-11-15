"""Exact source3.8 market boundaries and preserved older acid/silica routes."""

import json
import math
from copy import deepcopy

import pytest
from test_uvek_lfp_battery_312 import context

from brightpath import DATA_DIR, BrightwayInventory, TechnosphereProfile
from brightpath.background.rule_approvals import compatibility_rule_fingerprint
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource

REVIEW = json.loads((DATA_DIR / "export/uvek_ships_38_remaining_review.json").read_text())
ROWS = REVIEW["records"]


def source(amount, uncertainty=2, rows=ROWS):
    fg = {"name": "Shipping3.8 material user", "reference product": "output", "location": "CH", "unit": "unit"}
    edges = [
        {
            **row["original_rule"]["source"],
            "type": "technosphere",
            "amount": amount,
            "formula": "component_demand",
            "uncertainty type": uncertainty,
            "loc": math.log(abs(amount)) if uncertainty == 2 else amount,
            "scale": 0.2,
            "negative": amount < 0,
        }
        for row in rows
    ]
    return [{**fg, "exchanges": [{**fg, "type": "production", "amount": 1}, *edges]}]


def policy(version="3.8", rows=ROWS, approved=True):
    resource = load_uvek_technosphere_resource()
    ids = {r["rule_id"] for r in rows}
    scopes = tuple(
        compatibility_rule_fingerprint(
            resource,
            "replace",
            rule,
            TechnosphereProfile("ecoinvent", version, "cutoff"),
            TechnosphereProfile("uvek", "2025", "cutoff"),
        )
        for rule in resource["replace"]
        if rule["id"] in ids
    )
    return MigrationPolicy(
        approved_compatibility_rules=scopes if approved else (), on_information_loss=PolicyAction.WARN
    )


@pytest.mark.parametrize("amount", [-2.5, 2.5])
@pytest.mark.parametrize("uncertainty", [2, 3])
@pytest.mark.parametrize("release", ["2025", "2026"])
def test_exact38_native_markets_retain_mass_signs_statistics_and_final_delivery(amount, uncertainty, release):
    data = source(amount, uncertainty)
    before = deepcopy(data)
    result = BrightwayInventory.from_data(data, context=context("ecoinvent", "3.8")).migrate_background(
        context("uvek", release).background, policy=policy()
    )
    assert data == before and len(result.data) == 7
    helpers = {a["name"]: a for a in result.data[1:]}
    for i, row in enumerate(ROWS, 1):
        actual = result.data[0]["exchanges"][i]
        for key in ("amount", "formula", "uncertainty type", "loc", "scale", "negative"):
            assert actual[key] == before[0]["exchanges"][i][key]
        helper = helpers[actual["name"]]
        assert [e["amount"] for e in helper["exchanges"] if e["type"] == "production"] == [1]
        inputs = [e for e in helper["exchanges"] if e["type"] != "production"]
        wanted = REVIEW["recipes"][row["recipe_id"]]["inputs"]
        assert len(inputs) == len(wanted)
        for got, want in zip(inputs, wanted, strict=True):
            for key in (
                "name",
                "reference product",
                "location",
                "unit",
                "amount",
                "uncertainty type",
                "loc",
                "scale",
                "negative",
            ):
                if key in want:
                    assert got[key] == want[key]
        if row["kind"] == "wood":
            stocks = [e for e in inputs if e["unit"] == "kilogram"]
            assert len(stocks) == 6 and sum(e["amount"] for e in stocks) == pytest.approx(1)
            assert all("production mix, wet, measured as dry mass" in e["name"] for e in stocks)
            assert any("not a dry-chip-equivalent" in s for s in row["limits"])
        elif row["kind"] == "silica":
            assert len([e for e in inputs if e["unit"] == "ton kilometer"]) == 5
            assert len([e for e in inputs if e["name"] == "Cryolite, at plant"]) == 1
            assert len([e for e in inputs if e["name"] == "Silica sand, at plant"]) == 2
        elif row["kind"] == "acid":
            assert len([e for e in inputs if e["name"] == "Sulphuric acid, liquid, at plant"]) == 2
            assert len([e for e in inputs if e["unit"] == "ton kilometer"]) == 3
        elif row["kind"] == "furnace":
            assert all(e["name"] == "Industrial furnace, 1MW, natural gas" for e in inputs)
            assert sum(e["amount"] for e in inputs) == pytest.approx(1)


@pytest.mark.parametrize(
    "rid,version", [("bp-7740a48a01851669", "3.6"), ("bp-7740a48a01851669", "3.9"), ("bp-dbd744f486420f56", "3.9")]
)
@pytest.mark.parametrize("amount", [-2.5, 2.5])
def test_older_qualified_acid_and_silica_keep_original_suppliers_and_scaling(rid, version, amount):
    row = next(r for r in ROWS if r["rule_id"] == rid)
    data = source(amount, rows=[row])
    result = BrightwayInventory.from_data(data, context=context("ecoinvent", version)).migrate_background(
        context("uvek", "2026").background, policy=policy(version, [row])
    )
    edge = result.data[0]["exchanges"][1]
    assert edge["amount"] == amount
    if row["kind"] == "acid":
        assert len(result.data) == 1
        assert edge["name"] == "Sulphuric acid, liquid, at plant"
    else:
        assert len(result.data) == 2
        prior = load_uvek_technosphere_resource()["recipes"][row["original_rule"]["target"]["recipe_ref"]]
        assert edge["name"] == result.data[1]["name"]
        assert edge["name"].startswith(prior["name"] + " [BrightPath UVEK conversion ")
        retained = [e for e in result.data[1]["exchanges"] if e["type"] != "production"]
        assert len(retained) == len(prior["inputs"])
        for actual, expected in zip(retained, prior["inputs"], strict=True):
            for key in ("name", "reference product", "location", "unit", "amount"):
                assert actual[key] == expected[key]


@pytest.mark.parametrize(
    "version,model,approved",
    [
        ("3.7", "cutoff", True),
        ("3.10", "cutoff", True),
        ("3.12", "cutoff", True),
        ("3.8", "consequential", True),
        ("3.8", "cutoff", False),
    ],
)
def test_other_contexts_and_missing_approval_refuse_without_mutating_source(version, model, approved):
    data = source(-1)
    before = deepcopy(data)
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(data, context=context("ecoinvent", version, model)).migrate_background(
            context("uvek", "2026").background, policy=policy(approved=approved)
        )
    assert data == before
