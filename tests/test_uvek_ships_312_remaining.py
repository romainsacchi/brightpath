"""Ship production, battery treatment signs and complete native market boundaries."""

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
from brightpath.migrations.scaling import scale_exchange

REVIEW = json.loads((DATA_DIR / "export/uvek_ships_312_remaining_review.json").read_text())
ROWS = REVIEW["records"]


def source(amount, uncertainty=2):
    fg = {"name": "Shipping component user", "reference product": "output", "location": "CH", "unit": "unit"}
    edges = []
    for row in ROWS:
        edges.append(
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
        )
    return [{**fg, "exchanges": [{**fg, "type": "production", "amount": 1}, *edges]}]


def policy(approved=True):
    resource = load_uvek_technosphere_resource()
    ids = {r["rule_id"] for r in ROWS}
    scopes = tuple(
        compatibility_rule_fingerprint(
            resource,
            "replace",
            rule,
            TechnosphereProfile("ecoinvent", "3.12", "cutoff"),
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
def test_shipping_boundaries_keep_signed_demands_and_complete_suppliers(amount, uncertainty, release):
    data = source(amount, uncertainty)
    before = deepcopy(data)
    result = BrightwayInventory.from_data(data, context=context("ecoinvent", "3.12")).migrate_background(
        context("uvek", release).background, policy=policy()
    )
    assert data == before and len(result.data) == 6
    helpers = {a["name"]: a for a in result.data[1:]}
    for i, row in enumerate(ROWS, 1):
        expected = deepcopy(before[0]["exchanges"][i])
        scale_exchange(expected, row["compiled_rule"]["conversion_factor"])
        actual = result.data[0]["exchanges"][i]
        for key in ("amount", "formula", "uncertainty type", "loc", "scale", "negative"):
            assert actual[key] == expected[key]
        if row["recipe_id"] is None:
            assert row["kind"] in ("ship", "maintenance")
            assert actual["name"] == row["compiled_rule"]["target"]["name"]
            assert "Wrecking" not in actual["name"]
            continue
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
        if row["kind"] == "used":
            assert actual["amount"] == -amount
            treatments = [e for e in inputs if e["unit"] == "kilogram"]
            assert [e["amount"] for e in treatments] == [0.5, 0.5]
            assert {e["name"] for e in treatments} == {
                "Disposal, Li-ions batteries, pyrometallurgical",
                "Disposal, Li-ions batteries, hydrometallurgical",
            }
            assert len([e for e in inputs if e["unit"] == "ton kilometer"]) == 4
        elif row["kind"] == "battery":
            assert len([e for e in inputs if e["unit"] == "kilogram"]) == 2
            assert len([e for e in inputs if e["unit"] == "ton kilometer"]) == 4
            assert all(
                e["name"] == "Battery, Li-ion, NMC111, rechargeable, prismatic"
                for e in inputs
                if e["unit"] == "kilogram"
            )
        else:
            assert row["kind"] == "port"
            assert [e["amount"] for e in inputs] == [0.330592351639294, 0.669407648360706]


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
def test_uninspected_profiles_and_withdrawn_approvals_refuse_without_mutation(version, model, approved):
    data = source(-1)
    before = deepcopy(data)
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(data, context=context("ecoinvent", version, model)).migrate_background(
            context("uvek", "2026").background, policy=policy(approved)
        )
    assert data == before
