"""Petroleum pitch and nickel sulfate retain distinct route and allocation bases."""

import json
from copy import deepcopy

import pytest
from test_uvek_reference_signs import consumer, contexts

from brightpath import DATA_DIR, BrightwayInventory, TechnosphereProfile
from brightpath.background.rule_approvals import compatibility_rule_fingerprint
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource

REVIEW = json.loads((DATA_DIR / "export/uvek_shared_pitch_nickel_39_review.json").read_text())
ROWS = REVIEW["records"]


def policy(approved=True):
    resource = load_uvek_technosphere_resource()
    rules = {r["id"]: r for r in resource["replace"]}
    scopes = tuple(
        compatibility_rule_fingerprint(
            resource,
            "replace",
            rules[r["rule_id"]],
            TechnosphereProfile(**REVIEW["source_profile"]),
            TechnosphereProfile(**REVIEW["target_profile"]),
        )
        for r in ROWS
    )
    return MigrationPolicy(
        approved_compatibility_rules=scopes if approved else (),
        on_information_loss=PolicyAction.WARN,
    )


@pytest.mark.parametrize("amount", [-2.5, 2.5])
@pytest.mark.parametrize("uncertainty", [2, 3])
@pytest.mark.parametrize("release", ["2025", "2026"])
def test_signed_markets_keep_caller_statistics_supply_branches_and_delivery(amount, uncertainty, release):
    data = consumer(ROWS[0]["source"], amount, uncertainty)
    data[0]["exchanges"].append(consumer(ROWS[1]["source"], amount, uncertainty)[0]["exchanges"][1])
    original = deepcopy(data)
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.9")).migrate_background(
        contexts("uvek", release).background, policy=policy()
    )
    assert data == original and len(result.data) == 20
    helpers = {a["brightpath_conversion"]["recipe_id"]: a for a in result.data[1:]}
    for actual, old, row in zip(result.data[0]["exchanges"][1:], original[0]["exchanges"][1:], ROWS, strict=True):
        for key in ("amount", "loc", "scale", "formula", "negative", "minimum", "maximum", "uncertainty type"):
            if key in old:
                assert actual[key] == old[key]
        market = helpers[row["target"]["recipe_ref"]]
        assert actual["name"] == market["name"] and actual["unit"] == "kilogram"
        for got, expected in zip(
            market["exchanges"][1:], REVIEW["recipes"][row["target"]["recipe_ref"]]["inputs"], strict=True
        ):
            target = helpers[expected["recipe"]] if "recipe" in expected else expected
            assert all(got[k] == target[k] for k in ("name", "reference product", "location", "unit"))
            assert all(got[k] == expected[k] for k in ("amount", "uncertainty type", "loc") if k in expected)
    assert not any(a["name"].startswith(("Cobalt sulfate", "Pitch despergents")) for a in result.data[1:])
    assert not any(a.get("brightpath_conversion", {}).get("source_input_omissions") for a in result.data)


def test_chemical_branch_preserves_feed_wastewater_and_eight_signed_elementary_flows():
    recipe = REVIEW["recipes"]["shared-39-nickel-sulfate-direct-production"]
    assert len(recipe["inputs"]) == 23
    biosphere = [e for e in recipe["inputs"] if e.get("type") == "biosphere"]
    assert len(biosphere) == 8
    assert next(e for e in biosphere if e["name"] == "Nickel II")["amount"] == 0.00210126705225398
    assert next(e for e in biosphere if e["name"] == "Sulfur")["amount"] == 0.00114777949151113
    resource = load_uvek_technosphere_resource()
    nickel = [
        e
        for e in recipe["inputs"]
        if "recipe" in e and resource["recipes"][e["recipe"]]["reference product"] == "nickel, class 1"
    ]
    assert len(nickel) == 1 and nickel[0]["amount"] == 0.399240739928257
    wastewater = [e for e in recipe["inputs"] if e.get("recipe", "").startswith("pilot-wastewater-routes-")]
    assert len(wastewater) == 4 and all(e["amount"] > 0 for e in wastewater)
    assert not any(e.get("approved_omission_if_unavailable") for e in recipe["inputs"])


def test_coproduct_ratio_is_an_explicit_burden_proxy_with_native_limitations():
    check = REVIEW["cobalt_coproduct_allocation_check"]
    assert check["compared_nonproduction_exchange_identities"] == 290
    assert check["identical_identity_sets"] and check["all_exchanges_match"]
    assert not check["physical_chemical_substitution_claimed"]
    recipe = REVIEW["recipes"]["shared-39-nickel-sulfate-allocated-cobalt"]
    assert len(recipe["inputs"]) == 1
    assert recipe["inputs"][0]["name"] == "Cobalt, at plant"
    assert recipe["inputs"][0]["amount"] == check["constant_inventory_ratio"] == 0.11337491313401228
    assert "overburden/tailings" in recipe["rationale"]
    assert "not physical cobalt metal" in recipe["rationale"]


@pytest.mark.parametrize(
    "version,model", [("3.8", "cutoff"), ("3.10", "cutoff"), ("3.12", "cutoff"), ("3.9", "consequential")]
)
def test_other_source_profiles_receive_no_new_permission(version, model):
    data = consumer(ROWS[0]["source"], 1)
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(data, context=contexts("ecoinvent", version, model)).migrate_background(
            contexts("uvek", "2026").background, policy=policy()
        )


def test_missing_approval_still_refuses_exact_source():
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(
            consumer(ROWS[0]["source"], 1), context=contexts("ecoinvent", "3.9")
        ).migrate_background(contexts("uvek", "2026").background, policy=policy(False))
