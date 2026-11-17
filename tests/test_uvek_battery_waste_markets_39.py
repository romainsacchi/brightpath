"""Waste portfolios preserve fates; only proven empty kiln receipts are audited."""

import json
from copy import deepcopy

import pytest
from test_uvek_reference_signs import consumer, contexts

from brightpath import DATA_DIR, BrightwayInventory, TechnosphereProfile
from brightpath.background.reference_signs import select_profile_sign_rules
from brightpath.background.rule_approvals import compatibility_rule_fingerprint
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource
from brightpath.migrations.scaling import scale_exchange

REVIEW = json.loads((DATA_DIR / "export/uvek_battery_waste_markets_39_review.json").read_text())
ROWS = REVIEW["records"]


def policy():
    r = load_uvek_technosphere_resource()
    rules = {x["id"]: x for x in r["replace"]}
    scopes = tuple(
        compatibility_rule_fingerprint(
            r,
            "replace",
            rules[row["rule_id"]],
            TechnosphereProfile(**REVIEW["source_profile"]),
            TechnosphereProfile(**REVIEW["target_profile"]),
        )
        for row in ROWS
    )
    return MigrationPolicy(approved_compatibility_rules=scopes, on_information_loss=PolicyAction.WARN)


@pytest.mark.parametrize("amount", [-2.5, 2.5])
@pytest.mark.parametrize("uncertainty", [2, 3])
@pytest.mark.parametrize("release", ["2025", "2026"])
def test_negative_waste_reference_flips_once_and_retains_portfolio_statistics(amount, uncertainty, release):
    data = consumer(ROWS[0]["source"], amount, uncertainty)
    data[0]["exchanges"].extend(consumer(row["source"], amount, uncertainty)[0]["exchanges"][1] for row in ROWS[1:])
    original = deepcopy(data)
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.9")).migrate_background(
        contexts("uvek", release).background, policy=policy()
    )
    assert original == data and len(result.data) == 5
    helpers = {a["brightpath_conversion"]["recipe_id"]: a for a in result.data[1:]}
    for edge, old, row in zip(result.data[0]["exchanges"][1:], original[0]["exchanges"][1:], ROWS, strict=True):
        expected = deepcopy(old)
        scale_exchange(expected, -1)
        assert all(
            edge[k] == expected[k]
            for k in ("amount", "loc", "scale", "minimum", "maximum", "negative", "formula")
            if k in old
        )
        assert edge["amount"] == -amount
        helper = helpers[row["target"]["recipe_ref"]]
        assert edge["name"] == helper["name"] and helper["exchanges"][0]["amount"] == 1
        for got, raw in zip(
            helper["exchanges"][1:], REVIEW["recipes"][row["target"]["recipe_ref"]]["inputs"], strict=True
        ):
            assert all(got[k] == v for k, v in raw.items())
        assert all(e["amount"] > 0 for e in helper["exchanges"][1:])
    omissions = [
        a for h in result.data[1:] for a in h.get("brightpath_conversion", {}).get("source_input_omissions", [])
    ]
    assert len(omissions) == 3 and all(a["source_exchange"]["name"] == "clinker production" for a in omissions)
    assert not any(h.get("brightpath_conversion", {}).get("zero_burden") for h in result.data)


def test_only_empty_terminals_are_omitted_and_retained_disposal_shares_are_not_renormalized():
    assert REVIEW["nonempty_operations_omitted"] == REVIEW["zero_burden_helpers_created"] == 0
    for row in ROWS:
        recipe = REVIEW["recipes"][row["target"]["recipe_ref"]]
        fate = sum(e["amount"] for e in recipe["inputs"] if e["unit"] == "kilogram")
        omitted = -sum(e["source_exchange"]["amount"] for e in recipe.get("source_input_omissions", []))
        assert fate + omitted == pytest.approx(1)
        assert fate < 1 if omitted else fate == pytest.approx(1)
        assert len(recipe["inputs"]) + len(recipe.get("source_input_omissions", [])) == len(
            row["source_component_checksums"]
        )
        for audit in recipe.get("source_input_omissions", []):
            assert audit["source_activity"] == row["source"] and audit["source_profile"] == REVIEW["source_profile"]
            witnesses = [
                w
                for w in REVIEW["source_treatment_witnesses"]
                if all(
                    w["identity"][k] == audit["source_exchange"][k]
                    for k in ("name", "reference product", "location", "unit")
                )
            ]
            assert len(witnesses) == 1 and witnesses[0]["nonproduction_count"] == 0
    assert [c["nonproduction_exchange_count"] for c in REVIEW["identical_energy_recovery_label_checks"]] == [69, 69, 53]


def test_prior_requested_37_waste_oil_rule_and_recipe_remain_byte_equal():
    r = load_uvek_technosphere_resource()
    old = json.loads((DATA_DIR / "export/uvek_fuels_37_waste_oil_review.json").read_text())
    selected = next(
        x
        for x in select_profile_sign_rules(r, TechnosphereProfile(**old["source_profile"]))["replace"]
        if x["id"] == old["rule_id"]
    )
    prior = select_profile_sign_rules(
        {**r, "replace": [old["compiled_rule"]]}, TechnosphereProfile(**old["source_profile"])
    )["replace"][0]
    assert selected == prior
    assert all(r["recipes"][k] == v for k, v in old["recipes"].items())


@pytest.mark.parametrize("version,model", [("3.8", "cutoff"), ("3.10", "cutoff"), ("3.9", "consequential")])
def test_new_portfolios_do_not_authorize_other_source_profiles(version, model):
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(
            consumer(ROWS[0]["source"], -1), context=contexts("ecoinvent", version, model)
        ).migrate_background(contexts("uvek", "2026").background, policy=policy())
