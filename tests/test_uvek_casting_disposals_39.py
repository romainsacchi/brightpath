"""Waste portfolios preserve fates; only proven empty kiln receipts are audited."""

import json
from copy import deepcopy

import pytest
from test_uvek_reference_signs import consumer, contexts

from brightpath import DATA_DIR, BrightwayInventory, TechnosphereProfile
from brightpath.background.rule_approvals import compatibility_rule_fingerprint
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource
from brightpath.migrations.scaling import scale_exchange

REVIEW = json.loads((DATA_DIR / "export/uvek_casting_disposals_39_review.json").read_text())
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
    assert original == data and len(result.data) == 8
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
    assert not omissions
    assert not any(h.get("brightpath_conversion", {}).get("zero_burden") for h in result.data)
    for row in ROWS:
        helper = helpers[row["target"]["recipe_ref"]]
        fates = [e for e in helper["exchanges"][1:] if e["unit"] == "kilogram"]
        assert sum(e["amount"] for e in fates) == pytest.approx(1)
        product = row["source"]["reference product"]
        if product == "fly ash and scrubber sludge":
            assert [e["amount"] for e in fates] == [0.5, 0.5]
            assert all(
                e["name"] == "Disposal, separator sludge, 90% water, to hazardous waste incineration" for e in fates
            )
        elif product == "waste emulsion paint, separated":
            assert (
                len(fates) == 1 and fates[0]["name"] == "Disposal, building, emulsion paint remains, to final disposal"
            )
        elif product == "waste paint on metal":
            assert len(fates) == 2
            assert {e["name"] for e in fates} == {
                "Disposal, building, paint on metal, to final disposal",
                "Disposal, building, paint on metal, to sorting plant",
            }
            if row["source"]["location"] == "CH":
                assert next(e["amount"] for e in fates if "sorting plant" in e["name"]) == pytest.approx(
                    0.979999996420023
                )
            else:
                assert next(e["amount"] for e in fates if "sorting plant" in e["name"]) == pytest.approx(
                    0.173209335304642
                )
        else:
            assert product == "hazardous waste, for underground deposit"
            assert len(fates) == 1 and fates[0]["name"] == "Disposal, hazardous waste, 0% water, to underground deposit"


def test_inspected_incineration_label_pairs_do_not_create_recovery_credit():
    assert [c["nonproduction_exchange_count"] for c in REVIEW["identical_energy_recovery_label_checks"]] == [
        116,
        131,
        116,
    ]
    assert all(c["identical_nominal_exchange_vectors"] for c in REVIEW["identical_energy_recovery_label_checks"])
    for model in REVIEW["recipes"].values():
        assert len(model["inputs"]) > 1 and all(e["amount"] > 0 for e in model["inputs"])
        assert not model.get("source_input_omissions") and not model.get("zero_burden")


@pytest.mark.parametrize("version,model", [("3.8", "cutoff"), ("3.10", "cutoff"), ("3.9", "consequential")])
def test_new_portfolios_do_not_authorize_other_source_profiles(version, model):
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(
            consumer(ROWS[0]["source"], -1), context=contexts("ecoinvent", version, model)
        ).migrate_background(contexts("uvek", "2026").background, policy=policy())
