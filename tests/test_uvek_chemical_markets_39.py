"""Named chemical bases retain complete native products, source freight and caller uncertainty."""

import json
from copy import deepcopy

import pytest
from test_uvek_reference_signs import consumer, contexts

from brightpath import DATA_DIR, BrightwayInventory, TechnosphereProfile
from brightpath.background.rule_approvals import compatibility_rule_fingerprint
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource

REVIEW = json.loads((DATA_DIR / "export/uvek_chemical_markets_39_review.json").read_text())
ROWS = REVIEW["records"]


def policy(rows=ROWS):
    r = load_uvek_technosphere_resource()
    ids = {row["rule_id"] for row in rows}
    fingerprints = tuple(
        compatibility_rule_fingerprint(
            r,
            "replace",
            rule,
            TechnosphereProfile("ecoinvent", "3.9", "cutoff"),
            TechnosphereProfile("uvek", "2025", "cutoff"),
        )
        for rule in r["replace"]
        if rule["id"] in ids
    )
    return MigrationPolicy(approved_compatibility_rules=fingerprints, on_information_loss=PolicyAction.WARN)


def cohort(amount, uncertainty):
    data = consumer(ROWS[0]["original_rule"]["source"], amount, uncertainty)
    for row in ROWS[1:]:
        data[0]["exchanges"].append(consumer(row["original_rule"]["source"], amount, uncertainty)[0]["exchanges"][1])
    for i, edge in enumerate(data[0]["exchanges"][1:]):
        edge["formula"] = f"chemical_demand_{i}"
    data[0]["exchanges"].append(
        {
            "name": "Carbon dioxide, fossil",
            "categories": ["air"],
            "unit": "kilogram",
            "type": "biosphere",
            "amount": 0.5,
        }
    )
    return data


@pytest.mark.parametrize("amount", [-2.5, 2.5])
@pytest.mark.parametrize("uncertainty", [2, 3])
@pytest.mark.parametrize("release", ["2025", "2026"])
def test_all_nine_bases_preserve_signed_uncertainty_separate_emissions_and_delivery(amount, uncertainty, release):
    data = cohort(amount, uncertainty)
    before = deepcopy(data)
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.9")).migrate_background(
        contexts("uvek", release).background, policy=policy()
    )
    assert data == before and len(result.data) == 7
    assert len(result.data[0]["exchanges"]) == 11
    assert result.data[0]["exchanges"][-1] == before[0]["exchanges"][-1]
    helpers = {a["brightpath_conversion"]["recipe_id"]: a for a in result.data[1:]}
    for i, row in enumerate(ROWS, 1):
        edge = result.data[0]["exchanges"][i]
        for field in ("amount", "uncertainty type", "loc", "scale", "negative", "formula", "minimum", "maximum"):
            if field in before[0]["exchanges"][i]:
                assert edge[field] == before[0]["exchanges"][i][field]
        target = helpers[row["recipe_id"]] if row["recipe_id"] else row["compiled_rule"]["target"]
        assert all(edge[k] == target[k] for k in ["name", "reference product", "location", "unit"])
        if row["recipe_id"]:
            actual = [e for e in target["exchanges"] if e["type"] != "production"]
            expected = REVIEW["recipes"][row["recipe_id"]]["inputs"]
            assert len(actual) == len(expected)
            assert all(
                all(e[k] == v for k, v in reference.items()) for e, reference in zip(actual, expected, strict=True)
            )
            assert not target["brightpath_conversion"].get("source_input_omissions")


@pytest.mark.parametrize("row", ROWS, ids=lambda r: r["rule_id"])
def test_each_chemical_needs_its_own_scoped_approval(row):
    before = cohort(-2.5, 3)
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(before, context=contexts("ecoinvent", "3.9")).migrate_background(
            contexts("uvek", "2026").background, policy=policy([r for r in ROWS if r != row])
        )
    assert before == cohort(-2.5, 3)


@pytest.mark.parametrize(
    "version,model", [("3.8", "cutoff"), ("3.9.1", "cutoff"), ("3.10", "cutoff"), ("3.9", "consequential")]
)
def test_profile_grants_do_not_become_a_version_or_system_model_fallback(version, model):
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(cohort(1, 3), context=contexts("ecoinvent", version, model)).migrate_background(
            contexts("uvek", "2026").background, policy=policy()
        )


def test_product_and_market_bases_are_independently_bounded():
    expected_freight = {
        "bp-3d257e55be3541c6": [0.3091, 0.0246, 0.2088, 0.599],
        "bp-365d5afb2cddee67": [0.3091, 0.0246, 0.2088, 0.599],
        "bp-15cba0ab8cf2223b": [0.0438, 0.0219, 0.1713],
        "bp-b34986b27d859725": [0.3091, 0.0246, 0.2088, 0.599],
        "bp-92c2120012f2e193": [0.0438, 0.0219, 0.1713],
        "bp-3d4f6646db9a7399": [
            0.0224231473350459,
            0.08093386943083909,
            0.0056780093482361425,
            0.11165767412994829,
            0.08840729975593056,
            0.0246,
            0.0037411136550834123,
            0.18116364249000133,
            0.02389524385491526,
            0.599,
        ],
    }
    resource = load_uvek_technosphere_resource()
    for row in ROWS:
        assert row["source_profile"] == {"family": "ecoinvent", "version": "3.9", "system_model": "cutoff"}
        assert row["source_witness"]["reference_amounts"] == [1]
        assert all(w["reference_amounts"] == [1] for w in row["native_witnesses"].values())
        assert row["original_rule"]["target"] == row["compiled_rule"]["target"]
        assert next(r for r in resource["replace"] if r["id"] == row["rule_id"]) == row["compiled_rule"]
        if not row["recipe_id"]:
            assert row["compiled_rule"] == row["original_rule"]
            continue
        inputs = REVIEW["recipes"][row["recipe_id"]]["inputs"]
        bindings = row["retained_source_market_bindings"]
        assert len(inputs) == len(bindings)
        assert [e["amount"] for e in inputs] == [b["source_amount"] for b in bindings]
        assert sum(e["amount"] for e in inputs if e["unit"] == "kilogram") == pytest.approx(1)
        assert [e["amount"] for e in inputs if e["unit"] == "ton kilometer"] == expected_freight[row["rule_id"]]
        for e, b in zip(inputs, bindings, strict=True):
            assert all(e[k] == v for k, v in b["target"].items())
            if b["source_supplier"]["name"] == "oxidation of butane":
                assert e["name"] == "xx Acetone from butane, at plant"
        assert (
            row["compiled_rule"]["reference_flow_sign_review"]["profiles"][0]["source_profile"] == row["source_profile"]
        )
    facts = {r["rule_id"]: r["boundary_checks"] for r in ROWS}
    assert facts["bp-31d296d99ac55524"]["source_and_native_PVF_input_kg"] == 1.0526
    assert facts["bp-31d296d99ac55524"]["wet_or_dry_solids_fraction_independently_established"] is False
    assert facts["bp-c86dc86fdb185fd5"]["CAS"] == "110-71-4"
    assert facts["bp-365d5afb2cddee67"]["new_conversion_yield_or_carbon_removal_credit"] is False
    assert facts["bp-3d257e55be3541c6"]["gross_propylene_description_used_as_reference"] is False
    assert REVIEW["new_helpers"] == 6 and REVIEW["new_source_omissions"] == []


@pytest.mark.parametrize("drift", [None, "first_parent", "last_parent", "stock", "freight", "butane"])
def test_replay_from_frozen_inputs_is_atomic_and_idempotent(drift):
    from scripts.review_uvek_chemical_markets_39 import apply_chemical_markets_39_review

    expected = deepcopy(load_uvek_technosphere_resource())
    resource = deepcopy(expected)
    for row in ROWS:
        parent = next(r for r in resource["replace"] if r["id"] == row["rule_id"])
        parent.clear()
        parent.update(deepcopy(row["original_rule"]))
    for key in REVIEW["recipes"]:
        resource["recipes"].pop(key)
    if drift in ("first_parent", "last_parent"):
        rid = ROWS[0 if drift == "first_parent" else -1]["rule_id"]
        next(r for r in resource["replace"] if r["id"] == rid)["confidence"] = 0.01
    elif drift:
        row = next(r for r in ROWS if r["rule_id"] == "bp-92c2120012f2e193") if drift == "butane" else ROWS[0]
        resource["recipes"][row["recipe_id"]] = deepcopy(REVIEW["recipes"][row["recipe_id"]])
        idx = 1 if drift == "butane" else -1 if drift == "freight" else 0
        resource["recipes"][row["recipe_id"]]["inputs"][idx]["amount"] *= 2
    before = deepcopy(resource)
    if drift:
        with pytest.raises(ValueError):
            apply_chemical_markets_39_review(resource)
        assert resource == before
    else:
        apply_chemical_markets_39_review(resource)
        assert resource == expected
        apply_chemical_markets_39_review(resource)
        assert resource == expected
