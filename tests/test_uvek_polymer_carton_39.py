"""Polymer supply and converted-carton markets preserve product bases and prior approved profiles."""

import json
from copy import deepcopy

import pytest
from test_uvek_reference_signs import consumer, contexts

from brightpath import DATA_DIR, BrightwayInventory, TechnosphereProfile
from brightpath.background.rule_approvals import compatibility_rule_fingerprint
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource

REVIEW = json.loads((DATA_DIR / "export/uvek_polymer_carton_39_review.json").read_text())
ROWS = REVIEW["records"]


def policy(row, approved=True):
    resource = load_uvek_technosphere_resource()
    rule = next(r for r in resource["replace"] if r["id"] == row["rule_id"])
    fingerprint = compatibility_rule_fingerprint(
        resource,
        "replace",
        rule,
        TechnosphereProfile("ecoinvent", "3.9", "cutoff"),
        TechnosphereProfile("uvek", "2025", "cutoff"),
    )
    return MigrationPolicy(
        approved_compatibility_rules=(fingerprint,) if approved else (),
        on_information_loss=PolicyAction.WARN,
    )


@pytest.mark.parametrize("row", ROWS, ids=lambda r: r["rule_id"])
@pytest.mark.parametrize("amount", [-2.5, 2.5])
@pytest.mark.parametrize("uncertainty", [2, 3])
@pytest.mark.parametrize("release", ["2025", "2026"])
def test_complete_material_or_freight_preserves_caller_and_uncertainty(row, amount, uncertainty, release):
    data = consumer(row["original_rule"]["source"], amount, uncertainty)
    emission = {
        "name": "Carbon dioxide, fossil",
        "categories": ["air"],
        "unit": "kilogram",
        "type": "biosphere",
        "amount": 0.5,
    }
    data[0]["exchanges"].append(emission)
    before = deepcopy(data)
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.9")).migrate_background(
        contexts("uvek", release).background, policy=policy(row)
    )
    assert data == before and len(result.data) == (2 if row["recipe_id"] else 1)
    assert len(result.data[0]["exchanges"]) == 3
    actual = result.data[0]["exchanges"][1]
    for field in ("amount", "uncertainty type", "loc", "scale", "negative", "formula", "minimum", "maximum"):
        if field in before[0]["exchanges"][1]:
            assert actual[field] == before[0]["exchanges"][1][field]
    if row["recipe_id"]:
        helper = result.data[1]
        assert helper["brightpath_conversion"]["recipe_id"] == row["recipe_id"]
        inputs = [e for e in helper["exchanges"] if e["type"] != "production"]
        expected = REVIEW["recipes"][row["recipe_id"]]["inputs"]
        assert len(inputs) == len(expected)
        for edge, reference in zip(inputs, expected, strict=True):
            assert all(edge[k] == v for k, v in reference.items())
        assert not helper["brightpath_conversion"].get("source_input_omissions")
    else:
        assert all(actual[k] == v for k, v in row["compiled_rule"]["target"].items())
    assert result.data[0]["exchanges"][-1] == emission


@pytest.mark.parametrize("row", ROWS, ids=lambda r: r["rule_id"])
def test_material_or_freight_needs_its_own_exact_approval(row):
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(
            consumer(row["original_rule"]["source"], 1), context=contexts("ecoinvent", "3.9")
        ).migrate_background(contexts("uvek", "2026").background, policy=policy(row, False))


@pytest.mark.parametrize("row", ROWS, ids=lambda r: r["rule_id"])
@pytest.mark.parametrize(
    "version,model", [("3.8", "cutoff"), ("3.9.1", "cutoff"), ("3.10", "cutoff"), ("3.9", "consequential")]
)
def test_new_scope_does_not_authorize_other_profiles(row, version, model):
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(
            consumer(row["original_rule"]["source"], 1), context=contexts("ecoinvent", version, model)
        ).migrate_background(contexts("uvek", "2026").background, policy=policy(row))


@pytest.mark.parametrize("row", ROWS, ids=lambda r: r["rule_id"])
def test_delivery_modes_and_product_normalization_are_not_silently_changed(row):
    assert row["source_witness"]["reference_amounts"] == [1]
    assert all(w["reference_amounts"] == [1] for w in row["native_witnesses"].values())
    assert row["compiled_rule"]["target"] == row["original_rule"]["target"]
    if not row["recipe_id"]:
        assert row["compiled_rule"] == row["original_rule"]
        assert row["boundary_checks"]["actual_source_and_native_raw_material_is_HDPE"]
        return
    inputs = REVIEW["recipes"][row["recipe_id"]]["inputs"]
    carton = row["boundary_checks"]["kind"] == "converted_virgin_fibre_carton"
    bound_inputs = inputs[:1] + inputs[2:] if carton else inputs
    assert len(bound_inputs) == len(row["retained_source_market_bindings"])
    for e, b in zip(bound_inputs, row["retained_source_market_bindings"], strict=True):
        assert e["amount"] == b["target_amount"]
        assert all(e[k] == value for k, value in b["target"].items())
        if not carton or b["role"] != "native_SUB_board_with_documented_module_basis":
            assert e["amount"] == b["source_amount"]
    freight = [e["amount"] for e in inputs if e["unit"] == "ton kilometer"]
    expected = {
        "bp-e1c5c137f358792d": [0.2887, 0.4504, 0.5248],
        "bp-46961b2d2543df96": [0.2887, 0.4504, 0.5248],
        "bp-4cb0f787f01d05a7": [0.2887, 0.4504, 0.5248],
        "bp-1a39c7b4c1f58b77": [0.2887, 0.4504, 0.5248],
        "bp-4df468ffd8629c6d": [0.2887, 0.4504, 0.5248],
        "bp-1b65914b2f33ba7b": [0.2217, 0.0104, 0.3354, 0.334],
        "bp-14552642b34ae381": [0.0511, 0.0246, 0.3857, 1.1439],
        "bp-dc31a9f499822217": [0.0251409031418564, 0.00183079518546258, 0.186618551553528],
    }
    assert freight == expected[row["rule_id"]]
    if carton:
        assert len(inputs) == 5
        assert inputs[0]["name"] == "Solid unbleached board, SUB, at plant"
        assert inputs[0]["amount"] == 1 / 0.83
        assert inputs[1]["name"] == "xx Production of carton board boxes, offset printing, at plant"
        assert inputs[1]["amount"] == 1
        assert row["boundary_checks"]["bleached_unbleached_source_shares_known"] is False
        assert row["boundary_checks"]["new_17percent_waste_exchange"] is False
    else:
        assert sum(e["amount"] for e in inputs if e["unit"] == "kilogram") == pytest.approx(1)


def physical_binding(resource, rid, profile):
    from brightpath.background.reference_signs import select_profile_sign_rules

    selected = select_profile_sign_rules(resource, TechnosphereProfile(**profile))
    rule = next(r for r in selected["replace"] if r["id"] == rid)
    pending = [rule["target"]["recipe_ref"]] if "recipe_ref" in rule["target"] else []
    recipes = {}
    while pending:
        key = pending.pop()
        if key in recipes:
            continue
        recipes[key] = resource["recipes"][key]
        pending.extend(e["recipe"] for e in recipes[key]["inputs"] if "recipe" in e)
    return {
        "source": rule["source"],
        "target": rule["target"],
        "conversion_factor": rule.get("conversion_factor", 1),
        "recipes": recipes,
    }


@pytest.mark.parametrize(
    "prior", REVIEW["preserved_existing_scopes"], ids=lambda p: p["rule_id"] + p["source_profile"]["version"]
)
def test_existing_approved_profiles_keep_the_same_full_physical_binding(prior):
    resource = load_uvek_technosphere_resource()
    assert physical_binding(resource, prior["rule_id"], prior["source_profile"]) == prior["unchanged_physical_binding"]
    row = next(r for r in ROWS if r["rule_id"] == prior["rule_id"])
    baseline = deepcopy(resource)
    baseline["reviews"].update(deepcopy(REVIEW["previous_shared_reviews"]))
    parent = next(r for r in baseline["replace"] if r["id"] == row["rule_id"])
    parent.clear()
    parent.update(deepcopy(row["original_rule"]))
    assert physical_binding(baseline, prior["rule_id"], prior["source_profile"]) == prior["unchanged_physical_binding"]
    source = TechnosphereProfile(**prior["source_profile"])
    target = TechnosphereProfile(**prior["target_profile"])
    rule = next(r for r in resource["replace"] if r["id"] == prior["rule_id"])
    assert compatibility_rule_fingerprint(baseline, "replace", parent, source, target) == prior["previous_fingerprint"]
    assert compatibility_rule_fingerprint(resource, "replace", rule, source, target) == prior["rebound_fingerprint"]
    policy = MigrationPolicy(
        approved_compatibility_rules=(prior["rebound_fingerprint"],), on_information_loss=PolicyAction.WARN
    )
    before = consumer(rule["source"], -2.5, 3)
    result = BrightwayInventory.from_data(before, context=contexts("ecoinvent", source.version)).migrate_background(
        contexts("uvek", "2026").background, policy=policy
    )
    assert result.data[0]["exchanges"][1]["amount"] == -2.5
    expected = prior["unchanged_physical_binding"]["target"]
    if "recipe_ref" in expected:
        assert result.data[1]["brightpath_conversion"]["recipe_id"] == expected["recipe_ref"]
    else:
        assert len(result.data) == 1
        assert all(result.data[0]["exchanges"][1][k] == v for k, v in expected.items())


@pytest.mark.parametrize(
    "drift", [None, "first_parent", "last_parent", "stock", "freight", "old_recipe", "freight_rule"]
)
def test_grouped_replay_builds_from_originals_atomically_and_is_idempotent(drift):
    from scripts.archive_uvek_unreferenced_reviews import archive_unreferenced_reviews
    from scripts.review_uvek_polymer_carton_39 import apply_polymer_carton_39_review

    expected = deepcopy(load_uvek_technosphere_resource())
    resource = deepcopy(expected)
    for row in ROWS:
        parent = next(r for r in resource["replace"] if r["id"] == row["rule_id"])
        parent.clear()
        parent.update(deepcopy(row["original_rule"]))
    for key in REVIEW["recipes"]:
        resource["recipes"].pop(key)
    resource["reviews"].update(deepcopy(REVIEW["previous_shared_reviews"]))
    if drift in ("first_parent", "last_parent"):
        rid = ROWS[0 if drift == "first_parent" else -1]["rule_id"]
        next(r for r in resource["replace"] if r["id"] == rid)["confidence"] = 0.01
    elif drift in ("stock", "freight"):
        key = ROWS[0]["recipe_id"]
        resource["recipes"][key] = deepcopy(REVIEW["recipes"][key])
        resource["recipes"][key]["inputs"][-1 if drift == "freight" else 0]["amount"] *= 2
    elif drift == "old_recipe":
        key = next(iter(REVIEW["recipe_dependencies"]))
        resource["recipes"][key]["inputs"][0]["amount"] *= 2
    elif drift == "freight_rule":
        rid = REVIEW["source_rule_dependency_scopes"][0]["rule_id"]
        next(r for r in resource["replace"] if r["id"] == rid)["confidence"] = 0.01
    before = deepcopy(resource)
    if drift:
        with pytest.raises(ValueError):
            apply_polymer_carton_39_review(resource)
        assert resource == before
    else:
        apply_polymer_carton_39_review(resource)
        archive_unreferenced_reviews(resource)
        assert resource == expected
        apply_polymer_carton_39_review(resource)
        archive_unreferenced_reviews(resource)
        assert resource == expected
