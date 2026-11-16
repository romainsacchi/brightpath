"""Current car dependencies preserve service signs, stock allocation and plastic routes."""

import json
from copy import deepcopy

import pytest
from test_uvek_reference_signs import consumer, contexts

from brightpath import DATA_DIR, BrightwayInventory, TechnosphereProfile
from brightpath.background.reference_signs import select_profile_sign_rules
from brightpath.background.rule_approvals import compatibility_rule_fingerprint
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.migrations.resources import load_uvek_technosphere_resource
from brightpath.migrations.scaling import scale_exchange
from scripts.review_uvek_passenger_car_310 import apply_passenger_car_310_review

REVIEW = json.loads((DATA_DIR / "export/uvek_passenger_car_310_review.json").read_text())


@pytest.mark.parametrize("amount,uncertainty", [(-2, 2), (2, 3)])
@pytest.mark.parametrize("release", ["2025", "2026"])
def test_current_car_supplies_keep_signed_reference_quantities_and_complete_routes(amount, uncertainty, release):
    resource = load_uvek_technosphere_resource()
    indexed = {a["id"]: a for a in resource["replace"]}
    policy = MigrationPolicy(
        approved_compatibility_rules=tuple(
            compatibility_rule_fingerprint(
                resource,
                "replace",
                indexed[row["rule_id"]],
                TechnosphereProfile(**REVIEW["source_profile"]),
                TechnosphereProfile(**resource["target_profile"]),
            )
            for row in REVIEW["records"]
        ),
        on_information_loss=PolicyAction.WARN,
    )
    rows = REVIEW["records"]
    data = consumer(rows[0]["source"], amount, uncertainty)
    data[0]["exchanges"].extend(consumer(r["source"], amount, uncertainty)[0]["exchanges"][1] for r in rows[1:])
    before = deepcopy(data)
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.10")).migrate_background(
        contexts("uvek", release).background, policy=policy
    )
    assert data == before
    helpers = result.data[1:]
    for row, original, edge in zip(rows, before[0]["exchanges"][1:], result.data[0]["exchanges"][1:], strict=True):
        expected = deepcopy(original)
        scale_exchange(expected, row["conversion_factor"])
        for key in ("amount", "loc", "scale", "negative", "uncertainty type", "formula"):
            if key in expected:
                assert edge[key] == expected[key]
        if "native_witnesses" in row:
            assert all(edge[k] == v for k, v in row["target"].items())
        elif "existing_allocated_stock_recipe" in row:
            stock = next(
                h for h in helpers if h["brightpath_conversion"]["recipe_id"] == row["existing_allocated_stock_recipe"]
            )
            assert stock["brightpath_conversion"]["zero_burden"]
            assert len(stock["exchanges"]) == 1 and stock["exchanges"][0]["type"] == "production"
            assert row["source_witness"]["nonproduction_count"] == 0
        elif "inventory_model_profile" in row:
            models = [h for h in helpers if h["brightpath_conversion"]["recipe_id"] in row["model_recipe_closure_ids"]]
            assert models
            for model in models:
                assert model["brightpath_conversion"]["inventory_source_profile"] == row["inventory_model_profile"]
                assert model["brightpath_conversion"]["caller_source_profile"] == row["source_profile"]
    plastic = next(row for row in rows if row["rule_id"] == "bp-7b57882e271d26d2")
    market = next(h for h in helpers if h["brightpath_conversion"]["recipe_id"] == plastic["recipe_id"])
    purchases = [e for e in market["exchanges"] if e["type"] == "technosphere"]
    assert len(purchases) == len(plastic["bindings"]) == 5
    treatments = []
    for purchase, binding in zip(purchases, plastic["bindings"], strict=True):
        assert purchase["amount"] == binding["target_amount"]
        if binding.get("treatment_sign_conversion"):
            assert purchase["amount"] == -binding["source_amount"]
            treatments.append(purchase)
        else:
            assert purchase["amount"] == binding["source_amount"] and purchase["unit"] == "ton kilometer"
    assert sum(e["amount"] for e in treatments) == pytest.approx(1)
    assert len(plastic["nested_model_inspections"]) == 2
    assert not any(
        loss.code == "migration.recipe_source_input_omission" for loss in result.last_migration_report.losses
    )


def original_resource():
    resource = load_uvek_technosphere_resource()
    return {
        "replace": [deepcopy(row["original_rule"]) for row in REVIEW["records"]],
        "delete": [],
        "recipes": {key: deepcopy(resource["recipes"][key]) for key in REVIEW["recipe_dependency_sha256"]},
        "reviews": deepcopy(resource["reviews"]),
        "coverage": {"unresolved_identities": 0},
    }


def test_replay_preserves_old_profile_rules_and_is_idempotent():
    resource = original_resource()
    apply_passenger_car_310_review(resource)
    once = deepcopy(resource)
    apply_passenger_car_310_review(resource)
    assert resource == once
    for row in REVIEW["records"]:
        if row["rule_id"] == "bp-7b57882e271d26d2":
            continue  # No prior scope approved the replaced landfill candidate.
        for version in ("3.7", "3.8", "3.9", "3.12"):
            source = TechnosphereProfile("ecoinvent", version, "cutoff")
            assert select_profile_sign_rules({"replace": [row["original_rule"]]}, source) == select_profile_sign_rules(
                {"replace": [row["compiled_rule"]]}, source
            )


@pytest.mark.parametrize("change", ["parent", "dependency", "recipe"])
def test_changed_inputs_are_rejected_before_mutation(change):
    resource = original_resource()
    if change == "parent":
        resource["replace"][0]["conversion_factor"] = 2
    elif change == "dependency":
        key = next(iter(resource["recipes"]))
        resource["recipes"][key]["reference_amount"] = 2
    else:
        key, recipe = next(iter(REVIEW["recipes"].items()))
        resource["recipes"][key] = {**recipe, "inputs": []}
    before = deepcopy(resource)
    with pytest.raises(ValueError, match="changed"):
        apply_passenger_car_310_review(resource)
    assert resource == before
