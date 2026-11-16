"""The used AlLi recipe preserves source entries and correct supply boundaries."""

import json
from copy import deepcopy
from dataclasses import replace

import pytest
from test_uvek_reference_signs import consumer, contexts

from brightpath import DATA_DIR, BrightwayInventory, TechnosphereProfile
from brightpath.background.reference_signs import select_profile_sign_rules
from brightpath.background.rule_approvals import compatibility_rule_fingerprint
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource
from brightpath.migrations.scaling import scale_exchange
from scripts.review_uvek_passenger_car_37_corrections import apply_passenger_car_37_corrections_review

REVIEW = json.loads((DATA_DIR / "export/uvek_passenger_car_37_corrections_review.json").read_text())


def policy():
    resource = load_uvek_technosphere_resource()
    indexed = {r["id"]: r for r in resource["replace"]}
    return MigrationPolicy(
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


@pytest.mark.parametrize("amount,uncertainty", [(-2, 2), (2, 3)])
@pytest.mark.parametrize("release", ["2025", "2026"])
def test_car_corrections_keep_alloy_inputs_signed_demands_and_complete_fuels(amount, uncertainty, release):
    rows = REVIEW["records"]
    data = consumer(rows[0]["source"], amount, uncertainty)
    data[0]["exchanges"].extend(consumer(r["source"], amount, uncertainty)[0]["exchanges"][1] for r in rows[1:])
    before = deepcopy(data)
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.7")).migrate_background(
        contexts("uvek", release).background, policy=policy()
    )
    assert data == before
    for original, edge in zip(before[0]["exchanges"][1:], result.data[0]["exchanges"][1:], strict=True):
        for key in ("amount", "loc", "scale", "negative", "uncertainty type", "formula"):
            if key in original:
                assert edge[key] == original[key]
    assert result.data[0]["exchanges"][2]["name"] == "Petrol, fossil, at regional storage"
    assert "medium pressure network" in result.data[0]["exchanges"][3]["name"]
    alloy = next(r for r in rows if r["rule_id"] == "bp-c4e4184b24112209")
    helpers = {a["brightpath_conversion"]["recipe_id"]: a for a in result.data[1:]}
    fields = ("name", "reference product", "location", "unit")
    by_identity = {tuple(a[k] for k in fields): a for a in result.data[1:]}
    formula_count = 0
    for recipe_id, recipe in REVIEW["recipes"].items():
        helper = helpers[recipe_id]
        assert helper["reference product"] == "aluminium alloy, AlLi"
        assert helper["exchanges"][0]["amount"] == 1
        bindings = [b for b in alloy["bindings"] if b["recipe_id"] == recipe_id]
        purchases = helper["exchanges"][1:]
        assert len(purchases) == len(bindings) == len(recipe["inputs"])
        for edge, binding in zip(purchases, bindings, strict=True):
            expected = {"amount": binding["source_amount"], **binding["source_marginal_uncertainty"]}
            scale_exchange(expected, binding["conversion_factor"])
            for key, value in expected.items():
                assert edge[key] == value
            if binding["source_formula"]:
                formula_count += 1
                assert edge["source formula"] == binding["source_formula"] and "formula" not in edge
            assert "AlMg3" not in edge.get("name", "")
            if binding["source"].get("name") == "market for titanium, primary":
                supplier = by_identity[tuple(edge[k] for k in fields)]
                assert supplier["reference product"] == "titanium"
                assert supplier["brightpath_conversion"]["recipe_id"] == binding["target"]["recipe"]
            if binding["source"].get("name") == "market for potassium chloride":
                assert binding["conversion_factor"] == 0.6
            if binding["source"].get("name") == "market for sodium hydroxide, without water, in 50% solution state":
                assert binding["conversion_factor"] == 1
    assert formula_count == alloy["source_formula_texts_retained"] == 10
    market = helpers["car-37-alli-market"]["exchanges"][1:]
    assert len(market) == 6
    assert sum(e["amount"] for e in market if e["unit"] == "kilogram") == pytest.approx(1)
    assert [e["amount"] for e in market if e["unit"] == "ton kilometer"] == [0.0363, 0.3614, 0.3633, 0.3448]
    for helper in result.data[1:]:
        if helper["brightpath_conversion"].get("zero_burden"):
            assert helper["brightpath_conversion"]["inventory_source_profile"] == REVIEW["inventory_model_profile"]
            assert helper["brightpath_conversion"]["caller_source_profile"] == REVIEW["source_profile"]
    inherited = [
        loss.to_dict()["details"]
        for loss in result.last_migration_report.losses
        if loss.code == "migration.recipe_source_input_omission"
    ]
    assert inherited
    for loss in inherited:
        assert any(
            context["source_profile"] == loss["source_profile"]
            and context["approved_recipe_source_omissions"].get(loss["recipe_id"], [])[loss["omission_index"]]
            == loss["omission"]
            and loss["basis"] == {"amount": 1, "unit": context["source"]["unit"]}
            for context in alloy["nested_recipe_inspections"]
            if loss["recipe_id"] in context["approved_recipe_source_omissions"]
        )


def original_resource():
    resource = load_uvek_technosphere_resource()
    return {
        "replace": [deepcopy(r["original_rule"]) for r in REVIEW["records"]],
        "delete": [],
        "recipes": {k: deepcopy(resource["recipes"][k]) for k in REVIEW["recipe_dependency_sha256"]},
        "reviews": deepcopy(resource["reviews"]),
        "coverage": {"unresolved_identities": 0},
    }


def test_correction_replay_is_idempotent_and_preserves_older_cng_profiles():
    resource = original_resource()
    apply_passenger_car_37_corrections_review(resource)
    before = deepcopy(resource)
    apply_passenger_car_37_corrections_review(resource)
    assert resource == before
    row = next(r for r in REVIEW["records"] if r["rule_id"] == "bp-745c517f85a78ba7")
    for version in ("3.8", "3.9"):
        source = TechnosphereProfile("ecoinvent", version, "cutoff")
        assert select_profile_sign_rules({"replace": [row["original_rule"]]}, source) == select_profile_sign_rules(
            {"replace": [row["compiled_rule"]]}, source
        )


@pytest.mark.parametrize("change", ["parent", "dependency", "recipe"])
def test_changed_correction_inputs_are_rejected_atomically(change):
    resource = original_resource()
    if change == "parent":
        resource["replace"][0]["conversion_factor"] = 2
    elif change == "dependency":
        resource["recipes"][next(iter(resource["recipes"]))]["reference amount"] = 2
    else:
        key, recipe = next(iter(REVIEW["recipes"].items()))
        resource["recipes"][key] = {**recipe, "inputs": []}
    before = deepcopy(resource)
    with pytest.raises(ValueError, match="changed"):
        apply_passenger_car_37_corrections_review(resource)
    assert resource == before


def test_alloy_model_does_not_silently_pass_a_strict_information_loss_policy():
    row = next(r for r in REVIEW["records"] if r["rule_id"] == "bp-c4e4184b24112209")
    with pytest.raises(MigrationError) as error:
        BrightwayInventory.from_data(
            consumer(row["source"], 1), context=contexts("ecoinvent", "3.7")
        ).migrate_background(
            contexts("uvek", "2026").background,
            policy=replace(policy(), on_information_loss=PolicyAction.ERROR),
        )
    assert any(issue.code == "migration.technosphere_information_loss" for issue in error.value.report.issues)
