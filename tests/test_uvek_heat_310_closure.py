"""Metal boundaries and condensate fate/collection composition remain explicit."""

import json
from copy import deepcopy

import pytest
from test_uvek_reference_signs import consumer, contexts

from brightpath import DATA_DIR, BrightwayInventory, TechnosphereProfile
from brightpath.background.rule_approvals import compatibility_rule_fingerprint
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource
from scripts.review_uvek_heat_310_closure import apply_heat_310_closure_review, compiled_closure_rule

REVIEW = json.loads((DATA_DIR / "export/uvek_heat_310_closure_review.json").read_text())
CONDENSATE = next(row for row in REVIEW["records"] if row["action"] == "reconstruct_market")


@pytest.fixture(scope="module")
def policy():
    resource = load_uvek_technosphere_resource()
    indexed = {r["id"]: r for r in resource["replace"]}
    scopes = tuple(
        compatibility_rule_fingerprint(
            resource,
            "replace",
            indexed[row["rule_id"]],
            TechnosphereProfile("ecoinvent", "3.10", "cutoff"),
            TechnosphereProfile("uvek", "2025", "cutoff"),
        )
        for row in REVIEW["records"]
    )
    return MigrationPolicy(approved_compatibility_rules=scopes, on_information_loss=PolicyAction.WARN)


@pytest.mark.parametrize("row", REVIEW["records"], ids=lambda r: r["rule_id"])
@pytest.mark.parametrize("amount,uncertainty", [(-2, 2), (2, 2), (-2, 3), (2, 3)])
def test_signed_reference_coordinates_and_complete_recipe_inputs(row, amount, uncertainty, policy):
    data = consumer(row["source"], amount, uncertainty)
    before = deepcopy(data)
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.10")).migrate_background(
        contexts("uvek", "2026").background,
        policy=policy,
    )
    assert data == before
    edge = result.data[0]["exchanges"][1]
    old = before[0]["exchanges"][1]
    factor = row["conversion_factor"]
    assert edge["amount"] == amount * factor
    assert edge["amount"] / row["target_reference_amount"] == amount / row["source_reference_amount"]
    assert edge["scale"] == old["scale"]
    if factor == 1:
        assert edge["formula"] == old["formula"] and edge["loc"] == old["loc"]
    else:
        assert row["previous_rule"]["conversion_factor"] == -1
        assert edge["formula"] == "(dose) * (-1.0)"
        if uncertainty == 2:
            assert edge["loc"] == old["loc"] and edge["negative"] is not old["negative"]
        else:
            assert edge["loc"] == -old["loc"]
            assert edge["minimum"] == -old["maximum"] and edge["maximum"] == -old["minimum"]
    assert not any(
        loss.code == "migration.recipe_source_input_omission" for loss in result.last_migration_report.losses
    )
    if not row["new_recipes"]:
        assert len(result.data) == 1
        assert all(edge[k] == v for k, v in row["target"].items())
        return
    helpers = {a["brightpath_conversion"]["recipe_id"]: a for a in result.data[1:]}
    assert set(helpers) == set(row["new_recipes"])
    for rid, recipe in row["new_recipes"].items():
        actual = [e for e in helpers[rid]["exchanges"] if e["type"] != "production"]
        assert len(actual) == len(recipe["inputs"])
        for exchange, expected in zip(actual, recipe["inputs"], strict=True):
            for field, value in expected.items():
                if field == "recipe":
                    assert all(
                        exchange[k] == helpers[value][k] for k in ("name", "reference product", "unit", "location")
                    )
                elif field == "categories":
                    assert tuple(exchange[field]) == tuple(value)
                else:
                    assert exchange[field] == value


def test_condensate_has_literal_fates_separate_collection_and_no_sewer_double_count():
    row = CONDENSATE
    recipes = row["new_recipes"]
    treatment = recipes[row["treatment_recipe_ref"]]["inputs"]
    collection = recipes[row["collection_recipe_ref"]]["inputs"]
    market = recipes[row["market_recipe_ref"]]["inputs"]
    assert len(treatment) == 76
    assert sum(e["type"] == "technosphere" for e in treatment) == 26
    assert sum(e.get("type") == "biosphere" for e in treatment) == 50
    assert not any(e.get("name") == "Sewer grid, class 2" for e in treatment)
    assert collection == [row["native_sewer_relocation"]["removed_embedded_exchange"]]
    assert collection[0]["amount"] == 1.6832e-7 and collection[0]["unit"] == "kilometer"
    assert collection[0]["uncertainty type"] == 0
    treatment_links = [e for e in market if e.get("recipe") == row["treatment_recipe_ref"]]
    assert [e["amount"] for e in treatment_links] == [0.4126619233000178]
    collection_links = [e for e in market if e.get("recipe") == row["collection_recipe_ref"]]
    assert [e["amount"] for e in collection_links] == [0.1723, 0.8277]
    assert all(e["amount"] > 0 for e in treatment if e["type"] == "technosphere")
    assert len(row["retained_untreated_biosphere_inputs"]) == 17
    water = next(e for e in market if e.get("name") == "Water")
    assert water["amount"] == 0.587241188  # Source value, not renormalized complement.
    assert [e for e in market if e.get("type") == "biosphere"] == row["retained_untreated_biosphere_inputs"]


@pytest.mark.parametrize("row", REVIEW["records"], ids=lambda r: r["rule_id"])
@pytest.mark.parametrize("version,model", [("3.9", "cutoff"), ("3.11", "cutoff"), ("3.10", "consequential")])
def test_other_profiles_cannot_inherit_the_new_permissions(row, version, model, policy):
    data = consumer(row["source"], -1)
    before = deepcopy(data)
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(data, context=contexts("ecoinvent", version, model)).migrate_background(
            contexts("uvek", "2026").background,
            policy=policy,
        )
    assert data == before


@pytest.mark.parametrize("drift", ["none", "late_rule", "treatment", "collection", "market"])
def test_authoring_is_idempotent_and_refuses_drift_atomically(drift):
    resource = deepcopy(load_uvek_technosphere_resource())
    apply_heat_310_closure_review(resource)
    if drift == "late_rule":
        next(r for r in resource["replace"] if r["id"] == CONDENSATE["rule_id"])["conversion_factor"] = 1
    elif drift != "none":
        rid = CONDENSATE[drift + "_recipe_ref"]
        resource["recipes"][rid]["inputs"][0]["amount"] *= 2
    if drift != "none":
        resource["replace"] = [
            deepcopy(REVIEW["records"][1]["previous_rule"]) if r["id"] == REVIEW["records"][1]["rule_id"] else r
            for r in resource["replace"]
        ]
    before = deepcopy(resource)
    if drift == "none":
        apply_heat_310_closure_review(resource)
        assert all(
            next(r for r in resource["replace"] if r["id"] == row["rule_id"]) == compiled_closure_rule(row)
            for row in REVIEW["records"]
        )
    else:
        with pytest.raises(ValueError):
            apply_heat_310_closure_review(resource)
    assert resource == before
