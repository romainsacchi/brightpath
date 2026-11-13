"""The municipal-waste portfolio keeps routing, untreated fates and cut-off outputs."""

import hashlib
import json
import math
from copy import deepcopy

import pytest
from test_uvek_reference_signs import consumer, contexts

from brightpath import DATA_DIR, BrightwayInventory, TechnosphereProfile
from brightpath.background.rule_approvals import compatibility_rule_fingerprint
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource
from scripts.review_uvek_mining_38_msw_portfolio import apply_mining_38_msw_portfolio_review

REVIEW = json.loads((DATA_DIR / "export/uvek_mining_38_msw_portfolio_review.json").read_text())
ROW = REVIEW["records"][0]
ROOT = "mining-38-msw-europe-collection-and-treatment"


def migrate(amount, uncertainty, version="3.8"):
    resource = load_uvek_technosphere_resource()
    rule = next(r for r in resource["replace"] if r["id"] == ROW["rule_id"])
    fp = compatibility_rule_fingerprint(
        resource,
        "replace",
        rule,
        TechnosphereProfile("ecoinvent", "3.8", "cutoff"),
        TechnosphereProfile("uvek", "2025", "cutoff"),
    )
    data = consumer(rule["source"], amount, uncertainty)
    before = deepcopy(data)
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", version)).migrate_background(
        contexts("uvek", "2026").background,
        policy=MigrationPolicy(approved_compatibility_rules=(fp,), on_information_loss=PolicyAction.WARN),
    )
    assert data == before
    return result, before


@pytest.mark.parametrize("amount,uncertainty", [(-2, 2), (2, 2), (-2, 3), (2, 3)])
def test_signed_caller_and_every_nested_exchange_survive(amount, uncertainty):
    result, before = migrate(amount, uncertainty)
    edge = result.data[0]["exchanges"][1]
    assert edge["amount"] == -amount and edge["formula"] == "(dose) * (-1.0)"
    assert edge["scale"] == before[0]["exchanges"][1]["scale"]
    if uncertainty == 2:
        assert edge["loc"] == before[0]["exchanges"][1]["loc"]
        assert edge["negative"] is (amount > 0)
    else:
        assert edge["loc"] == -amount
        assert (edge["minimum"], edge["maximum"]) == (-amount - 1, -amount + 1)
    helpers = {a["brightpath_conversion"]["recipe_id"]: a for a in result.data[1:]}
    recipes = {**REVIEW["recipes"], **REVIEW["recipe_dependencies"]}
    assert set(helpers) == set(recipes) and len(helpers) == 12
    for recipe_id, helper in helpers.items():
        inputs = [e for e in helper["exchanges"] if e["type"] != "production"]
        assert len(inputs) == len(recipes[recipe_id]["inputs"])
        for actual, original in zip(inputs, recipes[recipe_id]["inputs"], strict=True):
            for key, value in original.items():
                if key not in {"recipe", "zero_burden_supplier"}:
                    assert actual[key] == value
            if "recipe" in original:
                assert all(
                    actual[k] == helpers[original["recipe"]][k]
                    for k in ("name", "reference product", "location", "unit")
                )


def test_original_disposal_weights_and_uncertain_freight_are_not_normalized():
    routing = REVIEW["routing"]
    assert routing["normalization_applied"] is False
    routes = routing["terminal_suppliers"]
    assert len(routes) == 30 and len(routing["collection_freight"]) == 35
    assert math.isclose(sum(r["treatment_activity"] for r in routes), 0.9999998698370718, abs_tol=1e-15)
    assert sum(r["treatment_activity"] for r in routes) != 1
    root_inputs = REVIEW["recipes"][ROOT]["inputs"]
    assert len(root_inputs) == 65
    for record in routing["collection_freight"]:
        e = record["compiled_exchange"]
        assert e in root_inputs and e["amount"] == record["source_amount"] * record["deterministic_parent_activity"]
        assert e["uncertainty type"] == 2 and e.get("pedigree")
        assert e["loc"] == pytest.approx(math.log(e["amount"]), abs=1e-12)
    uncontrolled = [r for r in routes if "recipe" in r["effective_target"]]
    assert len(uncontrolled) == 9
    assert sum(r["treatment_activity"] for r in uncontrolled) > 0.19
    assert len([r for r in routes if "incineration" in r["source_supplier"]["name"]]) == 20


def test_public_emissions_operations_and_scrap_outputs_all_remain():
    calc = REVIEW["public_calculation"]
    assert hashlib.sha256((DATA_DIR.parents[1] / calc["path"]).read_bytes()).hexdigest() == calc["sha256"]
    biosphere = [e for r in REVIEW["recipes"].values() for e in r["inputs"] if e.get("type") == "biosphere"]
    assert len(biosphere) == 1008
    burner = REVIEW["recipes"]["mining-38-msw-open-burning"]
    outputs = [e for e in burner["inputs"] if "recipe" in e]
    assert sorted(e["amount"] for e in outputs) == [-0.017098, -0.00083857]
    for e in outputs:
        assert e["negative"] is True and e["uncertainty type"] == 2
        assert e["zero_burden_supplier"]["source_profile"] == {
            "family": "ecoinvent",
            "version": "3.8",
            "system_model": "cutoff",
        }
        assert REVIEW["recipe_dependencies"][e["recipe"]]["inputs"] == []
    for climate in ("dry", "moist", "wet", "very-wet"):
        inputs = REVIEW["recipes"]["mining-38-msw-unsanitary-" + climate]["inputs"]
        operations = [e for e in inputs if e.get("type") == "technosphere"]
        assert [(e["unit"], e["amount"]) for e in operations] == [("cubic meter", 0.00031708), ("megajoule", 0.046738)]
        assert all(e["uncertainty type"] == 2 for e in operations)


@pytest.mark.parametrize("drift", [None, "parent", "recipe", "stock", "stock_approval"])
def test_replay_is_atomic_and_idempotent(drift):
    resource = deepcopy(load_uvek_technosphere_resource())
    if drift == "parent":
        next(r for r in resource["replace"] if r["id"] == ROW["rule_id"])["confidence"] = 0.1
    elif drift == "recipe":
        resource["recipes"][ROOT]["inputs"][0]["amount"] += 1
    elif drift == "stock":
        resource["recipes"][next(iter(REVIEW["recipe_dependencies"]))]["rationale"] += " changed"
    elif drift == "stock_approval":
        rule = next(r for r in resource["replace"] if r["id"] == REVIEW["component_bindings"][0]["rule_id"])
        approval = next(
            p for p in rule["reference_flow_sign_review"]["profiles"] if p["source_profile"] == ROW["source_profile"]
        )
        approval["review"]["reason"] += " changed"
    before = deepcopy(resource)
    if drift:
        with pytest.raises(ValueError):
            apply_mining_38_msw_portfolio_review(resource)
    else:
        apply_mining_38_msw_portfolio_review(resource)
    assert resource == before


def test_another_release_does_not_inherit_the_disposal_portfolio():
    with pytest.raises(MigrationError):
        migrate(-1, 2, version="3.9")
