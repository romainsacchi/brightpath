"""High-slag source composition feeds native finishing without double delivery."""

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
from scripts.review_uvek_mining_38_high_slag import apply_mining_38_high_slag_review

REVIEW = json.loads((DATA_DIR / "export/uvek_mining_38_high_slag_review.json").read_text())
CEMENT = "mining-38-us-high-slag-cement-85"
SLAG = "mining-38-us-delivered-ground-slag"
GYPSUM = "mining-38-row-delivered-mineral-gypsum"


def migrate(row, amount, uncertainty=2, version="3.8"):
    resource = load_uvek_technosphere_resource()
    rule = next(r for r in resource["replace"] if r["id"] == row["rule_id"])
    fp = compatibility_rule_fingerprint(
        resource,
        "replace",
        rule,
        TechnosphereProfile("ecoinvent", "3.8", "cutoff"),
        TechnosphereProfile("uvek", "2025", "cutoff"),
    )
    policy = MigrationPolicy(approved_compatibility_rules=(fp,), on_information_loss=PolicyAction.WARN)
    data = consumer(rule["source"], amount, uncertainty)
    before = deepcopy(data)
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", version)).migrate_background(
        contexts("uvek", "2026").background, policy=policy
    )
    assert data == before
    return result


@pytest.mark.parametrize("row", REVIEW["records"], ids=lambda row: row["rule_id"])
@pytest.mark.parametrize("amount,uncertainty", [(-2, 2), (2, 3)])
def test_signed_caller_and_all_nested_recipe_fields(row, amount, uncertainty):
    result = migrate(row, amount, uncertainty)
    edge = result.data[0]["exchanges"][1]
    assert edge["amount"] == amount
    assert edge["formula"] == "dose"
    assert edge["loc"] == pytest.approx(math.log(abs(amount)) if uncertainty == 2 else amount)
    helpers = {a["brightpath_conversion"]["recipe_id"]: a for a in result.data[1:]}
    expected = {SLAG} if row["compiled_rule"]["target"]["recipe_ref"] == SLAG else {CEMENT, SLAG, GYPSUM}
    assert set(helpers) == expected
    for recipe_id, helper in helpers.items():
        actual = [e for e in helper["exchanges"] if e["type"] != "production"]
        recipe = REVIEW["recipes"][recipe_id]
        assert len(actual) == len(recipe["inputs"])
        assert helper.get("parameters", []) == recipe.get("parameters", [])
        for edge, source in zip(actual, recipe["inputs"], strict=True):
            for key, value in source.items():
                if key != "recipe":
                    assert edge[key] == value, (recipe_id, key)
            if "recipe" in source:
                target = helpers[source["recipe"]]
                assert all(edge[k] == target[k] for k in ("name", "reference product", "location", "unit"))


def test_binder_mass_and_native_finishing_do_not_add_duplicate_material_freight():
    recipe = REVIEW["recipes"][CEMENT]
    inputs = recipe["inputs"]
    binder = [e for e in inputs if "recipe" in e or e.get("name") == "Clinker, at plant"]
    assert [e["amount"] for e in binder] == [0.1425, 0.85, 0.0075]
    assert sum(e["amount"] for e in binder) == 1
    assert not any(e.get("unit") == "ton kilometer" for e in inputs)
    assert len(REVIEW["native_freight_replaced_by_source_material_delivery"]) == 2
    assert next(e for e in inputs if e.get("unit") == "kilowatt hour")["amount"] == 0.0549
    assert next(e for e in inputs if "burned" in e.get("name", ""))["amount"] == 0.2336
    assert next(e for e in inputs if e.get("type") == "biosphere")["amount"] == 0.19764
    assert len(REVIEW["recipes"][GYPSUM]["inputs"]) == 12
    slag = REVIEW["recipes"][SLAG]["inputs"]
    assert next(e for e in slag if e["unit"] == "ton kilometer")["amount"] == 0.2
    assert next(e for e in slag if e["unit"] == "kilogram")["name"] == (
        "Ground granulated blast furnace slag, no burdens, at plant"
    )
    assert not any("with burdens" in e.get("name", "") for r in REVIEW["recipes"].values() for e in r["inputs"])


@pytest.mark.parametrize("drift", [None, "parent", "recipe"])
def test_replay_is_atomic_and_idempotent(drift):
    resource = deepcopy(load_uvek_technosphere_resource())
    if drift == "parent":
        next(r for r in resource["replace"] if r["id"] == REVIEW["records"][0]["rule_id"])["confidence"] = 0.1
    if drift == "recipe":
        resource["recipes"][CEMENT]["inputs"][0]["amount"] += 1
    before = deepcopy(resource)
    if drift:
        with pytest.raises(ValueError):
            apply_mining_38_high_slag_review(resource)
    else:
        apply_mining_38_high_slag_review(resource)
    assert resource == before


def test_review_does_not_authorize_another_release():
    with pytest.raises(MigrationError):
        migrate(REVIEW["records"][0], 1, version="3.9")
