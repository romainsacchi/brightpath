"""Public PAM reactants retain source production services and final delivery."""

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
from scripts.review_uvek_mining_38_pam_public import apply_mining_38_pam_public_review

REVIEW = json.loads((DATA_DIR / "export/uvek_mining_38_pam_public_review.json").read_text())
ROW = REVIEW["records"][0]
PRODUCTION = "mining-38-pam-reactants-and-production-services"
MARKET = "mining-38-pam-market-retained-delivery"


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


@pytest.mark.parametrize("amount,uncertainty", [(-2, 2), (2, 3)])
def test_signed_quantity_and_all_nested_delivery_fields(amount, uncertainty):
    result, original = migrate(amount, uncertainty)
    edge = result.data[0]["exchanges"][1]
    assert edge["amount"] == amount and edge["formula"] == "dose"
    assert edge["scale"] == original[0]["exchanges"][1]["scale"]
    assert edge["loc"] == pytest.approx(math.log(abs(amount)) if uncertainty == 2 else amount)
    if uncertainty == 2:
        assert edge["negative"] is (amount < 0)
    else:
        assert (edge["minimum"], edge["maximum"]) == (amount - 1, amount + 1)
    helpers = {a["brightpath_conversion"]["recipe_id"]: a for a in result.data[1:]}
    assert set(helpers) == {PRODUCTION, MARKET, *REVIEW["recipe_dependencies"]}
    for recipe_id, helper in helpers.items():
        inputs = [e for e in helper["exchanges"] if e["type"] != "production"]
        assert len(inputs) == len({**REVIEW["recipes"], **REVIEW["recipe_dependencies"]}[recipe_id]["inputs"])
        for actual, source in zip(
            inputs, {**REVIEW["recipes"], **REVIEW["recipe_dependencies"]}[recipe_id]["inputs"], strict=True
        ):
            for key, value in source.items():
                if key != "recipe":
                    assert actual[key] == value
            if "recipe" in source:
                assert all(
                    actual[k] == helpers[source["recipe"]][k] for k in ("name", "reference product", "location", "unit")
                )
    assert "Do not convert MJ to kg steam" in helpers[PRODUCTION]["comment"]
    assert "not physically zero or negligible" in helpers[PRODUCTION]["comment"]


def test_public_model_is_active_polymer_not_a_diluted_solution_or_generic_chemical():
    inputs = REVIEW["recipes"][PRODUCTION]["inputs"][:2]
    assert [e["amount"] for e in inputs] == [0.75, 0.25]
    assert sum(e["amount"] for e in inputs) == 1
    assert [e["name"] for e in inputs] == ["Acrylonitrile from Sohio process, at plant", "Tap water, at user"]
    assert all(e["unit"] == "kilogram" and e["location"] == "RER" for e in inputs)
    assert REVIEW["public_model"]["water_mass_is_dilution"] is False
    services = REVIEW["recipes"][PRODUCTION]["inputs"][2:]
    assert [e["amount"] for e in services] == [4e-10, 0.333, 0.29977153056, 1.70022846944]
    assert services[0]["unit"] == "unit" and services[1]["unit"] == "kilowatt hour"
    assert sum(e["amount"] for e in services[2:]) == 2
    assert all(e["uncertainty type"] == 2 and e.get("pedigree") for e in services)
    freight = [e for e in REVIEW["recipes"][MARKET]["inputs"] if "recipe" not in e]
    assert [e["amount"] for e in freight] == [0.3091, 0.0246, 0.599, 0.2088]
    assert all(e["uncertainty type"] == 2 and e.get("pedigree") for e in freight)


@pytest.mark.parametrize("drift", [None, "parent", "recipe", "heat_dependency"])
def test_replay_is_atomic_and_idempotent(drift):
    resource = deepcopy(load_uvek_technosphere_resource())
    if drift == "parent":
        next(r for r in resource["replace"] if r["id"] == ROW["rule_id"])["confidence"] = 0.1
    if drift == "recipe":
        resource["recipes"][PRODUCTION]["inputs"][0]["amount"] += 1
    if drift == "heat_dependency":
        key = next(iter(REVIEW["recipe_dependencies"]))
        resource["recipes"][key]["inputs"][0]["amount"] += 1
    before = deepcopy(resource)
    if drift:
        with pytest.raises(ValueError):
            apply_mining_38_pam_public_review(resource)
    else:
        apply_mining_38_pam_public_review(resource)
    assert resource == before


def test_another_release_does_not_inherit_the_proxy():
    with pytest.raises(MigrationError):
        migrate(1, 2, version="3.9")
