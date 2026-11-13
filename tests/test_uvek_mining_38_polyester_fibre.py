"""PET raw material and native conversion must both be present exactly once."""

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
from scripts.review_uvek_mining_38_polyester_fibre import apply_mining_38_polyester_fibre_review

REVIEW = json.loads((DATA_DIR / "export/uvek_mining_38_polyester_fibre_review.json").read_text())
ROW = REVIEW["records"][0]
RECIPE_ID = ROW["compiled_rule"]["target"]["recipe_ref"]


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
def test_signed_purchase_and_complete_native_material_plus_conversion(amount, uncertainty):
    result, original = migrate(amount, uncertainty)
    assert len(result.data) == 2
    edge = result.data[0]["exchanges"][1]
    assert edge["amount"] == amount and edge["formula"] == "dose"
    assert edge["scale"] == original[0]["exchanges"][1]["scale"]
    assert edge["loc"] == pytest.approx(math.log(abs(amount)) if uncertainty == 2 else amount)
    if uncertainty == 2:
        assert edge["negative"] is (amount < 0)
    else:
        assert edge["minimum"] == amount - 1 and edge["maximum"] == amount + 1
    helper = result.data[1]
    assert helper["brightpath_conversion"]["recipe_id"] == RECIPE_ID
    inputs = [e for e in helper["exchanges"] if e["type"] != "production"]
    assert inputs == [{**e, "product": e["reference product"]} for e in REVIEW["recipes"][RECIPE_ID]["inputs"]]
    assert [e["name"] for e in inputs] == [
        "Polyethylene terephthalate, granulate, amorphous, at plant",
        "Fleece production, polyethylene terephthalate",
    ]
    assert all(e["amount"] == 1 and e["unit"] == "kilogram" for e in inputs)
    assert not any("Glass fibre" in e["name"] or "resin" in e["name"] for e in inputs)
    assert "unity-net-yield approximation" in helper["comment"]
    assert "not reconstructed" in helper["comment"]


@pytest.mark.parametrize("drift", [None, "parent", "recipe"])
def test_replay_is_idempotent_and_refuses_drift_before_mutation(drift):
    resource = deepcopy(load_uvek_technosphere_resource())
    if drift == "parent":
        next(r for r in resource["replace"] if r["id"] == ROW["rule_id"])["confidence"] = 0.1
    if drift == "recipe":
        resource["recipes"][RECIPE_ID]["inputs"][0]["amount"] += 1
    before = deepcopy(resource)
    if drift:
        with pytest.raises(ValueError):
            apply_mining_38_polyester_fibre_review(resource)
    else:
        apply_mining_38_polyester_fibre_review(resource)
    assert resource == before


def test_exact_release_approval_does_not_admit_another_source_profile():
    with pytest.raises(MigrationError):
        migrate(1, 2, version="3.9")
