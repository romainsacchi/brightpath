"""The nitrate pilot reuses the complete reviewed Swiss concrete closure."""

from copy import deepcopy
import hashlib
import json

import pytest

from brightpath import BrightwayInventory, DATA_DIR, TechnosphereProfile
from brightpath.background.reference_signs import select_profile_sign_rules
from brightpath.background.rule_approvals import compatibility_rule_fingerprint
from brightpath.core.policies import MigrationPolicy
from brightpath.migrations.resources import load_uvek_technosphere_resource
from test_uvek_reference_signs import consumer, contexts
from test_uvek_refrigerant_disposal import target_context

REVIEW = json.loads((DATA_DIR / "export/uvek_pilot_nitrate_concrete_review.json").read_text())
(ROW,) = REVIEW["records"]


@pytest.mark.parametrize("amount", [-2, 0, 2])
@pytest.mark.parametrize("uncertainty", [2, 3])
def test_concrete_waste_sign_and_collection_uncertainty_survive(amount, uncertainty):
    data = consumer(ROW["source"], amount, uncertainty)
    before = deepcopy(data)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.9"))
    result = inventory.migrate_background(target_context("2026"), policy=MigrationPolicy.permissive())
    assert inventory.data == data == before
    actual = result.data[0]["exchanges"][1]
    original = before[0]["exchanges"][1]
    assert actual["amount"] == -amount
    assert actual["formula"] == "(dose) * (-1.0)"
    assert actual["scale"] == original["scale"]
    if uncertainty == 2:
        assert actual["loc"] == original["loc"]
        assert actual["negative"] is not original["negative"]
    else:
        assert actual["loc"] == -original["loc"]
        assert actual["minimum"] == -original["maximum"]
        assert actual["maximum"] == -original["minimum"]
    helpers = {r["code"]: r for r in result.data[1:]}
    assert len(helpers) == 3
    root = helpers[actual["input"][1]]
    (landfill_link,) = [x for x in root["exchanges"] if x["type"] == "technosphere" and x["unit"] == "kilogram"]
    assert landfill_link["amount"] == 1
    fields = ("name", "reference product", "location", "unit")
    (landfill,) = [h for h in helpers.values() if all(h[f] == landfill_link[f] for f in fields)]
    assert sorted(x["amount"] for x in landfill["exchanges"] if x["type"] == "technosphere") == [1.48e-9, 1]
    assert not any(x["type"] == "biosphere" for x in landfill["exchanges"])
    collection = [x for x in root["exchanges"] if x["type"] == "technosphere" and x["unit"] == "ton kilometer"]
    assert sorted(x["amount"] for x in collection) == [0.00356, 0.02577]
    assert all(x["uncertainty type"] == 2 and x["scale"] > 0 for x in collection)
    assert not any(i.code == "migration.recipe_input_omission" for i in result.last_migration_report.issues)


def test_nitrate_concrete_pins_existing_review_and_every_recipe_in_its_closure():
    resource = load_uvek_technosphere_resource()
    rule = next(r for r in resource["replace"] if r["id"] == ROW["rule_id"])
    digest = lambda value: hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()
    assert not REVIEW["raw_rules_changed"]
    assert digest(rule) == ROW["raw_rule_sha256"]
    assert ROW["conversion_factor"] == ROW["source_reference_amount"] == -1
    for authority in REVIEW["authorization_evidence"]:
        assert hashlib.sha256((DATA_DIR / authority["path"]).read_bytes()).hexdigest() == authority["sha256"]
    for recipe, expected in ROW["recipe_closure_sha256"].items():
        assert digest(resource["recipes"][recipe]) == expected
    selected = next(
        r
        for r in select_profile_sign_rules(resource, TechnosphereProfile(**ROW["source_profile"]))["replace"]
        if r["id"] == ROW["rule_id"]
    )
    assert selected["target"] == ROW["target"]
    assert (
        compatibility_rule_fingerprint(
            resource,
            "replace",
            rule,
            TechnosphereProfile(**ROW["source_profile"]),
            TechnosphereProfile(**ROW["bridge_target_profile"]),
        )
        == ROW["fingerprint"]
    )
    assert ROW["matching_function_evidence"]["source_generic_components_match_prior_full_inventory_witnesses"]
    assert not ROW["matching_function_evidence"]["equal_source_native_lifetime_established"]
