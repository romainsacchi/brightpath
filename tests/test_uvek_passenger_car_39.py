"""Native car supplies keep units and oxide proxies retain their actual products."""

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
from scripts.review_uvek_passenger_car_39 import apply_passenger_car_39_review

REVIEW = json.loads((DATA_DIR / "export/uvek_passenger_car_39_review.json").read_text())


@pytest.mark.parametrize("amount,uncertainty", [(-2, 2), (2, 3)])
@pytest.mark.parametrize("release", ["2025", "2026"])
def test_signed_stock_and_waste_demands_and_allocated_market_entries(amount, uncertainty, release):
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
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.9")).migrate_background(
        contexts("uvek", release).background, policy=policy
    )
    assert data == before and len(result.data) == 3
    helpers = {a["brightpath_conversion"]["recipe_id"]: a for a in result.data[1:]}
    for row, original, edge in zip(rows, before[0]["exchanges"][1:], result.data[0]["exchanges"][1:], strict=True):
        expected = deepcopy(original)
        scale_exchange(expected, row["conversion_factor"])
        for key in ("amount", "loc", "scale", "negative", "uncertainty type", "formula"):
            if key in expected:
                assert edge[key] == expected[key]
        if "recipe_id" not in row:
            assert all(edge[key] == value for key, value in row["target"].items())
            continue
        market = helpers[row["recipe_id"]]
        assert edge["name"] == market["name"]
        assert edge["reference product"] == row["source"]["reference product"]
        purchases = [e for e in market["exchanges"] if e["type"] == "technosphere"]
        assert len(purchases) == len(row["bindings"]) == 6
        shares = []
        for purchase, binding in zip(purchases, row["bindings"], strict=True):
            if binding.get("allocation_reference_not_material_feed"):
                assert purchase["amount"] / binding["reference_process_factor"] == pytest.approx(
                    binding["source_coefficient"]
                )
                assert purchase["reference product"] == "Neodymium oxide, at plant"
                shares.append(binding["source_coefficient"])
            else:
                assert purchase["amount"] == binding["source_coefficient"]
                assert purchase["unit"] == "ton kilometer"
                assert all(purchase[key] == value for key, value in binding["target"].items())
        assert sum(shares) == 1
        assert all(e["reference product"] != "Lithium manganese oxide, at plant" for e in purchases)


def original_resource():
    resource = load_uvek_technosphere_resource()
    return {
        "replace": [deepcopy(row["original_rule"]) for row in REVIEW["records"]],
        "delete": [],
        "recipes": {},
        "reviews": deepcopy(resource["reviews"]),
        "coverage": {"unresolved_identities": 0},
    }


def test_replay_preserves_previously_reviewed_profile_rules():
    resource = original_resource()
    apply_passenger_car_39_review(resource)
    once = deepcopy(resource)
    apply_passenger_car_39_review(resource)
    assert resource == once
    for row in REVIEW["records"]:
        if "recipe_id" in row:
            continue
        for version in ("3.8", "3.10", "3.12"):
            source = TechnosphereProfile("ecoinvent", version, "cutoff")
            assert select_profile_sign_rules({"replace": [row["original_rule"]]}, source) == select_profile_sign_rules(
                {"replace": [row["compiled_rule"]]}, source
            )


@pytest.mark.parametrize("change", ["parent", "recipe"])
def test_changed_parent_or_recipe_is_rejected_before_mutation(change):
    resource = original_resource()
    if change == "parent":
        resource["replace"][0]["conversion_factor"] = 2
    else:
        key, recipe = next(iter(REVIEW["recipes"].items()))
        resource["recipes"][key] = {**recipe, "inputs": []}
    before = deepcopy(resource)
    with pytest.raises(ValueError, match="changed"):
        apply_passenger_car_39_review(resource)
    assert resource == before
