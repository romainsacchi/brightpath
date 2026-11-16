"""Actual truck inputs preserve function, source scope and treatment coordinates."""

import hashlib
import json
from copy import deepcopy

import pytest
from test_uvek_reference_signs import consumer, contexts

from brightpath import DATA_DIR, BrightwayInventory, TechnosphereProfile
from brightpath.background.reference_signs import select_profile_sign_rules
from brightpath.background.rule_approvals import compatibility_rule_fingerprint
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource

REVIEW = json.loads((DATA_DIR / "export/uvek_trucks_native_38_review.json").read_text())
ROWS = REVIEW["records"] + REVIEW["additional_approvals"]
CHANGED = {r["rule_id"]: r for r in REVIEW["records"]}
OLD_SCOPES = [
    (operation, scope)
    for operation in REVIEW["preserved_prior_approval_operations"]
    if operation["rule_id"] in CHANGED
    for scope in operation["scopes"]
]


def policy(approved=True):
    resource = load_uvek_technosphere_resource()
    rules = {r["id"]: r for r in resource["replace"]}
    scopes = tuple(
        compatibility_rule_fingerprint(
            resource,
            "replace",
            rules[row["rule_id"]],
            TechnosphereProfile(**REVIEW["source_profile"]),
            TechnosphereProfile(**REVIEW["target_profile"]),
        )
        for row in ROWS
    )
    return MigrationPolicy(
        approved_compatibility_rules=scopes if approved else (), on_information_loss=PolicyAction.WARN
    )


@pytest.mark.parametrize("amount", [-2.5, 2.5])
@pytest.mark.parametrize("uncertainty", [2, 3])
@pytest.mark.parametrize("release", ["2025", "2026"])
def test_all_35_truck_inputs_keep_signed_coordinates_and_complete_native_boundaries(amount, uncertainty, release):
    data = consumer(ROWS[0]["source"], amount, uncertainty)
    data[0]["exchanges"].extend(consumer(row["source"], amount, uncertainty)[0]["exchanges"][1] for row in ROWS[1:])
    original = deepcopy(data)
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.8")).migrate_background(
        contexts("uvek", release).background, policy=policy()
    )
    assert data == original and len(result.data) == 3
    helpers = {a["brightpath_conversion"]["recipe_id"]: a for a in result.data[1:]}
    assert set(helpers) == {"trucks-38-reinforced-concrete-market", "remaining-freight-39-b84feb5869b37bc3"}
    assert sum(row["conversion_factor"] == -1 for row in ROWS) == 8
    for actual, old, row in zip(result.data[0]["exchanges"][1:], original[0]["exchanges"][1:], ROWS, strict=True):
        factor = row["conversion_factor"]
        assert actual["amount"] == old["amount"] * factor
        assert actual["loc"] == (old["loc"] if uncertainty == 2 else old["loc"] * factor)
        assert actual["scale"] == old["scale"]
        expected_formula = old["formula"] if factor == 1 else f"({old['formula']}) * (-1.0)"
        assert actual["formula"] == expected_formula
        if uncertainty == 2:
            assert actual["negative"] is (old["negative"] != (factor < 0))
        else:
            assert actual["minimum"] == (old["minimum"] if factor == 1 else -old["maximum"])
            assert actual["maximum"] == (old["maximum"] if factor == 1 else -old["minimum"])
        target = helpers[row["target"]["recipe_ref"]] if "recipe_ref" in row["target"] else row["target"]
        assert all(actual[k] == target[k] for k in ("name", "reference product", "location", "unit"))
        if row["rule_id"] == "bp-501eb0393277b522":
            assert actual["name"] == "Treatment, lorry production effluent, to wastewater treatment, class 1"
    concrete = helpers["trucks-38-reinforced-concrete-market"]["exchanges"][1:]
    expected = REVIEW["recipes"]["trucks-38-reinforced-concrete-market"]["inputs"]
    assert len(concrete) == 11
    assert sum(e["unit"] == "kilogram" for e in concrete) == 3
    assert sum(e["unit"] == "ton kilometer" for e in concrete) == 8
    assert all(
        all(actual[k] == value for k, value in old.items()) for actual, old in zip(concrete, expected, strict=True)
    )
    heavy = helpers["remaining-freight-39-b84feb5869b37bc3"]["exchanges"][1:]
    assert len(heavy) == 3 and sum(e["amount"] for e in heavy) == pytest.approx(1)
    assert all("diesel, 40t gross weight, 2020, EURO-VI" in e["name"] and e["location"] == "RER" for e in heavy)
    assert all(not a["brightpath_conversion"].get("source_input_omissions") for a in result.data[1:])


@pytest.mark.parametrize(
    "operation,scope", OLD_SCOPES, ids=[f"{op['rule_id']}-{s['source']['version']}" for op, s in OLD_SCOPES]
)
def test_previously_approved_selected_rules_and_all_borrowed_recipes_are_unchanged(operation, scope):
    resource = load_uvek_technosphere_resource()
    profile = TechnosphereProfile(**scope["source"])
    rid = operation["rule_id"]
    current = next(r for r in select_profile_sign_rules(resource, profile)["replace"] if r["id"] == rid)
    parent = {**resource, "replace": [CHANGED[rid]["original_rule"]]}
    assert current == select_profile_sign_rules(parent, profile)["replace"][0]
    for recipe_id, expected in REVIEW["recipe_dependency_sha256"].items():
        raw = json.dumps(resource["recipes"][recipe_id], sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        assert hashlib.sha256(raw.encode()).hexdigest() == expected


@pytest.mark.parametrize("version,model", [("3.9.1", "cutoff"), ("3.7", "cutoff"), ("3.8", "consequential")])
def test_new_scope_does_not_authorize_other_source_contexts(version, model):
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(
            consumer(ROWS[0]["source"], -1), context=contexts("ecoinvent", version, model)
        ).migrate_background(contexts("uvek", "2026").background, policy=policy())


def test_missing_scoped_approval_still_refuses_the_reviewed_source():
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(
            consumer(ROWS[0]["source"], -1), context=contexts("ecoinvent", "3.8")
        ).migrate_background(contexts("uvek", "2026").background, policy=policy(approved=False))
