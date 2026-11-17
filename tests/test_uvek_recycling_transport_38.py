"""Recycling transport keeps units and matching diesel EURO/weight classes."""

import hashlib
import json
import re
from copy import deepcopy

import pytest
from test_uvek_reference_signs import consumer, contexts

from brightpath import DATA_DIR, BrightwayInventory, TechnosphereProfile
from brightpath.background.reference_signs import select_profile_sign_rules
from brightpath.background.rule_approvals import compatibility_rule_fingerprint
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource

REVIEW = json.loads((DATA_DIR / "export/uvek_recycling_transport_38_review.json").read_text())
ROWS = REVIEW["records"] + REVIEW["additional_approvals"]
PRIOR = [(op, scope) for op in REVIEW["preserved_prior_approval_operations"] for scope in op["scopes"]]
FIELDS = ("name", "reference product", "location", "unit")


def policy(approved=True):
    resource = load_uvek_technosphere_resource()
    rules = {r["id"]: r for r in resource["replace"]}
    fingerprints = tuple(
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
        approved_compatibility_rules=fingerprints if approved else (), on_information_loss=PolicyAction.WARN
    )


@pytest.mark.parametrize("amount", [-2.5, 2.5])
@pytest.mark.parametrize("uncertainty", [2, 3])
@pytest.mark.parametrize("release", ["2025", "2026"])
def test_complete_transports_keep_quantity_coordinates_and_matching_euro_classes(amount, uncertainty, release):
    data = consumer(ROWS[0]["source"], amount, uncertainty)
    data[0]["exchanges"].extend(consumer(row["source"], amount, uncertainty)[0]["exchanges"][1] for row in ROWS[1:])
    before = deepcopy(data)
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.8")).migrate_background(
        contexts("uvek", release).background, policy=policy()
    )
    assert data == before and len(result.data) == 20
    helpers = {a["brightpath_conversion"]["recipe_id"]: a for a in result.data[1:]}
    for edge, old, row in zip(result.data[0]["exchanges"][1:], before[0]["exchanges"][1:], ROWS, strict=True):
        for key in ("amount", "loc", "scale", "formula", "negative", "minimum", "maximum", "uncertainty type"):
            if key in old:
                assert edge[key] == old[key]
        assert edge["unit"] == row["source"]["unit"]
        target = helpers[row["target"]["recipe_ref"]] if "recipe_ref" in row["target"] else row["target"]
        assert all(edge[k] == target[k] for k in FIELDS)
        recipe = row["target"].get("recipe_ref")
        if recipe in REVIEW["conditional_fleet_evidence"]:
            evidence = REVIEW["conditional_fleet_evidence"][recipe]
            inputs = target["exchanges"][1:]
            assert len(inputs) in (3, 9) and sum(e["amount"] for e in inputs) == pytest.approx(1)
            assert all(e["amount"] > 0 and e["unit"] == "ton kilometer" for e in inputs)
            assert all(e["name"].startswith("Transport, freight, lorry, diesel,") for e in inputs)
            assert all(f"EURO-{evidence['standard']}," in e["name"] for e in inputs)
            band = row["source"]["name"]
            weights = {float(re.search(r"diesel, ([\d.]+)t gross", e["name"])[1]) for e in inputs}
            assert weights == ({7.5} if "3.5-7.5" in band else {18, 26, 32} if "16-32" in band else {40})
            assert not target["brightpath_conversion"].get("source_input_omissions")
    assert REVIEW["new_helpers"] == 17 and REVIEW["new_foreground_omissions"] == []


@pytest.mark.parametrize("operation,scope", PRIOR, ids=[f"{op['rule_id']}-{s['source']['version']}" for op, s in PRIOR])
def test_prior_physical_suppliers_and_recipe_contents_are_unchanged(operation, scope):
    resource = load_uvek_technosphere_resource()
    row = next((r for r in REVIEW["records"] if r["rule_id"] == operation["rule_id"]), None)
    if row:
        profile = TechnosphereProfile(**scope["source"])
        actual = next(r for r in select_profile_sign_rules(resource, profile)["replace"] if r["id"] == row["rule_id"])
        parent = {**resource, "replace": [row["original_rule"]]}
        original = select_profile_sign_rules(parent, profile)["replace"][0]
        for key in ("source", "target", "mapping_method", "classification", "confidence"):
            assert actual.get(key) == original.get(key)
        assert actual.get("conversion_factor", 1) == original.get("conversion_factor", 1)
    for recipe, expected in REVIEW["recipe_dependency_sha256"].items():
        raw = json.dumps(resource["recipes"][recipe], sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        assert hashlib.sha256(raw.encode()).hexdigest() == expected


def test_previously_global_row_lorry_still_has_its_original_39_physical_target():
    resource = load_uvek_technosphere_resource()
    row = next(r for r in REVIEW["records"] if r["rule_id"] == "bp-c5304556b418deab")
    rule = next(r for r in resource["replace"] if r["id"] == row["rule_id"])
    fingerprint = compatibility_rule_fingerprint(
        resource,
        "replace",
        rule,
        TechnosphereProfile("ecoinvent", "3.9", "cutoff"),
        TechnosphereProfile(**REVIEW["target_profile"]),
    )
    prior_policy = MigrationPolicy(approved_compatibility_rules=(fingerprint,), on_information_loss=PolicyAction.WARN)
    result = BrightwayInventory.from_data(
        consumer(row["source"], 2), context=contexts("ecoinvent", "3.9")
    ).migrate_background(contexts("uvek", "2026").background, policy=prior_policy)
    assert len(result.data) == 1 and result.data[0]["exchanges"][1]["amount"] == 2
    assert result.data[0]["exchanges"][1]["name"] == "Transport, freight, lorry, 16t-32t gross weight, fleet average"


@pytest.mark.parametrize("version,model", [("3.8.1", "cutoff"), ("3.8", "consequential")])
def test_new_permissions_do_not_authorize_other_contexts(version, model):
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(
            consumer(ROWS[0]["source"], 1), context=contexts("ecoinvent", version, model)
        ).migrate_background(contexts("uvek", "2026").background, policy=policy())


def test_missing_permission_refuses_the_new_profile():
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(
            consumer(ROWS[0]["source"], 1), context=contexts("ecoinvent", "3.8")
        ).migrate_background(contexts("uvek", "2026").background, policy=policy(False))
