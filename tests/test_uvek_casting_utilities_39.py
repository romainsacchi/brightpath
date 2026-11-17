"""Casting services retain useful-heat, delivered-voltage and cut-off-stock bases."""

import hashlib
import json
from copy import deepcopy

import pytest
from test_uvek_reference_signs import consumer, contexts

from brightpath import DATA_DIR, BrightwayInventory, TechnosphereProfile
from brightpath.background.rule_approvals import compatibility_rule_fingerprint
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource

REVIEW = json.loads((DATA_DIR / "export/uvek_casting_utilities_39_review.json").read_text())
ROWS = REVIEW["records"] + REVIEW["additional_approvals"]


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
def test_complete_native_services_and_existing_stock_keep_signed_caller_statistics(amount, uncertainty, release):
    data = consumer(ROWS[0]["source"], amount, uncertainty)
    data[0]["exchanges"].extend(consumer(r["source"], amount, uncertainty)[0]["exchanges"][1] for r in ROWS[1:])
    before = deepcopy(data)
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.9")).migrate_background(
        contexts("uvek", release).background, policy=policy()
    )
    assert data == before and len(result.data) == 2
    for got, old in zip(result.data[0]["exchanges"][1:], before[0]["exchanges"][1:], strict=True):
        for k in ("amount", "loc", "scale", "shape", "minimum", "maximum", "negative", "formula", "uncertainty type"):
            if k in old:
                assert got[k] == old[k]
        assert got["unit"] == old["unit"]
    heat, grid, stock = result.data[0]["exchanges"][1:]
    assert heat["name"] == "xx Heat, natural gas, at boiler modulating >100kW"
    assert heat["location"] == "RER" and heat["unit"] == "megajoule"
    assert grid["name"] == "Electricity, medium voltage, at grid" and grid["location"] == "CA"
    assert grid["unit"] == "kilowatt hour"
    helper = result.data[1]
    assert helper["brightpath_conversion"]["zero_burden"]
    assert helper["brightpath_conversion"]["recipe_id"] == ROWS[2]["target"]["recipe_ref"]
    assert len(helper["exchanges"]) == 1 and helper["exchanges"][0]["amount"] == 1
    assert all(stock[k] == helper[k] for k in ("name", "reference product", "location", "unit"))


def test_existing_stock_authority_and_recipe_are_not_changed():
    resource = load_uvek_technosphere_resource()
    stock = next(r for r in resource["replace"] if r["id"] == ROWS[2]["rule_id"])
    value = json.dumps(stock, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    assert hashlib.sha256(value.encode()).hexdigest() == REVIEW["reused_stock_rule_sha256"]
    for recipe, digest in REVIEW["recipe_dependency_sha256"].items():
        value = json.dumps(resource["recipes"][recipe], sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        assert hashlib.sha256(value.encode()).hexdigest() == digest


@pytest.mark.parametrize("version,model", [("3.10", "cutoff"), ("3.12", "cutoff"), ("3.9", "consequential")])
def test_new_utility_permission_does_not_extend_other_versions_or_systems(version, model):
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(
            consumer(ROWS[0]["source"], 1), context=contexts("ecoinvent", version, model)
        ).migrate_background(contexts("uvek", "2026").background, policy=policy())


def test_without_exact_permission_the_conversion_refuses():
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(
            consumer(ROWS[0]["source"], 1), context=contexts("ecoinvent", "3.9")
        ).migrate_background(contexts("uvek", "2026").background, policy=policy(False))
