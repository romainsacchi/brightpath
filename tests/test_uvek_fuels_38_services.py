"""Native services retain functional quantities; petrol must not become flare gas."""

import json
from copy import deepcopy
from dataclasses import replace

import pytest
from test_uvek_reference_signs import consumer, contexts
from test_uvek_refrigerant_disposal import target_context

from brightpath import DATA_DIR, BrightwayInventory
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource
from scripts.review_uvek_fuels_38_services import apply_fuels_38_services_review

REVIEW = json.loads((DATA_DIR / "export/uvek_fuels_38_services_review.json").read_text())
RECORDS = {r["rule_id"]: r for r in REVIEW["records"]}


def migrate(rid, amount=1, uncertainty=3, version="3.8", model="cutoff", strict=False):
    source = BrightwayInventory.from_data(
        consumer(RECORDS[rid]["original_rule"]["source"], amount, uncertainty),
        context=contexts("ecoinvent", version, model),
    )
    before = deepcopy(source.data)
    policy = MigrationPolicy.permissive()
    if strict:
        policy = replace(
            policy,
            on_invalid_source=PolicyAction.ERROR,
            on_invalid_target=PolicyAction.ERROR,
            on_unresolved_link=PolicyAction.ERROR,
            minimum_coverage=1.0,
        )
    result = source.migrate_background(target_context("2026"), policy=policy)
    assert source.data == before
    return result


def helper(result):
    edge = result.data[0]["exchanges"][1]
    return next(a for a in result.data if a.get("code") == edge["input"][1])


@pytest.mark.parametrize("rid", RECORDS)
@pytest.mark.parametrize("amount", [-2, 2])
@pytest.mark.parametrize("uncertainty", [2, 3])
def test_caller_stock_mass_uncertainty_and_formula_survive(rid, amount, uncertainty):
    result = migrate(rid, amount, uncertainty)
    edge = result.data[0]["exchanges"][1]
    factor = RECORDS[rid]["source_witness"]["reference_amounts"][0]
    assert edge["amount"] == amount * factor and edge["uncertainty type"] == uncertainty
    assert edge["scale"] == 0.2
    assert edge["formula"] == ("dose" if factor == 1 else "(dose) * (-1.0)")
    if uncertainty == 2:
        assert edge["negative"] is (amount * factor < 0)
    else:
        assert edge["loc"] == amount * factor


@pytest.mark.parametrize("rid", [rid for rid, row in RECORDS.items() if row["resource_action"] != "unchanged_rule"])
@pytest.mark.parametrize("version,model", [("3.10", "cutoff"), ("3.8", "consequential")])
def test_unreviewed_profiles_are_not_inferred(rid, version, model):
    with pytest.raises(MigrationError):
        migrate(rid, version=version, model=model)


@pytest.mark.parametrize("rid", RECORDS)
def test_strict_native_target(rid):
    result = migrate(rid, strict=True)
    row = RECORDS[rid]
    edge = result.data[0]["exchanges"][1]
    if "recipe_ref" in row["new_target"]:
        ds = helper(result)
        assert ds["name"].startswith(REVIEW["recipes"][row["new_target"]["recipe_ref"]]["name"])
        assert len([e for e in ds["exchanges"] if e["type"] == "technosphere"]) == 4
        assert len([e for e in ds["exchanges"] if e["type"] == "biosphere"]) == 21
    else:
        assert all(edge[k] == value for k, value in row["new_target"].items())
        assert len(result.data) == 1


def test_fourteen_existing_rules_remain_unchanged():
    resource = load_uvek_technosphere_resource()
    unchanged = [r for r in REVIEW["records"] if r["resource_action"] == "unchanged_rule"]
    assert len(unchanged) == 14
    for row in unchanged:
        current = next(r for r in resource["replace"] if r["id"] == row["rule_id"])
        assert current == row["original_rule"] == row["compiled_rule"]


def test_petrol_and_agricultural_diesel_correct_physical_targets():
    result = migrate("bp-f5c20db2989094da", strict=True)
    petrol = result.data[0]["exchanges"][1]
    ds = helper(result)
    inputs = [e for e in ds["exchanges"] if e["type"] == "technosphere"]
    assert any(e["name"] == "Petrol, at regional storage" and e["amount"] == 0.02447 for e in inputs)
    assert not any("refinery gas" in e["name"].lower() for e in inputs)
    diesel = migrate("bp-ab9ef0df411ba0fe", strict=True).data[0]["exchanges"][1]
    assert diesel["name"] == "Diesel, burned in building machine, average"
    assert petrol["unit"] == diesel["unit"] == "megajoule"
    assert petrol["amount"] == diesel["amount"] == 1


def test_replay_idempotence_and_changed_parent_rejection():
    resource = deepcopy(load_uvek_technosphere_resource())
    apply_fuels_38_services_review(resource)
    before = deepcopy(resource)
    apply_fuels_38_services_review(resource)
    assert resource == before
    next(r for r in resource["replace"] if r["id"] == "bp-f5c20db2989094da")["conversion_factor"] = 2
    with pytest.raises(ValueError, match="parent"):
        apply_fuels_38_services_review(resource)
