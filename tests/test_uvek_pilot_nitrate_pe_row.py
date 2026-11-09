"""RoW PE retains every source disposal class, elementary flow and freight term."""

from copy import deepcopy
import hashlib
import json

import pytest

from brightpath import BrightwayInventory, DATA_DIR
from brightpath.core.policies import MigrationPolicy
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource
from scripts.review_uvek_pilot_nitrate_pe_row import apply_pilot_nitrate_pe_row_review
from test_uvek_reference_signs import consumer, contexts
from test_uvek_refrigerant_disposal import target_context

REVIEW = json.loads((DATA_DIR / "export/uvek_pilot_nitrate_pe_row_review.json").read_text())
(ROW,) = REVIEW["records"]


@pytest.mark.parametrize("amount", [-2, 0, 2])
@pytest.mark.parametrize("uncertainty", [2, 3])
def test_row_market_preserves_all_fates_freight_and_signed_uncertainty(amount, uncertainty):
    data = consumer(ROW["source"], amount, uncertainty)
    emission = {
        "name": "Carbon dioxide, fossil",
        "categories": ["air"],
        "unit": "kilogram",
        "type": "biosphere",
        "amount": 7,
    }
    data[0]["exchanges"].append(emission)
    before = deepcopy(data)
    source = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.9"))
    result = source.migrate_background(target_context("2026"), policy=MigrationPolicy.permissive())
    assert source.data == data == before
    records = result.data
    assert len(records) == 13 and records[0]["exchanges"][-1] == emission
    actual = records[0]["exchanges"][1]
    original = before[0]["exchanges"][1]
    assert actual["amount"] == -amount and actual["formula"] == "(dose) * (-1.0)"
    assert actual["scale"] == original["scale"]
    if uncertainty == 2:
        assert actual["loc"] == original["loc"] and actual["negative"] is not original["negative"]
    else:
        assert actual["loc"] == -original["loc"]
        assert actual["minimum"] == -original["maximum"] and actual["maximum"] == -original["minimum"]
    root = next(r for r in records if r.get("code") == actual["input"][1])
    inputs = [x for x in root["exchanges"] if x["type"] != "production"]
    assert len(inputs) == 16
    for actual_input, expected in zip(inputs, ROW["profiles"][0]["inputs"], strict=True):
        assert all(actual_input[k] == v for k, v in expected.items() if k != "recipe")
    rails = [x for x in inputs if x["name"] == "Transport, freight, rail"]
    assert len(rails) == 3 and all(x["location"] == "RER" for x in rails)
    assert sum(x["unit"] == "ton kilometer" for x in inputs) == 4
    assert sum(x["name"] == "Disposal, polyethylene, 0.4% water, to municipal incineration" for x in inputs) == 1
    assert not any("polyethylene/polypropylene products" in x["name"] or "coal freight" in x["name"] for x in inputs)
    assert sum(x["type"] == "biosphere" for r in records[1:] for x in r["exchanges"]) == 1145
    hyperarid = [
        r for r in records if r.get("brightpath_conversion", {}).get("recipe_id") in REVIEW["auxiliary_recipes"]
    ]
    assert len(hyperarid) == 2
    assert all("hyperarid infiltration class (-250mm)" in r["name"] for r in hyperarid)
    assert all(sum(x["type"] == "biosphere" for x in r["exchanges"]) == 101 for r in hyperarid)
    assert not any(i.code == "migration.recipe_input_omission" for i in result.last_migration_report.issues)


@pytest.mark.parametrize("version", ["3.6", "3.12"])
def test_row_helper_refuses_uninspected_source_profiles(version):
    source = BrightwayInventory.from_data(consumer(ROW["source"], -1), context=contexts("ecoinvent", version))
    before = deepcopy(source.data)
    with pytest.raises(MigrationError):
        source.migrate_background(target_context("2026"), policy=MigrationPolicy.permissive())
    assert source.data == before


@pytest.mark.parametrize("change", ["none", "parent", "new_helper", "prior_helper"])
def test_row_authoring_replay_is_atomic_and_freezes_old_and_new_helpers(change):
    resource = deepcopy(load_uvek_technosphere_resource())
    if change == "parent":
        next(r for r in resource["replace"] if r["id"] == ROW["rule_id"])["confidence"] = 0.123
    elif change == "new_helper":
        resource["recipes"][next(iter(REVIEW["auxiliary_recipes"]))]["inputs"][0]["amount"] += 1
    elif change == "prior_helper":
        resource["recipes"][next(iter(REVIEW["recipe_dependencies"]))]["inputs"][0]["amount"] += 1
    before = deepcopy(resource)
    if change == "none":
        apply_pilot_nitrate_pe_row_review(resource)
        assert resource == before
    else:
        with pytest.raises(ValueError):
            apply_pilot_nitrate_pe_row_review(resource)
        assert resource == before


def test_row_review_keeps_complete_definition_roles_and_exact_prior_authorities():
    assert len(REVIEW["recipe_dependencies"]) == 9 and len(REVIEW["auxiliary_recipes"]) == 2
    assert len(ROW["source_fates"]) == 12 and len(ROW["freight_witnesses"]) == 4
    assert len(REVIEW["canonical_biosphere_witnesses"]) == 101
    assert sum(w["flow_role"] == "natural resource" for w in REVIEW["canonical_biosphere_witnesses"]) == 1
    assert sum(w["flow_role"] == "emission" for w in REVIEW["canonical_biosphere_witnesses"]) == 100
    assert len(REVIEW["canonical_label_or_CAS_changes"]) == 10
    for witness in REVIEW["authorization_evidence"]:
        assert hashlib.sha256((DATA_DIR / witness["path"]).read_bytes()).hexdigest() == witness["sha256"]
