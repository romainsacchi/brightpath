"""Final-disposal helpers retain source components and every collection term."""

from copy import deepcopy
import hashlib
import json

import pytest

from brightpath import BrightwayInventory, DATA_DIR
from brightpath.core.policies import MigrationPolicy
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource
from scripts.review_uvek_pilot_nitrate_inert39 import apply_pilot_nitrate_inert39_review
from test_uvek_reference_signs import consumer, contexts
from test_uvek_refrigerant_disposal import target_context

REVIEW = json.loads((DATA_DIR / "export/uvek_pilot_nitrate_inert39_review.json").read_text())


@pytest.mark.parametrize("row", REVIEW["records"], ids=lambda r: r["rule_id"])
@pytest.mark.parametrize("amount", [-2, 0, 2])
@pytest.mark.parametrize("uncertainty", [2, 3])
def test_inert39_disposal_keeps_signed_uncertainty_components_and_collection(row, amount, uncertainty):
    data = consumer(row["source"], amount, uncertainty)
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
    assert len(records) == 3 and records[0]["exchanges"][-1] == emission
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
    assert len(inputs) == 1 + len(row["freight_witnesses"])
    assert len(row["freight_witnesses"]) == 9
    for actual_input, expected in zip(inputs, row["profiles"][0]["inputs"], strict=True):
        assert all(actual_input[k] == v for k, v in expected.items() if k != "recipe")
    treatment = next(
        r for r in records if r.get("brightpath_conversion", {}).get("recipe_id") == row["treatment_recipe"]
    )
    components = [x for x in treatment["exchanges"] if x["type"] != "production"]
    expected = REVIEW["auxiliary_recipes"][row["treatment_recipe"]]["inputs"]
    assert len(components) == len(expected) == 2
    assert all(all(x[k] == v for k, v in e.items()) for x, e in zip(components, expected, strict=True))
    assert sorted((x["unit"], x["amount"]) for x in components) == [("kilogram", 1), ("unit", 1.48e-9)]
    assert not any(x["type"] == "biosphere" for r in records[1:] for x in r["exchanges"])
    freights = [x for x in inputs if x["unit"] == "ton kilometer"]
    assert len(freights) == len(row["freight_witnesses"])
    assert all(x["name"] in ("Transport, freight, rail", "Transport, freight, lorry, fleet average") for x in freights)
    assert not any(i.code == "migration.recipe_input_omission" for i in result.last_migration_report.issues)


@pytest.mark.parametrize("row", REVIEW["records"], ids=lambda r: r["rule_id"])
@pytest.mark.parametrize("version", ["3.6", "3.10"])
def test_inert39_helpers_refuse_uninspected_source_versions(row, version):
    source = BrightwayInventory.from_data(consumer(row["source"], -1), context=contexts("ecoinvent", version))
    before = deepcopy(source.data)
    with pytest.raises(MigrationError):
        source.migrate_background(target_context("2026"), policy=MigrationPolicy.permissive())
    assert source.data == before


@pytest.mark.parametrize("change", ["none", "late_parent", "helper", "component_binding", "retained_recipe"])
def test_inert39_authoring_replay_validates_every_parent_before_mutating(change):
    resource = deepcopy(load_uvek_technosphere_resource())
    if change == "late_parent":
        next(r for r in resource["replace"] if r["id"] == REVIEW["records"][-1]["rule_id"])["confidence"] = 0.123
    elif change == "helper":
        resource["recipes"][next(iter(REVIEW["auxiliary_recipes"]))]["inputs"][0]["amount"] += 1
    elif change == "component_binding":
        source = REVIEW["records"][0]["profiles"][0]["bindings"][0]["source"]
        fields = ("name", "reference product", "location", "unit")
        next(r for r in resource["replace"] if [r["source"][f] for f in fields] == source)["confidence"] = 0.123
    if change == "retained_recipe":
        resource["recipes"][REVIEW["retained_312"]["recipe_id"]]["inputs"][0]["amount"] += 1
    before = deepcopy(resource)
    if change == "none":
        apply_pilot_nitrate_inert39_review(resource)
        assert resource == before
    else:
        with pytest.raises(ValueError):
            apply_pilot_nitrate_inert39_review(resource)
        assert resource == before


def test_inert39_retains_the_312_recipe_and_exact_selected_profile_entry():
    resource = load_uvek_technosphere_resource()
    retained = REVIEW["retained_312"]
    rule = next(r for r in resource["replace"] if r["id"] == REVIEW["records"][0]["rule_id"])
    assert rule["target"] == retained["target"]
    assert rule["mapping_method"] == REVIEW["mapping_method_retained"]
    assert rule["reference_flow_sign_review"]["profiles"][0] == retained["previous_selected_profile_entry"]
    assert (
        hashlib.sha256(json.dumps(resource["recipes"][retained["recipe_id"]], sort_keys=True).encode()).hexdigest()
        == retained["recipe_sha256"]
    )
    assert len(REVIEW["records"][0]["freight_witnesses"]) == 9
    for witness in REVIEW["authorization_evidence"]:
        assert hashlib.sha256((DATA_DIR / witness["path"]).read_bytes()).hexdigest() == witness["sha256"]
