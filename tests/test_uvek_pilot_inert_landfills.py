"""Bare landfill reconstruction keeps operation, infrastructure and caller signs."""

from copy import deepcopy
import json

import pytest
from test_uvek_reference_signs import consumer, contexts
from test_uvek_refrigerant_disposal import target_context

from brightpath import BrightwayInventory, DATA_DIR
from brightpath.core.policies import MigrationPolicy
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource
from scripts.review_uvek_pilot_inert_landfills import apply_pilot_inert_landfill_review

REVIEW = json.loads((DATA_DIR / "export/uvek_pilot_inert_landfill_review.json").read_text())


@pytest.mark.parametrize("row", REVIEW["records"], ids=lambda r: r["rule_id"])
@pytest.mark.parametrize("release", ("2025", "2026"))
@pytest.mark.parametrize("amount", (-2, 0, 2))
def test_bare_landfill_keeps_complete_components_and_separate_emissions(row, release, amount):
    data = consumer(row["source"], amount, 2)
    emission = {
        "name": "Carbon dioxide, fossil",
        "categories": ["air"],
        "unit": "kilogram",
        "type": "biosphere",
        "amount": 7,
    }
    # Both contexts retain the same 3.10 biosphere.
    data[0]["exchanges"].append(emission)
    before = deepcopy(data)
    context = contexts("ecoinvent", "3.6")
    target = target_context(release)
    inventory = BrightwayInventory.from_data(data, context=context)
    result = inventory.migrate_background(target, policy=MigrationPolicy.permissive())
    assert inventory.data == data == before
    edge = result.data[0]["exchanges"][1]
    original = data[0]["exchanges"][1]
    assert edge["amount"] == -amount and edge["formula"] == "(dose) * (-1.0)"
    assert edge["loc"] == original["loc"] and edge["scale"] == original["scale"]
    assert edge["negative"] is not original["negative"]
    assert result.data[0]["exchanges"][-1] == emission
    (helper,) = result.data[1:]
    assert edge["input"][1] == helper["code"]
    assert helper["reference product"] == row["source"]["reference product"]
    assert [(x["amount"], x["unit"]) for x in helper["exchanges"] if x["type"] == "production"] == [(1, "kilogram")]
    tech = [x for x in helper["exchanges"] if x["type"] == "technosphere"]
    assert {x["name"]: x["amount"] for x in tech} == {
        "Process-specific burdens, inert material landfill": 1,
        "xx Inert material landfill facility": 1.48e-9,
    }
    assert not any(x["type"] == "biosphere" for x in helper["exchanges"])
    assert any("retain that source-data gap" in i.message for i in result.last_migration_report.issues)


@pytest.mark.parametrize("version", ("3.7", "3.12"))
@pytest.mark.parametrize("row", REVIEW["records"], ids=lambda r: r["rule_id"])
def test_uninspected_source_profiles_cannot_use_the_helpers(version, row):
    inventory = BrightwayInventory.from_data(consumer(row["source"], -1), context=contexts("ecoinvent", version))
    before = deepcopy(inventory.data)
    with pytest.raises(MigrationError):
        inventory.migrate_background(target_context("2026"), policy=MigrationPolicy.permissive())
    assert inventory.data == before


@pytest.mark.parametrize("change", ("component", "parent", "helper"))
def test_changed_component_or_parent_or_helper_refuses_before_mutation(change):
    resource = deepcopy(load_uvek_technosphere_resource())
    if change == "component":
        next(r for r in resource["replace"] if r["id"] == "bp-64a47e8b46bc3ac7")["confidence"] = 0.123
    elif change == "parent":
        next(r for r in resource["replace"] if r["id"] == REVIEW["records"][-1]["rule_id"])["source"][
            "location"
        ] = "unreviewed"
    else:
        parent = next(r for r in resource["replace"] if r["id"] == REVIEW["records"][-1]["rule_id"])
        resource["recipes"][parent["target"]["recipe_ref"]]["inputs"][0]["amount"] = 999
    before = deepcopy(resource)
    with pytest.raises(ValueError, match="changed"):
        apply_pilot_inert_landfill_review(resource)
    assert resource == before


def test_inspected_helpers_replay_idempotently():
    resource = deepcopy(load_uvek_technosphere_resource())
    expected = deepcopy(resource)
    for row in REVIEW["records"]:
        parent = next(r for r in resource["replace"] if r["id"] == row["rule_id"])
        parent.clear()
        parent.update(deepcopy(row["previous_rule"]))
    apply_pilot_inert_landfill_review(resource)
    assert resource == expected
    apply_pilot_inert_landfill_review(resource)
    assert resource == expected
