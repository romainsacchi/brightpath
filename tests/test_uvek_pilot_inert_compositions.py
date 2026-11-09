"""Exact inert-waste helpers retain operation, assets, collection and signs."""

from copy import deepcopy
from dataclasses import replace
import json

import pytest

from brightpath import BrightwayInventory, DATA_DIR
from brightpath.core.policies import MigrationPolicy
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource
from scripts.review_uvek_pilot_inert_compositions import apply_pilot_inert_composition_review
from test_uvek_reference_signs import consumer, contexts
from test_uvek_refrigerant_disposal import target_context

REVIEW = json.loads((DATA_DIR / "export/uvek_pilot_inert_composition_review.json").read_text())


@pytest.mark.parametrize("row", REVIEW["records"], ids=lambda r: r["rule_id"])
@pytest.mark.parametrize("amount", [-2, 0, 2])
def test_inert_composition_preserves_signed_uncertainty_and_all_components(row, amount):
    data = consumer(row["source"], amount, 2)
    emission = {
        "name": "Carbon dioxide, fossil",
        "categories": ["air"],
        "unit": "kilogram",
        "type": "biosphere",
        "amount": 7,
    }
    data[0]["exchanges"].append(emission)
    before = deepcopy(data)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", row["source_profile"]["version"]))
    result = inventory.migrate_background(target_context("2026"), policy=MigrationPolicy.permissive())
    assert inventory.data == data == before
    edge = result.data[0]["exchanges"][1]
    original = before[0]["exchanges"][1]
    assert edge["amount"] == -amount and edge["formula"] == "(dose) * (-1.0)"
    assert edge["loc"] == original["loc"] and edge["scale"] == original["scale"]
    assert edge["negative"] is not original["negative"]
    assert result.data[0]["exchanges"][-1] == emission
    (helper,) = result.data[1:]
    assert edge["input"][1] == helper["code"]
    assert [(e["amount"], e["unit"]) for e in helper["exchanges"] if e["type"] == "production"] == [(1, "kilogram")]
    actual = [e for e in helper["exchanges"] if e["type"] != "production"]
    expected = row["profiles"][0]["inputs"]
    assert len(actual) == len(expected)
    for exchange, source in zip(actual, expected, strict=True):
        assert all(
            exchange[k] == v
            for k, v in source.items()
            if k not in ("approved_omission_if_unavailable", "omission_rationale")
        )
    roads = [e for e in actual if e["unit"] == "ton kilometer"]
    if row["source_profile"]["version"] == "3.12":
        assert len(roads) == 3
        assert sum(e["amount"] for e in roads) == pytest.approx(0.077)
        (indicator,) = [e for e in helper["exchanges"] if e["type"] == "biosphere"]
        assert indicator["name"] == "Non-hazardous waste disposed" and indicator["amount"] == 1
    else:
        assert roads == []
        assert not any(e["type"] == "biosphere" for e in helper["exchanges"])
    assert not any(i.code == "migration.recipe_input_omission" for i in result.last_migration_report.issues)
    assert any("retain that source-data gap" in i.message for i in result.last_migration_report.issues)


@pytest.mark.parametrize("row", REVIEW["records"], ids=lambda r: r["rule_id"])
def test_inert_composition_does_not_approve_other_source_versions(row):
    version = "3.12" if row["source_profile"]["version"] == "3.9" else "3.9"
    inventory = BrightwayInventory.from_data(consumer(row["source"], -1), context=contexts("ecoinvent", version))
    before = deepcopy(inventory.data)
    with pytest.raises(MigrationError):
        inventory.migrate_background(target_context("2026"), policy=MigrationPolicy.permissive())
    assert inventory.data == before


def test_only_unavailable_nonphysical_indicator_is_omitted_with_an_audit():
    from brightpath.background import PackageCatalogProvider

    class MissingIndicator(PackageCatalogProvider):
        def load_biosphere(self, profile):
            catalog = super().load_biosphere(profile)
            identities = frozenset(x for x in catalog.identities if x[0] != "Non-hazardous waste disposed")
            return replace(
                catalog,
                identities=identities,
                references={k: v for k, v in catalog.references.items() if k in identities},
                digest="",
            )

    row = REVIEW["records"][1]
    inventory = BrightwayInventory.from_data(consumer(row["source"], -2), context=contexts("ecoinvent", "3.12"))
    before = deepcopy(inventory.data)
    result = inventory.migrate_background(
        target_context("2026"), policy=MigrationPolicy.permissive(), catalog_provider=MissingIndicator()
    )
    (helper,) = result.data[1:]
    assert len([e for e in helper["exchanges"] if e["type"] == "technosphere"]) == 5
    assert not any(e["type"] == "biosphere" for e in helper["exchanges"])
    (omission,) = [i for i in result.last_migration_report.issues if i.code == "migration.recipe_input_omission"]
    assert omission.details["source_exchange"]["name"] == "Non-hazardous waste disposed"
    assert omission.details["source_exchange"]["amount"] == 1
    assert inventory.data == before


def test_inert_composition_replay_changes_only_two_parents_and_is_idempotent():
    resource = deepcopy(load_uvek_technosphere_resource())
    expected = deepcopy(resource)
    by_id = {r["id"]: r for r in resource["replace"]}
    for row in REVIEW["records"]:
        parent = by_id[row["rule_id"]]
        parent.clear()
        parent.update(deepcopy(row["previous_rule"]))
    apply_pilot_inert_composition_review(resource)
    assert resource == expected
    apply_pilot_inert_composition_review(resource)
    assert resource == expected


@pytest.mark.parametrize("change", ["component", "parent", "recipe"])
def test_inert_composition_binding_drift_refuses_atomically(change):
    resource = deepcopy(load_uvek_technosphere_resource())
    row = REVIEW["records"][-1]
    parent = next(r for r in resource["replace"] if r["id"] == row["rule_id"])
    if change == "component":
        next(r for r in resource["replace"] if r["id"] == "bp-f1b06903c98b44af")["confidence"] = 0.123
    elif change == "parent":
        parent["source"]["location"] = "unreviewed"
    else:
        resource["recipes"][parent["target"]["recipe_ref"]]["inputs"][0]["amount"] = 999
    before = deepcopy(resource)
    with pytest.raises(ValueError, match="changed"):
        apply_pilot_inert_composition_review(resource)
    assert resource == before
