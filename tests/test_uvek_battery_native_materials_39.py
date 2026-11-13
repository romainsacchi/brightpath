"""Reviewed native materials retain kg, metre and equipment-unit bases."""

import json
from copy import deepcopy

import pytest
from test_uvek_reference_signs import consumer, contexts

from brightpath import DATA_DIR, BrightwayInventory, TechnosphereProfile
from brightpath.background.rule_approvals import compatibility_rule_fingerprint
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource
from scripts.review_uvek_battery_native_materials_39 import apply_battery_native_materials_39_review

REVIEW = json.loads((DATA_DIR / "export/uvek_battery_native_materials_39_review.json").read_text())
FIELDS = ("name", "reference product", "location", "unit")
STATS = ("amount", "uncertainty type", "loc", "scale", "shape", "minimum", "maximum", "negative", "formula")


def policy(rows=None):
    resource = load_uvek_technosphere_resource()
    rules = {r["id"]: r for r in resource["replace"]}
    return MigrationPolicy(
        approved_compatibility_rules=tuple(
            compatibility_rule_fingerprint(
                resource,
                "replace",
                rules[row["rule_id"]],
                TechnosphereProfile("ecoinvent", "3.9", "cutoff"),
                TechnosphereProfile("uvek", "2025", "cutoff"),
            )
            for row in (REVIEW["records"] if rows is None else rows)
        ),
        on_information_loss=PolicyAction.WARN,
    )


@pytest.mark.parametrize("amount", [-2.5, 2.5])
@pytest.mark.parametrize("uncertainty", [2, 3])
@pytest.mark.parametrize("release", ["2025", "2026"])
def test_seven_signed_materials_preserve_uncertainty_and_delivery(amount, uncertainty, release):
    rows = REVIEW["records"]
    data = consumer(rows[0]["original_rule"]["source"], amount, uncertainty)
    for row in rows[1:]:
        data[0]["exchanges"].append(consumer(row["original_rule"]["source"], amount, uncertainty)[0]["exchanges"][1])
    data[0]["exchanges"].append(
        {
            "name": "Carbon dioxide, fossil",
            "categories": ["air"],
            "unit": "kilogram",
            "type": "biosphere",
            "amount": 0.5,
        }
    )
    before = deepcopy(data)
    migrated = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.9")).migrate_background(
        contexts("uvek", release).background, policy=policy()
    )
    assert data == before
    assert len(migrated.data) == 7
    assert migrated.data[0]["exchanges"][-1]["amount"] == 0.5
    helpers = {a["brightpath_conversion"]["recipe_id"]: a for a in migrated.data[1:]}
    assert set(helpers) == set(REVIEW["recipes"])
    for i, row in enumerate(rows, 1):
        actual = migrated.data[0]["exchanges"][i]
        original = before[0]["exchanges"][i]
        for k in STATS:
            if k in original:
                assert actual[k] == original[k]
        if row["recipe_id"]:
            helper = helpers[row["recipe_id"]]
            assert helper["reference product"] == row["source_witness"]["identity"]["reference product"]
            assert actual["unit"] == helper["unit"] == row["original_rule"]["source"]["unit"]
            prod = [e for e in helper["exchanges"] if e["type"] == "production"]
            assert len(prod) == 1 and prod[0]["amount"] == 1
            edges = [e for e in helper["exchanges"] if e["type"] == "technosphere"]
            expected = REVIEW["recipes"][row["recipe_id"]]["inputs"]
            assert len(edges) == len(expected)
            for a, b in zip(edges, expected, strict=True):
                for k in (*FIELDS, *STATS, "pedigree", "scale without pedigree"):
                    if k in b:
                        assert a[k] == b[k]
        else:
            assert all(actual[k] == v for k, v in row["compiled_rule"]["target"].items())


@pytest.mark.parametrize("row", REVIEW["records"], ids=lambda r: r["rule_id"])
def test_a_different_product_unit_does_not_inherit_the_review(row):
    data = consumer({**row["original_rule"]["source"], "unit": "cubic meter"}, 1)
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.9")).migrate_background(
            contexts("uvek", "2026").background, policy=policy()
        )


@pytest.mark.parametrize(
    "version,model", [("3.8", "cutoff"), ("3.9.1", "cutoff"), ("3.10", "cutoff"), ("3.9", "consequential")]
)
def test_other_profiles_are_not_granted_material_permissions(version, model):
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(
            consumer(REVIEW["records"][0]["original_rule"]["source"], 1), context=contexts("ecoinvent", version, model)
        ).migrate_background(contexts("uvek", "2026").background, policy=policy())


@pytest.mark.parametrize("row", REVIEW["records"], ids=lambda r: r["rule_id"])
def test_all_seven_rules_require_their_independent_permission(row):
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(
            consumer(row["original_rule"]["source"], 1), context=contexts("ecoinvent", "3.9")
        ).migrate_background(contexts("uvek", "2026").background, policy=policy(rows=[]))


def test_product_bases_packaging_and_vehicle_classes_remain_distinct():
    rows = {r["rule_id"]: r for r in REVIEW["records"]}
    lime = rows["bp-43b626507103b4e9"]
    assert [e["amount"] for e in lime["market_interfaces"]] == [1, 0.007513945, 0.001513088, 0.075702611]
    packed = rows["bp-e1e1fd673ebb9025"]
    assert packed["recipe_id"] is None
    assert "0.00141 MJ" in " ".join(packed["limitations"])
    phosphorus = rows["bp-855426f2360906db"]
    assert "PCl5" in " ".join(phosphorus["limitations"])
    assert [e["amount"] for e in phosphorus["market_interfaces"][:2]] == [0.0493720466375044, 0.950627953362496]
    cable = rows["bp-f0e4cb1e4047a1ca"]
    assert cable["source_witness"]["identity"]["unit"] == "meter"
    light = [
        e
        for e in cable["market_interfaces"]
        if e["target"]["name"] == "Transport, freight, lorry, 3.5t gross weight, fleet average"
    ]
    assert len(light) == 1 and light[0]["amount"] == 0.001106
    inverter = rows["bp-8be404e7d5db36be"]
    assert inverter["source_witness"]["identity"]["unit"] == "unit"
    assert len(inverter["market_interfaces"]) == 2
    assert all(e["kind"] == "producer" for e in inverter["market_interfaces"])
    assert sum(e["amount"] for e in inverter["market_interfaces"]) == 1
    assert len(rows["bp-a03bddee5eee2616"]["market_interfaces"]) == 11
    assert len(rows["bp-312bd6bf4beef9a9"]["market_interfaces"]) == 4
    assert all("coal freight" not in e["target"]["name"] for row in rows.values() for e in row["market_interfaces"])
    assert all(row["conversion_factor"] == 1 for row in rows.values())
    assert REVIEW["new_source_omissions"] == []


@pytest.mark.parametrize("drift", [None, "first", "last", "recipe"])
def test_frozen_replay_is_atomic_and_idempotent(drift):
    resource = deepcopy(load_uvek_technosphere_resource())
    if drift in ("first", "last"):
        row = REVIEW["records"][0 if drift == "first" else -1]
        next(r for r in resource["replace"] if r["id"] == row["rule_id"])["confidence"] = 0.01
    elif drift == "recipe":
        resource["recipes"][next(iter(REVIEW["recipes"]))]["inputs"][0]["amount"] *= 2
    before = deepcopy(resource)
    if drift:
        with pytest.raises(ValueError):
            apply_battery_native_materials_39_review(resource)
    else:
        apply_battery_native_materials_39_review(resource)
    assert resource == before
