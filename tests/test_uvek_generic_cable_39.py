"""Cable fabrication and regional waste demand keep their own quantities/signs."""

import json
from copy import deepcopy

import pytest
from test_uvek_reference_signs import consumer, contexts

from brightpath import DATA_DIR, BrightwayInventory, TechnosphereProfile
from brightpath.background.rule_approvals import compatibility_rule_fingerprint
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource
from brightpath.migrations.scaling import scale_exchange
from scripts.review_uvek_generic_cable_39 import apply_generic_cable_39_review

REVIEW = json.loads((DATA_DIR / "export/uvek_generic_cable_39_review.json").read_text())
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
def test_cable_and_negative_reference_markets_preserve_scaled_caller_and_helper_fields(amount, uncertainty, release):
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
    assert data == before and migrated.data[0]["exchanges"][-1]["amount"] == 0.5
    helpers = {a["brightpath_conversion"]["recipe_id"]: a for a in migrated.data[1:]}
    expected = {**REVIEW["recipes"], **REVIEW["recipe_dependencies"]}
    assert set(helpers) == set(expected)
    for i, row in enumerate(rows, 1):
        actual = migrated.data[0]["exchanges"][i]
        scaled = deepcopy(before[0]["exchanges"][i])
        scale_exchange(scaled, row["conversion_factor"])
        for field in STATS:
            if field in scaled:
                assert actual[field] == scaled[field]
        assert actual["unit"] == "kilogram"
    for key, helper in helpers.items():
        production = [e for e in helper["exchanges"] if e["type"] == "production"]
        assert len(production) == 1 and production[0]["amount"] == 1
        edges = [e for e in helper["exchanges"] if e["type"] != "production"]
        assert len(edges) == len(expected[key]["inputs"])
        for a, b in zip(edges, expected[key]["inputs"], strict=True):
            for field in STATS:
                if field in b:
                    assert a[field] == b[field]
    assert all("capacitor" not in a["name"].lower() for a in migrated.data)


def test_materials_water_and_country_routes_are_preserved_without_mass_or_climate_averaging():
    recipes = REVIEW["recipes"]
    production = recipes["cable-39-unspecified-production"]["inputs"]
    assert len(production) == 19
    copper = next(e for e in production if e.get("name") == "Copper, at regional storage")
    assert copper["amount"] == 0.65977935
    assert next(e for e in production if e.get("name") == "Wire drawing, copper")["amount"] == copper["amount"]
    assert next(e for e in production if e.get("name") == "Extrusion, plastic pipes")["amount"] == 0.33840125
    assert next(e for e in production if e.get("name") == "Rolling mill")["amount"] == 1.7695e-12
    water = [e for e in production if e.get("type") == "biosphere"]
    assert sorted((e["amount"], tuple(e["categories"])) for e in water) == [
        (8.8572e-05, ("air",)),
        (0.001363428, ("water",)),
    ]
    assert sum(e["amount"] for e in water) == pytest.approx(1.452 / 1000)
    (key,) = [key for key, w in REVIEW["source_market_witnesses"].items() if w["identity"]["location"] == "IN"]
    india = recipes[key]["inputs"]
    assert [e["amount"] for e in india] == [0.077, 0.284307580310206, 0.2105284767503, 0.505163942939495]
    assert india[1]["recipe"] == "pilot-uncontrolled-source-47f078cd8777b2ffee6b"
    assert all(e["amount"] > 0 for e in india)
    assert len(recipes["cable-39-unspecified-delivered-market"]["inputs"]) == 5
    assert REVIEW["new_source_omissions"] == []
    assert REVIEW["controlled_source_product_identity"]["wet_dry_mass_water_and_net_heating_value_identical"]
    assert not REVIEW["controlled_source_product_identity"]["all_controlled_source_properties_identical"]
    assert len([r for r in REVIEW["uncontrolled_leaf_reviews"] if r.get("new_fly_ash_extraction_proxy")]) == 1


def test_only_the_previous_four_hdpe_omissions_are_carried_forward():
    omissions = REVIEW["approved_recipe_source_omissions"]
    assert list(omissions) == ["pilot-nitrate-hdpe-dfcfe7b3600a00f94f69"]
    rows = omissions[next(iter(omissions))]
    assert len(rows) == 4
    assert sum(o["source_exchange"]["amount"] for o in rows) == pytest.approx(1.173141240030732e-13)
    assert all(o["source_profile"]["version"] == "3.9" and o["source_exchange"]["amount"] > 0 for o in rows)


def test_european_waste_market_alone_does_not_acquire_the_cables_hdpe_omissions():
    row = next(r for r in REVIEW["records"] if r["rule_id"] == "bp-735e189847cfb1be")
    migrated = BrightwayInventory.from_data(
        consumer(row["original_rule"]["source"], -1), context=contexts("ecoinvent", "3.9")
    ).migrate_background(contexts("uvek", "2026").background, policy=policy())
    assert migrated.data[0]["exchanges"][1]["amount"] == 1
    assert all(loss.code != "migration.recipe_source_input_omission" for loss in migrated.last_migration_report.losses)


@pytest.mark.parametrize("row", REVIEW["records"], ids=lambda r: r["rule_id"])
def test_each_source_rule_still_requires_its_own_approval(row):
    remaining = [r for r in REVIEW["records"] if r["rule_id"] != row["rule_id"]]
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(
            consumer(row["original_rule"]["source"], -1), context=contexts("ecoinvent", "3.9")
        ).migrate_background(contexts("uvek", "2026").background, policy=policy(remaining))


@pytest.mark.parametrize(
    "version,model", [("3.8", "cutoff"), ("3.9.1", "cutoff"), ("3.10", "cutoff"), ("3.9", "consequential")]
)
def test_other_profiles_cannot_inherit_cable_permissions(version, model):
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(
            consumer(REVIEW["records"][0]["original_rule"]["source"], 1), context=contexts("ecoinvent", version, model)
        ).migrate_background(contexts("uvek", "2026").background, policy=policy())


@pytest.mark.parametrize("row", REVIEW["records"], ids=lambda r: r["rule_id"])
def test_wrong_units_are_not_reinterpreted_as_kg(row):
    source = {**row["original_rule"]["source"], "unit": "meter"}
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(consumer(source, 1), context=contexts("ecoinvent", "3.9")).migrate_background(
            contexts("uvek", "2026").background, policy=policy()
        )


@pytest.mark.parametrize("drift", [None, "parent", "recipe", "dependency"])
def test_frozen_replay_is_atomic_and_idempotent(drift):
    resource = deepcopy(load_uvek_technosphere_resource())
    if drift == "parent":
        next(r for r in resource["replace"] if r["id"] == REVIEW["records"][-1]["rule_id"])["confidence"] = 0.01
    elif drift in ("recipe", "dependency"):
        key = next(iter(REVIEW["recipes" if drift == "recipe" else "recipe_dependencies"]))
        resource["recipes"][key]["inputs"][0]["amount"] *= 2
    before = deepcopy(resource)
    if drift:
        with pytest.raises(ValueError):
            apply_generic_cable_39_review(resource)
    else:
        apply_generic_cable_39_review(resource)
    assert resource == before
