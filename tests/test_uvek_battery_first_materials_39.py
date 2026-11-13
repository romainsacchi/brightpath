"""Reviewed material bases preserve signed kg and retain market delivery."""

import json
from copy import deepcopy

import pytest
from test_uvek_reference_signs import consumer, contexts

from brightpath import DATA_DIR, BrightwayInventory, TechnosphereProfile
from brightpath.background.rule_approvals import compatibility_rule_fingerprint
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource
from scripts.review_uvek_battery_first_materials_39 import apply_battery_first_materials_39_review

REVIEW = json.loads((DATA_DIR / "export/uvek_battery_first_materials_39_review.json").read_text())
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
def test_five_signed_materials_preserve_uncertainty_and_delivery(amount, uncertainty, release):
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
    assert len(migrated.data) == 3
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
            assert actual["unit"] == helper["unit"] == "kilogram"
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
def test_all_five_rules_require_their_independent_permission(row):
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(
            consumer(row["original_rule"]["source"], 1), context=contexts("ecoinvent", "3.9")
        ).migrate_background(contexts("uvek", "2026").background, policy=policy(rows=[]))


def test_market_bases_and_named_production_limitations_remain_explicit():
    rows = {r["rule_id"]: r for r in REVIEW["records"]}
    hf = rows["bp-0bbfeb07da304c0c"]
    tce = rows["bp-d3f9737d70620971"]
    assert [x["amount"] for x in hf["market_interfaces"] if x["kind"] == "producer"] == [1]
    assert sorted(x["amount"] for x in hf["market_interfaces"] if x["kind"] == "freight") == [0.0131, 0.0514]
    shares = [x["amount"] for x in tce["market_interfaces"] if x["kind"] == "producer"]
    assert sorted(shares) == [0.1581818181818183, 0.8418181818181817] and sum(shares) == 1
    assert sorted(x["amount"] for x in tce["market_interfaces"] if x["kind"] == "freight") == [
        0.0246,
        0.2088,
        0.3091,
        0.599,
    ]
    sea = next(x for x in tce["market_interfaces"] if x["kind"] == "freight" and "ship" in x["native_target"]["name"])
    assert sea["native_target"]["name"] == "Transport, transoceanic container ship"
    hf_limits = " ".join(hf["limitations"])
    assert all(
        x in hf_limits
        for x in ["73602-61-6", "7664-39-3", "Native assay is not independently established", "unquantified"]
    )
    assert "lacks main-process infrastructure and land-use" in " ".join(tce["limitations"])
    for row in rows.values():
        assert row["source_witness"]["reference_amounts"] == [1]
        assert all(n["reference_amounts"] == [1] for n in row["native_witnesses"].values())
        assert row["conversion_factor"] == 1
    assert REVIEW["new_source_omissions"] == []
    assert "1.5 kg" in " ".join(rows["bp-7a19b8bce18972c5"]["limitations"])
    assert "limestone where source lists lime" in " ".join(rows["bp-91dd24e733bdeb2c"]["limitations"])
    assert "Do not scale caller kg" in " ".join(rows["bp-62c56bf9797d2742"]["limitations"])


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
            apply_battery_first_materials_39_review(resource)
    else:
        apply_battery_first_materials_39_review(resource)
    assert resource == before
