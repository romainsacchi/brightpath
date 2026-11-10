"""Granular carbon retains reactivation, direct emissions and allocated ash fates."""

import json
from collections import Counter
from copy import deepcopy
from dataclasses import replace

import pytest
from test_uvek_reference_signs import consumer, contexts
from test_uvek_refrigerant_disposal import target_context

from brightpath import DATA_DIR, BrightwayInventory
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource
from scripts.review_uvek_fuels_39_carbon import REVIEW_PATH, apply_fuels_39_carbon_review, digest

REVIEW = json.loads((DATA_DIR / "export/uvek_fuels_39_carbon_review.json").read_text())
RECORDS = {r["rule_id"]: r for r in REVIEW["records"]}
MARKET = "bp-dd3e07b2dd28af80"
ASH = {"CH": "bp-daed85adc1fa3b47", "RoW": "bp-016903afcf2e9b33"}
PRODUCERS = [rid for rid in RECORDS if rid not in {MARKET, *ASH.values()}]


def migrate(rid, amount=1, uncertainty=3, version="3.9", model="cutoff", strict=False):
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


def recipe_dataset(result, rid):
    recipe = REVIEW["recipes"][RECORDS[rid]["new_recipe_id"]]
    return next(
        a
        for a in result.data
        if a["name"].startswith(recipe["name"] + " [BrightPath UVEK conversion ")
        and a["location"] == recipe["location"]
    )


@pytest.mark.parametrize("rid", RECORDS)
@pytest.mark.parametrize("amount", [-2, 2])
@pytest.mark.parametrize("uncertainty", [2, 3])
def test_reference_sign_caller_uncertainty_and_formula(rid, amount, uncertainty):
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
    assert any(loss.code == "migration.technosphere_information_loss" for loss in result.last_migration_report.losses)


@pytest.mark.parametrize("rid", RECORDS)
@pytest.mark.parametrize("version,model", [("3.10", "cutoff"), ("3.9", "consequential")])
def test_unreviewed_profiles_are_not_inferred(rid, version, model):
    with pytest.raises(MigrationError):
        migrate(rid, version=version, model=model)


def test_full_market_keeps_four_producers_288_biosphere_rows_and_12_parameters():
    result = migrate(MARKET, strict=True)
    losses = [
        loss for loss in result.last_migration_report.losses if loss.code == "migration.recipe_source_input_omission"
    ]
    assert len(losses) == 51
    market = recipe_dataset(result, MARKET)
    inputs = [e for e in market["exchanges"] if e["type"] == "technosphere"]
    assert len(inputs) == 8
    assert [e["amount"] for e in inputs[:4]] == [
        0.21153356,
        0.226133755582338,
        0.10446644,
        0.457866244417662,
    ]
    assert sum(inputs[i]["amount"] for i in (1, 3)) == pytest.approx(0.684)
    assert [e["amount"] for e in inputs[4:]] == [0.088, 0.011, 0.3552, 0.4816]
    counts = Counter()
    for rid in PRODUCERS:
        ds = recipe_dataset(result, rid)
        assert [e["amount"] for e in ds["exchanges"] if e["type"] == "production"] == [1]
        rows = [e for e in ds["exchanges"] if e["type"] != "production"]
        bio = [e for e in rows if e["type"] == "biosphere"]
        assert len(bio) == 72 and len(ds["parameters"]) == 3
        counts.update(e.get("uncertainty type") for e in rows)
        expected = REVIEW["recipes"][RECORDS[rid]["new_recipe_id"]]
        assert ds["parameters"] == expected["parameters"]
        for actual, original in zip(rows, expected["inputs"], strict=True):
            for key in (
                "amount",
                "uncertainty type",
                "loc",
                "scale",
                "shape",
                "negative",
                "minimum",
                "maximum",
                "pedigree",
                "scale without pedigree",
                "formula",
            ):
                if key in original:
                    assert actual[key] == original[key]
        ash = [e for e in rows if e.get("reference product") == "treatment of hard coal ash"]
        assert len(ash) == 1 and ash[0]["amount"] > 0 and ash[0]["negative"] is False
        if "reactivation" in ds["name"]:
            stock = [e for e in rows if "spent activated carbon" in e["name"]]
            assert len(stock) == 1 and stock[0]["amount"] == pytest.approx(1.1111111111111112)
        else:
            coal = [e for e in rows if e["name"].startswith("Hard coal")]
            assert sum(e["amount"] for e in coal) == pytest.approx(3)
            assert all("regional storage" in e["name"] for e in coal)
    assert counts == {2: 331, 0: 2}
    checks = REVIEW["biosphere_identity_checks"]
    assert len(checks) == 288 and sum(e["source_name"] != e["target_name"] for e in checks) == 24
    assert all(e["same_publisher_uuid"] and e["signed_quantity_retained"] for e in checks)
    assert not any(
        "Carbon black" in a["name"] or "Carbon fiber" in a["name"] or "with mercury" in a["name"] for a in result.data
    )


@pytest.mark.parametrize(
    "location,disposal,freight,omitted",
    [
        ("CH", 0.5300932129612254, 2, 5),
        ("RoW", 0.7883589051349253, 9, 46),
    ],
)
def test_ash_services_keep_fates_without_renormalizing_empty_receivers(location, disposal, freight, omitted):
    result = migrate(ASH[location], amount=-1, strict=True)
    ds = recipe_dataset(result, ASH[location])
    assert ds["reference product"] == "treatment of hard coal ash"
    rows = [e for e in ds["exchanges"] if e["type"] == "technosphere"]
    fates = [e for e in rows if e["unit"] == "kilogram"]
    assert sum(e["amount"] for e in fates) == pytest.approx(disposal)
    assert all(e["amount"] > 0 for e in rows)
    retention = RECORDS[ASH[location]]["retention"]
    assert retention["empty_receivers_omitted"] == omitted and retention["freight_rows"] == freight
    audit = [o for o in REVIEW["empty_receiver_omissions"] if o["market_identity"]["location"] == location]
    assert len(audit) == omitted
    declared = REVIEW["recipes"][RECORDS[ASH[location]]["new_recipe_id"]]["source_input_omissions"]
    assert ds["brightpath_conversion"]["source_input_omissions"] == declared
    assert (
        len(
            [
                loss
                for loss in result.last_migration_report.losses
                if loss.code == "migration.recipe_source_input_omission"
            ]
        )
        == omitted
    )
    assert sum(-o["signed_source_quantity"] for o in audit) + disposal == pytest.approx(1)
    assert all(o["source_price"] == 0 and o["nonproduction_count"] == 0 for o in audit)
    if location == "RoW":
        assert len(fates) == 15
        assert sum(e["amount"] for e in fates if "residual material landfill" in e["name"]) == pytest.approx(
            0.779874559507374
        )
        assert sum(e["amount"] for e in fates if "sanitary landfill" in e["name"]) == pytest.approx(
            0.006408693991497805
        )


def minimal_resource():
    full = load_uvek_technosphere_resource()
    originals = {row["rule_id"]: row["original_rule"] for row in REVIEW["records"]}
    ids = set(originals) | {r["rule_id"] for r in REVIEW["component_bindings"]}
    return {
        "replace": [deepcopy(originals.get(r["id"], r)) for r in full["replace"] if r["id"] in ids],
        "delete": [],
        "recipes": {k: deepcopy(full["recipes"][k]) for k in REVIEW["recipe_dependencies"]},
        "reviews": {},
        "coverage": {},
    }


def test_replay_preserves_prior_profiles_and_complete_existing_stock_heat_and_rail():
    resource = minimal_resource()
    apply_fuels_39_carbon_review(resource)
    before = deepcopy(resource)
    apply_fuels_39_carbon_review(resource)
    assert resource == before
    assert all(digest(resource["recipes"][rid]) == sha for rid, sha in REVIEW["recipe_dependencies"].items())
    assert not any(r.get("zero_burden") for r in REVIEW["recipes"].values())
    original = RECORDS[ASH["RoW"]]["original_rule"]["reference_flow_sign_review"]["profiles"]
    compiled = RECORDS[ASH["RoW"]]["compiled_rule"]["reference_flow_sign_review"]["profiles"]
    assert [p for p in compiled if p["source_profile"]["version"] != "3.9"] == [
        p for p in original if p["source_profile"]["version"] != "3.9"
    ]


@pytest.mark.parametrize("change", ["parent", "component", "dependency", "recipe", "shared_review", "evidence"])
def test_drift_refuses_before_mutation(tmp_path, change):
    resource = minimal_resource()
    options = {}
    if change == "parent":
        next(r for r in resource["replace"] if r["id"] in RECORDS)["confidence"] = -1
    elif change == "component":
        next(r for r in resource["replace"] if r["id"] not in RECORDS)["confidence"] = -1
    elif change == "dependency":
        next(r for r in resource["recipes"].values() if r["inputs"])["inputs"][0]["amount"] *= 2
    elif change in ("recipe", "shared_review"):
        apply_fuels_39_carbon_review(resource)
        if change == "recipe":
            resource["recipes"][RECORDS[MARKET]["new_recipe_id"]]["inputs"][0]["amount"] *= 2
        else:
            next(iter(resource["reviews"].values()))["rationale"] += " changed"
    else:
        path = tmp_path / REVIEW_PATH
        path.parent.mkdir(parents=True)
        path.write_text("{}")
        options["root"] = tmp_path
    before = deepcopy(resource)
    with pytest.raises(ValueError):
        apply_fuels_39_carbon_review(resource, **options)
    assert resource == before
