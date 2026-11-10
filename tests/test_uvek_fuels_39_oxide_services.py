"""Oxide delivery and native services retain exact reference signs and prior scopes."""

import json
from copy import deepcopy
from dataclasses import replace

import pytest
from test_uvek_reference_signs import consumer, contexts
from test_uvek_refrigerant_disposal import target_context

from brightpath import DATA_DIR, BrightwayInventory, TechnosphereProfile
from brightpath.background.reference_signs import select_profile_sign_rules
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource
from scripts.review_uvek_fuels_39_oxide_services import REVIEW_PATH, apply_fuels_39_oxide_services_review, digest

REVIEW = json.loads((DATA_DIR / "export/uvek_fuels_39_oxide_services_review.json").read_text())
RECORDS = {r["rule_id"]: r for r in REVIEW["records"]}


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
    assert any(loss.code == "migration.technosphere_information_loss" for loss in result.last_migration_report.losses)


@pytest.mark.parametrize("rid", RECORDS)
@pytest.mark.parametrize("version,model", [("3.10", "cutoff"), ("3.9", "consequential")])
def test_unreviewed_profiles_are_not_inferred(rid, version, model):
    with pytest.raises(MigrationError):
        migrate(rid, version=version, model=model)


@pytest.mark.parametrize("rid", RECORDS)
def test_strict_target_and_complete_oxide_delivery(rid):
    result = migrate(rid, strict=True)
    row = RECORDS[rid]
    if "new_recipe_id" not in row:
        edge = result.data[0]["exchanges"][1]
        assert all(edge[k] == v for k, v in row["new_target"].items())
        assert len(result.data) == 1
        return
    helpers = {a.get("brightpath_conversion", {}).get("recipe_id"): a for a in result.data[1:]}
    assert set(helpers) == set(row["new_recipe_ids"])
    total = 0
    for recipe_id in row["new_recipe_ids"]:
        recipe = REVIEW["recipes"][recipe_id]
        ds = helpers[recipe_id]
        inputs = [e for e in ds["exchanges"] if e["type"] != "production"]
        total += len(inputs)
        assert len(inputs) == len(recipe["inputs"])
        assert ds.get("parameters", []) == recipe.get("parameters", [])
        for actual, expected in zip(inputs, recipe["inputs"], strict=True):
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
                if key in expected:
                    assert actual[key] == expected[key]
    assert total == row["retention"]["all_market_import_rows"]
    assert not any(
        loss.code == "migration.recipe_source_input_omission" for loss in result.last_migration_report.losses
    )


def test_alumina_keeps_import_and_market_shipping_at_their_distinct_stages():
    row = RECORDS["bp-57c9160454525453"]
    root = REVIEW["recipes"][row["new_recipe_id"]]
    assert len(root["inputs"]) == 8 and len(row["new_recipe_ids"]) == 4
    assert [e["amount"] for e in root["inputs"] if e.get("name") == "Transport, transoceanic container ship"] == [0.2]
    imports = []
    for edge in root["inputs"]:
        if "recipe" in edge:
            child = REVIEW["recipes"][edge["recipe"]]
            (ocean,) = [e for e in child["inputs"] if e.get("name") == "Transport, transoceanic container ship"]
            imports.append((edge["amount"], ocean["amount"]))
    assert sorted(imports) == sorted([(0.012869604586062, 6.5755), (0.02144934097677, 0.2), (0.394667873972568, 7.5)])
    assert len(REVIEW["oxide_input_identity_checks"]) == 35
    assert all(
        e["same_quantity_and_unit_as_prior_binding"] and e["current_statistics_retained"]
        for e in REVIEW["oxide_input_identity_checks"]
    )


def test_wet_waste_basis_uses_water_in_wet_mass_and_exposes_composition_proxy():
    pp = RECORDS["bp-fa607ec8232396e4"]["source_wet_mass_properties"]
    assert pp["wet mass"]["amount"] == 1 and pp["dry mass"]["amount"] == 0.841
    assert pp["water in wet mass"]["amount"] == 0.159
    assert pp["water content"]["amount"] == pytest.approx(0.159 / 0.841)
    residue = RECORDS["bp-eb7ad12f2fa498ed"]
    assert residue["source_wet_mass_properties"]["water in wet mass"]["amount"] == 0.005
    assert "50000 ppm" in residue["limitations"][0] and "0.5%" in residue["limitations"][0]
    assert "per kg wet waste" in residue["limitations"][0]
    assert residue["compiled_rule"]["conversion_factor"] == -1


def test_all_previous_selected_rules_reviews_and_six_delivery_recipes_are_unchanged():
    resource = load_uvek_technosphere_resource()
    count = 0
    for row in REVIEW["records"]:
        for proof in row["preserved_profiles"]:
            profile = TechnosphereProfile(**proof["source_profile"])
            selected = next(
                r for r in select_profile_sign_rules(resource, profile)["replace"] if r["id"] == row["rule_id"]
            )
            previous = select_profile_sign_rules({"replace": [row["original_rule"]]}, profile)["replace"][0]
            assert selected == previous and digest(selected) == proof["selected_rule_sha256"]
            assert digest(resource["reviews"][selected["review_ref"]]) == proof["shared_review_sha256"]
            count += 1
    assert count == 4
    assert len(REVIEW["recipe_dependencies"]) == 6
    assert all(digest(resource["recipes"][rid]) == expected for rid, expected in REVIEW["recipe_dependencies"].items())


def minimal_resource():
    full = load_uvek_technosphere_resource()
    originals = {r["rule_id"]: r["original_rule"] for r in REVIEW["records"]}
    refs = {r.get("review_ref") for r in originals.values()}
    return {
        "replace": [deepcopy(originals[r["id"]]) for r in full["replace"] if r["id"] in originals],
        "delete": [],
        "recipes": {k: deepcopy(full["recipes"][k]) for k in REVIEW["recipe_dependencies"]},
        "reviews": {k: deepcopy(full["reviews"][k]) for k in refs if k is not None},
        "coverage": {},
    }


def test_replay_is_idempotent_and_introduces_no_omissions_or_zero_stock():
    resource = minimal_resource()
    apply_fuels_39_oxide_services_review(resource)
    before = deepcopy(resource)
    apply_fuels_39_oxide_services_review(resource)
    assert resource == before
    assert not any(r.get("zero_burden") or r.get("source_input_omissions") for r in REVIEW["recipes"].values())


@pytest.mark.parametrize("change", ["parent", "dependency", "recipe", "shared_review", "evidence"])
def test_drift_refuses_before_mutation(tmp_path, change):
    resource = minimal_resource()
    options = {}
    if change == "parent":
        resource["replace"][0]["confidence"] = -1
    elif change == "dependency":
        next(iter(resource["recipes"].values()))["inputs"][0]["amount"] *= 2
    elif change in ("recipe", "shared_review"):
        apply_fuels_39_oxide_services_review(resource)
        if change == "recipe":
            next(resource["recipes"][rid] for rid in REVIEW["recipes"])["inputs"][0]["amount"] *= 2
        else:
            from brightpath.migrations.schema import intern_review

            resource["reviews"][intern_review({}, REVIEW["reviews"][0])]["rationale"] += " changed"
    else:
        path = tmp_path / REVIEW_PATH
        path.parent.mkdir(parents=True)
        path.write_text("{}")
        options["root"] = tmp_path
    before = deepcopy(resource)
    with pytest.raises(ValueError):
        apply_fuels_39_oxide_services_review(resource, **options)
    assert resource == before
