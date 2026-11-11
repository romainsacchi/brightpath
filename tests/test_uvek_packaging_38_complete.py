"""Packaging services retain packed-product quantities and every disposal fate."""

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
from scripts.review_uvek_packaging_38_complete import apply_packaging_38_complete_review

REVIEW = json.loads((DATA_DIR / "export/uvek_packaging_38_complete_review.json").read_text())


@pytest.mark.parametrize("row", REVIEW["records"], ids=lambda row: row["rule_id"])
@pytest.mark.parametrize("amount", [-2.5, 0, 2.5])
@pytest.mark.parametrize("uncertainty", [2, 3])
def test_packaging_preserves_signed_caller_and_complete_owned_graph(row, amount, uncertainty):
    source = BrightwayInventory.from_data(
        consumer(row["original_rule"]["source"], amount, uncertainty), context=contexts("ecoinvent", "3.8", "cutoff")
    )
    before = deepcopy(source.data)
    policy = replace(
        MigrationPolicy.permissive(),
        on_invalid_source=PolicyAction.ERROR,
        on_invalid_target=PolicyAction.ERROR,
        on_unresolved_link=PolicyAction.ERROR,
        minimum_coverage=1,
    )
    result = source.migrate_background(target_context("2026"), policy=policy)
    assert source.data == before
    edge = result.data[0]["exchanges"][1]
    original = before[0]["exchanges"][1]
    for field in ("amount", "formula", "loc", "scale", "negative", "minimum", "maximum"):
        if field in original:
            assert edge[field] == original[field]
    helpers = {a.get("brightpath_conversion", {}).get("recipe_id"): a for a in result.data[1:]}
    resource = load_uvek_technosphere_resource()
    reachable = set()

    def walk(key):
        if key in reachable:
            return
        reachable.add(key)
        for entry in resource["recipes"][key]["inputs"]:
            if "recipe" in entry:
                walk(entry["recipe"])

    walk(row["root_recipe_id"])
    assert set(helpers) == reachable
    assert len(result.data) == len(reachable) + 1
    for key in reachable:
        entries = [e for e in helpers[key]["exchanges"] if e["type"] != "production"]
        expected = resource["recipes"][key]["inputs"]
        assert len(entries) == len(expected)
        for actual, reference in zip(entries, expected, strict=True):
            for field in (
                "amount",
                "uncertainty type",
                "loc",
                "scale",
                "shape",
                "negative",
                "minimum",
                "maximum",
                "formula",
            ):
                if field in reference:
                    assert actual[field] == reference[field]
            if reference.get("type") == "biosphere":
                assert actual["name"] == reference["name"] and tuple(actual["categories"]) == tuple(
                    reference["categories"]
                )
    factory = helpers[row["factory_recipe_id"]]
    factory_inputs = [e for e in factory["exchanges"] if e["type"] != "production"]
    assert len(factory_inputs) == 10
    assert (
        len(
            [
                e
                for e in factory_inputs
                if e.get("name", "").startswith("market for waste polyethylene")
                or e.get("name", "").startswith("market group for waste polyethylene")
            ]
        )
        == 8
    )
    root_inputs = [e for e in helpers[row["root_recipe_id"]]["exchanges"] if e["type"] != "production"]
    assert len([e for e in root_inputs if e["unit"] == "ton kilometer"]) == 3
    losses = [i for i in result.last_migration_report.losses if i.code == "migration.recipe_source_input_omission"]
    assert len(losses) == 4
    assert all(not resource["recipes"][key].get("zero_burden") for key in reachable)


@pytest.mark.parametrize("row", REVIEW["records"], ids=lambda row: row["rule_id"])
@pytest.mark.parametrize("version", ["3.7", "3.9", "3.12"])
def test_packaging_does_not_infer_other_source_release_approval(row, version):
    selected = select_profile_sign_rules(
        {"replace": [row["compiled_rule"]]}, TechnosphereProfile("ecoinvent", version, "cutoff")
    )["replace"][0]
    assert selected["reference_flow_sign_review"]["status"] == "unresolved"
    source = BrightwayInventory.from_data(
        consumer(row["original_rule"]["source"], 1), context=contexts("ecoinvent", version, "cutoff")
    )
    with pytest.raises(MigrationError):
        source.migrate_background(target_context("2026"), policy=MigrationPolicy.permissive())


@pytest.mark.parametrize("change", ["none", "late_parent", "dependency", "owned_recipe"])
def test_packaging_authoring_is_atomic_and_freezes_all_dependencies(change):
    resource = deepcopy(load_uvek_technosphere_resource())
    if change == "late_parent":
        next(r for r in resource["replace"] if r["id"] == REVIEW["records"][-1]["rule_id"])["confidence"] = 0.123
    elif change == "dependency":
        resource["recipes"][next(iter(REVIEW["recipe_dependencies"]))]["inputs"][0]["amount"] *= 2
    elif change == "owned_recipe":
        resource["recipes"][REVIEW["records"][0]["factory_recipe_id"]]["inputs"][0]["amount"] *= 2
    before = deepcopy(resource)
    if change == "none":
        apply_packaging_38_complete_review(resource)
        assert resource == before
    else:
        with pytest.raises(ValueError):
            apply_packaging_38_complete_review(resource)
        assert resource == before


def test_packaging_basis_models_and_constant_formula_boundaries():
    assert REVIEW["forming_basis"]["stretch_blow_moulding"]["source_and_native_output_kg_per_process_kg"] == 0.978
    assert REVIEW["forming_basis"]["injection_moulding"]["source_and_native_output_kg_per_process_kg"] == 0.994
    assert REVIEW["closure_counts"]["source_waste_markets"] == 45
    assert REVIEW["closure_counts"]["source_terminal_fates"] == 14
    assert REVIEW["closure_counts"]["public_nonzero_biosphere_coordinates"] == 612
    assert len(REVIEW["constant_formula_proofs"]) == 6
    for proof in REVIEW["constant_formula_proofs"]:
        assert proof["free_symbols"] == [] and proof["source_activity_parameter_count"] == 0
        assert proof["constant_arithmetic_matches_amount"] is True
        assert float(proof["canonical_formula"]) >= 0
    for row in REVIEW["records"]:
        inputs = REVIEW["recipes"][row["factory_recipe_id"]]["inputs"]
        material = [e for e in inputs if e.get("recipe", "").startswith("packaging-hdpe-")]
        forming = [e for e in inputs if e.get("name") in {"Stretch blow moulding", "Injection moulding"}]
        assert len(material) == len(forming) == 1
        expected = 0.00204918032787 if "fertilisers" in row["original_rule"]["source"]["name"] else 0.0583501006036
        assert material[0]["amount"] == forming[0]["amount"] == expected
