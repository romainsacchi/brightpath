"""Residue markets retain waste chemistry, allocation interfaces and audited zero losses."""

import json
from copy import deepcopy

import pytest
from test_uvek_reference_signs import consumer, contexts

from brightpath import DATA_DIR, BrightwayInventory, TechnosphereProfile
from brightpath.background.rule_approvals import compatibility_rule_fingerprint
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource
from scripts.review_uvek_pulp_heat_limestone import apply_pulp_heat_limestone_review

REVIEW = json.loads((DATA_DIR / "export/uvek_pulp_heat_limestone_review.json").read_text())


def policy():
    resource = load_uvek_technosphere_resource()
    rules = {r["id"]: r for r in resource["replace"]}
    scopes = tuple(
        compatibility_rule_fingerprint(
            resource,
            "replace",
            rules[row["rule_id"]],
            TechnosphereProfile("ecoinvent", "3.9", "cutoff"),
            TechnosphereProfile("uvek", "2025", "cutoff"),
        )
        for row in REVIEW["records"]
    )
    return MigrationPolicy(approved_compatibility_rules=scopes, on_information_loss=PolicyAction.WARN)


def graph(amount, uncertainty=0):
    data = consumer(REVIEW["records"][0]["source_witness"]["identity"], amount, uncertainty)
    data[0]["exchanges"] = data[0]["exchanges"][:1] + [
        consumer(r["source_witness"]["identity"], amount, uncertainty)[0]["exchanges"][1] for r in REVIEW["records"]
    ]
    return data


@pytest.mark.parametrize("amount", [-2, 2])
@pytest.mark.parametrize("uncertainty", [2, 3])
def test_all_exact_profiles_preserve_signs_uncertainty_helpers_and_source(amount, uncertainty):
    data = graph(amount, uncertainty)
    before = deepcopy(data)
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.9")).migrate_background(
        contexts("uvek", "2026").background, policy=policy()
    )
    assert data == before and len(result.data) == 3
    for row, edge in zip(REVIEW["records"], result.data[0]["exchanges"][1:], strict=True):
        factor = row["conversion_factor"]
        assert edge["amount"] == amount * factor and edge["scale"] == 0.2
        assert edge["formula"] == ("dose" if factor == 1 else "(dose) * (-1.0)")
        if uncertainty == 2:
            assert edge["negative"] is (edge["amount"] < 0)
        else:
            assert edge["loc"] == amount * factor
            assert edge["minimum"] == min((amount - 1) * factor, (amount + 1) * factor)
            assert edge["maximum"] == max((amount - 1) * factor, (amount + 1) * factor)
    helpers = {a.get("brightpath_conversion", {}).get("recipe_id"): a for a in result.data[1:]}
    assert set(helpers) == set(REVIEW["recipes"])
    for key, recipe in REVIEW["recipes"].items():
        actual = [e for e in helpers[key]["exchanges"] if e["type"] != "production"]
        assert len(actual) == len(recipe["inputs"])
        for a, b in zip(actual, recipe["inputs"], strict=True):
            for field in (
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
                if field in b:
                    assert a[field] == b[field]
            if b.get("type") == "biosphere":
                assert (a["name"], tuple(a["categories"]), a["unit"]) == (b["name"], tuple(b["categories"]), b["unit"])
        assert helpers[key].get("brightpath_conversion", {}).get("source_input_omissions", []) == recipe.get(
            "source_input_omissions", []
        )
    omissions = [i for i in result.last_migration_report.issues if i.code == "migration.recipe_source_input_omission"]
    assert len(omissions) == 2


@pytest.mark.parametrize("version, model", [("3.6", "cutoff"), ("3.12", "cutoff"), ("3.9", "consequential")])
def test_other_contexts_cannot_inherit_allocation_approvals(version, model):
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(graph(1), context=contexts("ecoinvent", version, model)).migrate_background(
            contexts("uvek", "2026").background, policy=policy()
        )


@pytest.mark.parametrize("drift", [None, "first", "last"])
def test_atomic_replay_rejects_parent_drift(drift):
    resource = deepcopy(load_uvek_technosphere_resource())
    if drift:
        row = REVIEW["records"][0 if drift == "first" else -1]
        next(r for r in resource["replace"] if r["id"] == row["rule_id"])["confidence"] = 0.123
    before = deepcopy(resource)
    if drift:
        with pytest.raises(ValueError, match="parent changed"):
            apply_pulp_heat_limestone_review(resource)
    else:
        apply_pulp_heat_limestone_review(resource)
    assert resource == before


def test_market_share_omissions_and_landfill_boundary_are_explicit():
    market = next(r for r in REVIEW["records"] if r["kind"] == "market")
    landfill = next(r for r in REVIEW["records"] if r["kind"] == "landfill")
    recipe = REVIEW["recipes"][market["recipe_id"]]
    assert len(recipe["inputs"]) == 10 and len(market["source_omissions"]) == 2
    assert recipe["inputs"][0]["amount"] == 8.47378205105051e-10
    assert sum(x["source_exchange"]["amount"] for x in market["source_omissions"]) == pytest.approx(-0.999999999152622)
    assert len(recipe["inputs"]) + len(market["source_omissions"]) == market["source_witness"]["nonproduction_count"]
    assert {e["name"] for e in REVIEW["recipes"][landfill["recipe_id"]]["inputs"]} == {
        "xx Inert material landfill facility",
        "Process-specific burdens, inert material landfill",
    }
    assert all(e.get("type") != "biosphere" for recipe in REVIEW["recipes"].values() for e in recipe["inputs"])
    for control in REVIEW["empty_clinker_controls"]:
        assert control["source_witness"]["reference_amounts"] == [-1]
        assert control["source_witness"]["nonproduction_count"] == 0
        assert control["main_clinker_witness"]["reference_amounts"] == [1]
        assert control["main_clinker_witness"]["nonproduction_count"] > 0
    assert sum(p["signed_exact_zero_matrix_checks"] for p in REVIEW["empty_clinker_matrix_proofs"]) == 4


def test_older_market_profiles_and_recipe_closures_are_unchanged():
    row = next(r for r in REVIEW["records"] if r["kind"] == "market")
    old = row["original_rule"]
    new = row["compiled_rule"]
    assert new["target"] == old["target"] and new["conversion_factor"] == old["conversion_factor"]
    for profile in old["reference_flow_sign_review"]["profiles"]:
        if profile["source_profile"]["version"] != "3.9":
            assert profile in new["reference_flow_sign_review"]["profiles"]
    resource = load_uvek_technosphere_resource()
    for key, recipe in REVIEW["recipe_dependencies"].items():
        assert resource["recipes"][key] == recipe


@pytest.mark.parametrize("control", REVIEW["empty_clinker_controls"])
def test_no_standalone_zero_correspondence_is_authorized(control):
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(
            consumer(control["source_witness"]["identity"], -1), context=contexts("ecoinvent", "3.9")
        ).migrate_background(contexts("uvek", "2026").background, policy=policy())


def test_returned_helpers_keep_exact_native_and_nested_supplier_links():
    market = next(r for r in REVIEW["records"] if r["kind"] == "market")
    result = BrightwayInventory.from_data(
        consumer(market["source_witness"]["identity"], -1), context=contexts("ecoinvent", "3.9")
    ).migrate_background(contexts("uvek", "2026").background, policy=policy())
    helpers = {a["brightpath_conversion"]["recipe_id"]: a for a in result.data[1:]}
    fields = ("name", "reference product", "location", "unit")
    for key, recipe in REVIEW["recipes"].items():
        edges = [e for e in helpers[key]["exchanges"] if e["type"] == "technosphere"]
        for actual, expected in zip(edges, recipe["inputs"], strict=True):
            target = helpers[expected["recipe"]] if "recipe" in expected else expected
            assert {k: actual[k] for k in fields} == {k: target[k] for k in fields}
