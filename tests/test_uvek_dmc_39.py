"""DMC retains source manufacturing and delivery through reviewed native dependencies."""

import json
from copy import deepcopy

import pytest
from test_uvek_reference_signs import consumer, contexts
from uvek_review_helpers import compiled_authoring_stage_resource

from brightpath import DATA_DIR, BrightwayInventory, TechnosphereProfile
from brightpath.background.rule_approvals import compatibility_rule_fingerprint
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource

REVIEW = json.loads((DATA_DIR / "export/uvek_dmc_39_review.json").read_text())
ROW = REVIEW["records"][0]


def policy(approved=True):
    resource = load_uvek_technosphere_resource()
    rule = next(r for r in resource["replace"] if r["id"] == ROW["rule_id"])
    fp = compatibility_rule_fingerprint(
        resource,
        "replace",
        rule,
        TechnosphereProfile("ecoinvent", "3.9", "cutoff"),
        TechnosphereProfile("uvek", "2025", "cutoff"),
    )
    return MigrationPolicy(
        approved_compatibility_rules=(fp,) if approved else (), on_information_loss=PolicyAction.WARN
    )


@pytest.mark.parametrize("amount", [-2.5, 2.5])
@pytest.mark.parametrize("uncertainty", [2, 3])
@pytest.mark.parametrize("release", ["2025", "2026"])
def test_signed_DMC_demand_preserves_both_producers_direct_emissions_and_final_delivery(amount, uncertainty, release):
    source = consumer(ROW["original_rule"]["source"], amount, uncertainty)
    source[0]["exchanges"].append(
        {
            "name": "Carbon dioxide, fossil",
            "categories": ["air"],
            "unit": "kilogram",
            "type": "biosphere",
            "amount": 0.5,
        }
    )
    before = deepcopy(source)
    result = BrightwayInventory.from_data(source, context=contexts("ecoinvent", "3.9")).migrate_background(
        contexts("uvek", release).background, policy=policy()
    )
    assert source == before and len(result.data) == 30
    edge = result.data[0]["exchanges"][1]
    assert edge["amount"] == amount and edge["formula"] == "dose"
    assert edge["scale"] == 0.2 and edge["uncertainty type"] == uncertainty
    if uncertainty == 2:
        assert edge["negative"] is (amount < 0)
    else:
        assert edge["loc"] == amount
    assert result.data[0]["exchanges"][-1] == before[0]["exchanges"][-1]
    nodes = {a.get("brightpath_conversion", {}).get("recipe_id"): a for a in result.data[1:]}
    market = nodes[ROW["recipe_id"]]
    assert market["reference product"] == "dimethyl carbonate"
    rows = [e for e in market["exchanges"] if e["type"] != "production"]
    assert len(rows) == 6 and [e["amount"] for e in rows[:2]] == REVIEW["source_market_producer_shares"]
    assert len([e for e in rows if e["unit"] == "ton kilometer"]) == 4
    for proof in REVIEW["source_production_reconstruction"]:
        recipe_id = proof["recipe_id"]
        actual = [e for e in nodes[recipe_id]["exchanges"] if e["type"] != "production"]
        expected = REVIEW["recipes"][recipe_id]["inputs"]
        assert len(actual) == len(expected) == proof["retained_nonproduction_count"]
        assert len([e for e in actual if e["type"] == "biosphere"]) == 15
        for got, want in zip(actual, expected, strict=True):
            for field in (
                "amount",
                "uncertainty type",
                "loc",
                "scale",
                "negative",
                "minimum",
                "maximum",
                "pedigree",
                "scale without pedigree",
                "formula",
            ):
                if field in want:
                    assert got[field] == want[field]
            if want.get("type") == "biosphere":
                assert all(got[k] == want[k] for k in ["name", "categories", "unit"])
        water = [e for e in actual if e["type"] == "biosphere" and e["name"] == "Ethylene carbonate"]
        assert len(water) == 1 and water[0]["categories"] == ["water"] and water[0]["amount"] == 0.040322691488694044
        assert all(e["amount"] > 0 for e in actual if e["type"] == "technosphere")


@pytest.mark.parametrize(
    "version,model,approved",
    [
        ("3.9", "cutoff", False),
        ("3.8", "cutoff", True),
        ("3.9.1", "cutoff", True),
        ("3.10", "cutoff", True),
        ("3.9", "consequential", True),
    ],
)
def test_DMC_permission_is_exact_and_does_not_use_another_carbonate(version, model, approved):
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(
            consumer(ROW["original_rule"]["source"], 1), context=contexts("ecoinvent", version, model)
        ).migrate_background(contexts("uvek", "2026").background, policy=policy(approved))


def test_frozen_dependencies_and_same_CAS_biosphere_alias_keep_the_reviewed_boundaries():
    resource = load_uvek_technosphere_resource()
    assert len(REVIEW["recipe_dependencies"]) == 26
    assert len(REVIEW["source_rule_dependency_scopes"]) == 34
    assert all(resource["recipes"][k] == v for k, v in REVIEW["recipe_dependencies"].items())
    assert ROW["boundary_checks"]["source_producer_nonproduction_counts"] == [25, 37]
    assert ROW["boundary_checks"]["native_lithium_carbonate_used_as_DMC"] is False
    assert ROW["boundary_checks"]["new_reaction_balance_yield_or_credit"] is False
    alias = REVIEW["canonical_biosphere_alias"]
    assert alias["source"]["CAS number"] == alias["target"]["CAS number"] == "000096-49-1"
    assert alias["source"]["categories"] == alias["target"]["categories"] == ["water"]
    assert alias["source"]["unit"] == alias["target"]["unit"] == "kilogram"
    assert REVIEW["inherited_literal_recipe_omission_count"] == 0 and REVIEW["new_source_omissions"] == []


@pytest.mark.parametrize("drift", [None, "parent", "source_dependency", "old_recipe", "new_recipe"])
def test_replay_is_atomic_and_refuses_changed_input_or_recipe_boundaries(drift):
    from scripts.review_uvek_dmc_39 import apply_dmc_39_review

    resource = compiled_authoring_stage_resource("review_uvek_dmc_39", "apply_dmc_39_review")
    if drift == "parent":
        next(r for r in resource["replace"] if r["id"] == ROW["rule_id"])["conversion_factor"] = 2
    elif drift == "source_dependency":
        next(r for r in resource["replace"] if r["id"] == REVIEW["source_rule_dependency_scopes"][0]["rule_id"])[
            "confidence"
        ] = 0.01
    elif drift == "old_recipe":
        resource["recipes"][next(iter(REVIEW["recipe_dependencies"]))]["inputs"][0]["amount"] *= 2
    elif drift == "new_recipe":
        resource["recipes"][ROW["recipe_id"]]["inputs"][0]["amount"] *= 2
    before = deepcopy(resource)
    if drift:
        with pytest.raises(ValueError):
            apply_dmc_39_review(resource)
    else:
        apply_dmc_39_review(resource)
    assert resource == before
