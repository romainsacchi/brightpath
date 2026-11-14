"""Component transport boundaries and purchased C2F6 retain correct reference quantities."""

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

REVIEW = json.loads((DATA_DIR / "export/uvek_components_fgas_39_review.json").read_text())
ROWS = REVIEW["records"]


def policy(approved=True):
    resource = load_uvek_technosphere_resource()
    fps = tuple(
        compatibility_rule_fingerprint(
            resource,
            "replace",
            next(r for r in resource["replace"] if r["id"] == row["rule_id"]),
            TechnosphereProfile("ecoinvent", "3.9", "cutoff"),
            TechnosphereProfile("uvek", "2025", "cutoff"),
        )
        for row in ROWS
    )
    return MigrationPolicy(approved_compatibility_rules=fps if approved else (), on_information_loss=PolicyAction.WARN)


@pytest.mark.parametrize("row", ROWS, ids=lambda r: r["rule_id"])
@pytest.mark.parametrize("amount", [-2.5, 2.5])
@pytest.mark.parametrize("uncertainty", [2, 3])
@pytest.mark.parametrize("release", ["2025", "2026"])
def test_correct_product_and_signed_caller_statistics_with_market_delivery(row, amount, uncertainty, release):
    data = consumer(row["original_rule"]["source"], amount, uncertainty)
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
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.9")).migrate_background(
        contexts("uvek", release).background, policy=policy()
    )
    assert data == before and len(result.data) == (2 if row["recipe_id"] else 1)
    edge = result.data[0]["exchanges"][1]
    expected = deepcopy(before[0]["exchanges"][1])
    scale_exchange(expected, 1)
    for k in ("amount", "uncertainty type", "loc", "scale", "negative", "minimum", "maximum", "formula"):
        if k in expected:
            assert edge[k] == expected[k]
    assert result.data[0]["exchanges"][-1] == before[0]["exchanges"][-1]
    if row["recipe_id"]:
        helper = next(a for a in result.data if a.get("code") == edge["input"][1])
        actual = [e for e in helper["exchanges"] if e["type"] != "production"]
        recipe = REVIEW["recipes"][row["recipe_id"]]
        assert len(actual) == row["boundary_checks"]["source_market_entries"]
        for actual_edge, source in zip(actual, recipe["inputs"], strict=True):
            for k in (
                "name",
                "reference product",
                "location",
                "unit",
                "amount",
                "uncertainty type",
                "loc",
                "scale",
                "scale without pedigree",
                "negative",
                "pedigree",
                "formula",
            ):
                if k in source:
                    assert actual_edge[k] == source[k]
        assert (
            len([e for e in actual if e["unit"] == "ton kilometer"]) == row["boundary_checks"]["final_delivery_entries"]
        )
        stocks = [e for e in actual if e["unit"] == "kilogram"]
        assert all(e["name"] == row["boundary_checks"]["correct_native_product"] for e in stocks)
    else:
        assert edge["name"] == row["boundary_checks"]["correct_native_product"]
        assert edge["unit"] == "kilogram"


@pytest.mark.parametrize("row", ROWS, ids=lambda r: r["rule_id"])
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
def test_exact_permissions_do_not_expand_to_unreviewed_profiles(row, version, model, approved):
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(
            consumer(row["original_rule"]["source"], 1), context=contexts("ecoinvent", version, model)
        ).migrate_background(contexts("uvek", "2026").background, policy=policy(approved))


def test_foil_transport_lineage_and_component_reference_bases_are_explicit():
    resource = load_uvek_technosphere_resource()
    foil = ROWS[0]
    assert (
        next(r for r in resource["replace"] if r["id"] == foil["rule_id"])
        == foil["original_rule"]
        == foil["compiled_rule"]
    )
    assert foil["recipe_id"] is None and foil["boundary_checks"]["foil_whole_supply_proxy"]
    assert REVIEW["foil_transport_lineage"]["native_five_tkm_per_kg"] == [0.0096, 0.0153, 0.0623, 0.31, 1.112]
    assert REVIEW["foil_transport_lineage"]["source39_four_tkm_per_kg"] == [0.0096, 0.0153, 0.31, 1.112]
    assert REVIEW["foil_transport_lineage"]["stage_attribution_is_inference"]
    assert REVIEW["retained_market_producer_entries"] == 4
    assert REVIEW["retained_market_freight_entries"] == 8
    assert sum(len(r["inputs"]) for r in REVIEW["recipes"].values()) == 12
    assert all(row["boundary_checks"]["ribbon_device_factor_added"] is False for row in ROWS)
    assert all(row["boundary_checks"]["passive_rounded_basket_renormalized"] is False for row in ROWS)
    assert ROWS[3]["boundary_checks"]["correct_native_product"] == "Hexafluorethane, at plant"
    assert ROWS[3]["boundary_checks"]["C2F6_source_route_replaced_fraction"] == 0.5
    assert [e["amount"] for e in REVIEW["recipes"][ROWS[3]["recipe_id"]]["inputs"][:2]] == [0.5, 0.5]
    assert REVIEW["new_source_omissions"] == []


@pytest.mark.parametrize("drift", [None, "parent", "dependency", "new_recipe"])
def test_review_replay_is_atomic_and_checks_exact_freight_dependencies(drift):
    from scripts.review_uvek_components_fgas_39 import apply_components_fgas_39_review

    resource = deepcopy(load_uvek_technosphere_resource())
    if drift == "parent":
        next(r for r in resource["replace"] if r["id"] == ROWS[0]["rule_id"])["conversion_factor"] = 2
    elif drift == "dependency":
        next(r for r in resource["replace"] if r["id"] == REVIEW["source_rule_dependency_scopes"][0]["rule_id"])[
            "confidence"
        ] = 0.01
    elif drift == "new_recipe":
        recipe_id = next(row["recipe_id"] for row in ROWS if row["recipe_id"])
        resource["recipes"][recipe_id]["inputs"][0]["amount"] *= 2
    before = deepcopy(resource)
    if drift:
        with pytest.raises(ValueError):
            apply_components_fgas_39_review(resource)
    else:
        apply_components_fgas_39_review(resource)
    assert resource == before
