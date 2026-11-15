"""Chemical markets retain delivery and explicit functional-reference limitations."""

import json
from copy import deepcopy

import pytest
from test_uvek_reference_signs import consumer, contexts

from brightpath import DATA_DIR, BrightwayInventory, TechnosphereProfile
from brightpath.background.rule_approvals import compatibility_rule_fingerprint
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource

REVIEW = json.loads((DATA_DIR / "export/uvek_metals_38_chemical_markets_review.json").read_text())


def policy():
    resource = load_uvek_technosphere_resource()
    return MigrationPolicy(
        approved_compatibility_rules=tuple(
            compatibility_rule_fingerprint(
                resource,
                "replace",
                row["compiled_rule"],
                TechnosphereProfile(**REVIEW["source_profile"]),
                TechnosphereProfile(**REVIEW["target_rule_profile"]),
            )
            for row in REVIEW["records"]
        ),
        on_information_loss=PolicyAction.WARN,
    )


@pytest.mark.parametrize("release", ["2025", "2026"])
@pytest.mark.parametrize("amount", [-2.5, 2.5])
@pytest.mark.parametrize("uncertainty", [2, 3])
def test_five_markets_keep_signed_callers_delivery_and_separate_emissions(release, amount, uncertainty):
    data = []
    for i, row in enumerate(REVIEW["records"]):
        a = consumer(row["original_rule"]["source"], amount, uncertainty)[0]
        a["name"] += str(i)
        a["exchanges"][0]["name"] = a["name"]
        a["exchanges"].append(
            {
                "type": "biosphere",
                "name": "Carbon dioxide, fossil",
                "categories": ["air"],
                "unit": "kilogram",
                "amount": 0.5,
            }
        )
        data.append(a)
    before = deepcopy(data)
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.8")).migrate_background(
        contexts("uvek", release).background, policy=policy()
    )
    assert data == before and len(result.data) == 9
    helpers = {a["code"]: a for a in result.data[5:]}
    assert len(helpers) == 4
    for original, output, row in zip(before, result.data[:5], REVIEW["records"], strict=True):
        edge = output["exchanges"][1]
        for key in ("amount", "uncertainty type", "loc", "scale", "negative", "formula", "minimum", "maximum"):
            if key in original["exchanges"][1]:
                assert edge[key] == original["exchanges"][1][key]
        assert output["exchanges"][-1] == original["exchanges"][-1]
        if row["recipe_id"]:
            helper = helpers[edge["input"][1]]
            assert helper["unit"] == row["original_rule"]["source"]["unit"] == "kilogram"
            (production,) = [e for e in helper["exchanges"] if e["type"] == "production"]
            assert production["amount"] == 1
            inputs = [e for e in helper["exchanges"] if e["type"] != "production"]
            assert len(inputs) == len(REVIEW["recipes"][row["recipe_id"]]["inputs"])
            assert (
                sum(e["unit"] == "ton kilometer" for e in inputs) == row["boundary_checks"]["freight_entries_retained"]
            )
            for actual, expected in zip(inputs, REVIEW["recipes"][row["recipe_id"]]["inputs"], strict=True):
                assert all(actual[k] == v for k, v in expected.items())
        else:
            assert all(edge[k] == v for k, v in row["compiled_rule"]["target"].items())
            assert "Sodium hydroxide" in edge["name"] and edge["amount"] == amount


def test_basis_ambiguity_and_neutralizer_portfolio_change_remain_explicit():
    rows = {r["kind"]: r for r in REVIEW["records"]}
    soda = rows["light_soda_implemented_reference_basis"]
    assert soda["boundary_checks"]["source_own_neutralizer_kg_soda_per_kg_NaOH_equivalent"] == 1.325
    assert not soda["boundary_checks"]["physical_heptahydrate_equivalence_verified"]
    assert not soda["boundary_checks"]["hydration_multiplier_or_water_added"]
    neutralizer = rows["NaOH_equivalent_neutralizer"]
    assert neutralizer["boundary_checks"]["source_reagent_portfolio_replaced"]
    assert not neutralizer["boundary_checks"]["water_or_50_percent_multiplier"]
    assert not neutralizer["boundary_checks"]["same_chemical_portfolio_claimed"]
    assert all(r["conversion_factor"] == 1 for r in REVIEW["records"])
    assert all(r["source_witness"]["source_database"] == "ecoinvent-3.8-cutoff" for r in REVIEW["records"])
    assert REVIEW["new_helpers"] == 4 and REVIEW["new_physical_omissions"] == 0
    assert REVIEW["new_standalone_freight_scopes"] == 0


def test_nested_freight_review_does_not_authorize_standalone_container_freight():
    row = next(r for r in REVIEW["freight_correspondences"] if r["rule_id"] == "bp-55c88a17f0ffb64f")
    data = consumer(row["source"], 1)
    before = deepcopy(data)
    with pytest.raises(MigrationError, match=row["rule_id"]):
        BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.8")).migrate_background(
            contexts("uvek", "2026").background, policy=policy()
        )
    assert data == before


def test_replay_is_atomic_idempotent_and_binds_active_chemical_evidence(tmp_path):
    from scripts.review_uvek_metals_38_chemical_markets import apply_metals_38_chemical_markets_review

    resource = deepcopy(load_uvek_technosphere_resource())
    before = deepcopy(resource)
    apply_metals_38_chemical_markets_review(resource)
    assert resource == before
    for row in REVIEW["records"]:
        rule = next(r for r in resource["replace"] if r["id"] == row["rule_id"])
        rule.clear()
        rule.update(deepcopy(row["original_rule"]))
    apply_metals_38_chemical_markets_review(resource)
    assert resource == before
    row = REVIEW["records"][-1]
    rule = next(r for r in resource["replace"] if r["id"] == row["rule_id"])
    rule["confidence"] = 0.1
    changed = deepcopy(resource)
    with pytest.raises(ValueError):
        apply_metals_38_chemical_markets_review(resource)
    assert resource == changed
    evidence_dir = tmp_path / "brightpath/data/export"
    evidence_dir.mkdir(parents=True)
    (evidence_dir / "uvek_metals_38_chemical_markets_review.json").write_bytes(
        (DATA_DIR / "export/uvek_metals_38_chemical_markets_review.json").read_bytes()
    )
    (evidence_dir / "uvek_pilot_active_chemical_review.json").write_text("changed basis")
    with pytest.raises(ValueError, match="active-chemical basis changed"):
        apply_metals_38_chemical_markets_review(resource, root=tmp_path)
    assert resource == changed
