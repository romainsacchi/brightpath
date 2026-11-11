"""Native heat/recycling proxies distinguish source-zero and native-zero allocations."""

import json
from copy import deepcopy
from dataclasses import replace

import pytest
from test_uvek_reference_signs import consumer, contexts
from test_uvek_refrigerant_disposal import target_context

from brightpath import DATA_DIR, BrightwayInventory
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource
from scripts.review_uvek_fuels_39_heat_recycling import REVIEW_PATH, apply_fuels_39_heat_recycling_review, digest

REVIEW = json.loads((DATA_DIR / "export/uvek_fuels_39_heat_recycling_review.json").read_text())
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
    assert any(
        issue.code in {"migration.technosphere_information_loss", "migration.zero_burden_supplier"}
        for issue in result.last_migration_report.issues
    )


@pytest.mark.parametrize("rid", [rid for rid, row in RECORDS.items() if row["resource_action"] != "unchanged_rule"])
@pytest.mark.parametrize("version,model", [("3.10", "cutoff"), ("3.9", "consequential")])
def test_unreviewed_profiles_are_not_inferred(rid, version, model):
    with pytest.raises(MigrationError):
        migrate(rid, version=version, model=model)


@pytest.mark.parametrize("rid", RECORDS)
def test_strict_target_and_existing_zero_gas_helper(rid):
    result = migrate(rid, strict=True)
    row = RECORDS[rid]
    edge = result.data[0]["exchanges"][1]
    if row["resource_action"] == "unchanged_rule":
        ds = helper(result)
        assert ds["reference product"] == "biogas" and ds["unit"] == "cubic meter"
        assert (
            len(ds["exchanges"]) == 1
            and ds["exchanges"][0]["type"] == "production"
            and ds["exchanges"][0]["amount"] == 1
        )
        assert row["source_witness"]["nonproduction_counts"] == {}
        assert row["compiled_rule"] == row["original_rule"]
    else:
        assert all(edge[k] == v for k, v in row["new_target"].items())
        assert len(result.data) == 1


def test_secondary_aluminium_is_metal_output_not_waste_or_primary_production_mix():
    row = RECORDS["bp-f6f59e6dec650096"]
    assert row["source_witness"]["reference_amounts"] == [1]
    assert row["new_target"]["name"] == "Aluminium, secondary, from old scrap, at plant"
    assert row["compiled_rule"]["conversion_factor"] == 1
    assert "1.0089799212995663" in row["limitations"][0] and "1.118" in row["limitations"][0]


def test_native_heat_proxy_and_observed_empty_source_stock_are_explicitly_different():
    evidence = REVIEW["waste_heat_source_market"]
    assert evidence["supplier_rows"] == 58 and evidence["empty_inventory_suppliers"] == 55
    assert len(evidence["nonempty_suppliers"]) == 3
    assert evidence["nonempty_supplier_share"] == pytest.approx(0.06587691540191035)
    assert evidence["source_lci"]["source_LCI_is_zero"] is False
    (native,) = [w for w in REVIEW["native_2026_witnesses"] if w["nonproduction_count"] == 0]
    assert native["identity"]["name"] == "Heat from waste, at municipal waste incineration plant"
    basis = native["allocation_basis"]
    assert basis["original_total_exchange_count"] == 1 and basis["allocation_percent"] == {"disposal": 100, "energy": 0}
    gas = RECORDS["bp-126729272c9c1747"]
    assert gas["source_witness"]["nonproduction_counts"] == {}
    assert gas["counterpart_witness"]["nonproduction_counts"] == {"tech": 4, "bio": 2}
    assert gas["counterpart_witness"]["reference_amount"] == -1


def test_heat_markets_use_regional_native_mix_without_boiler_or_coverage_scaling():
    for rid in ("bp-a6429485fc43a215", "bp-ae076e1077dfff2c"):
        row = RECORDS[rid]
        assert row["new_target"]["location"] == "ENTSO-E" and row["new_target"]["unit"] == "megajoule"
        assert row["compiled_rule"]["conversion_factor"] == 1
    row = RECORDS["bp-35c7a09fc71ff9db"]
    assert row["compiled_rule"]["conversion_factor"] == -1 and row["new_target"]["unit"] == "cubic meter"
    assert ".42534" in row["limitations"][0] and ".1556" in row["limitations"][0]


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


def test_replay_keeps_existing_gas_rule_all_profiles_and_three_recipes_identical():
    resource = minimal_resource()
    gas = deepcopy(next(r for r in resource["replace"] if r["id"] == "bp-126729272c9c1747"))
    apply_fuels_39_heat_recycling_review(resource)
    before = deepcopy(resource)
    apply_fuels_39_heat_recycling_review(resource)
    assert resource == before and next(r for r in resource["replace"] if r["id"] == gas["id"]) == gas
    assert len(REVIEW["recipe_dependencies"]) == 3
    assert all(digest(resource["recipes"][rid]) == sha for rid, sha in REVIEW["recipe_dependencies"].items())
    assert REVIEW["recipes"] == {}


@pytest.mark.parametrize("change", ["parent", "dependency", "shared_review", "evidence"])
def test_drift_refuses_before_mutation(tmp_path, change):
    resource = minimal_resource()
    options = {}
    if change == "parent":
        resource["replace"][0]["confidence"] = -1
    elif change == "dependency":
        next(iter(resource["recipes"].values()))["rationale"] += " changed"
    elif change == "shared_review":
        apply_fuels_39_heat_recycling_review(resource)
        from brightpath.migrations.schema import intern_review

        resource["reviews"][intern_review({}, REVIEW["reviews"][0])]["rationale"] += " changed"
    else:
        path = tmp_path / REVIEW_PATH
        path.parent.mkdir(parents=True)
        path.write_text("{}")
        options["root"] = tmp_path
    before = deepcopy(resource)
    with pytest.raises(ValueError):
        apply_fuels_39_heat_recycling_review(resource, **options)
    assert resource == before
