"""Native endpoint corrections retain voltage, solid-product and treatment bases."""

import json
from copy import deepcopy

import pytest
from test_uvek_reference_signs import consumer, contexts
from test_uvek_refrigerant_disposal import target_context

from brightpath import DATA_DIR, BrightwayInventory
from brightpath.core.policies import MigrationPolicy
from brightpath.exceptions import MigrationError
from scripts.review_uvek_fuels_39_endpoints import REVIEW_PATH, apply_fuels_39_endpoint_review

REVIEW = json.loads((DATA_DIR / "export/uvek_fuels_39_endpoint_review.json").read_text())
RECORDS = {r["rule_id"]: r for r in REVIEW["records"]}


@pytest.mark.parametrize("rid", RECORDS)
@pytest.mark.parametrize("amount", [-2, 2])
@pytest.mark.parametrize("uncertainty", [2, 3])
def test_endpoint_matches_native_service_preserving_signed_uncertainty(rid, amount, uncertainty):
    row = RECORDS[rid]
    data = consumer(row["original_rule"]["source"], amount, uncertainty)
    source = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.9"))
    before = deepcopy(source.data)
    result = source.migrate_background(target_context("2026"), policy=MigrationPolicy.permissive())
    edge = result.data[0]["exchanges"][1]
    factor = row["compiled_rule"]["conversion_factor"]
    assert edge["amount"] == factor * amount
    assert all(edge[k] == v for k, v in row["native_2026_witness"]["identity"].items())
    assert edge["scale"] == 0.2 and edge["uncertainty type"] == uncertainty
    if uncertainty == 3:
        assert edge["loc"] == factor * amount
        assert edge["minimum"] == amount - 1 if factor == 1 else edge["minimum"] == -amount - 1
        assert edge["maximum"] == amount + 1 if factor == 1 else edge["maximum"] == -amount + 1
    else:
        assert edge["negative"] is (factor * amount < 0)
    assert edge["formula"] == ("dose" if factor == 1 else "(dose) * (-1.0)")
    assert len(result.data) == 1  # Complete native supplier; no invented helper/credit.
    assert source.data == before
    assert any(loss.code == "migration.technosphere_information_loss" for loss in result.last_migration_report.losses)


@pytest.mark.parametrize("rid", RECORDS)
@pytest.mark.parametrize("version,model", [("3.10", "cutoff"), ("3.9", "consequential")])
def test_uninspected_releases_and_system_models_are_refused(rid, version, model):
    source = BrightwayInventory.from_data(
        consumer(RECORDS[rid]["original_rule"]["source"], 1), context=contexts("ecoinvent", version, model)
    )
    with pytest.raises(MigrationError):
        source.migrate_background(target_context("2026"), policy=MigrationPolicy.permissive())


def minimal_resource():
    return {
        "replace": [deepcopy(r["original_rule"]) for r in REVIEW["records"]],
        "delete": [],
        "reviews": {},
        "coverage": {},
    }


def test_replay_is_idempotent():
    resource = minimal_resource()
    apply_fuels_39_endpoint_review(resource)
    expected = deepcopy(resource)
    apply_fuels_39_endpoint_review(resource)
    assert resource == expected


@pytest.mark.parametrize("change", ["parent", "shared_review", "evidence"])
def test_drift_is_refused_before_mutation(tmp_path, change):
    resource = minimal_resource()
    root = None
    if change == "parent":
        resource["replace"][0]["target"]["location"] = "different"
    elif change == "shared_review":
        apply_fuels_39_endpoint_review(resource)
        next(iter(resource["reviews"].values()))["rationale"] += " changed"
    else:
        root = tmp_path
        path = root / REVIEW_PATH
        path.parent.mkdir(parents=True)
        path.write_text("{}")
    before = deepcopy(resource)
    with pytest.raises(ValueError):
        apply_fuels_39_endpoint_review(resource, **({"root": root} if root else {}))
    assert resource == before


def test_silicate_basis_checks_are_primary_inventory_checks_not_title_scaling():
    row = RECORDS["bp-2b86a55ab1009223"]
    assert len(row["basis_comparison"]["matches"]) == 3
    assert all(x["same_amount_and_unit"] for x in row["basis_comparison"]["matches"])
    assert row["compiled_rule"]["conversion_factor"] == 1
    assert row["compiled_rule"]["target"]["name"] == "Sodium silicate, furnace process, pieces, at plant"
    assert RECORDS["bp-17d4104946dac764"]["compiled_rule"]["target"]["location"] == "RER"
    assert "foam as building waste" not in RECORDS["bp-cd3492574a089af6"]["compiled_rule"]["target"]["name"]
