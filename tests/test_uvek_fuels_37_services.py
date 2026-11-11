"""Service proxies preserve quantity bases and complete compressed-air networks."""

import json
from copy import deepcopy

import pytest
from test_uvek_reference_signs import consumer, contexts
from test_uvek_refrigerant_disposal import target_context

from brightpath import DATA_DIR, BrightwayInventory
from brightpath.core.policies import MigrationPolicy
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource
from scripts.review_uvek_fuels_37_services import apply_fuels_37_services_review

REVIEW = json.loads((DATA_DIR / "export/uvek_fuels_37_services_review.json").read_text())
RECORDS = {r["rule_id"]: r for r in REVIEW["records"]}
AIR = "bp-ad43c87e102ce339"


@pytest.mark.parametrize("rid", RECORDS)
@pytest.mark.parametrize("amount", [-2.5, 2.5])
@pytest.mark.parametrize("uncertainty", [2, 3])
def test_service_coordinates_statistics_and_network_rows(rid, amount, uncertainty):
    row = RECORDS[rid]
    source = BrightwayInventory.from_data(
        consumer(row["original_rule"]["source"], amount, uncertainty), context=contexts("ecoinvent", "3.7", "cutoff")
    )
    before = deepcopy(source.data)
    result = source.migrate_background(target_context("2026"), policy=MigrationPolicy.permissive())
    edge = result.data[0]["exchanges"][1]
    original = before[0]["exchanges"][1]
    for key in ("amount", "formula", "uncertainty type", "loc", "scale", "negative", "minimum", "maximum"):
        if key in original:
            assert edge[key] == original[key]
    if rid == AIR:
        assert len(result.data) == 2
        expected = REVIEW["recipes"]["fuels-37-compressed-air-complete-network-mix"]["inputs"]
        actual = [e for e in result.data[1]["exchanges"] if e["type"] != "production"]
        assert len(actual) == 3
        for a, b in zip(actual, expected, strict=True):
            assert all(a.get(k) == v for k, v in b.items())
            assert "at supply network" in a["name"]
    else:
        assert len(result.data) == 1
        assert all(edge[k] == v for k, v in row["compiled_rule"]["target"].items())
    assert source.data == before


def test_network_coefficients_and_distinct_factory_bases_are_explicit():
    proof = {r["kind"]: r for r in REVIEW["network_boundary_checks"]}
    for kind, factor in {"average": 1.3, "best": 1.1, "optimised": 1.15}.items():
        assert proof[kind]["native_generation_factor"] == pytest.approx(factor)
        assert proof[kind]["inspected_patch_share"] == 0.333333333333333
    assert "different loss convention" in RECORDS[AIR]["limitations"][0]
    assert RECORDS["bp-3c662478c0f234c1"]["quantity_basis"] == "kilogram"
    assert RECORDS["bp-270bf2428cce37a2"]["quantity_basis"] == "unit"
    assert REVIEW["patch_evidence_decision"]["requested_version"] == "3.7"
    assert REVIEW["patch_evidence_decision"]["inspected_version"] == "3.7.1"


@pytest.mark.parametrize("version", ["3.6", "3.8"])
def test_other_air_profiles_do_not_inherit_network_review(version):
    source = BrightwayInventory.from_data(
        consumer(RECORDS[AIR]["original_rule"]["source"], 1, 2), context=contexts("ecoinvent", version, "cutoff")
    )
    with pytest.raises(MigrationError) as caught:
        source.migrate_background(target_context("2026"), policy=MigrationPolicy.permissive())
    assert "migration.reference_flow_sign_unresolved" in json.dumps(caught.value.report.to_dict())


def test_replay_is_idempotent_and_rejects_network_change():
    resource = deepcopy(load_uvek_technosphere_resource())
    before = deepcopy(resource)
    apply_fuels_37_services_review(resource)
    assert resource == before
    resource["recipes"]["fuels-37-compressed-air-complete-network-mix"]["inputs"][0]["amount"] *= 2
    with pytest.raises(ValueError, match="helper changed"):
        apply_fuels_37_services_review(resource)
