"""Boundary corrections preserve coordinates and retire the erroneous lime approval."""

import json
from copy import deepcopy

import pytest
from test_uvek_reference_signs import consumer, contexts

from brightpath import DATA_DIR, BrightwayInventory, TechnosphereProfile
from brightpath.background.rule_approvals import compatibility_rule_fingerprint
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource
from scripts.review_uvek_heat_38_corrections import apply_heat_38_corrections_review, compiled_boundary_rule
from scripts.review_uvek_pilot_materials import apply_pilot_material_review

REVIEW = json.loads((DATA_DIR / "export/uvek_heat_38_corrections_review.json").read_text())
CASES = [(r, p) for r in REVIEW["records"] for p in r["profiles"]]


@pytest.fixture(scope="module")
def policy():
    resource = load_uvek_technosphere_resource()
    indexed = {r["id"]: r for r in resource["replace"]}
    scopes = tuple(
        compatibility_rule_fingerprint(
            resource,
            "replace",
            indexed[r["rule_id"]],
            TechnosphereProfile(**p["source_profile"]),
            TechnosphereProfile("uvek", "2025", "cutoff"),
        )
        for r, p in CASES
    )
    return MigrationPolicy(approved_compatibility_rules=scopes, on_information_loss=PolicyAction.WARN)


@pytest.mark.parametrize("row,p", CASES, ids=[r["rule_id"] + "-" + p["source_profile"]["version"] for r, p in CASES])
@pytest.mark.parametrize("amount,uncertainty", [(-2, 2), (2, 2), (-2, 3), (2, 3)])
def test_native_boundary_correction_keeps_reference_coordinate_and_uncertainty(row, p, amount, uncertainty, policy):
    data = consumer(row["source"], amount, uncertainty)
    before = deepcopy(data)
    result = BrightwayInventory.from_data(
        data, context=contexts("ecoinvent", p["source_profile"]["version"])
    ).migrate_background(contexts("uvek", "2026").background, policy=policy)
    assert data == before and len(result.data) == 1
    edge = result.data[0]["exchanges"][1]
    assert all(edge[k] == v for k, v in row["target"].items())
    original = before[0]["exchanges"][1]
    factor = row["conversion_factor"]
    assert edge["amount"] == amount * factor
    assert edge["amount"] / p["target_reference_amount"] == amount / p["source_reference_amount"]
    assert edge["scale"] == original["scale"]
    if factor == 1:
        assert all(edge[k] == v for k, v in original.items() if k not in row["source"])
    elif uncertainty == 2:
        assert edge["loc"] == original["loc"] and edge["negative"] is not original["negative"]
    else:
        assert edge["loc"] == -original["loc"]
        assert edge["minimum"] == -original["maximum"] and edge["maximum"] == -original["minimum"]


@pytest.mark.parametrize("row", REVIEW["records"], ids=lambda r: r["rule_id"])
@pytest.mark.parametrize("version,model", [("3.11", "cutoff"), ("3.8", "consequential")])
def test_uninspected_profiles_cannot_inherit_corrections(row, version, model, policy):
    data = consumer(row["source"], -1)
    before = deepcopy(data)
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(data, context=contexts("ecoinvent", version, model)).migrate_background(
            contexts("uvek", "2026").background, policy=policy
        )
    assert data == before


@pytest.mark.parametrize("drift", [False, True])
def test_authoring_is_atomic_and_idempotent(drift):
    resource = deepcopy(load_uvek_technosphere_resource())
    # Test this historical authoring stage against its frozen inputs. The live
    # resource includes later coal sign-review extensions that it must not erase.
    previous = {row["rule_id"]: row["previous_rule"] for row in REVIEW["records"]}
    resource["replace"] = [deepcopy(previous.get(rule["id"], rule)) for rule in resource["replace"]]
    apply_heat_38_corrections_review(resource)
    if drift:
        next(r for r in resource["replace"] if r["id"] == REVIEW["records"][-1]["rule_id"])["target"][
            "name"
        ] = "Wrong material"
        resource["replace"] = [
            deepcopy(REVIEW["records"][0]["previous_rule"]) if r["id"] == REVIEW["records"][0]["rule_id"] else r
            for r in resource["replace"]
        ]
    before = deepcopy(resource)
    if drift:
        with pytest.raises(ValueError):
            apply_heat_38_corrections_review(resource)
    else:
        apply_heat_38_corrections_review(resource)
        assert all(
            next(r for r in resource["replace"] if r["id"] == row["rule_id"]) == compiled_boundary_rule(row)
            for row in REVIEW["records"]
        )
    assert resource == before


def test_old_material_authoring_cannot_restore_erroneous_quicklime():
    resource = deepcopy(load_uvek_technosphere_resource())
    row = next(r for r in REVIEW["records"] if r["superseded_approved_scopes"])
    current = next(r for r in resource["replace"] if r["id"] == row["rule_id"])
    before = deepcopy(current)
    apply_pilot_material_review(resource)
    assert current == before and current["target"]["name"] == "Limestone, milled, loose, at plant"
