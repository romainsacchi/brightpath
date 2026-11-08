"""Inspected material identities retain signed coordinates and exact profile gates."""

from copy import deepcopy
from dataclasses import replace
import json
import math

import pytest

from brightpath import DATA_DIR, BrightwayInventory
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource
from scripts.review_uvek_pilot_materials import (
    apply_pilot_material_review,
    apply_pilot_paint_market_review,
    apply_pilot_silver_market_review,
)
from test_uvek_reference_signs import consumer, contexts

REVIEW = json.loads((DATA_DIR / "export/uvek_pilot_material_review.json").read_text())
PAINT_MARKET = json.loads((DATA_DIR / "export/uvek_pilot_paint_market_review.json").read_text())
SILVER_MARKET = json.loads((DATA_DIR / "export/uvek_pilot_silver_market_review.json").read_text())
CASES = [
    (d, p["source_profile"]["version"])
    for review in (REVIEW, PAINT_MARKET, SILVER_MARKET)
    for d in review["rules"]
    for p in d["profiles"]
]
POLICY = replace(
    MigrationPolicy.strict(), on_heuristic_mapping=PolicyAction.WARN, on_information_loss=PolicyAction.WARN
)


@pytest.mark.parametrize("decision,version", CASES, ids=[f"{d['label']}-{v}" for d, v in CASES])
@pytest.mark.parametrize("amount", [-3.5, 3.5])
@pytest.mark.parametrize("target_version", ["2025", "2026"])
def test_material_correction_preserves_reference_sign_uncertainty_and_formula(
    decision, version, amount, target_version
):
    data = consumer(decision["source"], amount, uncertainty=2)
    before = deepcopy(data)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", version))
    result = inventory.migrate_background(contexts("uvek", target_version).background, policy=POLICY)
    assert inventory.data == data == before
    edge = result.data[0]["exchanges"][1]
    assert all(edge[f] == value for f, value in decision["target"].items())
    factor = decision["conversion_factor"]
    assert edge["amount"] == pytest.approx(amount * factor)
    assert edge["loc"] == pytest.approx(before[0]["exchanges"][1]["loc"] + math.log(factor))
    assert "dose" in edge["formula"]
    if factor == 1:
        assert edge["formula"] == before[0]["exchanges"][1]["formula"]
    for field in ("uncertainty type", "scale", "negative"):
        assert edge[field] == before[0]["exchanges"][1][field]
    assert any(i.code == "migration.technosphere_information_loss" for i in result.last_migration_report.issues)


@pytest.mark.parametrize("label,version", [("nylon", "3.10.1"), ("fan", "3.11"), ("box", "3.11")])
def test_uninspected_version_has_no_implicit_material_approval(label, version):
    decision = next(d for d in REVIEW["rules"] if d["label"] == label)
    inventory = BrightwayInventory.from_data(consumer(decision["source"], -2), context=contexts("ecoinvent", version))
    before = inventory.data
    with pytest.raises(MigrationError):
        inventory.migrate_background(contexts("uvek", "2026").background, policy=POLICY)
    assert inventory.data == before


def test_material_review_replay_is_idempotent_and_refuses_changed_supplier_atomically():
    source = load_uvek_technosphere_resource()
    wanted = {d["rule_id"] for d in REVIEW["rules"]}
    resource = {
        "replace": deepcopy([r for r in source["replace"] if r["id"] in wanted]),
        "reviews": deepcopy(source["reviews"]),
        "source_profiles": deepcopy(source["source_profiles"]),
        "coverage": {},
    }
    apply_pilot_material_review(resource)
    before = deepcopy(resource)
    apply_pilot_material_review(resource)
    assert resource == before
    resource["replace"][-1]["target"]["name"] = "Changed supplier"
    before = deepcopy(resource)
    with pytest.raises(ValueError, match="source, supplier or unit changed"):
        apply_pilot_material_review(resource)
    assert resource == before


def test_corrections_do_not_approve_the_heuristic_resource_globally():
    decision = REVIEW["rules"][0]
    inventory = BrightwayInventory.from_data(consumer(decision["source"], 1), context=contexts("ecoinvent", "3.6"))
    with pytest.raises(MigrationError, match="heuristic"):
        inventory.migrate_background(contexts("uvek", "2026").background)


@pytest.mark.parametrize(
    "review,apply_review",
    [(PAINT_MARKET, apply_pilot_paint_market_review), (SILVER_MARKET, apply_pilot_silver_market_review)],
)
def test_market_replay_is_idempotent_and_refuses_supplier_drift(review, apply_review):
    resource = deepcopy(load_uvek_technosphere_resource())
    apply_review(resource)
    before = deepcopy(resource)
    apply_review(resource)
    assert resource == before
    rule = next(r for r in resource["replace"] if r["id"] == review["rules"][0]["rule_id"])
    rule["target"]["name"] = "Unreviewed solvent paint"
    before = deepcopy(resource)
    with pytest.raises(ValueError, match="source, supplier or unit changed"):
        apply_review(resource)
    assert resource == before


@pytest.mark.parametrize("review", [PAINT_MARKET, SILVER_MARKET])
def test_market_correction_does_not_approve_other_source_releases(review):
    inventory = BrightwayInventory.from_data(
        consumer(review["rules"][0]["source"], -2), context=contexts("ecoinvent", "3.11")
    )
    before = deepcopy(inventory.data)
    with pytest.raises(MigrationError):
        inventory.migrate_background(contexts("uvek", "2026").background, policy=POLICY)
    assert inventory.data == before
