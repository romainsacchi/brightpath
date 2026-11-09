"""Sanitary disposal keeps its waste basis, sign, exact scope and limitations."""

from copy import deepcopy
from dataclasses import replace
import json

import pytest

from brightpath import DATA_DIR, BrightwayInventory
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource
from scripts.review_uvek_pilot_sanitary_inert import apply_pilot_sanitary_inert_review
from test_uvek_reference_signs import consumer, contexts

REVIEW = json.loads((DATA_DIR / "export/uvek_pilot_sanitary_inert_review.json").read_text())
POLICY = replace(
    MigrationPolicy.strict(), on_heuristic_mapping=PolicyAction.WARN, on_information_loss=PolicyAction.WARN
)


@pytest.mark.parametrize("decision", REVIEW["rules"], ids=lambda d: d["rule_id"])
@pytest.mark.parametrize("amount", [-2.5, 2.5])
def test_sanitary_inert_preserves_signed_wet_mass_and_uncertainty(decision, amount):
    data = consumer(decision["source"], amount, uncertainty=2)
    original = deepcopy(data)
    version = decision["profiles"][0]["source_profile"]["version"]
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", version))
    result = inventory.migrate_background(contexts("uvek", "2026").background, policy=POLICY)
    edge = result.data[0]["exchanges"][1]
    assert all(edge[k] == v for k, v in decision["target"].items())
    assert edge["amount"] == -amount
    assert edge["negative"] is (amount > 0)
    assert edge["loc"] == original[0]["exchanges"][1]["loc"]
    assert edge["scale"] == 0.2
    assert edge["formula"] == "(dose) * (-1.0)"
    assert inventory.data == data == original
    assert any(i.code == "migration.technosphere_information_loss" for i in result.last_migration_report.issues)


@pytest.mark.parametrize("decision", REVIEW["rules"], ids=lambda d: d["rule_id"])
def test_sanitary_inert_does_not_approve_uninspected_profiles(decision):
    inventory = BrightwayInventory.from_data(consumer(decision["source"], -1), context=contexts("ecoinvent", "3.12"))
    before = deepcopy(inventory.data)
    with pytest.raises(MigrationError):
        inventory.migrate_background(contexts("uvek", "2026").background, policy=POLICY)
    assert inventory.data == before


def test_sanitary_inert_replay_changes_only_inspected_endpoints_and_is_idempotent():
    resource = deepcopy(load_uvek_technosphere_resource())
    expected = deepcopy(resource)
    by_id = {r["id"]: r for r in resource["replace"]}
    for decision in REVIEW["rules"]:
        by_id[decision["rule_id"]].clear()
        by_id[decision["rule_id"]].update(deepcopy(decision["previous_rule"]))
    apply_pilot_sanitary_inert_review(resource)
    assert resource == expected
    apply_pilot_sanitary_inert_review(resource)
    assert resource == expected


def test_sanitary_inert_replay_refuses_supplier_drift_atomically():
    resource = deepcopy(load_uvek_technosphere_resource())
    by_id = {r["id"]: r for r in resource["replace"]}
    by_id[REVIEW["rules"][-1]["rule_id"]]["target"]["name"] = "Unreviewed landfill"
    before = deepcopy(resource)
    with pytest.raises(ValueError, match="supplier or unit changed"):
        apply_pilot_sanitary_inert_review(resource)
    assert resource == before


@pytest.mark.parametrize("change", ["review", "authority"])
def test_sanitary_inert_replay_refuses_evidence_drift(tmp_path, change):
    from scripts.review_uvek_pilot_sanitary_inert import REVIEW_PATH, ROOT

    path = tmp_path / REVIEW_PATH
    path.parent.mkdir(parents=True)
    path.write_bytes((ROOT / REVIEW_PATH).read_bytes() if change == "authority" else b"{}")
    if change == "authority":
        for row in REVIEW["authorization_evidence"]:
            (tmp_path / "brightpath/data" / row["path"]).write_bytes(b"{}")
    with pytest.raises(ValueError, match="evidence changed|authority changed"):
        apply_pilot_sanitary_inert_review({}, tmp_path)
