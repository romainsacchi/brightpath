"""Reviewed source-3.8 car inputs retain signed quantities and exact profiles."""

import json
from copy import deepcopy

import pytest
from test_uvek_reference_signs import consumer, contexts

from brightpath import DATA_DIR, BrightwayInventory, TechnosphereProfile
from brightpath.background.rule_approvals import compatibility_rule_fingerprint
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.migrations.resources import load_uvek_technosphere_resource

REVIEW = json.loads((DATA_DIR / "export/uvek_passenger_car_38_review.json").read_text())


@pytest.mark.parametrize("amount", [-2, 2])
@pytest.mark.parametrize("release", ["2025", "2026"])
def test_required_car_materials_preserve_quantities_uncertainty_and_source_ownership(amount, release):
    resource = load_uvek_technosphere_resource()
    indexed = {r["id"]: r for r in resource["replace"]}
    rows = REVIEW["rules"]
    policy = MigrationPolicy(
        approved_compatibility_rules=tuple(
            compatibility_rule_fingerprint(
                resource,
                "replace",
                indexed[row["rule_id"]],
                TechnosphereProfile(**REVIEW["source_profile"]),
                TechnosphereProfile(**resource["target_profile"]),
            )
            for row in rows
        ),
        on_information_loss=PolicyAction.WARN,
    )
    data = consumer(rows[0]["source"], amount, 3)
    data[0]["exchanges"].extend(consumer(r["source"], amount, 3)[0]["exchanges"][1] for r in rows[1:])
    before = deepcopy(data)
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.8")).migrate_background(
        contexts("uvek", release).background, policy=policy
    )
    assert data == before and len(result.data) == 1
    for row, edge in zip(rows, result.data[0]["exchanges"][1:], strict=True):
        assert all(edge[k] == v for k, v in row["target"].items())
        assert edge["amount"] == edge["loc"] == amount
        assert edge["scale"] == 0.2 and edge["formula"] == "dose"
