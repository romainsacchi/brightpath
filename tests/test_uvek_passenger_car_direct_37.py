"""Reviewed source-3.7 car inputs retain signed quantities and exact profiles."""

import json
from copy import deepcopy

import pytest
from test_uvek_reference_signs import consumer, contexts

from brightpath import DATA_DIR, BrightwayInventory, TechnosphereProfile
from brightpath.background.rule_approvals import compatibility_rule_fingerprint
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.migrations.resources import load_uvek_technosphere_resource
from brightpath.migrations.scaling import scale_exchange

REVIEW = json.loads((DATA_DIR / "export/uvek_passenger_car_direct_37_review.json").read_text())


@pytest.mark.parametrize("amount,uncertainty", [(-2, 2), (2, 3)])
@pytest.mark.parametrize("release", ["2025", "2026"])
def test_required_car_services_keep_reference_signs_and_quantities(amount, uncertainty, release):
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
    data = consumer(rows[0]["source"], amount, uncertainty)
    data[0]["exchanges"].extend(consumer(r["source"], amount, uncertainty)[0]["exchanges"][1] for r in rows[1:])
    before = deepcopy(data)
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.7")).migrate_background(
        contexts("uvek", release).background, policy=policy
    )
    assert data == before and len(result.data) == 1
    for row, edge in zip(rows, result.data[0]["exchanges"][1:], strict=True):
        assert all(edge[k] == v for k, v in row["target"].items())
        expected = deepcopy(before[0]["exchanges"][rows.index(row) + 1])
        scale_exchange(expected, row["conversion_factor"])
        for key in ("amount", "loc", "scale", "negative", "uncertainty type", "formula"):
            if key in expected:
                assert edge[key] == expected[key]
