"""The inspected car suppliers preserve signed reference quantities and units."""

import json
from copy import deepcopy

import pytest
from test_uvek_reference_signs import consumer, contexts

from brightpath import DATA_DIR, BrightwayInventory, TechnosphereProfile
from brightpath.background.rule_approvals import compatibility_rule_fingerprint
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource

REVIEW = json.loads((DATA_DIR / "export/uvek_passenger_car_direct_312_review.json").read_text())


def policy():
    resource = load_uvek_technosphere_resource()
    indexed = {r["id"]: r for r in resource["replace"]}
    return MigrationPolicy(
        approved_compatibility_rules=tuple(
            compatibility_rule_fingerprint(
                resource,
                "replace",
                indexed[row["rule_id"]],
                TechnosphereProfile(**REVIEW["source_profile"]),
                TechnosphereProfile(**resource["target_profile"]),
            )
            for row in REVIEW["rules"]
        ),
        on_information_loss=PolicyAction.WARN,
    )


@pytest.mark.parametrize("amount", [-2, 2])
@pytest.mark.parametrize("release", ["2025", "2026"])
def test_car_suppliers_preserve_quantities_uncertainty_and_ownership(amount, release):
    rows = REVIEW["rules"]
    data = consumer(rows[0]["source"], amount, 3)
    data[0]["exchanges"].extend(consumer(r["source"], amount, 3)[0]["exchanges"][1] for r in rows[1:])
    before = deepcopy(data)
    converted = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.12")).migrate_background(
        contexts("uvek", release).background, policy=policy()
    )
    assert data == before and len(converted.data) == 1
    for row, edge in zip(rows, converted.data[0]["exchanges"][1:], strict=True):
        assert all(edge[key] == value for key, value in row["target"].items())
        assert edge["amount"] == edge["loc"] == amount
        assert edge["scale"] == 0.2 and edge["formula"] == "dose"


def test_the_new_approval_is_not_a_permission_for_another_source_release():
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(
            consumer(REVIEW["rules"][0]["source"], 1), context=contexts("ecoinvent", "3.11")
        ).migrate_background(contexts("uvek", "2026").background, policy=policy())
