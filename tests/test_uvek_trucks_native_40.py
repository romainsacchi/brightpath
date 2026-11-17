"""Forty actual truck supplies retain product bases and exact source permissions."""

import json
from copy import deepcopy

import pytest
from test_uvek_reference_signs import consumer, contexts

from brightpath import DATA_DIR, BrightwayInventory, TechnosphereProfile
from brightpath.background.rule_approvals import compatibility_rule_fingerprint
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource

REVIEW = json.loads((DATA_DIR / "export/uvek_trucks_native_40_review.json").read_text())
ROWS = REVIEW["records"] + REVIEW["additional_approvals"]


def policy():
    resource = load_uvek_technosphere_resource()
    rules = {r["id"]: r for r in resource["replace"]}
    return MigrationPolicy(
        approved_compatibility_rules=tuple(
            compatibility_rule_fingerprint(
                resource,
                "replace",
                rules[r["rule_id"]],
                TechnosphereProfile(**REVIEW["source_profile"]),
                TechnosphereProfile(**REVIEW["target_profile"]),
            )
            for r in ROWS
        ),
        on_information_loss=PolicyAction.WARN,
    )


def batch(amount, uncertainty):
    data = consumer(ROWS[0]["source"], amount, uncertainty)
    data[0]["exchanges"].extend(consumer(r["source"], amount, uncertainty)[0]["exchanges"][1] for r in ROWS[1:])
    return data


@pytest.mark.parametrize("amount", [-2.5, 2.5])
@pytest.mark.parametrize("uncertainty", [2, 3])
@pytest.mark.parametrize("release", ["2025", "2026"])
def test_all_40_products_keep_reference_quantities_and_statistics_without_helpers(amount, uncertainty, release):
    data = batch(amount, uncertainty)
    before = deepcopy(data)
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.8")).migrate_background(
        contexts("uvek", release).background, policy=policy()
    )
    assert data == before and len(result.data) == 1
    for edge, old, row in zip(result.data[0]["exchanges"][1:], before[0]["exchanges"][1:], ROWS, strict=True):
        assert all(edge[k] == value for k, value in row["target"].items())
        for key in ("amount", "loc", "scale", "formula", "negative", "minimum", "maximum", "uncertainty type"):
            if key in old:
                assert edge[key] == old[key]
    assert all(r["conversion_factor"] == 1 for r in ROWS)


def test_unreviewed_recycled_hdpe_is_not_silently_replaced_by_virgin_material():
    source = {
        "name": "market for polyethylene, high density, granulate, recycled",
        "reference product": "polyethylene, high density, granulate, recycled",
        "location": "RoW",
        "unit": "kilogram",
    }
    with pytest.raises(MigrationError, match="bp-561db61b644cb833"):
        BrightwayInventory.from_data(consumer(source, 1), context=contexts("ecoinvent", "3.8")).migrate_background(
            contexts("uvek", "2026").background, policy=policy()
        )


@pytest.mark.parametrize("version,model", [("3.9", "cutoff"), ("3.8", "consequential")])
def test_the_new_permissions_do_not_authorize_other_source_contexts(version, model):
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(
            consumer(ROWS[0]["source"], 1), context=contexts("ecoinvent", version, model)
        ).migrate_background(contexts("uvek", "2026").background, policy=policy())
