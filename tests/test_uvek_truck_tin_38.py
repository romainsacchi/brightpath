"""The required truck tin supply uses an exact source3.8 approval."""

import json
from copy import deepcopy

import pytest
from test_uvek_reference_signs import consumer, contexts

from brightpath import DATA_DIR, BrightwayInventory, TechnosphereProfile
from brightpath.background.rule_approvals import compatibility_rule_fingerprint
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource

REVIEW = json.loads((DATA_DIR / "export/uvek_truck_tin_38_review.json").read_text())
ROW = REVIEW["records"][0]


def policy():
    resource = load_uvek_technosphere_resource()
    rule = next(r for r in resource["replace"] if r["id"] == ROW["rule_id"])
    assert rule == ROW["rule"]
    scope = compatibility_rule_fingerprint(
        resource,
        "replace",
        rule,
        TechnosphereProfile(**REVIEW["source_profile"]),
        TechnosphereProfile(**REVIEW["target_profile"]),
    )
    return MigrationPolicy(approved_compatibility_rules=(scope,), on_information_loss=PolicyAction.WARN)


@pytest.mark.parametrize("amount", [-2.5, 2.5])
@pytest.mark.parametrize("uncertainty", [2, 3])
@pytest.mark.parametrize("release", ["2025", "2026"])
def test_signed_tin_supply_uses_complete_native_storage_without_extra_helpers(amount, uncertainty, release):
    data = consumer(ROW["source"], amount, uncertainty)
    original = deepcopy(data)
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.8")).migrate_background(
        contexts("uvek", release).background, policy=policy()
    )
    assert data == original and len(result.data) == 1
    actual = result.data[0]["exchanges"][1]
    assert all(actual[k] == value for k, value in ROW["target"].items())
    for key in ("amount", "loc", "scale", "negative", "formula", "uncertainty type"):
        if key in original[0]["exchanges"][1]:
            assert actual[key] == original[0]["exchanges"][1][key]


@pytest.mark.parametrize("version,model", [("3.9", "cutoff"), ("3.8", "consequential")])
def test_new_scope_alone_does_not_authorize_another_source_context(version, model):
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(
            consumer(ROW["source"], 1), context=contexts("ecoinvent", version, model)
        ).migrate_background(contexts("uvek", "2026").background, policy=policy())
