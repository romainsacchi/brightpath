"""Current lorry services retain exact units and disposal-demand sign changes."""

import json
from copy import deepcopy

import pytest
from test_uvek_reference_signs import consumer, contexts

from brightpath import DATA_DIR, BrightwayInventory, TechnosphereProfile
from brightpath.background.rule_approvals import compatibility_rule_fingerprint
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource

REVIEW = json.loads((DATA_DIR / "export/uvek_trucks_native_services_312_review.json").read_text())
ROWS = REVIEW["additional_approvals"]


def policy(approved=True):
    resource = load_uvek_technosphere_resource()
    rules = {r["id"]: r for r in resource["replace"]}
    return MigrationPolicy(
        on_information_loss=PolicyAction.WARN,
        approved_compatibility_rules=(
            tuple(
                compatibility_rule_fingerprint(
                    resource,
                    "replace",
                    rules[r["rule_id"]],
                    TechnosphereProfile(**REVIEW["source_profile"]),
                    TechnosphereProfile(**REVIEW["target_profile"]),
                )
                for r in ROWS
            )
            if approved
            else ()
        ),
    )


@pytest.mark.parametrize("amount", [-2.5, 2.5])
@pytest.mark.parametrize("uncertainty", [2, 3])
@pytest.mark.parametrize("release", ["2025", "2026"])
def test_lorry_services_keep_class_units_and_signed_quantities(amount, uncertainty, release):
    data = consumer(ROWS[0]["source"], amount, uncertainty)
    data[0]["exchanges"].extend(consumer(r["source"], amount, uncertainty)[0]["exchanges"][1] for r in ROWS[1:])
    original = deepcopy(data)
    migrated = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.12")).migrate_background(
        contexts("uvek", release).background, policy=policy()
    )
    assert data == original and len(migrated.data) == 1
    for e, old, row in zip(migrated.data[0]["exchanges"][1:], original[0]["exchanges"][1:], ROWS, strict=True):
        factor = row["conversion_factor"]
        assert all(e[k] == row["target"][k] for k in ("name", "reference product", "location", "unit"))
        assert e["unit"] == row["source"]["unit"] and e["amount"] == factor * amount
        assert e["scale"] == old["scale"]
        assert e["loc"] == (old["loc"] if uncertainty == 2 else old["loc"] * factor)
        if factor == -1:
            assert e["formula"] == "(dose) * (-1.0)"
            if uncertainty == 2:
                assert e["negative"] is not old["negative"]
            else:
                assert e["minimum"] == -old["maximum"] and e["maximum"] == -old["minimum"]
        else:
            assert e["formula"] == "dose"
        assert (factor == 1) is row["source"]["name"].startswith("maintenance")
        for v in row["native_suppliers"].values():
            assert v["reference_amounts"] == [1] and v["nonproduction_count"] > 0


def test_current_service_permission_does_not_cover_a_patch_release():
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(
            consumer(ROWS[0]["source"], -1), context=contexts("ecoinvent", "3.12.1")
        ).migrate_background(contexts("uvek", "2026").background, policy=policy())


def test_without_permission_the_current_service_still_refuses():
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(
            consumer(ROWS[0]["source"], -1), context=contexts("ecoinvent", "3.12")
        ).migrate_background(contexts("uvek", "2026").background, policy=policy(False))
