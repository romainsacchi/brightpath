"""Reviewed source-3.8 correspondences preserve caller data and treatment signs."""

import json
from copy import deepcopy

import pytest
from test_uvek_reference_signs import consumer, contexts

from brightpath import DATA_DIR, BrightwayInventory, TechnosphereProfile
from brightpath.background.rule_approvals import compatibility_rule_fingerprint
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource
from brightpath.migrations.scaling import scale_exchange

REVIEW = json.loads((DATA_DIR / "export/uvek_battery_native_direct_38_review.json").read_text())
ROWS = REVIEW["records"]
REUSED = json.loads((DATA_DIR / "export/uvek_battery_reused_native_38_review.json").read_text())
ROWS = ROWS + REUSED["records"]


def scoped_policy(approved=True):
    resource = load_uvek_technosphere_resource()
    return MigrationPolicy(
        approved_compatibility_rules=(
            tuple(
                compatibility_rule_fingerprint(
                    resource,
                    "replace",
                    row["rule"],
                    TechnosphereProfile("ecoinvent", "3.8", "cutoff"),
                    TechnosphereProfile("uvek", "2025", "cutoff"),
                )
                for row in ROWS
            )
            if approved
            else ()
        ),
        on_information_loss=PolicyAction.WARN,
    )


@pytest.mark.parametrize("amount", [-2.5, 2.5])
@pytest.mark.parametrize("uncertainty", [2, 3])
@pytest.mark.parametrize("release", ["2025", "2026"])
def test_named_supplies_and_disposal_keep_signed_statistics_and_caller_metadata(amount, uncertainty, release):
    source = consumer(ROWS[0]["source"], amount, uncertainty)
    source[0]["exchanges"] = [source[0]["exchanges"][0]] + [
        consumer(row["source"], amount, uncertainty)[0]["exchanges"][1] for row in ROWS
    ]
    original = deepcopy(source)
    result = BrightwayInventory.from_data(source, context=contexts("ecoinvent", "3.8")).migrate_background(
        contexts("uvek", release).background, policy=scoped_policy()
    )
    assert source == original and len(result.data) == 1
    assert len(result.data[0]["exchanges"]) == 15
    for edge, before, row in zip(result.data[0]["exchanges"][1:], original[0]["exchanges"][1:], ROWS, strict=True):
        assert all(edge[k] == value for k, value in row["target"].items())
        expected = deepcopy(before)
        scale_exchange(expected, row["conversion_factor"])
        for key in ("amount", "loc", "scale", "negative", "minimum", "maximum", "formula", "uncertainty type"):
            if key in expected:
                assert edge[key] == expected[key]


@pytest.mark.parametrize(
    "version,model,approved", [("3.8", "cutoff", False), ("3.9", "cutoff", True), ("3.8", "consequential", True)]
)
def test_permissions_do_not_cover_another_release_or_system_model(version, model, approved):
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(
            consumer(ROWS[0]["source"], 1), context=contexts("ecoinvent", version, model)
        ).migrate_background(contexts("uvek", "2026").background, policy=scoped_policy(approved))


def test_review_excludes_wrong_product_boundaries_and_records_nonempty_source_evidence():
    resource = load_uvek_technosphere_resource()
    rules = {rule["id"]: rule for rule in resource["replace"]}
    assert len(ROWS) == 14 and not REVIEW["rules_changed"] and not REUSED["rules_changed"]
    assert REVIEW["new_helpers"] == 0 and REVIEW["new_foreground_omissions"] == []
    for row in ROWS:
        assert row["rule"] == rules[row["rule_id"]]
        assert row["source_witness"]["nonproduction_count"] > 0
        assert row["actual_nonzero_foreground_uses"] > 0
        assert all(w["reference_amount"] == 1 for w in row["native_witnesses"].values())
    disposal = next(row for row in ROWS if row["category"] == "WEEE shredder-residue disposal")
    assert disposal["source_witness"]["reference_amount"] == disposal["conversion_factor"] == -1
    assert all(row["conversion_factor"] == 1 for row in ROWS if row is not disposal)
    assert "Synthetic graphite" in REVIEW["scope"]
