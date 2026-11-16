"""Named chemical proxies and PVC disposal retain the reference-coordinate basis."""

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

REVIEW = json.loads((DATA_DIR / "export/uvek_battery_three_native_39_review.json").read_text())
ROWS = REVIEW["records"]


def scoped_policy(approved=True):
    resource = load_uvek_technosphere_resource()
    bridge = TechnosphereProfile("uvek", "2025", "cutoff")
    scopes = tuple(
        compatibility_rule_fingerprint(
            resource, "replace", row["compiled_rule"], TechnosphereProfile("ecoinvent", version, "cutoff"), bridge
        )
        for row in ROWS
        for version in row["source_witnesses"]
    )
    return MigrationPolicy(
        approved_compatibility_rules=scopes if approved else (), on_information_loss=PolicyAction.WARN
    )


@pytest.mark.parametrize("amount", [-2.5, 2.5])
@pytest.mark.parametrize("uncertainty", [2, 3])
@pytest.mark.parametrize("release", ["2025", "2026"])
def test_named_native_supplies_and_treatment_preserve_signed_statistics(amount, uncertainty, release):
    source = consumer(ROWS[0]["source"], amount, uncertainty)
    source[0]["exchanges"] = [source[0]["exchanges"][0]] + [
        consumer(row["source"], amount, uncertainty)[0]["exchanges"][1] for row in ROWS
    ]
    original = deepcopy(source)
    result = BrightwayInventory.from_data(source, context=contexts("ecoinvent", "3.9")).migrate_background(
        contexts("uvek", release).background, policy=scoped_policy()
    )
    assert source == original and len(result.data) == 1
    assert len(result.data[0]["exchanges"]) == 4
    for edge, before, row in zip(result.data[0]["exchanges"][1:], original[0]["exchanges"][1:], ROWS, strict=True):
        assert all(edge[k] == value for k, value in row["target"].items())
        expected = deepcopy(before)
        scale_exchange(expected, row["conversion_factor"])
        for key in ("amount", "loc", "scale", "negative", "minimum", "maximum", "formula", "uncertainty type"):
            if key in expected:
                assert edge[key] == expected[key]
    assert result.data[0]["exchanges"][-1]["amount"] == -amount


def test_source38_teos_is_not_approved_without_an_actual_required_use():
    with pytest.raises(MigrationError, match=ROWS[0]["rule_id"]):
        BrightwayInventory.from_data(
            consumer(ROWS[0]["source"], 1), context=contexts("ecoinvent", "3.8")
        ).migrate_background(contexts("uvek", "2026").background, policy=scoped_policy())


@pytest.mark.parametrize(
    "version,model,approved", [("3.9", "cutoff", False), ("3.10", "cutoff", True), ("3.9", "consequential", True)]
)
def test_other_profiles_and_unapproved_correspondences_remain_refused(version, model, approved):
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(
            consumer(ROWS[0]["source"], 1), context=contexts("ecoinvent", version, model)
        ).migrate_background(contexts("uvek", "2026").background, policy=scoped_policy(approved))


def test_specific_chemical_correction_does_not_invent_a_complete_process_description():
    resource = load_uvek_technosphere_resource()
    indexed = {row["id"]: row for row in resource["replace"]}
    assert REVIEW["override_decisions_changed"] == 1 and REVIEW["new_helpers"] == 0
    assert REVIEW["new_foreground_omissions"] == []
    for row in ROWS:
        assert indexed[row["rule_id"]] == row["compiled_rule"]
        assert all(
            witness["reference_amount"] == row["conversion_factor"] for witness in row["source_witnesses"].values()
        )
        assert all(witness["reference_amount"] == 1 for witness in row["native_witnesses"].values())
    assert ROWS[0]["original_rule"]["target"]["name"] == "Chemicals organic, at plant"
    assert ROWS[0]["target"]["name"] == "Tetraethylorthosilicat"
    assert all(w["nonproduction_count"] == 3 for w in ROWS[0]["native_witnesses"].values())
    assert "not documented" in ROWS[0]["limitations"][0]
    assert "allocation" in ROWS[1]["limitations"][0] and "unverified" in ROWS[1]["limitations"][0]
    assert "demolition" in ROWS[2]["limitations"][0] and ROWS[2]["conversion_factor"] == -1
