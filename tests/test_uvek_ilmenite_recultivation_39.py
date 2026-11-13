"""Explicit limestone restoration preserves source area and signed demand."""

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

REVIEW = json.loads((DATA_DIR / "export/uvek_ilmenite_recultivation_39_review.json").read_text())
ACTIVE_ROWS = REVIEW["records"]


def policy(rows=None):
    resource = load_uvek_technosphere_resource()
    rules = {r["id"]: r for r in resource["replace"]}
    return MigrationPolicy(
        approved_compatibility_rules=tuple(
            compatibility_rule_fingerprint(
                resource,
                "replace",
                rules[row["rule_id"]],
                TechnosphereProfile("ecoinvent", "3.9", "cutoff"),
                TechnosphereProfile("uvek", "2025", "cutoff"),
            )
            for row in (ACTIVE_ROWS if rows is None else rows)
        ),
        on_information_loss=PolicyAction.WARN,
    )


@pytest.mark.parametrize("amount", [-2.5, 2.5])
@pytest.mark.parametrize("uncertainty", [2, 3])
@pytest.mark.parametrize("release", ["2025", "2026"])
def test_native_services_keep_functional_units_and_scaled_uncertainty(amount, uncertainty, release):
    rows = ACTIVE_ROWS
    data = consumer(rows[0]["original_rule"]["source"], amount, uncertainty)
    data[0]["exchanges"].append(consumer(rows[1]["original_rule"]["source"], amount, uncertainty)[0]["exchanges"][1])
    data[0]["exchanges"].append(
        {
            "name": "Carbon dioxide, fossil",
            "categories": ["air"],
            "unit": "kilogram",
            "type": "biosphere",
            "amount": 0.5,
        }
    )
    before = deepcopy(data)
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.9")).migrate_background(
        contexts("uvek", release).background, policy=policy()
    )
    assert data == before and len(result.data) == 1
    assert result.data[0]["exchanges"][-1]["amount"] == 0.5
    for i, row in enumerate(rows, 1):
        actual = result.data[0]["exchanges"][i]
        expected = deepcopy(before[0]["exchanges"][i])
        scale_exchange(expected, row["conversion_factor"])
        for key in ("amount", "uncertainty type", "loc", "scale", "minimum", "maximum", "negative", "formula"):
            if key in expected:
                assert actual[key] == expected[key]
        assert all(actual[k] == v for k, v in row["compiled_rule"]["target"].items())
        assert actual["unit"] == row["source_witness"]["identity"]["unit"]


@pytest.mark.parametrize("row", ACTIVE_ROWS, ids=lambda r: r["rule_id"])
def test_both_services_still_require_their_independent_scope(row):
    remaining = [r for r in ACTIVE_ROWS if r["rule_id"] != row["rule_id"]]
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(
            consumer(row["original_rule"]["source"], -1), context=contexts("ecoinvent", "3.9")
        ).migrate_background(contexts("uvek", "2026").background, policy=policy(remaining))


@pytest.mark.parametrize("row", ACTIVE_ROWS, ids=lambda r: r["rule_id"])
@pytest.mark.parametrize(
    "version,model", [("3.8", "cutoff"), ("3.9.1", "cutoff"), ("3.10", "cutoff"), ("3.9", "consequential")]
)
def test_unreviewed_profiles_do_not_receive_the_39_scope(version, model, row):
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(
            consumer(row["original_rule"]["source"], -1), context=contexts("ecoinvent", version, model)
        ).migrate_background(contexts("uvek", "2026").background, policy=policy())


def test_explicit_source_proxy_and_land_reference_are_recorded():
    assert REVIEW["matched_land_exchange_count"] == 2
    assert REVIEW["source_and_native_diesel_MJ_per_m2"] == 7.44
    assert REVIEW["source_and_native_land_transformation_m2"] == {
        "from_mineral_extraction_site": 1,
        "to_unspecified_forest": 1,
    }
    assert REVIEW["new_source_omissions"] == []
    for row in ACTIVE_ROWS:
        assert row["source_witness"]["reference_amounts"] == [1]
        assert row["compiled_rule"]["target"]["name"] == "Recultivation, limestone mine"
        assert row["compiled_rule"]["target"]["unit"] == "square meter"
        assert all(w["reference_amounts"] == [1] for w in row["native_witnesses"].values())


@pytest.mark.parametrize("drift", [None, "parent", "target"])
def test_replay_is_atomic_and_keeps_the_specific_restoration_service(drift):
    from scripts.review_uvek_ilmenite_recultivation_39 import apply_ilmenite_recultivation_39_review

    r = deepcopy(load_uvek_technosphere_resource())
    rule = next(a for a in r["replace"] if a["id"] == ACTIVE_ROWS[0]["rule_id"])
    if drift == "parent":
        rule["confidence"] = 0.01
    elif drift == "target":
        rule["target"]["name"] = "Recultivation, bauxite mine"
    before = deepcopy(r)
    if drift:
        with pytest.raises(ValueError):
            apply_ilmenite_recultivation_39_review(r)
    else:
        apply_ilmenite_recultivation_39_review(r)
    assert r == before
