"""Existing waste rules preserve service units and signed uncertainty without new helpers."""

import hashlib
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

REVIEW = json.loads((DATA_DIR / "export/uvek_direct_waste_services_39_review.json").read_text())
ROWS = REVIEW["records"]


def policy(approved=True):
    return MigrationPolicy(
        approved_compatibility_rules=tuple(r["scope_fingerprint"] for r in ROWS) if approved else (),
        on_information_loss=PolicyAction.WARN,
    )


@pytest.mark.parametrize("row", ROWS, ids=lambda r: r["rule_id"])
@pytest.mark.parametrize("amount", [-2.5, 2.5])
@pytest.mark.parametrize("uncertainty", [2, 3])
@pytest.mark.parametrize("release", ["2025", "2026"])
def test_negative_waste_and_positive_avoided_disposal_demands_keep_service_units(row, amount, uncertainty, release):
    data = consumer(row["rule"]["source"], amount, uncertainty)
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
    edge = result.data[0]["exchanges"][1]
    expected = deepcopy(before[0]["exchanges"][1])
    scale_exchange(expected, -1)
    assert edge["amount"] == -amount
    assert (
        edge["amount"] / row["boundary_checks"]["native_reference_amount"]
        == amount / row["boundary_checks"]["source_reference_amount"]
    )
    for key in ("amount", "uncertainty type", "loc", "scale", "negative", "formula", "minimum", "maximum"):
        if key in expected:
            assert edge[key] == expected[key]
    assert all(edge[k] == v for k, v in row["rule"]["target"].items())
    assert result.data[0]["exchanges"][-1] == before[0]["exchanges"][-1]
    assert not any(
        loss.code == "migration.recipe_source_input_omission" for loss in result.last_migration_report.losses
    )


def test_all_current_rules_and_exact_fingerprints_are_preserved():
    resource = load_uvek_technosphere_resource()
    for row in ROWS:
        rule = next(r for r in resource["replace"] if r["id"] == row["rule_id"])
        assert rule == row["rule"] and rule["conversion_factor"] == -1
        assert row["source_witness"]["reference_amounts"] == [-1]
        assert all(w["reference_amounts"] == [1] and w["nonproduction_count"] for w in row["native_witnesses"].values())
        assert (
            compatibility_rule_fingerprint(
                resource,
                "replace",
                rule,
                TechnosphereProfile(**REVIEW["source_profile"]),
                TechnosphereProfile(**REVIEW["target_rule_profile"]),
            )
            == row["scope_fingerprint"]
        )
        for prior in row["prior_approvals_unchanged"]:
            assert hashlib.sha256((DATA_DIR / prior["review_path"]).read_bytes()).hexdigest() == prior["review_sha256"]
            for scope in prior["scopes"]:
                assert (
                    compatibility_rule_fingerprint(
                        resource,
                        "replace",
                        rule,
                        TechnosphereProfile(**scope["source"]),
                        TechnosphereProfile(**scope["target"]),
                    )
                    == scope["fingerprint"]
                )
    assert REVIEW["new_helpers"] == 0 and REVIEW["migration_resource_changes"] == []
    assert REVIEW["new_source_omissions"] == REVIEW["runtime_python_changes"] == []


@pytest.mark.parametrize("row", ROWS, ids=lambda r: r["rule_id"])
def test_each_rule_requires_its_exact_profile_permission(row):
    data = consumer(row["rule"]["source"], -1)
    before = deepcopy(data)
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.9")).migrate_background(
            contexts("uvek", "2026").background, policy=policy(False)
        )
    assert data == before


@pytest.mark.parametrize(
    "version,model",
    [("3.6", "cutoff"), ("3.8", "cutoff"), ("3.9.1", "cutoff"), ("3.10", "cutoff"), ("3.9", "consequential")],
)
def test_new_permission_does_not_authorize_other_profiles(version, model):
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(
            consumer(ROWS[0]["rule"]["source"], -1), context=contexts("ecoinvent", version, model)
        ).migrate_background(contexts("uvek", "2026").background, policy=policy())


def test_native_model_limits_and_unreviewed_portfolios_are_explicit():
    text = " ".join(limit for r in ROWS for limit in r["limitations"])
    assert "land-use-only non-sulfidic model" in text
    assert "60000-year leachate model" in text
    assert "25%water" in text and "0.4kg cement" in text
    assert "0.015tkm" in text and "32.89MJ/kg" in text and "27.77MJ/kg" in text
    assert "8744.7and8610.5ppm" in text
    # Five groups cover six source markets; spent solvents have CH and Europe cases.
    assert len(REVIEW["unreviewed_portfolio_cases"]) == 5
    assert "spent-solvent markets CH/Europe" in REVIEW["unreviewed_portfolio_cases"]
