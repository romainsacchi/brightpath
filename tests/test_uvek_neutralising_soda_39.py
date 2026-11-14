"""Light-soda delivery and neutralising capacity retain distinct quantity bases."""

import json
from copy import deepcopy

import pytest
from test_uvek_reference_signs import consumer, contexts

from brightpath import DATA_DIR, BrightwayInventory, TechnosphereProfile
from brightpath.background.rule_approvals import compatibility_rule_fingerprint
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource

REVIEW = json.loads((DATA_DIR / "export/uvek_neutralising_soda_39_review.json").read_text())
ROWS = REVIEW["records"]
MARKET, SERVICE = ROWS


def policy(approved=True):
    resource = load_uvek_technosphere_resource()
    ids = {row["rule_id"] for row in ROWS}
    fingerprints = tuple(
        compatibility_rule_fingerprint(
            resource,
            "replace",
            rule,
            TechnosphereProfile("ecoinvent", "3.9", "cutoff"),
            TechnosphereProfile("uvek", "2025", "cutoff"),
        )
        for rule in resource["replace"]
        if rule["id"] in ids
    )
    return MigrationPolicy(
        approved_compatibility_rules=fingerprints if approved else (),
        on_information_loss=PolicyAction.WARN,
    )


@pytest.mark.parametrize("row", ROWS, ids=["delivered-soda", "neutralising-capacity"])
@pytest.mark.parametrize("amount", [-2.5, 2.5])
@pytest.mark.parametrize("uncertainty", [2, 3])
@pytest.mark.parametrize("release", ["2025", "2026"])
def test_nested_functional_demand_retains_quantity_uncertainty_and_delivery(row, amount, uncertainty, release):
    data = consumer(row["original_rule"]["source"], amount, uncertainty)
    before = deepcopy(data)
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.9")).migrate_background(
        contexts("uvek", release).background, policy=policy()
    )
    assert data == before
    helpers = {a["brightpath_conversion"]["recipe_id"]: a for a in result.data[1:]}
    assert set(helpers) == ({MARKET["recipe_id"]} if row == MARKET else set(REVIEW["recipes"]))
    edge = result.data[0]["exchanges"][1]
    assert all(edge[k] == helpers[row["recipe_id"]][k] for k in ["name", "reference product", "location", "unit"])
    for key in ("amount", "uncertainty type", "loc", "scale", "negative", "formula", "minimum", "maximum"):
        if key in before[0]["exchanges"][1]:
            assert edge[key] == before[0]["exchanges"][1][key]
    for key, helper in helpers.items():
        actual = [e for e in helper["exchanges"] if e["type"] != "production"]
        expected = REVIEW["recipes"][key]["inputs"]
        assert len(actual) == len(expected)
        for exchange, definition in zip(actual, expected, strict=True):
            assert all(exchange[k] == v for k, v in definition.items() if k != "recipe")
            if "recipe" in definition:
                assert all(
                    exchange[k] == helpers[definition["recipe"]][k]
                    for k in ["name", "reference product", "unit", "location"]
                )
        assert not helper["brightpath_conversion"].get("source_input_omissions")
    names = [e["name"] for a in result.data for e in a["exchanges"]]
    assert not any("Lithium hydroxide" in n or "Light emitting diode" in n for n in names)


def test_capacity_is_not_physical_soda_mass_and_internal_statistics_survive():
    (edge,) = REVIEW["recipes"][SERVICE["recipe_id"]]["inputs"]
    assert edge["amount"] == 1.325 and edge["uncertainty type"] == 2
    assert edge["loc"] == 0.2814124594381855 and edge["scale"] == 0.10295630140987
    assert edge["pedigree"]["reliability"] == 4
    assert edge["recipe"] == MARKET["recipe_id"]
    entries = REVIEW["recipes"][MARKET["recipe_id"]]["inputs"]
    assert [e["amount"] for e in entries] == [0.7763011797364658, 0.223698820263534, 0.1546, 0.0246, 0.1044, 0.599]
    assert all(e["name"] == "Soda, powder, at plant" for e in entries[:2])
    assert SERVICE["conversion_factor"] == MARKET["conversion_factor"] == 1
    assert all(r["source_witness"]["reference_amounts"] == [1] for r in ROWS)
    assert all(w["reference_amounts"] == [1] for r in ROWS for w in r["native_witnesses"].values())
    assert REVIEW["new_source_omissions"] == []


@pytest.mark.parametrize("row", ROWS)
def test_each_mapping_requires_own_permission(row):
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(
            consumer(row["original_rule"]["source"], 1), context=contexts("ecoinvent", "3.9")
        ).migrate_background(contexts("uvek", "2026").background, policy=policy(False))


@pytest.mark.parametrize(
    "version,model", [("3.8", "cutoff"), ("3.9.1", "cutoff"), ("3.10", "cutoff"), ("3.9", "consequential")]
)
def test_new_permission_does_not_authorize_other_profiles(version, model):
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(
            consumer(SERVICE["original_rule"]["source"], 1), context=contexts("ecoinvent", version, model)
        ).migrate_background(contexts("uvek", "2026").background, policy=policy())


@pytest.mark.parametrize("drift", [None, "parent", "delivery", "capacity"])
def test_frozen_replay_is_idempotent_and_refuses_drift_atomically(drift):
    from scripts.review_uvek_neutralising_soda_39 import apply_neutralising_soda_39_review

    resource = deepcopy(load_uvek_technosphere_resource())
    if drift == "parent":
        next(r for r in resource["replace"] if r["id"] == SERVICE["rule_id"])["confidence"] = 0.001
    elif drift == "delivery":
        resource["recipes"][MARKET["recipe_id"]]["inputs"][-1]["amount"] *= 2
    elif drift == "capacity":
        resource["recipes"][SERVICE["recipe_id"]]["inputs"][0]["amount"] = 1
    before = deepcopy(resource)
    if drift:
        with pytest.raises(ValueError):
            apply_neutralising_soda_39_review(resource)
    else:
        apply_neutralising_soda_39_review(resource)
    assert resource == before
