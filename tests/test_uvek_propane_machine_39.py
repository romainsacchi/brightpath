"""Propane building-machine service retains fuel MJ, engine emissions and each utility demand."""

import json
from copy import deepcopy

import pytest
from test_uvek_reference_signs import consumer, contexts

from brightpath import DATA_DIR, BrightwayInventory, TechnosphereProfile
from brightpath.background.rule_approvals import compatibility_rule_fingerprint
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource

REVIEW = json.loads((DATA_DIR / "export/uvek_propane_machine_39_review.json").read_text())
ROWS = REVIEW["records"]


def policy(row, approved=True):
    resource = load_uvek_technosphere_resource()
    rule = next(r for r in resource["replace"] if r["id"] == row["rule_id"])
    fingerprint = compatibility_rule_fingerprint(
        resource,
        "replace",
        rule,
        TechnosphereProfile("ecoinvent", "3.9", "cutoff"),
        TechnosphereProfile("uvek", "2025", "cutoff"),
    )
    return MigrationPolicy(
        approved_compatibility_rules=(fingerprint,) if approved else (),
        on_information_loss=PolicyAction.WARN,
    )


@pytest.mark.parametrize("row", ROWS, ids=lambda r: r["rule_id"])
@pytest.mark.parametrize("amount", [-2.5, 2.5])
@pytest.mark.parametrize("uncertainty", [2, 3])
@pytest.mark.parametrize("release", ["2025", "2026"])
def test_machine_preserves_caller_and_all_retained_interfaces(row, amount, uncertainty, release):
    data = consumer(row["original_rule"]["source"], amount, uncertainty)
    emission = {
        "name": "Carbon dioxide, fossil",
        "categories": ["air"],
        "unit": "kilogram",
        "type": "biosphere",
        "amount": 0.5,
    }
    data[0]["exchanges"].append(emission)
    before = deepcopy(data)
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.9")).migrate_background(
        contexts("uvek", release).background, policy=policy(row)
    )
    assert data == before and len(result.data) == 1 + len(REVIEW["recipes"]) + len(REVIEW["recipe_dependencies"])
    assert len(result.data[0]["exchanges"]) == 3
    actual = result.data[0]["exchanges"][1]
    for field in ("amount", "uncertainty type", "loc", "scale", "negative", "formula", "minimum", "maximum"):
        if field in before[0]["exchanges"][1]:
            assert actual[field] == before[0]["exchanges"][1][field]
    helpers = {a["brightpath_conversion"]["recipe_id"]: a for a in result.data[1:]}
    assert set(helpers) == set(REVIEW["recipes"]) | set(REVIEW["recipe_dependencies"])
    helper = helpers[row["recipe_id"]]
    edges = [e for e in helper["exchanges"] if e["type"] != "production"]
    wanted = REVIEW["recipes"][row["recipe_id"]]["inputs"]
    assert len(edges) == len(wanted) == 27
    for edge, original in zip(edges, wanted, strict=True):
        for key in ("amount", "uncertainty type", "loc", "scale", "negative", "formula", "pedigree"):
            if key in original:
                assert edge[key] == original[key]
        target = helpers[original["recipe"]] if "recipe" in original else original
        assert edge["name"] == target["name"] and edge["unit"] == target["unit"]
    bio = [e for e in edges if e["type"] == "biosphere"]
    assert len(bio) == 20
    assert all(
        list(e["categories"]) == list(o["categories"])
        for e, o in zip(bio, [e for e in wanted if e.get("type") == "biosphere"], strict=True)
    )
    assert not any(e["name"].startswith("Diesel, burned") or "burned in boiler" in e["name"] for e in edges)
    assert result.data[0]["exchanges"][-1] == emission


@pytest.mark.parametrize("row", ROWS, ids=lambda r: r["rule_id"])
def test_grid_needs_its_own_exact_approval(row):
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(
            consumer(row["original_rule"]["source"], 1), context=contexts("ecoinvent", "3.9")
        ).migrate_background(contexts("uvek", "2026").background, policy=policy(row, False))


@pytest.mark.parametrize("row", ROWS, ids=lambda r: r["rule_id"])
@pytest.mark.parametrize(
    "version,model", [("3.8", "cutoff"), ("3.9.1", "cutoff"), ("3.10", "cutoff"), ("3.9", "consequential")]
)
def test_new_scope_does_not_authorize_other_profiles(row, version, model):
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(
            consumer(row["original_rule"]["source"], 1), context=contexts("ecoinvent", version, model)
        ).migrate_background(contexts("uvek", "2026").background, policy=policy(row))


@pytest.mark.parametrize("drift", [None, 0, 1])
def test_frozen_replay_is_atomic_and_idempotent(drift):
    from scripts.review_uvek_propane_machine_39 import apply_propane_machine_39_review

    resource = deepcopy(load_uvek_technosphere_resource())
    if drift is not None:
        next(r for r in resource["replace"] if r["id"] == ROWS[drift]["rule_id"])["confidence"] = 0.01
    before = deepcopy(resource)
    if drift is None:
        apply_propane_machine_39_review(resource)
    else:
        with pytest.raises(ValueError):
            apply_propane_machine_39_review(resource)
    assert resource == before


def test_fuel_energy_basis_and_scoped_component_decisions_are_explicit():
    assert REVIEW["new_source_omissions"] == []
    assert REVIEW["retained_biosphere_count"] == 20
    assert len(REVIEW["recipes"]) == 1
    assert all(f["same_uuid"] for f in REVIEW["biosphere_identity_checks"])
    for row in ROWS:
        assert row["source_witness"]["reference_amounts"] == [1]
        assert row["conversion_factor"] == 1
        assert row["original_rule"]["target"] == row["compiled_rule"]["target"]
    interfaces = REVIEW["component_interfaces"]
    assert len(interfaces) == 7
    assert [e["factor"] for e in interfaces[-3:]] == [-1, -1, -1]
    assert all(e["statistics"]["amount"] > 0 for e in interfaces[-3:])
    fuel = next(e for e in interfaces if "propane" == e["source"]["reference product"])
    assert fuel["statistics"]["amount"] == 0.0215238915195867
    assert fuel["native_target"]["name"] == "Liquefied petroleum gas, at service station"
    assert all("Only inside" in e["scope"] for e in interfaces)


@pytest.mark.parametrize(
    "component",
    [e for e in REVIEW["component_interfaces"] if not e["existing_scoped_correspondence_reused"]],
    ids=lambda e: e["source"]["name"] + "-" + e["source"]["location"],
)
def test_scoped_machine_proxy_does_not_approve_standalone_component(component):
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(
            consumer(component["source"], 1), context=contexts("ecoinvent", "3.9")
        ).migrate_background(contexts("uvek", "2026").background, policy=policy(ROWS[0]))
