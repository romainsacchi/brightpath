"""Native SO2 supply and explicit sodium-salt recipes retain reviewed boundaries."""

import json
from copy import deepcopy

import pytest
from test_uvek_reference_signs import consumer, contexts

from brightpath import (
    DATA_DIR,
    BackgroundContext,
    BiosphereProfile,
    BrightwayInventory,
    FormatProfile,
    InventoryContext,
    TechnosphereProfile,
)
from brightpath.background.rule_approvals import compatibility_rule_fingerprint
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource
from scripts.review_uvek_pulp_heat_so2_sulfite import apply_pulp_heat_so2_sulfite_review

REVIEW = json.loads((DATA_DIR / "export/uvek_pulp_heat_so2_sulfite_review.json").read_text())
SALTS = [r for r in REVIEW["records"] if r["source_witness"]["identity"]["reference product"] == "sodium sulfite"]


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
            for row in (REVIEW["records"] if rows is None else rows)
        ),
        on_information_loss=PolicyAction.WARN,
    )


def check_helpers(records):
    helpers = {a["brightpath_conversion"]["recipe_id"]: a for a in records[1:]}
    for rid, recipe in REVIEW["recipes"].items():
        if rid not in helpers:
            continue
        actual = helpers[rid]
        assert actual.get("parameters") == recipe.get("parameters")
        assert [x["amount"] for x in actual["exchanges"] if x["type"] == "production"] == [1]
        inputs = [x for x in actual["exchanges"] if x["type"] != "production"]
        assert len(inputs) == len(recipe["inputs"])
        for edge, expected in zip(inputs, recipe["inputs"], strict=True):
            for key, value in expected.items():
                if key == "recipe":
                    child = helpers[value]
                    assert edge["name"] == child["name"]
                    assert edge.get("reference product", edge.get("product")) == child["reference product"]
                    assert edge["location"] == child["location"] and edge["unit"] == child["unit"]
                elif key == "categories":
                    assert tuple(edge[key]) == tuple(value)
                else:
                    assert edge[key] == value


@pytest.mark.parametrize("row", REVIEW["records"], ids=lambda row: row["rule_id"])
@pytest.mark.parametrize("amount", [-2, 0, 2])
@pytest.mark.parametrize("uncertainty", [2, 3])
@pytest.mark.parametrize("release", ["2025", "2026"])
def test_signed_callers_and_all_new_recipe_fields_are_preserved(row, amount, uncertainty, release):
    data = consumer(row["source_witness"]["identity"], amount, uncertainty)
    original = deepcopy(data)
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.9")).migrate_background(
        contexts("uvek", release).background, policy=policy()
    )
    assert data == original
    actual = result.data[0]["exchanges"][1]
    for key in ("amount", "uncertainty type", "loc", "scale", "negative", "minimum", "maximum", "formula"):
        if key in original[0]["exchanges"][1]:
            assert actual[key] == original[0]["exchanges"][1][key]
    target = row["compiled_rule"]["target"]
    if "recipe_ref" in target:
        assert any(a["brightpath_conversion"]["recipe_id"] == target["recipe_ref"] for a in result.data[1:])
    else:
        assert len(result.data) == 1 and all(actual[k] == v for k, v in target.items())
    check_helpers(result.data)


@pytest.mark.parametrize("row", SALTS, ids=lambda row: row["rule_id"])
@pytest.mark.parametrize("biosphere", ["3.9", "3.10"])
def test_sodium_salt_recipes_bind_the_canonical_biosphere_independently(row, biosphere):
    source = InventoryContext(
        FormatProfile("brightway_excel"),
        BackgroundContext(TechnosphereProfile("ecoinvent", "3.9", "cutoff"), BiosphereProfile("ecoinvent", biosphere)),
    )
    result = BrightwayInventory.from_data(
        consumer(row["source_witness"]["identity"], 1), context=source
    ).migrate_background(contexts("uvek", "2026").background, policy=policy())
    check_helpers(result.data)


@pytest.mark.parametrize("row", REVIEW["records"], ids=lambda row: row["rule_id"])
def test_other_profile_or_peer_approval_cannot_grant_consent(row):
    data = consumer(row["source_witness"]["identity"], 1)
    before = deepcopy(data)
    peers = [r for r in REVIEW["records"] if r["rule_id"] != row["rule_id"]]
    with pytest.raises(MigrationError) as caught:
        BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.9")).migrate_background(
            contexts("uvek", "2026").background, policy=policy(peers)
        )
    codes = {i["code"] for s in caught.value.report.to_dict()["stages"] for i in s.get("issues", [])}
    assert "migration.unapproved_correspondence" in codes
    for version, model in (("3.10", "cutoff"), ("3.12", "cutoff"), ("3.9", "consequential")):
        with pytest.raises(MigrationError):
            BrightwayInventory.from_data(data, context=contexts("ecoinvent", version, model)).migrate_background(
                contexts("uvek", "2026").background, policy=policy()
            )
    assert data == before


def test_proxy_boundaries_salt_reference_and_complete_source_rows_are_explicit():
    assert len(REVIEW["records"]) == 5 and len(REVIEW["recipes"]) == 4
    assert len(REVIEW["recipe_dependencies"]) == 17
    assert REVIEW["new_source_omissions"] == [] and REVIEW["inherited_source_omissions"] == {}
    so2 = [
        r for r in REVIEW["records"] if r["source_witness"]["identity"]["reference product"] == "sulfur dioxide, liquid"
    ]
    assert sum(len(r["component_bindings"]) for r in so2) == 15
    row = next(r for r in so2 if r["source_witness"]["identity"]["location"] == "RoW")
    producers = [x for x in row["component_bindings"] if x["kind"] != "freight"]
    assert len(producers) == 4 and {x["target"]["name"] for x in producers} == {"Sulphur dioxide, liquid, at plant"}
    assert "18.6519413306824%" in row["limitations"][0]
    assert "not a reconstruction" in row["limitations"][0]
    assert sum(r["source_witness"]["nonproduction_rows"] for r in SALTS) == 43
    assert all(r["source_witness"]["reference_amounts"] == [1] for r in SALTS)
    assert all(x.get("name") != "Sulphite, at plant" for r in REVIEW["recipes"].values() for x in r["inputs"])


@pytest.mark.parametrize("drift", [None, "parent", "recipe", "dependency"])
def test_replay_is_idempotent_or_refuses_atomically(drift):
    resource = deepcopy(load_uvek_technosphere_resource())
    if drift == "parent":
        next(r for r in resource["replace"] if r["id"] == REVIEW["records"][-1]["rule_id"])["confidence"] = 0.1
    elif drift == "recipe":
        resource["recipes"][next(iter(REVIEW["recipes"]))]["inputs"][0]["amount"] = 9
    elif drift == "dependency":
        resource["recipes"][next(iter(REVIEW["recipe_dependencies"]))]["inputs"][0]["amount"] = 9
    before = deepcopy(resource)
    if drift is None:
        apply_pulp_heat_so2_sulfite_review(resource)
    else:
        with pytest.raises(ValueError):
            apply_pulp_heat_so2_sulfite_review(resource)
    assert resource == before
