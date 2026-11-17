"""A closed casting service retains processing, internal treatment signs and scope."""

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

REVIEW = json.loads((DATA_DIR / "export/uvek_closed_casting_39_review.json").read_text())
MODELS = json.loads((DATA_DIR / REVIEW["model_review_path"]).read_text())
ROW = REVIEW["records"][0]
FIELDS = ("name", "reference product", "location", "unit")


def policy():
    resource = load_uvek_technosphere_resource()
    rule = next(r for r in resource["replace"] if r["id"] == ROW["rule_id"])
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
def test_casting_service_preserves_caller_and_closed_recipe_quantities(amount, uncertainty, release):
    data = consumer(ROW["source"], amount, uncertainty)
    before = deepcopy(data)
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.9")).migrate_background(
        contexts("uvek", release).background, policy=policy()
    )
    assert data == before
    helpers = {a["brightpath_conversion"]["recipe_id"]: a for a in result.data[1:]}
    assert set(helpers) == set(MODELS["recipes"]) | set(MODELS["recipe_dependency_sha256"])
    got, old = result.data[0]["exchanges"][1], before[0]["exchanges"][1]
    for k in ("amount", "loc", "scale", "shape", "minimum", "maximum", "negative", "formula", "uncertainty type"):
        if k in old:
            assert got[k] == old[k]
    root = helpers[ROW["target"]["recipe_ref"]]
    assert all(got[k] == root[k] for k in FIELDS)
    producers = root["exchanges"][1:]
    assert [e["amount"] for e in producers] == [0.995, 0.005]
    assert [e["location"] for e in producers] == ["RoW", "CA-QC"]
    for rid, recipe in MODELS["recipes"].items():
        actual = helpers[rid]
        assert actual["location"] == recipe["location"] and actual["unit"] == recipe["unit"]
        assert actual["exchanges"][0]["amount"] == 1
        assert len(actual["exchanges"]) == len(recipe["inputs"]) + 1
        for edge, expected in zip(actual["exchanges"][1:], recipe["inputs"], strict=True):
            if "recipe" in expected:
                target = helpers[expected["recipe"]]
                assert all(edge[k] == target[k] for k in FIELDS)
            elif release == "2025":
                assert all(edge[k] == value for k, value in expected.items())
            for k in ("amount", "loc", "scale", "shape", "minimum", "maximum", "negative", "uncertainty type"):
                if k in expected:
                    assert edge[k] == expected[k]
    omissions = [o for a in result.data[1:] for o in a["brightpath_conversion"].get("source_input_omissions", [])]
    new_omission = MODELS["audited_empty_receipt_omissions"][0]["audit"]
    assert omissions.count(new_omission) == 1
    assert len(omissions) == 61
    assert new_omission["source_exchange"]["name"] == "clinker production"
    assert new_omission["source_exchange"]["amount"] < 0
    assert not any(a.get("formula") for h in result.data[1:] for a in h["exchanges"])


def test_all_source_positions_are_restored_and_existing_dependencies_stay_frozen():
    assert MODELS["source_foreground_exchanges_retained"] == 350
    assert sum(len(v) for v in MODELS["foreground_exchange_checksums"].values()) == 350
    assert (
        sum(b["held_waste_binding_restored"] for v in MODELS["foreground_exchange_checksums"].values() for b in v) == 13
    )
    resource = load_uvek_technosphere_resource()
    for rid, digest in MODELS["recipe_dependency_sha256"].items():
        raw = json.dumps(resource["recipes"][rid], sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        assert hashlib.sha256(raw.encode()).hexdigest() == digest
    assert MODELS["new_zero_authorities"] == 0


@pytest.mark.parametrize("version,model", [("3.10", "cutoff"), ("3.9.1", "cutoff"), ("3.9", "consequential")])
def test_uninspected_casting_profiles_are_not_admitted(version, model):
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(
            consumer(ROW["source"], 1), context=contexts("ecoinvent", version, model)
        ).migrate_background(contexts("uvek", "2026").background, policy=policy())


def test_child_omission_binding_does_not_grant_standalone_waste_permission():
    child = MODELS["records"][0]
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(
            consumer(child["source"], -1), context=contexts("ecoinvent", "3.9")
        ).migrate_background(contexts("uvek", "2026").background, policy=policy())


def test_numeric_exchange_comment_is_retained_when_recipe_adds_provenance():
    data = consumer(ROW["source"], 1)
    data[0]["exchanges"][1]["comment"] = 1.25
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.9")).migrate_background(
        contexts("uvek", "2026").background, policy=policy()
    )
    edge = result.data[0]["exchanges"][1]
    assert edge["amount"] == 1
    assert edge["comment"].startswith("1.25\nApply reviewed physical recipe")
    assert data[0]["exchanges"][1]["comment"] == 1.25
