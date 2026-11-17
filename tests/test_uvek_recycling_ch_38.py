"""Required recycling keeps paid operations, treatment fates and stock signs."""

import hashlib
import json
from copy import deepcopy

import pytest
from test_uvek_reference_signs import consumer, contexts

from brightpath import DATA_DIR, BrightwayInventory, TechnosphereProfile
from brightpath.background.reference_signs import select_profile_sign_rules
from brightpath.background.rule_approvals import compatibility_rule_fingerprint
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource

REVIEW = json.loads((DATA_DIR / "export/uvek_recycling_ch_38_review.json").read_text())
ROWS = [r for r in REVIEW["records"] if not r.get("parent_only_zero_authority")]


def policy(approved=True):
    resource = load_uvek_technosphere_resource()
    rules = {r["id"]: r for r in resource["replace"]}
    scopes = tuple(
        compatibility_rule_fingerprint(
            resource,
            "replace",
            rules[row["rule_id"]],
            TechnosphereProfile(**REVIEW["source_profile"]),
            TechnosphereProfile(**REVIEW["target_profile"]),
        )
        for row in ROWS
    )
    return MigrationPolicy(
        approved_compatibility_rules=scopes if approved else (), on_information_loss=PolicyAction.WARN
    )


@pytest.mark.parametrize("amount", [-2.5, 2.5])
@pytest.mark.parametrize("uncertainty", [2, 3])
@pytest.mark.parametrize("release", ["2025", "2026"])
def test_recycled_resin_preserves_signed_demands_and_nonempty_paid_operations(amount, uncertainty, release):
    data = consumer(ROWS[0]["source"], amount, uncertainty)
    data[0]["exchanges"].append(consumer(ROWS[1]["source"], amount, uncertainty)[0]["exchanges"][1])
    original = deepcopy(data)
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.8")).migrate_background(
        contexts("uvek", release).background, policy=policy()
    )
    assert data == original
    helpers = {a["brightpath_conversion"]["recipe_id"]: a for a in result.data[1:]}
    for edge, old in zip(result.data[0]["exchanges"][1:], original[0]["exchanges"][1:], strict=True):
        for k in ("amount", "loc", "scale", "negative", "minimum", "maximum", "formula", "uncertainty type"):
            if k in old:
                assert edge[k] == old[k]
    paid = [
        a
        for a in helpers.values()
        if a["name"].startswith(
            (
                "polyethylene production, high density, granulate, recycled,",
                "polyethylene terephthalate production, granulate, amorphous, recycled,",
            )
        )
    ]
    assert len(paid) == 2
    assert all(len(a["exchanges"]) > 9 and not a["brightpath_conversion"].get("zero_burden") for a in paid)
    assert all(any(e["type"] == "biosphere" for e in a["exchanges"]) for a in paid)
    assert not any(e["name"].startswith("xx Recycling") for a in paid for e in a["exchanges"])
    zeros = [a for a in helpers.values() if a["brightpath_conversion"].get("zero_burden")]
    assert len(zeros) >= 7 and all(len(a["exchanges"]) == 1 for a in zeros)
    assert all(
        "Recycled Content cut-off" in a["name"]
        or a["name"].startswith("Allocated cut-off stock,")
        or "untreated waste wood" in a["name"]
        for a in zeros
    )
    pet = helpers[next(r["target"]["recipe_ref"] for r in ROWS if "terephthalate" in r["source"]["name"])]
    resin = [e for e in pet["exchanges"][1:] if e["unit"] == "kilogram"]
    recycled = [e for e in resin if "recycled to generic" in e["name"]]
    assert len(resin) == 7 and len(recycled) == 4
    assert sum(e["amount"] for e in resin) == pytest.approx(1)
    assert sum(e["amount"] for e in recycled) == pytest.approx(0.000999000999001)
    for kind in ("090187e1cd9f8fc7", "4fd6d63ab0a5b89b", "84dcff7694e15d91"):
        market = helpers["recycling-38-ch-waste-" + kind]
        treatment = [e for e in market["exchanges"][1:] if e["unit"] == "kilogram"]
        freight = [e for e in market["exchanges"][1:] if e["unit"] == "ton kilometer"]
        assert len(treatment) == 3 and all(e["amount"] > 0 for e in treatment + freight)
        assert sum(e["amount"] for e in treatment) == pytest.approx(1, abs=3e-7)
        assert any("open-burning" in e["name"].lower() or "open burning" in e["name"] for e in treatment)
        assert any("open-dump" in e["name"].lower() or "open dump" in e["name"] for e in treatment)
    gas = helpers["recycling-38-ch-heat-96a0736abf7d8584"]
    assert len(gas["exchanges"]) == 4
    assert all("natural gas, allocation exergy" in e["name"] for e in gas["exchanges"][1:])
    nongas = helpers["recycling-38-ch-heat-e1e78b3a4f5e06c7"]
    recovered = [
        e for e in nongas["exchanges"] if e["name"] == "Heat from waste, at municipal waste incineration plant"
    ]
    assert len(recovered) == 1 and recovered[0]["amount"] == pytest.approx(0.942951152694128)
    for a in helpers.values():
        if "parameters" in a:
            parameters = {p["name"]: p["amount"] for p in a["parameters"]}
            for e in a["exchanges"]:
                if e.get("formula"):
                    assert eval(e["formula"], {"__builtins__": {}}, parameters) == pytest.approx(e["amount"])


def test_prior_pet_profiles_and_borrowed_models_remain_unchanged():
    resource = load_uvek_technosphere_resource()
    for op in REVIEW["preserved_prior_approval_operations"]:
        row = next(r for r in ROWS if r["rule_id"] == op["rule_id"])
        for scope in op["scopes"]:
            p = TechnosphereProfile(**scope["source"])
            actual = next(r for r in select_profile_sign_rules(resource, p)["replace"] if r["id"] == row["rule_id"])
            original = select_profile_sign_rules({**resource, "replace": [row["original_rule"]]}, p)["replace"][0]
            assert actual == original
    for recipe, expected in REVIEW["recipe_dependency_sha256"].items():
        raw = json.dumps(resource["recipes"][recipe], sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        assert hashlib.sha256(raw.encode()).hexdigest() == expected


@pytest.mark.parametrize("version,model", [("3.8.1", "cutoff"), ("3.8", "consequential")])
def test_uninspected_profiles_still_refuse(version, model):
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(
            consumer(ROWS[0]["source"], 1), context=contexts("ecoinvent", version, model)
        ).migrate_background(contexts("uvek", "2026").background, policy=policy())


def test_without_exact_parent_permission_recycling_still_refuses():
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(
            consumer(ROWS[0]["source"], 1), context=contexts("ecoinvent", "3.8")
        ).migrate_background(contexts("uvek", "2026").background, policy=policy(False))
