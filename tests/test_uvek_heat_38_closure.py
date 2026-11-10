"""Check delivery, reference signs, uncertainty and exact heat-profile refusals."""

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
from scripts.review_uvek_heat_38_closure import apply_heat_38_closure_review

REVIEW = json.loads((DATA_DIR / "export/uvek_heat_38_closure_review.json").read_text())


@pytest.fixture(scope="module")
def policy():
    resource = load_uvek_technosphere_resource()
    indexed = {r["id"]: r for r in resource["replace"]}
    return MigrationPolicy(
        approved_compatibility_rules=tuple(
            compatibility_rule_fingerprint(
                resource,
                "replace",
                indexed[r["rule_id"]],
                TechnosphereProfile(**r["source_profile"]),
                TechnosphereProfile("uvek", "2025", "cutoff"),
            )
            for r in REVIEW["records"]
        ),
        on_information_loss=PolicyAction.WARN,
    )


@pytest.mark.parametrize("row", REVIEW["records"], ids=lambda r: r["rule_id"])
@pytest.mark.parametrize("amount,uncertainty", [(-2, 2), (2, 2), (-2, 3), (2, 3)])
def test_heat_closure_preserves_signed_reference_and_caller_uncertainty(row, amount, uncertainty, policy):
    data = consumer(row["source"], amount, uncertainty)
    before = deepcopy(data)
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.8")).migrate_background(
        contexts("uvek", "2026").background, policy=policy
    )
    assert data == before
    edge = result.data[0]["exchanges"][1]
    assert edge["amount"] == amount * row["conversion_factor"]
    assert edge["amount"] == amount / row["source_reference_amount"]
    assert edge["scale"] == before[0]["exchanges"][1]["scale"]
    if row["conversion_factor"] == 1:
        assert edge["formula"] == before[0]["exchanges"][1]["formula"]
    else:
        assert edge["formula"] == "(dose) * (-1.0)"
    if "recipe_ref" in row["target"]:
        helpers = {a["brightpath_conversion"]["recipe_id"]: a for a in result.data[1:]}
        target = helpers[row["target"]["recipe_ref"]]
        assert all(edge[f] == target[f] for f in ("name", "reference product", "location", "unit"))
        for rid, recipe in row["new_recipes"].items():
            actual = helpers[rid]["exchanges"][1:]
            assert len(actual) == len(recipe["inputs"])
            for expected, mapped in zip(recipe["inputs"], actual, strict=True):
                for key in (
                    "amount",
                    "formula",
                    "uncertainty type",
                    "loc",
                    "scale",
                    "minimum",
                    "maximum",
                    "negative",
                    "pedigree",
                ):
                    if key in expected:
                        assert mapped[key] == expected[key]
    else:
        assert len(result.data) == 1
        assert all(edge[k] == v for k, v in row["target"].items())


@pytest.mark.parametrize("row", REVIEW["records"], ids=lambda r: r["rule_id"])
@pytest.mark.parametrize("version,model", [("3.11", "cutoff"), ("3.8", "consequential")])
def test_closure_approval_cannot_escape_exact_profile(row, version, model, policy):
    data = consumer(row["source"], -1)
    before = deepcopy(data)
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(data, context=contexts("ecoinvent", version, model)).migrate_background(
            contexts("uvek", "2026").background, policy=policy
        )
    assert data == before


@pytest.mark.parametrize("drift", ["none", "rule", "import", "steam"])
def test_authoring_batch_is_atomic_and_existing_helpers_cannot_drift(drift):
    resource = deepcopy(load_uvek_technosphere_resource())
    # Replay the original 3.8 authoring stage, before the independently reviewed
    # 3.9 oxide extension. Current full replay and 3.9 tests cover that extension.
    path = DATA_DIR / "export/uvek_fuels_39_oxide_services_review.json"
    assert hashlib.sha256(path.read_bytes()).hexdigest() == (
        "ab280c0ca2a309588ca89c2dbcc4e4d4d374bf9c7e97d64925c9936c2c38fb45"
    )
    from brightpath.background.reference_signs import select_profile_sign_rules

    original_ids = {r["rule_id"] for r in REVIEW["records"]}
    restored = 0
    for row in json.loads(path.read_text())["records"]:
        if row["rule_id"] not in original_ids:
            continue
        current = next(r for r in resource["replace"] if r["id"] == row["rule_id"])
        profile = TechnosphereProfile("ecoinvent", "3.8", "cutoff")
        assert (
            select_profile_sign_rules({"replace": [current]}, profile)["replace"]
            == select_profile_sign_rules({"replace": [row["original_rule"]]}, profile)["replace"]
        )
        current.clear()
        current.update(deepcopy(row["original_rule"]))
        restored += 1
    assert restored == 3
    apply_heat_38_closure_review(resource)
    if drift == "rule":
        next(r for r in resource["replace"] if r["id"] == REVIEW["records"][-1]["rule_id"])["source"][
            "unit"
        ] = "kilogram"
    elif drift == "import":
        row = next(r for r in REVIEW["records"] if r["rule_id"] == "bp-57c9160454525453")
        rid = next(k for k, r in row["new_recipes"].items() if "import from Northern America" in r["name"])
        resource["recipes"][rid]["inputs"][-1]["amount"] = 0
    elif drift == "steam":
        row = REVIEW["records"][-1]
        resource["recipes"][row["target"]["recipe_ref"]]["inputs"][1]["amount"] = 0
    if drift != "none":
        first = REVIEW["records"][0]
        resource["replace"] = [
            deepcopy(first["previous_rule"]) if r["id"] == first["rule_id"] else r for r in resource["replace"]
        ]
    before = deepcopy(resource)
    if drift == "none":
        apply_heat_38_closure_review(resource)
    else:
        with pytest.raises(ValueError):
            apply_heat_38_closure_review(resource)
    assert resource == before


def test_european_alumina_retains_import_and_market_shipping_as_distinct_services():
    row = next(r for r in REVIEW["records"] if r["rule_id"] == "bp-57c9160454525453")
    recipes = row["new_recipes"]
    market = recipes[row["target"]["recipe_ref"]]
    direct_ocean = [e["amount"] for e in market["inputs"] if e.get("name") == "Transport, transoceanic container ship"]
    assert direct_ocean == [0.2]
    imported_ocean = {}
    for e in market["inputs"]:
        if "recipe" in e:
            helper = recipes[e["recipe"]]
            ocean = [x["amount"] for x in helper["inputs"] if x.get("name") == "Transport, transoceanic container ship"]
            assert len(ocean) == 1
            imported_ocean[helper["name"].split(", source delivery preserved")[0]] = (e["amount"], ocean[0])
    assert sorted(imported_ocean.values()) == sorted(
        [(0.012869604586062, 6.5755), (0.02144934097677, 0.2), (0.394667873972568, 7.5)]
    )
    assert len(market["inputs"]) == 8 and len(recipes) == 4


def test_exact_steam_recipe_keeps_mj_basis_and_water_releases():
    resource = load_uvek_technosphere_resource()
    row = next(r for r in REVIEW["records"] if r["rule_id"] == "bp-46a8f88f9ecae2d1")
    assert row["resource_action"] == "unchanged_rule" and row["new_recipes"] == {}
    inputs = resource["recipes"][row["target"]["recipe_ref"]]["inputs"]
    assert [e["amount"] for e in inputs if e["unit"] == "megajoule"] == [1]
    assert len([e for e in inputs if e["type"] == "technosphere" and e["unit"] == "kilogram"]) == 7
    assert sorted((e["categories"], e["amount"]) for e in inputs if e["type"] == "biosphere") == [
        (["air"], 9.27272727272727e-6),
        (["water"], 5.25454545454545e-5),
    ]
