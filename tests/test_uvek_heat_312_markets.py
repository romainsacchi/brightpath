"""Check market/service quantities, uncertainty and exact heat-profile refusals."""

import json
from copy import deepcopy

import pytest
from test_uvek_reference_signs import consumer, contexts

from brightpath import DATA_DIR, BrightwayInventory, TechnosphereProfile
from brightpath.background.rule_approvals import compatibility_rule_fingerprint
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource
from scripts.review_uvek_heat_312_markets import apply_heat_312_markets_review

REVIEW = json.loads((DATA_DIR / "export/uvek_heat_312_markets_review.json").read_text())


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
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.12")).migrate_background(
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
@pytest.mark.parametrize("version,model", [("3.11", "cutoff"), ("3.12", "consequential")])
def test_closure_approval_cannot_escape_exact_profile(row, version, model, policy):
    data = consumer(row["source"], -1)
    before = deepcopy(data)
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(data, context=contexts("ecoinvent", version, model)).migrate_background(
            contexts("uvek", "2026").background, policy=policy
        )
    assert data == before


@pytest.mark.parametrize("drift", ["none", "rule", "recipe"])
def test_authoring_batch_is_atomic_and_existing_helpers_cannot_drift(drift):
    resource = deepcopy(load_uvek_technosphere_resource())
    apply_heat_312_markets_review(resource)
    if drift == "rule":
        next(r for r in resource["replace"] if r["id"] == REVIEW["records"][-1]["rule_id"])["source"][
            "unit"
        ] = "cubic meter"
    elif drift == "recipe":
        row = REVIEW["records"][0]
        resource["recipes"][row["target"]["recipe_ref"]]["inputs"][-1]["amount"] = 0
    if drift != "none":
        first = REVIEW["records"][0]
        resource["replace"] = [
            deepcopy(first["previous_rule"]) if r["id"] == first["rule_id"] else r for r in resource["replace"]
        ]
    before = deepcopy(resource)
    if drift == "none":
        apply_heat_312_markets_review(resource)
    else:
        with pytest.raises(ValueError):
            apply_heat_312_markets_review(resource)
    assert resource == before


def test_film_market_keeps_service_basis_and_excludes_converted_polymer():
    row = next(r for r in REVIEW["records"] if r["rule_id"] == "bp-7e5fcabe5aabe23c")
    inputs = row["new_recipes"][row["target"]["recipe_ref"]]["inputs"]
    assert len(inputs) == 3
    assert [e["amount"] for e in inputs] == [0.00444285714285715, 0.330592351639303, 0.66496479121784]
    assert all(e["name"] == "Extrusion, plastic film" and e["unit"] == "kilogram" for e in inputs)
    assert row["conversion_factor"] == 1
