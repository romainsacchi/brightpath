"""Recipe-local parameter scopes survive both independently implemented materializers."""

from copy import deepcopy

import pytest

from brightpath import BiosphereProfile, TechnosphereProfile
from brightpath.background import PackageCatalogProvider, uvek_import
from brightpath.background.uvek_export import apply_uvek_export
from brightpath.core.policies import MigrationPolicy
from brightpath.exceptions import MigrationError
from brightpath.migrations.schema import intern_review, normalize_migration_resource


@pytest.fixture
def parameter_resource(migration_resource):
    resource = deepcopy(migration_resource)
    resource["quality"] = "reviewed"
    ref = intern_review(
        resource, {"rationale": "Synthetic parameterized treatment.", "evidence": ["Synthetic fixture"]}
    )
    for rule in resource["replace"]:
        rule.update(
            target={"recipe_ref": "main"}, conversion_factor=-1, classification="correspondence", review_ref=ref
        )
    for rule in resource["replace"]:
        rule["reference_flow_sign_review"] = {
            "status": "verified",
            "source_sign": -1,
            "target_sign": 1,
            "reason": "Synthetic negative waste reference to positive service.",
            "evidence": ["Synthetic fixture"],
        }
    bio = {"name": "Carbon dioxide, fossil", "categories": ["air"], "unit": "kilogram", "type": "biosphere"}
    resource["recipes"] = {
        "main": {
            "name": "Parameterized treatment",
            "reference product": "treatment",
            "location": "CH",
            "unit": "kilogram",
            "parameters": [
                {"name": "reference", "amount": -1},
                {
                    "name": "dose",
                    "amount": 1,
                    "uncertainty type": 2,
                    "loc": 0,
                    "scale": 0.2,
                    "extra": {"preserved": [1]},
                },
            ],
            "inputs": [{**bio, "amount": 1, "formula": "ABS(reference) * dose"}, {"recipe": "child", "amount": 3}],
            "rationale": "Synthetic parent scope.",
            "evidence": ["Synthetic fixture"],
        },
        "child": {
            "name": "Other local scope",
            "reference product": "other treatment",
            "location": "CH",
            "unit": "kilogram",
            "parameters": [{"name": "reference", "amount": 2}],
            "inputs": [{**bio, "amount": 2, "formula": "ABS(reference)"}],
            "rationale": "Synthetic independent dependency scope.",
            "evidence": ["Synthetic fixture"],
        },
    }
    return resource


def materialize(resource, direction, amount, monkeypatch):
    rule = resource["replace"][0]
    fg = {"name": "Caller", "reference product": "caller product", "location": "CH", "unit": "unit"}
    data = [
        {
            **fg,
            "parameters": [{"name": "dose", "amount": 99}],
            "exchanges": [
                {**fg, "type": "production", "amount": 1},
                {
                    **rule["source"],
                    "type": "technosphere",
                    "amount": amount,
                    "formula": "dose",
                    "uncertainty type": 3,
                    "loc": amount,
                    "scale": 0.1,
                },
            ],
        }
    ]
    if direction == "forward":
        normalize_migration_resource(resource)
        monkeypatch.setattr(uvek_import, "load_uvek_technosphere_resource", lambda: resource)
        report, helpers = uvek_import.apply_uvek_import_recipes(
            data,
            TechnosphereProfile("ecoinvent", "3.12", "cutoff"),
            BiosphereProfile("ecoinvent", "3.10"),
            PackageCatalogProvider(),
            database_name="synthetic",
            policy=MigrationPolicy.permissive(),
        )
    else:
        resource["source_profiles"] = [{"family": "uvek", "version": "2025", "system_model": "cutoff"}]
        resource["target_profile"] = {"family": "ecoinvent", "version": "3.12", "system_model": "cutoff"}
        report, helpers = apply_uvek_export(data, MigrationPolicy.permissive(), resource=resource)
    assert not report.has_errors
    return data, {h["brightpath_conversion"]["recipe_id"]: h for h in helpers}


@pytest.mark.parametrize("direction", ("forward", "reverse"))
@pytest.mark.parametrize("amount", (-2, 0, 2))
def test_parameters_formulas_and_shared_dependency_scopes_are_preserved(
    parameter_resource, direction, amount, monkeypatch
):
    saved = deepcopy(parameter_resource)
    data, helpers = materialize(parameter_resource, direction, amount, monkeypatch)
    assert helpers.keys() == {"main", "child"}
    for rid, helper in helpers.items():
        assert helper["parameters"] == saved["recipes"][rid]["parameters"]
        assert helper["parameters"] is not parameter_resource["recipes"][rid]["parameters"]
        (original,) = [e for e in saved["recipes"][rid]["inputs"] if e.get("type") == "biosphere"]
        (final,) = [e for e in helper["exchanges"] if e["type"] == "biosphere"]
        assert final["formula"] == original["formula"]
        variables = {p["name"]: p["amount"] for p in helper["parameters"]}
        assert eval(final["formula"], {"__builtins__": {}, "ABS": abs}, variables) == final["amount"]
    assert data[0]["parameters"] == [{"name": "dose", "amount": 99}]
    assert data[0]["exchanges"][1]["amount"] == -amount
    assert data[0]["exchanges"][1]["formula"] == "(dose) * (-1.0)"
    assert data[0]["exchanges"][1]["loc"] == -amount
    assert data[0]["exchanges"][1]["scale"] == 0.1
    helpers["main"]["parameters"][1]["extra"]["preserved"].append(2)
    assert parameter_resource["recipes"]["main"] == saved["recipes"]["main"]


@pytest.mark.parametrize("direction", ("forward", "reverse"))
def test_parameter_changes_change_generated_identity(parameter_resource, direction, monkeypatch):
    _, old = materialize(deepcopy(parameter_resource), direction, -1, monkeypatch)
    parameter_resource["recipes"]["main"]["parameters"][1]["amount"] = 2
    parameter_resource["recipes"]["main"]["inputs"][0]["amount"] = 2
    _, new = materialize(parameter_resource, direction, -1, monkeypatch)
    assert old["main"]["code"] != new["main"]["code"]
    assert old["child"]["code"] == new["child"]["code"]


@pytest.mark.parametrize(
    "parameters",
    (
        None,
        {},
        [{"name": "x"}],
        [{"amount": 1}],
        [{"name": "", "amount": 1}],
        [{"name": "x", "amount": True}],
        [{"name": "x", "amount": float("nan")}],
        [{"name": "x", "amount": 1, "scale": -1}],
        [{"name": "x", "amount": 1, "minimum": 2, "maximum": 1}],
        [{"name": "x", "amount": 1}, {"name": "x", "amount": 2}],
    ),
)
def test_invalid_parameter_metadata_is_rejected_without_mutation(parameter_resource, parameters):
    parameter_resource["recipes"]["main"]["parameters"] = parameters
    before = repr(parameter_resource)
    with pytest.raises(MigrationError):
        normalize_migration_resource(parameter_resource)
    assert repr(parameter_resource) == before
