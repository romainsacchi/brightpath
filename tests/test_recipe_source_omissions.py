"""Declared source losses stay explicit, immutable and limited to an exact owner."""

from copy import deepcopy

import pytest
import test_recipe_parameters

from brightpath import BiosphereProfile, TechnosphereProfile
from brightpath.background import PackageCatalogProvider, uvek_import
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.schema import normalize_migration_resource


@pytest.fixture
def omission_resource(migration_resource):
    resource = test_recipe_parameters.parameter_resource.__wrapped__(migration_resource)
    rule = resource["replace"][0]
    profile = resource["source_profiles"][0]
    rule["reference_flow_sign_review"]["status"] = "unresolved"
    rule["reference_flow_sign_review"]["profiles"] = [
        {
            "source_profile": deepcopy(profile),
            "target": {"recipe_ref": "main"},
            "conversion_factor": -1,
            "review": {
                "status": "verified",
                "source_sign": -1,
                "target_sign": 1,
                "reason": "Synthetic exact profile",
                "evidence": ["Synthetic fixture"],
            },
        }
    ]
    resource["replace"][1]["target"] = {"recipe_ref": "child"}
    resource["recipes"]["main"]["source_input_omissions"] = [
        {
            "source_profile": deepcopy(profile),
            "source_activity": deepcopy(rule["source"]),
            "source_inventory_sha256": "a" * 64,
            "original_index": 2,
            "source_exchange": {
                **rule["source"],
                "name": "Synthetic missing supplier",
                "type": "technosphere",
                "amount": 1e-13,
                "uncertainty type": 2,
                "loc": -29.933606208922594,
                "scale": 0.2,
            },
            "rationale": "Explicit synthetic positive trace omission, never a zero stock.",
        }
    ]
    return resource


def test_source_omission_is_a_loss_and_portable_helper_audit(omission_resource, monkeypatch):
    saved = deepcopy(omission_resource)
    data, helpers = test_recipe_parameters.materialize(omission_resource, "forward", -2, monkeypatch)
    audit = helpers["main"]["brightpath_conversion"]["source_input_omissions"]
    assert audit == saved["recipes"]["main"]["source_input_omissions"]
    assert omission_resource == saved
    audit[0]["source_exchange"]["amount"] = 9
    assert omission_resource == saved
    # Exercise the report directly; materialize intentionally returns only data/helpers.
    report, _ = uvek_import.apply_uvek_import_recipes(
        [
            {
                "name": "Fresh caller",
                "reference product": "output",
                "location": "CH",
                "unit": "unit",
                "exchanges": [{**saved["replace"][0]["source"], "type": "technosphere", "amount": -1}],
            }
        ],
        TechnosphereProfile("ecoinvent", "3.12", "cutoff"),
        BiosphereProfile("ecoinvent", "3.10"),
        PackageCatalogProvider(),
        database_name="synthetic",
        policy=MigrationPolicy.permissive(),
    )
    losses = [x for x in report.losses if x.code == "migration.recipe_source_input_omission"]
    assert len(losses) == 1
    details = losses[0].to_dict()["details"]
    assert details["omission"] == saved["recipes"]["main"]["source_input_omissions"][0]
    assert details["basis"] == {"amount": 1, "unit": "kilogram"}
    assert data[0]["exchanges"][1]["amount"] == 2


@pytest.mark.parametrize("mutation", ["profile", "activity", "duplicate", "zero", "second_owner", "unverified"])
def test_invalid_source_omission_scope_is_refused(omission_resource, mutation):
    entry = omission_resource["recipes"]["main"]["source_input_omissions"][0]
    if mutation == "profile":
        entry["source_profile"]["system_model"] = "consequential"
    elif mutation == "activity":
        entry["source_activity"]["location"] = "Elsewhere"
    elif mutation == "duplicate":
        omission_resource["recipes"]["main"]["source_input_omissions"].append(deepcopy(entry))
    elif mutation == "zero":
        entry["source_exchange"]["amount"] = 0
    elif mutation == "second_owner":
        omission_resource["replace"][1]["target"] = {"recipe_ref": "main"}
    else:
        omission_resource["replace"][0]["reference_flow_sign_review"]["profiles"][0]["review"]["status"] = "unresolved"
    before = deepcopy(omission_resource)
    with pytest.raises(MigrationError):
        normalize_migration_resource(omission_resource)
    assert omission_resource == before


def test_source_omission_strict_policy_retains_an_error(omission_resource, monkeypatch):
    normalize_migration_resource(omission_resource)
    monkeypatch.setattr(uvek_import, "load_uvek_technosphere_resource", lambda: omission_resource)
    source = omission_resource["replace"][0]["source"]
    report, _ = uvek_import.apply_uvek_import_recipes(
        [
            {
                "name": "Caller",
                "reference product": "output",
                "location": "CH",
                "unit": "unit",
                "exchanges": [{**source, "type": "technosphere", "amount": -1}],
            }
        ],
        TechnosphereProfile("ecoinvent", "3.12", "cutoff"),
        BiosphereProfile("ecoinvent", "3.10"),
        PackageCatalogProvider(),
        database_name="synthetic",
        policy=MigrationPolicy(on_information_loss=PolicyAction.ERROR),
    )
    assert any(
        x.code == "migration.recipe_source_input_omission" and x.severity.value == "error" for x in report.issues
    )


def test_changed_omission_evidence_changes_the_generated_identity(omission_resource, monkeypatch):
    _, old = test_recipe_parameters.materialize(deepcopy(omission_resource), "forward", -1, monkeypatch)
    omission_resource["recipes"]["main"]["source_input_omissions"][0]["rationale"] += " Additional limitation."
    _, new = test_recipe_parameters.materialize(deepcopy(omission_resource), "forward", -1, monkeypatch)
    assert new["main"]["code"] != old["main"]["code"]
    assert new["child"]["code"] == old["child"]["code"]
