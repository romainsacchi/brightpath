"""Historical model provenance stays distinct from the caller's source version."""

from copy import deepcopy

import pytest
import test_recipe_parameters
import test_recipe_source_omissions

from brightpath import BiosphereProfile, TechnosphereProfile
from brightpath.background import PackageCatalogProvider, uvek_import
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.schema import normalize_migration_resource

CALLER = {"family": "ecoinvent", "version": "3.12", "system_model": "cutoff"}
MODEL = {"family": "ecoinvent", "version": "3.9", "system_model": "cutoff"}


@pytest.fixture
def historical_proxy(migration_resource):
    resource = test_recipe_source_omissions.omission_resource.__wrapped__(migration_resource)
    resource["source_profiles"].append(deepcopy(MODEL))
    owner = resource["replace"][0]
    owner["reference_flow_sign_review"]["profiles"][0]["source_profile"] = deepcopy(MODEL)
    resource["recipes"]["main"]["source_input_omissions"][0]["source_profile"] = deepcopy(MODEL)
    caller = deepcopy(owner)
    caller["id"] = "synthetic-explicit-historical-proxy"
    caller["source"]["name"] = "Synthetic newer-version input"
    caller["target"] = {"recipe_ref": "wrapper"}
    entry = caller["reference_flow_sign_review"]["profiles"][0]
    entry.update(source_profile=deepcopy(CALLER), target={"recipe_ref": "wrapper"})
    resource["replace"].append(caller)
    resource["coverage"]["mapped_identities"] += 1
    resource["coverage"]["source_identities"] += 1
    resource["recipes"]["wrapper"] = {
        "name": "Explicit historical model proxy",
        "reference product": "treatment",
        "location": "CH",
        "unit": "kilogram",
        "inputs": [
            {"recipe": "main", "amount": 1, "inventory_source_profile": deepcopy(MODEL)},
            {"recipe": "child", "amount": 1},
        ],
        "rationale": "Explicit older inventory, retaining its actual omission provenance.",
        "evidence": ["Synthetic reviewed model fixture"],
    }
    return resource


def materialize(resource, monkeypatch, *, strict=False):
    normalize_migration_resource(resource)
    monkeypatch.setattr(uvek_import, "load_uvek_technosphere_resource", lambda: resource)
    source = resource["replace"][-1]["source"]
    data = [
        {
            "name": "Caller",
            "reference product": "output",
            "location": "CH",
            "unit": "unit",
            "exchanges": [{**source, "type": "technosphere", "amount": -2}],
        }
    ]
    return uvek_import.apply_uvek_import_recipes(
        data,
        TechnosphereProfile(**CALLER),
        BiosphereProfile("ecoinvent", "3.10"),
        PackageCatalogProvider(),
        database_name="synthetic",
        policy=MigrationPolicy(on_information_loss=PolicyAction.ERROR if strict else PolicyAction.WARN),
    )


def test_explicit_model_profile_keeps_omission_owner_and_separate_helper_contexts(historical_proxy, monkeypatch):
    before = deepcopy(historical_proxy)
    report, helpers = materialize(historical_proxy, monkeypatch)
    assert not report.has_errors, [(i.code, i.message) for i in report.issues]
    assert historical_proxy == before
    assert len(helpers) == 4
    child = [h for h in helpers if h["brightpath_conversion"]["recipe_id"] == "child"]
    assert len(child) == 2 and child[0]["code"] != child[1]["code"]
    older = next(h for h in child if h["brightpath_conversion"].get("inventory_source_profile") == MODEL)
    assert older["brightpath_conversion"]["caller_source_profile"] == CALLER
    assert all("inventory_source_profile" not in e for h in helpers for e in h["exchanges"])
    losses = [loss for loss in report.losses if loss.code == "migration.recipe_source_input_omission"]
    assert len(losses) == 1
    details = losses[0].to_dict()["details"]
    assert details["source_profile"] == MODEL and details["caller_source_profile"] == CALLER
    assert details["omission"] == before["recipes"]["main"]["source_input_omissions"][0]
    assert details["basis"] == {"amount": 1, "unit": "kilogram"}
    boundaries = [loss for loss in report.losses if loss.code == "migration.technosphere_information_loss"]
    assert len(boundaries) == 1
    assert boundaries[0].to_dict()["details"]["inventory_source_profile"] == MODEL


@pytest.mark.parametrize("changed_profile", [None, "caller_source_profile", "inventory_source_profile"])
def test_historical_helper_replays_only_its_reviewed_biosphere_binding(historical_proxy, monkeypatch, changed_profile):
    target_profile = {"family": "uvek", "version": "2026"}
    source_flow = {"name": "Original release", "categories": ["air"], "unit": "kilogram"}
    target_flow = {**source_flow, "name": "Reviewed native release"}
    historical_proxy["source_profiles"].append({**MODEL, "version": "3.8"})
    historical_proxy["recipes"]["main"]["inputs"].append(
        {
            **source_flow,
            "type": "biosphere",
            "amount": 2.5,
            "biosphere_targets": [
                {"profile": target_profile, "target": target_flow, "reason": "Synthetic exact native correspondence"}
            ],
        }
    )
    report, helpers = materialize(historical_proxy, monkeypatch)
    assert not report.has_errors
    owner = next(h for h in helpers if h["brightpath_conversion"]["recipe_id"] == "main")
    metadata = owner["brightpath_conversion"]
    assert metadata["caller_source_profile"] == CALLER
    assert metadata["inventory_source_profile"] == MODEL
    if changed_profile:
        metadata[changed_profile] = {**metadata[changed_profile], "version": "3.8"}
    data = [{"exchanges": deepcopy(owner["exchanges"])}]
    result = uvek_import.apply_reviewed_recipe_biosphere_bindings(
        data, [owner], target_profile, {("Reviewed native release", ("air",), "kilogram")}, MigrationPolicy.strict()
    )
    if changed_profile:
        assert [issue.code for issue in result.issues] == ["migration.recipe_biosphere_review_stale"]
        assert data[0]["exchanges"] == owner["exchanges"]
    else:
        assert not result.has_errors
        assert [change.code for change in result.changes] == ["migration.recipe_biosphere_binding"]
        assert data[0]["exchanges"][-1] == {**target_flow, "type": "biosphere", "amount": 2.5}
        assert owner["exchanges"][-1] == {**source_flow, "type": "biosphere", "amount": 2.5}


def test_absent_explicit_model_profile_does_not_fall_back(historical_proxy, monkeypatch):
    historical_proxy["recipes"]["wrapper"]["inputs"][0].pop("inventory_source_profile")
    report, _ = materialize(historical_proxy, monkeypatch)
    assert report.has_errors
    assert any("different source profile" in i.message for i in report.issues)


@pytest.mark.parametrize(
    "profile",
    [
        {"family": "ecoinvent", "version": "3.9", "system_model": "consequential"},
        {"family": "uvek", "version": "2025", "system_model": "cutoff"},
        {"family": "ecoinvent", "version": "3.11", "system_model": "cutoff"},
    ],
)
def test_model_context_cannot_cross_families_models_or_undeclared_profiles(historical_proxy, monkeypatch, profile):
    historical_proxy["recipes"]["wrapper"]["inputs"][0]["inventory_source_profile"] = profile
    report, _ = materialize(historical_proxy, monkeypatch)
    assert report.has_errors
    assert any("inventory source profile" in i.message for i in report.issues)


def test_explicit_older_model_does_not_silence_strict_omission_loss(historical_proxy, monkeypatch):
    report, _ = materialize(historical_proxy, monkeypatch, strict=True)
    assert any(
        i.code == "migration.recipe_source_input_omission" and i.severity.value == "error" for i in report.issues
    )
    assert any(
        i.code == "migration.technosphere_information_loss" and i.severity.value == "error" for i in report.issues
    )


def test_explicit_inventory_profile_is_not_silently_accepted_by_reverse_recipes(migration_resource):
    resource = test_recipe_parameters.parameter_resource.__wrapped__(migration_resource)
    profile = {"family": "uvek", "version": "2025", "system_model": "cutoff"}
    resource["source_profiles"] = [profile]
    resource["target_profile"] = deepcopy(CALLER)
    normalize_migration_resource(resource)
    resource["recipes"]["main"]["inputs"][1]["inventory_source_profile"] = profile
    with pytest.raises(MigrationError, match="recipes.main.inputs.1"):
        normalize_migration_resource(resource)
