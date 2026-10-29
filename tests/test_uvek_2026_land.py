"""Exact land identities and generated recipe biosphere contexts."""

import json
from copy import deepcopy

import pytest

from brightpath import BackgroundContext, BiosphereProfile, FormatProfile, InventoryContext, TechnosphereProfile
from brightpath.background import PackageCatalogProvider
from brightpath.background.execution import execute_background_migration
from brightpath.background.uvek_export import identity as tech_identity
from brightpath.background.uvek_export import load_uvek_export_resource
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.migrations.resources import load_uvek_release_resources
from brightpath.models import InventoryDocument
from scripts.review_uvek_2026_biosphere import identity
from scripts.review_uvek_2026_land import METHOD, REPORT, apply_land_review, resource_hash, validate_decision
from tests.test_uvek_2026_biosphere_review import CATALOGS, context, document
from tests.test_uvek_export import data_for
from tests.uvek_biosphere_history import before_land_review


def test_both_directions_preserve_prior_rules_and_reproduce_packaged_resources():
    report = json.loads(REPORT.read_text())
    assert report["summary"] == {"forward_additions": 5, "reverse_additions": 7}
    prior = before_land_review()
    current = list(load_uvek_release_resources("biosphere"))
    assert apply_land_review(prior, REPORT, CATALOGS) == current
    for direction, family, total in (("forward", "uvek", 1935), ("reverse", "ecoinvent", 1297)):
        previous = next(row for row in prior if row["source_profiles"][0]["family"] == family)
        packaged = next(row for row in current if row["source_profiles"][0]["family"] == family)
        assert resource_hash(previous) == report["sources"][f"baseline_{direction}_sha256"]
        old = {row["id"]: row for row in previous["replace"]}
        assert {row["id"]: row for row in packaged["replace"] if row["id"] in old} == old
        assert len(packaged["replace"]) == total


@pytest.mark.parametrize("direction", ["forward", "reverse"])
def test_land_directions_execute_independently_and_preserve_quantities(direction):
    rules = [
        row
        for resource in load_uvek_release_resources("biosphere")
        if (resource["source_profiles"][0]["family"] == "uvek") == (direction == "forward")
        for row in resource["replace"]
        if row["mapping_method"] == METHOD
    ]
    source = ("uvek", "2026") if direction == "forward" else ("ecoinvent", "3.10")
    target = ("ecoinvent", "3.10") if direction == "forward" else ("uvek", "2026")
    original = document(rules)
    data = original.data
    data[0]["parameters"] = [{"name": "dose", "amount": 12}]
    for flow in data[0]["exchanges"][1:]:
        flow["formula"] = "dose"
    original = original.replace(data=data, context=InventoryContext(original.context.format, context(source)))
    saved = deepcopy(original.data)
    result = execute_background_migration(original, context(target), PackageCatalogProvider())
    assert result.succeeded, result.report.to_json()
    assert len(rules) == (5 if direction == "forward" else 7)
    for flow, rule in zip(result.value.data[0]["exchanges"][1:], rules, strict=True):
        assert identity(flow) == identity(rule["target"])
        assert (flow["amount"], flow["loc"], flow["scale"], flow["formula"], flow["custom"]) == (
            12,
            12,
            0.3,
            "dose",
            [1],
        )
    assert original.data == saved
    repeated = execute_background_migration(result.value, context(target), PackageCatalogProvider())
    assert repeated.succeeded and repeated.value.data == result.value.data


@pytest.mark.parametrize(
    "mutation", ["name", "context", "unit", "uuid", "publisher", "cas", "formula", "factor", "direction"]
)
def test_land_review_rejects_unsubstantiated_changes(mutation):
    decision = deepcopy(json.loads(REPORT.read_text())["decisions"][0])
    if mutation == "name":
        decision["native"]["name"] += ", unspecified"
    elif mutation == "context":
        decision["native"]["categories"] = ["resources", "unspecified"]
    elif mutation == "unit":
        decision["native"]["unit"] = "kilogram"
    elif mutation == "uuid":
        decision["ecoinvent"]["uuid"] = "wrong"
    elif mutation == "publisher":
        decision["publisher_row"]["Formula"] = "CORINE 999"
    elif mutation == "cas":
        decision["source_cas"] = ["7732-18-5"]
    elif mutation == "formula":
        decision["source_formulas"] = ["H2O"]
    elif mutation == "factor":
        decision["conversion_factor"] = 1
    else:
        decision["direction"] = "inferred"
    catalogs = [
        json.loads((CATALOGS / name).read_text())["biosphere"]
        for name in ("uvek__2026__cutoff.json", "ecoinvent__3.10__cutoff.json")
    ]
    with pytest.raises(ValueError, match="Land correspondence"):
        validate_decision(decision, *catalogs)


def cdte_document(biosphere):
    rule = next(
        row
        for row in load_uvek_export_resource()["replace"]
        if row["source"]["name"] == "Photovoltaic panel factory CdTe"
    )
    bridge = next(
        row for row in load_uvek_release_resources("technosphere") if row["source_profiles"][0]["version"] == "2026"
    )
    native = next(
        row["source"] for row in bridge["replace"] if tech_identity(row["target"]) == tech_identity(rule["source"])
    )
    data = data_for({"source": native})
    if biosphere.family == "uvek":
        # A deliberately unresolved native flow stays untouched when only the
        # process axis migrates. It must not be routed through ecoinvent merely
        # because generated helper flows need their own biosphere conversion.
        resource = next(
            row for row in load_uvek_release_resources("biosphere") if row["source_profiles"][0]["family"] == "uvek"
        )
        data[0]["exchanges"].append({**resource["unresolved"][0], "type": "biosphere", "amount": 3, "custom": [1]})
    return InventoryDocument(
        data=data,
        context=InventoryContext(
            FormatProfile("brightway_excel"),
            BackgroundContext(TechnosphereProfile("uvek", "2026", "cutoff"), biosphere),
        ),
    )


def test_cdte_generated_land_flows_follow_preserved_native_biosphere():
    biosphere = BiosphereProfile("uvek", "2026")
    original = cdte_document(biosphere)
    saved = deepcopy(original.data)
    target = BackgroundContext(TechnosphereProfile("ecoinvent", "3.12", "cutoff"), biosphere)
    policy = MigrationPolicy(on_information_loss=PolicyAction.WARN, on_inferred_reverse=PolicyAction.WARN)
    provider = PackageCatalogProvider()
    result = execute_background_migration(original, target, provider, policy)
    assert result.succeeded, result.report.to_json()
    assert len(result.value.data) > 1
    assert result.value.data[0]["exchanges"][-1] == saved[0]["exchanges"][-1]
    flows = [flow for helper in result.value.data[1:] for flow in helper["exchanges"] if flow["type"] == "biosphere"]
    assert flows and all(identity(flow) in provider.load_biosphere(biosphere).identities for flow in flows)
    assert any(stage.metrics.get("scope") == "generated_helpers" for stage in result.report.stages)
    assert original.data == saved
    repeated = execute_background_migration(result.value, target, provider, policy)
    assert repeated.succeeded and repeated.value.data == result.value.data


def test_helper_reverse_policy_refusal_rolls_back_the_entire_export():
    biosphere = BiosphereProfile("uvek", "2026")
    original = cdte_document(biosphere)
    saved = deepcopy(original.data)
    target = BackgroundContext(TechnosphereProfile("ecoinvent", "3.12", "cutoff"), biosphere)
    result = execute_background_migration(
        original, target, PackageCatalogProvider(), MigrationPolicy(on_information_loss=PolicyAction.WARN)
    )
    assert not result.succeeded
    assert result.value.data == original.data == saved
    assert not result.report.changes
    assert any(issue.details.get("scope") == "generated_helpers" for issue in result.report.issues)


def test_helpers_already_valid_in_preserved_ecoinvent_biosphere_need_no_reverse_consent():
    biosphere = BiosphereProfile("ecoinvent", "3.10")
    original = cdte_document(biosphere)
    target = BackgroundContext(TechnosphereProfile("ecoinvent", "3.12", "cutoff"), biosphere)
    result = execute_background_migration(
        original, target, PackageCatalogProvider(), MigrationPolicy(on_information_loss=PolicyAction.WARN)
    )
    assert result.succeeded, result.report.to_json()
    assert not any(stage.metrics.get("scope") == "generated_helpers" for stage in result.report.stages)
