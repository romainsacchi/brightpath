"""Complete processing compositions keep burdens, credits and signed caller demand separate."""

from copy import deepcopy

import pytest
from test_uvek_market_compositions import assert_composition_quantities, batch
from test_uvek_reference_signs import FIELDS, contexts
from test_uvek_refrigerant_disposal import target_context
from uvek_review_helpers import historical_forward_resource

from brightpath import BrightwayInventory, TechnosphereProfile
from brightpath.background.reference_signs import select_profile_sign_rules
from brightpath.core.policies import MigrationPolicy
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource
from scripts.review_uvek_processing_compositions import (
    APPROVAL,
    PROCESSING,
    processing_decision,
    processing_recipe,
    validate_processing_bindings,
)

LABELS = [
    label
    for label in sorted({p["label"] for r in PROCESSING.values() for p in r["profiles"]})
    if label.endswith("-cutoff")
]
POLICY = MigrationPolicy.permissive()


@pytest.mark.parametrize("label", LABELS)
@pytest.mark.parametrize("amount", [-10, 0, 10])
def test_all_processing_profiles_preserve_burdens_credits_signed_quantity_uncertainty_and_formula(label, amount):
    assert_composition_quantities(
        label,
        amount,
        definitions=PROCESSING,
        recipe_factory=processing_recipe,
        warning_fragment="Retain processing inputs, freight and genuine recovered-product credits",
    )


@pytest.mark.parametrize("kind", ["2025", "2026", "native2026"])
def test_processing_helpers_preserve_separately_listed_foreground_emissions_and_release_context(kind):
    data = batch("ecoinvent-3.12-cutoff", -1, definitions=PROCESSING)
    emission = {
        "name": "Carbon dioxide, fossil",
        "categories": ["air"],
        "unit": "kilogram",
        "type": "biosphere",
        "amount": 7,
    }
    data[0]["exchanges"].append(emission)
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.12", "cutoff")).migrate_background(
        target_context(kind), policy=POLICY
    )
    assert result.data[0]["exchanges"][-1]["amount"] == 7 and result.data[0]["exchanges"][-1]["type"] == "biosphere"
    if kind == "2025":
        assert result.data[0]["exchanges"][-1] == emission
    assert not any(i.code.endswith("link_unresolved") for i in result.last_migration_report.issues)


def test_processing_profiles_require_loss_policy_and_leave_foreground_suppliers_intact():
    data = batch("ecoinvent-3.12-cutoff", -1, definitions=PROCESSING)
    original = deepcopy(data)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.12", "cutoff"))
    with pytest.raises(MigrationError):
        inventory.migrate_background(target_context("2025"))
    assert inventory.data == original
    for source, row in PROCESSING.items():
        if "ecoinvent-3.12-cutoff" in {p["label"] for p in row["profiles"]}:
            supplier = dict(zip(FIELDS, source, strict=True))
            data.append({**supplier, "exchanges": [{**supplier, "type": "production", "amount": -1}]})
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.12", "cutoff")).migrate_background(
        target_context("2025"), policy=POLICY
    )
    assert result.data == data
    selected = select_profile_sign_rules(
        load_uvek_technosphere_resource(), TechnosphereProfile("ecoinvent", "3.12.1", "cutoff")
    )
    assert all(
        r["reference_flow_sign_review"]["status"] == "unresolved"
        for r in selected["replace"]
        if tuple(r["source"][f] for f in FIELDS) in PROCESSING
    )


def test_processing_approval_scope_is_exact_and_contains_no_unreviewed_empty_fallback():
    resource = historical_forward_resource()
    validate_processing_bindings(resource)
    rules = [r for r in resource["replace"] if tuple(r["source"][f] for f in FIELDS) in PROCESSING]
    assert len(rules) == APPROVAL["source_count"] == 106
    assert (
        sum(
            sum(e["review"]["status"] == "verified" for e in r["reference_flow_sign_review"]["profiles"]) for r in rules
        )
        == APPROVAL["source_profile_count"]
        == 817
    )
    assert all(r["mapping_method"] == "reviewed_processing_composition" for r in rules)
    for source, row in PROCESSING.items():
        for profile in row["profiles"]:
            _, recipe = processing_recipe(source, profile)
            assert recipe["inputs"] and not recipe.get("zero_burden")


@pytest.mark.parametrize("change", ["profile", "hash", "prior_target"])
def test_changed_processing_source_requires_independent_reinspection(change):
    source, approval = next(iter(PROCESSING.items()))
    rule = {"source": approval["source"], "target": deepcopy(approval["previous_target"])}
    observations = [(p["label"], p["source_sign"]) for p in approval["profiles"]]
    inventories = [(p["label"], p["inventory_sha256"]) for p in approval["profiles"]]
    if change == "profile":
        observations[0] = ("ecoinvent-3.12.1-consequential", -1)
    elif change == "hash":
        inventories[0] = (inventories[0][0], "0" * 64)
    else:
        rule["target"]["name"] = "Unreviewed"
    with pytest.raises(ValueError, match="repeat the scientific review"):
        processing_decision(rule, observations, inventories, "synthetic", None)


def test_processing_components_cannot_silently_change_after_review():
    # This authoring guard also freezes historical-only dependencies that the
    # executable cut-off projection intentionally removes from its recipe pool.
    resource = historical_forward_resource()
    validate_processing_bindings(resource)
    key = next(k for k in APPROVAL["recipe_dependencies"] if resource["recipes"][k]["inputs"])
    resource["recipes"][key]["inputs"][0]["amount"] = 999
    with pytest.raises(ValueError, match="repeat the scientific review"):
        validate_processing_bindings(resource)
