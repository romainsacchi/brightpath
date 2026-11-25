"""Factory-gate chemical supplies retain delivery without duplicate native distribution."""

import hashlib
import json
from copy import deepcopy

import pytest
from test_uvek_reference_signs import consumer, contexts
from uvek_review_helpers import compiled_authoring_stage_resource

from brightpath import DATA_DIR, BrightwayInventory, TechnosphereProfile
from brightpath.background.reference_signs import select_profile_sign_rules
from brightpath.background.rule_approvals import compatibility_rule_fingerprint
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource
from brightpath.migrations.scaling import scale_exchange

REVIEW = json.loads((DATA_DIR / "export/uvek_solvent_supply_markets_39_review.json").read_text())
ROWS = REVIEW["records"]


def policy(exclude=None):
    resource = load_uvek_technosphere_resource()
    fingerprints = tuple(
        compatibility_rule_fingerprint(
            resource,
            "replace",
            next(r for r in resource["replace"] if r["id"] == row["rule_id"]),
            TechnosphereProfile("ecoinvent", "3.9", "cutoff"),
            TechnosphereProfile("uvek", "2025", "cutoff"),
        )
        for row in ROWS
        if row["rule_id"] != exclude
    )
    return MigrationPolicy(approved_compatibility_rules=fingerprints, on_information_loss=PolicyAction.WARN)


@pytest.mark.parametrize("amount", [-2.5, 2.5])
@pytest.mark.parametrize("uncertainty", [2, 3])
@pytest.mark.parametrize("release", ["2025", "2026"])
def test_four_markets_keep_signed_uncertain_product_kg_and_all_27_supplier_entries(amount, uncertainty, release):
    data = consumer(ROWS[0]["original_rule"]["source"], amount, uncertainty)
    data[0]["exchanges"].extend(
        consumer(row["original_rule"]["source"], amount, uncertainty)[0]["exchanges"][1] for row in ROWS[1:]
    )
    data[0]["exchanges"].append(
        {
            "name": "Carbon dioxide, fossil",
            "categories": ["air"],
            "unit": "kilogram",
            "type": "biosphere",
            "amount": 0.5,
        }
    )
    before = deepcopy(data)
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.9")).migrate_background(
        contexts("uvek", release).background, policy=policy()
    )
    assert data == before and len(result.data) == 5
    helpers = {a["brightpath_conversion"]["recipe_id"]: a for a in result.data[1:]}
    for i, row in enumerate(ROWS, 1):
        edge = result.data[0]["exchanges"][i]
        expected = deepcopy(before[0]["exchanges"][i])
        scale_exchange(expected, 1)
        for key in ("amount", "uncertainty type", "loc", "scale", "negative", "formula", "minimum", "maximum"):
            if key in expected:
                assert edge[key] == expected[key]
        helper = helpers[row["recipe_id"]]
        assert all(edge[k] == helper[k] for k in ["name", "reference product", "location", "unit"])
        entries = [e for e in helper["exchanges"] if e["type"] != "production"]
        expected_inputs = REVIEW["recipes"][row["recipe_id"]]["inputs"]
        assert len(entries) == len(expected_inputs)
        assert all(
            all(e[k] == v for k, v in definition.items())
            for e, definition in zip(entries, expected_inputs, strict=True)
        )
        assert not helper["brightpath_conversion"].get("source_input_omissions")
    assert result.data[0]["exchanges"][-1] == before[0]["exchanges"][-1]
    assert sum(len(a["exchanges"]) - 1 for a in helpers.values()) == 27


@pytest.mark.parametrize("row", ROWS, ids=lambda r: r["rule_id"])
def test_each_new_exact_profile_requires_its_own_permission(row):
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(
            consumer(row["original_rule"]["source"], -1), context=contexts("ecoinvent", "3.9")
        ).migrate_background(contexts("uvek", "2026").background, policy=policy(row["rule_id"]))


@pytest.mark.parametrize(
    "version,model", [("3.8", "cutoff"), ("3.9.1", "cutoff"), ("3.10", "cutoff"), ("3.9", "consequential")]
)
def test_new_39_permissions_do_not_cover_other_versions_or_system_models(version, model):
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(
            consumer(ROWS[0]["original_rule"]["source"], -1), context=contexts("ecoinvent", version, model)
        ).migrate_background(contexts("uvek", "2026").background, policy=policy())


def test_ammonia_fuel_difference_is_explicit_and_native_distribution_is_not_added():
    ammonia = next(row for row in ROWS if row["boundary_checks"]["kind"] == "liquid_anhydrous_ammonia")
    facts = ammonia["boundary_checks"]
    assert facts["source_partial_oxidation_feedstock"] == "coal"
    assert facts["native_partial_oxidation_feedstock"] == "heavy fuel oil"
    assert facts["explicit_coal_to_oil_screening_proxy"] is True and facts["coal_branch_reproduced"] is False
    producer_entries = [e for e in REVIEW["recipes"][ammonia["recipe_id"]]["inputs"] if e["unit"] == "kilogram"]
    assert [e["amount"] for e in producer_entries] == [0.35, 0.65]
    assert [e["name"] for e in producer_entries] == [
        "Ammonia, partial oxidation, liquid, at plant",
        "Ammonia, steam reforming, liquid, at plant",
    ]
    assert facts["native_storehouse_distribution_and_15_85_mix_added"] is False
    assert any("entire35%" in limit and "screening proxy" in limit for limit in ammonia["limitations"])


def test_methanol_uses_fossil_at_plant_product_and_tanker_without_swiss_storage_overlay():
    methanol = next(row for row in ROWS if row["boundary_checks"]["kind"] == "methanol")
    inputs = REVIEW["recipes"][methanol["recipe_id"]]["inputs"]
    assert [e["name"] for e in inputs if e["unit"] == "kilogram"] == ["Methanol, at plant"] * 2
    assert inputs[0]["amount"] == 0.00367102654107643
    assert any(e["name"] == "Transport, transoceanic tanker" and e["amount"] == 0.599 for e in inputs)
    assert not any("storage" in e["name"].lower() for e in inputs)
    assert methanol["boundary_checks"]["biomass_syngas_alternative_excluded"]
    assert methanol["boundary_checks"]["native_negative_furnace_infrastructure_retained"]
    assert REVIEW["new_source_omissions"] == [] and REVIEW["new_helpers"] == 4


@pytest.mark.parametrize(
    "prior", REVIEW["preserved_existing_scopes"], ids=lambda p: p["rule_id"] + p["source_profile"]["version"]
)
def test_older_permissions_keep_physical_suppliers_authority_and_factor(prior):
    resource = load_uvek_technosphere_resource()
    row = next(row for row in ROWS if row["rule_id"] == prior["rule_id"])
    source = TechnosphereProfile(**prior["source_profile"])
    target = TechnosphereProfile(**prior["target_profile"])
    current = next(r for r in resource["replace"] if r["id"] == prior["rule_id"])
    selected = next(r for r in select_profile_sign_rules(resource, source)["replace"] if r["id"] == prior["rule_id"])
    assert selected["source"] == row["original_rule"]["source"]
    assert selected["target"] == row["original_rule"]["target"] == prior["unchanged_physical_binding"]["target"]
    assert selected.get("conversion_factor", 1) == row["original_rule"].get("conversion_factor", 1) == 1
    assert current["mapping_method"] == row["original_rule"]["mapping_method"]
    assert (
        hashlib.sha256((DATA_DIR / prior["previous_review_path"]).read_bytes()).hexdigest()
        == prior["previous_review_sha256"]
    )
    assert compatibility_rule_fingerprint(resource, "replace", current, source, target) == prior["rebound_fingerprint"]
    baseline = deepcopy(resource)
    parent = next(r for r in baseline["replace"] if r["id"] == prior["rule_id"])
    parent.clear()
    parent.update(deepcopy(row["original_rule"]))
    assert compatibility_rule_fingerprint(baseline, "replace", parent, source, target) == prior["previous_fingerprint"]
    allowed = MigrationPolicy(
        approved_compatibility_rules=(prior["rebound_fingerprint"],), on_information_loss=PolicyAction.WARN
    )
    result = BrightwayInventory.from_data(
        consumer(selected["source"], -2.5), context=contexts("ecoinvent", source.version)
    ).migrate_background(contexts("uvek", "2026").background, policy=allowed)
    assert len(result.data) == 1 and result.data[0]["exchanges"][1]["amount"] == -2.5
    assert all(result.data[0]["exchanges"][1][k] == v for k, v in selected["target"].items())


@pytest.mark.parametrize("drift", [None, "first_parent", "last_parent", "recipe", "freight"])
def test_exact_replay_rejects_unreviewed_changes_without_partial_mutation(drift):
    from scripts.review_uvek_solvent_supply_markets_39 import apply_solvent_supply_markets_39_review

    resource = compiled_authoring_stage_resource(
        "review_uvek_solvent_supply_markets_39", "apply_solvent_supply_markets_39_review"
    )
    if drift in ("first_parent", "last_parent"):
        row = ROWS[0] if drift == "first_parent" else ROWS[-1]
        next(r for r in resource["replace"] if r["id"] == row["rule_id"])["confidence"] = 0.001
    elif drift == "recipe":
        resource["recipes"][ROWS[0]["recipe_id"]]["inputs"][0]["amount"] *= 2
    elif drift == "freight":
        resource["recipes"][ROWS[-1]["recipe_id"]]["inputs"][-1]["amount"] *= 2
    before = deepcopy(resource)
    if drift:
        with pytest.raises(ValueError):
            apply_solvent_supply_markets_39_review(resource)
    else:
        apply_solvent_supply_markets_39_review(resource)
    assert resource == before


def test_pending_sodium_methoxide_is_not_implicitly_approved():
    source = {
        "name": "market for sodium methoxide",
        "reference product": "sodium methoxide",
        "location": "GLO",
        "unit": "kilogram",
    }
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(consumer(source, 1), context=contexts("ecoinvent", "3.9")).migrate_background(
            contexts("uvek", "2026").background, policy=policy()
        )
