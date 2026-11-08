"""Waste reference coordinates must preserve treatment demand and actual credits."""

import json
import math
import pickle
import sqlite3
from copy import deepcopy
from pathlib import Path

import pytest
from uvek_review_helpers import historical_forward_resource

from brightpath import (
    DATA_DIR,
    BackgroundContext,
    BiosphereProfile,
    BrightwayInventory,
    FormatProfile,
    InventoryContext,
    TechnosphereProfile,
)
from brightpath.background.catalogs import InMemoryCatalogProvider, TechnosphereCatalog
from brightpath.background.reference_signs import select_profile_sign_rules
from brightpath.background.uvek_export import load_uvek_export_resource
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource
from brightpath.migrations.schema import validate_migration_resource
from scripts.review_uvek_dedicated_disposals import DISPOSALS
from scripts.review_uvek_eaf_landfills import LANDFILLS
from scripts.review_uvek_lcd_treatments import LCD_TREATMENTS
from scripts.review_uvek_modal_transport import TRANSPORTS
from scripts.review_uvek_natural_gas_flares import FLARES as NATURAL_GAS_FLARES
from scripts.review_uvek_older_regional_road import SOURCES as OLDER_REGIONAL_ROAD_SOURCES
from scripts.review_uvek_reference_signs import (
    APPROVED_DISCHARGE_TREATMENTS,
    APPROVED_FEED_SUBSTITUTION_OMISSIONS,
    APPROVED_GENERIC_WASTEWATER,
    APPROVED_GEOGRAPHIC_TREATMENTS,
    APPROVED_INCINERATION_COMPOSITION_PROXIES,
    APPROVED_NUTRIENT_REFERENCE_OMISSIONS,
    APPROVED_PARTIALLY_UNTREATED_MARKETS,
    APPROVED_PROFILED_TREATMENTS,
    APPROVED_RECOVERY_CREDITS,
    APPROVED_STEEL_RECOVERY_OMISSIONS,
    APPROVED_WASTE_TREATMENTS,
    APPROVED_WASTEWATER_TREATMENTS,
    BATTERY_PREVIOUS_TARGET,
    BATTERY_SOURCE,
    BATTERY_TARGET,
    COAL_TAR_SOURCE,
    COAL_TAR_TARGET,
    CONCRETE_GRADE_SOURCE,
    GENERIC_WASTEWATER_TARGET,
    LI_ION_MARKET_SOURCE,
    LI_ION_MARKET_TARGET,
    NIMH_MARKET_SOURCE,
    NIMH_SOURCE,
    NIMH_TARGET,
    PET_INCINERATION_NAME,
    PET_INCINERATION_PRODUCT,
    PET_INCINERATION_TARGET,
    PYRO_BATTERY_SOURCE,
    PYRO_BATTERY_TARGET,
    REVIEW_PATH,
    apply_sign_review,
    approved_supplier_rule,
    build_review,
    coal_tar_review,
    collect_reference_profiles,
    dumps_review,
    model_specific_review,
)
from scripts.review_uvek_refinery_flare import FLARES
from scripts.review_uvek_regional_freight import SOURCES as REGIONAL_FREIGHT_SOURCES
from scripts.review_uvek_whole_freight import SOURCES as WHOLE_FREIGHT_SOURCES

ROOT = Path(__file__).resolve().parents[1]
FIELDS = ("name", "reference product", "location", "unit")


def contexts(family, version, model="cutoff"):
    background = BackgroundContext(TechnosphereProfile(family, version, model), BiosphereProfile("ecoinvent", "3.10"))
    return InventoryContext(FormatProfile("brightway_excel"), background)


def consumer(supplier, amount, uncertainty=0, exchange_type="technosphere"):
    foreground = {"name": "Sign regression consumer", "reference product": "output", "location": "CH", "unit": "unit"}
    exchange = {**supplier, "amount": amount, "type": exchange_type, "uncertainty type": uncertainty, "formula": "dose"}
    if uncertainty == 2:
        exchange.update(loc=math.log(abs(amount)) if amount else 0, scale=0.2, negative=amount < 0)
    elif uncertainty == 3:
        exchange.update(loc=amount, scale=0.2, minimum=amount - 1, maximum=amount + 1)
    elif uncertainty in (4, 5):
        exchange.update(loc=amount, minimum=amount - 1, maximum=amount + 1)
    return [{**foreground, "exchanges": [{**foreground, "amount": 1, "type": "production"}, exchange]}]


def powertrain_rule():
    return next(
        rule
        for rule in load_uvek_technosphere_resource()["replace"]
        if rule["source"]["name"] == "treatment of used powertrain for electric passenger car, manual dismantling"
    )


@pytest.mark.parametrize("amount", [-10, 10, 0])
@pytest.mark.parametrize("uncertainty", [0, 2, 3, 4, 5])
@pytest.mark.parametrize("exchange_type", ["technosphere", "substitution"])
def test_forward_treatment_sign_scales_quantity_uncertainty_and_formula(amount, uncertainty, exchange_type):
    rule = powertrain_rule()
    data = consumer(rule["source"], amount, uncertainty, exchange_type)
    original = deepcopy(data)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.12"))
    result = inventory.migrate_background(contexts("uvek", "2025").background, policy=MigrationPolicy.permissive())
    exchange = result.data[0]["exchanges"][1]
    assert exchange["amount"] == -amount
    assert exchange["type"] == exchange_type
    assert exchange["formula"] == "(dose) * (-1.0)"
    assert tuple(exchange[field] for field in FIELDS) == tuple(rule["target"][field] for field in FIELDS)
    if uncertainty == 2:
        assert exchange["negative"] is (amount >= 0)
        assert exchange["loc"] == original[0]["exchanges"][1]["loc"]
        assert exchange["scale"] == 0.2
    elif uncertainty in (3, 4, 5):
        assert exchange["loc"] == -amount
        assert exchange["minimum"] == -amount - 1
        assert exchange["maximum"] == -amount + 1
        if uncertainty == 3:
            assert exchange["scale"] == 0.2
    assert result.data[0]["exchanges"][0] == original[0]["exchanges"][0]
    assert inventory.data == data == original


@pytest.mark.parametrize("amount", [-10, 10])
@pytest.mark.parametrize("version", ["2025", "2026"])
def test_reverse_treatment_services_keep_existing_explicit_negative_factors(version, amount):
    rules = [
        rule
        for rule in load_uvek_export_resource()["replace"]
        if rule.get("reference_flow_sign_review", {}).get("target_sign") == -1
    ]
    assert len(rules) == 69
    data = consumer(rules[0]["source"], amount, 2)
    data[0]["exchanges"] = data[0]["exchanges"][:1] + [
        consumer(rule["source"], amount, 2)[0]["exchanges"][1] for rule in rules
    ]
    original = deepcopy(data)
    inventory = BrightwayInventory.from_data(data, context=contexts("uvek", version))
    result = inventory.migrate_background(contexts("ecoinvent", "3.12").background, policy=MigrationPolicy.permissive())
    for exchange, rule in zip(result.data[0]["exchanges"][1:], rules, strict=True):
        assert exchange["amount"] == pytest.approx(amount * rule["conversion_factor"])
        assert exchange["negative"] is (amount > 0)
        assert exchange["scale"] == 0.2
    assert inventory.data == data == original


def test_reversible_treatment_round_trip_restores_quantity():
    rule = next(
        rule
        for rule in load_uvek_technosphere_resource()["replace"]
        if rule["source"]["name"] == "treatment of waste aluminium, municipal incineration FAE"
        and rule["source"]["location"] == "CH"
    )
    inventory = BrightwayInventory.from_data(consumer(rule["source"], -10), context=contexts("ecoinvent", "3.12"))
    converted = inventory.migrate_background(contexts("uvek", "2025").background, policy=MigrationPolicy.permissive())
    restored = converted.migrate_background(inventory.context.background, policy=MigrationPolicy.permissive())
    exchange = restored.data[0]["exchanges"][1]
    assert exchange["amount"] == -10
    assert tuple(exchange[field] for field in FIELDS) == tuple(rule["source"][field] for field in FIELDS)


@pytest.mark.parametrize("forward", [True, False])
def test_ambiguous_signs_fail_without_mutating_input_even_under_permissive_policy(
    forward, monkeypatch, unreviewed_uvek_correspondence
):
    if forward:
        rule = {"source": unreviewed_uvek_correspondence}
    else:
        resource = deepcopy(load_uvek_export_resource())
        rule = next(rule for rule in resource["replace"] if rule["classification"] == "correspondence")
        rule["reference_flow_sign_review"] = {
            "status": "unresolved",
            "reason": "Synthetic unresolved reverse sign review.",
            "evidence": ["Synthetic test evidence"],
        }
        monkeypatch.setattr("brightpath.background.uvek_export.load_uvek_export_resource", lambda: resource)
    data = consumer(rule["source"], -10)
    original = deepcopy(data)
    source = contexts("ecoinvent", "3.12") if forward else contexts("uvek", "2025")
    target = contexts("uvek", "2025") if forward else contexts("ecoinvent", "3.12")
    inventory = BrightwayInventory.from_data(data, context=source)
    with pytest.raises(MigrationError) as caught:
        inventory.migrate_background(target.background, policy=MigrationPolicy.permissive())
    assert "migration.reference_flow_sign_unresolved" in {issue.code for issue in caught.value.report.issues}
    assert inventory.data == data == original


def test_recovered_product_from_treatment_preserves_legitimate_negative_exchange():
    rule = next(
        rule
        for rule in load_uvek_technosphere_resource()["replace"]
        if rule["source"]["name"] == "treatment of used Li-ion battery, hydrometallurgical treatment"
        and rule["source"]["reference product"] == "cobalt"
    )
    inventory = BrightwayInventory.from_data(consumer(rule["source"], -10), context=contexts("ecoinvent", "3.12"))
    result = inventory.migrate_background(contexts("uvek", "2025").background, policy=MigrationPolicy.permissive())
    assert result.data[0]["exchanges"][1]["amount"] == -10


def test_sign_review_replays_idempotently_and_rejects_stale_target_or_factor():
    ledger = json.loads((ROOT / REVIEW_PATH).read_text())
    resource = historical_forward_resource()
    before = deepcopy(resource)
    apply_sign_review(resource, ledger)
    assert resource == before
    rule = next(
        rule for rule in resource["replace"] if rule.get("reference_flow_sign_review", {}).get("status") == "verified"
    )
    rule["target"]["name"] = "Changed target"
    with pytest.raises(ValueError, match="Stale"):
        apply_sign_review(resource, ledger)
    resource = deepcopy(before)
    rule = next(
        rule for rule in resource["replace"] if rule.get("reference_flow_sign_review", {}).get("status") == "verified"
    )
    rule["conversion_factor"] = 17
    with pytest.raises(ValueError, match="Stale"):
        apply_sign_review(resource, ledger)
    rule["conversion_factor"] = (
        -rule["reference_flow_sign_review"]["source_sign"] * rule["reference_flow_sign_review"]["target_sign"]
    )
    with pytest.raises(MigrationError, match="contradicts"):
        validate_migration_resource(resource)


def test_every_reference_sign_review_has_replayable_evidence():
    ledger = json.loads((DATA_DIR / "export/uvek_reference_flow_sign_review.json").read_text())
    assert len(ledger["provenance"]) == 15
    # Complete scoped treatment/market helpers replace former flat sign corrections.
    assert ledger["summary"]["ecoinvent-to-uvek-2025.json"]["corrected"] == 4192
    assert ledger["summary"]["ecoinvent-to-uvek-2025.json"]["approved_copper_ash_reconstruction"] == 2
    assert ledger["summary"]["ecoinvent-to-uvek-2025.json"]["approved_waste_concrete_inert_reconstruction"] == 2
    assert ledger["summary"]["ecoinvent-to-uvek-2025.json"]["approved_older_pet_whole_freight_extension"] == 2
    assert ledger["summary"]["ecoinvent-to-uvek-2025.json"]["approved_average_sludge_landfarming_recipe"] == 8
    assert ledger["summary"]["ecoinvent-to-uvek-2025.json"]["approved_cable_rail_function_repair"] == 4
    assert ledger["summary"]["ecoinvent-to-uvek-2025.json"]["omitted"] == 257
    assert ledger["summary"]["uvek-2025-to-ecoinvent-3.12-cutoff.json"]["already_correct"] == 69
    assert ledger["summary"]["uvek-2025-to-ecoinvent-3.12-cutoff.json"]["omitted"] == 1
    assert "unresolved" not in ledger["summary"]["uvek-2025-to-ecoinvent-3.12-cutoff.json"]
    for resource in (historical_forward_resource(), load_uvek_export_resource()):
        for rule in resource["replace"]:
            if "reference_flow_sign_review" in rule:
                assert ledger["resources"][resource["name"]][rule["id"]]["review"] == rule["reference_flow_sign_review"]


def test_unsupported_uncertainty_rolls_back_before_committing_sign_conversion():
    data = consumer(powertrain_rule()["source"], -10, 8)
    original = deepcopy(data)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.12"))
    with pytest.raises(MigrationError):
        inventory.migrate_background(contexts("uvek", "2025").background, policy=MigrationPolicy.permissive())
    assert inventory.data == data == original


@pytest.mark.parametrize("problem", ["missing_sign", "contradictory_sign", "missing_evidence", "invalid_status"])
def test_invalid_sign_reviews_cannot_load(problem):
    resource = deepcopy(load_uvek_technosphere_resource())
    rule = next(
        rule for rule in resource["replace"] if rule.get("reference_flow_sign_review", {}).get("status") == "verified"
    )
    review = rule["reference_flow_sign_review"]
    if problem == "missing_sign":
        del review["source_sign"]
    elif problem == "contradictory_sign":
        review["source_sign"] *= -1
    elif problem == "missing_evidence":
        review["evidence"] = []
    else:
        review["status"] = "assumed"
    with pytest.raises(MigrationError):
        validate_migration_resource(resource)


@pytest.mark.parametrize("reference_amount", [-1, 1, 0])
def test_reference_collector_is_read_only_and_rejects_zero_reference_amount(tmp_path, reference_amount):
    directory = tmp_path / "reference-project/lci"
    directory.mkdir(parents=True)
    path = directory / "databases.db"
    activity = {
        "name": "Provider without a treatment label",
        "reference product": "reference",
        "location": "CH",
        "unit": "kilogram",
        "code": "provider",
    }
    exchange = {"input": ("background", "provider"), "amount": reference_amount}
    with sqlite3.connect(path) as connection:
        connection.execute("CREATE TABLE activitydataset (data BLOB, database TEXT)")
        connection.execute(
            "CREATE TABLE exchangedataset (output_code TEXT, data BLOB, output_database TEXT, type TEXT)"
        )
        connection.execute("INSERT INTO activitydataset VALUES (?, ?)", (pickle.dumps(activity), "background"))
        connection.execute(
            "INSERT INTO exchangedataset VALUES (?, ?, ?, ?)",
            ("provider", pickle.dumps(exchange), "background", "production"),
        )
    provenance = [
        {
            "profile": ["ecoinvent", "3.12", "cutoff"],
            "project": "reference-project",
            "database": "background",
            "reference_count": 1,
        }
    ]
    before = path.read_bytes()
    if reference_amount:
        profiles = collect_reference_profiles(tmp_path, provenance)
        assert profiles[0]["references"] == [[*(activity[field] for field in FIELDS), reference_amount]]
    else:
        with pytest.raises(ValueError, match="Invalid reference"):
            collect_reference_profiles(tmp_path, provenance)
    assert path.read_bytes() == before


def test_compact_audit_serialization_preserves_unicode_and_every_decision():
    ledger = {"summary": {"reason": "Zürich → Genève"}, "resources": {"route": {"rule": {"factor": -1}}}}
    encoded = dumps_review(ledger)
    assert json.loads(encoded) == ledger
    assert dumps_review(json.loads(encoded)) == encoded


@pytest.fixture
def profile_resource(migration_resource):
    resource = deepcopy(migration_resource)
    resource["name"] = "ecoinvent-3.x-to-uvek-2025-technosphere"
    resource["quality"] = "heuristic"
    resource["source_profiles"] = [
        {"family": "ecoinvent", "version": "3.12", "system_model": model} for model in ("cutoff", "consequential")
    ]
    for rule in resource["replace"]:
        rule["reference_flow_sign_review"] = {
            "status": "unresolved",
            "reason": "Synthetic independently reviewed profile decisions.",
            "evidence": ["Synthetic test evidence, not approval of a real supplier."],
            "profiles": [
                {
                    "source_profile": dict(profile),
                    "conversion_factor": 0.1 * sign,
                    "review": {
                        "status": "verified",
                        "reason": "Synthetic supplier approval.",
                        "evidence": ["Synthetic independent review."],
                        "source_sign": sign,
                        "target_sign": 1,
                    },
                }
                for profile, sign in zip(resource["source_profiles"], (1, -1), strict=True)
            ],
        }
    return resource


@pytest.mark.parametrize("model,sign", [("cutoff", 1), ("consequential", -1)])
@pytest.mark.parametrize("amount", [-10, 10])
def test_independently_approved_profiles_select_factors_without_changing_cached_rules(
    profile_resource, monkeypatch, model, sign, amount
):
    validate_migration_resource(profile_resource)
    before = deepcopy(profile_resource)
    monkeypatch.setattr("brightpath.background.migration.load_uvek_technosphere_resource", lambda: profile_resource)
    monkeypatch.setattr("brightpath.background.execution.load_uvek_technosphere_resource", lambda: profile_resource)
    source = contexts("ecoinvent", "3.12", model)
    target = contexts("uvek", "2025")
    rule = profile_resource["replace"][0]
    supplier = tuple(rule["source"][field] for field in FIELDS)
    provider = InMemoryCatalogProvider(
        technosphere=[
            TechnosphereCatalog(source.background.technosphere, {supplier}),
            TechnosphereCatalog(target.background.technosphere, {tuple(rule["target"][field] for field in FIELDS)}),
        ]
    )
    data = consumer(rule["source"], amount, 2)
    original = deepcopy(data)
    inventory = BrightwayInventory.from_data(data, context=source)
    if model != "cutoff":
        with pytest.raises(MigrationError, match="system models"):
            inventory.migrate_background(
                target.background, catalog_provider=provider, policy=MigrationPolicy.permissive()
            )
        assert profile_resource == before and inventory.data == data == original
        return
    result = inventory.migrate_background(
        target.background, catalog_provider=provider, policy=MigrationPolicy.permissive()
    )
    exchange = result.data[0]["exchanges"][1]
    assert exchange["amount"] == pytest.approx(amount * 0.1 * sign)
    assert exchange["negative"] is ((amount < 0) != (sign < 0))
    assert exchange["loc"] == pytest.approx(math.log(abs(amount) * 0.1))
    assert exchange["scale"] == 0.2
    assert profile_resource == before
    assert inventory.data == data == original


@pytest.mark.parametrize(
    "profile",
    [
        TechnosphereProfile("ecoinvent", "3.11", "cutoff"),
        TechnosphereProfile("ecoinvent", "3.12.1", "cutoff"),
        TechnosphereProfile("uvek", "2025", "cutoff"),
    ],
)
def test_missing_profile_decisions_fail_closed_without_series_or_model_fallback(profile_resource, profile):
    before = deepcopy(profile_resource)
    selected = select_profile_sign_rules(profile_resource, profile)
    for rule in selected["replace"]:
        assert rule["reference_flow_sign_review"]["status"] == "unresolved"
        assert "No reference-flow sign decision" in rule["reference_flow_sign_review"]["reason"]
        assert rule["conversion_factor"] == 0.1
    assert profile_resource == before


def test_a_profile_with_pending_supplier_review_stays_blocked(profile_resource):
    for rule in profile_resource["replace"]:
        entry = rule["reference_flow_sign_review"]["profiles"][1]
        entry["review"]["status"] = "unresolved"
        del entry["conversion_factor"]
    selected = select_profile_sign_rules(profile_resource, TechnosphereProfile("ecoinvent", "3.12", "consequential"))
    assert all(rule["reference_flow_sign_review"]["status"] == "unresolved" for rule in selected["replace"])
    assert all(rule["conversion_factor"] == 0.1 for rule in selected["replace"])


@pytest.mark.parametrize(
    "problem", ["duplicate", "undeclared", "wrong_factor", "missing_factor", "unresolved_factor", "global_verified"]
)
def test_invalid_profile_sign_decisions_cannot_load(profile_resource, problem):
    review = profile_resource["replace"][0]["reference_flow_sign_review"]
    entry = review["profiles"][0]
    if problem == "duplicate":
        review["profiles"].append(deepcopy(entry))
    elif problem == "undeclared":
        entry["source_profile"]["version"] = "3.6"
    elif problem == "wrong_factor":
        entry["conversion_factor"] *= -1
    elif problem == "missing_factor":
        del entry["conversion_factor"]
    elif problem == "unresolved_factor":
        entry["review"]["status"] = "unresolved"
    else:
        review.update(status="verified", source_sign=1, target_sign=1)
    with pytest.raises(MigrationError):
        validate_migration_resource(profile_resource)


def test_all_approved_model_dependent_rules_have_exact_profile_decisions():
    from scripts.review_uvek_cutoff_feed_products import PRODUCTS as CUTOFF_FEED_PRODUCTS
    from scripts.review_uvek_slag_landfill_components import SOURCES as SLAG_LANDFILL_COMPONENTS

    resource = load_uvek_technosphere_resource()
    ledger = json.loads((ROOT / REVIEW_PATH).read_text(encoding="utf-8"))
    pilot = json.loads((DATA_DIR / "export/uvek_pilot_material_review.json").read_text())
    pilot_rules = {row["rule_id"]: row for row in pilot["rules"]}
    scoped = [
        rule
        for rule in resource["replace"]
        if "profiles" in rule.get("reference_flow_sign_review", {})
        and rule["source"]["reference product"] != "coal tar"
    ]
    # Preserve exact profiles, including partial markets and mixed-sign slag services.
    assert len(scoped) == 990 + len(pilot_rules)
    for rule in scoped:
        if rule["id"] in pilot_rules:
            decision = pilot_rules[rule["id"]]
            entries = rule["reference_flow_sign_review"]["profiles"]
            assert [e["source_profile"] for e in entries] == [e["source_profile"] for e in decision["profiles"]]
            assert all(e["review"]["status"] == "verified" for e in entries)
            assert all(e["conversion_factor"] == decision["conversion_factor"] for e in entries)
            assert rule["target"] == decision["target"]
            continue
        observations = ledger["resources"][resource["name"]][rule["id"]]["source_observations"]
        entries = rule["reference_flow_sign_review"]["profiles"]
        assert {
            (
                "-".join(entry["source_profile"][field] for field in ("family", "version", "system_model")),
                entry["review"]["source_sign"],
            )
            for entry in entries
        } == {tuple(row) for row in observations if row[0].endswith("-cutoff")}
        if rule.get("mapping_method") == "reviewed_source_gas_flare_service":
            from scripts.review_uvek_remaining_gas_flares import SOURCES

            source = tuple(rule["source"][field] for field in FIELDS)
            assert source in SOURCES and len(entries) == 9
            for entry in entries:
                assert entry["review"]["status"] == "verified"
                if "recipe_ref" in entry.get("target", rule["target"]):
                    assert entry["conversion_factor"] == -1
                    assert entry["target"]["recipe_ref"] in resource["recipes"]
                else:
                    assert source in NATURAL_GAS_FLARES
                    assert entry["source_profile"]["version"] in {"3.9", "3.10", "3.11", "3.12"}
                    assert entry["conversion_factor"] == -1 / 36
        elif "recipe_ref" in rule["target"]:
            for entry in entries:
                if entry.get("operation") == "delete":
                    assert rule["mapping_method"] == "reviewed_cutoff_feed_market_recipe"
                    assert entry["approved_omission"] and entry["review"]["status"] == "verified"
                    assert entry["review_ref"] in resource["reviews"]
                    assert "conversion_factor" not in entry and "target" not in entry
                elif entry["review"]["status"] == "verified":
                    assert entry["conversion_factor"] == entry["review"]["source_sign"]
                    assert entry["target"]["recipe_ref"] in resource["recipes"]
                else:
                    assert "target" not in entry and "conversion_factor" not in entry
                    assert rule["mapping_method"] in {
                        "reviewed_market_composition",
                        "reviewed_processing_composition",
                        "reviewed_resolved_child_market_service",
                        "reviewed_steel_residue_service",
                        "reviewed_additional_cutoff_waste_market_service",
                        "reviewed_pet_waste_market_service",
                    }
                    if rule["mapping_method"] == "reviewed_additional_cutoff_waste_market_service":
                        from scripts.review_uvek_additional_cutoff_waste_markets import SOURCES

                        source = tuple(rule["source"][field] for field in FIELDS)
                        label = "-".join(entry["source_profile"][f] for f in ("family", "version", "system_model"))
                        assert label in {p["label"] for p in SOURCES[source]["pending_profiles"]}
        elif tuple(rule["source"][field] for field in FIELDS) in NATURAL_GAS_FLARES:
            for entry in entries:
                if entry["source_profile"]["version"] in {"3.9", "3.10", "3.11", "3.12"}:
                    assert entry["review"]["status"] == "verified" and entry["conversion_factor"] == -1 / 36
                else:
                    assert entry["review"]["status"] == "unresolved" and "conversion_factor" not in entry
        elif tuple(rule["source"][field] for field in FIELDS) in (
            APPROVED_PROFILED_TREATMENTS
            | APPROVED_DISCHARGE_TREATMENTS
            | FLARES
            | LANDFILLS
            | LCD_TREATMENTS
            | DISPOSALS
            | TRANSPORTS
            | CUTOFF_FEED_PRODUCTS
            | SLAG_LANDFILL_COMPONENTS
            | WHOLE_FREIGHT_SOURCES
            | REGIONAL_FREIGHT_SOURCES
            | OLDER_REGIONAL_ROAD_SOURCES
            | {CONCRETE_GRADE_SOURCE: None}
        ):
            assert all(entry["review"]["status"] == "verified" for entry in entries)
            assert all(entry["conversion_factor"] == entry["review"]["source_sign"] for entry in entries)
        elif tuple(rule["source"][field] for field in FIELDS) in APPROVED_STEEL_RECOVERY_OMISSIONS:
            assert all(entry["review"]["status"] == "verified" for entry in entries)
            assert all(entry["operation"] == "delete" and "conversion_factor" not in entry for entry in entries)
        elif tuple(rule["source"][field] for field in FIELDS) in APPROVED_RECOVERY_CREDITS:
            approval = APPROVED_RECOVERY_CREDITS[tuple(rule["source"][field] for field in FIELDS)]
            assert all(entry["review"]["status"] == "verified" for entry in entries)
            assert all(entry["review"]["role_conversion"] == "recovery_credit" for entry in entries)
            assert all(entry["conversion_factor"] == approval["quantity"] for entry in entries)
        elif tuple(rule["source"][field] for field in FIELDS) in APPROVED_FEED_SUBSTITUTION_OMISSIONS:
            for entry in entries:
                if entry["source_profile"]["system_model"] == "consequential":
                    assert entry["review"]["status"] == "verified" and entry["operation"] == "delete"
                else:
                    assert entry["review"]["status"] == "unresolved" and "operation" not in entry
                assert "conversion_factor" not in entry
        elif tuple(rule["source"][field] for field in FIELDS) in APPROVED_NUTRIENT_REFERENCE_OMISSIONS:
            assert all(entry["operation"] == "delete" and entry["review"]["source_sign"] == 1 for entry in entries)
            assert all(
                entry["review"]["status"] == "verified" and "conversion_factor" not in entry for entry in entries
            )
        else:
            assert all(entry["review"]["status"] == "unresolved" for entry in entries)
            assert all("conversion_factor" not in entry for entry in entries)
    coal_tar = next(rule for rule in resource["replace"] if rule["source"]["name"] == "market for coal tar")
    assert len(coal_tar["reference_flow_sign_review"]["profiles"]) == 7


def test_inventory_reinspection_preserves_independent_profile_approval():
    observations = [("ecoinvent-3.12-cutoff", 1), ("ecoinvent-3.12-consequential", -1)]
    reviews = model_specific_review(observations, ["Synthetic evidence"])
    approved = reviews[0]
    approved["review"].update(status="verified", reason="Independent synthetic supplier approval")
    approved["conversion_factor"] = approved["review"]["source_sign"]
    before = deepcopy(reviews)
    assert model_specific_review(observations, ["Synthetic evidence"], reviews) == reviews
    assert reviews == before
    approved["review"]["source_sign"] *= -1
    with pytest.raises(ValueError, match="changed after profile approval"):
        model_specific_review(observations, ["Synthetic evidence"], reviews)


@pytest.mark.parametrize("version", ["3.6", "3.7", "3.8", "3.9", "3.10", "3.11", "3.12"])
@pytest.mark.parametrize("model", ["cutoff"])
@pytest.mark.parametrize("amount", [-10, 10])
@pytest.mark.parametrize("uncertainty", [2, 3])
def test_coal_tar_proxy_respects_approved_versions_and_preserves_credits(version, model, amount, uncertainty):
    supplier = dict(zip(FIELDS, COAL_TAR_SOURCE, strict=True))
    data = consumer(supplier, amount, uncertainty)
    before = deepcopy(data)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", version, model))
    target = contexts("uvek", "2025").background
    if model == "consequential" and version in {"3.6", "3.7", "3.8", "3.9"}:
        converted = inventory.migrate_background(target, policy=MigrationPolicy.permissive())
        assert converted.data[0]["exchanges"] == [before[0]["exchanges"][0]]
        warnings = [i for i in converted.last_migration_report.issues if i.code == "migration.technosphere_omission"]
        assert len(warnings) == 1 and "26.6 MJ" in warnings[0].message
    else:
        converted = inventory.migrate_background(target, policy=MigrationPolicy.permissive())
        exchange = converted.data[0]["exchanges"][1]
        assert tuple(exchange[field] for field in FIELDS) == COAL_TAR_TARGET
        assert {key: exchange[key] for key in before[0]["exchanges"][1] if key not in FIELDS} == {
            key: value for key, value in before[0]["exchanges"][1].items() if key not in FIELDS
        }
        warnings = [
            issue
            for issue in converted.last_migration_report.issues
            if issue.code == "migration.technosphere_information_loss"
        ]
        assert len(warnings) == 1
        assert "energy allocation" in warnings[0].message
    assert inventory.data == data == before


def test_coal_tar_proxy_requires_explicit_information_loss_acceptance():
    data = consumer(dict(zip(FIELDS, COAL_TAR_SOURCE, strict=True)), 10)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.12"))
    with pytest.raises(MigrationError) as caught:
        inventory.migrate_background(
            contexts("uvek", "2025").background, policy=MigrationPolicy(on_heuristic_mapping=PolicyAction.WARN)
        )
    assert "migration.technosphere_information_loss" in {issue.code for issue in caught.value.report.issues}
    assert inventory.data == data


def test_coal_tar_has_seven_cutoff_production_profiles_and_no_cross_model_omissions():
    resource = load_uvek_technosphere_resource()
    rule = next(
        rule for rule in resource["replace"] if tuple(rule["source"][field] for field in FIELDS) == COAL_TAR_SOURCE
    )
    entries = rule["reference_flow_sign_review"]["profiles"]
    assert all(entry["review"]["status"] == "verified" for entry in entries)
    assert len([entry for entry in entries if "conversion_factor" in entry]) == 7
    assert not any(entry.get("operation") == "delete" for entry in entries)
    assert "4.1%" in resource["reviews"][rule["review_ref"]]["limitations"][0]
    ledger = json.loads((ROOT / REVIEW_PATH).read_text(encoding="utf-8"))
    assert ledger["summary"]["ecoinvent-to-uvek-2025.json"]["approved_with_profile_omissions"] == 118
    assert ledger["summary"]["ecoinvent-to-uvek-2025.json"].get("partially_approved_with_profile_omissions", 0) == 0
    assert ledger["summary"]["ecoinvent-to-uvek-2025.json"]["unresolved"] == 328


def test_changed_coal_tar_observations_require_reapproval():
    observations = [
        (
            f"ecoinvent-{version}-{model}",
            -1 if model == "consequential" and version in {"3.6", "3.7", "3.8", "3.9"} else 1,
        )
        for version in ("3.6", "3.7", "3.8", "3.9", "3.10", "3.11", "3.12")
        for model in ("cutoff", "consequential")
    ]
    assert len(coal_tar_review(observations, ["Synthetic test evidence"])) == 14
    observations[0] = (observations[0][0], -1)
    with pytest.raises(ValueError, match="changed"):
        coal_tar_review(observations, ["Synthetic test evidence"])


@pytest.mark.parametrize("version", ["3.6", "3.7", "3.8", "3.9", "3.10", "3.11", "3.12"])
@pytest.mark.parametrize("model", ["cutoff"])
@pytest.mark.parametrize("amount", [-10, 10])
@pytest.mark.parametrize("uncertainty", [2, 3])
@pytest.mark.parametrize(
    "supplier,target_supplier",
    [
        (BATTERY_SOURCE, BATTERY_TARGET),
        (PYRO_BATTERY_SOURCE, PYRO_BATTERY_TARGET),
        (LI_ION_MARKET_SOURCE, LI_ION_MARKET_TARGET),
        (NIMH_SOURCE, NIMH_TARGET),
        (NIMH_MARKET_SOURCE, NIMH_TARGET),
    ],
)
def test_approved_battery_treatment_converts_waste_and_avoided_treatment_consistently(
    version, model, amount, uncertainty, supplier, target_supplier
):
    data = consumer(dict(zip(FIELDS, supplier, strict=True)), amount, uncertainty)
    before = deepcopy(data)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", version, model))
    converted = inventory.migrate_background(contexts("uvek", "2025").background, policy=MigrationPolicy.permissive())
    exchange = converted.data[0]["exchanges"][1]
    assert tuple(exchange[field] for field in FIELDS) == target_supplier
    assert exchange["amount"] == -amount
    assert exchange["formula"] == "(dose) * (-1.0)"
    assert exchange["scale"] == 0.2
    if uncertainty == 2:
        assert exchange["negative"] is (amount > 0)
        assert exchange["loc"] == pytest.approx(math.log(abs(amount)))
    else:
        assert exchange["loc"] == -amount
        assert exchange["minimum"] == -amount - 1
        assert exchange["maximum"] == -amount + 1
    warnings = [
        issue
        for issue in converted.last_migration_report.issues
        if issue.code == "migration.technosphere_information_loss"
    ]
    assert len(warnings) == 1
    if supplier[0].startswith("market"):
        assert "transport burdens are not represented" in warnings[0].message
    else:
        assert "100%" in warnings[0].message
    assert inventory.data == data == before


def test_battery_waste_approval_does_not_change_recovered_product_mappings():
    resource = load_uvek_technosphere_resource()
    source_name = BATTERY_SOURCE[0]
    products = {
        rule["source"]["reference product"]: rule
        for rule in resource["replace"]
        if rule["source"]["name"] == source_name
    }
    assert products["used Li-ion battery"]["target"]["name"] == BATTERY_TARGET[0]
    assert products["cobalt"]["target"]["name"] == "Cobalt, at plant"
    assert products["lithium carbonate"]["target"]["name"] == "Lithium carbonate, at plant"
    assert "conversion_factor" not in products["cobalt"]
    assert "conversion_factor" not in products["lithium carbonate"]


@pytest.mark.parametrize(
    "supplier,target_supplier",
    [(source, approval["target"]) for source, approval in APPROVED_WASTE_TREATMENTS.items()],
)
def test_treatment_correction_is_exact_and_requires_reapproval_for_unknown_targets(supplier, target_supplier):
    original = {
        "source": dict(zip(FIELDS, supplier, strict=True)),
        "target": dict(
            zip(
                FIELDS,
                APPROVED_WASTE_TREATMENTS[supplier].get("previous_target", BATTERY_PREVIOUS_TARGET),
                strict=True,
            )
        ),
    }
    before = deepcopy(original)
    corrected = approved_supplier_rule(original)
    assert tuple(corrected["target"][field] for field in FIELDS) == target_supplier
    assert original == before
    assert approved_supplier_rule(corrected) == corrected
    original["target"]["name"] = "Unreviewed supplier"
    with pytest.raises(ValueError, match="changed after"):
        approved_supplier_rule(original)


@pytest.mark.parametrize(
    "supplier,version",
    [
        (source, version)
        for source, approval in APPROVED_GEOGRAPHIC_TREATMENTS.items()
        for version in approval["source_versions"]
    ],
)
@pytest.mark.parametrize("model", ["cutoff"])
@pytest.mark.parametrize("amount", [-10, 10])
@pytest.mark.parametrize("uncertainty", [2, 3])
def test_pet_geographic_proxy_preserves_treatment_direction_and_discloses_loss(
    supplier, version, model, amount, uncertainty
):
    data = consumer(dict(zip(FIELDS, supplier, strict=True)), amount, uncertainty)
    before = deepcopy(data)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", version, model))
    converted = inventory.migrate_background(contexts("uvek", "2025").background, policy=MigrationPolicy.permissive())
    exchange = converted.data[0]["exchanges"][1]
    assert tuple(exchange[field] for field in FIELDS) == PET_INCINERATION_TARGET
    assert exchange["amount"] == -amount
    assert exchange["formula"] == "(dose) * (-1.0)"
    assert exchange["scale"] == 0.2
    if uncertainty == 2:
        assert exchange["negative"] is (amount > 0)
        assert exchange["loc"] == pytest.approx(math.log(abs(amount)))
    else:
        assert exchange["loc"] == -amount
        assert exchange["minimum"] == -amount - 1
        assert exchange["maximum"] == -amount + 1
    warnings = [
        issue
        for issue in converted.last_migration_report.issues
        if issue.code == "migration.technosphere_information_loss"
    ]
    assert len(warnings) == 1
    assert f"{supplier[2]} PET incineration" in warnings[0].message
    assert "CH service for 0.2% water" in warnings[0].message
    assert "not established or quantified" in warnings[0].message
    assert inventory.data == data == before


def test_pet_geographic_proxy_requires_information_loss_acceptance():
    supplier = dict(zip(FIELDS, (PET_INCINERATION_NAME, PET_INCINERATION_PRODUCT, "GLO", "kilogram"), strict=True))
    data = consumer(supplier, -10)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.12"))
    with pytest.raises(MigrationError) as caught:
        inventory.migrate_background(
            contexts("uvek", "2025").background, policy=MigrationPolicy(on_heuristic_mapping=PolicyAction.WARN)
        )
    assert "migration.technosphere_information_loss" in {issue.code for issue in caught.value.report.issues}
    assert inventory.data == data


def test_pet_approval_preserves_other_geographies_energy_and_treatment_references():
    resource = load_uvek_technosphere_resource()
    excluded = [
        rule
        for rule in resource["replace"]
        if "treatment of waste polyethylene terephthalate" in rule["source"]["name"]
        and tuple(rule["source"][field] for field in FIELDS) not in APPROVED_GEOGRAPHIC_TREATMENTS
        and tuple(rule["source"][field] for field in FIELDS) not in APPROVED_INCINERATION_COMPOSITION_PROXIES
        and tuple(rule["source"][field] for field in FIELDS) not in DISPOSALS
    ]
    assert excluded
    for rule in excluded:
        before = deepcopy(rule)
        assert approved_supplier_rule(rule) == rule == before
    for name, location, version, product in [
        # Bottom-ash services have a separate complete review and runtime tests
        # in test_uvek_bottom_ash; they are no longer part of this blocked cohort.
        # RoW is independently approved; its uninspected patch profile remains blocked.
        ("treatment of waste polyethylene terephthalate, sanitary landfill", "RoW", "3.9.1", PET_INCINERATION_PRODUCT),
    ]:
        data = consumer(dict(zip(FIELDS, (name, product, location, "kilogram"), strict=True)), -10)
        inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", version))
        with pytest.raises(MigrationError) as caught:
            inventory.migrate_background(contexts("uvek", "2025").background, policy=MigrationPolicy.permissive())
        assert "migration.reference_flow_sign_unresolved" in {issue.code for issue in caught.value.report.issues}
        assert inventory.data == data


@pytest.mark.parametrize(
    "supplier,version",
    [
        (source, version)
        for source, approval in APPROVED_WASTEWATER_TREATMENTS.items()
        for version in approval["source_versions"]
    ],
)
@pytest.mark.parametrize("model", ["cutoff"])
@pytest.mark.parametrize("amount", [-10, 10])
@pytest.mark.parametrize("uncertainty", [2, 3])
def test_wastewater_service_output_approval_preserves_direction_and_boundary_warnings(
    supplier, version, model, amount, uncertainty
):
    approval = APPROVED_WASTEWATER_TREATMENTS[supplier]
    data = consumer(dict(zip(FIELDS, supplier, strict=True)), amount, uncertainty)
    before = deepcopy(data)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", version, model))
    converted = inventory.migrate_background(contexts("uvek", "2025").background, policy=MigrationPolicy.permissive())
    exchange = converted.data[0]["exchanges"][1]
    assert tuple(exchange[field] for field in FIELDS) == approval["target"]
    assert exchange["amount"] == -amount
    assert exchange["formula"] == "(dose) * (-1.0)"
    assert exchange["scale"] == 0.2
    if uncertainty == 2:
        assert exchange["negative"] is (amount > 0)
        assert exchange["loc"] == pytest.approx(math.log(abs(amount)))
    else:
        assert exchange["loc"] == -amount
        assert exchange["minimum"] == -amount - 1
        assert exchange["maximum"] == -amount + 1
    warnings = [
        issue
        for issue in converted.last_migration_report.issues
        if issue.code == "migration.technosphere_information_loss"
    ]
    assert len(warnings) == 1
    if approval["source_includes_sewer"]:
        assert "Both inspected source and proxy include sewer infrastructure" in warnings[0].message
        assert "double-counted" not in warnings[0].message
    else:
        assert "upstream market" in warnings[0].message and "double-counted" in warnings[0].message
    assert "LCIA equivalence is not established" in warnings[0].message
    if supplier in APPROVED_PARTIALLY_UNTREATED_MARKETS:
        assert "100% treatment of the full source volume" in warnings[0].message
        assert "untreated-discharge emissions are omitted" in warnings[0].message
        assert "understate water-pollution impacts and overstate treatment burdens" in warnings[0].message
        assert ("87.3%" if supplier[2] == "RER" else "41.3%") in warnings[0].message
    assert inventory.data == data == before


@pytest.mark.parametrize("source", list(APPROVED_WASTEWATER_TREATMENTS))
def test_wastewater_proxy_requires_explicit_loss_acceptance_and_records_native_service_role(source):
    approval = APPROVED_WASTEWATER_TREATMENTS[source]
    data = consumer(dict(zip(FIELDS, source, strict=True)), -10)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", approval["source_versions"][-1]))
    with pytest.raises(MigrationError) as caught:
        inventory.migrate_background(
            contexts("uvek", "2025").background, policy=MigrationPolicy(on_heuristic_mapping=PolicyAction.WARN)
        )
    assert "migration.technosphere_information_loss" in {issue.code for issue in caught.value.report.issues}
    assert inventory.data == data
    resource = load_uvek_technosphere_resource()
    rule = next(rule for rule in resource["replace"] if tuple(rule["source"][field] for field in FIELDS) == source)
    ledger = json.loads((ROOT / REVIEW_PATH).read_text(encoding="utf-8"))
    decision = ledger["resources"][resource["name"]][rule["id"]]
    assert decision["uvek_native_flow_type"] == "PRODUCT_FLOW"
    assert decision["review"]["status"] == "verified"
    assert "not recovered material" in decision["review"]["reason"]
    assert decision["source_observations"] == sorted(
        [
            [f"ecoinvent-{version}-{model}", -1]
            for version in approval["source_versions"]
            for model in ("cutoff", "consequential")
        ]
    )


@pytest.mark.parametrize(
    "name,product,location",
    [
        (
            "treatment of leachate, SLF, hard coal ash, wastewater treatment",
            "leachate, SLF, hard coal ash",
            "GLO",
        ),
        ("market for leachate, SLF, hard coal ash", "leachate, SLF, hard coal ash", "GLO"),
    ],
)
def test_wastewater_approval_excludes_unapproved_effluents(name, product, location):
    data = consumer(dict(zip(FIELDS, (name, product, location, "cubic meter"), strict=True)), -10)
    before = deepcopy(data)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.12"))
    with pytest.raises(MigrationError) as caught:
        inventory.migrate_background(contexts("uvek", "2025").background, policy=MigrationPolicy.permissive())
    assert "migration.reference_flow_sign_unresolved" in {issue.code for issue in caught.value.report.issues}
    assert inventory.data == data == before


@pytest.mark.parametrize("source", [*APPROVED_PARTIALLY_UNTREATED_MARKETS, *APPROVED_GENERIC_WASTEWATER])
def test_full_treatment_proxy_preserves_separate_foreground_pollutants(source):
    from brightpath.background import PackageCatalogProvider

    provider = PackageCatalogProvider()
    biosphere = provider.load_biosphere(contexts("uvek", "2025").background.biosphere).identities
    name, categories, unit = next(row for row in biosphere if row[0] == "COD, Chemical Oxygen Demand")
    data = consumer(dict(zip(FIELDS, source, strict=True)), -10)
    discharge = {"name": name, "categories": list(categories), "unit": unit, "type": "biosphere", "amount": 0.42}
    data[0]["exchanges"].append(discharge)
    before = deepcopy(data)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.12"))
    result = inventory.migrate_background(contexts("uvek", "2025").background, policy=MigrationPolicy.permissive())
    assert result.data[0]["exchanges"][1]["amount"] == 10
    assert result.data[0]["exchanges"][2] == discharge
    assert len(result.data[0]["exchanges"]) == len(before[0]["exchanges"])
    assert inventory.data == data == before


@pytest.mark.parametrize(
    "source,version",
    [
        (source, version)
        for source, approval in APPROVED_GENERIC_WASTEWATER.items()
        for version in approval["source_versions"]
    ],
)
@pytest.mark.parametrize("model", ["cutoff"])
@pytest.mark.parametrize("amount", [-10, 10])
@pytest.mark.parametrize("uncertainty", [2, 3])
def test_generic_effluent_proxy_preserves_volume_and_reports_composition_loss(
    source, version, model, amount, uncertainty
):
    approval = APPROVED_GENERIC_WASTEWATER[source]
    data = consumer(dict(zip(FIELDS, source, strict=True)), amount, uncertainty)
    before = deepcopy(data)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", version, model))
    result = inventory.migrate_background(contexts("uvek", "2025").background, policy=MigrationPolicy.permissive())
    exchange = result.data[0]["exchanges"][1]
    assert tuple(exchange[field] for field in FIELDS) == GENERIC_WASTEWATER_TARGET
    assert exchange["amount"] == -amount  # COD is evidence of information loss, not a quantity factor.
    assert exchange["formula"] == "(dose) * (-1.0)"
    assert exchange["scale"] == 0.2
    if uncertainty == 2:
        assert exchange["negative"] is (amount > 0)
        assert exchange["loc"] == pytest.approx(math.log(abs(amount)))
    else:
        assert exchange["loc"] == -amount
        assert exchange["minimum"] == -amount - 1
        assert exchange["maximum"] == -amount + 1
    warnings = [i for i in result.last_migration_report.issues if i.code == "migration.technosphere_information_loss"]
    assert len(warnings) == 1
    message = warnings[0].message
    assert f"COD {approval['source_cod_kg_per_m3']} kg/m3" in message
    assert "COD 0.1556 kg/m3" in message and "without COD-based scaling" in message
    if source[0].startswith("market"):
        assert "untreated-discharge emissions are omitted" in message and "100% treatment" in message
    else:
        assert "double-counted" in message and "upstream market" in message
        assert "untreated-discharge emissions are omitted" not in message
    assert inventory.data == data == before


@pytest.mark.parametrize("source", list(APPROVED_GENERIC_WASTEWATER))
def test_generic_composition_proxy_requires_loss_acceptance_and_retains_original_factor(source):
    data = consumer(dict(zip(FIELDS, source, strict=True)), -10)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.12"))
    with pytest.raises(MigrationError) as caught:
        inventory.migrate_background(
            contexts("uvek", "2025").background, policy=MigrationPolicy(on_heuristic_mapping=PolicyAction.WARN)
        )
    assert "migration.technosphere_information_loss" in {i.code for i in caught.value.report.issues}
    assert inventory.data == data
    resource = load_uvek_technosphere_resource()
    rule = next(r for r in resource["replace"] if tuple(r["source"][f] for f in FIELDS) == source)
    decision = json.loads((ROOT / REVIEW_PATH).read_text(encoding="utf-8"))["resources"][resource["name"]][rule["id"]]
    assert decision["factor"] == -1 and decision["original_factor"] == 1
    assert decision["uvek_native_flow_type"] == "WASTE_FLOW"
    assert (
        tuple(decision["original_target"][f] for f in FIELDS) == APPROVED_GENERIC_WASTEWATER[source]["previous_target"]
    )
    assert decision["source_observations"] == sorted(
        [[f"ecoinvent-{version}-{model}", -1] for version in ("3.11", "3.12") for model in ("cutoff", "consequential")]
    )


@pytest.mark.parametrize("change", [None, "positive_source", "native_waste_role", "missing_profile", "negative_target"])
def test_service_approval_reinspection_rejects_changed_reference_conventions(tmp_path, monkeypatch, change):
    # This fixture audits one wastewater correspondence. Unrelated global
    # esterquat/feed evidence is absent by design; its strict validators have
    # separate missing/changed-evidence tests and the full audit replay.
    for validator in (
        "validate_esterquat_providers",
        "validate_cutoff_feed_inventories",
        "validate_cutoff_feed_market_bindings",
    ):
        monkeypatch.setattr(f"scripts.review_uvek_reference_signs.{validator}", lambda *args: None)
    source = (
        "treatment of wastewater from hard fibreboard production, wastewater treatment",
        "wastewater from hard fibreboard production",
        "RER",
        "cubic meter",
    )
    approval = APPROVED_WASTEWATER_TREATMENTS[source]
    target = approval["target"]
    export = tmp_path / "brightpath/data/export"
    catalogs = export / "reference_catalogs"
    catalogs.mkdir(parents=True)
    native = export / "openlca_references/uvek__2025__cutoff__ecoinvent__3.10.json"
    native.parent.mkdir()

    def catalog_row(record):
        return dict(zip(("name", "reference_product", "location", "unit"), record, strict=True))

    row = catalog_row(target)
    row.update(native_flow_type="PRODUCT_FLOW", native_reference_is_input=False)
    if change == "native_waste_role":
        row.update(native_flow_type="WASTE_FLOW", native_reference_is_input=True)
    native.write_text(json.dumps({"technosphere": [row]}), encoding="utf-8")
    profiles = []
    for version in ("3.6", "3.7", "3.8", "3.9", "3.10", "3.11", "3.12"):
        for model in ("cutoff", "consequential"):
            present = version in approval["source_versions"] and not (
                change == "missing_profile" and version == "3.12" and model == "consequential"
            )
            references = [[*source, 1 if change == "positive_source" else -1]] if present else []
            profiles.append({"profile": ["ecoinvent", version, model], "references": references})
            (catalogs / f"ecoinvent__{version}__{model}.json").write_text(
                json.dumps({"technosphere": [catalog_row(source)] if present else []}), encoding="utf-8"
            )
    profiles.append(
        {"profile": ["uvek", "2025", "cutoff"], "references": [[*target, -1 if change == "negative_target" else 1]]}
    )
    (catalogs / "uvek__2025__cutoff.json").write_text(
        json.dumps({"technosphere": [catalog_row(target)]}), encoding="utf-8"
    )
    migrations = tmp_path / "brightpath/data/migrations/uvek"
    migrations.mkdir(parents=True)
    rule = {
        "id": "inspected-service",
        "source": dict(zip(FIELDS, source, strict=True)),
        "target": dict(zip(FIELDS, target, strict=True)),
    }
    (migrations / "ecoinvent-to-uvek-2025.json").write_text(
        json.dumps({"name": "forward", "replace": [rule]}), encoding="utf-8"
    )
    (migrations / "uvek-2025-to-ecoinvent-3.12-cutoff.json").write_text(
        json.dumps({"name": "reverse", "replace": []}), encoding="utf-8"
    )
    if change is not None:
        with pytest.raises(ValueError, match="changed"):
            build_review(tmp_path, profiles)
    else:
        result = build_review(tmp_path, profiles)["resources"]["forward"]["inspected-service"]
        assert result["review"]["status"] == "verified" and result["factor"] == -1
        assert result["uvek_native_flow_type"] == "PRODUCT_FLOW"
