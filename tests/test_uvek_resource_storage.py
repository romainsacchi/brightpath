import json
from copy import deepcopy
from pathlib import Path

import pytest
from uvek_review_helpers import historical_forward_resource

from brightpath import DATA_DIR
from brightpath.migrations.resources import load_uvek_technosphere_resource
from brightpath.migrations.schema import (
    dumps_migration_resource,
    intern_review,
    normalize_migration_resource,
    rule_review,
    validate_migration_resource,
)
from scripts.generate_uvek_export import legacy_export_projection as legacy_projection
from scripts.generate_uvek_migration_resources import _write_resource
from scripts.review_uvek_dedicated_disposals import DISPOSALS
from scripts.review_uvek_eaf_landfills import LANDFILLS
from scripts.review_uvek_lcd_treatments import LCD_TREATMENTS
from scripts.review_uvek_modal_transport import TRANSPORTS
from scripts.review_uvek_natural_gas_flares import FLARES as NATURAL_GAS_FLARES
from scripts.review_uvek_refinery_flare import FLARES


def test_shared_reviews_and_inline_identities_round_trip_without_mutation(migration_resource):
    original = deepcopy(migration_resource)
    restored = normalize_migration_resource(migration_resource)
    serialized = dumps_migration_resource(restored)
    assert migration_resource == original == restored == json.loads(serialized)
    assert isinstance(restored["replace"][0]["target"], dict)
    assert len(restored["reviews"]) == 1
    assert restored["replace"][0]["review_ref"] == restored["replace"][1]["review_ref"]
    assert dumps_migration_resource(json.loads(serialized)) == serialized
    assert (
        intern_review(restored, rule_review(restored, restored["replace"][0])) == restored["replace"][0]["review_ref"]
    )
    assert len(restored["reviews"]) == 1


def test_copies_and_resolved_reviews_are_independently_owned(migration_resource):
    restored = normalize_migration_resource(migration_resource)
    restored["replace"][0]["target"]["name"] = "Changed"
    review = rule_review(restored, restored["replace"][0])
    review["limitations"].append("Changed")
    assert restored["replace"][1] == migration_resource["replace"][1]
    assert rule_review(restored, restored["replace"][0]) == rule_review(
        migration_resource, migration_resource["replace"][0]
    )
    assert migration_resource["replace"][0]["target"]["name"] == "Target"


def test_generator_uses_the_same_shared_writer(migration_resource, tmp_path):
    for filename in ("ecoinvent-to-uvek-2025.json", "uvek-2025-to-ecoinvent-3.12-cutoff.json"):
        path = tmp_path / filename
        _write_resource(path, migration_resource)
        assert path.read_text(encoding="utf-8") == dumps_migration_resource(migration_resource)
        assert json.loads(path.read_text(encoding="utf-8")) == migration_resource


def test_both_packaged_uvek_resources_use_one_schema():
    for filename, count in (("ecoinvent-to-uvek-2025.json", 30732), ("uvek-2025-to-ecoinvent-3.12-cutoff.json", 1518)):
        path = DATA_DIR / "migrations/uvek" / filename
        result = json.loads(path.read_text(encoding="utf-8"))
        validate_migration_resource(result)
        assert result["schema"] == "brightpath.migration"
        assert result["schema_version"] == 3
        assert len(result["replace"]) + len(result["delete"]) == result["coverage"]["mapped_identities"] == count
        assert result["reverse_policy"] == "explicit_only"
        assert {
            "replace",
            "disaggregate",
            "delete",
            "reviews",
            "recipes",
            "source_profiles",
            "target_profile",
        } <= result.keys()
        assert not {"rules", "targets", "source_profile"} & result.keys()
        assert all(isinstance(rule["target"], dict) for rule in result["replace"])
        assert {
            rule["review_ref"]
            for operation in ("replace", "disaggregate", "delete")
            for rule in result[operation]
            if "review_ref" in rule
        } | {
            entry["review_ref"]
            for rule in result["replace"]
            for entry in rule.get("reference_flow_sign_review", {}).get("profiles", [])
            if "review_ref" in entry
        } == result[
            "reviews"
        ].keys()
    forward = load_uvek_technosphere_resource()
    # Includes subsequent heat/deferred reviews and the cement landfill service.
    assert len(forward["reviews"]) == 796
    assert len(forward["delete"]) == forward["coverage"]["omitted_identities"] == 257
    reverse = json.loads((DATA_DIR / "migrations/uvek/uvek-2025-to-ecoinvent-3.12-cutoff.json").read_text())
    assert len(reverse["replace"]) == 1517
    assert len(reverse["delete"]) == reverse["coverage"]["omitted_identities"] == 1
    assert (
        len(
            {
                tuple(rule["target"][field] for field in ("name", "reference product", "location", "unit"))
                for rule in forward["replace"]
                if "recipe_ref" not in rule["target"]
            }
        )
        == 3993
    )


@pytest.mark.parametrize(
    "filename,expected",
    [
        # Three reviewed commodity targets changed; their identities, consent and
        # quantity preservation are tested in test_uvek_native_commodity_correspondences.
        ("ecoinvent-to-uvek-2025.json", "6eae7b664fec2e50437cb99084f734c3e342b5a44c625a54a4b7c2cfdead343d"),
        ("uvek-2025-to-ecoinvent-3.12-cutoff.json", "74ad437135442bad5fe82055ab8b4fda188a46a22eabb7a9c49a88cb7340112e"),
    ],
)
def test_all_mapping_values_and_runtime_evidence_match_the_baseline(filename, expected):
    # Reverse: the pre-upgrade v2 file. Forward: the saved pre-cleanup file,
    # projecting reviews to the retained rationale and limitations fields,
    # with only the three reviewed forward commodity targets corrected.
    import hashlib

    migration_resource = json.loads((DATA_DIR / "migrations/uvek" / filename).read_text(encoding="utf-8"))
    if filename == "ecoinvent-to-uvek-2025.json":
        # The frozen hash covers historical authoring inputs, including retired
        # model rows. Runtime cut-off preservation is checked independently.
        migration_resource = historical_forward_resource()
    if filename.startswith("uvek-"):
        # Independently fingerprinted gas additions have their own exact guard;
        # retain the frozen publisher/legacy mapping hash below.
        migration_resource = legacy_projection(migration_resource)
    sign_audit = json.loads((DATA_DIR / "export/uvek_reference_flow_sign_review.json").read_text(encoding="utf-8"))
    sign_decisions = sign_audit["resources"][migration_resource["name"]]
    fields = ("source", "target", "conversion_factor", "classification", "confidence", "mapping_method")
    rows = []
    rules = list(migration_resource["replace"])
    rules.extend(
        decision["original_rule"] for decision in sign_decisions.values() if decision.get("operation") == "delete"
    )
    if migration_resource["delete"]:
        rules.sort(
            key=lambda rule: tuple(rule["source"][field] for field in ("name", "reference product", "location", "unit"))
        )
    for rule in rules:
        recipe_decision = sign_decisions.get(rule["id"], {})
        if recipe_decision.get("operation") == "recipe":
            assert rule["target"] == recipe_decision["target"]
            rule = recipe_decision["original_rule"]
        # Preserve the exact historical 354-rule contract while testing the
        # publisher-backed additions independently in test_uvek_export_lineage.
        if rule.get("mapping_method") == "publisher_product_correspondence":
            continue
        row = {key: rule[key] for key in fields if key in rule}
        # These reconstructed services had implicit default factors in the
        # frozen publisher projection. Waste services formerly used flat sign corrections;
        # retain that historical representation while testing runtime factors separately.
        if recipe_decision.get("mapping_method") in {
            "reviewed_cable_wire_plastic_market",
            "reviewed_cable_treatment_composition",
            "reviewed_copper_ash_source_reconstruction",
            "reviewed_waste_concrete_inert_reconstruction",
            "reviewed_older_swiss_rail",
        }:
            assert recipe_decision["original_factor"] == row["conversion_factor"] == 1
            expected_factor = 1 if recipe_decision["mapping_method"] == "reviewed_older_swiss_rail" else -1
            assert recipe_decision["factor"] == expected_factor
            row.pop("conversion_factor")
        decision = sign_decisions.get(rule["id"])
        if decision and decision.get("operation") == "recipe":
            decision = None
        if decision and "original_target" in decision:
            assert row["target"]["name"] in {
                "Disposal, Li-ions batteries, hydrometallurgical",
                "Disposal, Li-ions batteries, pyrometallurgical",
                "Disposal, Li-ions batteries, mixed technology",
                "Disposal, NiMH batteries",
                "Disposal, polyethylene terephtalate, 0.2% water, to municipal incineration",
                "Disposal, polyethylene, 0.4% water, to municipal incineration",
                "Disposal, coatings in CRT screens,  to municipal waste incineration",
                "Disposal, residues, mechanical treatment, CRT screen, in MSWI",
                "Disposal, treatment of CRT glass",
                "Dismantling, CRT screen, manually, at plant",
                "Dismantling, CRT screen, mechanically, at plant",
                "xx Disposal, CRT screen, 17 inches, to WEEE treatment",
                "Disposal, glass, 0% water, to construction waste landfill",
                "Disposal, frit for CRT tube production, to residual material landfill",
                "Treatment hard fibreboard production effluent, to wastewater treatment, class 1",
                "Treatment, medium density fibreboard production effluent, to wastewater treatment, class 1",
                "Treatment, particle board production effluent, to wastewater treatment, class 1",
                "Treatment, sewage, to wastewater treatment, class 1",
                "Zinc concentrate, at beneficiation",
                "Zinc, primary, at regional storage",
                "Bronze, at plant",
                "Chromium steel 18/8, at plant",
                "Copper, at regional storage",
                "Concrete for building construction, CEM III, A, NPK B, primary, at plant",
                "Pesticide unspecified, at regional storehouse",
                "Discharge, produced water, onshore",
                "Discharge, produced water, offshore",
                "Refinery gas, burned in flare",
                "Natural gas, sweet, burned in production flare",
                "xx DDGS, from corn, at distillery",
                "xx DDGS, from potatoes, at distillery",
                "xx DDGS, from rye, at distillery",
                "xx Grass fibres, at digestion",
                "xx Grass fibres, at fermentation",
                "Sweet sorghum grains, at farm",
                "Pulps, from sugar beet, at sugar refinery",
                "Yeast paste, from whey, at fermentation",
                "Disposal, dust, alloyed EAF steel, 15.4% water, to residual material landfill",
                "Disposal, dust, unalloyed EAF steel, 15.4% water, to residual material landfill",
                "Disposal, slag, unalloyed electr. steel, 0% water, to residual material landfill",
                "Dismantling, LCD screen, manually, at plant",
                "Dismantling, LCD screen, mechanically, at plant",
                "Disposal, residues, mechanical treatment, LCD screen, in MSWI",
                "Transport, freight, rail",
                "Transport, freight, lorry, fleet average",
                "Transport, freight, lorry, 3.5t gross weight, fleet average",
                "Transport, barge",
                "Disposal, catalyst base CH2O production, 0% water, to residual material landfill",
                "xx Disposal, polyethylene terephtalate, 0.2% water, to sanitary landfill",
                "Disposal, glider, for electric scooter",
                "Disposal, powertrain, for electric scooter",
                "Disposal, internal combustion engine, for passenger car",
                "Sewer grid, class 1",
                "Sewer grid, class 2",
                "Sewer grid, class 3",
                "Sewer grid, class 4",
                "Sewer grid, class 5",
                "Slag compartment",
                "Process-specific burdens, slag compartment",
            }
            row["target"] = decision["original_target"]
        if decision and decision["factor"] != decision["original_factor"]:
            assert row["conversion_factor"] == decision["factor"]
            if decision["original_factor"] == 1:
                row.pop("conversion_factor")
            else:
                row["conversion_factor"] = decision["original_factor"]
        # Compare software-neutral method labels with their historical names.
        if row.get("mapping_method") in {"pairwise_review", "product_checked", "followup_review"}:
            row["mapping_method"] = "clic_" + row["mapping_method"]
        if "review_ref" in rule:
            if recipe_decision.get("operation") == "recipe":
                # Runtime pruning can remove the restored rule's old review;
                # its exact original content remains bound in the audit ledger.
                if recipe_decision["original_review"] is not None:
                    row["review"] = recipe_decision["original_review"]
            elif decision and "original_review" in decision:
                if decision.get("operation") != "delete":
                    assert rule_review(migration_resource, rule) == decision["supplier_proxy_review"]
                if decision["original_review"] is not None:
                    row["review"] = decision["original_review"]
            else:
                row["review"] = rule_review(migration_resource, rule)
        if filename.startswith("uvek-"):
            row["id"] = rule["id"]
        rows.append(row)
    recipe_additions = {
        key
        for decision in sign_decisions.values()
        if decision.get("operation") == "recipe"
        for key in decision["recipes"]
    }
    values = {
        "replace": rows,
        "recipes": {k: v for k, v in migration_resource["recipes"].items() if k not in recipe_additions},
    }
    encoded = json.dumps(values, sort_keys=True, ensure_ascii=False, separators=(",", ":"), allow_nan=False)
    assert hashlib.sha256(encoded.encode()).hexdigest() == expected


def test_packaged_uvek_resources_exclude_generation_and_audit_metadata():
    for path in (DATA_DIR / "migrations/uvek").glob("*.json"):
        resource = json.loads(path.read_text(encoding="utf-8"))
        assert not {"methodology", "source_catalogs", "evidence_sha256"} & resource.keys()
        for review in resource.get("reviews", {}).values():
            assert review.keys() <= {"rationale", "limitations", "evidence"}
        assert not any(rule.get("mapping_method", "").startswith("clic_") for rule in resource["replace"])
        assert not any(key.startswith("clic_") for key in resource["coverage"].get("mapping_methods", {}))
    forward = load_uvek_technosphere_resource()
    coal_tar = next(rule for rule in forward["replace"] if rule["source"]["name"] == "market for coal tar")
    assert forward["reviews"][coal_tar["review_ref"]].keys() == {"rationale", "limitations", "evidence"}
    battery = next(
        rule
        for rule in forward["replace"]
        if rule["source"]["name"] == "treatment of used Li-ion battery, hydrometallurgical treatment"
        and rule["source"]["reference product"] == "used Li-ion battery"
    )
    assert forward["reviews"][battery["review_ref"]].keys() == {"rationale", "limitations", "evidence"}
    pyro_battery = next(
        rule
        for rule in forward["replace"]
        if rule["source"]["name"] == "treatment of used Li-ion battery, pyrometallurgical treatment"
        and rule["source"]["reference product"] == "used Li-ion battery"
    )
    assert forward["reviews"][pyro_battery["review_ref"]].keys() == {"rationale", "limitations", "evidence"}
    approved_sources = {
        "market for used Li-ion battery",
        "market for used Ni-metal hydride battery",
        "treatment of used Ni-metal hydride battery, pyrometallurgical treatment",
    }
    auto_references = {
        rule["review_ref"]
        for rule in forward["replace"]
        if rule["source"]["name"] in approved_sources
        and rule["source"]["reference product"] in {"used Li-ion battery", "used Ni-metal hydride battery"}
    }
    assert len(auto_references) == 3
    geographic_references = {
        rule["review_ref"]
        for rule in forward["replace"]
        if rule["source"]["name"] == "treatment of waste polyethylene terephthalate, municipal incineration"
        and rule["source"]["reference product"] == "waste polyethylene terephthalate"
        and rule["source"]["location"] in {"RoW", "GLO"}
    }
    assert len(geographic_references) == 2
    from scripts.review_uvek_reference_signs import (
        APPROVED_CRT_GLASS_TREATMENTS,
        APPROVED_CRT_INCINERATION_TREATMENTS,
        APPROVED_CRT_LANDFILL_TREATMENTS,
        APPROVED_CRT_MANUAL_TREATMENTS,
        APPROVED_CRT_MECHANICAL_TREATMENTS,
        APPROVED_CRT_RESIDUE_MARKETS,
        APPROVED_DISCHARGE_TREATMENTS,
        APPROVED_GENERIC_WASTEWATER,
        APPROVED_INCINERATION_COMPOSITION_PROXIES,
        APPROVED_PROFILED_TREATMENTS,
        APPROVED_WASTEWATER_TREATMENTS,
        identity,
    )

    wastewater_references = {
        rule["review_ref"] for rule in forward["replace"] if identity(rule["source"]) in APPROVED_WASTEWATER_TREATMENTS
    }
    assert len(wastewater_references) == 18
    generic_references = {
        rule["review_ref"] for rule in forward["replace"] if identity(rule["source"]) in APPROVED_GENERIC_WASTEWATER
    }
    assert len(generic_references) == 10
    discharge_references = {
        rule["review_ref"] for rule in forward["replace"] if identity(rule["source"]) in APPROVED_DISCHARGE_TREATMENTS
    }
    assert len(discharge_references) == 4
    flare_references = {rule["review_ref"] for rule in forward["replace"] if identity(rule["source"]) in FLARES}
    assert len(flare_references) == 2
    natural_gas_flare_references = {
        rule["review_ref"] for rule in forward["replace"] if identity(rule["source"]) in NATURAL_GAS_FLARES
    }
    from scripts.review_uvek_remaining_gas_flares import REVIEW_PATH as SOURCE_GAS_REVIEW
    from scripts.review_uvek_remaining_gas_flares import SOURCES as SOURCE_GAS_FLARES

    source_gas_references = {r["review_ref"] for r in forward["replace"] if identity(r["source"]) in SOURCE_GAS_FLARES}
    assert natural_gas_flare_references == source_gas_references
    assert len(source_gas_references) == 1
    assert all(forward["reviews"][ref]["evidence"] == [SOURCE_GAS_REVIEW] for ref in source_gas_references)
    from scripts.review_uvek_cutoff_feed_products import PRODUCTS

    cutoff_feed_product_references = {
        rule["review_ref"] for rule in forward["replace"] if identity(rule["source"]) in PRODUCTS
    }
    assert len(cutoff_feed_product_references) == 16

    eaf_references = {rule["review_ref"] for rule in forward["replace"] if identity(rule["source"]) in LANDFILLS}
    assert len(eaf_references) == 3
    lcd_references = {rule["review_ref"] for rule in forward["replace"] if identity(rule["source"]) in LCD_TREATMENTS}
    assert len(lcd_references) == 5
    modal_references = {rule["review_ref"] for rule in forward["replace"] if identity(rule["source"]) in TRANSPORTS}
    assert len(modal_references) == 3
    dedicated_references = {rule["review_ref"] for rule in forward["replace"] if identity(rule["source"]) in DISPOSALS}
    assert len(dedicated_references) == 8
    omission_references = {rule["review_ref"] for rule in forward["delete"]}
    scoped_omission_references = {
        entry["review_ref"]
        for rule in forward["replace"]
        for entry in rule.get("reference_flow_sign_review", {}).get("profiles", [])
        if "review_ref" in entry
    }
    assert len(omission_references) == 257
    profile_references = {
        rule["review_ref"] for rule in forward["replace"] if identity(rule["source"]) in APPROVED_PROFILED_TREATMENTS
    }
    assert len(profile_references) == 3
    composition_references = {
        rule["review_ref"]
        for rule in forward["replace"]
        if identity(rule["source"]) in APPROVED_INCINERATION_COMPOSITION_PROXIES
    }
    assert len(composition_references) == 2
    crt_references = {
        rule["review_ref"]
        for rule in forward["replace"]
        if identity(rule["source"]) in APPROVED_CRT_INCINERATION_TREATMENTS
    }
    assert len(crt_references) == 12
    crt_glass_references = {
        rule["review_ref"] for rule in forward["replace"] if identity(rule["source"]) in APPROVED_CRT_GLASS_TREATMENTS
    }
    assert len(crt_glass_references) == 2
    crt_manual_references = {
        rule["review_ref"] for rule in forward["replace"] if identity(rule["source"]) in APPROVED_CRT_MANUAL_TREATMENTS
    }
    assert len(crt_manual_references) == 2
    crt_mechanical_references = {
        rule["review_ref"]
        for rule in forward["replace"]
        if identity(rule["source"]) in APPROVED_CRT_MECHANICAL_TREATMENTS
    }
    crt_residue_market_references = {
        rule["review_ref"] for rule in forward["replace"] if identity(rule["source"]) in APPROVED_CRT_RESIDUE_MARKETS
    }
    assert len(crt_mechanical_references) == len(crt_residue_market_references) == 2
    crt_landfill_references = {
        rule["review_ref"]
        for rule in forward["replace"]
        if identity(rule["source"]) in APPROVED_CRT_LANDFILL_TREATMENTS
    }
    assert len(crt_landfill_references) == 10
    assert all(
        forward["reviews"][reference].keys() == {"rationale", "limitations", "evidence"}
        for reference in auto_references
        | geographic_references
        | wastewater_references
        | generic_references
        | discharge_references
        | flare_references
        | natural_gas_flare_references
        | cutoff_feed_product_references
        | eaf_references
        | lcd_references
        | modal_references
        | dedicated_references
        | omission_references
        | scoped_omission_references
        | profile_references
        | composition_references
        | crt_references
        | crt_glass_references
        | crt_manual_references
        | crt_mechanical_references
        | crt_residue_market_references
        | crt_landfill_references
    )
    recipe_references = {r["review_ref"] for r in forward["replace"] if "recipe_ref" in r["target"]}
    assert all(forward["reviews"][ref].keys() == {"rationale", "limitations", "evidence"} for ref in recipe_references)
    from scripts.review_uvek_copper_cathode import SOURCE as COPPER_CATHODE_SOURCE
    from scripts.review_uvek_older_regional_road import SOURCES as OLDER_REGIONAL_ROAD_SOURCES
    from scripts.review_uvek_regional_freight import SOURCES as REGIONAL_FREIGHT_SOURCES
    from scripts.review_uvek_sewage_digestion import SOURCES as SEWAGE_DIGESTION_SOURCES
    from scripts.review_uvek_sewer_grids import SOURCES as SEWER_GRIDS
    from scripts.review_uvek_slag_landfill_components import SOURCES as SLAG_LANDFILL_COMPONENTS
    from scripts.review_uvek_stainless_stock import SOURCES as STAINLESS_STOCKS
    from scripts.review_uvek_whole_freight import SOURCES as WHOLE_FREIGHT_SOURCES

    freight_references = {r["review_ref"] for r in forward["replace"] if identity(r["source"]) in WHOLE_FREIGHT_SOURCES}
    assert len(freight_references) == 1
    assert all(forward["reviews"][ref].keys() == {"rationale", "limitations", "evidence"} for ref in freight_references)
    regional_freight_references = {
        r["review_ref"] for r in forward["replace"] if identity(r["source"]) in REGIONAL_FREIGHT_SOURCES
    }
    assert len(regional_freight_references) == 1
    assert all(
        forward["reviews"][ref].keys() == {"rationale", "limitations", "evidence"}
        for ref in regional_freight_references
    )
    older_road_references = {
        r["review_ref"] for r in forward["replace"] if identity(r["source"]) in OLDER_REGIONAL_ROAD_SOURCES
    }
    assert len(older_road_references) == 1
    assert all(
        forward["reviews"][ref].keys() == {"rationale", "limitations", "evidence"} for ref in older_road_references
    )
    from scripts.review_uvek_concrete_grade import SOURCE as CONCRETE_GRADE_SOURCE

    concrete_references = {
        r["review_ref"] for r in forward["replace"] if identity(r["source"]) == CONCRETE_GRADE_SOURCE
    }
    assert len(concrete_references) == 1
    assert all(
        forward["reviews"][ref].keys() == {"rationale", "limitations", "evidence"} for ref in concrete_references
    )
    copper_references = {r["review_ref"] for r in forward["replace"] if identity(r["source"]) == COPPER_CATHODE_SOURCE}
    assert len(copper_references) == 1
    assert all(forward["reviews"][ref].keys() == {"rationale", "limitations", "evidence"} for ref in copper_references)
    slag_references = {r["review_ref"] for r in forward["replace"] if identity(r["source"]) in SLAG_LANDFILL_COMPONENTS}
    assert len(slag_references) == 1
    assert all(forward["reviews"][ref].keys() == {"rationale", "limitations", "evidence"} for ref in slag_references)
    sewer_references = {r["review_ref"] for r in forward["replace"] if identity(r["source"]) in SEWER_GRIDS}
    assert len(sewer_references) == 1
    assert all(forward["reviews"][ref].keys() == {"rationale", "limitations", "evidence"} for ref in sewer_references)
    stock_references = {r["review_ref"] for r in forward["replace"] if identity(r["source"]) in STAINLESS_STOCKS}
    assert len(stock_references) == 1
    assert all(forward["reviews"][ref].keys() == {"rationale", "limitations", "evidence"} for ref in stock_references)
    digestion_references = {
        r["review_ref"] for r in forward["replace"] if identity(r["source"]) in SEWAGE_DIGESTION_SOURCES
    }
    assert len(digestion_references) == 2
    assert all(
        forward["reviews"][ref].keys() == {"rationale", "limitations", "evidence"} for ref in digestion_references
    )
    pilot = json.loads((DATA_DIR / "export/uvek_pilot_material_review.json").read_text())
    pilot_ids = {row["rule_id"] for row in pilot["rules"]}
    pilot_references = {r["review_ref"] for r in forward["replace"] if r["id"] in pilot_ids}
    assert len(pilot_ids) == 6 and len(pilot_references) == 6
    assert all(forward["reviews"][ref].keys() == {"rationale", "limitations", "evidence"} for ref in pilot_references)
    incinerators = json.loads((DATA_DIR / "export/uvek_pilot_incinerator_endpoint_review.json").read_text())
    incinerator_ids = {row["rule_id"] for row in incinerators["records"]}
    incinerator_references = {r["review_ref"] for r in forward["replace"] if r["id"] in incinerator_ids}
    assert len(incinerator_ids) == 4 and len(incinerator_references) == 1
    assert all(
        forward["reviews"][ref].keys() == {"rationale", "limitations", "evidence"} for ref in incinerator_references
    )
    paint_market = json.loads((DATA_DIR / "export/uvek_pilot_paint_market_review.json").read_text())
    paint_market_ids = {row["rule_id"] for row in paint_market["rules"]}
    paint_market_references = {r["review_ref"] for r in forward["replace"] if r["id"] in paint_market_ids}
    assert len(paint_market_ids) == len(paint_market_references) == 1
    assert all(
        forward["reviews"][ref].keys() == {"rationale", "limitations", "evidence"} for ref in paint_market_references
    )
    silver_market = json.loads((DATA_DIR / "export/uvek_pilot_silver_market_review.json").read_text())
    silver_market_ids = {row["rule_id"] for row in silver_market["rules"]}
    silver_market_references = {r["review_ref"] for r in forward["replace"] if r["id"] in silver_market_ids}
    assert len(silver_market_ids) == len(silver_market_references) == 1
    assert all(
        forward["reviews"][ref].keys() == {"rationale", "limitations", "evidence"} for ref in silver_market_references
    )
    bare_landfills = json.loads((DATA_DIR / "export/uvek_pilot_bare_landfill_review.json").read_text())
    corrected_landfill_ids = {row["rule_id"] for row in bare_landfills["records"] if row["correct_endpoint"]}
    landfill_references = {r["review_ref"] for r in forward["replace"] if r["id"] in corrected_landfill_ids}
    assert corrected_landfill_ids == {"bp-b39070e2c2728597"} and len(landfill_references) == 1
    assert all(
        forward["reviews"][ref].keys() == {"rationale", "limitations", "evidence"} for ref in landfill_references
    )
    generator = json.loads((DATA_DIR / "export/uvek_pilot_generator_fuel_review.json").read_text())
    generator_ids = {row["rule_id"] for row in generator["rules"]}
    generator_references = {r["review_ref"] for r in forward["replace"] if r["id"] in generator_ids}
    assert generator_ids == {"bp-07968f74bde91ad7"} and len(generator_references) == 1
    assert all(
        forward["reviews"][ref].keys() == {"rationale", "limitations", "evidence"} for ref in generator_references
    )
    grid_converter = json.loads((DATA_DIR / "export/uvek_pilot_grid_converter_review.json").read_text())
    grid_converter_ids = {row["rule_id"] for row in grid_converter["rules"]}
    grid_converter_references = {r["review_ref"] for r in forward["replace"] if r["id"] in grid_converter_ids}
    assert grid_converter_ids == {"bp-4389bb6ee9ab35e9", "bp-ac05197a0852076a"}
    assert len(grid_converter_references) == 2
    assert all(
        forward["reviews"][ref].keys() == {"rationale", "limitations", "evidence"} for ref in grid_converter_references
    )
    copper_supply = json.loads((DATA_DIR / "export/uvek_pilot_metal_chemical_supply_review.json").read_text())
    copper_supply_ids = {row["rule_id"] for row in copper_supply["rules"]}
    copper_supply_references = {r["review_ref"] for r in forward["replace"] if r["id"] in copper_supply_ids}
    assert copper_supply_ids == {"bp-1cb5578cf49cfd48"} and len(copper_supply_references) == 1
    assert all(
        forward["reviews"][ref].keys() == {"rationale", "limitations", "evidence"} for ref in copper_supply_references
    )
    wastewater_endpoint = json.loads((DATA_DIR / "export/uvek_pilot_wastewater_endpoint_review.json").read_text())
    wastewater_endpoint_ids = {row["rule_id"] for row in wastewater_endpoint["records"]}
    wastewater_endpoint_references = {r["review_ref"] for r in forward["replace"] if r["id"] in wastewater_endpoint_ids}
    assert wastewater_endpoint_ids == {"bp-fb5a0385ef4ba237"} and len(wastewater_endpoint_references) == 1
    assert all(
        forward["reviews"][ref].keys() == {"rationale", "limitations", "evidence"}
        for ref in wastewater_endpoint_references
    )
    wood_steel_freight = json.loads((DATA_DIR / "export/uvek_pilot_wood_steel_freight_review.json").read_text())
    wood_steel_freight_ids = {row["rule_id"] for row in wood_steel_freight["rules"]}
    wood_steel_freight_references = {r["review_ref"] for r in forward["replace"] if r["id"] in wood_steel_freight_ids}
    assert wood_steel_freight_ids == {"bp-84bd9cf473cdeee9", "bp-4529f104ab14ba19", "bp-07d1ab8592869587"}
    assert len(wood_steel_freight_references) == 3
    assert all(
        forward["reviews"][ref].keys() == {"rationale", "limitations", "evidence"}
        for ref in wood_steel_freight_references
    )
    secondary_aluminium = json.loads((DATA_DIR / "export/uvek_pilot_secondary_aluminium_pcb_review.json").read_text())
    secondary_aluminium_ids = {row["rule_id"] for row in secondary_aluminium["rules"]}
    secondary_aluminium_references = {r["review_ref"] for r in forward["replace"] if r["id"] in secondary_aluminium_ids}
    assert secondary_aluminium_ids == {"bp-84737a1a1790ee76"} and len(secondary_aluminium_references) == 1
    assert all(
        forward["reviews"][ref].keys() == {"rationale", "limitations", "evidence"}
        for ref in secondary_aluminium_references
    )
    sanitary_inert = json.loads((DATA_DIR / "export/uvek_pilot_sanitary_inert_review.json").read_text())
    sanitary_ids = {row["rule_id"] for row in sanitary_inert["rules"]}
    sanitary_references = {r["review_ref"] for r in forward["replace"] if r["id"] in sanitary_ids}
    assert len(sanitary_ids) == 2 and len(sanitary_references) == 1
    assert all(
        forward["reviews"][ref].keys() == {"rationale", "limitations", "evidence"} for ref in sanitary_references
    )
    pe = json.loads((DATA_DIR / "export/uvek_pilot_pe_collection_review.json").read_text())
    pe_ids = {row["rule_id"] for row in pe["records"]}
    pe_references = {r["review_ref"] for r in forward["replace"] if r["id"] in pe_ids}
    assert len(pe_ids) == len(pe_references) == 1
    assert all(forward["reviews"][ref].keys() == {"rationale", "limitations", "evidence"} for ref in pe_references)
    goods = json.loads((DATA_DIR / "export/uvek_pilot_nitrate_goods_review.json").read_text())
    goods_ids = {row["rule_id"] for row in goods["records"]}
    goods_references = {r["review_ref"] for r in forward["replace"] if r["id"] in goods_ids}
    assert len(goods_ids) == 9 and len(goods_references) == 8
    assert all(forward["reviews"][ref].keys() == {"rationale", "limitations", "evidence"} for ref in goods_references)
    country_pe = json.loads((DATA_DIR / "export/uvek_pilot_nitrate_pe_country_review.json").read_text())
    country_pe_ids = {row["rule_id"] for row in country_pe["records"]}
    country_pe_references = {r["review_ref"] for r in forward["replace"] if r["id"] in country_pe_ids}
    assert len(country_pe_ids) == 6 and len(country_pe_references) == 1
    assert all(
        forward["reviews"][ref].keys() == {"rationale", "limitations", "evidence"} for ref in country_pe_references
    )
    row_pe = json.loads((DATA_DIR / "export/uvek_pilot_nitrate_pe_row_review.json").read_text())
    row_pe_ids = {row["rule_id"] for row in row_pe["records"]}
    row_pe_references = {r["review_ref"] for r in forward["replace"] if r["id"] in row_pe_ids}
    assert len(row_pe_ids) == len(row_pe_references) == 1
    assert all(forward["reviews"][ref].keys() == {"rationale", "limitations", "evidence"} for ref in row_pe_references)
    cleanfill = json.loads((DATA_DIR / "export/uvek_pilot_nitrate_cleanfill_review.json").read_text())
    cleanfill_ids = {row["rule_id"] for row in cleanfill["records"]}
    cleanfill_references = {r["review_ref"] for r in forward["replace"] if r["id"] in cleanfill_ids}
    assert len(cleanfill_ids) == 5 and len(cleanfill_references) == 2
    assert all(
        forward["reviews"][ref].keys() == {"rationale", "limitations", "evidence"} for ref in cleanfill_references
    )
    wool = json.loads((DATA_DIR / "export/uvek_pilot_nitrate_wool_review.json").read_text())
    wool_ids = {row["rule_id"] for row in wool["records"]}
    wool_references = {r["review_ref"] for r in forward["replace"] if r["id"] in wool_ids}
    assert len(wool_ids) == len(wool_references) == 2
    assert all(forward["reviews"][ref].keys() == {"rationale", "limitations", "evidence"} for ref in wool_references)
    wool_europe = json.loads((DATA_DIR / "export/uvek_pilot_nitrate_wool_europe_review.json").read_text())
    wool_europe_ids = {row["rule_id"] for row in wool_europe["records"]}
    wool_europe_references = {r["review_ref"] for r in forward["replace"] if r["id"] in wool_europe_ids}
    assert len(wool_europe_ids) == len(wool_europe_references) == 1
    assert all(
        forward["reviews"][ref].keys() == {"rationale", "limitations", "evidence"} for ref in wool_europe_references
    )
    inert39 = json.loads((DATA_DIR / "export/uvek_pilot_nitrate_inert39_review.json").read_text())
    inert39_ids = {row["rule_id"] for row in inert39["records"]}
    inert39_references = {r["review_ref"] for r in forward["replace"] if r["id"] in inert39_ids}
    assert len(inert39_ids) == len(inert39_references) == 1
    assert all(forward["reviews"][ref].keys() == {"rationale", "limitations", "evidence"} for ref in inert39_references)
    ps = json.loads((DATA_DIR / "export/uvek_pilot_nitrate_ps_review.json").read_text())
    ps_ids = {row["rule_id"] for row in ps["records"]}
    ps_references = {r["review_ref"] for r in forward["replace"] if r["id"] in ps_ids}
    assert len(ps_ids) == 7 and len(ps_references) == 1
    assert all(forward["reviews"][ref].keys() == {"rationale", "limitations", "evidence"} for ref in ps_references)
    scrap = json.loads((DATA_DIR / "export/uvek_pilot_nitrate_scrap_review.json").read_text())
    scrap_ids = {row["rule_id"] for row in scrap["records"]}
    scrap_references = {r["review_ref"] for r in forward["replace"] if r["id"] in scrap_ids}
    assert len(scrap_ids) == 2 and len(scrap_references) == 1
    assert all(forward["reviews"][ref].keys() == {"rationale", "limitations", "evidence"} for ref in scrap_references)
    hdpe = json.loads((DATA_DIR / "export/uvek_pilot_nitrate_hdpe_review.json").read_text())
    hdpe_ids = {row["rule_id"] for row in hdpe["records"]}
    hdpe_references = {r["review_ref"] for r in forward["replace"] if r["id"] in hdpe_ids}
    assert len(hdpe_ids) == len(hdpe_references) == 1
    assert all(forward["reviews"][ref].keys() == {"rationale", "limitations", "evidence"} for ref in hdpe_references)
    foam = json.loads((DATA_DIR / "export/uvek_pilot_nitrate_foam_review.json").read_text())
    foam_ids = {row["rule_id"] for row in foam["records"]}
    foam_references = {r["review_ref"] for r in forward["replace"] if r["id"] in foam_ids}
    assert len(foam_ids) == len(foam_references) == 1
    assert all(forward["reviews"][ref].keys() == {"rationale", "limitations", "evidence"} for ref in foam_references)
    sand = json.loads((DATA_DIR / "export/uvek_pilot_nitrate_sand_review.json").read_text())
    sand_ids = {row["rule_id"] for row in sand["records"]}
    sand_references = {r["review_ref"] for r in forward["replace"] if r["id"] in sand_ids}
    assert len(sand_ids) == 3 and len(sand_references) == 1
    assert all(forward["reviews"][ref].keys() == {"rationale", "limitations", "evidence"} for ref in sand_references)
    glass = json.loads((DATA_DIR / "export/uvek_pilot_nitrate_glass_review.json").read_text())
    glass_ids = {row["rule_id"] for row in glass["records"]}
    glass_references = {r["review_ref"] for r in forward["replace"] if r["id"] in glass_ids}
    assert len(glass_ids) == 7 and len(glass_references) == 1
    assert all(forward["reviews"][ref].keys() == {"rationale", "limitations", "evidence"} for ref in glass_references)
    water = json.loads((DATA_DIR / "export/uvek_pilot_nitrate_water_review.json").read_text())
    water_ids = {row["rule_id"] for row in water["records"]}
    water_references = {r["review_ref"] for r in forward["replace"] if r["id"] in water_ids and "review_ref" in r}
    assert len(water_ids) == 7 and len(water_references) == 3
    assert all(forward["reviews"][ref].keys() == {"rationale", "limitations", "evidence"} for ref in water_references)
    paint = json.loads((DATA_DIR / "export/uvek_pilot_nitrate_paint_review.json").read_text())
    paint_ids = {row["rule_id"] for row in paint["records"]}
    paint_references = {r["review_ref"] for r in forward["replace"] if r["id"] in paint_ids}
    assert len(paint_ids) == 2 and len(paint_references) == 1
    assert all(forward["reviews"][ref].keys() == {"rationale", "limitations", "evidence"} for ref in paint_references)
    concrete_delivery = json.loads((DATA_DIR / "export/uvek_pilot_nitrate_concrete_delivery_review.json").read_text())
    concrete_delivery_ids = {row["rule_id"] for row in concrete_delivery["records"]}
    concrete_delivery_references = {r["review_ref"] for r in forward["replace"] if r["id"] in concrete_delivery_ids}
    assert len(concrete_delivery_ids) == 9 and len(concrete_delivery_references) == 1
    assert all(
        forward["reviews"][ref].keys() == {"rationale", "limitations", "evidence"}
        for ref in concrete_delivery_references
    )
    fae_routes = json.loads((DATA_DIR / "export/uvek_pilot_fae_routes_review.json").read_text())
    fae_routes_ids = {row["rule_id"] for row in fae_routes["records"]}
    fae_routes_references = {r["review_ref"] for r in forward["replace"] if r["id"] in fae_routes_ids}
    assert len(fae_routes_ids) == 5 and len(fae_routes_references) == 1
    assert all(
        forward["reviews"][ref].keys() == {"rationale", "limitations", "evidence"} for ref in fae_routes_references
    )
    kcl = json.loads((DATA_DIR / "export/uvek_pilot_kcl_site_review.json").read_text())
    kcl_ids = {row["rule_id"] for row in kcl["records"]}
    kcl_references = {r["review_ref"] for r in forward["replace"] if r["id"] in kcl_ids}
    assert len(kcl_ids) == len(kcl_references) == 1
    assert all(forward["reviews"][ref].keys() == {"rationale", "limitations", "evidence"} for ref in kcl_references)
    purge = json.loads((DATA_DIR / "export/uvek_pilot_purge_handover_review.json").read_text())
    purge_ids = {purge["stock"]["rule_id"], *(row["rule_id"] for row in purge["records"])}
    purge_references = {r["review_ref"] for r in forward["replace"] if r["id"] in purge_ids}
    assert len(purge_ids) == len(purge_references) == 2
    assert all(forward["reviews"][ref].keys() == {"rationale", "limitations", "evidence"} for ref in purge_references)
    teg = json.loads((DATA_DIR / "export/uvek_pilot_teg_supply_review.json").read_text())
    teg_ids = {teg["stock"]["rule_id"], *(row["rule_id"] for row in teg["records"])}
    teg_references = {r["review_ref"] for r in forward["replace"] if r["id"] in teg_ids}
    assert len(teg_ids) == 2 and len(teg_references) == 2
    assert all(forward["reviews"][ref].keys() == {"rationale", "limitations", "evidence"} for ref in teg_references)
    # Later frozen reviews legitimately carry evidence too. Keep the storage
    # invariant independent of an ever-growing handwritten rule whitelist;
    # correspondence-specific tests verify the exact authorities and decisions.
    for review in forward["reviews"].values():
        assert review.keys() in (
            {"rationale", "limitations"},
            {"rationale", "limitations", "evidence"},
        )
        if "evidence" in review:
            assert isinstance(review["evidence"], list) and review["evidence"]
            assert all(isinstance(item, str) and item.strip() for item in review["evidence"])


@pytest.mark.parametrize("default_encoding", ["utf-8", "cp1252"])
def test_authoring_json_io_preserves_unicode_with_any_default_encoding(tmp_path, monkeypatch, default_encoding):
    from scripts.generate_uvek_export import load, write

    original_open = Path.open

    def legacy_open(path, mode="r", buffering=-1, encoding=None, errors=None, newline=None):
        if "b" not in mode and encoding is None:
            encoding = default_encoding
        return original_open(path, mode, buffering, encoding, errors, newline)

    monkeypatch.setattr(Path, "open", legacy_open)
    path = tmp_path / "unicode-evidence.json"
    evidence = {"rationale": "Zürich → Genève", "confidence": 0.5}
    write(path, evidence)
    assert json.loads(path.read_bytes().decode("utf-8")) == evidence
    assert load(path) == evidence


def test_all_generated_helpers_match_the_pre_upgrade_output_baseline():
    import hashlib

    from brightpath.background.uvek_export import apply_uvek_export, load_uvek_export_resource
    from brightpath.core.policies import MigrationPolicy, PolicyAction

    migration_resource = legacy_projection(load_uvek_export_resource())
    values = {}
    for rule in migration_resource["replace"]:
        if "recipe_ref" not in rule["target"]:
            continue
        data = [
            {
                "name": "Synthetic foreground",
                "reference product": "service",
                "location": "GLO",
                "unit": "unit",
                "exchanges": [
                    {
                        **rule["source"],
                        "type": "technosphere",
                        "amount": 2,
                        "uncertainty type": 3,
                        "loc": 2,
                        "scale": 0.1,
                    }
                ],
            }
        ]
        report, helpers = apply_uvek_export(data, MigrationPolicy(on_information_loss=PolicyAction.WARN))
        assert not report.has_errors
        values[rule["id"]] = {"data": data, "helpers": helpers}
    encoded = json.dumps(values, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    assert (
        hashlib.sha256(encoded.encode()).hexdigest()
        == "e913accb42429af3aca2bf2520a8d67df8dbec4679d85d3fef6f7f9a56463353"
    )
