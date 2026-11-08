"""Audit and replay evidence-backed reference-flow signs for both UVEK directions."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import pickle
import sqlite3
from collections import Counter, defaultdict
from copy import deepcopy
from pathlib import Path

from brightpath.migrations.schema import (
    dumps_migration_resource,
    intern_review,
    rule_review,
)

try:
    from review_uvek_additional_cutoff_waste_markets import EVIDENCE_SCOPE as ADDITIONAL_CUTOFF_WASTE_MARKET_SCOPE
    from review_uvek_additional_cutoff_waste_markets import SOURCES as ADDITIONAL_CUTOFF_WASTE_MARKET_SOURCES
    from review_uvek_additional_cutoff_waste_markets import (
        collect_extra_inventories as collect_additional_cutoff_waste_market_extras,
    )
    from review_uvek_additional_cutoff_waste_markets import forward_decision as additional_cutoff_waste_market_decision
    from review_uvek_additional_cutoff_waste_markets import (
        inventory_record as additional_cutoff_waste_market_inventory_record,
    )
    from review_uvek_additional_cutoff_waste_markets import (
        validate_bindings as validate_additional_cutoff_waste_market_bindings,
    )
    from review_uvek_additional_cutoff_waste_markets import (
        validate_inventories as validate_additional_cutoff_waste_market_inventories,
    )
    from review_uvek_antifouling import (
        APPROVED_ANTIFOULING_OMISSIONS,
        validate_antifouling_inventories,
    )
    from review_uvek_ash_markets import EVIDENCE_SCOPE as ASH_MARKET_SCOPE
    from review_uvek_ash_markets import METHOD as ASH_MARKET_REVERSE_METHOD
    from review_uvek_ash_markets import SOURCES as ASH_MARKET_SOURCES
    from review_uvek_ash_markets import collect_extra_inventories as collect_ash_market_extras
    from review_uvek_ash_markets import forward_decision as ash_market_decision
    from review_uvek_ash_markets import inventory_record as ash_market_inventory_record
    from review_uvek_ash_markets import validate_bindings as validate_ash_market_bindings
    from review_uvek_ash_markets import validate_inventories as validate_ash_market_inventories
    from review_uvek_average_sludge_landfarming import EVIDENCE_SCOPE as AVERAGE_SLUDGE_LANDFARMING_SCOPE
    from review_uvek_average_sludge_landfarming import SOURCES as AVERAGE_SLUDGE_LANDFARMING_SOURCES
    from review_uvek_average_sludge_landfarming import (
        collect_extra_inventories as collect_average_sludge_landfarming_extras,
    )
    from review_uvek_average_sludge_landfarming import forward_decision as average_sludge_landfarming_decision
    from review_uvek_average_sludge_landfarming import inventory_record as average_sludge_landfarming_inventory_record
    from review_uvek_average_sludge_landfarming import validate_bindings as validate_average_sludge_landfarming_bindings
    from review_uvek_average_sludge_landfarming import (
        validate_inventories as validate_average_sludge_landfarming_inventories,
    )
    from review_uvek_bottom_ash import EVIDENCE_SCOPE as BOTTOM_ASH_SCOPE
    from review_uvek_bottom_ash import SOURCES as BOTTOM_ASH_SOURCES
    from review_uvek_bottom_ash import collect_extra_inventories as collect_bottom_ash_extras
    from review_uvek_bottom_ash import forward_decision as bottom_ash_decision
    from review_uvek_bottom_ash import inventory_record as bottom_ash_inventory_record
    from review_uvek_bottom_ash import validate_bindings as validate_bottom_ash_bindings
    from review_uvek_bottom_ash import validate_inventories as validate_bottom_ash_inventories
    from review_uvek_cable_rail import EVIDENCE_SCOPE as CABLE_RAIL_SCOPE
    from review_uvek_cable_rail import SOURCES as CABLE_RAIL_SOURCES
    from review_uvek_cable_rail import collect_extra_inventories as collect_cable_rail_extras
    from review_uvek_cable_rail import extend_parent_patch_evidence as extend_cable_parent_patch_evidence
    from review_uvek_cable_rail import forward_decision as cable_rail_decision
    from review_uvek_cable_rail import inventory_record as cable_rail_inventory_record
    from review_uvek_cable_rail import validate_existing_freight_bindings as validate_cable_freight_bindings
    from review_uvek_cable_rail import validate_inventories as validate_cable_rail_inventories
    from review_uvek_chemical_heat import EVIDENCE_SCOPE as CHEMICAL_HEAT_SCOPE
    from review_uvek_chemical_heat import SOURCES as CHEMICAL_HEAT_SOURCES
    from review_uvek_chemical_heat import collect_extra_inventories as collect_chemical_heat_extras
    from review_uvek_chemical_heat import forward_decision as chemical_heat_decision
    from review_uvek_chemical_heat import inventory_record as chemical_heat_inventory_record
    from review_uvek_chemical_heat import validate_bindings as validate_chemical_heat_bindings
    from review_uvek_chemical_heat import validate_inventories as validate_chemical_heat_inventories
    from review_uvek_complete_patch_boundaries import CASES as COMPLETE_PATCH_BOUNDARY_CASES
    from review_uvek_complete_patch_boundaries import EVIDENCE_SCOPE as COMPLETE_PATCH_BOUNDARY_SCOPE
    from review_uvek_complete_patch_boundaries import PATCH_STOCKS as COMPLETE_PATCH_STOCKS
    from review_uvek_complete_patch_boundaries import (
        annotate_handoff_profiles as annotate_complete_patch_handoff_profiles,
    )
    from review_uvek_complete_patch_boundaries import (
        collect_extra_inventories as collect_complete_patch_boundary_extras,
    )
    from review_uvek_complete_patch_boundaries import (
        extend_handoff_observations as extend_complete_patch_handoff_observations,
    )
    from review_uvek_complete_patch_boundaries import inventory_record as complete_patch_boundary_inventory_record
    from review_uvek_complete_patch_boundaries import stock_patch_decision as complete_patch_stock_decision
    from review_uvek_complete_patch_boundaries import validate_bindings as validate_complete_patch_boundary_bindings
    from review_uvek_complete_patch_boundaries import (
        validate_inventories as validate_complete_patch_boundary_inventories,
    )
    from review_uvek_concrete_grade import SOURCE as CONCRETE_GRADE_SOURCE
    from review_uvek_concrete_grade import approved_rule as approved_concrete_grade_rule
    from review_uvek_concrete_grade import collect_inventories as collect_concrete_grade_inventories
    from review_uvek_concrete_grade import forward_decision as concrete_grade_decision
    from review_uvek_concrete_grade import validate_inventories as validate_concrete_grade_inventories
    from review_uvek_copper_ash import SOURCES as COPPER_ASH_SOURCES
    from review_uvek_copper_ash import collect_inventories as collect_copper_ash_inventories
    from review_uvek_copper_ash import forward_decision as copper_ash_decision
    from review_uvek_copper_ash import validate_bindings as validate_copper_ash_bindings
    from review_uvek_copper_ash import validate_inventories as validate_copper_ash_inventories
    from review_uvek_copper_cathode import EVIDENCE_SCOPE as COPPER_CATHODE_SCOPE
    from review_uvek_copper_cathode import SOURCE as COPPER_CATHODE_SOURCE
    from review_uvek_copper_cathode import approved_copper_rule as approved_copper_cathode_rule
    from review_uvek_copper_cathode import collect_extra_inventories as collect_copper_cathode_extras
    from review_uvek_copper_cathode import forward_decision as copper_cathode_decision
    from review_uvek_copper_cathode import inventory_record as copper_cathode_inventory_record
    from review_uvek_copper_cathode import validate_inventories as validate_copper_cathode_inventories
    from review_uvek_copper_stock import EVIDENCE_SCOPE as COPPER_STOCK_SCOPE
    from review_uvek_copper_stock import SOURCES as COPPER_STOCK_SOURCES
    from review_uvek_copper_stock import collect_extra_inventories as collect_copper_stock_extras
    from review_uvek_copper_stock import forward_decision as copper_stock_decision
    from review_uvek_copper_stock import inventory_record as copper_stock_inventory_record
    from review_uvek_copper_stock import validate_bindings as validate_copper_stock_bindings
    from review_uvek_copper_stock import validate_inventories as validate_copper_stock_inventories
    from review_uvek_cutoff_feed_products import EVIDENCE_SCOPE as CUTOFF_FEED_SCOPE
    from review_uvek_cutoff_feed_products import MARKETS as CUTOFF_FEED_MARKETS
    from review_uvek_cutoff_feed_products import PRODUCTS as CUTOFF_FEED_PRODUCTS
    from review_uvek_cutoff_feed_products import REVERSE_METHOD as CUTOFF_FEED_REVERSE_METHOD
    from review_uvek_cutoff_feed_products import REVERSE_PRODUCTS as CUTOFF_FEED_REVERSE_PRODUCTS
    from review_uvek_cutoff_feed_products import approved_product_rule as approved_cutoff_feed_product_rule
    from review_uvek_cutoff_feed_products import inventory_record as cutoff_feed_inventory_record
    from review_uvek_cutoff_feed_products import market_decision as cutoff_feed_market_decision
    from review_uvek_cutoff_feed_products import product_decision as cutoff_feed_product_decision
    from review_uvek_cutoff_feed_products import reverse_decision as cutoff_feed_reverse_decision
    from review_uvek_cutoff_feed_products import validate_inventories as validate_cutoff_feed_inventories
    from review_uvek_cutoff_feed_products import validate_market_bindings as validate_cutoff_feed_market_bindings
    from review_uvek_cutoff_slag_products import EVIDENCE_SCOPE as CUTOFF_SLAG_SCOPE
    from review_uvek_cutoff_slag_products import PRODUCTS as CUTOFF_SLAG_PRODUCTS
    from review_uvek_cutoff_slag_products import REVERSE_METHOD as CUTOFF_SLAG_REVERSE_METHOD
    from review_uvek_cutoff_slag_products import inventory_record as cutoff_slag_inventory_record
    from review_uvek_cutoff_slag_products import product_decision as cutoff_slag_product_decision
    from review_uvek_cutoff_slag_products import validate_bindings as validate_cutoff_slag_bindings
    from review_uvek_cutoff_slag_products import validate_inventories as validate_cutoff_slag_inventories
    from review_uvek_cutoff_stocks import STOCKS, stock_decision
    from review_uvek_dedicated_disposals import DISPOSALS, dedicated_disposal_profiles
    from review_uvek_dedicated_disposals import TARGETS as DEDICATED_DISPOSAL_TARGETS
    from review_uvek_digester_stock import SOURCES as DIGESTER_STOCK_SOURCES
    from review_uvek_digester_stock import collect_inventories as collect_digester_stock_inventories
    from review_uvek_digester_stock import forward_decision as digester_stock_decision
    from review_uvek_digester_stock import validate_bindings as validate_digester_stock_bindings
    from review_uvek_digester_stock import validate_inventories as validate_digester_stock_inventories
    from review_uvek_eaf_landfills import LANDFILLS, landfill_profiles
    from review_uvek_eaf_landfills import TARGETS as EAF_LANDFILL_TARGETS
    from review_uvek_esterquat_markets import EVIDENCE_SCOPE as ESTERQUAT_MARKET_SCOPE
    from review_uvek_esterquat_markets import validate_provider_inventories as validate_esterquat_providers
    from review_uvek_exact_patch_boundaries import CASES as EXACT_PATCH_BOUNDARY_CASES
    from review_uvek_exact_patch_boundaries import EVIDENCE_SCOPE as EXACT_PATCH_BOUNDARY_SCOPE
    from review_uvek_exact_patch_boundaries import PATCH_STOCKS as EXACT_PATCH_STOCKS
    from review_uvek_exact_patch_boundaries import SOURCES as EXACT_PATCH_BOUNDARY_SOURCES
    from review_uvek_exact_patch_boundaries import annotate_handoff_profiles as annotate_exact_patch_handoff_profiles
    from review_uvek_exact_patch_boundaries import collect_extra_inventories as collect_exact_patch_boundary_extras
    from review_uvek_exact_patch_boundaries import (
        extend_handoff_observations as extend_exact_patch_handoff_observations,
    )
    from review_uvek_exact_patch_boundaries import inventory_record as exact_patch_boundary_inventory_record
    from review_uvek_exact_patch_boundaries import stock_patch_decision as exact_patch_stock_decision
    from review_uvek_exact_patch_boundaries import validate_bindings as validate_exact_patch_boundary_bindings
    from review_uvek_exact_patch_boundaries import validate_inventories as validate_exact_patch_boundary_inventories
    from review_uvek_five_simple_waste_markets import EVIDENCE_SCOPE as FIVE_SIMPLE_WASTE_MARKET_SCOPE
    from review_uvek_five_simple_waste_markets import SOURCES as FIVE_SIMPLE_WASTE_MARKET_SOURCES
    from review_uvek_five_simple_waste_markets import (
        collect_extra_inventories as collect_five_simple_waste_market_extras,
    )
    from review_uvek_five_simple_waste_markets import forward_decision as five_simple_waste_market_decision
    from review_uvek_five_simple_waste_markets import inventory_record as five_simple_waste_market_inventory_record
    from review_uvek_five_simple_waste_markets import validate_bindings as validate_five_simple_waste_market_bindings
    from review_uvek_five_simple_waste_markets import (
        validate_inventories as validate_five_simple_waste_market_inventories,
    )
    from review_uvek_gas_markets import EVIDENCE_SCOPE as GAS_MARKET_EVIDENCE_SCOPE
    from review_uvek_gas_markets import (
        MARKETS,
        market_decision,
        validate_market_dependencies,
    )
    from review_uvek_ghana_uncontrolled import SOURCES as GHANA_UNCONTROLLED_SOURCES
    from review_uvek_ghana_uncontrolled import collect_inventories as collect_ghana_uncontrolled_inventories
    from review_uvek_ghana_uncontrolled import extend_observations as extend_ghana_uncontrolled_observations
    from review_uvek_ghana_uncontrolled import forward_decision as ghana_uncontrolled_decision
    from review_uvek_ghana_uncontrolled import validate_bindings as validate_ghana_uncontrolled_bindings
    from review_uvek_ghana_uncontrolled import validate_inventories as validate_ghana_uncontrolled_inventories
    from review_uvek_industrial_gas import (
        GAS_SOURCES,
        PRIMITIVES,
        gas_decision,
        validate_credit_correspondences,
    )
    from review_uvek_industrial_landfarming import EVIDENCE_SCOPE as INDUSTRIAL_LANDFARMING_SCOPE
    from review_uvek_industrial_landfarming import SOURCES as INDUSTRIAL_LANDFARMING_SOURCES
    from review_uvek_industrial_landfarming import collect_extra_inventories as collect_industrial_landfarming_extras
    from review_uvek_industrial_landfarming import forward_decision as industrial_landfarming_decision
    from review_uvek_industrial_landfarming import inventory_record as industrial_landfarming_inventory_record
    from review_uvek_industrial_landfarming import validate_bindings as validate_industrial_landfarming_bindings
    from review_uvek_industrial_landfarming import validate_inventories as validate_industrial_landfarming_inventories
    from review_uvek_industrial_waste_markets import EVIDENCE_SCOPE as INDUSTRIAL_WASTE_MARKET_SCOPE
    from review_uvek_industrial_waste_markets import SOURCES as INDUSTRIAL_WASTE_MARKET_SOURCES
    from review_uvek_industrial_waste_markets import collect_extra_inventories as collect_industrial_waste_market_extras
    from review_uvek_industrial_waste_markets import forward_decision as industrial_waste_market_decision
    from review_uvek_industrial_waste_markets import inventory_record as industrial_waste_market_inventory_record
    from review_uvek_industrial_waste_markets import validate_bindings as validate_industrial_waste_market_bindings
    from review_uvek_industrial_waste_markets import (
        validate_inventories as validate_industrial_waste_market_inventories,
    )
    from review_uvek_lcd_treatments import LCD_TREATMENTS, lcd_profiles
    from review_uvek_lcd_treatments import TARGETS as LCD_TARGETS
    from review_uvek_market_compositions import (
        COMPOSITIONS,
        composition_decision,
        validate_composition_bindings,
    )
    from review_uvek_metalliferous_stock import SOURCE as METALLIFEROUS_STOCK
    from review_uvek_metalliferous_stock import collect_inventories as collect_metalliferous_stock_inventories
    from review_uvek_metalliferous_stock import collect_parent_inventories as collect_metalliferous_market_inventories
    from review_uvek_metalliferous_stock import extend_observations as extend_metalliferous_stock_observations
    from review_uvek_metalliferous_stock import forward_decision as metalliferous_stock_decision
    from review_uvek_metalliferous_stock import validate_inventories as validate_metalliferous_stock_inventories
    from review_uvek_metalliferous_stock import validate_parent_inventories as validate_metalliferous_market_inventories
    from review_uvek_modal_transport import TARGETS as MODAL_TRANSPORT_TARGETS
    from review_uvek_modal_transport import (
        TRANSPORTS,
        approved_transport_rule,
        transport_decision,
    )
    from review_uvek_natural_gas_flares import EVIDENCE_SCOPE as NATURAL_GAS_FLARE_SCOPE
    from review_uvek_natural_gas_flares import FLARES as NATURAL_GAS_FLARES
    from review_uvek_natural_gas_flares import REVERSE_METHOD as NATURAL_GAS_FLARE_REVERSE_METHOD
    from review_uvek_natural_gas_flares import TARGET as NATURAL_GAS_FLARE_TARGET
    from review_uvek_natural_gas_flares import VOLUME_PER_MJ as NATURAL_GAS_VOLUME_PER_MJ
    from review_uvek_natural_gas_flares import forward_profiles as natural_gas_flare_profiles
    from review_uvek_natural_gas_flares import inventory_record as natural_gas_flare_inventory_record
    from review_uvek_natural_gas_flares import reverse_decision as natural_gas_flare_reverse_decision
    from review_uvek_older_pet_whole_freight import SOURCES as OLDER_PET_WHOLE_FREIGHT_SOURCES
    from review_uvek_older_pet_whole_freight import collect_inventories as collect_older_pet_whole_freight_inventories
    from review_uvek_older_pet_whole_freight import forward_decision as older_pet_whole_freight_decision
    from review_uvek_older_pet_whole_freight import validate_bindings as validate_older_pet_whole_freight_bindings
    from review_uvek_older_pet_whole_freight import validate_inventories as validate_older_pet_whole_freight_inventories
    from review_uvek_older_regional_road import EVIDENCE_SCOPE as OLDER_REGIONAL_ROAD_SCOPE
    from review_uvek_older_regional_road import SOURCES as OLDER_REGIONAL_ROAD_SOURCES
    from review_uvek_older_regional_road import approved_freight_rule as approved_older_regional_road_rule
    from review_uvek_older_regional_road import collect_extra_inventories as collect_older_regional_road_extras
    from review_uvek_older_regional_road import forward_decision as older_regional_road_decision
    from review_uvek_older_regional_road import inventory_record as older_regional_road_inventory_record
    from review_uvek_older_regional_road import validate_bindings as validate_older_regional_road_bindings
    from review_uvek_older_regional_road import validate_inventories as validate_older_regional_road_inventories
    from review_uvek_packaging_paper_landfarming import EVIDENCE_SCOPE as PACKAGING_PAPER_LANDFARMING_SCOPE
    from review_uvek_packaging_paper_landfarming import SOURCES as PACKAGING_PAPER_LANDFARMING_SOURCES
    from review_uvek_packaging_paper_landfarming import (
        collect_extra_inventories as collect_packaging_paper_landfarming_extras,
    )
    from review_uvek_packaging_paper_landfarming import forward_decision as packaging_paper_landfarming_decision
    from review_uvek_packaging_paper_landfarming import inventory_record as packaging_paper_landfarming_inventory_record
    from review_uvek_packaging_paper_landfarming import (
        validate_bindings as validate_packaging_paper_landfarming_bindings,
    )
    from review_uvek_packaging_paper_landfarming import (
        validate_inventories as validate_packaging_paper_landfarming_inventories,
    )
    from review_uvek_packed_wool import EVIDENCE_SCOPE as PACKED_WOOL_SCOPE
    from review_uvek_packed_wool import REVERSE_METHOD as PACKED_WOOL_REVERSE_METHOD
    from review_uvek_packed_wool import SOURCE as PACKED_WOOL_SOURCE
    from review_uvek_packed_wool import collect_extra_inventories as collect_packed_wool_extras
    from review_uvek_packed_wool import forward_decision as packed_wool_decision
    from review_uvek_packed_wool import inventory_record as packed_wool_inventory_record
    from review_uvek_packed_wool import reverse_decision as packed_wool_reverse_decision
    from review_uvek_packed_wool import validate_bindings as validate_packed_wool_bindings
    from review_uvek_packed_wool import validate_inventories as validate_packed_wool_inventories
    from review_uvek_pam_frontier_stocks import EVIDENCE_SCOPE as PAM_FRONTIER_STOCK_SCOPE
    from review_uvek_pam_frontier_stocks import SOURCES as PAM_FRONTIER_STOCK_SOURCES
    from review_uvek_pam_frontier_stocks import collect_extra_inventories as collect_pam_frontier_stock_extras
    from review_uvek_pam_frontier_stocks import forward_decision as pam_frontier_stock_decision
    from review_uvek_pam_frontier_stocks import inventory_record as pam_frontier_stock_inventory_record
    from review_uvek_pam_frontier_stocks import validate_bindings as validate_pam_frontier_stock_bindings
    from review_uvek_pam_frontier_stocks import validate_inventories as validate_pam_frontier_stock_inventories
    from review_uvek_pet_market_groups import EVIDENCE_SCOPE as PET_MARKET_GROUP_SCOPE
    from review_uvek_pet_market_groups import SOURCES as PET_MARKET_GROUP_SOURCES
    from review_uvek_pet_market_groups import collect_extra_inventories as collect_pet_market_group_extras
    from review_uvek_pet_market_groups import forward_decision as pet_market_group_decision
    from review_uvek_pet_market_groups import inventory_record as pet_market_group_inventory_record
    from review_uvek_pet_market_groups import validate_bindings as validate_pet_market_group_bindings
    from review_uvek_pet_market_groups import validate_inventories as validate_pet_market_group_inventories
    from review_uvek_pet_waste_markets import EVIDENCE_SCOPE as PET_WASTE_MARKET_SCOPE
    from review_uvek_pet_waste_markets import SOURCES as PET_WASTE_MARKET_SOURCES
    from review_uvek_pet_waste_markets import collect_extra_inventories as collect_pet_waste_market_extras
    from review_uvek_pet_waste_markets import forward_decision as pet_waste_market_decision
    from review_uvek_pet_waste_markets import inventory_record as pet_waste_market_inventory_record
    from review_uvek_pet_waste_markets import validate_bindings as validate_pet_waste_market_bindings
    from review_uvek_pet_waste_markets import validate_inventories as validate_pet_waste_market_inventories
    from review_uvek_pp_uncontrolled_source import SOURCES as PP_UNCONTROLLED_SOURCES
    from review_uvek_pp_uncontrolled_source import collect_inventories as collect_pp_uncontrolled_inventories
    from review_uvek_pp_uncontrolled_source import extend_observations as extend_pp_uncontrolled_observations
    from review_uvek_pp_uncontrolled_source import forward_decision as pp_uncontrolled_decision
    from review_uvek_pp_uncontrolled_source import validate_inventories as validate_pp_uncontrolled_inventories
    from review_uvek_precious_anode_markets import SOURCES as PRECIOUS_ANODE_MARKETS
    from review_uvek_precious_anode_markets import collect_inventories as collect_precious_anode_inventories
    from review_uvek_precious_anode_markets import extend_observations as extend_precious_anode_observations
    from review_uvek_precious_anode_markets import forward_decision as precious_anode_decision
    from review_uvek_precious_anode_markets import validate_inventories as validate_precious_anode_inventories
    from review_uvek_processing_compositions import (
        PROCESSING,
        processing_decision,
        validate_processing_bindings,
    )
    from review_uvek_produced_water import (
        APPROVED_DISCHARGE_TREATMENTS,
        DISCHARGE_TARGETS,
        discharge_profiles,
    )
    from review_uvek_r134a_reclamation import (
        ELECTRICITY_TARGET,
        RECLAMATION_SOURCES,
        REFRIGERANT_TARGET,
        reclamation_decision,
    )
    from review_uvek_refinery_flare import FLARES, flare_profiles
    from review_uvek_refinery_flare import TARGET as FLARE_TARGET
    from review_uvek_refrigerant_disposal import (
        DISPOSAL_SOURCES,
        DISPOSAL_TARGET,
        disposal_decision,
    )
    from review_uvek_refrigerant_patch_formulas import SOURCES as REFRIGERANT_PATCH_SOURCES
    from review_uvek_refrigerant_patch_formulas import collect_inventories as collect_refrigerant_patch_inventories
    from review_uvek_refrigerant_patch_formulas import extend_observations as extend_refrigerant_patch_observations
    from review_uvek_refrigerant_patch_formulas import forward_decision as refrigerant_patch_decision
    from review_uvek_refrigerant_patch_formulas import validate_bindings as validate_refrigerant_patch_bindings
    from review_uvek_refrigerant_patch_formulas import validate_inventories as validate_refrigerant_patch_inventories
    from review_uvek_regional_freight import EVIDENCE_SCOPE as REGIONAL_FREIGHT_SCOPE
    from review_uvek_regional_freight import SOURCES as REGIONAL_FREIGHT_SOURCES
    from review_uvek_regional_freight import approved_freight_rule as approved_regional_freight_rule
    from review_uvek_regional_freight import collect_extra_inventories as collect_regional_freight_extras
    from review_uvek_regional_freight import forward_decision as regional_freight_decision
    from review_uvek_regional_freight import inventory_record as regional_freight_inventory_record
    from review_uvek_regional_freight import validate_bindings as validate_regional_freight_bindings
    from review_uvek_regional_freight import validate_inventories as validate_regional_freight_inventories
    from review_uvek_remaining_gas_flares import SOURCES as SOURCE_GAS_FLARES
    from review_uvek_remaining_gas_flares import collect_inventories as collect_source_gas_flare_inventories
    from review_uvek_remaining_gas_flares import extend_observations as extend_source_gas_flare_observations
    from review_uvek_remaining_gas_flares import forward_decision as source_gas_flare_decision
    from review_uvek_remaining_gas_flares import validate_inventories as validate_source_gas_flare_inventories
    from review_uvek_residual_landfills import EVIDENCE_SCOPE as RESIDUAL_LANDFILL_SCOPE
    from review_uvek_residual_landfills import SOURCES as RESIDUAL_LANDFILLS
    from review_uvek_residual_landfills import collect_extra_inventories as collect_residual_landfill_extras
    from review_uvek_residual_landfills import forward_decision as residual_landfill_decision
    from review_uvek_residual_landfills import inventory_record as residual_landfill_inventory_record
    from review_uvek_residual_landfills import validate_bindings as validate_residual_landfill_bindings
    from review_uvek_residual_landfills import validate_inventories as validate_residual_landfill_inventories
    from review_uvek_resolved_child_markets import EVIDENCE_SCOPE as RESOLVED_CHILD_MARKET_SCOPE
    from review_uvek_resolved_child_markets import SOURCES as RESOLVED_CHILD_MARKETS
    from review_uvek_resolved_child_markets import collect_extra_inventories as collect_resolved_child_market_extras
    from review_uvek_resolved_child_markets import forward_decision as resolved_child_market_decision
    from review_uvek_resolved_child_markets import inventory_record as resolved_child_market_inventory_record
    from review_uvek_resolved_child_markets import validate_bindings as validate_resolved_child_market_bindings
    from review_uvek_resolved_child_markets import validate_inventories as validate_resolved_child_market_inventories
    from review_uvek_reverse_ash import EVIDENCE_SCOPE as REVERSE_ASH_EVIDENCE_SCOPE
    from review_uvek_reverse_ash import METHOD as REVERSE_ASH_METHOD
    from review_uvek_reverse_ash import validate_reverse_ash_inventories
    from review_uvek_reverse_gas import EVIDENCE_SCOPE as REVERSE_GAS_EVIDENCE_SCOPE
    from review_uvek_reverse_gas import METHOD as REVERSE_GAS_METHOD
    from review_uvek_reverse_gas import SERVICES as REVERSE_GAS_SERVICES
    from review_uvek_reverse_gas import validate_reverse_gas_inventories
    from review_uvek_sewage_digestion import EVIDENCE_SCOPE as SEWAGE_DIGESTION_SCOPE
    from review_uvek_sewage_digestion import MARKETS as SEWAGE_SLUDGE_MARKETS
    from review_uvek_sewage_digestion import METHOD as SEWAGE_DIGESTION_REVERSE_METHOD
    from review_uvek_sewage_digestion import SOURCES as SEWAGE_DIGESTION_SOURCES
    from review_uvek_sewage_digestion import collect_extra_inventories as collect_sewage_digestion_extras
    from review_uvek_sewage_digestion import declare_reviewed_profiles as declare_sewage_digestion_profiles
    from review_uvek_sewage_digestion import forward_decision as sewage_digestion_decision
    from review_uvek_sewage_digestion import inventory_record as sewage_digestion_inventory_record
    from review_uvek_sewage_digestion import market_decision as sewage_sludge_market_decision
    from review_uvek_sewage_digestion import validate_bindings as validate_sewage_digestion_bindings
    from review_uvek_sewage_digestion import validate_inventories as validate_sewage_digestion_inventories
    from review_uvek_sewer_grids import EVIDENCE_SCOPE as SEWER_GRID_SCOPE
    from review_uvek_sewer_grids import SOURCES as SEWER_GRIDS
    from review_uvek_sewer_grids import approved_grid_rule as approved_sewer_grid_rule
    from review_uvek_sewer_grids import grid_decision as sewer_grid_decision
    from review_uvek_sewer_grids import inventory_record as sewer_grid_inventory_record
    from review_uvek_sewer_grids import validate_inventories as validate_sewer_grid_inventories
    from review_uvek_sewer_throughput import EVIDENCE_SCOPE as SEWER_THROUGHPUT_SCOPE
    from review_uvek_sewer_throughput import SOURCES as SEWER_THROUGHPUT_SOURCES
    from review_uvek_sewer_throughput import collect_extra_inventories as collect_sewer_throughput_extras
    from review_uvek_sewer_throughput import forward_decision as sewer_throughput_decision
    from review_uvek_sewer_throughput import inventory_record as sewer_throughput_inventory_record
    from review_uvek_sewer_throughput import validate_bindings as validate_sewer_throughput_bindings
    from review_uvek_sewer_throughput import validate_inventories as validate_sewer_throughput_inventories
    from review_uvek_slag_landfill_components import EVIDENCE_SCOPE as SLAG_LANDFILL_COMPONENT_SCOPE
    from review_uvek_slag_landfill_components import SOURCES as SLAG_LANDFILL_COMPONENTS
    from review_uvek_slag_landfill_components import approved_component_rule as approved_slag_landfill_component_rule
    from review_uvek_slag_landfill_components import collect_extra_inventories as collect_slag_landfill_component_extras
    from review_uvek_slag_landfill_components import component_decision as slag_landfill_component_decision
    from review_uvek_slag_landfill_components import inventory_record as slag_landfill_component_inventory_record
    from review_uvek_slag_landfill_components import (
        validate_inventories as validate_slag_landfill_component_inventories,
    )
    from review_uvek_slf_landfarming import EVIDENCE_SCOPE as SLF_LANDFARMING_SCOPE
    from review_uvek_slf_landfarming import SOURCES as SLF_LANDFARMING_SOURCES
    from review_uvek_slf_landfarming import collect_extra_inventories as collect_slf_landfarming_extras
    from review_uvek_slf_landfarming import forward_decision as slf_landfarming_decision
    from review_uvek_slf_landfarming import inventory_record as slf_landfarming_inventory_record
    from review_uvek_slf_landfarming import validate_bindings as validate_slf_landfarming_bindings
    from review_uvek_slf_landfarming import validate_inventories as validate_slf_landfarming_inventories
    from review_uvek_stainless_stock import EVIDENCE_SCOPE as STAINLESS_STOCK_SCOPE
    from review_uvek_stainless_stock import SOURCES as STAINLESS_STOCKS
    from review_uvek_stainless_stock import approved_stock_rule as approved_stainless_stock_rule
    from review_uvek_stainless_stock import inventory_record as stainless_stock_inventory_record
    from review_uvek_stainless_stock import stock_decision as stainless_stock_decision
    from review_uvek_stainless_stock import validate_inventories as validate_stainless_stock_inventories
    from review_uvek_steel_residue_markets import EVIDENCE_SCOPE as STEEL_RESIDUE_MARKET_SCOPE
    from review_uvek_steel_residue_markets import SOURCES as STEEL_RESIDUE_MARKET_SOURCES
    from review_uvek_steel_residue_markets import collect_extra_inventories as collect_steel_residue_market_extras
    from review_uvek_steel_residue_markets import forward_decision as steel_residue_market_decision
    from review_uvek_steel_residue_markets import inventory_record as steel_residue_market_inventory_record
    from review_uvek_steel_residue_markets import validate_bindings as validate_steel_residue_market_bindings
    from review_uvek_steel_residue_markets import validate_inventories as validate_steel_residue_market_inventories
    from review_uvek_swiss_cement_markets import SOURCES as SWISS_CEMENT_MARKETS
    from review_uvek_swiss_cement_markets import collect_inventories as collect_swiss_cement_inventories
    from review_uvek_swiss_cement_markets import forward_decision as swiss_cement_market_decision
    from review_uvek_swiss_cement_markets import validate_bindings as validate_swiss_cement_bindings
    from review_uvek_swiss_cement_markets import validate_inventories as validate_swiss_cement_inventories
    from review_uvek_swiss_pet_fluwa import EVIDENCE_SCOPE as SWISS_PET_FLUWA_SCOPE
    from review_uvek_swiss_pet_fluwa import SOURCES as SWISS_PET_FLUWA_SOURCES
    from review_uvek_swiss_pet_fluwa import collect_extra_inventories as collect_swiss_pet_fluwa_extras
    from review_uvek_swiss_pet_fluwa import forward_decision as swiss_pet_fluwa_decision
    from review_uvek_swiss_pet_fluwa import inventory_record as swiss_pet_fluwa_inventory_record
    from review_uvek_swiss_pet_fluwa import validate_bindings as validate_swiss_pet_fluwa_bindings
    from review_uvek_swiss_pet_fluwa import validate_inventories as validate_swiss_pet_fluwa_inventories
    from review_uvek_uncontrolled_disposal import EVIDENCE_SCOPE as UNCONTROLLED_DISPOSAL_SCOPE
    from review_uvek_uncontrolled_disposal import SOURCES as UNCONTROLLED_DISPOSAL_SOURCES
    from review_uvek_uncontrolled_disposal import collect_extra_inventories as collect_uncontrolled_disposal_extras
    from review_uvek_uncontrolled_disposal import (
        extend_companion_market_evidence as extend_uncontrolled_companion_market_evidence,
    )
    from review_uvek_uncontrolled_disposal import forward_decision as uncontrolled_disposal_decision
    from review_uvek_uncontrolled_disposal import inventory_record as uncontrolled_disposal_inventory_record
    from review_uvek_uncontrolled_disposal import validate_bindings as validate_uncontrolled_disposal_bindings
    from review_uvek_uncontrolled_disposal import validate_inventories as validate_uncontrolled_disposal_inventories
    from review_uvek_venting import (
        VENTING_SOURCES,
        compiled_venting_rule,
        venting_decision,
    )
    from review_uvek_waste_concrete_inert import SOURCES as WASTE_CONCRETE_INERT_SOURCES
    from review_uvek_waste_concrete_inert import collect_inventories as collect_waste_concrete_inert_inventories
    from review_uvek_waste_concrete_inert import forward_decision as waste_concrete_inert_decision
    from review_uvek_waste_concrete_inert import validate_bindings as validate_waste_concrete_inert_bindings
    from review_uvek_waste_concrete_inert import validate_inventories as validate_waste_concrete_inert_inventories
    from review_uvek_whole_freight import EVIDENCE_SCOPE as WHOLE_FREIGHT_SCOPE
    from review_uvek_whole_freight import REVERSE_METHOD as WHOLE_FREIGHT_REVERSE_METHOD
    from review_uvek_whole_freight import SOURCES as WHOLE_FREIGHT_SOURCES
    from review_uvek_whole_freight import approved_freight_rule as approved_whole_freight_rule
    from review_uvek_whole_freight import collect_extra_inventories as collect_whole_freight_extras
    from review_uvek_whole_freight import forward_decision as whole_freight_decision
    from review_uvek_whole_freight import inventory_record as whole_freight_inventory_record
    from review_uvek_whole_freight import reverse_decision as whole_freight_reverse_decision
    from review_uvek_whole_freight import validate_bindings as validate_whole_freight_bindings
    from review_uvek_whole_freight import validate_inventories as validate_whole_freight_inventories
    from review_uvek_wwt_residues import (
        APPROVED_WWT_RESIDUE_OMISSIONS,
        validate_wwt_residue_inventories,
    )
except ModuleNotFoundError:
    from scripts.review_uvek_additional_cutoff_waste_markets import (
        EVIDENCE_SCOPE as ADDITIONAL_CUTOFF_WASTE_MARKET_SCOPE,
    )
    from scripts.review_uvek_additional_cutoff_waste_markets import SOURCES as ADDITIONAL_CUTOFF_WASTE_MARKET_SOURCES
    from scripts.review_uvek_additional_cutoff_waste_markets import (
        collect_extra_inventories as collect_additional_cutoff_waste_market_extras,
    )
    from scripts.review_uvek_additional_cutoff_waste_markets import (
        forward_decision as additional_cutoff_waste_market_decision,
    )
    from scripts.review_uvek_additional_cutoff_waste_markets import (
        inventory_record as additional_cutoff_waste_market_inventory_record,
    )
    from scripts.review_uvek_additional_cutoff_waste_markets import (
        validate_bindings as validate_additional_cutoff_waste_market_bindings,
    )
    from scripts.review_uvek_additional_cutoff_waste_markets import (
        validate_inventories as validate_additional_cutoff_waste_market_inventories,
    )
    from scripts.review_uvek_antifouling import (
        APPROVED_ANTIFOULING_OMISSIONS,
        validate_antifouling_inventories,
    )
    from scripts.review_uvek_ash_markets import EVIDENCE_SCOPE as ASH_MARKET_SCOPE
    from scripts.review_uvek_ash_markets import METHOD as ASH_MARKET_REVERSE_METHOD
    from scripts.review_uvek_ash_markets import SOURCES as ASH_MARKET_SOURCES
    from scripts.review_uvek_ash_markets import collect_extra_inventories as collect_ash_market_extras
    from scripts.review_uvek_ash_markets import forward_decision as ash_market_decision
    from scripts.review_uvek_ash_markets import inventory_record as ash_market_inventory_record
    from scripts.review_uvek_ash_markets import validate_bindings as validate_ash_market_bindings
    from scripts.review_uvek_ash_markets import validate_inventories as validate_ash_market_inventories
    from scripts.review_uvek_average_sludge_landfarming import EVIDENCE_SCOPE as AVERAGE_SLUDGE_LANDFARMING_SCOPE
    from scripts.review_uvek_average_sludge_landfarming import SOURCES as AVERAGE_SLUDGE_LANDFARMING_SOURCES
    from scripts.review_uvek_average_sludge_landfarming import (
        collect_extra_inventories as collect_average_sludge_landfarming_extras,
    )
    from scripts.review_uvek_average_sludge_landfarming import forward_decision as average_sludge_landfarming_decision
    from scripts.review_uvek_average_sludge_landfarming import (
        inventory_record as average_sludge_landfarming_inventory_record,
    )
    from scripts.review_uvek_average_sludge_landfarming import (
        validate_bindings as validate_average_sludge_landfarming_bindings,
    )
    from scripts.review_uvek_average_sludge_landfarming import (
        validate_inventories as validate_average_sludge_landfarming_inventories,
    )
    from scripts.review_uvek_bottom_ash import EVIDENCE_SCOPE as BOTTOM_ASH_SCOPE
    from scripts.review_uvek_bottom_ash import SOURCES as BOTTOM_ASH_SOURCES
    from scripts.review_uvek_bottom_ash import collect_extra_inventories as collect_bottom_ash_extras
    from scripts.review_uvek_bottom_ash import forward_decision as bottom_ash_decision
    from scripts.review_uvek_bottom_ash import inventory_record as bottom_ash_inventory_record
    from scripts.review_uvek_bottom_ash import validate_bindings as validate_bottom_ash_bindings
    from scripts.review_uvek_bottom_ash import validate_inventories as validate_bottom_ash_inventories
    from scripts.review_uvek_cable_rail import EVIDENCE_SCOPE as CABLE_RAIL_SCOPE
    from scripts.review_uvek_cable_rail import SOURCES as CABLE_RAIL_SOURCES
    from scripts.review_uvek_cable_rail import collect_extra_inventories as collect_cable_rail_extras
    from scripts.review_uvek_cable_rail import extend_parent_patch_evidence as extend_cable_parent_patch_evidence
    from scripts.review_uvek_cable_rail import forward_decision as cable_rail_decision
    from scripts.review_uvek_cable_rail import inventory_record as cable_rail_inventory_record
    from scripts.review_uvek_cable_rail import validate_existing_freight_bindings as validate_cable_freight_bindings
    from scripts.review_uvek_cable_rail import validate_inventories as validate_cable_rail_inventories
    from scripts.review_uvek_chemical_heat import EVIDENCE_SCOPE as CHEMICAL_HEAT_SCOPE
    from scripts.review_uvek_chemical_heat import SOURCES as CHEMICAL_HEAT_SOURCES
    from scripts.review_uvek_chemical_heat import collect_extra_inventories as collect_chemical_heat_extras
    from scripts.review_uvek_chemical_heat import forward_decision as chemical_heat_decision
    from scripts.review_uvek_chemical_heat import inventory_record as chemical_heat_inventory_record
    from scripts.review_uvek_chemical_heat import validate_bindings as validate_chemical_heat_bindings
    from scripts.review_uvek_chemical_heat import validate_inventories as validate_chemical_heat_inventories
    from scripts.review_uvek_complete_patch_boundaries import CASES as COMPLETE_PATCH_BOUNDARY_CASES
    from scripts.review_uvek_complete_patch_boundaries import EVIDENCE_SCOPE as COMPLETE_PATCH_BOUNDARY_SCOPE
    from scripts.review_uvek_complete_patch_boundaries import PATCH_STOCKS as COMPLETE_PATCH_STOCKS
    from scripts.review_uvek_complete_patch_boundaries import (
        annotate_handoff_profiles as annotate_complete_patch_handoff_profiles,
    )
    from scripts.review_uvek_complete_patch_boundaries import (
        collect_extra_inventories as collect_complete_patch_boundary_extras,
    )
    from scripts.review_uvek_complete_patch_boundaries import (
        extend_handoff_observations as extend_complete_patch_handoff_observations,
    )
    from scripts.review_uvek_complete_patch_boundaries import (
        inventory_record as complete_patch_boundary_inventory_record,
    )
    from scripts.review_uvek_complete_patch_boundaries import stock_patch_decision as complete_patch_stock_decision
    from scripts.review_uvek_complete_patch_boundaries import (
        validate_bindings as validate_complete_patch_boundary_bindings,
    )
    from scripts.review_uvek_complete_patch_boundaries import (
        validate_inventories as validate_complete_patch_boundary_inventories,
    )
    from scripts.review_uvek_concrete_grade import SOURCE as CONCRETE_GRADE_SOURCE
    from scripts.review_uvek_concrete_grade import approved_rule as approved_concrete_grade_rule
    from scripts.review_uvek_concrete_grade import collect_inventories as collect_concrete_grade_inventories
    from scripts.review_uvek_concrete_grade import forward_decision as concrete_grade_decision
    from scripts.review_uvek_concrete_grade import validate_inventories as validate_concrete_grade_inventories
    from scripts.review_uvek_copper_ash import SOURCES as COPPER_ASH_SOURCES
    from scripts.review_uvek_copper_ash import collect_inventories as collect_copper_ash_inventories
    from scripts.review_uvek_copper_ash import forward_decision as copper_ash_decision
    from scripts.review_uvek_copper_ash import validate_bindings as validate_copper_ash_bindings
    from scripts.review_uvek_copper_ash import validate_inventories as validate_copper_ash_inventories
    from scripts.review_uvek_copper_cathode import EVIDENCE_SCOPE as COPPER_CATHODE_SCOPE
    from scripts.review_uvek_copper_cathode import SOURCE as COPPER_CATHODE_SOURCE
    from scripts.review_uvek_copper_cathode import approved_copper_rule as approved_copper_cathode_rule
    from scripts.review_uvek_copper_cathode import collect_extra_inventories as collect_copper_cathode_extras
    from scripts.review_uvek_copper_cathode import forward_decision as copper_cathode_decision
    from scripts.review_uvek_copper_cathode import inventory_record as copper_cathode_inventory_record
    from scripts.review_uvek_copper_cathode import validate_inventories as validate_copper_cathode_inventories
    from scripts.review_uvek_copper_stock import EVIDENCE_SCOPE as COPPER_STOCK_SCOPE
    from scripts.review_uvek_copper_stock import SOURCES as COPPER_STOCK_SOURCES
    from scripts.review_uvek_copper_stock import collect_extra_inventories as collect_copper_stock_extras
    from scripts.review_uvek_copper_stock import forward_decision as copper_stock_decision
    from scripts.review_uvek_copper_stock import inventory_record as copper_stock_inventory_record
    from scripts.review_uvek_copper_stock import validate_bindings as validate_copper_stock_bindings
    from scripts.review_uvek_copper_stock import validate_inventories as validate_copper_stock_inventories
    from scripts.review_uvek_cutoff_feed_products import EVIDENCE_SCOPE as CUTOFF_FEED_SCOPE
    from scripts.review_uvek_cutoff_feed_products import MARKETS as CUTOFF_FEED_MARKETS
    from scripts.review_uvek_cutoff_feed_products import PRODUCTS as CUTOFF_FEED_PRODUCTS
    from scripts.review_uvek_cutoff_feed_products import REVERSE_METHOD as CUTOFF_FEED_REVERSE_METHOD
    from scripts.review_uvek_cutoff_feed_products import REVERSE_PRODUCTS as CUTOFF_FEED_REVERSE_PRODUCTS
    from scripts.review_uvek_cutoff_feed_products import approved_product_rule as approved_cutoff_feed_product_rule
    from scripts.review_uvek_cutoff_feed_products import inventory_record as cutoff_feed_inventory_record
    from scripts.review_uvek_cutoff_feed_products import market_decision as cutoff_feed_market_decision
    from scripts.review_uvek_cutoff_feed_products import product_decision as cutoff_feed_product_decision
    from scripts.review_uvek_cutoff_feed_products import reverse_decision as cutoff_feed_reverse_decision
    from scripts.review_uvek_cutoff_feed_products import validate_inventories as validate_cutoff_feed_inventories
    from scripts.review_uvek_cutoff_feed_products import (
        validate_market_bindings as validate_cutoff_feed_market_bindings,
    )
    from scripts.review_uvek_cutoff_slag_products import EVIDENCE_SCOPE as CUTOFF_SLAG_SCOPE
    from scripts.review_uvek_cutoff_slag_products import PRODUCTS as CUTOFF_SLAG_PRODUCTS
    from scripts.review_uvek_cutoff_slag_products import REVERSE_METHOD as CUTOFF_SLAG_REVERSE_METHOD
    from scripts.review_uvek_cutoff_slag_products import inventory_record as cutoff_slag_inventory_record
    from scripts.review_uvek_cutoff_slag_products import product_decision as cutoff_slag_product_decision
    from scripts.review_uvek_cutoff_slag_products import validate_bindings as validate_cutoff_slag_bindings
    from scripts.review_uvek_cutoff_slag_products import validate_inventories as validate_cutoff_slag_inventories
    from scripts.review_uvek_cutoff_stocks import STOCKS, stock_decision
    from scripts.review_uvek_dedicated_disposals import (
        DISPOSALS,
        dedicated_disposal_profiles,
    )
    from scripts.review_uvek_dedicated_disposals import TARGETS as DEDICATED_DISPOSAL_TARGETS
    from scripts.review_uvek_digester_stock import SOURCES as DIGESTER_STOCK_SOURCES
    from scripts.review_uvek_digester_stock import collect_inventories as collect_digester_stock_inventories
    from scripts.review_uvek_digester_stock import forward_decision as digester_stock_decision
    from scripts.review_uvek_digester_stock import validate_bindings as validate_digester_stock_bindings
    from scripts.review_uvek_digester_stock import validate_inventories as validate_digester_stock_inventories
    from scripts.review_uvek_eaf_landfills import LANDFILLS, landfill_profiles
    from scripts.review_uvek_eaf_landfills import TARGETS as EAF_LANDFILL_TARGETS
    from scripts.review_uvek_esterquat_markets import EVIDENCE_SCOPE as ESTERQUAT_MARKET_SCOPE
    from scripts.review_uvek_esterquat_markets import validate_provider_inventories as validate_esterquat_providers
    from scripts.review_uvek_exact_patch_boundaries import CASES as EXACT_PATCH_BOUNDARY_CASES
    from scripts.review_uvek_exact_patch_boundaries import EVIDENCE_SCOPE as EXACT_PATCH_BOUNDARY_SCOPE
    from scripts.review_uvek_exact_patch_boundaries import PATCH_STOCKS as EXACT_PATCH_STOCKS
    from scripts.review_uvek_exact_patch_boundaries import SOURCES as EXACT_PATCH_BOUNDARY_SOURCES
    from scripts.review_uvek_exact_patch_boundaries import (
        annotate_handoff_profiles as annotate_exact_patch_handoff_profiles,
    )
    from scripts.review_uvek_exact_patch_boundaries import (
        collect_extra_inventories as collect_exact_patch_boundary_extras,
    )
    from scripts.review_uvek_exact_patch_boundaries import (
        extend_handoff_observations as extend_exact_patch_handoff_observations,
    )
    from scripts.review_uvek_exact_patch_boundaries import inventory_record as exact_patch_boundary_inventory_record
    from scripts.review_uvek_exact_patch_boundaries import stock_patch_decision as exact_patch_stock_decision
    from scripts.review_uvek_exact_patch_boundaries import validate_bindings as validate_exact_patch_boundary_bindings
    from scripts.review_uvek_exact_patch_boundaries import (
        validate_inventories as validate_exact_patch_boundary_inventories,
    )
    from scripts.review_uvek_five_simple_waste_markets import EVIDENCE_SCOPE as FIVE_SIMPLE_WASTE_MARKET_SCOPE
    from scripts.review_uvek_five_simple_waste_markets import SOURCES as FIVE_SIMPLE_WASTE_MARKET_SOURCES
    from scripts.review_uvek_five_simple_waste_markets import (
        collect_extra_inventories as collect_five_simple_waste_market_extras,
    )
    from scripts.review_uvek_five_simple_waste_markets import forward_decision as five_simple_waste_market_decision
    from scripts.review_uvek_five_simple_waste_markets import (
        inventory_record as five_simple_waste_market_inventory_record,
    )
    from scripts.review_uvek_five_simple_waste_markets import (
        validate_bindings as validate_five_simple_waste_market_bindings,
    )
    from scripts.review_uvek_five_simple_waste_markets import (
        validate_inventories as validate_five_simple_waste_market_inventories,
    )
    from scripts.review_uvek_gas_markets import EVIDENCE_SCOPE as GAS_MARKET_EVIDENCE_SCOPE
    from scripts.review_uvek_gas_markets import (
        MARKETS,
        market_decision,
        validate_market_dependencies,
    )
    from scripts.review_uvek_ghana_uncontrolled import SOURCES as GHANA_UNCONTROLLED_SOURCES
    from scripts.review_uvek_ghana_uncontrolled import collect_inventories as collect_ghana_uncontrolled_inventories
    from scripts.review_uvek_ghana_uncontrolled import extend_observations as extend_ghana_uncontrolled_observations
    from scripts.review_uvek_ghana_uncontrolled import forward_decision as ghana_uncontrolled_decision
    from scripts.review_uvek_ghana_uncontrolled import validate_bindings as validate_ghana_uncontrolled_bindings
    from scripts.review_uvek_ghana_uncontrolled import validate_inventories as validate_ghana_uncontrolled_inventories
    from scripts.review_uvek_industrial_gas import (
        GAS_SOURCES,
        PRIMITIVES,
        gas_decision,
        validate_credit_correspondences,
    )
    from scripts.review_uvek_industrial_landfarming import EVIDENCE_SCOPE as INDUSTRIAL_LANDFARMING_SCOPE
    from scripts.review_uvek_industrial_landfarming import SOURCES as INDUSTRIAL_LANDFARMING_SOURCES
    from scripts.review_uvek_industrial_landfarming import (
        collect_extra_inventories as collect_industrial_landfarming_extras,
    )
    from scripts.review_uvek_industrial_landfarming import forward_decision as industrial_landfarming_decision
    from scripts.review_uvek_industrial_landfarming import inventory_record as industrial_landfarming_inventory_record
    from scripts.review_uvek_industrial_landfarming import validate_bindings as validate_industrial_landfarming_bindings
    from scripts.review_uvek_industrial_landfarming import (
        validate_inventories as validate_industrial_landfarming_inventories,
    )
    from scripts.review_uvek_industrial_waste_markets import EVIDENCE_SCOPE as INDUSTRIAL_WASTE_MARKET_SCOPE
    from scripts.review_uvek_industrial_waste_markets import SOURCES as INDUSTRIAL_WASTE_MARKET_SOURCES
    from scripts.review_uvek_industrial_waste_markets import (
        collect_extra_inventories as collect_industrial_waste_market_extras,
    )
    from scripts.review_uvek_industrial_waste_markets import forward_decision as industrial_waste_market_decision
    from scripts.review_uvek_industrial_waste_markets import (
        inventory_record as industrial_waste_market_inventory_record,
    )
    from scripts.review_uvek_industrial_waste_markets import (
        validate_bindings as validate_industrial_waste_market_bindings,
    )
    from scripts.review_uvek_industrial_waste_markets import (
        validate_inventories as validate_industrial_waste_market_inventories,
    )
    from scripts.review_uvek_lcd_treatments import LCD_TREATMENTS, lcd_profiles
    from scripts.review_uvek_lcd_treatments import TARGETS as LCD_TARGETS
    from scripts.review_uvek_market_compositions import (
        COMPOSITIONS,
        composition_decision,
        validate_composition_bindings,
    )
    from scripts.review_uvek_metalliferous_stock import SOURCE as METALLIFEROUS_STOCK
    from scripts.review_uvek_metalliferous_stock import collect_inventories as collect_metalliferous_stock_inventories
    from scripts.review_uvek_metalliferous_stock import (
        collect_parent_inventories as collect_metalliferous_market_inventories,
    )
    from scripts.review_uvek_metalliferous_stock import extend_observations as extend_metalliferous_stock_observations
    from scripts.review_uvek_metalliferous_stock import forward_decision as metalliferous_stock_decision
    from scripts.review_uvek_metalliferous_stock import validate_inventories as validate_metalliferous_stock_inventories
    from scripts.review_uvek_metalliferous_stock import (
        validate_parent_inventories as validate_metalliferous_market_inventories,
    )
    from scripts.review_uvek_modal_transport import TARGETS as MODAL_TRANSPORT_TARGETS
    from scripts.review_uvek_modal_transport import (
        TRANSPORTS,
        approved_transport_rule,
        transport_decision,
    )
    from scripts.review_uvek_natural_gas_flares import EVIDENCE_SCOPE as NATURAL_GAS_FLARE_SCOPE
    from scripts.review_uvek_natural_gas_flares import FLARES as NATURAL_GAS_FLARES
    from scripts.review_uvek_natural_gas_flares import REVERSE_METHOD as NATURAL_GAS_FLARE_REVERSE_METHOD
    from scripts.review_uvek_natural_gas_flares import TARGET as NATURAL_GAS_FLARE_TARGET
    from scripts.review_uvek_natural_gas_flares import VOLUME_PER_MJ as NATURAL_GAS_VOLUME_PER_MJ
    from scripts.review_uvek_natural_gas_flares import forward_profiles as natural_gas_flare_profiles
    from scripts.review_uvek_natural_gas_flares import inventory_record as natural_gas_flare_inventory_record
    from scripts.review_uvek_natural_gas_flares import reverse_decision as natural_gas_flare_reverse_decision
    from scripts.review_uvek_older_pet_whole_freight import SOURCES as OLDER_PET_WHOLE_FREIGHT_SOURCES
    from scripts.review_uvek_older_pet_whole_freight import (
        collect_inventories as collect_older_pet_whole_freight_inventories,
    )
    from scripts.review_uvek_older_pet_whole_freight import forward_decision as older_pet_whole_freight_decision
    from scripts.review_uvek_older_pet_whole_freight import (
        validate_bindings as validate_older_pet_whole_freight_bindings,
    )
    from scripts.review_uvek_older_pet_whole_freight import (
        validate_inventories as validate_older_pet_whole_freight_inventories,
    )
    from scripts.review_uvek_older_regional_road import EVIDENCE_SCOPE as OLDER_REGIONAL_ROAD_SCOPE
    from scripts.review_uvek_older_regional_road import SOURCES as OLDER_REGIONAL_ROAD_SOURCES
    from scripts.review_uvek_older_regional_road import approved_freight_rule as approved_older_regional_road_rule
    from scripts.review_uvek_older_regional_road import collect_extra_inventories as collect_older_regional_road_extras
    from scripts.review_uvek_older_regional_road import forward_decision as older_regional_road_decision
    from scripts.review_uvek_older_regional_road import inventory_record as older_regional_road_inventory_record
    from scripts.review_uvek_older_regional_road import validate_bindings as validate_older_regional_road_bindings
    from scripts.review_uvek_older_regional_road import validate_inventories as validate_older_regional_road_inventories
    from scripts.review_uvek_packaging_paper_landfarming import EVIDENCE_SCOPE as PACKAGING_PAPER_LANDFARMING_SCOPE
    from scripts.review_uvek_packaging_paper_landfarming import SOURCES as PACKAGING_PAPER_LANDFARMING_SOURCES
    from scripts.review_uvek_packaging_paper_landfarming import (
        collect_extra_inventories as collect_packaging_paper_landfarming_extras,
    )
    from scripts.review_uvek_packaging_paper_landfarming import forward_decision as packaging_paper_landfarming_decision
    from scripts.review_uvek_packaging_paper_landfarming import (
        inventory_record as packaging_paper_landfarming_inventory_record,
    )
    from scripts.review_uvek_packaging_paper_landfarming import (
        validate_bindings as validate_packaging_paper_landfarming_bindings,
    )
    from scripts.review_uvek_packaging_paper_landfarming import (
        validate_inventories as validate_packaging_paper_landfarming_inventories,
    )
    from scripts.review_uvek_packed_wool import EVIDENCE_SCOPE as PACKED_WOOL_SCOPE
    from scripts.review_uvek_packed_wool import REVERSE_METHOD as PACKED_WOOL_REVERSE_METHOD
    from scripts.review_uvek_packed_wool import SOURCE as PACKED_WOOL_SOURCE
    from scripts.review_uvek_packed_wool import collect_extra_inventories as collect_packed_wool_extras
    from scripts.review_uvek_packed_wool import forward_decision as packed_wool_decision
    from scripts.review_uvek_packed_wool import inventory_record as packed_wool_inventory_record
    from scripts.review_uvek_packed_wool import reverse_decision as packed_wool_reverse_decision
    from scripts.review_uvek_packed_wool import validate_bindings as validate_packed_wool_bindings
    from scripts.review_uvek_packed_wool import validate_inventories as validate_packed_wool_inventories
    from scripts.review_uvek_pam_frontier_stocks import EVIDENCE_SCOPE as PAM_FRONTIER_STOCK_SCOPE
    from scripts.review_uvek_pam_frontier_stocks import SOURCES as PAM_FRONTIER_STOCK_SOURCES
    from scripts.review_uvek_pam_frontier_stocks import collect_extra_inventories as collect_pam_frontier_stock_extras
    from scripts.review_uvek_pam_frontier_stocks import forward_decision as pam_frontier_stock_decision
    from scripts.review_uvek_pam_frontier_stocks import inventory_record as pam_frontier_stock_inventory_record
    from scripts.review_uvek_pam_frontier_stocks import validate_bindings as validate_pam_frontier_stock_bindings
    from scripts.review_uvek_pam_frontier_stocks import validate_inventories as validate_pam_frontier_stock_inventories
    from scripts.review_uvek_pet_market_groups import EVIDENCE_SCOPE as PET_MARKET_GROUP_SCOPE
    from scripts.review_uvek_pet_market_groups import SOURCES as PET_MARKET_GROUP_SOURCES
    from scripts.review_uvek_pet_market_groups import collect_extra_inventories as collect_pet_market_group_extras
    from scripts.review_uvek_pet_market_groups import forward_decision as pet_market_group_decision
    from scripts.review_uvek_pet_market_groups import inventory_record as pet_market_group_inventory_record
    from scripts.review_uvek_pet_market_groups import validate_bindings as validate_pet_market_group_bindings
    from scripts.review_uvek_pet_market_groups import validate_inventories as validate_pet_market_group_inventories
    from scripts.review_uvek_pet_waste_markets import EVIDENCE_SCOPE as PET_WASTE_MARKET_SCOPE
    from scripts.review_uvek_pet_waste_markets import SOURCES as PET_WASTE_MARKET_SOURCES
    from scripts.review_uvek_pet_waste_markets import collect_extra_inventories as collect_pet_waste_market_extras
    from scripts.review_uvek_pet_waste_markets import forward_decision as pet_waste_market_decision
    from scripts.review_uvek_pet_waste_markets import inventory_record as pet_waste_market_inventory_record
    from scripts.review_uvek_pet_waste_markets import validate_bindings as validate_pet_waste_market_bindings
    from scripts.review_uvek_pet_waste_markets import validate_inventories as validate_pet_waste_market_inventories
    from scripts.review_uvek_pp_uncontrolled_source import SOURCES as PP_UNCONTROLLED_SOURCES
    from scripts.review_uvek_pp_uncontrolled_source import collect_inventories as collect_pp_uncontrolled_inventories
    from scripts.review_uvek_pp_uncontrolled_source import extend_observations as extend_pp_uncontrolled_observations
    from scripts.review_uvek_pp_uncontrolled_source import forward_decision as pp_uncontrolled_decision
    from scripts.review_uvek_pp_uncontrolled_source import validate_inventories as validate_pp_uncontrolled_inventories
    from scripts.review_uvek_precious_anode_markets import SOURCES as PRECIOUS_ANODE_MARKETS
    from scripts.review_uvek_precious_anode_markets import collect_inventories as collect_precious_anode_inventories
    from scripts.review_uvek_precious_anode_markets import extend_observations as extend_precious_anode_observations
    from scripts.review_uvek_precious_anode_markets import forward_decision as precious_anode_decision
    from scripts.review_uvek_precious_anode_markets import validate_inventories as validate_precious_anode_inventories
    from scripts.review_uvek_processing_compositions import (
        PROCESSING,
        processing_decision,
        validate_processing_bindings,
    )
    from scripts.review_uvek_produced_water import (
        APPROVED_DISCHARGE_TREATMENTS,
        DISCHARGE_TARGETS,
        discharge_profiles,
    )
    from scripts.review_uvek_r134a_reclamation import (
        ELECTRICITY_TARGET,
        RECLAMATION_SOURCES,
        REFRIGERANT_TARGET,
        reclamation_decision,
    )
    from scripts.review_uvek_refinery_flare import FLARES, flare_profiles
    from scripts.review_uvek_refinery_flare import TARGET as FLARE_TARGET
    from scripts.review_uvek_refrigerant_disposal import (
        DISPOSAL_SOURCES,
        DISPOSAL_TARGET,
        disposal_decision,
    )
    from scripts.review_uvek_refrigerant_patch_formulas import SOURCES as REFRIGERANT_PATCH_SOURCES
    from scripts.review_uvek_refrigerant_patch_formulas import (
        collect_inventories as collect_refrigerant_patch_inventories,
    )
    from scripts.review_uvek_refrigerant_patch_formulas import (
        extend_observations as extend_refrigerant_patch_observations,
    )
    from scripts.review_uvek_refrigerant_patch_formulas import forward_decision as refrigerant_patch_decision
    from scripts.review_uvek_refrigerant_patch_formulas import validate_bindings as validate_refrigerant_patch_bindings
    from scripts.review_uvek_refrigerant_patch_formulas import (
        validate_inventories as validate_refrigerant_patch_inventories,
    )
    from scripts.review_uvek_regional_freight import EVIDENCE_SCOPE as REGIONAL_FREIGHT_SCOPE
    from scripts.review_uvek_regional_freight import SOURCES as REGIONAL_FREIGHT_SOURCES
    from scripts.review_uvek_regional_freight import approved_freight_rule as approved_regional_freight_rule
    from scripts.review_uvek_regional_freight import collect_extra_inventories as collect_regional_freight_extras
    from scripts.review_uvek_regional_freight import forward_decision as regional_freight_decision
    from scripts.review_uvek_regional_freight import inventory_record as regional_freight_inventory_record
    from scripts.review_uvek_regional_freight import validate_bindings as validate_regional_freight_bindings
    from scripts.review_uvek_regional_freight import validate_inventories as validate_regional_freight_inventories
    from scripts.review_uvek_remaining_gas_flares import SOURCES as SOURCE_GAS_FLARES
    from scripts.review_uvek_remaining_gas_flares import collect_inventories as collect_source_gas_flare_inventories
    from scripts.review_uvek_remaining_gas_flares import extend_observations as extend_source_gas_flare_observations
    from scripts.review_uvek_remaining_gas_flares import forward_decision as source_gas_flare_decision
    from scripts.review_uvek_remaining_gas_flares import validate_inventories as validate_source_gas_flare_inventories
    from scripts.review_uvek_residual_landfills import EVIDENCE_SCOPE as RESIDUAL_LANDFILL_SCOPE
    from scripts.review_uvek_residual_landfills import SOURCES as RESIDUAL_LANDFILLS
    from scripts.review_uvek_residual_landfills import collect_extra_inventories as collect_residual_landfill_extras
    from scripts.review_uvek_residual_landfills import forward_decision as residual_landfill_decision
    from scripts.review_uvek_residual_landfills import inventory_record as residual_landfill_inventory_record
    from scripts.review_uvek_residual_landfills import validate_bindings as validate_residual_landfill_bindings
    from scripts.review_uvek_residual_landfills import validate_inventories as validate_residual_landfill_inventories
    from scripts.review_uvek_resolved_child_markets import EVIDENCE_SCOPE as RESOLVED_CHILD_MARKET_SCOPE
    from scripts.review_uvek_resolved_child_markets import SOURCES as RESOLVED_CHILD_MARKETS
    from scripts.review_uvek_resolved_child_markets import (
        collect_extra_inventories as collect_resolved_child_market_extras,
    )
    from scripts.review_uvek_resolved_child_markets import forward_decision as resolved_child_market_decision
    from scripts.review_uvek_resolved_child_markets import inventory_record as resolved_child_market_inventory_record
    from scripts.review_uvek_resolved_child_markets import validate_bindings as validate_resolved_child_market_bindings
    from scripts.review_uvek_resolved_child_markets import (
        validate_inventories as validate_resolved_child_market_inventories,
    )
    from scripts.review_uvek_reverse_ash import EVIDENCE_SCOPE as REVERSE_ASH_EVIDENCE_SCOPE
    from scripts.review_uvek_reverse_ash import METHOD as REVERSE_ASH_METHOD
    from scripts.review_uvek_reverse_ash import (
        validate_reverse_ash_inventories,
    )
    from scripts.review_uvek_reverse_gas import EVIDENCE_SCOPE as REVERSE_GAS_EVIDENCE_SCOPE
    from scripts.review_uvek_reverse_gas import METHOD as REVERSE_GAS_METHOD
    from scripts.review_uvek_reverse_gas import SERVICES as REVERSE_GAS_SERVICES
    from scripts.review_uvek_reverse_gas import (
        validate_reverse_gas_inventories,
    )
    from scripts.review_uvek_sewage_digestion import EVIDENCE_SCOPE as SEWAGE_DIGESTION_SCOPE
    from scripts.review_uvek_sewage_digestion import MARKETS as SEWAGE_SLUDGE_MARKETS
    from scripts.review_uvek_sewage_digestion import METHOD as SEWAGE_DIGESTION_REVERSE_METHOD
    from scripts.review_uvek_sewage_digestion import SOURCES as SEWAGE_DIGESTION_SOURCES
    from scripts.review_uvek_sewage_digestion import collect_extra_inventories as collect_sewage_digestion_extras
    from scripts.review_uvek_sewage_digestion import declare_reviewed_profiles as declare_sewage_digestion_profiles
    from scripts.review_uvek_sewage_digestion import forward_decision as sewage_digestion_decision
    from scripts.review_uvek_sewage_digestion import inventory_record as sewage_digestion_inventory_record
    from scripts.review_uvek_sewage_digestion import market_decision as sewage_sludge_market_decision
    from scripts.review_uvek_sewage_digestion import validate_bindings as validate_sewage_digestion_bindings
    from scripts.review_uvek_sewage_digestion import validate_inventories as validate_sewage_digestion_inventories
    from scripts.review_uvek_sewer_grids import EVIDENCE_SCOPE as SEWER_GRID_SCOPE
    from scripts.review_uvek_sewer_grids import SOURCES as SEWER_GRIDS
    from scripts.review_uvek_sewer_grids import approved_grid_rule as approved_sewer_grid_rule
    from scripts.review_uvek_sewer_grids import grid_decision as sewer_grid_decision
    from scripts.review_uvek_sewer_grids import inventory_record as sewer_grid_inventory_record
    from scripts.review_uvek_sewer_grids import validate_inventories as validate_sewer_grid_inventories
    from scripts.review_uvek_sewer_throughput import EVIDENCE_SCOPE as SEWER_THROUGHPUT_SCOPE
    from scripts.review_uvek_sewer_throughput import SOURCES as SEWER_THROUGHPUT_SOURCES
    from scripts.review_uvek_sewer_throughput import collect_extra_inventories as collect_sewer_throughput_extras
    from scripts.review_uvek_sewer_throughput import forward_decision as sewer_throughput_decision
    from scripts.review_uvek_sewer_throughput import inventory_record as sewer_throughput_inventory_record
    from scripts.review_uvek_sewer_throughput import validate_bindings as validate_sewer_throughput_bindings
    from scripts.review_uvek_sewer_throughput import validate_inventories as validate_sewer_throughput_inventories
    from scripts.review_uvek_slag_landfill_components import EVIDENCE_SCOPE as SLAG_LANDFILL_COMPONENT_SCOPE
    from scripts.review_uvek_slag_landfill_components import SOURCES as SLAG_LANDFILL_COMPONENTS
    from scripts.review_uvek_slag_landfill_components import (
        approved_component_rule as approved_slag_landfill_component_rule,
    )
    from scripts.review_uvek_slag_landfill_components import (
        collect_extra_inventories as collect_slag_landfill_component_extras,
    )
    from scripts.review_uvek_slag_landfill_components import component_decision as slag_landfill_component_decision
    from scripts.review_uvek_slag_landfill_components import (
        inventory_record as slag_landfill_component_inventory_record,
    )
    from scripts.review_uvek_slag_landfill_components import (
        validate_inventories as validate_slag_landfill_component_inventories,
    )
    from scripts.review_uvek_slf_landfarming import EVIDENCE_SCOPE as SLF_LANDFARMING_SCOPE
    from scripts.review_uvek_slf_landfarming import SOURCES as SLF_LANDFARMING_SOURCES
    from scripts.review_uvek_slf_landfarming import collect_extra_inventories as collect_slf_landfarming_extras
    from scripts.review_uvek_slf_landfarming import forward_decision as slf_landfarming_decision
    from scripts.review_uvek_slf_landfarming import inventory_record as slf_landfarming_inventory_record
    from scripts.review_uvek_slf_landfarming import validate_bindings as validate_slf_landfarming_bindings
    from scripts.review_uvek_slf_landfarming import validate_inventories as validate_slf_landfarming_inventories
    from scripts.review_uvek_stainless_stock import EVIDENCE_SCOPE as STAINLESS_STOCK_SCOPE
    from scripts.review_uvek_stainless_stock import SOURCES as STAINLESS_STOCKS
    from scripts.review_uvek_stainless_stock import approved_stock_rule as approved_stainless_stock_rule
    from scripts.review_uvek_stainless_stock import inventory_record as stainless_stock_inventory_record
    from scripts.review_uvek_stainless_stock import stock_decision as stainless_stock_decision
    from scripts.review_uvek_stainless_stock import validate_inventories as validate_stainless_stock_inventories
    from scripts.review_uvek_steel_residue_markets import EVIDENCE_SCOPE as STEEL_RESIDUE_MARKET_SCOPE
    from scripts.review_uvek_steel_residue_markets import SOURCES as STEEL_RESIDUE_MARKET_SOURCES
    from scripts.review_uvek_steel_residue_markets import (
        collect_extra_inventories as collect_steel_residue_market_extras,
    )
    from scripts.review_uvek_steel_residue_markets import forward_decision as steel_residue_market_decision
    from scripts.review_uvek_steel_residue_markets import inventory_record as steel_residue_market_inventory_record
    from scripts.review_uvek_steel_residue_markets import validate_bindings as validate_steel_residue_market_bindings
    from scripts.review_uvek_steel_residue_markets import (
        validate_inventories as validate_steel_residue_market_inventories,
    )
    from scripts.review_uvek_swiss_cement_markets import SOURCES as SWISS_CEMENT_MARKETS
    from scripts.review_uvek_swiss_cement_markets import collect_inventories as collect_swiss_cement_inventories
    from scripts.review_uvek_swiss_cement_markets import forward_decision as swiss_cement_market_decision
    from scripts.review_uvek_swiss_cement_markets import validate_bindings as validate_swiss_cement_bindings
    from scripts.review_uvek_swiss_cement_markets import validate_inventories as validate_swiss_cement_inventories
    from scripts.review_uvek_swiss_pet_fluwa import EVIDENCE_SCOPE as SWISS_PET_FLUWA_SCOPE
    from scripts.review_uvek_swiss_pet_fluwa import SOURCES as SWISS_PET_FLUWA_SOURCES
    from scripts.review_uvek_swiss_pet_fluwa import collect_extra_inventories as collect_swiss_pet_fluwa_extras
    from scripts.review_uvek_swiss_pet_fluwa import forward_decision as swiss_pet_fluwa_decision
    from scripts.review_uvek_swiss_pet_fluwa import inventory_record as swiss_pet_fluwa_inventory_record
    from scripts.review_uvek_swiss_pet_fluwa import validate_bindings as validate_swiss_pet_fluwa_bindings
    from scripts.review_uvek_swiss_pet_fluwa import validate_inventories as validate_swiss_pet_fluwa_inventories
    from scripts.review_uvek_uncontrolled_disposal import EVIDENCE_SCOPE as UNCONTROLLED_DISPOSAL_SCOPE
    from scripts.review_uvek_uncontrolled_disposal import SOURCES as UNCONTROLLED_DISPOSAL_SOURCES
    from scripts.review_uvek_uncontrolled_disposal import (
        collect_extra_inventories as collect_uncontrolled_disposal_extras,
    )
    from scripts.review_uvek_uncontrolled_disposal import (
        extend_companion_market_evidence as extend_uncontrolled_companion_market_evidence,
    )
    from scripts.review_uvek_uncontrolled_disposal import forward_decision as uncontrolled_disposal_decision
    from scripts.review_uvek_uncontrolled_disposal import inventory_record as uncontrolled_disposal_inventory_record
    from scripts.review_uvek_uncontrolled_disposal import validate_bindings as validate_uncontrolled_disposal_bindings
    from scripts.review_uvek_uncontrolled_disposal import (
        validate_inventories as validate_uncontrolled_disposal_inventories,
    )
    from scripts.review_uvek_venting import (
        VENTING_SOURCES,
        compiled_venting_rule,
        venting_decision,
    )
    from scripts.review_uvek_waste_concrete_inert import SOURCES as WASTE_CONCRETE_INERT_SOURCES
    from scripts.review_uvek_waste_concrete_inert import collect_inventories as collect_waste_concrete_inert_inventories
    from scripts.review_uvek_waste_concrete_inert import forward_decision as waste_concrete_inert_decision
    from scripts.review_uvek_waste_concrete_inert import validate_bindings as validate_waste_concrete_inert_bindings
    from scripts.review_uvek_waste_concrete_inert import (
        validate_inventories as validate_waste_concrete_inert_inventories,
    )
    from scripts.review_uvek_whole_freight import EVIDENCE_SCOPE as WHOLE_FREIGHT_SCOPE
    from scripts.review_uvek_whole_freight import REVERSE_METHOD as WHOLE_FREIGHT_REVERSE_METHOD
    from scripts.review_uvek_whole_freight import SOURCES as WHOLE_FREIGHT_SOURCES
    from scripts.review_uvek_whole_freight import approved_freight_rule as approved_whole_freight_rule
    from scripts.review_uvek_whole_freight import collect_extra_inventories as collect_whole_freight_extras
    from scripts.review_uvek_whole_freight import forward_decision as whole_freight_decision
    from scripts.review_uvek_whole_freight import inventory_record as whole_freight_inventory_record
    from scripts.review_uvek_whole_freight import reverse_decision as whole_freight_reverse_decision
    from scripts.review_uvek_whole_freight import validate_bindings as validate_whole_freight_bindings
    from scripts.review_uvek_whole_freight import validate_inventories as validate_whole_freight_inventories
    from scripts.review_uvek_wwt_residues import (
        APPROVED_WWT_RESIDUE_OMISSIONS,
        validate_wwt_residue_inventories,
    )

FIELDS = ("name", "reference product", "location", "unit")
REVIEW_PATH = "brightpath/data/export/uvek_reference_flow_sign_review.json"
FORWARD = "ecoinvent-to-uvek-2025.json"
REVERSE = "uvek-2025-to-ecoinvent-3.12-cutoff.json"
COAL_TAR_SOURCE = ("market for coal tar", "coal tar", "GLO", "kilogram")
COAL_TAR_TARGET = ("Tar, at coke plant", "Tar, at coke plant", "GLO", "kilogram")
COAL_TAR_LIMITATIONS = "Approved approximate production proxy: UVEK tar uses coke-plant energy allocation, including 4.1% of the coke-plant burdens allocated to tar. Ecoinvent market coverage and consequential modelling are not reproduced."
COAL_TAR_TREATMENT_SOURCE = (
    "treatment of coal tar, in industrial furnace 1MW",
    "coal tar",
    "GLO",
    "kilogram",
)
COAL_TAR_TREATMENT_OBSERVATIONS = [(f"ecoinvent-{v}-consequential", -1) for v in ("3.6", "3.7", "3.8", "3.9")]
APPROVED_PROFILED_OMISSIONS = {
    source: {
        "rationale": f"Approved on 2026-11-05, including similar cases: omit {source[0]} / GLO / kilogram only in the exact consequential ecoinvent 3.6--3.9 profiles and continue conversion with a warning. These references supply coal-tar combustion treatment, whereas Tar, at coke plant supplies allocated tar production. No suitable complete UVEK treatment counterpart was established. Combustion emissions, operating burdens and the credit for displacing 26.6 MJ of heat per kilogram are excluded. "
        + ("Additional market freight is excluded too. " if source == COAL_TAR_SOURCE else "")
        + "Score effects are method-dependent and unquantified. The complete original exchange remains in the loss report; production, matching foreground suppliers and separately listed foreground emissions are preserved.",
        "limitations": [
            "Omission excludes both treatment burdens and heat substitution credits; it does not establish zero impact or equivalent treatment. The ten approved positive-reference coal-tar market profiles retain their production proxy and factor +1. Missing or unreviewed exact profiles remain blocked. No reverse reconstruction is inferred."
        ],
        "evidence": [
            "Read-only inspection of both exact identities across consequential ecoinvent 3.6--3.9: eight source/profile pairs with production -1 kg. Each market supplies 100% coal-tar furnace treatment plus train/lorry freight. Each treatment has active technosphere and biosphere exchanges and a -26.6 MJ heat substitution exchange per kilogram of coal tar.",
            "UVEK bafu-521869 has production +1 kg and native PRODUCT_FLOW output. Its documentation allocates 4.1% of coke-plant burdens to tar production. The inspected catalog has no complete matching coal-tar furnace-treatment service; generic hazardous-waste incineration does not establish the same composition, technology or heat substitution model.",
            f"User approved these two scoped omissions and auto-approved similar cases on 2026-11-05. Exact prior targets, factors and sign observations remain audited in {REVIEW_PATH}.",
        ],
    }
    for source in (COAL_TAR_SOURCE, COAL_TAR_TREATMENT_SOURCE)
}
PROFILE_OMISSION_TARGETS = {source: COAL_TAR_TARGET for source in APPROVED_PROFILED_OMISSIONS}
PROFILE_OMISSION_OBSERVATIONS = {COAL_TAR_TREATMENT_SOURCE: COAL_TAR_TREATMENT_OBSERVATIONS}
PROFILE_OMISSION_APPROVED_PROFILES = {}
APPROVED_RECOVERY_CREDITS = {}
for (
    name,
    product,
    market_product,
    previous_name,
    target_name,
    location,
    code,
    quantity,
    limitation,
) in (
    (
        "metalliferous hydroxide sludge to market for zinc concentrate",
        "metalliferous hydroxide sludge",
        "zinc concentrate",
        "Lithium hydroxide, at plant",
        "Zinc concentrate, at beneficiation",
        "GLO",
        "bafu-370641",
        0.0622641509433962,
        "The source is already a credit-only proxy without Waelz recovery burdens; this approval does not invent those missing processing inputs. Concentrate grades, mine/beneficiation allocation and supply models differ.",
    ),
    (
        "zinc scrap, post-consumer to generic market for zinc",
        "zinc scrap, post-consumer",
        "zinc",
        "Zinc concentrate, at beneficiation",
        "Zinc, primary, at regional storage",
        "RER",
        "bafu-236764",
        1.0,
        "Primary zinc metal replaces the credited zinc commodity, not zinc concentrate or physical scrap processing. The European supply mix and regional-storage boundary approximate the global market; grades and co-product allocation may differ.",
    ),
    (
        "bronze scrap, post-consumer to generic market for bronze",
        "bronze scrap, post-consumer",
        "bronze",
        "Contour, bronze",
        "Bronze, at plant",
        "CH",
        "bafu-368900",
        1.0,
        "Bulk bronze replaces the credited bronze commodity, not a manufactured contour. The Swiss provider assumes 95% copper/5% tin and documents poor data quality; alloy grade, geography, production technology and allocation differences remain accepted screening limitations.",
    ),
):
    source = (name, product, "GLO", "kilogram")
    explicit = product == "metalliferous hydroxide sludge"
    review = {
        "rationale": f"{'Approved' if explicit else 'Auto-approved under the similar documented metal-recovery-credit authorization'} on 2026-11-05: preserve the exact {name} / GLO / kilogram recovery credit using {target_name} / {location} / kilogram only in inspected consequential ecoinvent 3.6--3.12. Each kilogram of source represents a credit of {quantity} kg of {market_product}. Apply the positive factor {quantity} so a negative waste input remains a negative credited-product input; this is an explicit waste-to-product-credit role conversion, not treatment-service sign inversion. {limitation}",
        "limitations": [
            f"Approved recovery-credit screening proxy: preserve {quantity} kg of credited {market_product} per kilogram of {product}, including its incoming sign. {limitation} Ecoinvent's global market mix, freight and consequential supply model are not reproduced. No additional metal-content or moisture scaling is inferred. Foreground production, matching local suppliers, other exchanges, supported uncertainty and formulas are preserved. Missing exact source profiles remain blocked; no reverse reconstruction of waste from primary product is inferred."
        ],
        "evidence": [
            f"Read-only inspection of all seven exact consequential 3.6--3.12 inventories: production -1 kg, one active technosphere exchange of {-quantity} kg to market for {market_product} / GLO / kilogram, market reference +1, and no active direct biosphere exchanges. All 21 pairs in this three-identity batch have credit-only source functions, not inventories of physical recovery processing.",
            f"Inspected UVEK 2025 {code}: {target_name}, {location}, kilogram, production +1, non-empty production inventory and independently verified native PRODUCT_FLOW output. {limitation}",
            f"User approved preservation of the zinc-concentrate credit and auto-approved independently inspected similar cases on 2026-11-05. Prior targets/factors, signed reference evidence and exact credit quantities remain audited in {REVIEW_PATH}.",
        ],
    }
    APPROVED_RECOVERY_CREDITS[source] = {
        "previous_target": (
            previous_name,
            previous_name,
            "RER" if product == "bronze scrap, post-consumer" else "GLO",
            "kilogram",
        ),
        "target": (target_name, target_name, location, "kilogram"),
        "credited_supplier": (
            f"market for {market_product}",
            market_product,
            "GLO",
            "kilogram",
        ),
        "quantity": quantity,
        "approval_scope": "explicit" if explicit else "similar_case",
        "observations": sorted(
            (f"ecoinvent-{version}-consequential", -1)
            for version in ("3.6", "3.7", "3.8", "3.9", "3.10", "3.11", "3.12")
        ),
        "review": review,
    }
PESTICIDE_CREDIT_SOURCE = (
    "dichloropropene to generic market for pesticide, unspecified",
    "dichloropropene",
    "GLO",
    "kilogram",
)
APPROVED_RECOVERY_CREDITS[PESTICIDE_CREDIT_SOURCE] = {
    "previous_target": (
        "Diode, unspecified, at plant",
        "Diode, unspecified, at plant",
        "GLO",
        "kilogram",
    ),
    "target": (
        "Pesticide unspecified, at regional storehouse",
        "Pesticide unspecified, at regional storehouse",
        "RER",
        "kilogram",
    ),
    "credited_supplier": (
        "market for pesticide, unspecified",
        "pesticide, unspecified",
        "GLO",
        "kilogram",
    ),
    "quantity": 1.0,
    "approval_scope": "similar_case",
    "observations": sorted((f"ecoinvent-{version}-consequential", -1) for version in ("3.6", "3.7", "3.8", "3.9")),
    "review": {
        "rationale": "Auto-approved on 2026-11-05 under the similar documented single-product substitution-credit authorization: preserve dichloropropene to generic market for pesticide, unspecified / GLO / kilogram only in the four inspected consequential ecoinvent 3.6--3.9 profiles. Each kilogram credits 1 kg of unspecified pesticide. Replace the unrelated diode-production target by Pesticide unspecified, at regional storehouse / RER / kilogram, applying positive factor 1 so the incoming credit sign is retained. This represents the source's generic pesticide credit, not dichloropropene manufacture, application or release.",
        "limitations": [
            "Approved generic pesticide-credit screening proxy: preserve the source credit of 1 kg pesticide per kilogram dichloropropene, including its incoming sign. The European pesticide-production mixture, formulation, technology, toxicity inventory and regional-storehouse boundary approximate the source global market; its mix, freight and consequential supply model are not reproduced. No ingredient-specific toxicity equivalence is asserted and no application emissions are added. Foreground production, matching local suppliers, separately listed chemical emissions, supported uncertainty and formulas remain intact. Missing exact profiles stay blocked; no reverse reconstruction is inferred."
        ],
        "evidence": [
            "Read-only inspection of all four exact consequential 3.6--3.9 source inventories: production -1 kg, one -1 kg input to market for pesticide, unspecified / GLO / kilogram, market production +1, no other active technosphere/biosphere exchanges, and explicit documentation of a generic-pesticide linking function.",
            "Inspected UVEK 2025 bafu-285276: Pesticide unspecified, at regional storehouse, RER/kg, production +1, independently verified native PRODUCT_FLOW output; non-empty generic pesticide-production inventory with 101 active technosphere and 147 biosphere exchanges. The description covers pesticide production including materials, energy, infrastructure and emissions; it does not establish ingredient-specific equivalence.",
            f"User approved preserving documented single-product credits and auto-approved independently inspected similar cases. Prior target/factor and the four signed credit quantities remain audited in {REVIEW_PATH}.",
        ],
    },
}
APPROVED_STEEL_RECOVERY_OMISSIONS = {}
APPROVED_FEED_SUBSTITUTION_OMISSIONS = {}
for product, link_name, link_target, market_target, location, energy, protein in (
    (
        "Distiller's Dried Grains with Solubles",
        "Distiller's Dried Grains with Solubles to generic market for protein feed",
        "Grain maize IP, at feed mill",
        "Rye grains IP, at farm",
        "CH",
        18.009,
        0.38,
    ),
    (
        "fodder yeast",
        "fodder yeast to generic market for protein feed",
        "Fodder beets IP, at farm",
        "Fodder beets IP, at farm",
        "CH",
        18.814,
        0.4876,
    ),
    (
        "grass fibre",
        "grass fibre to generic market for energy feed",
        "xx Grass fibres, at digestion",
        "xx Grass fibres, at digestion",
        "CH",
        17.954,
        0.0,
    ),
    (
        "sugar beet pulp",
        "sugar beet pulp to generic market for energy feed",
        "Pulps, from sugar beet, at sugar refinery",
        "Pulps, from sugar beet, at sugar refinery",
        "CH",
        4.23,
        0.0225,
    ),
    (
        "sweet sorghum grain",
        "sweet sorghum grain to generic market for energy feed",
        "Sweet sorghum grains, at farm",
        "Sweet sorghum grains, at farm",
        "CN",
        16.68924,
        0.34542,
    ),
):
    basis = f"{energy} MJ gross feed energy" + (
        f" and {protein} kg 100%-crude-protein feed" if protein else " only, with no protein-feed credit"
    )
    for market in (False, True):
        source = (
            f"market for {product}" if market else link_name,
            product,
            "GLO",
            "kilogram",
        )
        previous_name = market_target if market else link_target
        freight = market and product != "sugar beet pulp"
        boundary = (
            "Its additional market freight is excluded too; separately listed foreground transport is preserved."
            if freight
            else "This source has no additional market freight to omit."
        )
        review = {
            "rationale": f"{'Auto-approved under the similar consequential feed-market authorization' if market else 'Approved'} on 2026-11-05: omit {source[0]} / GLO / kilogram only in the inspected consequential ecoinvent 3.6--3.12 profiles and continue conversion with a warning. Its source-unit substitution basis is {basis}. These nutritional substitution contributions and their associated credits are excluded from the target dataset. The former {previous_name} mass-based product proxy does not establish equivalent feed-energy/protein substitution. Score effects are method-dependent and unquantified. {boundary} The complete original exchange remains in the loss report; production, matching local suppliers, other foreground inputs and separately listed emissions are preserved.",
            "limitations": [
                f"The omitted feed-substitution contributions and credits are not represented in the target dataset. A validated nutritional substitution model would be needed to preserve the gross-energy basis and, where present, the crude-protein basis together. No arbitrary crop-mass, moisture or single-nutrient scaling is inferred. This is not a zero-impact claim for feed production, crop processing or the foreground activity. {boundary} Positive-reference cut-off product markets receive no omission approval. Missing exact profiles remain blocked; no reverse reconstruction is inferred."
            ],
            "evidence": [
                f"Read-only inspection of all seven exact consequential 3.6--3.12 linking inventories for {product}: production -1 kg, negative inputs only to gross-energy-feed markets (MJ) and, where present, 100%-crude-protein-feed markets (kg), and no active direct biosphere exchanges. Their aggregate source-unit basis is {basis}; tiny floating-point differences in geographic market splits are not additional scaling factors.",
                (
                    "Independently inspected all seven consequential market inventories: production -1 kg, exactly -1 kg of this product's own nutritional linking activity, no direct biosphere exchanges, and freight only where stated. DDGS, yeast and sorghum markets add train/lorry freight; grass fibre adds lorry freight; beet pulp has no freight. All 35 cut-off market counterparts instead have production +1 kg and remain pending separate supplier review."
                    if market
                    else "The approved batch covers five exact linking identities and 35 source/profile pairs. Four credit gross energy and crude protein; grass fibre credits gross energy only. Current UVEK crop/product-mass references do not provide the same generic nutrient-market basis."
                ),
                f"User approved omission of all five nutritional feed-credit links and auto-approved independently inspected similar cases on 2026-11-05. Exact prior target/factor and reference signs remain audited in {REVIEW_PATH}.",
            ],
        }
        observations = sorted(
            (f"ecoinvent-{version}-{model}", -1 if model == "consequential" else 1)
            for version in ("3.6", "3.7", "3.8", "3.9", "3.10", "3.11", "3.12")
            for model in (("cutoff", "consequential") if market else ("consequential",))
        )
        approval = {
            "review": review,
            "previous_target": (previous_name, previous_name, location, "kilogram"),
            "observations": observations,
            "kind": "market" if market else "link",
            "approval_scope": "similar_case" if market else "explicit",
            "gross_energy_MJ": energy,
            "crude_protein_kg": protein,
            "market_freight": freight,
        }
        APPROVED_FEED_SUBSTITUTION_OMISSIONS[source] = approval
        APPROVED_PROFILED_OMISSIONS[source] = review
        PROFILE_OMISSION_TARGETS[source] = approval["previous_target"]
        PROFILE_OMISSION_OBSERVATIONS[source] = observations
FEED_NUTRIENT_REVIEW_PATH = "brightpath/data/export/uvek_feed_nutrient_omission_review.json"
APPROVED_NUTRIENT_REFERENCE_OMISSIONS = {}
nutrient_review = json.loads(
    (Path(__file__).resolve().parents[1] / FEED_NUTRIENT_REVIEW_PATH).read_text(encoding="utf-8")
)
for record in nutrient_review["rules"]:
    source = tuple(record["source"][field] for field in FIELDS)
    previous_target = tuple(record["previous_target"][field] for field in FIELDS)
    observations = sorted(tuple(row) for row in record["observations"])
    if (
        source[1] not in ("energy feed, gross", "protein feed, 100% crude")
        or source in APPROVED_NUTRIENT_REFERENCE_OMISSIONS
        or any(sign != 1 for _, sign in observations)
    ):
        raise ValueError("Invalid or duplicate normalized nutrient omission approval.")
    basis = (
        "gross nutritional feed energy in MJ, not industrial heat or fuel energy"
        if source[1] == "energy feed, gross"
        else "100%-crude-protein feed in kg, not an unscaled kilogram of crop, oil or unspecified protein concentrate"
    )
    review = {
        "rationale": f"Auto-approved on 2026-11-05 under the similar unvalidated nutritional-basis omission authorization: omit the external {source[0]} / {source[1]} / {source[2]} / {source[3]} reference only in its enumerated inspected profiles and continue conversion with a warning. This reference represents {basis}. No validated nutritional conversion establishes equivalence with the former {previous_target[0]} / {previous_target[2]} provider. Its nutritional supply contributions and any associated substitution credits are excluded from the target dataset; score effects are method-dependent and unquantified. The complete original exchange remains in the loss report. Production, matching foreground suppliers and other foreground inputs/emissions are preserved.",
        "limitations": [
            "Nutritional reference-unit contributions and credits are excluded until a validated nutritional substitution model is available. Equal dimensional units do not establish equivalent nutrient function. No arbitrary crop/protein-content scaling or substitution of industrial heat for nutritional energy is inferred. This does not establish zero impact for feed production or the foreground inventory. Ordinary coproduct-mass references are outside this approval. Missing exact profiles remain blocked; no reverse reconstruction is inferred."
        ],
        "evidence": [
            f"Exact approved source/prior-target/profile whitelist in {FEED_NUTRIENT_REVIEW_PATH}: 95 normalized nutrient identities, 692 inspected positive-reference source/profile pairs, 45 gross-energy MJ references and 50 100%-crude-protein kg references. Existing heuristic factors are all 1 and do not document nutritional equivalence. Prior targets supply thermal/fuel energy, crop mass, crude oil or an unspecified protein concentrate, not a validated generic nutritional market.",
            "Independent native UVEK reference metadata and inspected source-reference profiles confirm positive PRODUCT_FLOW target references and positive source references. Positive coordinates do not validate the changed nutritional basis. This is an omission decision, not a coordinate sign inversion or a complete background inventory reproduction.",
            "User approved omitting unsupported nutritional feed contributions and auto-approved independently inspected similar cases. The whitelist is fixed authoring evidence; runtime matches exact identities and exact profiles, never nutrient keywords.",
        ],
    }
    approval = {
        "previous_target": previous_target,
        "observations": observations,
        "review": review,
        "approval_scope": "similar_case",
    }
    APPROVED_NUTRIENT_REFERENCE_OMISSIONS[source] = approval
    APPROVED_PROFILED_OMISSIONS[source] = review
    PROFILE_OMISSION_TARGETS[source] = previous_target
    PROFILE_OMISSION_OBSERVATIONS[source] = observations
    PROFILE_OMISSION_APPROVED_PROFILES[source] = {label for label, _ in observations}
if (
    len(APPROVED_NUTRIENT_REFERENCE_OMISSIONS) != nutrient_review["source_count"]
    or sum(len(a["observations"]) for a in APPROVED_NUTRIENT_REFERENCE_OMISSIONS.values())
    != nutrient_review["profile_pairs"]
):
    raise ValueError("Normalized nutrient omission whitelist coverage changed.")
for material, target_name, target_code in (
    ("basic oxygen furnace dust", "Basic oxygen furnace slag, at plant", "bafu-607456"),
    (
        "basic oxygen furnace sludge",
        "Basic oxygen furnace slag, at plant",
        "bafu-607456",
    ),
    ("basic oxygen furnace slag", "Basic oxygen furnace slag, at plant", "bafu-607456"),
    (
        "basic oxygen furnace secondary metallurgy slag",
        "Basic oxygen furnace slag, at plant",
        "bafu-607456",
    ),
    ("blast furnace dust", "Blast furnace slag, at plant", "bafu-167904"),
    ("blast furnace sludge", "Blast furnace slag, at plant", "bafu-167904"),
    ("electric arc furnace slag", "Electric arc furnace slag, at plant", "bafu-339275"),
    (
        "electric arc furnace secondary metallurgy slag",
        "Electric arc furnace slag, at plant",
        "bafu-339275",
    ),
):
    separator = " " if "secondary metallurgy" in material else ", "
    name = f"{material} to market for {material}{separator}for recovery"
    for location in ("RER", "RoW"):
        source = (name, material, location, "kilogram")
        explicit = material == "basic oxygen furnace dust"
        approval = {
            "previous_target": (target_name, target_name, "RER", "kilogram"),
            "approval_scope": "explicit" if explicit else "similar_case",
            "observations": sorted(
                (f"ecoinvent-{version}-{model}", -1)
                for version in ("3.7", "3.8", "3.9", "3.10", "3.11", "3.12")
                for model in ("cutoff", "consequential")
            ),
            "review": {
                "rationale": f"{'Approved' if explicit else 'Auto-approved under the similar steelmaking recovery-handoff authorization'} on 2026-11-05: omit {name} / {location} / kilogram only in the inspected ecoinvent 3.7--3.12 cut-off/consequential profiles and continue conversion with a warning. This is a waste-to-recyclable renaming handoff, with no direct emissions or physical treatment inventory. Its cut-off chain terminates at an empty recycled-content boundary. Its consequential chain carries supplementary-cementitious-material recovery credits, which are excluded by this omission. The former {target_name} target supplies economically allocated slag production, not the same handoff or consequential credit. Score effects are method-dependent and unquantified. The complete original exchange remains in the loss report; production, matching foreground suppliers and separately listed foreground emissions are preserved.",
                "limitations": [
                    "Only the exact external recovery-handoff exchange is omitted. Consequential recovery contributions/credits are absent from the target dataset; their score effect is unquantified. The inspected cut-off handoff has an empty upstream chain, but this does not establish zero impact for steelmaking, slag, dust, sludge, disposal, transport or recycling as a whole. Separate foreground inputs, emissions, production and matching local suppliers remain intact. Unreviewed exact source profiles remain blocked; no reverse omission or keyword-based deletion is inferred."
                ],
                "evidence": [
                    f"Read-only inspection of all twelve exact source/profile inventories for {name} / {location} / kg: production -1, a single -1 technosphere input to its own recovered-material supplier, no active direct biosphere exchanges, and a documented renaming function. The full batch contains sixteen identities and 192 pairs: 96 cut-off chains end at a +1-reference process with no other active exchanges; 96 consequential chains connect +1-reference recovery markets to supplementary-cementitious-material markets. No complete background inventories are published.",
                    f"Inspected UVEK 2025 {target_code}: {target_name}, RER, kilogram, positive reference +1, independently verified native PRODUCT_FLOW output; documented economically allocated steelmaking/slag-production inventory rather than a waste-to-recyclable renaming service. BOF, blast-furnace and EAF allocation factors are respectively 0.0077, 0.015 and 0.0052. No equivalent recovery credit is inferred from a matching material name.",
                    f"User explicitly approved the RER/RoW basic-oxygen-furnace-dust handoff omissions and authorized independently inspected similar cases on 2026-11-05. Exact prior targets/factors and all source-profile signs remain in {REVIEW_PATH}.",
                ],
            },
        }
        APPROVED_STEEL_RECOVERY_OMISSIONS[source] = approval
        APPROVED_PROFILED_OMISSIONS[source] = approval["review"]
        PROFILE_OMISSION_TARGETS[source] = approval["previous_target"]
        PROFILE_OMISSION_OBSERVATIONS[source] = approval["observations"]
extend_exact_patch_handoff_observations(APPROVED_STEEL_RECOVERY_OMISSIONS, PROFILE_OMISSION_OBSERVATIONS)
extend_complete_patch_handoff_observations(APPROVED_STEEL_RECOVERY_OMISSIONS, PROFILE_OMISSION_OBSERVATIONS)

APPROVED_PROFILED_TREATMENTS = {}
for material, treatment, water, code, versions in (
    (
        "newspaper",
        "municipal incineration",
        "14.7",
        "bafu-341912",
        ("3.6", "3.7", "3.8", "3.9"),
    ),
    (
        "newspaper",
        "sanitary landfill",
        "14.7",
        "bafu-602962",
        ("3.6", "3.7", "3.8", "3.9", "3.10", "3.11", "3.12"),
    ),
    (
        "packaging paper",
        "municipal incineration",
        "13.7",
        "bafu-445807",
        ("3.6", "3.7", "3.8", "3.9"),
    ),
):
    target_name = f"Disposal, {material}, {water}% water, to {treatment}"
    source = (
        f"treatment of waste {material}, {treatment}",
        f"waste {material}",
        "CH",
        "kilogram",
    )
    APPROVED_PROFILED_TREATMENTS[source] = {
        "target": (target_name, target_name, "CH", "kilogram"),
        "observations": [("ecoinvent-3.9-cutoff", 1)]
        + [(f"ecoinvent-{version}-consequential", -1) for version in versions],
        "rationale": f"Approved on 2026-11-05: represent the exact CH, kilogram waste-{material} {treatment} reference with its inspected CH UVEK disposal service. Apply factor +1 only in ecoinvent 3.9 cut-off and factor -1 only in the inspected consequential profiles. Source-profile signs are selected independently; no global sign or missing-profile fallback is approved.",
        "limitations": f"Approved profile-specific paper-disposal screening proxy: CH waste {material} is represented by UVEK {treatment} of {water}% water material. Waste composition, treatment technology, emissions, allocation and background supplies may differ; LCIA equivalence is not established or quantified. "
        + (
            "The newer consequential newspaper-landfill electricity and heat substitution credits are not reproduced by the UVEK service. "
            if treatment == "sanitary landfill"
            else "Energy recovery, emission controls and residue treatment are approximated by the UVEK incineration service. "
        )
        + "Recovered-energy product references remain separate and unchanged.",
        "inventory_evidence": [
            f"Read-only inspection of all {len(versions) + 1} exact source/profile inventories: active waste-treatment inputs and emissions, production +1 kg in 3.9 cut-off versus -1 kg in consequential {', '.join(versions)}. The 3.9 cut-off entry is not an empty recycling placeholder."
            + (
                " Newer consequential newspaper landfill includes electricity/heat substitution credits."
                if treatment == "sanitary landfill"
                else ""
            ),
            f"Inspected UVEK 2025 {code}: {target_name}, CH, kilogram, production +1, independently verified native WASTE_FLOW input reference; active treatment inputs and biosphere emissions, with no negative technosphere supplier credits.",
        ],
    }
BATTERY_SOURCE = (
    "treatment of used Li-ion battery, hydrometallurgical treatment",
    "used Li-ion battery",
    "GLO",
    "kilogram",
)
BATTERY_PREVIOUS_TARGET = (
    "Anode, graphite, for Li-ion battery",
    "Anode, graphite, for Li-ion battery",
    "GLO",
    "kilogram",
)
BATTERY_TARGET = (
    "Disposal, Li-ions batteries, hydrometallurgical",
    "Disposal, Li-ions batteries, hydrometallurgical",
    "GLO",
    "kilogram",
)
BATTERY_LIMITATIONS = "Approved hydrometallurgical waste-treatment screening proxy: the inspected UVEK inventory allocates 100% of the process to disposal. Recovery yields, inventory coefficients, background supply chains and consequential credits may differ; LCIA equivalence is not established."
PYRO_BATTERY_SOURCE = (
    "treatment of used Li-ion battery, pyrometallurgical treatment",
    "used Li-ion battery",
    "GLO",
    "kilogram",
)
PYRO_BATTERY_TARGET = (
    "Disposal, Li-ions batteries, pyrometallurgical",
    "Disposal, Li-ions batteries, pyrometallurgical",
    "GLO",
    "kilogram",
)
LI_ION_MARKET_SOURCE = (
    "market for used Li-ion battery",
    "used Li-ion battery",
    "GLO",
    "kilogram",
)
LI_ION_MARKET_TARGET = (
    "Disposal, Li-ions batteries, mixed technology",
    "Disposal, Li-ions batteries, mixed technology",
    "GLO",
    "kilogram",
)
NIMH_SOURCE = (
    "treatment of used Ni-metal hydride battery, pyrometallurgical treatment",
    "used Ni-metal hydride battery",
    "GLO",
    "kilogram",
)
NIMH_MARKET_SOURCE = (
    "market for used Ni-metal hydride battery",
    "used Ni-metal hydride battery",
    "GLO",
    "kilogram",
)
NIMH_TARGET = (
    "Disposal, NiMH batteries",
    "Disposal, NiMH batteries",
    "GLO",
    "kilogram",
)
NIMH_PREVIOUS_TARGET = (
    "Hydroxide, for Li-ion NMC111 battery",
    "Hydroxide, for Li-ion NMC111 battery",
    "GLO",
    "kilogram",
)
APPROVED_BATTERY_TREATMENTS = {
    BATTERY_SOURCE: {
        "target": BATTERY_TARGET,
        "technology": "hydrometallurgical",
        "limitations": BATTERY_LIMITATIONS,
        "ecoinvent_code": "7ccb7dd5440fe71953c3e1092757f68e",
        "uvek_code": "bafu-272203",
        "excluded_references": "Recovered cobalt and lithium reference products are outside this approval.",
    },
    PYRO_BATTERY_SOURCE: {
        "target": PYRO_BATTERY_TARGET,
        "technology": "pyrometallurgical",
        "limitations": "Approved pyrometallurgical waste-treatment screening proxy: the inspected UVEK inventory allocates 100% of the process to disposal. Recovery yields, inventory coefficients, background supply chains and consequential credits may differ; LCIA equivalence is not established.",
        "ecoinvent_code": "0beb59f36d514b91a6f430c81284f385",
        "uvek_code": "bafu-265762",
        "excluded_references": "Recovered cobalt and manganese-dioxide reference products are outside this approval.",
    },
    LI_ION_MARKET_SOURCE: {
        "target": LI_ION_MARKET_TARGET,
        "technology": "mixed technology",
        "rationale": "Approved on 2026-11-04: replace the anode-manufacturing proxy for the GLO, kilogram used-Li-ion-battery market with the inspected UVEK 50% hydrometallurgical / 50% pyrometallurgical treatment mix, with factor -1. The additional source-market freight inputs are explicitly omitted.",
        "limitations": "Approved 50% hydrometallurgical / 50% pyrometallurgical screening proxy: the UVEK mix omits the four source-market freight inputs. Additional market-level transport burdens are not represented. Recovery, allocation, background and consequential credits may differ; LCIA equivalence is not established.",
        "inventory_evidence": [
            "Inspected ecoinvent market 809a4e0018b2e9537c1e60908266285d and UVEK bafu-523176: GLO, kilogram, reference production -1/+1, 50% hydro / 50% pyro. The same treatment shares and four freight supplier functions were inspected in all 14 source profiles."
        ],
    },
    NIMH_SOURCE: {
        "target": NIMH_TARGET,
        "previous_target": NIMH_PREVIOUS_TARGET,
        "technology": "pyrometallurgical NiMH",
        "rationale": "Auto-approved on 2026-11-04 under the user's similar-treatment policy: replace the Li-ion NMC111 hydroxide-manufacturing proxy for the exact GLO, kilogram Ni-metal-hydride waste-treatment reference with UVEK's inspected pyrometallurgical NiMH disposal service and factor -1. Chemistry, treatment technology, location and unit agree; recovered-metal references remain separate.",
        "limitations": "Approved NiMH pyrometallurgical screening proxy: UVEK allocates 100% of the process to disposal. Recovered-metal fate, recovery yields, background supply chains and consequential credits may differ; LCIA equivalence is not established.",
        "inventory_evidence": [
            "Inspected ecoinvent f5b9cd04cc359cfce2e1263b75aaa28f and UVEK bafu-202361: pyrometallurgical NiMH disposal, GLO, kilogram, Waste reference -1 versus service reference +1. UVEK allocates 100% to disposal."
        ],
    },
    NIMH_MARKET_SOURCE: {
        "target": NIMH_TARGET,
        "previous_target": NIMH_PREVIOUS_TARGET,
        "technology": "pyrometallurgical NiMH market",
        "rationale": "Auto-approved on 2026-11-04 under the user's similar-treatment policy: replace the Li-ion NMC111 hydroxide-manufacturing proxy for the GLO, kilogram used-Ni-metal-hydride battery market with the inspected 100% pyrometallurgical NiMH disposal service and factor -1. Additional market-level freight is excluded and disclosed.",
        "limitations": "Approved 100% pyrometallurgical NiMH market proxy: the UVEK disposal service omits the four source-market freight inputs. Additional market-level transport burdens are not represented. Recovery, allocation, background and consequential credits may differ; LCIA equivalence is not established.",
        "inventory_evidence": [
            "Inspected ecoinvent market 3f72df9a85677ad5b525dd744d715f1d: 100% NiMH pyrometallurgical treatment plus four freight inputs; UVEK bafu-202361 provides the same treatment service without that market freight. Reference production -1/+1; GLO, kilogram."
        ],
    },
}
PET_INCINERATION_NAME = "treatment of waste polyethylene terephthalate, municipal incineration"
PET_INCINERATION_PRODUCT = "waste polyethylene terephthalate"
PET_INCINERATION_TARGET = (
    "Disposal, polyethylene terephtalate, 0.2% water, to municipal incineration",
    "Disposal, polyethylene terephtalate, 0.2% water, to municipal incineration",
    "CH",
    "kilogram",
)
PET_PREVIOUS_TARGET = (
    "Fleece production, polyethylene terephthalate",
    "Fleece production, polyethylene terephthalate",
    "RER",
    "kilogram",
)
APPROVED_GEOGRAPHIC_TREATMENTS = {
    (PET_INCINERATION_NAME, PET_INCINERATION_PRODUCT, location, "kilogram"): {
        "target": PET_INCINERATION_TARGET,
        "previous_target": PET_PREVIOUS_TARGET,
        "technology": "municipal PET incineration",
        "source_versions": versions,
        "rationale": f"Approved on 2026-11-04: replace the fleece-manufacturing proxy for the exact {location}, kilogram waste-PET municipal-incineration reference with the inspected CH UVEK incineration service and factor -1. Waste material, treatment function, 0.2% water content and quantity basis agree. Geography is an explicitly accepted screening approximation; dust, landfill and recovered-energy references are outside this approval.",
        "limitations": f"Approved geographic treatment proxy: {location} PET incineration is represented by the CH service for 0.2% water waste. Swiss energy recovery, emission controls, residue treatment, allocation and background supply chains may differ from the source. LCIA equivalence is not established or quantified.",
        "inventory_evidence": [
            f"Inspected the exact {location} waste reference in ecoinvent {', '.join(versions)}, cut-off and consequential: kilogram, Waste classification, production -1, H2O 2000 ppm (0.2%), upper/gross heating value 23.13 MJ/kg, municipal incineration of PET in communal waste. Earlier RoW descriptions explicitly use Swiss 2010 technology; later GLO descriptions state that treatment-technology parameters are not geography-specific.",
            "Inspected UVEK 2025 bafu-514203: CH, kilogram, production +1; municipal incineration of PET in communal waste, 0.2% water, upper heating value 23.13 MJ/kg, independently verified native WASTE_FLOW input reference.",
        ],
    }
    for location, versions in (
        ("RoW", ("3.6", "3.7", "3.8", "3.9")),
        ("GLO", ("3.10", "3.11", "3.12")),
    )
}
PET_DUST_SOURCE = (
    "treatment of waste polyethylene terephthalate, dust, municipal incineration",
    "waste polyethylene terephthalate, dust",
    "RER",
    "kilogram",
)
LDPE_CARTON_PRODUCT = "LDPE in waste fresh milk carton (chilled), post-industrial"
LDPE_CARTON_SOURCE = (
    f"treatment of {LDPE_CARTON_PRODUCT}, municipal incineration",
    LDPE_CARTON_PRODUCT,
    "RER",
    "kilogram",
)
LDPE_INCINERATION_NAME = "Disposal, polyethylene, 0.4% water, to municipal incineration"
APPROVED_INCINERATION_COMPOSITION_PROXIES = {
    PET_DUST_SOURCE: {
        "target": PET_INCINERATION_TARGET,
        "previous_target": PET_PREVIOUS_TARGET,
        "technology": "municipal incineration of PET recycling dust",
        "source_versions": ("3.11", "3.12"),
        "rationale": "Approved on 2026-11-05: replace the fleece-manufacturing proxy for the exact RER, kilogram PET-recycling-dust incineration reference with the CH UVEK PET municipal-incineration service and factor -1. Preserve waste mass without heating-value or moisture scaling. Composition, geography and consequential energy-credit differences are explicitly accepted screening approximations.",
        "limitations": "Approved PET-dust incineration composition proxy: recycling dust with 2.7027% water and gross heating value 37.1 MJ/kg is represented by CH communal-waste PET incineration at 0.2% water and upper heating value 23.13 MJ/kg. Preserve wet waste mass without heating-value or moisture scaling. Dust composition, trace pollutants, emissions, controls, residue treatment, energy recovery, allocation and background supplies may differ; consequential electricity and heat substitution credits are not reproduced. RER is approximated by CH. LCIA equivalence is not established or quantified. Recovered-energy reference products remain separate.",
        "inventory_evidence": [
            "Read-only inspection of all four exact RER/kg source inventories in ecoinvent 3.11--3.12 cut-off/consequential: production -1, PET dust from mechanical recycling, H2O 27027 ppm, gross heating value 37.1 MJ/kg. Active incineration inputs and emissions; consequential inventories include electricity/heat substitution suppliers. Collection and transport to the incinerator are excluded.",
            "Inspected UVEK 2025 bafu-514203: CH/kg, production +1, native WASTE_FLOW input, non-empty PET municipal-incineration service at 0.2% water and upper heating value 23.13 MJ/kg, with no negative technosphere supplier credits. Full background inventories are not retained.",
        ],
    },
    LDPE_CARTON_SOURCE: {
        "target": (LDPE_INCINERATION_NAME, LDPE_INCINERATION_NAME, "CH", "kilogram"),
        "previous_target": (
            "Industrial machine, heavy, unspecified, at plant",
            "Industrial machine, heavy, unspecified, at plant",
            "RER",
            "kilogram",
        ),
        "technology": "municipal incineration of post-industrial LDPE milk-carton fraction",
        "source_versions": ("3.10", "3.11", "3.12"),
        "rationale": "Auto-approved on 2026-11-05 under the user's similar-incineration composition-proxy authorization: replace the industrial-machine manufacturing target for the exact RER/kg LDPE fraction from fresh-milk-carton production with the CH UVEK polyethylene incineration service and factor -1. The inspected source waste is 100% LDPE, not a whole composite carton. Preserve mass without heating-value or moisture scaling; disclose the composition, geography and consequential-credit differences.",
        "limitations": "Approved LDPE-fraction incineration composition proxy: post-industrial waste identified as 100% LDPE, with 13.775% water and gross heating value 38.79 MJ/kg, is represented by CH communal-waste polyethylene incineration at 0.4% water and upper heating value 42.82 MJ/kg. Preserve wet waste mass without heating-value or moisture scaling. Trace pollutants, emissions, controls, residue treatment, energy recovery, allocation and background supplies may differ; consequential electricity and heat substitution credits are not reproduced. RER is approximated by CH. LCIA equivalence is not established or quantified. Whole cartons, aluminium/polymer composites, ash and recovered-energy references are outside this approval.",
        "inventory_evidence": [
            "Read-only inspection of all six exact RER/kg source inventories in ecoinvent 3.10--3.12 cut-off/consequential: production -1, documented fraction 100% waste polyethylene, low density, H2O 137750 ppm, gross heating value 38.79 MJ/kg. Active incineration inputs and emissions; consequential inventories include electricity/heat substitution suppliers. Collection and transport to the incinerator are excluded.",
            "Inspected UVEK 2025 bafu-275730: CH/kg, production +1, native WASTE_FLOW input, non-empty polyethylene municipal-incineration service at 0.4% water and upper heating value 42.82 MJ/kg, with no negative technosphere supplier credits. No equivalence of pollutant composition or impact scores is inferred.",
        ],
    },
}
CRT_COATING_PRODUCT = "coating from waste cathode ray tube display"
CRT_RESIDUE_PRODUCT = "residue from mechanical treatment, cathode ray tube display"
CRT_COATING_NAME = "Disposal, coatings in CRT screens,  to municipal waste incineration"
CRT_RESIDUE_NAME = "Disposal, residues, mechanical treatment, CRT screen, in MSWI"
CRT_COATING_TARGET = (CRT_COATING_NAME, CRT_COATING_NAME, "CH", "kilogram")
CRT_RESIDUE_TARGET = (CRT_RESIDUE_NAME, CRT_RESIDUE_NAME, "CH", "kilogram")
CRT_SCREEN_PREVIOUS_TARGET = (
    "Cathode-ray tube, CRT screen, at plant",
    "Cathode-ray tube, CRT screen, at plant",
    "GLO",
    "kilogram",
)
CRT_ZINC_PREVIOUS_NAME = "Disposal, zinc in car shredder residue, 0% water, to municipal incineration"
CRT_ZINC_PREVIOUS_TARGET = (
    CRT_ZINC_PREVIOUS_NAME,
    CRT_ZINC_PREVIOUS_NAME,
    "CH",
    "kilogram",
)
CRT_OLDER_VERSIONS = ("3.6", "3.7", "3.8", "3.9")
CRT_NEWER_VERSIONS = ("3.10", "3.11", "3.12")


def crt_incineration_approval(source, versions, previous_target, *, coating, explicit=False):
    """Describe an exact inventory-inspected CRT proxy, never a label-based fallback."""
    market = source[0].startswith("market for")
    target = CRT_COATING_TARGET if coating else CRT_RESIDUE_TARGET
    boundary = (
        "The source market supplies 100% incineration of the same coating. Additional market-level freight is omitted; its transport burdens are not represented. CH markets have two freight suppliers, RoW markets have three, eight or nine depending on profile."
        if market
        else "The exact source is a direct incineration service; no market-level freight is inferred or added."
    )
    material = (
        "CRT coating/luminophore powder at 10% water is represented by the dedicated coating-disposal service. Legacy documentation contains inconsistent LCD-use wording; the CRT dataset name, luminophore synonym and active silver, strontium, sulfate and zinc flows support the CRT interpretation. This ambiguity is disclosed, not corrected or treated as proof of equal toxicity."
        if coating
        else "Mixed shredder residues from CRT-screen recycling are represented by their dedicated CRT-residue disposal service, not by screen manufacturing or zinc-only car-shredder-residue disposal. Source descriptions identify a mixture including rubber, electronics plastics, glass and metals; the proxy's intended use explicitly identifies CRT shredder residues."
    )
    return {
        "target": target,
        "previous_target": previous_target,
        "technology": ("CRT-coating incineration market" if market else "CRT incineration service"),
        "source_versions": versions,
        "approval_scope": "explicit" if explicit else "similar_case",
        "rationale": f"{'Approved' if explicit else 'Auto-approved under the similar-CRT-treatment authorization'} on 2026-11-05: use the inspected dedicated CH, kilogram CRT {'coating' if coating else 'shredder-residue'} incineration service for the exact {source[0]} / {source[2]} reference with factor -1. Preserve wet waste mass without heating-value or moisture scaling. {boundary}",
        "limitations": f"Approved CRT-incineration screening proxy: {material} Preserve wet waste mass without heating-value or moisture scaling. {source[2]} is represented by the CH service. Emission controls, fly-ash extraction, ash treatment, leaching, metal speciation and long-term metal releases may differ; source-specific extraction/leaching performance is not reconstructed. Energy recovery, metal recovery, allocation, background supplies and consequential substitution credits may differ; the source credit model is not reconstructed. LCIA equivalence, including toxicity impacts, is not established or quantified. {boundary} Glass, whole-screen treatment, sludge, ash-disposal and recovered-energy reference products remain separate.",
        "inventory_evidence": [
            f"Read-only inspection of all {2 * len(versions)} exact source/profile inventories in ecoinvent {', '.join(versions)}, cut-off and consequential: reference production -1 kilogram. Direct services have active treatment inputs and emissions; coating markets each supply exactly one kilogram of incineration of the same coating, with only freight inputs in addition and no active direct biosphere releases. This batch inspects 100 source/profile pairs in total, including all 28 coating-market pairs.",
            f"Inspected UVEK 2025 {'bafu-269681' if coating else 'bafu-377324'}: CH, kilogram, production +1, independently verified native WASTE_FLOW input reference; active technosphere inputs and biosphere emissions and no negative technosphere supplier credits. {'Name, luminophore synonym and selected metal flows support the coating function despite inconsistent legacy LCD wording.' if coating else 'The source comment specifies incineration of CRT-screen mechanical-treatment residues and identifies rubber as one constituent.'} No complete background inventory or pollutant recipe is retained.",
        ],
    }


APPROVED_CRT_INCINERATION_TREATMENTS = {}
for coating, technology, location, versions, previous_target, explicit in (
    (
        True,
        "municipal incineration",
        "GLO",
        CRT_NEWER_VERSIONS,
        CRT_SCREEN_PREVIOUS_TARGET,
        True,
    ),
    (
        True,
        "municipal incineration FAE",
        "CH",
        CRT_NEWER_VERSIONS,
        CRT_COATING_TARGET,
        False,
    ),
    (
        True,
        "municipal incineration with fly ash extraction",
        "CH",
        CRT_OLDER_VERSIONS,
        CRT_SCREEN_PREVIOUS_TARGET,
        True,
    ),
    (
        True,
        "municipal waste incineration",
        "CH",
        CRT_OLDER_VERSIONS,
        CRT_COATING_TARGET,
        False,
    ),
    (
        True,
        "municipal waste incineration",
        "RoW",
        CRT_OLDER_VERSIONS,
        CRT_SCREEN_PREVIOUS_TARGET,
        False,
    ),
    (
        False,
        "municipal incineration",
        "GLO",
        CRT_NEWER_VERSIONS,
        CRT_SCREEN_PREVIOUS_TARGET,
        False,
    ),
    (
        False,
        "municipal incineration FAE",
        "CH",
        CRT_NEWER_VERSIONS,
        CRT_RESIDUE_TARGET,
        False,
    ),
    (
        False,
        "municipal incineration with fly ash extraction",
        "CH",
        CRT_OLDER_VERSIONS,
        CRT_ZINC_PREVIOUS_TARGET,
        False,
    ),
    (
        False,
        "municipal waste incineration",
        "CH",
        CRT_OLDER_VERSIONS,
        CRT_RESIDUE_TARGET,
        False,
    ),
    (
        False,
        "municipal waste incineration",
        "RoW",
        CRT_OLDER_VERSIONS,
        CRT_RESIDUE_TARGET,
        False,
    ),
):
    product = CRT_COATING_PRODUCT if coating else CRT_RESIDUE_PRODUCT
    source = (f"treatment of {product}, {technology}", product, location, "kilogram")
    APPROVED_CRT_INCINERATION_TREATMENTS[source] = crt_incineration_approval(
        source, versions, previous_target, coating=coating, explicit=explicit
    )
for location in ("CH", "RoW"):
    source = (
        f"market for {CRT_COATING_PRODUCT}",
        CRT_COATING_PRODUCT,
        location,
        "kilogram",
    )
    APPROVED_CRT_INCINERATION_TREATMENTS[source] = crt_incineration_approval(
        source,
        CRT_OLDER_VERSIONS + CRT_NEWER_VERSIONS,
        CRT_SCREEN_PREVIOUS_TARGET,
        coating=True,
    )
CRT_GLASS_PRODUCT = "glass from used cathode ray tube"
CRT_GLASS_NAME = "Disposal, treatment of CRT glass"
CRT_GLASS_TARGET = (CRT_GLASS_NAME, CRT_GLASS_NAME, "GLO", "kilogram")
APPROVED_CRT_GLASS_TREATMENTS = {}
for market in (False, True):
    source = (
        f"{'market for' if market else 'treatment of'} {CRT_GLASS_PRODUCT}",
        CRT_GLASS_PRODUCT,
        "GLO",
        "kilogram",
    )
    boundary = (
        "The source market supplies exactly 100% CRT-glass processing plus four freight suppliers. Additional market-level freight is omitted; the proxy retains its own treatment-internal transport model."
        if market
        else "Both source and proxy treatment descriptions include treatment-internal transport; background supplies and transport parameters may differ."
    )
    APPROVED_CRT_GLASS_TREATMENTS[source] = {
        "target": CRT_GLASS_TARGET,
        "previous_target": CRT_SCREEN_PREVIOUS_TARGET,
        "technology": ("CRT-glass processing market" if market else "CRT-glass processing service"),
        "source_versions": CRT_OLDER_VERSIONS + CRT_NEWER_VERSIONS,
        "approval_scope": "similar_case",
        "rationale": f"Auto-approved on 2026-11-05 under the similar-CRT-treatment authorization: replace the screen-manufacturing target for the exact GLO/kg {source[0]} waste reference with the dedicated GLO CRT-glass processing service and factor -1. The source describes crushing, washing and sorting CRT glass; both process descriptions document 100% allocation to disposal and panel/funnel/mixed-glass co-products. Preserve wet waste mass without heating-value or moisture scaling. {boundary}",
        "limitations": f"Approved CRT-glass processing screening proxy: crushing, washing and sorting glass from used CRTs is represented by the dedicated CRT-glass treatment service. Both descriptions document 100% allocation to disposal and recovered panel, funnel and mixed-glass co-products. Recovery yields, glass composition, energy, infrastructure, transport, downstream coating disposal, allocation and consequential credit models may differ; the source-specific credit model is not reconstructed. Downstream metal speciation, leaching and long-term releases are not independently validated. LCIA equivalence, including toxicity impacts, is not established or quantified. Preserve wet waste mass without heating-value or moisture scaling. {boundary} Glass-cullet landfill, whole-screen dismantling, sludge and recovered-glass reference products remain separate.",
        "inventory_evidence": [
            f"Read-only inspection of all 14 exact source/profile inventories for {source[0]} in ecoinvent 3.6--3.12 cut-off/consequential: single self-linked production -1 kg. The direct source documents CRT-glass crushing/washing/sorting and 100% disposal allocation. All 14 markets supply exactly one kilogram of that processing service plus four freight suppliers, without active direct biosphere releases.",
            "Inspected UVEK 2025 bafu-285219: GLO/kg, production +1, native WASTE_FLOW input, active coating disposal, electricity, treatment infrastructure, transport and biosphere resource exchanges. Its multi-output process comment documents 100% allocation to disposal, with panel/funnel/mixed glass as co-products. No complete inventory or reconstruction of downstream metal fate is published.",
        ],
    }
CRT_MANUAL_PRODUCT = "used cathode ray tube display"
CRT_MANUAL_NAME = "Dismantling, CRT screen, manually, at plant"
CRT_MANUAL_TARGET = (CRT_MANUAL_NAME, CRT_MANUAL_NAME, "CH", "kilogram")
APPROVED_CRT_MANUAL_TREATMENTS = {}
for location, versions in (
    ("CH", CRT_OLDER_VERSIONS + CRT_NEWER_VERSIONS),
    ("RoW", CRT_NEWER_VERSIONS),
):
    source = (
        f"treatment of {CRT_MANUAL_PRODUCT}, manual dismantling",
        CRT_MANUAL_PRODUCT,
        location,
        "kilogram",
    )
    APPROVED_CRT_MANUAL_TREATMENTS[source] = {
        "target": CRT_MANUAL_TARGET,
        "previous_target": CRT_SCREEN_PREVIOUS_TARGET,
        "technology": "manual CRT dismantling service",
        "source_versions": versions,
        "approval_scope": "similar_case",
        "rationale": f"Auto-approved on 2026-11-05 under the similar-CRT-treatment authorization: replace the CRT-screen manufacturing target for the exact {location}/kg manual-dismantling waste reference with the inspected CH/kg manual CRT dismantling service and factor -1. Both functions dismantle used CRT screens manually into fractions; no kilogram-to-device conversion is needed. Preserve the original waste mass. Mechanical shredding and mixed-route markets are excluded.",
        "limitations": f"Approved manual CRT dismantling screening proxy: the {location} source is represented by CH manual dismantling using Swiss experience and transfer coefficients. Dismantling performance, recovery yields, fraction composition, downstream metal speciation, treatment of separated fractions, allocation, background supplies and consequential credit models may differ; the source-specific credit model is not reconstructed. LCIA equivalence, including toxicity impacts, is not established or quantified. Preserve the original waste mass. No additional mechanical-shredding stage or market freight is inferred. Mechanical treatment, mixed-route markets, glass-cullet landfill and recovered-material product references remain separate.",
        "inventory_evidence": [
            f"Read-only inspection of all {2 * len(versions)} exact {location}/kg manual-dismantling inventories in ecoinvent {', '.join(versions)}, cut-off and consequential: one self-linked reference production -1 kg, active treatment inputs and documentation describing manual dismantling into fractions. Collection and transport to the treatment facility are excluded in modern source documentation.",
            "Inspected UVEK 2025 bafu-448178: CH/kg manual CRT dismantling into fractions based on transfer coefficients and Swiss experience; production +1, native WASTE_FLOW input, eight active technosphere inputs and no negative technosphere supplier credits. UVEK mechanical treatment explicitly adds shredding after manual dismantling and is outside this approval.",
        ],
    }
CRT_MECHANICAL_NAME = "Dismantling, CRT screen, mechanically, at plant"
CRT_MECHANICAL_TARGET = (CRT_MECHANICAL_NAME, CRT_MECHANICAL_NAME, "GLO", "kilogram")
CRT_SCREEN_MIX_NAME = "xx Disposal, CRT screen, 17 inches, to WEEE treatment"
CRT_SCREEN_MIX_TARGET = (CRT_SCREEN_MIX_NAME, CRT_SCREEN_MIX_NAME, "CH", "unit")
CRT_SCREEN_REFERENCE_MASS_KG = 19.9
CRT_MECHANICAL_SOURCE = (
    f"treatment of {CRT_MANUAL_PRODUCT}, mechanical treatment",
    CRT_MANUAL_PRODUCT,
    "GLO",
    "kilogram",
)
CRT_SCREEN_MARKET_SOURCE = (
    f"market for {CRT_MANUAL_PRODUCT}",
    CRT_MANUAL_PRODUCT,
    "GLO",
    "kilogram",
)
APPROVED_CRT_MECHANICAL_TREATMENTS = {
    CRT_MECHANICAL_SOURCE: {
        "target": CRT_MECHANICAL_TARGET,
        "previous_target": CRT_SCREEN_PREVIOUS_TARGET,
        "technology": "mechanical CRT treatment with an added manual-dismantling boundary",
        "source_versions": CRT_OLDER_VERSIONS + CRT_NEWER_VERSIONS,
        "approval_scope": "explicit",
        "rationale": "Approved on 2026-11-05: replace the GLO/kg screen-manufacturing target for mechanical treatment of used CRT displays with the dedicated GLO/kg mechanical dismantling service and factor -1. Preserve original waste mass. The source describes shredding/separation; UVEK explicitly describes manual dismantling before shredding/separation. This broader boundary is an approved screening approximation.",
        "limitations": "Approved mechanical CRT treatment boundary proxy: the UVEK service explicitly includes manual dismantling before mechanical shredding/separation, whereas all inspected source profiles describe a four-step shredding/separation procedure and have no manual-dismantling service input. The broader proxy may add or double-count manual dismantling where that stage is separately represented. Foreground manual-treatment exchanges are preserved. Recovery yields, fraction composition, downstream metal speciation, allocation, background supplies and consequential credit models may differ; the source-specific credit model is not reconstructed. LCIA equivalence, including toxicity impacts, is not established or quantified. Preserve the original waste mass; no device-count conversion or market freight is inferred for this direct service.",
        "inventory_evidence": [
            "Read-only inspection of all 14 exact GLO/kg mechanical-treatment source inventories in ecoinvent 3.6--3.12 cut-off/consequential: self-linked production -1 kg, active treatment inputs, four-step shredder/separation/shredder/separation documentation, no manual-dismantling service supplier.",
            "Inspected UVEK 2025 bafu-201280: GLO/kg, production +1, native WASTE_FLOW input, five active technosphere inputs, no negative technosphere supplier credits. Its documentation explicitly states manual dismantling plus subsequent shredding/separation. The user approved that broader treatment boundary and similar cases.",
        ],
    },
    CRT_SCREEN_MARKET_SOURCE: {
        "target": CRT_SCREEN_MIX_TARGET,
        "previous_target": CRT_SCREEN_PREVIOUS_TARGET,
        "technology": "23% manual / 77% mechanical CRT treatment market",
        "source_versions": CRT_OLDER_VERSIONS + CRT_NEWER_VERSIONS,
        "approval_scope": "similar_case",
        "target_reference_mass_kg": CRT_SCREEN_REFERENCE_MASS_KG,
        "rationale": "Auto-approved on 2026-11-05 under the similar-CRT-boundary authorization: represent the exact GLO/kg used-CRT-display market by the inspected CH 17-inch screen-disposal mix. Both mixes route 23% of mass to manual dismantling and 77% to mechanical treatment. One proxy unit represents 19.9 kg, so factor -1/19.9 preserves source waste mass and route shares. The mechanical branch retains the separately approved broader manual-plus-mechanical boundary. Additional market-level freight is omitted.",
        "limitations": "Approved CRT-screen market and reference-mass proxy: preserve 23% manual / 77% mechanical treatment shares and original waste mass through one unit per 19.9 kg, with signed factor -1/19.9. This reference mass comes from this exact UVEK mix definition and is not a generic weight assumption for all CRT screens. The mechanical branch explicitly includes manual dismantling before shredding/separation and may add or double-count a separately represented manual stage. Additional market-level freight is omitted: the source market's four freight suppliers are not reproduced. The proxy's CH/manual and GLO/mechanical geography replaces the source manual-treatment geography. Recovery yields, fraction composition, downstream metal speciation, allocation, background supplies and consequential credits may differ; the source-specific credit model is not reconstructed. LCIA equivalence, including toxicity impacts, is not established or quantified. Foreground treatment/freight and recovered-material references remain separate.",
        "inventory_evidence": [
            "Read-only inspection of all 14 exact GLO/kg used-CRT-display markets in ecoinvent 3.6--3.12 cut-off/consequential: production -1 kg, 23% manual treatment (CH through 3.9, RoW from 3.10) and 77% GLO mechanical treatment, four additional freight suppliers and no active direct biosphere releases. The mass shares are stable across all profiles.",
            "Inspected UVEK 2025 bafu-352244: CH/unit, production +1, native WASTE_FLOW input. Its only active inputs are the reviewed manual and mechanical kilogram-based services; their combined reference mass is 19.9 kg and their normalized shares are exactly 23%/77%. No other active technosphere or biosphere inputs occur in this mix. Only the reference-mass basis and shares are retained as conversion evidence, not a background inventory dump.",
        ],
    },
}
APPROVED_CRT_RESIDUE_MARKETS = {}
for location in ("CH", "RoW"):
    source = (
        f"market for {CRT_RESIDUE_PRODUCT}",
        CRT_RESIDUE_PRODUCT,
        location,
        "kilogram",
    )
    APPROVED_CRT_RESIDUE_MARKETS[source] = {
        "target": CRT_RESIDUE_TARGET,
        "previous_target": CRT_RESIDUE_TARGET,
        "technology": "100% incinerated CRT-shredder-residue market",
        "source_versions": CRT_OLDER_VERSIONS + CRT_NEWER_VERSIONS,
        "approval_scope": "similar_case",
        "rationale": f"Auto-approved on 2026-11-05 under the similar-CRT-treatment and market-proxy authorizations: retain the dedicated CH/kg CRT-residue incineration service and existing factor -1 for the exact {location} market. All inspected source profiles supply 100% incineration of the same shredder residue. Additional market-level freight is omitted and disclosed; mass is preserved.",
        "limitations": f"Approved CRT-residue market proxy: the {location} source supplies 100% incineration of the same mixed shredder residues and is represented by the dedicated CH disposal service. Additional market-level freight is omitted: CH has two freight suppliers; RoW has three, eight or nine depending on profile. Emission controls, fly-ash extraction, ash treatment, leaching, metal speciation, recovery yields, energy recovery, allocation, background supplies and consequential credit models may differ; the source-specific credit model is not reconstructed. LCIA equivalence, including toxicity impacts, is not established or quantified. Preserve wet waste mass without heating-value or moisture scaling. Foreground transport and treatment, glass-cullet landfill, sludge and recovered-material references remain separate.",
        "inventory_evidence": [
            f"Read-only inspection of all 14 exact {location}/kg CRT-residue markets in ecoinvent 3.6--3.12 cut-off/consequential: reference production -1 kg, exactly one kilogram of incineration of the same residue plus freight only, no active direct biosphere releases. CH uses the CH fly-ash-extraction service; RoW uses older RoW or newer GLO incineration.",
            "Inspected UVEK 2025 bafu-377324: CH/kg CRT-screen mechanical-treatment residue incineration, production +1, native WASTE_FLOW input and active treatment inputs/emissions. The existing supplier and factor are retained, with newly explicit market-freight and treatment-model disclosures.",
        ],
    }
CRT_CULLET_LANDFILL_NAME = "Disposal, glass, 0% water, to construction waste landfill"
CRT_CULLET_LANDFILL_TARGET = (
    CRT_CULLET_LANDFILL_NAME,
    CRT_CULLET_LANDFILL_NAME,
    "CH",
    "kilogram",
)
CRT_FRIT_PRODUCT = "waste frit from cathode ray tube production"
CRT_FRIT_LANDFILL_NAME = "Disposal, frit for CRT tube production, to residual material landfill"
CRT_FRIT_LANDFILL_TARGET = (
    CRT_FRIT_LANDFILL_NAME,
    CRT_FRIT_LANDFILL_NAME,
    "CH",
    "kilogram",
)
CRT_FRIT_PREVIOUS_TARGET = (
    "Frit, for CRT tube production, at plant",
    "Frit, for CRT tube production, at plant",
    "GLO",
    "kilogram",
)
APPROVED_CRT_LANDFILL_TREATMENTS = {}
for product in (
    "glass cullet, from used cathode ray tube panels",
    "glass cullet, lead containing, from used cathode ray tube",
    "glass cullet, mixed glass from used cathode ray tube",
):
    for market in (False, True):
        name = f"market for {product}" if market else f"treatment of {product}, 0% water, inert material landfill"
        source = (name, product, "GLO", "kilogram")
        explicit = "lead containing" in product
        boundary = (
            "The source market supplies 100% landfill of the same cullet. Additional market-level freight is omitted: four suppliers through 3.9 and one from 3.10. Foreground freight is preserved."
            if market
            else "The source is a direct landfill service; no market-level freight is inferred or added."
        )
        APPROVED_CRT_LANDFILL_TREATMENTS[source] = {
            "target": CRT_CULLET_LANDFILL_TARGET,
            "previous_target": CRT_SCREEN_PREVIOUS_TARGET,
            "technology": "generic-glass landfill composition and leachate-boundary proxy",
            "source_versions": CRT_OLDER_VERSIONS + CRT_NEWER_VERSIONS,
            "approval_scope": "explicit" if explicit else "similar_case",
            "rationale": f"{'Approved' if explicit else 'Auto-approved under the similar-CRT-glass-landfill authorization'} on 2026-11-05: replace screen manufacturing for the exact GLO/kg {name} waste reference with the CH ordinary-glass construction-waste landfill service and factor -1. Preserve waste mass. The source explicitly excludes direct leachate emissions as negligible, while the proxy includes generic-glass metal releases. Composition, geography and this broader emission model are disclosed screening approximations. {boundary}",
            "limitations": f"Approved CRT-cullet landfill screening proxy: {product} is represented by ordinary construction-waste glass in CH at the same kilogram basis. The ecoinvent direct landfill model explicitly excludes direct leachate emissions as negligible; UVEK includes generic-glass metal releases, including chromium, copper, lead and zinc. These are proxy metal releases, not a reconstruction of this cullet's lead or other metal content. Composition, leaching, metal speciation, landfill design, hydrology, infrastructure, energy, allocation and background supplies may differ. LCIA equivalence, including toxicity impacts, is not established or quantified; the proxy may introduce direct metal-emission contributions absent from the source landfill inventory. Preserve the original waste mass without composition or leaching-based scaling. {boundary} Production, separately listed foreground emissions/treatment and recovered-glass references are preserved.",
            "inventory_evidence": [
                f"Read-only inspection of all 14 exact GLO/kg {name} source/profile inventories in ecoinvent 3.6--3.12 cut-off/consequential: one self-linked production -1 kg. All direct cullet landfills document excluded leachate emissions; their only active biosphere exchange in 3.12 is Non-hazardous waste disposed, with no active direct biosphere exchanges in earlier inspected versions. Each market supplies exactly one kilogram of landfill of its own cullet plus freight only, with no active direct biosphere releases.",
                "Inspected UVEK 2025 bafu-506481: CH/kg ordinary construction-waste glass landfill, production +1, verified native WASTE_FLOW input, active infrastructure/energy inputs and 23 active biosphere exchanges, including generic-glass metal releases. Its intended use describes building glass and potentially packaging glass, not a validated lead-specific CRT inventory. No full inventory is copied or equal toxicity assumed.",
            ],
        }
for market, location, versions in (
    (False, "CH", CRT_OLDER_VERSIONS),
    (False, "RoW", CRT_OLDER_VERSIONS),
    (False, "GLO", CRT_NEWER_VERSIONS),
    (True, "GLO", CRT_OLDER_VERSIONS + CRT_NEWER_VERSIONS),
):
    name = (
        f"market for {CRT_FRIT_PRODUCT}" if market else f"treatment of {CRT_FRIT_PRODUCT}, residual material landfill"
    )
    source = (name, CRT_FRIT_PRODUCT, location, "kilogram")
    boundary = (
        "The source market supplies 100% residual-material landfill of the same frit: an older CH/RoW treatment mix and a newer GLO supplier. Additional market-level freight is omitted: four suppliers through 3.9 and one from 3.10."
        if market
        else "The source is a direct residual-material landfill service; no market-level freight is inferred or added."
    )
    APPROVED_CRT_LANDFILL_TREATMENTS[source] = {
        "target": CRT_FRIT_LANDFILL_TARGET,
        "previous_target": (CRT_FRIT_PREVIOUS_TARGET if market else CRT_FRIT_LANDFILL_TARGET),
        "technology": "specific CRT-production frit residual-material landfill service",
        "source_versions": versions,
        "approval_scope": "similar_case",
        "rationale": f"Auto-approved on 2026-11-05 under the similar-CRT-landfill and matching-service authorizations: use the dedicated CH/kg CRT-production frit residual-landfill service for the exact {location}/kg {name} waste reference with factor -1. The market's frit-manufacturing target is replaced; matching direct suppliers and factors are retained. Preserve waste mass. {boundary}",
        "limitations": f"Approved CRT-production frit landfill screening proxy: the {location} source is represented by the dedicated CH frit residual-material landfill service. Both functions treat frit waste and model releases from residual-material landfill; this is separate from the inert-cullet model that excludes direct leachate releases. Waste composition, solidification, hydrology, landfill design, leaching, metal speciation, short/long-term emissions, allocation and background supplies may differ. LCIA equivalence, including toxicity impacts, is not established or quantified. Preserve the original waste mass without composition or leaching-based scaling. {boundary} Foreground transport/emissions and recovered-material references are preserved.",
        "inventory_evidence": [
            f"Read-only inspection of all {2 * len(versions)} exact source/profile inventories for {name} / {location} / kg in ecoinvent {', '.join(versions)}, cut-off and consequential: one self-linked production -1 kg. Direct services have active landfill inputs and metal releases; modern documentation includes solidification and short/long-term leachate emissions. All fourteen market profiles supply residual-landfill treatment totaling 100% of the same frit plus freight only, without active direct biosphere releases.",
            "Inspected UVEK 2025 bafu-466727: CH/kg, production +1, verified native WASTE_FLOW input, dedicated disposal of 100% CRT-production frit in residual material landfill, six active technosphere inputs and ten active biosphere exchanges. Sparse function/sign/boundary evidence is retained; no full source or target inventory is published.",
        ],
    }
WASTEWATER_TARGETS = {
    "hard fibreboard": (
        "Treatment hard fibreboard production effluent, to wastewater treatment, class 1",
        "bafu-193650",
    ),
    "medium density board": (
        "Treatment, medium density fibreboard production effluent, to wastewater treatment, class 1",
        "bafu-414420",
    ),
    "particle board": (
        "Treatment, particle board production effluent, to wastewater treatment, class 1",
        "bafu-397133",
    ),
}
WASTEWATER_COHORTS = (
    (
        "hard fibreboard",
        "hard fibreboard",
        "capacity 5E9l/year",
        ("RER", "RoW"),
        ("3.6", "3.7", "3.8"),
        True,
    ),
    (
        "hard fibreboard",
        "hard fibreboard",
        "wastewater treatment",
        ("RER", "RoW"),
        ("3.9", "3.10", "3.11", "3.12"),
        False,
    ),
    (
        "medium density fibreboard",
        "medium density board",
        "capacity 5E9l/year",
        ("RER", "RoW"),
        ("3.6", "3.7", "3.8"),
        True,
    ),
    (
        "medium density board",
        "medium density board",
        "wastewater treatment",
        ("RER", "RoW"),
        ("3.9", "3.10", "3.11", "3.12"),
        False,
    ),
    (
        "particle board",
        "particle board",
        "capacity 5E9l/year",
        ("RER",),
        ("3.6",),
        True,
    ),
    (
        "particleboard",
        "particleboard",
        "capacity 5E9l/year",
        ("RER",),
        ("3.7", "3.8"),
        True,
    ),
    (
        "particle board",
        "particle board",
        "wastewater treatment",
        ("RER",),
        ("3.9", "3.10", "3.11", "3.12"),
        False,
    ),
)
APPROVED_WASTEWATER_TREATMENTS = {}
for (
    process,
    material,
    technology,
    locations,
    versions,
    includes_sewer,
) in WASTEWATER_COHORTS:
    target_name, target_code = WASTEWATER_TARGETS[material.replace("particleboard", "particle board")]
    target = (target_name, target_name, "RER", "cubic meter")
    for location in locations:
        source = (
            f"treatment of wastewater from {process} production, {technology}",
            f"wastewater from {material} production",
            location,
            "cubic meter",
        )
        sewer_boundary = (
            "Both inspected source and proxy include sewer infrastructure."
            if includes_sewer
            else "The UVEK service includes sewer infrastructure which the source treatment places in its upstream market; sewer burdens may be double-counted where separately represented."
        )
        APPROVED_WASTEWATER_TREATMENTS[source] = {
            "target": target,
            "previous_target": target,
            "technology": "class-1 wastewater treatment service",
            "source_versions": versions,
            "native_product_service": True,
            "source_includes_sewer": includes_sewer,
            "rationale": f"Approved on 2026-11-04, including independently inspected similar cases: retain the {material} effluent treatment service for the exact source {source[0]} / {location} / cubic meter, with factor -1. The native UVEK PRODUCT_FLOW output is a volume of wastewater treatment, not a recovered material. Wastewater type and quantity basis agree; process-boundary, treatment-model and geographic approximations are explicitly accepted. Wastewater markets and different effluent types are excluded.",
            "limitations": f"Approved wastewater-service screening proxy: {sewer_boundary} The {location} source is represented by the RER class-1 service using Swiss infrastructure. Plant capacity, treatment-stage mix, wastewater composition, pollutant removal, sludge fate, energy recovery, allocation and background supplies may differ; LCIA equivalence is not established or quantified.",
            "inventory_evidence": [
                f"Inspected every available exact source in ecoinvent {', '.join(versions)}, cut-off and consequential: Waste reference production -1 cubic meter. Active sewer inputs {'are present in the source' if includes_sewer else 'are absent from the source; its documentation assigns sewering to the upstream market'}.",
                f"Inspected UVEK 2025 {target_code}: RER, production +1 cubic meter, matching {material} effluent treatment, active class-1 sewer and wastewater-treatment-plant inputs. Independent native metadata declares PRODUCT_FLOW with an output reference; the inspected physical function is a wastewater-treatment service.",
                "UVEK class-1 infrastructure documents 47.1 million cubic meters/year. Later ecoinvent regional treatment mixes, earlier capacity-5E9l/year models, and sewer boundary conventions are distinct approximations; no impact-equivalence claim is made.",
            ],
        }
for material in ("hard fibreboard", "medium density board"):
    target_name, target_code = WASTEWATER_TARGETS[material]
    target = (target_name, target_name, "RER", "cubic meter")
    source = (
        f"market for wastewater from {material} production",
        f"wastewater from {material} production",
        "GLO",
        "cubic meter",
    )
    APPROVED_WASTEWATER_TREATMENTS[source] = {
        "target": target,
        "previous_target": target,
        "technology": "100% treated wastewater market",
        "source_versions": ("3.6", "3.7", "3.8"),
        "native_product_service": True,
        "source_includes_sewer": True,
        "rationale": f"Auto-approved on 2026-11-04 under the similar wastewater-service and geographic-proxy authorizations: represent the exact GLO, cubic meter {material} wastewater market with its inspected RER class-1 effluent treatment service and factor -1. All six available source profiles supply 100% treatment of the same wastewater with no direct untreated emissions. Newer partially untreated markets are excluded.",
        "limitations": "Approved 100% treated wastewater-market screening proxy: Both inspected source and proxy include sewer infrastructure through treatment services. The global treatment-supplier geography mix is replaced by the RER class-1 service using Swiss infrastructure. Plant capacity, wastewater composition, pollutant removal, sludge fate, energy recovery, allocation and background supplies may differ; LCIA equivalence is not established or quantified.",
        "inventory_evidence": [
            f"Inspected the exact GLO {material} wastewater market in ecoinvent 3.6, 3.7 and 3.8, cut-off and consequential: Waste reference -1 cubic meter, two geographic treatment suppliers for the same effluent, treatment shares sum to 100%, no active direct biosphere emissions. Their inspected older treatment suppliers include sewer infrastructure.",
            f"Inspected UVEK 2025 {target_code}: matching {material} effluent, RER, +1 cubic meter; active class-1 sewer and wastewater-treatment infrastructure; native PRODUCT_FLOW output represents the treatment service. No untreated source discharges are being discarded in this approved older market cohort.",
        ],
    }
APPROVED_PARTIALLY_UNTREATED_MARKETS = {}
for material, locations in (
    ("hard fibreboard", ("RER", "RoW")),
    ("medium density board", ("RER", "RoW")),
    ("particle board", ("RER",)),
):
    target_name, target_code = WASTEWATER_TARGETS[material]
    target = (target_name, target_name, "RER", "cubic meter")
    for location in locations:
        treated, untreated = (87.3, 12.7) if location == "RER" else (41.3, 58.7)
        source = (
            f"market for wastewater from {material} production",
            f"wastewater from {material} production",
            location,
            "cubic meter",
        )
        APPROVED_PARTIALLY_UNTREATED_MARKETS[source] = {
            "target": target,
            "previous_target": target,
            "technology": "full-treatment approximation of a partially untreated wastewater market",
            "source_versions": ("3.9", "3.10", "3.11", "3.12"),
            "native_product_service": True,
            "source_includes_sewer": True,
            "rationale": f"Approved on 2026-11-04, including independently inspected similar cases: represent the exact {location}, cubic meter {material} wastewater market by its matching RER UVEK treatment service with factor -1 for the full source volume. The source market treats about {treated}% and directly discharges the untreated remainder; this explicitly accepted approximation represents 100% treatment and omits those untreated-discharge emissions. Different effluent types remain outside this approval.",
            "limitations": f"Approved full-treatment market screening proxy: the {location} source treats about {treated}% of the wastewater and has about {untreated}% untreated discharge. The UVEK proxy represents 100% treatment of the full source volume; untreated-discharge emissions are omitted. This may understate water-pollution impacts and overstate treatment burdens. Both inspected source and proxy include sewer infrastructure; regional sewer and treatment mixes are replaced by the RER class-1 service using Swiss infrastructure. Wastewater composition, pollutant removal, sludge fate, energy recovery, allocation and background supplies may differ; LCIA equivalence is not established or quantified.",
            "inventory_evidence": [
                f"Inspected the exact {material} wastewater market in {location} in all eight ecoinvent 3.9--3.12 cut-off/consequential profiles: Waste reference -1 cubic meter, about {treated}% treatment from suppliers of the same effluent, active sewer inputs and direct biosphere releases associated with the untreated remainder. Rounded shares are stable across the inspected profiles; no complete pollutant inventory is retained.",
                f"Inspected UVEK 2025 {target_code}: matching {material} effluent service, RER, +1 cubic meter, active class-1 sewer and treatment infrastructure. Native PRODUCT_FLOW output represents treatment. Factor -1 converts reference coordinates only; the approved proxy uses the full volume, without scaling by the source treated share or reconstructing untreated emissions.",
            ],
        }
APPROVED_WASTEWATER_TREATMENTS.update(APPROVED_PARTIALLY_UNTREATED_MARKETS)
GENERIC_WASTEWATER_TARGET = (
    "Treatment, sewage, to wastewater treatment, class 1",
    "Treatment, sewage, to wastewater treatment, class 1",
    "CH",
    "cubic meter",
)
APPROVED_GENERIC_WASTEWATER = {}
for process, cod in (
    ("PET pelletising", 0.373),
    ("recycling of waste PET", 0.213),
    ("recycling of waste PP", 0.425),
):
    for market in (False, True):
        source = (
            (
                f"market for wastewater from {process}"
                if market
                else f"treatment of wastewater from {process}, wastewater treatment"
            ),
            f"wastewater from {process}",
            "RER",
            "cubic meter",
        )
        previous_name, previous_location = (
            (
                "Treatment, particle board production effluent, to wastewater treatment, class 1",
                "RER",
            )
            if process == "PET pelletising"
            else (
                (
                    "Treatment, soft fibreboard production effluent, to wastewater treatment, class 1",
                    "RER",
                )
                if market
                else (
                    "Treatment, sewage, from residence, to wastewater treatment, class 2",
                    "CH",
                )
            )
        )
        boundary = (
            "Both inspected source and proxy include sewer infrastructure. The source market treats about 87.3% of its volume; the proxy represents 100% treatment of the full source volume and untreated-discharge emissions are omitted. This may understate water-pollution impacts and overstate treatment burdens."
            if market
            else "The UVEK service adds sewer infrastructure which the source treatment places in its upstream market; sewer burdens may be double-counted where separately represented."
        )
        APPROVED_GENERIC_WASTEWATER[source] = {
            "target": GENERIC_WASTEWATER_TARGET,
            "previous_target": (
                previous_name,
                previous_name,
                previous_location,
                "cubic meter",
            ),
            "technology": "generic class-1 sewage treatment composition proxy",
            "source_versions": ("3.11", "3.12"),
            "source_includes_sewer": market,
            "source_cod_kg_per_m3": cod,
            "rationale": f"Approved on 2026-11-04 for PET-pelletising wastewater and auto-approved for independently inspected similar plastic-recycling cases: replace the exact RER, cubic meter {source[0]} correspondence with generic CH class-1 sewage treatment and factor -1. Preserve wastewater volume; source COD {cod} kg/m3 and proxy COD 0.1556 kg/m3 are not equivalent. Market coverage and sewer boundaries are disclosed separately. Textile wastewater, landfill leachate and direct untreated-disposal activities are outside this approval.",
            "limitations": f"Approved generic wastewater composition screening proxy: plastic-processing wastewater with COD {cod} kg/m3 is represented by generic CH municipal sewage treatment with COD 0.1556 kg/m3. The full wastewater volume is preserved without COD-based scaling. Pollutant concentrations, fossil/biogenic carbon fate, sludge treatment, treatment performance, energy recovery, allocation and background supplies may differ; LCIA equivalence is not established or quantified. {boundary}",
            "inventory_evidence": [
                f"Inspected the exact RER source in ecoinvent 3.11 and 3.12, cut-off and consequential: Waste reference -1 cubic meter. The corresponding {process} treatment documentation reports COD {cod} kg/m3. Direct treatment excludes sewering; market suppliers treat about 87.3%, with active sewer inputs and direct untreated releases.",
                "Inspected UVEK 2025 bafu-299668: CH, +1 cubic meter, generic municipal wastewater treatment, class 1, documented COD 0.1556 kg/m3 and active sewer infrastructure. Independently verified native WASTE_FLOW input reference. No source-specific pollutant inventory is copied or inferred from COD.",
            ],
        }
for location in ("RER", "RoW"):
    for market in (False, True):
        source = (
            (
                "market for wastewater from textile production"
                if market
                else "treatment of wastewater from textile production, wastewater treatment"
            ),
            "wastewater from textile production",
            location,
            "cubic meter",
        )
        previous_name, previous_location = (
            (
                "Treatment, particle board production effluent, to wastewater treatment, class 1",
                "RER",
            )
            if location == "RER"
            else (
                "Treatment, effluent from NF3 production, to wastewater treatment, class 3",
                "CH",
            )
        )
        treated = 87.3 if location == "RER" else 41.3
        boundary = (
            f"Both inspected source and proxy include sewer infrastructure. The source market treats about {treated}% of its volume; the proxy represents 100% treatment of the full source volume and untreated-discharge emissions are omitted. This may understate water-pollution impacts and overstate treatment burdens."
            if market
            else "The UVEK service adds sewer infrastructure which the source treatment places in its upstream market; sewer burdens may be double-counted where separately represented."
        )
        APPROVED_GENERIC_WASTEWATER[source] = {
            "target": GENERIC_WASTEWATER_TARGET,
            "previous_target": (
                previous_name,
                previous_name,
                previous_location,
                "cubic meter",
            ),
            "technology": "generic class-1 sewage treatment composition proxy",
            "source_versions": ("3.11", "3.12"),
            "source_includes_sewer": market,
            "source_cod_kg_per_m3": 0.93446,
            "rationale": f"Auto-approved on 2026-11-04 under the generic wastewater composition-proxy authorization: replace the exact {location}, cubic meter {source[0]} correspondence with generic CH class-1 sewage treatment and factor -1. The inspected modern source uses municipal wastewater treatment. Source COD 0.93446 kg/m3 is about six times the proxy's 0.1556 kg/m3; this is an explicitly disclosed composition approximation, not impact equivalence. Fully untreated direct-disposal activities and markets remain outside this approval.",
            "limitations": f"Approved generic wastewater composition screening proxy: textile wastewater with COD 0.93446 kg/m3 is represented by generic CH municipal sewage treatment with COD 0.1556 kg/m3. The full wastewater volume is preserved without COD-based scaling. Pollutant concentrations, fossil/biogenic carbon fate, sludge treatment, treatment performance, energy recovery, allocation and background supplies may differ; LCIA equivalence is not established or quantified. The {location} source geography and regional treatment mix are approximated by CH class-1 service. {boundary}",
            "inventory_evidence": [
                f"Inspected the exact {location} source in ecoinvent 3.11 and 3.12, cut-off and consequential: Waste reference -1 cubic meter. The associated treatment documentation describes municipal WWT, COD 0.93446 kg/m3 and 35% biogenic carbon. Direct treatment excludes sewering; the market includes sewer inputs, treats about {treated}% and directly releases the untreated remainder.",
                "Inspected UVEK 2025 bafu-299668: CH, +1 cubic meter, municipal wastewater treatment class 1, documented COD 0.1556 kg/m3, active sewer infrastructure, verified native WASTE_FLOW input. No source-specific pollutant recipe or COD-based impact multiplier is introduced.",
            ],
        }
# Auto-approval authorizes scientific review of similar cases, not fuzzy matching.
# Every executable substitution is enumerated after inspecting its inventories.
APPROVED_WASTE_TREATMENTS = {
    **LCD_TREATMENTS,
    **DISPOSALS,
    **LANDFILLS,
    **FLARES,
    **NATURAL_GAS_FLARES,
    **APPROVED_DISCHARGE_TREATMENTS,
    **APPROVED_BATTERY_TREATMENTS,
    **APPROVED_GEOGRAPHIC_TREATMENTS,
    **APPROVED_INCINERATION_COMPOSITION_PROXIES,
    **APPROVED_CRT_INCINERATION_TREATMENTS,
    **APPROVED_CRT_GLASS_TREATMENTS,
    **APPROVED_CRT_MANUAL_TREATMENTS,
    **APPROVED_CRT_MECHANICAL_TREATMENTS,
    **APPROVED_CRT_RESIDUE_MARKETS,
    **APPROVED_CRT_LANDFILL_TREATMENTS,
    **APPROVED_WASTEWATER_TREATMENTS,
    **APPROVED_GENERIC_WASTEWATER,
}
OMISSION_PREVIOUS_TARGET = (
    "Treatment, effluent from NF3 production, to wastewater treatment, class 3",
    "Treatment, effluent from NF3 production, to wastewater treatment, class 3",
    "CH",
    "cubic meter",
)
APPROVED_TECHNOSPHERE_OMISSIONS = {
    (name, "wastewater from textile production", location, "cubic meter"): {
        "source_versions": (
            ("3.6", "3.7", "3.8", "3.9", "3.10", "3.11")
            if location == "GLO" and name.startswith("direct")
            else (("3.6", "3.7", "3.8", "3.9", "3.10") if location == "GLO" else ("3.12",))
        ),
        "review": {
            "rationale": f"Approved omission: {name} / {location} / cubic meter represents fully untreated wastewater discharge. The inspected source has no wastewater-treatment process; its market supplies only direct disposal. Omit the exchange instead of substituting NF3 or municipal wastewater treatment, and continue conversion with a warning.",
            "limitations": [
                "The omitted exchange's untreated-discharge impacts are absent from the target dataset and may understate its score. Separately listed foreground emissions are preserved. The source exchange is retained in the migration loss report; no reverse reconstruction is inferred."
            ],
            "evidence": [
                "Read-only inspection of all 30 available source/profile pairs: GLO direct disposal in 3.6--3.11, GLO markets in 3.6--3.10, BD/IN direct disposal and markets in 3.12, cut-off and consequential. Direct disposal has sewer inputs and direct emissions but no treatment supplier; each market supplies 100% direct disposal.",
                f"User instruction: omit the {location} exchange from the target dataset with a warning instead of blocking conversion. Exact identity and prior NF3 supplier are audited in {REVIEW_PATH}.",
            ],
        },
    }
    for name in (
        "direct disposal of wastewater from textile production",
        "market for wastewater from textile production",
    )
    for location in ("GLO", "BD", "IN")
}
BIOWASTE_SOURCE = (
    "Biowaste, at collection point",
    "Biowaste, at collection point",
    "CH",
    "kilogram",
)
APPROVED_COMPOSTING_OMISSIONS = {
    (
        "treatment of biowaste, industrial composting",
        "biowaste",
        location,
        "kilogram",
    ): {
        "source_versions": ("3.6", "3.7", "3.8", "3.9", "3.10", "3.11", "3.12"),
        "previous_target": BIOWASTE_SOURCE,
        "previous_native_flow_type": "PRODUCT_FLOW",
        "review": {
            "rationale": f"Approved on 2026-11-05: omit industrial composting of biowaste / {location} / kilogram and continue conversion with a warning. The former Biowaste, at collection point target supplies collected material, not composting treatment. UVEK Composting organic waste (RER) is an empty cut-off placeholder, not an inventory of treatment burdens. Composting energy, machinery, infrastructure, residual-waste handling and emissions including methane, nitrous oxide and ammonia are omitted from the target dataset.",
            "limitations": [
                "The composting contribution is excluded rather than approximated by collection or silently assigned a zero-impact treatment service. Effects on impact scores are method-dependent and unquantified. Separately listed foreground emissions and other inputs are preserved; the original exchange is retained in the loss report. No reverse reconstruction is inferred."
            ],
            "evidence": [
                f"Exact {location}, kilogram biowaste treatment reference has production -1 in all 14 inspected ecoinvent 3.6--3.12 cut-off/consequential profiles. Read-only inspection of the 3.12 cut-off CH and RoW treatment inventories identifies energy, machinery, infrastructure, residual-waste treatment and active biosphere emissions.",
                "Read-only inspection of UVEK 2025 bafu-509472: Composting organic waste, RER, kilogram, production +1; no technosphere or biosphere exchanges. Its source comment explicitly describes an empty process due to the cut-off at recycling/composting. Native WASTE_FLOW input metadata alone does not establish treatment coverage.",
                f"User explicitly approved omission of these two exact mappings on 2026-11-05. The original collected-material target and source sign observations remain audited in {REVIEW_PATH}.",
            ],
        },
    }
    for location in ("CH", "RoW")
}
APPROVED_TECHNOSPHERE_OMISSIONS.update(APPROVED_COMPOSTING_OMISSIONS)
CRT_PAPER_SLUDGE_NAME = "Disposal, sludge from pulp and paper production, 25% water, to sanitary landfill"
CRT_PAPER_SLUDGE_TARGET = (
    CRT_PAPER_SLUDGE_NAME,
    CRT_PAPER_SLUDGE_NAME,
    "CH",
    "kilogram",
)
CRT_IRON_SLUDGE_NAME = "Disposal, sludge from FeCl3 production, 30% water, to underground deposit"
CRT_IRON_SLUDGE_TARGET = (CRT_IRON_SLUDGE_NAME, CRT_IRON_SLUDGE_NAME, "DE", "kilogram")
CRT_RAW_SLUDGE_NAME = "Disposal, raw sewage sludge, to municipal incineration"
CRT_RAW_SLUDGE_TARGET = (CRT_RAW_SLUDGE_NAME, CRT_RAW_SLUDGE_NAME, "CH", "kilogram")
CRT_SLAG_COMPONENT_NAME = "Process-specific burdens, slag compartment"
CRT_SLAG_COMPONENT_TARGET = (
    CRT_SLAG_COMPONENT_NAME,
    CRT_SLAG_COMPONENT_NAME,
    "CH",
    "kilogram",
)
APPROVED_CRT_WASTEWATER_RESIDUE_OMISSIONS = {}


def crt_wastewater_residue_omission(source, previous_target, route, *, explicit=False):
    """Record only exact inspected industrial wastewater-residue omissions."""
    market = source[0].startswith("market for")
    impacts = {
        "sludge landfill": "sanitary-landfill burdens, leachate, landfill-gas emissions and gas/energy-recovery contributions",
        "sludge incineration": "sludge-incineration burdens, metal/emission releases, ash handling and energy-recovery contributions",
        "sludge landfarming": "landfarming emissions, nutrient/fertilizer functions and their associated credits",
        "bottom-ash landfill": "slag-compartment burdens, metal leaching and landfill infrastructure/operation contributions",
        "residue landfill": "residual-material-landfill burdens, metal leaching and landfill infrastructure/operation contributions",
    }[route]
    material = (
        "The inspected industrial CRT-production wastewater sludge has 0% biogenic carbon. Its 70%, 75% or 97% water content and composition are not represented by municipal sewage sludge, paper-production sludge, iron-chloride sludge or frit manufacturing. Paper/municipal proxies contain non-fossil carbon flows and unrelated feedstocks; moisture scaling alone would not establish equivalence."
        if route.startswith("sludge")
        else "The inspected ash/residue comes from incinerating CRT-production wastewater sludge. Frit manufacturing, frit landfill, mechanical CRT-screen residue incineration and generic process-specific slag burdens do not establish a complete matching treatment inventory for this industrial wastewater residue."
    )
    boundary = (
        "The source market supplies treatment of this exact residue plus freight only; additional market freight is omitted too."
        if market
        else "This omits an external direct-treatment exchange; separately listed foreground emissions and treatment are preserved."
    )
    return {
        "source_versions": ("3.9", "3.10", "3.11", "3.12"),
        "previous_target": previous_target,
        "previous_native_flow_type": ("PRODUCT_FLOW" if previous_target == CRT_FRIT_PREVIOUS_TARGET else "WASTE_FLOW"),
        "approval_scope": "explicit" if explicit else "similar_case",
        "route": route,
        "review": {
            "rationale": f"{'Approved' if explicit else 'Auto-approved under the similar unsupported-CRT-wastewater-residue omission authorization'} on 2026-11-05: omit the exact {source[0]} / {source[2]} / kilogram exchange and continue conversion with a warning. {material} No suitable UVEK counterpart was established. Omitted contributions include {impacts}. Any associated credits are excluded too; score effects are method-dependent and unquantified. {boundary}",
            "limitations": [
                f"The omitted {route} contributions and any associated credits are absent from the target dataset; effects on scores are method-dependent and unquantified. This is an explicit information loss, not a zero-impact or scientifically equivalent treatment. The complete original exchange, amount, uncertainty, formula and other metadata remain in the loss report. Caller-owned data, production, matching foreground suppliers and separately listed foreground emissions are preserved. {boundary} No reverse reconstruction or wider omission by name/keyword is inferred."
            ],
            "evidence": [
                f"Read-only inspection of all eight exact source/profile inventories for {source[0]} / {source[2]} / kg in ecoinvent 3.9--3.12 cut-off/consequential: single self-linked production -1 kg and active treatment inputs. Direct services have active biosphere releases; every market supplies 100% treatment of its own residue plus freight only, with no active direct biosphere releases. This batch inspects all 144 source/profile pairs for 18 identities; all 40 direct-sludge pairs document 0% biogenic carbon.",
                "Inspected the exact prior supplier's native reference role and the UVEK 2025 catalog. Available candidates represent municipal/paper/refinery/iron-chloride/other-industry sludge, CRT frit or screen-recycling residues, or a process-specific landfill component; none establishes a complete matching inventory for these CRT-production wastewater residues. The prior pulp/paper sludge provider is at 25% water and has non-fossil CO2/methane flows. Sparse prior-target/sign/function evidence is retained without publishing complete inventories.",
                f"User explicitly approved the GLO 75%-water sanitary-landfill sludge treatment and market omissions and authorized similar omissions. Exact prior targets, factors and source-profile observations are retained in {REVIEW_PATH}.",
            ],
        },
    }


for water, route, bindings in (
    (
        70,
        "sludge incineration",
        (
            (False, "GLO", "municipal incineration", CRT_RAW_SLUDGE_TARGET),
            (False, "CH", "municipal incineration FAE", CRT_RAW_SLUDGE_TARGET),
            (True, "CH", "", CRT_PAPER_SLUDGE_TARGET),
            (True, "RoW", "", CRT_IRON_SLUDGE_TARGET),
        ),
    ),
    (
        75,
        "sludge landfill",
        (
            (False, "GLO", "sanitary landfill", CRT_PAPER_SLUDGE_TARGET),
            (True, "GLO", "", CRT_FRIT_PREVIOUS_TARGET),
        ),
    ),
    (
        97,
        "sludge landfarming",
        (
            (False, "CH", "landfarming", CRT_PAPER_SLUDGE_TARGET),
            (False, "RoW", "landfarming", CRT_PAPER_SLUDGE_TARGET),
            (True, "CH", "", CRT_PAPER_SLUDGE_TARGET),
            (True, "RoW", "", CRT_PAPER_SLUDGE_TARGET),
        ),
    ),
):
    product = f"sewage sludge, {water}% water, WWT, WW from CRT production"
    for market, location, technology, previous_target in bindings:
        name = f"market for {product}" if market else f"treatment of {product}, {technology}"
        source = (name, product, location, "kilogram")
        APPROVED_CRT_WASTEWATER_RESIDUE_OMISSIONS[source] = crt_wastewater_residue_omission(
            source, previous_target, route, explicit=water == 75
        )
for material, tag, location, technology, market_target, direct_target in (
    (
        "bottom ash",
        "MSWI-WWT",
        "GLO",
        "slag compartment",
        CRT_FRIT_PREVIOUS_TARGET,
        CRT_FRIT_PREVIOUS_TARGET,
    ),
    (
        "bottom ash",
        "MSWI[F]-WWT",
        "CH",
        "slag compartment",
        CRT_FRIT_LANDFILL_TARGET,
        CRT_SLAG_COMPONENT_TARGET,
    ),
    (
        "residues",
        "MSWI-WWT",
        "GLO",
        "residual material landfill",
        CRT_FRIT_PREVIOUS_TARGET,
        CRT_FRIT_LANDFILL_TARGET,
    ),
    (
        "residues",
        "MSWI[F]-WWT",
        "CH",
        "residual material landfill",
        CRT_RESIDUE_TARGET,
        CRT_FRIT_LANDFILL_TARGET,
    ),
):
    product = f"{material}, {tag}, WW from CRT production"
    for market, previous_target in ((True, market_target), (False, direct_target)):
        name = f"market for {product}" if market else f"treatment of {product}, {technology}"
        source = (name, product, location, "kilogram")
        route = "bottom-ash landfill" if material == "bottom ash" else "residue landfill"
        APPROVED_CRT_WASTEWATER_RESIDUE_OMISSIONS[source] = crt_wastewater_residue_omission(
            source, previous_target, route
        )
APPROVED_TECHNOSPHERE_OMISSIONS.update(APPROVED_CRT_WASTEWATER_RESIDUE_OMISSIONS)
APPROVED_TECHNOSPHERE_OMISSIONS.update(APPROVED_ANTIFOULING_OMISSIONS)
APPROVED_TECHNOSPHERE_OMISSIONS.update(APPROVED_WWT_RESIDUE_OMISSIONS)
BIOWASTE_PREVIOUS_TARGET = ("market for biowaste", "biowaste", "CH", "kilogram")
APPROVED_REVERSE_OMISSIONS = {
    BIOWASTE_SOURCE: {
        "review": {
            "rationale": "Approved reverse omission: Biowaste, at collection point (CH, kilogram) supplies collected material, while ecoinvent market for biowaste supplies waste treatment. Omit the input and continue conversion with a warning; collection transport and atmospheric CO2 uptake contributions are also omitted.",
            "limitations": [
                "The source contains two collection/transport inputs and an atmospheric CO2 uptake flow. These contributions and the material input are not represented in the target dataset; score changes are method-dependent and unquantified. The original exchange is retained in the migration loss report. No waste-treatment substitution or sign inversion is inferred."
            ],
            "evidence": [
                "Read-only inspection of UVEK 2025 bafu-405466: CH, kilogram, reference production +1, native PRODUCT_FLOW output, two active transport suppliers and Carbon dioxide, in air as a resource input. Its consumers use the collected material as an input.",
                "Read-only inspection of ecoinvent 3.12 cut-off 941c2456a8793ea97e14aeeb4bb99bc6: market for biowaste / biowaste / CH / kilogram, Waste reference -1, suppliers for composting, anaerobic digestion, incineration and transport. This is not the same collection/material-supply function.",
                f"User-approved omission of this reverse correspondence with an explicit warning about omitted collection transport and CO2 uptake; original publisher correspondence is retained for audit in {REVIEW_PATH}.",
            ],
        },
    },
}


def identity(record):
    """Return an exact supplier identity, without label-based sign inference."""
    return tuple(record[field] for field in FIELDS)


def rule_binding(rule):
    """Bind evidence to the exact source and target, independently of review prose."""
    value = [rule["source"], rule["target"]]
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False).encode()).hexdigest()


def reviewed_reference_mass_factor(source, target, original, reference_mass):
    """Convert only the inspected kilogram-to-service-unit reference basis."""
    if (
        source[3] != "kilogram"
        or target[3] != "unit"
        or original != 1
        or isinstance(reference_mass, bool)
        or not isinstance(reference_mass, (int, float))
        or not math.isfinite(reference_mass)
        or reference_mass <= 0
    ):
        raise ValueError("Reviewed reference-mass conversion changed; repeat the scientific review.")
    return -1 / reference_mass


def approved_supplier_rule(rule):
    """Project independently reviewed supplier corrections on an owned copy."""
    rule = approved_slag_landfill_component_rule(rule)
    rule = approved_stainless_stock_rule(rule)
    rule = approved_sewer_grid_rule(rule)
    rule = approved_copper_cathode_rule(rule)
    rule = approved_concrete_grade_rule(rule)
    rule = approved_transport_rule(rule)
    rule = approved_whole_freight_rule(rule)
    rule = approved_regional_freight_rule(rule)
    rule = approved_older_regional_road_rule(rule)
    rule = approved_cutoff_feed_product_rule(rule)
    credit = APPROVED_RECOVERY_CREDITS.get(identity(rule["source"]))
    if credit:
        if identity(rule["target"]) not in (
            credit["previous_target"],
            credit["target"],
        ):
            raise ValueError("Recovery-credit supplier changed after approval.")
        return {**rule, "target": dict(zip(FIELDS, credit["target"], strict=True))}
    approval = APPROVED_WASTE_TREATMENTS.get(identity(rule["source"]))
    if approval is None:
        return rule
    if identity(rule["target"]) not in (
        approval.get("previous_target", BATTERY_PREVIOUS_TARGET),
        approval["target"],
    ):
        raise ValueError("Waste-treatment supplier changed after the scoped approval.")
    return {**rule, "target": dict(zip(FIELDS, approval["target"], strict=True))}


def model_specific_review(observations, evidence, previous=()):
    """Scope the approved model-dependent cohort without approving supplier substitutions."""
    by_model = defaultdict(set)
    for profile, sign in observations:
        by_model[profile.rsplit("-", 1)[1]].add(sign)
    if set().union(*by_model.values()) != {-1, 1} or any(len(signs) != 1 for signs in by_model.values()):
        return None
    prior = {
        tuple(entry["source_profile"][field] for field in ("family", "version", "system_model")): entry
        for entry in previous
    }
    profiles = []
    for profile, sign in sorted(observations):
        family, version, system_model = profile.split("-")
        profiles.append(
            {
                "source_profile": {
                    "family": family,
                    "version": version,
                    "system_model": system_model,
                },
                "review": {
                    "status": "unresolved",
                    "reason": f"The observed {profile} reference sign is {sign:+d}. The supplier substitution still requires separate scientific approval; a negative reference alone does not establish a waste-treatment function.",
                    "evidence": list(evidence),
                    "source_sign": sign,
                    "target_sign": 1,
                },
            }
        )
        approved = prior.get((family, version, system_model))
        if approved and approved["review"]["status"] == "verified":
            if approved["review"]["source_sign"] != sign or approved["review"]["target_sign"] != 1:
                raise ValueError(f"Reference signs changed after profile approval: {profile}")
            profiles[-1] = deepcopy(approved)
    return profiles


def coal_tar_review(observations, evidence):
    """Apply the approved positive-reference scope; retain older consequential blocks."""
    expected = {
        f"ecoinvent-{version}-{model}": (
            -1 if model == "consequential" and version in {"3.6", "3.7", "3.8", "3.9"} else 1
        )
        for version in ("3.6", "3.7", "3.8", "3.9", "3.10", "3.11", "3.12")
        for model in ("cutoff", "consequential")
    }
    if dict(observations) != expected:
        raise ValueError("Coal-tar reference observations changed; repeat the scoped supplier review.")
    profiles = []
    for profile, sign in sorted(observations):
        family, version, system_model = profile.split("-")
        entry = {
            "source_profile": {
                "family": family,
                "version": version,
                "system_model": system_model,
            },
            "review": {
                "status": "verified" if sign == 1 else "unresolved",
                "reason": (
                    COAL_TAR_LIMITATIONS
                    if sign == 1
                    else "Consequential 3.6–3.9 coal-tar coproduct/credit modelling remains pending scientific approval; no sign inversion is authorised."
                ),
                "evidence": list(evidence),
                "source_sign": sign,
                "target_sign": 1,
            },
        }
        if sign == 1:
            entry["conversion_factor"] = 1.0
        profiles.append(entry)
    return profiles


def profile_omission_profiles(source, observations, evidence, previous=()):
    """Approve only inspected omissions while preserving independent production decisions."""
    if source == COAL_TAR_SOURCE:
        profiles = coal_tar_review(observations, evidence)
    else:
        if sorted(observations) != PROFILE_OMISSION_OBSERVATIONS[source]:
            raise ValueError("Profile omission coverage changed; repeat the scoped omission review.")
        profiles = model_specific_review(observations, evidence, previous) or [
            {
                "source_profile": {
                    "family": label.split("-")[0],
                    "version": label.split("-")[1],
                    "system_model": label.split("-")[2],
                },
                "review": {
                    "status": "unresolved",
                    "reason": "This exact source profile requires an independent action approval.",
                    "evidence": list(evidence),
                    "source_sign": sign,
                    "target_sign": 1,
                },
            }
            for label, sign in sorted(observations)
        ]
    review = APPROVED_PROFILED_OMISSIONS[source]
    reference = intern_review({}, review)
    for entry in profiles:
        label = "-".join(entry["source_profile"][field] for field in ("family", "version", "system_model"))
        approved_profiles = PROFILE_OMISSION_APPROVED_PROFILES.get(source)
        if (approved_profiles is not None and label not in approved_profiles) or (
            approved_profiles is None and entry["review"]["source_sign"] != -1
        ):
            continue
        if source in APPROVED_STEEL_RECOVERY_OMISSIONS:
            reason = "Approved omission of the waste-to-recyclable handoff and its consequential recovery credits; no allocated slag-production proxy or sign inversion is applied."
        elif source in APPROVED_FEED_SUBSTITUTION_OMISSIONS:
            reason = "Approved omission of nutritional feed-substitution contributions and credits; no unvalidated crop-mass proxy or sign inversion is applied."
        elif source in APPROVED_NUTRIENT_REFERENCE_OMISSIONS:
            reason = "Approved omission of an unvalidated normalized nutritional reference; positive coordinates do not establish equivalent feed-energy or crude-protein supply."
        else:
            reason = "Approved omission of combustion treatment and its heat substitution credit; no production proxy or sign inversion is applied."
        entry["review"].update(
            status="verified",
            reason=reason,
        )
        entry.pop("conversion_factor", None)
        entry.update(operation="delete", approved_omission=True, review_ref=reference)
    profiles = annotate_exact_patch_handoff_profiles(source, profiles, previous)
    return annotate_complete_patch_handoff_profiles(source, profiles, previous)


def recovery_credit_profiles(approval, observations, quantities, evidence):
    """Bind independently approved credits to exact reference roles and inspected amounts."""
    if sorted(observations) != approval["observations"] or sorted(label for label, _ in quantities) != [
        label for label, _ in approval["observations"]
    ]:
        raise ValueError("Recovery-credit profile coverage changed; repeat the scientific review.")
    profiles = []
    for label, record in sorted(quantities):
        if (
            tuple(record["supplier"]) != approval["credited_supplier"]
            or record["source_reference_amount"] != -1
            or record["supplier_reference_amount"] != 1
            or record["credited_amount"] != -approval["quantity"]
        ):
            raise ValueError("Recovery-credit quantity or credited supplier changed; repeat the scientific review.")
        family, version, model = label.split("-")
        profiles.append(
            {
                "source_profile": {
                    "family": family,
                    "version": version,
                    "system_model": model,
                },
                "conversion_factor": approval["quantity"],
                "review_ref": intern_review({}, approval["review"]),
                "review": {
                    "status": "verified",
                    "reason": "Explicitly approved recovered-product credit: retain the incoming sign and apply the inspected positive credit quantity. This changes reference roles; it is not a treatment-service coordinate inversion.",
                    "evidence": list(evidence),
                    "source_sign": -1,
                    "target_sign": 1,
                    "role_conversion": "recovery_credit",
                },
            }
        )
    return profiles


def profiled_treatment_review(approval, observations, evidence):
    """Approve only the exact inspected treatment profiles and their reference signs."""
    if sorted(observations) != sorted(approval["observations"]):
        raise ValueError("Profile-specific treatment observations changed; repeat the scientific review.")
    profiles = model_specific_review(observations, evidence)
    if profiles is None:
        raise ValueError("Profile-specific treatment no longer has its inspected model-dependent signs.")
    for entry in profiles:
        entry["review"].update(status="verified", reason=approval["limitations"])
        entry["conversion_factor"] = entry["review"]["source_sign"]
    return profiles


def apply_sign_review(resource, ledger):
    """Replay a sign audit after scientific mapping authoring; reject stale decisions."""
    decisions = ledger["resources"].get(resource["name"], {})
    declare_sewage_digestion_profiles(resource, decisions)
    indexed = {rule["id"]: rule for rule in reviewed_replacements(resource, decisions)}
    if decisions.keys() - indexed.keys():
        raise ValueError("Reference-flow sign review contains unknown rules.")
    pending = []
    for rule_id, decision in decisions.items():
        rule = indexed[rule_id]
        candidate = approved_supplier_rule(rule)
        if rule_binding(candidate) != decision["binding"]:
            raise ValueError(f"Stale reference-flow sign target: {rule_id}")
        factor = rule.get("conversion_factor", 1)
        expected = decision["original_factor"]
        target_factor = decision["factor"]
        if factor not in (expected, target_factor):
            raise ValueError(f"Stale reference-flow sign factor: {rule_id}")
        pending.append((rule, decision, candidate))
    for rule, decision, candidate in pending:
        if decision.get("operation") == "delete":
            continue
        if decision.get("operation") == "recipe":
            rule.clear()
            rule.update(compiled_venting_rule(decision))
            resource["recipes"].update(deepcopy(decision["recipes"]))
            intern_review(resource, decision["recipe_review"])
            if "profile_omission_review" in decision:
                intern_review(resource, decision["profile_omission_review"])
            continue
        rule["target"] = deepcopy(candidate["target"])
        if decision["factor"] != decision["original_factor"]:
            rule["conversion_factor"] = decision["factor"]
        rule["reference_flow_sign_review"] = deepcopy(decision["review"])
        if "profile_omission_review" in decision:
            intern_review(resource, decision["profile_omission_review"])
        if "supplier_proxy_review" in decision:
            rule["review_ref"] = intern_review(resource, decision["supplier_proxy_review"])
            rule["technosphere_information_loss"] = decision["supplier_proxy_review"]["limitations"][0]
    resource["replace"] = [
        (indexed[rule["id"]] if decisions.get(rule["id"], {}).get("operation") == "recipe" else rule)
        for rule in resource["replace"]
    ]
    omitted = {rule_id: decision for rule_id, decision in decisions.items() if decision.get("operation") == "delete"}
    if omitted:
        resource["replace"] = [rule for rule in resource["replace"] if rule["id"] not in omitted]
        resource["delete"] = [rule for rule in resource["delete"] if rule.get("id") not in omitted]
        for rule_id, decision in sorted(omitted.items()):
            resource["delete"].append(
                {
                    "id": rule_id,
                    "source": deepcopy(decision["original_rule"]["source"]),
                    "approved_omission": True,
                    "review_ref": intern_review(resource, decision["omission_review"]),
                }
            )
        resource["delete"].sort(key=lambda rule: identity(rule["source"]))
        coverage = resource.get("coverage", {})
        coverage["omitted_identities"] = len(omitted)
        if "mapping_methods" in coverage:
            coverage["mapping_methods"] = dict(
                sorted(Counter(rule["mapping_method"] for rule in resource["replace"]).items())
            )
        if "mean_confidence" in coverage:
            coverage["mean_confidence"] = round(
                sum(rule["confidence"] for rule in resource["replace"]) / len(resource["replace"]),
                6,
            )
        if "dispositions" in coverage:
            dispositions = Counter(
                ("recipe" if "recipe_ref" in rule["target"] else rule.get("classification", "unclassified"))
                for rule in resource["replace"]
            )
            dispositions["omitted"] = len(omitted)
            if coverage.get("unresolved_identities"):
                dispositions["unresolved"] = coverage["unresolved_identities"]
            coverage["dispositions"] = dict(dispositions)
        referenced = {
            rule["review_ref"]
            for operation in ("replace", "disaggregate", "delete")
            for rule in resource[operation]
            if "review_ref" in rule
        }
        referenced.update(
            entry["review_ref"]
            for rule in resource["replace"]
            for entry in rule.get("reference_flow_sign_review", {}).get("profiles", [])
            if "review_ref" in entry
        )
        resource["reviews"] = {key: value for key, value in resource["reviews"].items() if key in referenced}


def reviewed_replacements(resource, decisions):
    """Recover prior supplier metadata for replay only; omissions remain deletions at runtime."""
    rules = []
    for rule in resource["replace"]:
        decision = decisions.get(rule.get("id"), {})
        if decision.get("operation") == "recipe" and (
            "recipe_ref" in rule["target"] or rule.get("mapping_method") == "reviewed_source_gas_flare_service"
        ):
            if (
                rule != compiled_venting_rule(decision)
                or any(resource["recipes"].get(key) != recipe for key, recipe in decision["recipes"].items())
                or rule_review(resource, rule) != decision["recipe_review"]
            ):
                raise ValueError("Stale approved venting recipe.")
            rules.append(deepcopy(decision["original_rule"]))
        else:
            rules.append(rule)
    for rule in resource.get("delete", []):
        decision = decisions.get(rule.get("id"), {})
        if not rule.get("approved_omission"):
            if decision.get("operation") == "delete":
                raise ValueError("Stale approved technosphere omission.")
            continue
        original = decision.get("original_rule", {})
        if (
            decision.get("operation") != "delete"
            or set(rule) != {"id", "source", "approved_omission", "review_ref"}
            or rule["source"] != original.get("source")
            or rule_review(resource, rule) != decision.get("omission_review")
        ):
            raise ValueError("Stale approved technosphere omission.")
        rules.append(deepcopy(original))
    return rules


def apply_packaged_sign_review(resource, root):
    """Apply the checked-in audit to a regenerated migration resource."""
    ledger = json.loads((root / REVIEW_PATH).read_text(encoding="utf-8"))
    native = root / "brightpath/data/export/openlca_references/uvek__2025__cutoff__ecoinvent__3.10.json"
    if hashlib.sha256(native.read_bytes()).hexdigest() != ledger["native_reference_sha256"]:
        raise ValueError("Native reference metadata changed; repeat the sign review.")
    apply_sign_review(resource, ledger)


def collect_reference_profiles(brightway_root, provenance):
    """Read only trusted, locally installed Brightway reference metadata.

    Brightway stores Python pickles in its SQLite database. Do not use this
    collector with a database supplied by an untrusted party.
    """
    profiles = []
    for entry in provenance:
        path = brightway_root / entry["project"] / "lci/databases.db"
        database = entry["database"]
        with sqlite3.connect(f"{path.as_uri()}?mode=ro", uri=True) as connection:
            activities = {}
            for (raw,) in connection.execute("SELECT data FROM activitydataset WHERE database=?", (database,)):
                activity = pickle.loads(raw)
                activities[activity["code"]] = identity(activity)
            production = defaultdict(list)
            for code, raw in connection.execute(
                "SELECT output_code,data FROM exchangedataset WHERE output_database=? AND type=?",
                (database, "production"),
            ):
                production[code].append(pickle.loads(raw))
            credits = []
            venting_inventories = []
            disposal_inventories = []
            reclamation_inventories = []
            cutoff_stock_inventories = []
            discharge_inventories = []
            flare_inventories = []
            natural_gas_flare_inventories = []
            eaf_landfill_inventories = []
            lcd_treatment_inventories = []
            dedicated_disposal_inventories = []
            modal_transport_inventories = []
            antifouling_inventories = []
            industrial_gas_inventories = []
            wwt_residue_inventories = []
            reverse_gas_inventories = []
            gas_market_inventories = []
            market_composition_inventories = []
            esterquat_market_component_inventories = []
            cutoff_feed_product_inventories = []
            reverse_ash_inventories = []
            processing_composition_inventories = []
            cutoff_slag_product_inventories = []
            sewer_grid_inventories = []
            slag_landfill_component_inventories = []
            ash_market_inventories = []
            bottom_ash_inventories = []
            stainless_stock_inventories = []
            residual_landfill_inventories = []
            resolved_child_market_inventories = []
            sewer_throughput_inventories = []
            packaging_paper_landfarming_inventories = []
            steel_residue_market_inventories = []
            industrial_landfarming_inventories = []
            industrial_waste_market_inventories = []
            additional_cutoff_waste_market_inventories = []
            uncontrolled_disposal_inventories = []
            pet_waste_market_inventories = []
            five_simple_waste_market_inventories = []
            swiss_pet_fluwa_inventories = []
            slf_landfarming_inventories = []
            average_sludge_landfarming_inventories = []
            exact_patch_boundary_inventories = []
            complete_patch_boundary_inventories = []
            cable_rail_inventories = []
            pet_market_group_inventories = []
            copper_stock_inventories = []
            copper_cathode_inventories = []
            pam_frontier_stock_inventories = []
            regional_freight_inventories = []
            older_regional_road_inventories = []
            chemical_heat_inventories = []
            whole_freight_inventories = []
            packed_wool_inventories = []
            sewage_digestion_inventories = []
            for code, record in activities.items():
                if record in RESIDUAL_LANDFILL_SCOPE.get(tuple(entry["profile"]), set()):
                    blobs = [
                        raw
                        for raw, in connection.execute(
                            "SELECT data FROM exchangedataset WHERE output_database=? AND output_code=?",
                            (database, code),
                        )
                    ]
                    metadata = pickle.loads(
                        connection.execute(
                            "SELECT data FROM activitydataset WHERE database=? AND code=?", (database, code)
                        ).fetchone()[0]
                    )
                    residual_landfill_inventories.append(
                        residual_landfill_inventory_record(
                            record, blobs, metadata.get("comment"), metadata.get("product_information")
                        )
                    )
                if record in RESOLVED_CHILD_MARKET_SCOPE.get(tuple(entry["profile"]), set()):
                    blobs = [
                        raw
                        for raw, in connection.execute(
                            "SELECT data FROM exchangedataset WHERE output_database=? AND output_code=?",
                            (database, code),
                        )
                    ]
                    metadata = pickle.loads(
                        connection.execute(
                            "SELECT data FROM activitydataset WHERE database=? AND code=?", (database, code)
                        ).fetchone()[0]
                    )
                    resolved_child_market_inventories.append(
                        resolved_child_market_inventory_record(
                            record, blobs, metadata.get("comment"), metadata.get("product_information")
                        )
                    )
                if record in SEWER_THROUGHPUT_SCOPE.get(tuple(entry["profile"]), set()):
                    blobs = [
                        raw
                        for raw, in connection.execute(
                            "SELECT data FROM exchangedataset WHERE output_database=? AND output_code=?",
                            (database, code),
                        )
                    ]
                    metadata = pickle.loads(
                        connection.execute(
                            "SELECT data FROM activitydataset WHERE database=? AND code=?", (database, code)
                        ).fetchone()[0]
                    )
                    sewer_throughput_inventories.append(
                        sewer_throughput_inventory_record(
                            record, blobs, metadata.get("comment"), metadata.get("product_information")
                        )
                    )
                if record in PACKAGING_PAPER_LANDFARMING_SCOPE.get(tuple(entry["profile"]), set()):
                    blobs = [
                        raw
                        for raw, in connection.execute(
                            "SELECT data FROM exchangedataset WHERE output_database=? AND output_code=?",
                            (database, code),
                        )
                    ]
                    metadata = pickle.loads(
                        connection.execute(
                            "SELECT data FROM activitydataset WHERE database=? AND code=?", (database, code)
                        ).fetchone()[0]
                    )
                    packaging_paper_landfarming_inventories.append(
                        packaging_paper_landfarming_inventory_record(
                            record, blobs, metadata.get("comment"), metadata.get("product_information")
                        )
                    )
                if record in STEEL_RESIDUE_MARKET_SCOPE.get(tuple(entry["profile"]), set()):
                    blobs = [
                        raw
                        for raw, in connection.execute(
                            "SELECT data FROM exchangedataset WHERE output_database=? AND output_code=?",
                            (database, code),
                        )
                    ]
                    metadata = pickle.loads(
                        connection.execute(
                            "SELECT data FROM activitydataset WHERE database=? AND code=?", (database, code)
                        ).fetchone()[0]
                    )
                    steel_residue_market_inventories.append(
                        steel_residue_market_inventory_record(
                            record, blobs, metadata.get("comment"), metadata.get("product_information")
                        )
                    )
                if record in INDUSTRIAL_LANDFARMING_SCOPE.get(tuple(entry["profile"]), set()):
                    blobs = [
                        raw
                        for raw, in connection.execute(
                            "SELECT data FROM exchangedataset WHERE output_database=? AND output_code=?",
                            (database, code),
                        )
                    ]
                    metadata = pickle.loads(
                        connection.execute(
                            "SELECT data FROM activitydataset WHERE database=? AND code=?", (database, code)
                        ).fetchone()[0]
                    )
                    industrial_landfarming_inventories.append(
                        industrial_landfarming_inventory_record(
                            record, blobs, metadata.get("comment"), metadata.get("product_information")
                        )
                    )
                if record in INDUSTRIAL_WASTE_MARKET_SCOPE.get(tuple(entry["profile"]), set()):
                    blobs = [
                        raw
                        for raw, in connection.execute(
                            "SELECT data FROM exchangedataset WHERE output_database=? AND output_code=?",
                            (database, code),
                        )
                    ]
                    metadata = pickle.loads(
                        connection.execute(
                            "SELECT data FROM activitydataset WHERE database=? AND code=?", (database, code)
                        ).fetchone()[0]
                    )
                    industrial_waste_market_inventories.append(
                        industrial_waste_market_inventory_record(
                            record, blobs, metadata.get("comment"), metadata.get("product_information")
                        )
                    )
                if record in ADDITIONAL_CUTOFF_WASTE_MARKET_SCOPE.get(tuple(entry["profile"]), set()):
                    blobs = [
                        raw
                        for raw, in connection.execute(
                            "SELECT data FROM exchangedataset WHERE output_database=? AND output_code=?",
                            (database, code),
                        )
                    ]
                    metadata = pickle.loads(
                        connection.execute(
                            "SELECT data FROM activitydataset WHERE database=? AND code=?", (database, code)
                        ).fetchone()[0]
                    )
                    additional_cutoff_waste_market_inventories.append(
                        additional_cutoff_waste_market_inventory_record(
                            record, blobs, metadata.get("comment"), metadata.get("product_information")
                        )
                    )
                if record in UNCONTROLLED_DISPOSAL_SCOPE.get(tuple(entry["profile"]), set()):
                    blobs = [
                        raw
                        for raw, in connection.execute(
                            "SELECT data FROM exchangedataset WHERE output_database=? AND output_code=?",
                            (database, code),
                        )
                    ]
                    metadata = pickle.loads(
                        connection.execute(
                            "SELECT data FROM activitydataset WHERE database=? AND code=?", (database, code)
                        ).fetchone()[0]
                    )
                    uncontrolled_disposal_inventories.append(
                        uncontrolled_disposal_inventory_record(
                            record, blobs, metadata.get("comment"), metadata.get("product_information")
                        )
                    )
                if record in PET_WASTE_MARKET_SCOPE.get(tuple(entry["profile"]), set()):
                    blobs = [
                        raw
                        for raw, in connection.execute(
                            "SELECT data FROM exchangedataset WHERE output_database=? AND output_code=?",
                            (database, code),
                        )
                    ]
                    metadata = pickle.loads(
                        connection.execute(
                            "SELECT data FROM activitydataset WHERE database=? AND code=?", (database, code)
                        ).fetchone()[0]
                    )
                    pet_waste_market_inventories.append(
                        pet_waste_market_inventory_record(
                            record, blobs, metadata.get("comment"), metadata.get("product_information")
                        )
                    )
                if record in FIVE_SIMPLE_WASTE_MARKET_SCOPE.get(tuple(entry["profile"]), set()):
                    blobs = [
                        raw
                        for raw, in connection.execute(
                            "SELECT data FROM exchangedataset WHERE output_database=? AND output_code=?",
                            (database, code),
                        )
                    ]
                    metadata = pickle.loads(
                        connection.execute(
                            "SELECT data FROM activitydataset WHERE database=? AND code=?", (database, code)
                        ).fetchone()[0]
                    )
                    five_simple_waste_market_inventories.append(
                        five_simple_waste_market_inventory_record(
                            record, blobs, metadata.get("comment"), metadata.get("product_information")
                        )
                    )
                if record in SWISS_PET_FLUWA_SCOPE.get(tuple(entry["profile"]), set()):
                    blobs = [
                        raw
                        for raw, in connection.execute(
                            "SELECT data FROM exchangedataset WHERE output_database=? AND output_code=?",
                            (database, code),
                        )
                    ]
                    metadata = pickle.loads(
                        connection.execute(
                            "SELECT data FROM activitydataset WHERE database=? AND code=?", (database, code)
                        ).fetchone()[0]
                    )
                    swiss_pet_fluwa_inventories.append(
                        swiss_pet_fluwa_inventory_record(
                            record, blobs, metadata.get("comment"), metadata.get("product_information")
                        )
                    )
                if record in SLF_LANDFARMING_SCOPE.get(tuple(entry["profile"]), set()):
                    blobs = [
                        raw
                        for raw, in connection.execute(
                            "SELECT data FROM exchangedataset WHERE output_database=? AND output_code=?",
                            (database, code),
                        )
                    ]
                    metadata = pickle.loads(
                        connection.execute(
                            "SELECT data FROM activitydataset WHERE database=? AND code=?", (database, code)
                        ).fetchone()[0]
                    )
                    slf_landfarming_inventories.append(
                        slf_landfarming_inventory_record(
                            record, blobs, metadata.get("comment"), metadata.get("product_information")
                        )
                    )
                if record in AVERAGE_SLUDGE_LANDFARMING_SCOPE.get(tuple(entry["profile"]), set()):
                    blobs = [
                        raw
                        for raw, in connection.execute(
                            "SELECT data FROM exchangedataset WHERE output_database=? AND output_code=?",
                            (database, code),
                        )
                    ]
                    metadata = pickle.loads(
                        connection.execute(
                            "SELECT data FROM activitydataset WHERE database=? AND code=?", (database, code)
                        ).fetchone()[0]
                    )
                    average_sludge_landfarming_inventories.append(
                        average_sludge_landfarming_inventory_record(
                            record, blobs, metadata.get("comment"), metadata.get("product_information")
                        )
                    )
                if record in EXACT_PATCH_BOUNDARY_SCOPE.get(tuple(entry["profile"]), set()):
                    blobs = [
                        raw
                        for raw, in connection.execute(
                            "SELECT data FROM exchangedataset WHERE output_database=? AND output_code=?",
                            (database, code),
                        )
                    ]
                    metadata = pickle.loads(
                        connection.execute(
                            "SELECT data FROM activitydataset WHERE database=? AND code=?", (database, code)
                        ).fetchone()[0]
                    )
                    exact_patch_boundary_inventories.append(
                        exact_patch_boundary_inventory_record(
                            record, blobs, metadata.get("comment"), metadata.get("product_information")
                        )
                    )
                if record in COMPLETE_PATCH_BOUNDARY_SCOPE.get(tuple(entry["profile"]), set()):
                    blobs = [
                        raw
                        for raw, in connection.execute(
                            "SELECT data FROM exchangedataset WHERE output_database=? AND output_code=?",
                            (database, code),
                        )
                    ]
                    metadata = pickle.loads(
                        connection.execute(
                            "SELECT data FROM activitydataset WHERE database=? AND code=?", (database, code)
                        ).fetchone()[0]
                    )
                    complete_patch_boundary_inventories.append(
                        complete_patch_boundary_inventory_record(
                            record, blobs, metadata.get("comment"), metadata.get("product_information")
                        )
                    )
                if record in CABLE_RAIL_SCOPE.get(tuple(entry["profile"]), set()):
                    blobs = [
                        raw
                        for raw, in connection.execute(
                            "SELECT data FROM exchangedataset WHERE output_database=? AND output_code=?",
                            (database, code),
                        )
                    ]
                    metadata = pickle.loads(
                        connection.execute(
                            "SELECT data FROM activitydataset WHERE database=? AND code=?", (database, code)
                        ).fetchone()[0]
                    )
                    cable_rail_inventories.append(
                        cable_rail_inventory_record(
                            record, blobs, metadata.get("comment"), metadata.get("product_information")
                        )
                    )
                if record in PET_MARKET_GROUP_SCOPE.get(tuple(entry["profile"]), set()):
                    blobs = [
                        raw
                        for raw, in connection.execute(
                            "SELECT data FROM exchangedataset WHERE output_database=? AND output_code=?",
                            (database, code),
                        )
                    ]
                    metadata = pickle.loads(
                        connection.execute(
                            "SELECT data FROM activitydataset WHERE database=? AND code=?", (database, code)
                        ).fetchone()[0]
                    )
                    pet_market_group_inventories.append(
                        pet_market_group_inventory_record(
                            record, blobs, metadata.get("comment"), metadata.get("product_information")
                        )
                    )
                if record in PACKED_WOOL_SCOPE.get(tuple(entry["profile"]), set()):
                    blobs = [
                        raw
                        for raw, in connection.execute(
                            "SELECT data FROM exchangedataset WHERE output_database=? AND output_code=?",
                            (database, code),
                        )
                    ]
                    metadata = pickle.loads(
                        connection.execute(
                            "SELECT data FROM activitydataset WHERE database=? AND code=?", (database, code)
                        ).fetchone()[0]
                    )
                    packed_wool_inventories.append(
                        packed_wool_inventory_record(
                            record, blobs, metadata.get("comment"), metadata.get("product_information")
                        )
                    )
                if record in WHOLE_FREIGHT_SCOPE.get(tuple(entry["profile"]), set()):
                    blobs = [
                        raw
                        for raw, in connection.execute(
                            "SELECT data FROM exchangedataset WHERE output_database=? AND output_code=?",
                            (database, code),
                        )
                    ]
                    metadata = pickle.loads(
                        connection.execute(
                            "SELECT data FROM activitydataset WHERE database=? AND code=?", (database, code)
                        ).fetchone()[0]
                    )
                    whole_freight_inventories.append(
                        whole_freight_inventory_record(
                            record, blobs, metadata.get("comment"), metadata.get("product_information")
                        )
                    )
                if record in CHEMICAL_HEAT_SCOPE.get(tuple(entry["profile"]), set()):
                    blobs = [
                        raw
                        for raw, in connection.execute(
                            "SELECT data FROM exchangedataset WHERE output_database=? AND output_code=?",
                            (database, code),
                        )
                    ]
                    metadata = pickle.loads(
                        connection.execute(
                            "SELECT data FROM activitydataset WHERE database=? AND code=?", (database, code)
                        ).fetchone()[0]
                    )
                    chemical_heat_inventories.append(
                        chemical_heat_inventory_record(
                            record, blobs, metadata.get("comment"), metadata.get("product_information")
                        )
                    )
                if record in PAM_FRONTIER_STOCK_SCOPE.get(tuple(entry["profile"]), set()):
                    blobs = [
                        raw
                        for raw, in connection.execute(
                            "SELECT data FROM exchangedataset WHERE output_database=? AND output_code=?",
                            (database, code),
                        )
                    ]
                    metadata = pickle.loads(
                        connection.execute(
                            "SELECT data FROM activitydataset WHERE database=? AND code=?", (database, code)
                        ).fetchone()[0]
                    )
                    pam_frontier_stock_inventories.append(
                        pam_frontier_stock_inventory_record(
                            record, blobs, metadata.get("comment"), metadata.get("product_information")
                        )
                    )
                if record in REGIONAL_FREIGHT_SCOPE.get(tuple(entry["profile"]), set()):
                    blobs = [
                        raw
                        for raw, in connection.execute(
                            "SELECT data FROM exchangedataset WHERE output_database=? AND output_code=?",
                            (database, code),
                        )
                    ]
                    metadata = pickle.loads(
                        connection.execute(
                            "SELECT data FROM activitydataset WHERE database=? AND code=?", (database, code)
                        ).fetchone()[0]
                    )
                    regional_freight_inventories.append(
                        regional_freight_inventory_record(
                            record, blobs, metadata.get("comment"), metadata.get("product_information")
                        )
                    )
                if record in OLDER_REGIONAL_ROAD_SCOPE.get(tuple(entry["profile"]), set()):
                    blobs = [
                        raw
                        for raw, in connection.execute(
                            "SELECT data FROM exchangedataset WHERE output_database=? AND output_code=?",
                            (database, code),
                        )
                    ]
                    metadata = pickle.loads(
                        connection.execute(
                            "SELECT data FROM activitydataset WHERE database=? AND code=?", (database, code)
                        ).fetchone()[0]
                    )
                    older_regional_road_inventories.append(
                        older_regional_road_inventory_record(
                            record, blobs, metadata.get("comment"), metadata.get("product_information")
                        )
                    )
                if record in COPPER_CATHODE_SCOPE.get(tuple(entry["profile"]), set()):
                    blobs = [
                        raw
                        for raw, in connection.execute(
                            "SELECT data FROM exchangedataset WHERE output_database=? AND output_code=?",
                            (database, code),
                        )
                    ]
                    metadata = pickle.loads(
                        connection.execute(
                            "SELECT data FROM activitydataset WHERE database=? AND code=?", (database, code)
                        ).fetchone()[0]
                    )
                    copper_cathode_inventories.append(
                        copper_cathode_inventory_record(
                            record, blobs, metadata.get("comment"), metadata.get("product_information")
                        )
                    )
                if record in COPPER_STOCK_SCOPE.get(tuple(entry["profile"]), set()):
                    blobs = [
                        raw
                        for raw, in connection.execute(
                            "SELECT data FROM exchangedataset WHERE output_database=? AND output_code=?",
                            (database, code),
                        )
                    ]
                    metadata = pickle.loads(
                        connection.execute(
                            "SELECT data FROM activitydataset WHERE database=? AND code=?", (database, code)
                        ).fetchone()[0]
                    )
                    copper_stock_inventories.append(
                        copper_stock_inventory_record(
                            record, blobs, metadata.get("comment"), metadata.get("product_information")
                        )
                    )
                if record in SEWAGE_DIGESTION_SCOPE.get(tuple(entry["profile"]), set()):
                    blobs = [
                        raw
                        for raw, in connection.execute(
                            "SELECT data FROM exchangedataset WHERE output_database=? AND output_code=?",
                            (database, code),
                        )
                    ]
                    metadata = pickle.loads(
                        connection.execute(
                            "SELECT data FROM activitydataset WHERE database=? AND code=?", (database, code)
                        ).fetchone()[0]
                    )
                    sewage_digestion_inventories.append(sewage_digestion_inventory_record(record, blobs, metadata))
                if record in SLAG_LANDFILL_COMPONENT_SCOPE.get(tuple(entry["profile"]), set()):
                    blobs = [
                        raw
                        for raw, in connection.execute(
                            "SELECT data FROM exchangedataset WHERE output_database=? AND output_code=?",
                            (database, code),
                        )
                    ]
                    metadata = pickle.loads(
                        connection.execute(
                            "SELECT data FROM activitydataset WHERE database=? AND code=?", (database, code)
                        ).fetchone()[0]
                    )
                    slag_landfill_component_inventories.append(
                        slag_landfill_component_inventory_record(
                            record, blobs, metadata.get("comment"), metadata.get("product_information")
                        )
                    )
                if record in ASH_MARKET_SCOPE.get(tuple(entry["profile"]), set()):
                    blobs = [
                        raw
                        for raw, in connection.execute(
                            "SELECT data FROM exchangedataset WHERE output_database=? AND output_code=?",
                            (database, code),
                        )
                    ]
                    metadata = pickle.loads(
                        connection.execute(
                            "SELECT data FROM activitydataset WHERE database=? AND code=?", (database, code)
                        ).fetchone()[0]
                    )
                    ash_market_inventories.append(
                        ash_market_inventory_record(
                            record, blobs, metadata.get("comment"), metadata.get("product_information")
                        )
                    )
                if record in BOTTOM_ASH_SCOPE.get(tuple(entry["profile"]), set()):
                    blobs = [
                        raw
                        for raw, in connection.execute(
                            "SELECT data FROM exchangedataset WHERE output_database=? AND output_code=?",
                            (database, code),
                        )
                    ]
                    metadata = pickle.loads(
                        connection.execute(
                            "SELECT data FROM activitydataset WHERE database=? AND code=?", (database, code)
                        ).fetchone()[0]
                    )
                    bottom_ash_inventories.append(
                        bottom_ash_inventory_record(
                            record, blobs, metadata.get("comment"), metadata.get("product_information")
                        )
                    )
                if record in STAINLESS_STOCK_SCOPE.get(tuple(entry["profile"]), set()):
                    blobs = [
                        raw
                        for raw, in connection.execute(
                            "SELECT data FROM exchangedataset WHERE output_database=? AND output_code=?",
                            (database, code),
                        )
                    ]
                    metadata = pickle.loads(
                        connection.execute(
                            "SELECT data FROM activitydataset WHERE database=? AND code=?", (database, code)
                        ).fetchone()[0]
                    )
                    stainless_stock_inventories.append(
                        stainless_stock_inventory_record(
                            record, blobs, metadata.get("comment"), metadata.get("product_information")
                        )
                    )
                if record in SEWER_GRID_SCOPE.get(tuple(entry["profile"]), set()):
                    blobs = [
                        raw
                        for raw, in connection.execute(
                            "SELECT data FROM exchangedataset WHERE output_database=? AND output_code=?",
                            (database, code),
                        )
                    ]
                    metadata = pickle.loads(
                        connection.execute(
                            "SELECT data FROM activitydataset WHERE database=? AND code=?", (database, code)
                        ).fetchone()[0]
                    )
                    sewer_grid_inventories.append(
                        sewer_grid_inventory_record(
                            record, blobs, metadata.get("comment"), metadata.get("product_information")
                        )
                    )
                if record in CUTOFF_SLAG_SCOPE.get(tuple(entry["profile"]), set()):
                    blobs = [
                        raw
                        for raw, in connection.execute(
                            "SELECT data FROM exchangedataset WHERE output_database=? AND output_code=?",
                            (database, code),
                        )
                    ]
                    metadata = pickle.loads(
                        connection.execute(
                            "SELECT data FROM activitydataset WHERE database=? AND code=?", (database, code)
                        ).fetchone()[0]
                    )
                    cutoff_slag_product_inventories.append(
                        cutoff_slag_inventory_record(
                            record, blobs, metadata.get("comment"), metadata.get("product_information")
                        )
                    )
                if entry["profile"][0] == "ecoinvent" and record in PROCESSING:
                    blobs = [
                        raw
                        for (raw,) in connection.execute(
                            "SELECT data FROM exchangedataset WHERE output_database=? AND output_code=?",
                            (database, code),
                        )
                    ]
                    processing_composition_inventories.append(
                        {
                            "source": list(record),
                            "inventory_sha256": hashlib.sha256(b"".join(sorted(blobs))).hexdigest(),
                        }
                    )
                if record in REVERSE_ASH_EVIDENCE_SCOPE.get(tuple(entry["profile"]), set()):
                    blobs = [
                        raw
                        for (raw,) in connection.execute(
                            "SELECT data FROM exchangedataset WHERE output_database=? AND output_code=?",
                            (database, code),
                        )
                    ]
                    reverse_ash_inventories.append(
                        {
                            "source": list(record),
                            "inventory_sha256": hashlib.sha256(b"".join(sorted(blobs))).hexdigest(),
                        }
                    )
                if record in CUTOFF_FEED_SCOPE.get(tuple(entry["profile"]), set()):
                    blobs = [
                        raw
                        for (raw,) in connection.execute(
                            "SELECT data FROM exchangedataset WHERE output_database=? AND output_code=?",
                            (database, code),
                        )
                    ]
                    activity = pickle.loads(
                        connection.execute(
                            "SELECT data FROM activitydataset WHERE database=? AND code=?", (database, code)
                        ).fetchone()[0]
                    )
                    cutoff_feed_product_inventories.append(
                        cutoff_feed_inventory_record(
                            record, blobs, activity.get("comment"), activity.get("product_information")
                        )
                    )
                if record in ESTERQUAT_MARKET_SCOPE.get(tuple(entry["profile"]), set()):
                    blobs = [
                        raw
                        for (raw,) in connection.execute(
                            "SELECT data FROM exchangedataset WHERE output_database=? AND output_code=?",
                            (database, code),
                        )
                    ]
                    esterquat_market_component_inventories.append(
                        {
                            "source": list(record),
                            "inventory_sha256": hashlib.sha256(b"".join(sorted(blobs))).hexdigest(),
                        }
                    )
                if record in COMPOSITIONS:
                    blobs = [
                        raw
                        for (raw,) in connection.execute(
                            "SELECT data FROM exchangedataset WHERE output_database=? AND output_code=?",
                            (database, code),
                        )
                    ]
                    market_composition_inventories.append(
                        {
                            "source": list(record),
                            "inventory_sha256": hashlib.sha256(b"".join(sorted(blobs))).hexdigest(),
                        }
                    )
                if record in GAS_MARKET_EVIDENCE_SCOPE:
                    blobs = [
                        raw
                        for (raw,) in connection.execute(
                            "SELECT data FROM exchangedataset WHERE output_database=? AND output_code=?",
                            (database, code),
                        )
                    ]
                    gas_market_inventories.append(
                        {
                            "source": list(record),
                            "inventory_sha256": hashlib.sha256(b"".join(sorted(blobs))).hexdigest(),
                        }
                    )
                if record in REVERSE_GAS_EVIDENCE_SCOPE.get(tuple(entry["profile"]), set()):
                    blobs = [
                        raw
                        for (raw,) in connection.execute(
                            "SELECT data FROM exchangedataset WHERE output_database=? AND output_code=?",
                            (database, code),
                        )
                    ]
                    reverse_gas_inventories.append(
                        {
                            "source": list(record),
                            "inventory_sha256": hashlib.sha256(b"".join(sorted(blobs))).hexdigest(),
                        }
                    )
                if record in APPROVED_WWT_RESIDUE_OMISSIONS:
                    blobs = [
                        raw
                        for (raw,) in connection.execute(
                            "SELECT data FROM exchangedataset WHERE output_database=? AND output_code=?",
                            (database, code),
                        )
                    ]
                    wwt_residue_inventories.append(
                        {
                            "source": list(record),
                            "inventory_sha256": hashlib.sha256(b"".join(sorted(blobs))).hexdigest(),
                        }
                    )
                if record in GAS_SOURCES or record in PRIMITIVES:
                    blobs = [
                        raw
                        for (raw,) in connection.execute(
                            "SELECT data FROM exchangedataset WHERE output_database=? AND output_code=?",
                            (database, code),
                        )
                    ]
                    industrial_gas_inventories.append(
                        {
                            "source": list(record),
                            "inventory_sha256": hashlib.sha256(b"".join(sorted(blobs))).hexdigest(),
                        }
                    )
                if record in APPROVED_ANTIFOULING_OMISSIONS:
                    blobs = [
                        raw
                        for (raw,) in connection.execute(
                            "SELECT data FROM exchangedataset WHERE output_database=? AND output_code=?",
                            (database, code),
                        )
                    ]
                    antifouling_inventories.append(
                        {
                            "source": list(record),
                            "inventory_sha256": hashlib.sha256(b"".join(sorted(blobs))).hexdigest(),
                        }
                    )
                if record in APPROVED_DISCHARGE_TREATMENTS or record in DISCHARGE_TARGETS:
                    blobs = [
                        raw
                        for (raw,) in connection.execute(
                            "SELECT data FROM exchangedataset WHERE output_database=? AND output_code=?",
                            (database, code),
                        )
                    ]
                    discharge_inventories.append(
                        {
                            "source": list(record),
                            "inventory_sha256": hashlib.sha256(b"".join(sorted(blobs))).hexdigest(),
                        }
                    )
                if record in NATURAL_GAS_FLARE_SCOPE.get(tuple(entry["profile"]), set()):
                    blobs = [
                        raw
                        for (raw,) in connection.execute(
                            "SELECT data FROM exchangedataset WHERE output_database=? AND output_code=?",
                            (database, code),
                        )
                    ]
                    raw_activity = connection.execute(
                        "SELECT data FROM activitydataset WHERE database=? AND code=?", (database, code)
                    ).fetchone()[0]
                    natural_gas_flare_inventories.append(
                        natural_gas_flare_inventory_record(record, blobs, pickle.loads(raw_activity).get("comment"))
                    )
                if record in FLARES or record == FLARE_TARGET:
                    blobs = [
                        raw
                        for (raw,) in connection.execute(
                            "SELECT data FROM exchangedataset WHERE output_database=? AND output_code=?",
                            (database, code),
                        )
                    ]
                    flare_inventories.append(
                        {
                            "source": list(record),
                            "inventory_sha256": hashlib.sha256(b"".join(sorted(blobs))).hexdigest(),
                        }
                    )
                if record in LANDFILLS or record in EAF_LANDFILL_TARGETS:
                    blobs = [
                        raw
                        for (raw,) in connection.execute(
                            "SELECT data FROM exchangedataset WHERE output_database=? AND output_code=?",
                            (database, code),
                        )
                    ]
                    eaf_landfill_inventories.append(
                        {
                            "source": list(record),
                            "inventory_sha256": hashlib.sha256(b"".join(sorted(blobs))).hexdigest(),
                        }
                    )
                if entry["profile"][0] == "ecoinvent" and record in STOCKS:
                    raw_stock = [
                        raw
                        for (raw,) in connection.execute(
                            "SELECT data FROM exchangedataset WHERE output_database=? AND output_code=?",
                            (database, code),
                        )
                    ]
                    exchanges = [pickle.loads(raw) for raw in raw_stock]
                    cutoff_stock_inventories.append(
                        {
                            "source": list(record),
                            "inventory_sha256": hashlib.sha256(b"".join(sorted(raw_stock))).hexdigest(),
                            "total_exchanges": len(exchanges),
                            "only_self_production": len(exchanges) == 1
                            and exchanges[0].get("type") == "production"
                            and tuple(exchanges[0].get("input", ())) == (database, code),
                            "production_amount": exchanges[0].get("amount") if len(exchanges) == 1 else None,
                        }
                    )
                if record in LCD_TREATMENTS or record in LCD_TARGETS:
                    blobs = [
                        raw
                        for (raw,) in connection.execute(
                            "SELECT data FROM exchangedataset WHERE output_database=? AND output_code=?",
                            (database, code),
                        )
                    ]
                    lcd_treatment_inventories.append(
                        {
                            "source": list(record),
                            "inventory_sha256": hashlib.sha256(b"".join(sorted(blobs))).hexdigest(),
                        }
                    )
                if record in DISPOSALS or record in DEDICATED_DISPOSAL_TARGETS:
                    blobs = [
                        raw
                        for (raw,) in connection.execute(
                            "SELECT data FROM exchangedataset WHERE output_database=? AND output_code=?",
                            (database, code),
                        )
                    ]
                    dedicated_disposal_inventories.append(
                        {
                            "source": list(record),
                            "inventory_sha256": hashlib.sha256(b"".join(sorted(blobs))).hexdigest(),
                        }
                    )
                if record in TRANSPORTS or record in MODAL_TRANSPORT_TARGETS:
                    blobs = [
                        raw
                        for (raw,) in connection.execute(
                            "SELECT data FROM exchangedataset WHERE output_database=? AND output_code=?",
                            (database, code),
                        )
                    ]
                    modal_transport_inventories.append(
                        {
                            "source": list(record),
                            "inventory_sha256": hashlib.sha256(b"".join(sorted(blobs))).hexdigest(),
                        }
                    )
                if record in RECLAMATION_SOURCES:
                    blobs = [
                        raw
                        for (raw,) in connection.execute(
                            "SELECT data FROM exchangedataset WHERE output_database=? AND output_code=?",
                            (database, code),
                        )
                    ]
                    record_evidence = {
                        "source": list(record),
                        "inventory_sha256": hashlib.sha256(b"".join(sorted(blobs))).hexdigest(),
                    }
                    if record[1] == "used refrigerant R134a" and entry["profile"][2] == "consequential":
                        credited = [
                            e
                            for raw in blobs
                            if (e := pickle.loads(raw)).get("type") == "technosphere"
                            and e.get("amount")
                            and activities.get(e.get("input", (None, None))[1], (None, None))[1]
                            in ("refrigerant R134a", "tetrafluoroethane, R134a")
                        ]
                        if len(credited) != 1:
                            raise ValueError("Reclamation credited supply changed; repeat the review.")
                        credited_code = credited[0]["input"][1]
                        market_blobs = [
                            raw
                            for (raw,) in connection.execute(
                                "SELECT data FROM exchangedataset WHERE output_database=? AND output_code=?",
                                (database, credited_code),
                            )
                        ]
                        record_evidence.update(
                            credit_market_source=list(activities[credited_code]),
                            credit_market_inventory_sha256=hashlib.sha256(b"".join(sorted(market_blobs))).hexdigest(),
                        )
                    reclamation_inventories.append(record_evidence)
                if record in VENTING_SOURCES or record in DISPOSAL_SOURCES:
                    blobs = [
                        raw
                        for (raw,) in connection.execute(
                            "SELECT data FROM exchangedataset WHERE output_database=? AND output_code=?",
                            (database, code),
                        )
                    ]
                    (venting_inventories if record in VENTING_SOURCES else disposal_inventories).append(
                        {
                            "source": list(record),
                            "inventory_sha256": hashlib.sha256(b"".join(sorted(blobs))).hexdigest(),
                        }
                    )
                if record not in APPROVED_RECOVERY_CREDITS:
                    continue
                active = [
                    exchange
                    for (raw,) in connection.execute(
                        "SELECT data FROM exchangedataset WHERE output_database=? AND output_code=?",
                        (database, code),
                    )
                    if (exchange := pickle.loads(raw)).get("amount") and exchange.get("type") != "production"
                ]
                if len(active) != 1 or active[0].get("type") != "technosphere":
                    raise ValueError(
                        "Recovery-credit source now has additional processing or biosphere exchanges; repeat its scientific review."
                    )
                supplier_database, supplier_code = active[0]["input"]
                if supplier_database != database or len(production[code]) != 1 or len(production[supplier_code]) != 1:
                    raise ValueError("Recovery-credit supplier reference changed; repeat the review.")
                credits.append(
                    {
                        "source": list(record),
                        "supplier": list(activities[supplier_code]),
                        "source_reference_amount": production[code][0]["amount"],
                        "supplier_reference_amount": production[supplier_code][0]["amount"],
                        "credited_amount": active[0]["amount"],
                    }
                )
        references = []
        for code, record in activities.items():
            rows = production[code]
            if len(rows) != 1 or tuple(rows[0].get("input", ())) != (database, code):
                raise ValueError(f"Ambiguous reference production: {database}, {code}")
            amount = rows[0].get("amount")
            if (
                isinstance(amount, bool)
                or not isinstance(amount, (int, float))
                or not math.isfinite(amount)
                or not amount
            ):
                raise ValueError(f"Invalid reference production amount: {database}, {code}")
            references.append([*record, 1 if amount > 0 else -1])
        if len(references) != entry["reference_count"]:
            raise ValueError(f"Reference coverage changed: {database}")
        if tuple(entry["profile"]) == ("uvek", "2025", "cutoff"):
            residual_landfill_inventories.extend(collect_residual_landfill_extras(brightway_root))
            resolved_child_market_inventories.extend(collect_resolved_child_market_extras(brightway_root))
            sewer_throughput_inventories.extend(collect_sewer_throughput_extras(brightway_root))
            packaging_paper_landfarming_inventories.extend(collect_packaging_paper_landfarming_extras(brightway_root))
            steel_residue_market_inventories.extend(collect_steel_residue_market_extras(brightway_root))
            industrial_landfarming_inventories.extend(collect_industrial_landfarming_extras(brightway_root))
            industrial_waste_market_inventories.extend(collect_industrial_waste_market_extras(brightway_root))
            additional_cutoff_waste_market_inventories.extend(
                collect_additional_cutoff_waste_market_extras(brightway_root)
            )
            uncontrolled_disposal_inventories.extend(collect_uncontrolled_disposal_extras(brightway_root))
            pet_waste_market_inventories.extend(collect_pet_waste_market_extras(brightway_root))
            five_simple_waste_market_inventories.extend(collect_five_simple_waste_market_extras(brightway_root))
            swiss_pet_fluwa_inventories.extend(collect_swiss_pet_fluwa_extras(brightway_root))
            slf_landfarming_inventories.extend(collect_slf_landfarming_extras(brightway_root))
            average_sludge_landfarming_inventories.extend(collect_average_sludge_landfarming_extras(brightway_root))
            exact_patch_boundary_inventories.extend(collect_exact_patch_boundary_extras(brightway_root))
            complete_patch_boundary_inventories.extend(collect_complete_patch_boundary_extras(brightway_root))
            cable_rail_inventories.extend(collect_cable_rail_extras(brightway_root))
            pet_market_group_inventories.extend(collect_pet_market_group_extras(brightway_root))
            copper_stock_inventories.extend(collect_copper_stock_extras(brightway_root))
            copper_cathode_inventories.extend(collect_copper_cathode_extras(brightway_root))
            pam_frontier_stock_inventories.extend(collect_pam_frontier_stock_extras(brightway_root))
            regional_freight_inventories.extend(collect_regional_freight_extras(brightway_root))
            older_regional_road_inventories.extend(collect_older_regional_road_extras(brightway_root))
            chemical_heat_inventories.extend(collect_chemical_heat_extras(brightway_root))
            whole_freight_inventories.extend(collect_whole_freight_extras(brightway_root))
            packed_wool_inventories.extend(collect_packed_wool_extras(brightway_root))
            sewage_digestion_inventories.extend(collect_sewage_digestion_extras(brightway_root))
            slag_landfill_component_inventories.extend(collect_slag_landfill_component_extras(brightway_root))
            ash_market_inventories.extend(collect_ash_market_extras(brightway_root))
            bottom_ash_inventories.extend(collect_bottom_ash_extras(brightway_root))
        profiles.append(
            {key: entry[key] for key in ("profile", "project", "database")}
            | {
                "references": sorted(references),
                "recovery_credits": sorted(credits, key=lambda row: row["source"]),
                "venting_inventories": sorted(venting_inventories, key=lambda row: row["source"]),
                "disposal_inventories": sorted(disposal_inventories, key=lambda row: row["source"]),
                "reclamation_inventories": sorted(reclamation_inventories, key=lambda row: row["source"]),
                "cutoff_stock_inventories": sorted(cutoff_stock_inventories, key=lambda row: row["source"]),
                "discharge_inventories": sorted(discharge_inventories, key=lambda row: row["source"]),
                "flare_inventories": sorted(flare_inventories, key=lambda row: row["source"]),
                "natural_gas_flare_inventories": sorted(natural_gas_flare_inventories, key=lambda row: row["source"]),
                "cutoff_feed_product_inventories": sorted(
                    cutoff_feed_product_inventories, key=lambda row: row["source"]
                ),
                "eaf_landfill_inventories": sorted(eaf_landfill_inventories, key=lambda row: row["source"]),
                "lcd_treatment_inventories": sorted(lcd_treatment_inventories, key=lambda row: row["source"]),
                "dedicated_disposal_inventories": sorted(dedicated_disposal_inventories, key=lambda row: row["source"]),
                "modal_transport_inventories": sorted(modal_transport_inventories, key=lambda row: row["source"]),
                "antifouling_inventories": sorted(antifouling_inventories, key=lambda row: row["source"]),
                "industrial_gas_inventories": sorted(industrial_gas_inventories, key=lambda row: row["source"]),
                "wwt_residue_inventories": sorted(wwt_residue_inventories, key=lambda row: row["source"]),
                "reverse_gas_inventories": sorted(reverse_gas_inventories, key=lambda row: row["source"]),
                "gas_market_inventories": sorted(gas_market_inventories, key=lambda row: row["source"]),
                "market_composition_inventories": sorted(market_composition_inventories, key=lambda row: row["source"]),
                "esterquat_market_component_inventories": sorted(
                    esterquat_market_component_inventories, key=lambda row: row["source"]
                ),
                "reverse_ash_inventories": sorted(reverse_ash_inventories, key=lambda row: row["source"]),
                "sewer_grid_inventories": sorted(sewer_grid_inventories, key=lambda row: row["source"]),
                "slag_landfill_component_inventories": sorted(
                    slag_landfill_component_inventories, key=lambda row: (row.get("label", ""), row["source"])
                ),
                "ash_market_inventories": sorted(
                    ash_market_inventories, key=lambda row: (row.get("label", ""), row["source"])
                ),
                "bottom_ash_inventories": sorted(
                    bottom_ash_inventories, key=lambda row: (row.get("label", ""), row["source"])
                ),
                "stainless_stock_inventories": sorted(stainless_stock_inventories, key=lambda row: row["source"]),
                "residual_landfill_inventories": sorted(
                    residual_landfill_inventories, key=lambda row: (row.get("label", ""), row["source"])
                ),
                "resolved_child_market_inventories": sorted(
                    resolved_child_market_inventories, key=lambda row: (row.get("label", ""), row["source"])
                ),
                "sewer_throughput_inventories": sorted(
                    sewer_throughput_inventories, key=lambda row: (row.get("label", ""), row["source"])
                ),
                "packaging_paper_landfarming_inventories": sorted(
                    packaging_paper_landfarming_inventories, key=lambda row: (row.get("label", ""), row["source"])
                ),
                "steel_residue_market_inventories": sorted(
                    steel_residue_market_inventories, key=lambda row: (row.get("label", ""), row["source"])
                ),
                "industrial_landfarming_inventories": sorted(
                    industrial_landfarming_inventories, key=lambda row: (row.get("label", ""), row["source"])
                ),
                "industrial_waste_market_inventories": sorted(
                    industrial_waste_market_inventories, key=lambda row: (row.get("label", ""), row["source"])
                ),
                "additional_cutoff_waste_market_inventories": sorted(
                    additional_cutoff_waste_market_inventories, key=lambda row: (row.get("label", ""), row["source"])
                ),
                "uncontrolled_disposal_inventories": sorted(
                    uncontrolled_disposal_inventories, key=lambda row: (row.get("label", ""), row["source"])
                ),
                "pet_waste_market_inventories": sorted(
                    pet_waste_market_inventories, key=lambda row: (row.get("label", ""), row["source"])
                ),
                "five_simple_waste_market_inventories": sorted(
                    five_simple_waste_market_inventories, key=lambda row: (row.get("label", ""), row["source"])
                ),
                "swiss_pet_fluwa_inventories": sorted(
                    swiss_pet_fluwa_inventories, key=lambda row: (row.get("label", ""), row["source"])
                ),
                "slf_landfarming_inventories": sorted(
                    slf_landfarming_inventories, key=lambda row: (row.get("label", ""), row["source"])
                ),
                "average_sludge_landfarming_inventories": sorted(
                    average_sludge_landfarming_inventories, key=lambda row: (row.get("label", ""), row["source"])
                ),
                "exact_patch_boundary_inventories": sorted(
                    exact_patch_boundary_inventories, key=lambda row: (row.get("label", ""), row["source"])
                ),
                "complete_patch_boundary_inventories": sorted(
                    complete_patch_boundary_inventories, key=lambda row: (row.get("label", ""), row["source"])
                ),
                "cable_rail_inventories": sorted(
                    cable_rail_inventories, key=lambda row: (row.get("label", ""), row["source"])
                ),
                "pet_market_group_inventories": sorted(
                    pet_market_group_inventories, key=lambda row: (row.get("label", ""), row["source"])
                ),
                "packed_wool_inventories": sorted(
                    packed_wool_inventories, key=lambda row: (row.get("label", ""), row["source"])
                ),
                "whole_freight_inventories": sorted(
                    whole_freight_inventories, key=lambda row: (row.get("label", ""), row["source"])
                ),
                "chemical_heat_inventories": sorted(
                    chemical_heat_inventories, key=lambda row: (row.get("label", ""), row["source"])
                ),
                "pam_frontier_stock_inventories": sorted(
                    pam_frontier_stock_inventories, key=lambda row: (row.get("label", ""), row["source"])
                ),
                "regional_freight_inventories": sorted(
                    regional_freight_inventories, key=lambda row: (row.get("label", ""), row["source"])
                ),
                "older_regional_road_inventories": sorted(
                    older_regional_road_inventories, key=lambda row: (row.get("label", ""), row["source"])
                ),
                "copper_cathode_inventories": sorted(
                    copper_cathode_inventories, key=lambda row: (row.get("label", ""), row["source"])
                ),
                "copper_stock_inventories": sorted(
                    copper_stock_inventories, key=lambda row: (row.get("label", ""), row["source"])
                ),
                "sewage_digestion_inventories": sorted(
                    sewage_digestion_inventories, key=lambda row: (row.get("label", ""), row["source"])
                ),
                "cutoff_slag_product_inventories": sorted(
                    cutoff_slag_product_inventories, key=lambda row: row["source"]
                ),
                "processing_composition_inventories": sorted(
                    processing_composition_inventories, key=lambda row: row["source"]
                ),
            }
        )
    if any(tuple(row[:4]) == CONCRETE_GRADE_SOURCE for profile in profiles for row in profile["references"]):
        profiles[0]["concrete_grade_inventories"] = collect_concrete_grade_inventories(brightway_root)
    if any(tuple(row[:4]) in SOURCE_GAS_FLARES for profile in profiles for row in profile["references"]):
        profiles[0]["source_gas_flare_inventories"] = collect_source_gas_flare_inventories(brightway_root)
    if any(tuple(row[:4]) in PRECIOUS_ANODE_MARKETS for profile in profiles for row in profile["references"]):
        profiles[0]["precious_anode_inventories"] = collect_precious_anode_inventories(brightway_root)
    if any(tuple(row[:4]) == METALLIFEROUS_STOCK for profile in profiles for row in profile["references"]):
        profiles[0]["metalliferous_stock_inventories"] = collect_metalliferous_stock_inventories(brightway_root)
        profiles[0]["metalliferous_stock_market_inventories"] = collect_metalliferous_market_inventories(brightway_root)
    if any(tuple(row[:4]) in PP_UNCONTROLLED_SOURCES for profile in profiles for row in profile["references"]):
        profiles[0]["pp_uncontrolled_source_inventories"] = collect_pp_uncontrolled_inventories(brightway_root)
        profiles[0]["ghana_uncontrolled_inventories"] = collect_ghana_uncontrolled_inventories(brightway_root)
        profiles[0]["refrigerant_patch_formula_inventories"] = collect_refrigerant_patch_inventories(brightway_root)
        profiles[0]["digester_stock_inventories"] = collect_digester_stock_inventories(brightway_root)
        profiles[0]["swiss_cement_market_inventories"] = collect_swiss_cement_inventories(brightway_root)
        profiles[0]["copper_ash_inventories"] = collect_copper_ash_inventories(brightway_root)
        profiles[0]["waste_concrete_inert_inventories"] = collect_waste_concrete_inert_inventories(brightway_root)
        profiles[0]["older_pet_whole_freight_inventories"] = collect_older_pet_whole_freight_inventories(brightway_root)
    return profiles


def prior_recipe_review(resource, rule, prior):
    """Read retained authoring evidence without dereferencing a pruned runtime review."""
    if "original_review" in prior:
        return prior["original_review"]
    return rule_review(resource, rule) if "review_ref" in rule else None


def build_review(root, profiles):
    """Audit signed production amounts and independently verified native waste roles.

    Profiles contain sparse reference metadata only, collected from installed
    Brightway databases. No technosphere or biosphere recipes are retained.
    """
    previous_path = root / REVIEW_PATH
    previous = json.loads(previous_path.read_text(encoding="utf-8"))["resources"] if previous_path.exists() else {}
    expected_profiles = {
        ("ecoinvent", version, model)
        for version in ("3.6", "3.7", "3.8", "3.9", "3.10", "3.11", "3.12")
        for model in ("cutoff", "consequential")
    } | {("uvek", "2025", "cutoff")}
    if (
        len(profiles) != len(expected_profiles)
        or {tuple(profile["profile"]) for profile in profiles} != expected_profiles
    ):
        raise ValueError("The sign audit requires all 14 source profiles and the UVEK 2025 background.")
    observations = defaultdict(lambda: defaultdict(set))
    credit_quantities = defaultdict(list)
    venting_inventories = defaultdict(list)
    disposal_inventories = defaultdict(list)
    reclamation_inventories = defaultdict(list)
    cutoff_stock_inventories = defaultdict(list)
    discharge_inventories = defaultdict(list)
    flare_inventories = defaultdict(list)
    natural_gas_flare_inventories = defaultdict(list)
    cutoff_feed_product_inventories = defaultdict(list)
    eaf_landfill_inventories = defaultdict(list)
    lcd_treatment_inventories = defaultdict(list)
    dedicated_disposal_inventories = defaultdict(list)
    modal_transport_inventories = defaultdict(list)
    antifouling_inventories = defaultdict(list)
    industrial_gas_inventories = defaultdict(list)
    wwt_residue_inventories = defaultdict(list)
    reverse_gas_inventories = defaultdict(list)
    gas_market_inventories = defaultdict(list)
    market_composition_inventories = defaultdict(list)
    esterquat_market_component_inventories = defaultdict(list)
    reverse_ash_inventories = defaultdict(list)
    processing_composition_inventories = defaultdict(list)
    cutoff_slag_product_inventories = defaultdict(list)
    sewer_grid_inventories = defaultdict(list)
    slag_landfill_component_inventories = defaultdict(list)
    ash_market_inventories = defaultdict(list)
    bottom_ash_inventories = defaultdict(list)
    stainless_stock_inventories = defaultdict(list)
    residual_landfill_inventories = defaultdict(list)
    resolved_child_market_inventories = defaultdict(list)
    sewer_throughput_inventories = defaultdict(list)
    packaging_paper_landfarming_inventories = defaultdict(list)
    steel_residue_market_inventories = defaultdict(list)
    industrial_landfarming_inventories = defaultdict(list)
    industrial_waste_market_inventories = defaultdict(list)
    additional_cutoff_waste_market_inventories = defaultdict(list)
    uncontrolled_disposal_inventories = defaultdict(list)
    pet_waste_market_inventories = defaultdict(list)
    five_simple_waste_market_inventories = defaultdict(list)
    swiss_pet_fluwa_inventories = defaultdict(list)
    slf_landfarming_inventories = defaultdict(list)
    average_sludge_landfarming_inventories = defaultdict(list)
    exact_patch_boundary_inventories = defaultdict(list)
    complete_patch_boundary_inventories = defaultdict(list)
    cable_rail_inventories = defaultdict(list)
    pet_market_group_inventories = defaultdict(list)
    copper_stock_inventories = defaultdict(list)
    copper_cathode_inventories = defaultdict(list)
    pam_frontier_stock_inventories = defaultdict(list)
    regional_freight_inventories = defaultdict(list)
    older_regional_road_inventories = defaultdict(list)
    chemical_heat_inventories = defaultdict(list)
    whole_freight_inventories = defaultdict(list)
    packed_wool_inventories = defaultdict(list)
    sewage_digestion_inventories = defaultdict(list)
    for profile in profiles:
        family = profile["profile"][0]
        label = "-".join(profile["profile"])
        for record in profile.get("residual_landfill_inventories", []):
            own_label = record.get("label", label)
            own_source = tuple(record["source"])
            residual_landfill_inventories[own_source].append({**record, "label": own_label})
            if own_source in RESIDUAL_LANDFILLS:
                observations["ecoinvent"][own_source].add((own_label, record["reference_amount"]))
        for record in profile.get("resolved_child_market_inventories", []):
            own_label = record.get("label", label)
            own_source = tuple(record["source"])
            resolved_child_market_inventories[own_source].append({**record, "label": own_label})
            if own_source in RESOLVED_CHILD_MARKETS:
                observations["ecoinvent"][own_source].add((own_label, record["reference_amount"]))
        for record in profile.get("sewer_throughput_inventories", []):
            own_label = record.get("label", label)
            own_source = tuple(record["source"])
            sewer_throughput_inventories[own_source].append({**record, "label": own_label})
            if own_source in SEWER_THROUGHPUT_SOURCES:
                observations["ecoinvent"][own_source].add((own_label, record["reference_amount"]))
        for record in profile.get("packaging_paper_landfarming_inventories", []):
            own_label = record.get("label", label)
            own_source = tuple(record["source"])
            packaging_paper_landfarming_inventories[own_source].append({**record, "label": own_label})
            if own_source in PACKAGING_PAPER_LANDFARMING_SOURCES:
                observations["ecoinvent"][own_source].add((own_label, record["reference_amount"]))
        for record in profile.get("steel_residue_market_inventories", []):
            own_label = record.get("label", label)
            own_source = tuple(record["source"])
            steel_residue_market_inventories[own_source].append({**record, "label": own_label})
            if own_source in STEEL_RESIDUE_MARKET_SOURCES:
                observations["ecoinvent"][own_source].add((own_label, record["reference_amount"]))
        for record in profile.get("industrial_landfarming_inventories", []):
            own_label = record.get("label", label)
            own_source = tuple(record["source"])
            industrial_landfarming_inventories[own_source].append({**record, "label": own_label})
            if own_source in INDUSTRIAL_LANDFARMING_SOURCES:
                observations["ecoinvent"][own_source].add((own_label, record["reference_amount"]))
        for record in profile.get("industrial_waste_market_inventories", []):
            own_label = record.get("label", label)
            own_source = tuple(record["source"])
            industrial_waste_market_inventories[own_source].append({**record, "label": own_label})
            if own_source in INDUSTRIAL_WASTE_MARKET_SOURCES:
                observations["ecoinvent"][own_source].add((own_label, record["reference_amount"]))
        for record in profile.get("additional_cutoff_waste_market_inventories", []):
            own_label = record.get("label", label)
            own_source = tuple(record["source"])
            additional_cutoff_waste_market_inventories[own_source].append({**record, "label": own_label})
            if own_source in ADDITIONAL_CUTOFF_WASTE_MARKET_SOURCES:
                observations["ecoinvent"][own_source].add((own_label, record["reference_amount"]))
        for record in profile.get("uncontrolled_disposal_inventories", []):
            own_label = record.get("label", label)
            own_source = tuple(record["source"])
            uncontrolled_disposal_inventories[own_source].append({**record, "label": own_label})
            if own_source in UNCONTROLLED_DISPOSAL_SOURCES:
                observations["ecoinvent"][own_source].add((own_label, record["reference_amount"]))
        for record in profile.get("pet_waste_market_inventories", []):
            own_label = record.get("label", label)
            own_source = tuple(record["source"])
            pet_waste_market_inventories[own_source].append({**record, "label": own_label})
            if own_source in PET_WASTE_MARKET_SOURCES:
                observations["ecoinvent"][own_source].add((own_label, record["reference_amount"]))
        for record in profile.get("five_simple_waste_market_inventories", []):
            own_label = record.get("label", label)
            own_source = tuple(record["source"])
            five_simple_waste_market_inventories[own_source].append({**record, "label": own_label})
            if own_source in FIVE_SIMPLE_WASTE_MARKET_SOURCES:
                observations["ecoinvent"][own_source].add((own_label, record["reference_amount"]))
        for record in profile.get("swiss_pet_fluwa_inventories", []):
            own_label = record.get("label", label)
            own_source = tuple(record["source"])
            swiss_pet_fluwa_inventories[own_source].append({**record, "label": own_label})
            if own_source in SWISS_PET_FLUWA_SOURCES:
                observations["ecoinvent"][own_source].add((own_label, record["reference_amount"]))
        for record in profile.get("slf_landfarming_inventories", []):
            own_label = record.get("label", label)
            own_source = tuple(record["source"])
            slf_landfarming_inventories[own_source].append({**record, "label": own_label})
            if own_source in SLF_LANDFARMING_SOURCES:
                observations["ecoinvent"][own_source].add((own_label, record["reference_amount"]))
        for record in profile.get("average_sludge_landfarming_inventories", []):
            own_label = record.get("label", label)
            own_source = tuple(record["source"])
            average_sludge_landfarming_inventories[own_source].append({**record, "label": own_label})
            if own_source in AVERAGE_SLUDGE_LANDFARMING_SOURCES:
                observations["ecoinvent"][own_source].add((own_label, record["reference_amount"]))
        for record in profile.get("exact_patch_boundary_inventories", []):
            own_label = record.get("label", label)
            own_source = tuple(record["source"])
            exact_patch_boundary_inventories[own_source].append({**record, "label": own_label})
            if (own_label, own_source) in EXACT_PATCH_BOUNDARY_CASES:
                observations["ecoinvent"][own_source].add((own_label, record["reference_amount"]))
        for record in profile.get("complete_patch_boundary_inventories", []):
            own_label = record.get("label", label)
            own_source = tuple(record["source"])
            complete_patch_boundary_inventories[own_source].append({**record, "label": own_label})
            if (own_label, own_source) in COMPLETE_PATCH_BOUNDARY_CASES:
                observations["ecoinvent"][own_source].add((own_label, record["reference_amount"]))
        for record in profile.get("cable_rail_inventories", []):
            own_label = record.get("label", label)
            own_source = tuple(record["source"])
            cable_rail_inventories[own_source].append({**record, "label": own_label})
        for record in profile.get("pet_market_group_inventories", []):
            own_label = record.get("label", label)
            own_source = tuple(record["source"])
            pet_market_group_inventories[own_source].append({**record, "label": own_label})
            if own_source in PET_MARKET_GROUP_SOURCES:
                observations["ecoinvent"][own_source].add((own_label, record["reference_amount"]))
        for record in profile.get("packed_wool_inventories", []):
            own_label = record.get("label", label)
            own_source = tuple(record["source"])
            packed_wool_inventories[own_source].append({**record, "label": own_label})
            observations[own_label.split("-")[0]][own_source].add((own_label, record["reference_amount"]))
        for record in profile.get("whole_freight_inventories", []):
            own_label = record.get("label", label)
            own_source = tuple(record["source"])
            whole_freight_inventories[own_source].append({**record, "label": own_label})
            observations[own_label.split("-")[0]][own_source].add((own_label, record["reference_amount"]))
        for record in profile.get("chemical_heat_inventories", []):
            own_label = record.get("label", label)
            own_source = tuple(record["source"])
            chemical_heat_inventories[own_source].append({**record, "label": own_label})
            observations[own_label.split("-")[0]][own_source].add((own_label, record["reference_amount"]))
        for record in profile.get("pam_frontier_stock_inventories", []):
            own_label = record.get("label", label)
            own_source = tuple(record["source"])
            pam_frontier_stock_inventories[own_source].append({**record, "label": own_label})
            observations[own_label.split("-")[0]][own_source].add((own_label, record["reference_amount"]))
        for record in profile.get("regional_freight_inventories", []):
            own_label = record.get("label", label)
            own_source = tuple(record["source"])
            regional_freight_inventories[own_source].append({**record, "label": own_label})
            observations[own_label.split("-")[0]][own_source].add((own_label, record["reference_amount"]))
        for record in profile.get("older_regional_road_inventories", []):
            own_label = record.get("label", label)
            own_source = tuple(record["source"])
            older_regional_road_inventories[own_source].append({**record, "label": own_label})
            observations[own_label.split("-")[0]][own_source].add((own_label, record["reference_amount"]))
        for record in profile.get("copper_cathode_inventories", []):
            own_label = record.get("label", label)
            own_source = tuple(record["source"])
            copper_cathode_inventories[own_source].append({**record, "label": own_label})
            observations[own_label.split("-")[0]][own_source].add((own_label, record["reference_amount"]))
        for record in profile.get("copper_stock_inventories", []):
            own_label = record.get("label", label)
            own_source = tuple(record["source"])
            copper_stock_inventories[own_source].append({**record, "label": own_label})
            if own_source in COPPER_STOCK_SOURCES:
                observations["ecoinvent"][own_source].add((own_label, record["reference_amount"]))
        for record in profile.get("sewage_digestion_inventories", []):
            sewage_digestion_inventories[tuple(record["source"])].append(
                {**record, "label": record.get("label", label)}
            )
        for record in profile.get("slag_landfill_component_inventories", []):
            own_label = record.get("label", label)
            slag_landfill_component_inventories[tuple(record["source"])].append({**record, "label": own_label})
            if "reference_amount" in record:
                observations[own_label.split("-")[0]][tuple(record["source"])].add(
                    (own_label, record["reference_amount"])
                )
        for record in profile.get("ash_market_inventories", []):
            own_label = record.get("label", label)
            ash_market_inventories[tuple(record["source"])].append({**record, "label": own_label})
            if "reference_amount" in record:
                observations[own_label.split("-")[0]][tuple(record["source"])].add(
                    (own_label, record["reference_amount"])
                )
        for record in profile.get("bottom_ash_inventories", []):
            own_label = record.get("label", label)
            bottom_ash_inventories[tuple(record["source"])].append({**record, "label": own_label})
            if "reference_amount" in record:
                observations[own_label.split("-")[0]][tuple(record["source"])].add(
                    (own_label, record["reference_amount"])
                )
        for record in profile.get("stainless_stock_inventories", []):
            stainless_stock_inventories[tuple(record["source"])].append({**record, "label": label})
        for record in profile.get("sewer_grid_inventories", []):
            sewer_grid_inventories[tuple(record["source"])].append({**record, "label": label})
        for record in profile.get("cutoff_slag_product_inventories", []):
            cutoff_slag_product_inventories[tuple(record["source"])].append({**record, "label": label})
        for record in profile.get("cutoff_feed_product_inventories", []):
            cutoff_feed_product_inventories[tuple(record["source"])].append({**record, "label": label})
        for record in profile.get("natural_gas_flare_inventories", []):
            natural_gas_flare_inventories[tuple(record["source"])].append({**record, "label": label})
        for record in profile.get("flare_inventories", []):
            flare_inventories[tuple(record["source"])].append((label, record["inventory_sha256"]))
        for record in profile.get("eaf_landfill_inventories", []):
            eaf_landfill_inventories[tuple(record["source"])].append((label, record["inventory_sha256"]))
        for record in profile.get("lcd_treatment_inventories", []):
            lcd_treatment_inventories[tuple(record["source"])].append((label, record["inventory_sha256"]))
        for record in profile.get("dedicated_disposal_inventories", []):
            dedicated_disposal_inventories[tuple(record["source"])].append((label, record["inventory_sha256"]))
        for record in profile.get("modal_transport_inventories", []):
            modal_transport_inventories[tuple(record["source"])].append((label, record["inventory_sha256"]))
        for record in profile.get("reverse_gas_inventories", []):
            reverse_gas_inventories[tuple(record["source"])].append((label, record["inventory_sha256"]))
        for record in profile.get("gas_market_inventories", []):
            gas_market_inventories[tuple(record["source"])].append((label, record["inventory_sha256"]))
        for record in profile.get("esterquat_market_component_inventories", []):
            esterquat_market_component_inventories[tuple(record["source"])].append((label, record["inventory_sha256"]))
        for record in profile.get("market_composition_inventories", []):
            market_composition_inventories[tuple(record["source"])].append((label, record["inventory_sha256"]))
        for record in profile.get("reverse_ash_inventories", []):
            reverse_ash_inventories[tuple(record["source"])].append((label, record["inventory_sha256"]))
        for record in profile.get("processing_composition_inventories", []):
            processing_composition_inventories[tuple(record["source"])].append((label, record["inventory_sha256"]))
        for record in profile.get("wwt_residue_inventories", []):
            wwt_residue_inventories[tuple(record["source"])].append((label, record["inventory_sha256"]))
        for record in profile.get("industrial_gas_inventories", []):
            industrial_gas_inventories[tuple(record["source"])].append((label, record["inventory_sha256"]))
        for record in profile.get("antifouling_inventories", []):
            antifouling_inventories[tuple(record["source"])].append((label, record["inventory_sha256"]))
        for record in profile.get("discharge_inventories", []):
            discharge_inventories[tuple(record["source"])].append((label, record["inventory_sha256"]))
        for record in profile.get("reclamation_inventories", []):
            reclamation_inventories[tuple(record["source"])].append({**record, "label": label})
        for record in profile.get("cutoff_stock_inventories", []):
            cutoff_stock_inventories[tuple(record["source"])].append({**record, "label": label})
        for record in profile.get("venting_inventories", []):
            venting_inventories[tuple(record["source"])].append((label, record["inventory_sha256"]))
        for record in profile.get("disposal_inventories", []):
            disposal_inventories[tuple(record["source"])].append((label, record["inventory_sha256"]))
        for record in profile.get("recovery_credits", []):
            credit_quantities[tuple(record["source"])].append((label, record))
        catalog_name = "__".join(profile["profile"]) + ".json"
        catalog_path = root / "brightpath/data/export/reference_catalogs" / catalog_name
        catalog = json.loads(catalog_path.read_text(encoding="utf-8"))
        expected = {
            (row["name"], row["reference_product"], row["location"], row["unit"]) for row in catalog["technosphere"]
        }
        actual = {tuple(row[:4]) for row in profile["references"]}
        if actual != expected or len(actual) != len(profile["references"]):
            raise ValueError(f"Reference sign coverage disagrees with the exact catalog: {label}")
        for *record, sign in profile["references"]:
            if type(sign) is not int or sign not in (-1, 1):
                raise ValueError("Invalid reference-flow sign evidence.")
            observations[family][tuple(record)].add((label, sign))
    extend_cable_parent_patch_evidence(observations, market_composition_inventories, cable_rail_inventories)
    extend_uncontrolled_companion_market_evidence(
        observations, market_composition_inventories, uncontrolled_disposal_inventories
    )
    native_path = root / "brightpath/data/export/openlca_references/uvek__2025__cutoff__ecoinvent__3.10.json"
    native_bytes = native_path.read_bytes()
    native = {
        (row["name"], row["reference_product"], row["location"], row["unit"]): row
        for row in json.loads(native_bytes)["technosphere"]
    }
    validate_esterquat_providers(esterquat_market_component_inventories, observations, native)
    validate_cutoff_feed_inventories(cutoff_feed_product_inventories, observations, native)
    concrete_grade_inventories = [row for profile in profiles for row in profile.get("concrete_grade_inventories", [])]
    for row in concrete_grade_inventories:
        if tuple(row["source"][f] for f in FIELDS) == CONCRETE_GRADE_SOURCE:
            observations["ecoinvent"][CONCRETE_GRADE_SOURCE].add((row["label"], row["reference_amount"]))
    if CONCRETE_GRADE_SOURCE in observations["ecoinvent"]:
        validate_concrete_grade_inventories(concrete_grade_inventories, observations, native)
    source_gas_flare_inventories = [
        row for profile in profiles for row in profile.get("source_gas_flare_inventories", [])
    ]
    extend_source_gas_flare_observations(source_gas_flare_inventories, observations)
    if any(s in observations["ecoinvent"] for s in SOURCE_GAS_FLARES):
        validate_source_gas_flare_inventories(source_gas_flare_inventories, observations, native)
    precious_anode_inventories = [row for profile in profiles for row in profile.get("precious_anode_inventories", [])]
    extend_precious_anode_observations(precious_anode_inventories, observations)
    if any(s in observations["ecoinvent"] for s in PRECIOUS_ANODE_MARKETS):
        validate_precious_anode_inventories(precious_anode_inventories, observations, native)
    metalliferous_stock_inventories = [
        row for profile in profiles for row in profile.get("metalliferous_stock_inventories", [])
    ]
    extend_metalliferous_stock_observations(metalliferous_stock_inventories, observations)
    if METALLIFEROUS_STOCK in observations["ecoinvent"]:
        validate_metalliferous_stock_inventories(metalliferous_stock_inventories, observations)
        parent_records = [
            row for profile in profiles for row in profile.get("metalliferous_stock_market_inventories", [])
        ]
        validate_metalliferous_market_inventories(parent_records)
        for row in parent_records:
            source = identity(row["source"])
            observations["ecoinvent"][source].add((row["label"], 1))
            entry = (row["label"], row["inventory_sha256"])
            existing = [p for p in market_composition_inventories[source] if p[0] == row["label"]]
            if existing:
                if existing != [entry]:
                    raise ValueError("Conflicting metalliferous parent patch fingerprints.")
            else:
                market_composition_inventories[source].append(entry)

    pp_uncontrolled_inventories = [
        row for profile in profiles for row in profile.get("pp_uncontrolled_source_inventories", [])
    ]
    extend_pp_uncontrolled_observations(pp_uncontrolled_inventories, observations)
    if any(s in observations["ecoinvent"] for s in PP_UNCONTROLLED_SOURCES):
        validate_pp_uncontrolled_inventories(pp_uncontrolled_inventories, observations)
    ghana_uncontrolled_inventories = [
        row for profile in profiles for row in profile.get("ghana_uncontrolled_inventories", [])
    ]
    extend_ghana_uncontrolled_observations(ghana_uncontrolled_inventories, observations)
    if any(s in observations["ecoinvent"] for s in GHANA_UNCONTROLLED_SOURCES):
        validate_ghana_uncontrolled_inventories(ghana_uncontrolled_inventories, observations)
        from brightpath.migrations.resources import load_uvek_technosphere_resource

        validate_ghana_uncontrolled_bindings(load_uvek_technosphere_resource())
    refrigerant_patch_inventories = [
        row for profile in profiles for row in profile.get("refrigerant_patch_formula_inventories", [])
    ]
    extend_refrigerant_patch_observations(refrigerant_patch_inventories, observations)
    if any(source in observations["ecoinvent"] for source in REFRIGERANT_PATCH_SOURCES):
        validate_refrigerant_patch_inventories(refrigerant_patch_inventories)
        from brightpath.migrations.resources import load_uvek_technosphere_resource

        current = load_uvek_technosphere_resource()
        validate_refrigerant_patch_bindings(current, previous[current["name"]])
    older_pet_whole_freight_inventories = [
        row for profile in profiles for row in profile.get("older_pet_whole_freight_inventories", [])
    ]
    if older_pet_whole_freight_inventories or any(
        source in observations["ecoinvent"] for source in OLDER_PET_WHOLE_FREIGHT_SOURCES
    ):
        validate_older_pet_whole_freight_inventories(older_pet_whole_freight_inventories, observations, native)
        from brightpath.migrations.resources import load_uvek_technosphere_resource

        validate_older_pet_whole_freight_bindings(load_uvek_technosphere_resource())
    waste_concrete_inert_inventories = [
        row for profile in profiles for row in profile.get("waste_concrete_inert_inventories", [])
    ]
    if waste_concrete_inert_inventories or any(
        source in observations["ecoinvent"] for source in WASTE_CONCRETE_INERT_SOURCES
    ):
        validate_waste_concrete_inert_inventories(waste_concrete_inert_inventories, observations, native)
        from brightpath.migrations.resources import load_uvek_technosphere_resource

        validate_waste_concrete_inert_bindings(load_uvek_technosphere_resource())
    copper_ash_inventories = [row for profile in profiles for row in profile.get("copper_ash_inventories", [])]
    if copper_ash_inventories or any(source in observations["ecoinvent"] for source in COPPER_ASH_SOURCES):
        validate_copper_ash_inventories(copper_ash_inventories, observations, native)
        from brightpath.migrations.resources import load_uvek_technosphere_resource

        validate_copper_ash_bindings(load_uvek_technosphere_resource())
    swiss_cement_inventories = [
        row for profile in profiles for row in profile.get("swiss_cement_market_inventories", [])
    ]
    if swiss_cement_inventories or any(source in observations["ecoinvent"] for source in SWISS_CEMENT_MARKETS):
        validate_swiss_cement_inventories(swiss_cement_inventories, observations, native)
        from brightpath.migrations.resources import load_uvek_technosphere_resource

        validate_swiss_cement_bindings(load_uvek_technosphere_resource())
    digester_stock_inventories = [row for profile in profiles for row in profile.get("digester_stock_inventories", [])]
    if any(source in observations["ecoinvent"] for source in DIGESTER_STOCK_SOURCES):
        validate_digester_stock_inventories(digester_stock_inventories, observations)
        from brightpath.migrations.resources import load_uvek_technosphere_resource

        current = load_uvek_technosphere_resource()
        validate_digester_stock_bindings(current, previous[current["name"]])
    resources, summary = {}, {}
    for filename in (FORWARD, REVERSE):
        resource = json.loads((root / "brightpath/data/migrations/uvek" / filename).read_text(encoding="utf-8"))
        forward = filename == FORWARD
        if (
            forward
            and resource["name"] == "ecoinvent-3.x-to-uvek-2025-technosphere"
            and all(profile["system_model"] == "cutoff" for profile in resource["source_profiles"])
        ):
            # Replay historical fingerprints, including retired consequential
            # observations, independently of the executable cut-off projection.
            if __package__:
                from scripts.generate_uvek_migration_resources import _build_historical_review_resources
            else:
                from generate_uvek_migration_resources import _build_historical_review_resources
            resource, _ = _build_historical_review_resources(
                root / "brightpath/data/export/reference_catalogs",
                root / "brightpath/data/export/ecoinvent_to_uvek_mapping.csv",
                root / "brightpath/data/export/uvek_conversion_factors.yaml",
            )
        if forward and any(identity(r["source"]) == PACKED_WOOL_SOURCE for r in resource["replace"]):
            validate_packed_wool_inventories(packed_wool_inventories, observations, native)
            validate_packed_wool_bindings(resource)
        if forward and any(identity(r["source"]) in WHOLE_FREIGHT_SOURCES for r in resource["replace"]):
            validate_whole_freight_inventories(whole_freight_inventories, observations, native)
            validate_whole_freight_bindings(resource)
        if forward and any(identity(r["source"]) in CHEMICAL_HEAT_SOURCES for r in resource["replace"]):
            validate_chemical_heat_inventories(chemical_heat_inventories, observations, native)
            validate_chemical_heat_bindings(resource)
        if forward and any(identity(r["source"]) in PAM_FRONTIER_STOCK_SOURCES for r in resource["replace"]):
            validate_pam_frontier_stock_inventories(pam_frontier_stock_inventories, observations, native)
            validate_pam_frontier_stock_bindings(resource)
        if forward and any(identity(r["source"]) in REGIONAL_FREIGHT_SOURCES for r in resource["replace"]):
            validate_regional_freight_inventories(regional_freight_inventories, observations, native)
            validate_regional_freight_bindings(resource)
        if forward and any(identity(r["source"]) in OLDER_REGIONAL_ROAD_SOURCES for r in resource["replace"]):
            validate_older_regional_road_inventories(older_regional_road_inventories, observations, native)
            validate_older_regional_road_bindings(resource)
        if forward and any(identity(r["source"]) == COPPER_CATHODE_SOURCE for r in resource["replace"]):
            validate_copper_cathode_inventories(copper_cathode_inventories, observations, native)
        if forward and any(identity(r["source"]) in SEWER_GRIDS for r in resource["replace"]):
            validate_sewer_grid_inventories(sewer_grid_inventories, observations, native)
        if forward and any(identity(r["source"]) in SLAG_LANDFILL_COMPONENTS for r in resource["replace"]):
            validate_slag_landfill_component_inventories(slag_landfill_component_inventories, observations, native)
        if any(
            (forward and identity(r["source"]) in ASH_MARKET_SOURCES)
            or r.get("mapping_method") == ASH_MARKET_REVERSE_METHOD
            for r in resource["replace"]
        ):
            validate_ash_market_inventories(ash_market_inventories, observations, native)
            if forward:
                validate_ash_market_bindings(resource)
        if forward and any(identity(r["source"]) in RESIDUAL_LANDFILLS for r in resource["replace"]):
            validate_residual_landfill_inventories(residual_landfill_inventories, observations, native)
            validate_residual_landfill_bindings(resource)
        if forward and any(identity(r["source"]) in RESOLVED_CHILD_MARKETS for r in resource["replace"]):
            validate_resolved_child_market_inventories(resolved_child_market_inventories, observations, native)
            validate_resolved_child_market_bindings(resource)
        if forward and any(identity(r["source"]) in SEWER_THROUGHPUT_SOURCES for r in resource["replace"]):
            validate_sewer_throughput_inventories(sewer_throughput_inventories, observations, native)
            validate_sewer_throughput_bindings(resource)
        if forward and any(identity(r["source"]) in PACKAGING_PAPER_LANDFARMING_SOURCES for r in resource["replace"]):
            validate_packaging_paper_landfarming_inventories(
                packaging_paper_landfarming_inventories, observations, native
            )
            validate_packaging_paper_landfarming_bindings(resource)
        if forward and any(identity(r["source"]) in STEEL_RESIDUE_MARKET_SOURCES for r in resource["replace"]):
            validate_steel_residue_market_inventories(steel_residue_market_inventories, observations, native)
            validate_steel_residue_market_bindings(resource)
        if forward and any(identity(r["source"]) in INDUSTRIAL_LANDFARMING_SOURCES for r in resource["replace"]):
            validate_industrial_landfarming_inventories(industrial_landfarming_inventories, observations, native)
            validate_industrial_landfarming_bindings(resource)
        if forward and any(identity(r["source"]) in INDUSTRIAL_WASTE_MARKET_SOURCES for r in resource["replace"]):
            validate_industrial_waste_market_inventories(industrial_waste_market_inventories, observations, native)
            validate_industrial_waste_market_bindings(resource)
        if forward and any(
            identity(r["source"]) in ADDITIONAL_CUTOFF_WASTE_MARKET_SOURCES for r in resource["replace"]
        ):
            validate_additional_cutoff_waste_market_inventories(
                additional_cutoff_waste_market_inventories, observations, native
            )
            validate_additional_cutoff_waste_market_bindings(resource)
        if forward and any(identity(r["source"]) in UNCONTROLLED_DISPOSAL_SOURCES for r in resource["replace"]):
            validate_uncontrolled_disposal_inventories(uncontrolled_disposal_inventories, observations, native)
            validate_uncontrolled_disposal_bindings(resource)
        if forward and any(identity(r["source"]) in PET_WASTE_MARKET_SOURCES for r in resource["replace"]):
            validate_pet_waste_market_inventories(pet_waste_market_inventories, observations, native)
            validate_pet_waste_market_bindings(resource)
        if forward and any(identity(r["source"]) in FIVE_SIMPLE_WASTE_MARKET_SOURCES for r in resource["replace"]):
            validate_five_simple_waste_market_inventories(five_simple_waste_market_inventories, observations, native)
            validate_five_simple_waste_market_bindings(resource)
        if forward and any(identity(r["source"]) in SWISS_PET_FLUWA_SOURCES for r in resource["replace"]):
            validate_swiss_pet_fluwa_inventories(swiss_pet_fluwa_inventories, observations, native)
            validate_swiss_pet_fluwa_bindings(resource)
        if forward and any(identity(r["source"]) in SLF_LANDFARMING_SOURCES for r in resource["replace"]):
            validate_slf_landfarming_inventories(slf_landfarming_inventories, observations, native)
            validate_slf_landfarming_bindings(resource)
        if forward and any(identity(r["source"]) in EXACT_PATCH_BOUNDARY_SOURCES for r in resource["replace"]):
            validate_exact_patch_boundary_inventories(exact_patch_boundary_inventories)
            validate_exact_patch_boundary_bindings(resource)
            validate_complete_patch_boundary_inventories(complete_patch_boundary_inventories)
            validate_complete_patch_boundary_bindings(resource)
        if forward and any(identity(r["source"]) in AVERAGE_SLUDGE_LANDFARMING_SOURCES for r in resource["replace"]):
            validate_average_sludge_landfarming_inventories(
                average_sludge_landfarming_inventories, observations, native
            )
            validate_average_sludge_landfarming_bindings(resource)
        if forward and any(identity(r["source"]) in PET_MARKET_GROUP_SOURCES for r in resource["replace"]):
            validate_pet_market_group_inventories(pet_market_group_inventories, observations, native)
            validate_pet_market_group_bindings(resource)
        if forward and any(identity(r["source"]) in COPPER_STOCK_SOURCES for r in resource["replace"]):
            validate_copper_stock_inventories(copper_stock_inventories, observations, native)
            validate_copper_stock_bindings(resource)
        if forward and any(identity(r["source"]) in BOTTOM_ASH_SOURCES for r in resource["replace"]):
            validate_bottom_ash_inventories(bottom_ash_inventories, observations["ecoinvent"])
            validate_bottom_ash_bindings(resource)
        if forward and any(identity(r["source"]) in STAINLESS_STOCKS for r in resource["replace"]):
            validate_stainless_stock_inventories(stainless_stock_inventories, observations, native)
        if any(
            (forward and identity(r["source"]) in (SEWAGE_DIGESTION_SOURCES | SEWAGE_SLUDGE_MARKETS))
            or r.get("mapping_method") == SEWAGE_DIGESTION_REVERSE_METHOD
            for r in resource["replace"]
        ):
            validate_sewage_digestion_inventories(sewage_digestion_inventories, observations, native)
            if forward:
                validate_sewage_digestion_bindings(resource)
        if forward and any(identity(r["source"]) in GAS_SOURCES for r in resource["replace"]):
            validate_credit_correspondences(resource)
        if not forward and any(r.get("mapping_method") == REVERSE_GAS_METHOD for r in resource["replace"]):
            validate_reverse_gas_inventories(reverse_gas_inventories)
        if not forward and any(r.get("mapping_method") == REVERSE_ASH_METHOD for r in resource["replace"]):
            validate_reverse_ash_inventories(reverse_ash_inventories)
        if forward and any(identity(r["source"]) in MARKETS for r in resource["replace"]):
            validate_market_dependencies(resource)
        if forward and any(identity(r["source"]) in COMPOSITIONS for r in resource["replace"]):
            validate_composition_bindings(resource)
        if any(
            identity(r["source"]) in CUTOFF_SLAG_PRODUCTS or r.get("mapping_method") == CUTOFF_SLAG_REVERSE_METHOD
            for r in resource["replace"]
        ):
            validate_cutoff_slag_inventories(cutoff_slag_product_inventories, observations, native)
            if forward:
                validate_cutoff_slag_bindings(resource)
        if forward and any(identity(r["source"]) in PROCESSING for r in resource["replace"]):
            validate_processing_bindings(resource)
        if forward:
            validate_cutoff_feed_market_bindings(resource)
        if forward and cable_rail_inventories:
            validate_cable_freight_bindings(resource)
        decisions, counts = {}, Counter()
        for rule in reviewed_replacements(resource, previous.get(resource["name"], {})):
            if "recipe_ref" in rule["target"]:
                counts["recipe_unchanged"] += 1
                continue
            original_binding = rule_binding(rule)
            if forward:
                rule = approved_supplier_rule(rule)
            source, target = identity(rule["source"]), identity(rule["target"])
            ecoinvent, uvek = (source, target) if forward else (target, source)
            eco_rows = observations["ecoinvent"].get(ecoinvent, set())
            if not forward:
                eco_rows = {row for row in eco_rows if row[0] == "ecoinvent-3.12-cutoff"}
            eco_signs = {sign for _, sign in eco_rows}
            uvek_signs = {sign for _, sign in observations["uvek"].get(uvek, set())}
            native_row = native.get(uvek, {})
            waste = (
                native_row.get("native_flow_type") == "WASTE_FLOW"
                and native_row.get("native_reference_is_input") is True
            )
            product = (
                native_row.get("native_flow_type") == "PRODUCT_FLOW"
                and native_row.get("native_reference_is_input") is False
            )
            if not forward and rule.get("mapping_method") in (WHOLE_FREIGHT_REVERSE_METHOD, PACKED_WOOL_REVERSE_METHOD):
                decision_fn = (
                    whole_freight_reverse_decision
                    if rule["mapping_method"] == WHOLE_FREIGHT_REVERSE_METHOD
                    else packed_wool_reverse_decision
                )
                decisions[rule["id"]] = decision_fn(rule, eco_rows, uvek_signs, native_row, rule_binding(rule))
                counts["approved_independent_freight_wool_reverse"] += 1
                continue
            if forward and source in CABLE_RAIL_SOURCES:
                validate_cable_rail_inventories(cable_rail_inventories)
                prior = previous.get(resource["name"], {}).get(rule["id"], {})
                decisions[rule["id"]] = cable_rail_decision(
                    rule, eco_rows, rule_binding(rule), prior_recipe_review(resource, rule, prior)
                )
                counts["approved_cable_rail_function_repair"] += 1
                continue
            if forward and source == PACKED_WOOL_SOURCE:
                decisions[rule["id"]] = packed_wool_decision(
                    rule,
                    eco_rows,
                    packed_wool_inventories[source],
                    rule_binding(rule),
                    rule_review(resource, rule) if "review_ref" in rule else None,
                )
                counts["approved_complete_packed_wool_market"] += 1
                continue
            if forward and source in WHOLE_FREIGHT_SOURCES:
                prior = previous.get(resource["name"], {}).get(rule["id"], {})
                decisions[rule["id"]] = whole_freight_decision(
                    rule,
                    eco_rows,
                    uvek_signs,
                    native_row,
                    rule_binding(rule),
                    prior,
                    rule_review(resource, rule) if "review_ref" in rule else None,
                )
                counts["approved_whole_freight_proxy"] += 1
                continue
            if forward and source in CHEMICAL_HEAT_SOURCES:
                decisions[rule["id"]] = chemical_heat_decision(
                    rule,
                    eco_rows,
                    chemical_heat_inventories[source],
                    rule_binding(rule),
                    rule_review(resource, rule) if "review_ref" in rule else None,
                )
                counts["approved_chemical_heat_with_retained_water"] += 1
                continue
            if forward and source in PAM_FRONTIER_STOCK_SOURCES:
                decisions[rule["id"]] = pam_frontier_stock_decision(
                    rule,
                    eco_rows,
                    pam_frontier_stock_inventories[source],
                    rule_binding(rule),
                    rule_review(resource, rule) if "review_ref" in rule else None,
                )
                counts["approved_pam_frontier_cutoff_stocks"] += 1
                continue
            if forward and source in REGIONAL_FREIGHT_SOURCES:
                prior = previous.get(resource["name"], {}).get(rule["id"], {})
                decisions[rule["id"]] = regional_freight_decision(
                    rule,
                    eco_rows,
                    regional_freight_inventories[source],
                    rule_binding(rule),
                    rule_review(resource, rule) if "review_ref" in rule else None,
                    prior,
                )
                counts["approved_pam_frontier_cutoff_stocks"] += 1
                continue
            if forward and source in OLDER_REGIONAL_ROAD_SOURCES:
                prior = previous.get(resource["name"], {}).get(rule["id"], {})
                decisions[rule["id"]] = older_regional_road_decision(
                    rule,
                    eco_rows,
                    older_regional_road_inventories[source],
                    rule_binding(rule),
                    rule_review(resource, rule) if "review_ref" in rule else None,
                    prior,
                )
                counts["approved_older_regional_road_proxy"] += 1
                continue
            if forward and source in WASTE_CONCRETE_INERT_SOURCES:
                prior = previous.get(resource["name"], {}).get(rule["id"], {})
                decisions[rule["id"]] = waste_concrete_inert_decision(
                    rule, eco_rows, rule_binding(rule), prior_recipe_review(resource, rule, prior)
                )
                counts["approved_waste_concrete_inert_reconstruction"] += 1
                continue
            if forward and source in COPPER_ASH_SOURCES:
                prior = previous.get(resource["name"], {}).get(rule["id"], {})
                decisions[rule["id"]] = copper_ash_decision(
                    rule, eco_rows, rule_binding(rule), prior_recipe_review(resource, rule, prior)
                )
                counts["approved_copper_ash_reconstruction"] += 1
                continue
            if forward and source in SWISS_CEMENT_MARKETS:
                decisions[rule["id"]] = swiss_cement_market_decision(
                    rule, eco_rows, rule_binding(rule), rule_review(resource, rule) if "review_ref" in rule else None
                )
                counts["approved_swiss_cement_market_delivery"] += 1
                continue
            if forward and source in DIGESTER_STOCK_SOURCES:
                prior = previous.get(resource["name"], {}).get(rule["id"], {})
                original_factor = prior.get("original_factor", rule.get("conversion_factor", 1))
                rule = {k: v for k, v in rule.items() if k != "conversion_factor"}
                if original_factor != 1:
                    rule["conversion_factor"] = original_factor
                decisions[rule["id"]] = digester_stock_decision(
                    rule, eco_rows, rule_binding(rule), prior_recipe_review(resource, rule, prior), prior
                )
                counts["approved_digester_stock_correction"] += 1
                continue
            if forward and source in REFRIGERANT_PATCH_SOURCES:
                prior = previous.get(resource["name"], {}).get(rule["id"], {})
                original_factor = prior.get("original_factor", rule.get("conversion_factor", 1))
                rule = {k: v for k, v in rule.items() if k != "conversion_factor"}
                if original_factor != 1:
                    rule["conversion_factor"] = original_factor
                decisions[rule["id"]] = refrigerant_patch_decision(
                    rule,
                    eco_rows,
                    rule_binding(rule),
                    prior_recipe_review(resource, rule, prior),
                    prior,
                )
                counts["approved_refrigerant_patch_formula_inventory"] += 1
                continue
            if forward and source in GHANA_UNCONTROLLED_SOURCES:
                prior = previous.get(resource["name"], {}).get(rule["id"], {})
                original_factor = prior.get("original_factor", rule.get("conversion_factor", 1))
                rule = {k: v for k, v in rule.items() if k != "conversion_factor"}
                if original_factor != 1:
                    rule["conversion_factor"] = original_factor
                decisions[rule["id"]] = ghana_uncontrolled_decision(
                    rule,
                    eco_rows,
                    rule_binding(rule),
                    prior_recipe_review(resource, rule, prior),
                )
                counts["approved_ghana_uncontrolled_inventory"] += 1
                continue
            if forward and source in PP_UNCONTROLLED_SOURCES:
                prior = previous.get(resource["name"], {}).get(rule["id"], {})
                original_factor = prior.get("original_factor", rule.get("conversion_factor", 1))
                rule = {k: v for k, v in rule.items() if k != "conversion_factor"}
                if original_factor != 1:
                    rule["conversion_factor"] = original_factor
                decisions[rule["id"]] = pp_uncontrolled_decision(
                    rule,
                    eco_rows,
                    rule_binding(rule),
                    prior_recipe_review(resource, rule, prior),
                )
                counts["approved_pp_uncontrolled_source_inventory"] += 1
                continue
            if forward and source == METALLIFEROUS_STOCK:
                prior = previous.get(resource["name"], {}).get(rule["id"], {})
                decisions[rule["id"]] = metalliferous_stock_decision(
                    rule,
                    eco_rows,
                    metalliferous_stock_inventories,
                    rule_binding(rule),
                    prior_recipe_review(resource, rule, prior),
                )
                counts["approved_metalliferous_cutoff_stock"] += 1
                continue
            if forward and source in PRECIOUS_ANODE_MARKETS:
                prior = previous.get(resource["name"], {}).get(rule["id"], {})
                decisions[rule["id"]] = precious_anode_decision(
                    rule,
                    eco_rows,
                    precious_anode_inventories,
                    rule_binding(rule),
                    prior_recipe_review(resource, rule, prior),
                )
                counts["approved_precious_anode_market"] += 1
                continue
            if forward and source in SOURCE_GAS_FLARES:
                prior = previous.get(resource["name"], {}).get(rule["id"], {})
                decisions[rule["id"]] = source_gas_flare_decision(
                    rule,
                    eco_rows,
                    rule_binding(rule),
                    prior,
                    prior_recipe_review(resource, rule, prior),
                )
                counts["approved_source_gas_flare_service"] += 1
                continue
            if forward and source == CONCRETE_GRADE_SOURCE:
                prior = previous.get(resource["name"], {}).get(rule["id"], {})
                decisions[rule["id"]] = concrete_grade_decision(
                    rule,
                    eco_rows,
                    uvek_signs,
                    native_row,
                    rule_binding(rule),
                    prior,
                    rule_review(resource, rule) if "review_ref" in rule else None,
                )
                counts["approved_concrete_grade_proxy"] += 1
                continue
            if forward and source == COPPER_CATHODE_SOURCE:
                prior = previous.get(resource["name"], {}).get(rule["id"], {})
                decisions[rule["id"]] = copper_cathode_decision(
                    rule,
                    eco_rows,
                    uvek_signs,
                    native_row,
                    rule_binding(rule),
                    prior,
                    rule_review(resource, rule) if "review_ref" in rule else None,
                )
                counts["approved_copper_cathode_proxy"] += 1
                continue
            if forward and source in SEWER_GRIDS:
                prior = previous.get(resource["name"], {}).get(rule["id"], {})
                decisions[rule["id"]] = sewer_grid_decision(
                    rule,
                    eco_rows,
                    uvek_signs,
                    native_row,
                    rule_binding(rule),
                    prior,
                    rule_review(resource, rule) if "review_ref" in rule else None,
                )
                counts["approved_sewer_grid_proxy"] += 1
                continue
            if forward and source in SLAG_LANDFILL_COMPONENTS:
                prior = previous.get(resource["name"], {}).get(rule["id"], {})
                decisions[rule["id"]] = slag_landfill_component_decision(
                    rule,
                    eco_rows,
                    uvek_signs,
                    native_row,
                    rule_binding(rule),
                    prior,
                    rule_review(resource, rule) if "review_ref" in rule else None,
                )
                counts["approved_slag_landfill_component_proxy"] += 1
                continue
            if forward and source in STAINLESS_STOCKS:
                prior = previous.get(resource["name"], {}).get(rule["id"], {})
                decisions[rule["id"]] = stainless_stock_decision(
                    rule,
                    eco_rows,
                    uvek_signs,
                    native_row,
                    rule_binding(rule),
                    prior,
                    rule_review(resource, rule) if "review_ref" in rule else None,
                )
                counts["approved_stainless_stock_proxy"] += 1
                continue
            if forward and source in ASH_MARKET_SOURCES:
                prior = previous.get(resource["name"], {}).get(rule["id"], {})
                decisions[rule["id"]] = ash_market_decision(
                    rule,
                    eco_rows,
                    ash_market_inventories[source],
                    rule_binding(rule),
                    prior_recipe_review(resource, rule, prior),
                )
                counts["approved_ash_market_recipe"] += 1
                continue
            if forward and source in RESIDUAL_LANDFILLS:
                prior = previous.get(resource["name"], {}).get(rule["id"], {})
                decisions[rule["id"]] = residual_landfill_decision(
                    rule,
                    eco_rows,
                    residual_landfill_inventories[source],
                    rule_binding(rule),
                    prior_recipe_review(resource, rule, prior),
                )
                counts["approved_residual_landfill_recipe"] += 1
                continue
            if forward and source in RESOLVED_CHILD_MARKETS:
                prior = previous.get(resource["name"], {}).get(rule["id"], {})
                decisions[rule["id"]] = resolved_child_market_decision(
                    rule,
                    eco_rows,
                    resolved_child_market_inventories[source],
                    rule_binding(rule),
                    prior_recipe_review(resource, rule, prior),
                )
                counts["approved_resolved_child_market_recipe"] += 1
                continue
            if forward and source in SEWER_THROUGHPUT_SOURCES:
                prior = previous.get(resource["name"], {}).get(rule["id"], {})
                decisions[rule["id"]] = sewer_throughput_decision(
                    rule,
                    eco_rows,
                    sewer_throughput_inventories[source],
                    rule_binding(rule),
                    prior_recipe_review(resource, rule, prior),
                )
                counts["approved_sewer_throughput_recipe"] += 1
                continue
            if forward and source in PACKAGING_PAPER_LANDFARMING_SOURCES:
                prior = previous.get(resource["name"], {}).get(rule["id"], {})
                decisions[rule["id"]] = packaging_paper_landfarming_decision(
                    rule,
                    eco_rows,
                    packaging_paper_landfarming_inventories[source],
                    rule_binding(rule),
                    prior_recipe_review(resource, rule, prior),
                )
                counts["approved_packaging_paper_landfarming_recipe"] += 1
                continue
            if forward and source in STEEL_RESIDUE_MARKET_SOURCES:
                prior = previous.get(resource["name"], {}).get(rule["id"], {})
                # Reconstruct the original authoring coordinate before replacing an
                # already signed direct treatment rule with its complete inventory.
                # The new helper applies its independently checked reference factor.
                original_factor = prior.get("original_factor", rule.get("conversion_factor", 1))
                rule = {k: v for k, v in rule.items() if k != "conversion_factor"}
                if original_factor != 1:
                    rule["conversion_factor"] = original_factor
                decisions[rule["id"]] = steel_residue_market_decision(
                    rule,
                    eco_rows,
                    steel_residue_market_inventories[source],
                    rule_binding(rule),
                    prior_recipe_review(resource, rule, prior),
                )
                counts["approved_steel_residue_market_recipe"] += 1
                continue
            if forward and source in INDUSTRIAL_LANDFARMING_SOURCES:
                prior = previous.get(resource["name"], {}).get(rule["id"], {})
                decisions[rule["id"]] = industrial_landfarming_decision(
                    rule,
                    eco_rows,
                    industrial_landfarming_inventories[source],
                    rule_binding(rule),
                    prior_recipe_review(resource, rule, prior),
                )
                counts["approved_industrial_landfarming_recipe"] += 1
                continue
            if forward and source in INDUSTRIAL_WASTE_MARKET_SOURCES:
                prior = previous.get(resource["name"], {}).get(rule["id"], {})
                decisions[rule["id"]] = industrial_waste_market_decision(
                    rule,
                    eco_rows,
                    industrial_waste_market_inventories[source],
                    rule_binding(rule),
                    prior_recipe_review(resource, rule, prior),
                )
                counts["approved_industrial_waste_market_recipe"] += 1
                continue
            if forward and source in ADDITIONAL_CUTOFF_WASTE_MARKET_SOURCES:
                prior = previous.get(resource["name"], {}).get(rule["id"], {})
                decisions[rule["id"]] = additional_cutoff_waste_market_decision(
                    rule,
                    eco_rows,
                    additional_cutoff_waste_market_inventories[source],
                    rule_binding(rule),
                    prior_recipe_review(resource, rule, prior),
                )
                counts["approved_additional_cutoff_waste_market_recipe"] += 1
                continue
            if forward and source in UNCONTROLLED_DISPOSAL_SOURCES:
                prior = previous.get(resource["name"], {}).get(rule["id"], {})
                decisions[rule["id"]] = uncontrolled_disposal_decision(
                    rule,
                    eco_rows,
                    uncontrolled_disposal_inventories[source],
                    rule_binding(rule),
                    prior_recipe_review(resource, rule, prior),
                )
                counts["approved_uncontrolled_disposal_recipe"] += 1
                continue
            if forward and source in OLDER_PET_WHOLE_FREIGHT_SOURCES:
                prior = previous.get(resource["name"], {}).get(rule["id"], {})
                inventories = (
                    pet_waste_market_inventories[source]
                    if source in PET_WASTE_MARKET_SOURCES
                    else [r for r in older_pet_whole_freight_inventories if identity(r["source"]) == source]
                )
                decisions[rule["id"]] = older_pet_whole_freight_decision(
                    rule, eco_rows, inventories, rule_binding(rule), prior_recipe_review(resource, rule, prior)
                )
                counts["approved_older_pet_whole_freight_extension"] += 1
                continue
            if forward and source in PET_WASTE_MARKET_SOURCES:
                prior = previous.get(resource["name"], {}).get(rule["id"], {})
                decisions[rule["id"]] = pet_waste_market_decision(
                    rule,
                    eco_rows,
                    pet_waste_market_inventories[source],
                    rule_binding(rule),
                    prior_recipe_review(resource, rule, prior),
                )
                counts["approved_pet_waste_market_recipe"] += 1
                continue
            if forward and source in FIVE_SIMPLE_WASTE_MARKET_SOURCES:
                prior = previous.get(resource["name"], {}).get(rule["id"], {})
                decisions[rule["id"]] = five_simple_waste_market_decision(
                    rule,
                    eco_rows,
                    five_simple_waste_market_inventories[source],
                    rule_binding(rule),
                    prior_recipe_review(resource, rule, prior),
                )
                counts["approved_five_simple_waste_market_recipe"] += 1
                continue
            if forward and source in SWISS_PET_FLUWA_SOURCES:
                prior = previous.get(resource["name"], {}).get(rule["id"], {})
                decisions[rule["id"]] = swiss_pet_fluwa_decision(
                    rule,
                    eco_rows,
                    swiss_pet_fluwa_inventories[source],
                    rule_binding(rule),
                    prior_recipe_review(resource, rule, prior),
                )
                counts["approved_swiss_pet_fluwa_recipe"] += 1
                continue
            if forward and source in SLF_LANDFARMING_SOURCES:
                prior = previous.get(resource["name"], {}).get(rule["id"], {})
                decisions[rule["id"]] = slf_landfarming_decision(
                    rule,
                    eco_rows,
                    slf_landfarming_inventories[source],
                    rule_binding(rule),
                    prior_recipe_review(resource, rule, prior),
                )
                counts["approved_slf_landfarming_recipe"] += 1
                continue
            if forward and source in AVERAGE_SLUDGE_LANDFARMING_SOURCES:
                prior = previous.get(resource["name"], {}).get(rule["id"], {})
                decisions[rule["id"]] = average_sludge_landfarming_decision(
                    rule,
                    eco_rows,
                    average_sludge_landfarming_inventories[source],
                    rule_binding(rule),
                    prior_recipe_review(resource, rule, prior),
                )
                counts["approved_average_sludge_landfarming_recipe"] += 1
                continue
            if forward and source in PET_MARKET_GROUP_SOURCES:
                prior = previous.get(resource["name"], {}).get(rule["id"], {})
                decisions[rule["id"]] = pet_market_group_decision(
                    rule,
                    eco_rows,
                    pet_market_group_inventories[source],
                    rule_binding(rule),
                    prior_recipe_review(resource, rule, prior),
                )
                counts["approved_pet_market_group_recipe"] += 1
                continue
            if forward and source in COPPER_STOCK_SOURCES:
                prior = previous.get(resource["name"], {}).get(rule["id"], {})
                decisions[rule["id"]] = copper_stock_decision(
                    rule,
                    eco_rows,
                    copper_stock_inventories[source],
                    rule_binding(rule),
                    prior_recipe_review(resource, rule, prior),
                )
                counts["approved_copper_stock_recipe"] += 1
                continue
            if forward and source in BOTTOM_ASH_SOURCES:
                prior = previous.get(resource["name"], {}).get(rule["id"], {})
                decisions[rule["id"]] = bottom_ash_decision(
                    rule,
                    eco_rows,
                    bottom_ash_inventories[source],
                    rule_binding(rule),
                    prior_recipe_review(resource, rule, prior),
                )
                counts["approved_bottom_ash_recipe"] += 1
                continue
            if forward and source in SEWAGE_SLUDGE_MARKETS:
                prior = previous.get(resource["name"], {}).get(rule["id"], {})
                decisions[rule["id"]] = sewage_sludge_market_decision(
                    rule,
                    eco_rows,
                    sewage_digestion_inventories[source],
                    rule_binding(rule),
                    prior_recipe_review(resource, rule, prior),
                )
                counts["approved_sewage_sludge_market_alias"] += 1
                continue
            if forward and source in SEWAGE_DIGESTION_SOURCES:
                prior = previous.get(resource["name"], {}).get(rule["id"], {})
                decisions[rule["id"]] = sewage_digestion_decision(
                    rule,
                    eco_rows,
                    sewage_digestion_inventories[source],
                    rule_binding(rule),
                    prior_recipe_review(resource, rule, prior),
                )
                counts["approved_sewage_digestion_recipe"] += 1
                continue
            if (
                not forward
                and source in CUTOFF_FEED_REVERSE_PRODUCTS
                and rule.get("mapping_method") == CUTOFF_FEED_REVERSE_METHOD
            ):
                decisions[rule["id"]] = cutoff_feed_reverse_decision(
                    rule, eco_rows, uvek_signs, native_row, rule_binding(rule)
                )
                counts["approved_cutoff_feed_product_reverse"] += 1
                continue
            if forward and source in CUTOFF_FEED_PRODUCTS:
                if not product or uvek_signs != {1}:
                    raise ValueError("Physical cut-off coproduct target reference changed; repeat the review.")
                decisions[rule["id"]] = cutoff_feed_product_decision(rule, eco_rows, rule_binding(rule))
                counts["approved_cutoff_feed_product_proxy"] += 1
                continue
            if forward and source in CUTOFF_FEED_MARKETS:
                row = CUTOFF_FEED_MARKETS[source]
                if not product or uvek_signs != {1} or row["previous_native_flow_type"] != "PRODUCT_FLOW":
                    raise ValueError("Physical cut-off market prior product reference changed; repeat the review.")
                prior = previous.get(resource["name"], {}).get(rule["id"], {})
                decisions[rule["id"]] = cutoff_feed_market_decision(
                    rule,
                    eco_rows,
                    cutoff_feed_product_inventories[source],
                    rule_binding(rule),
                    prior_recipe_review(resource, rule, prior),
                    APPROVED_FEED_SUBSTITUTION_OMISSIONS[source]["review"],
                )
                counts["approved_cutoff_feed_market_recipe"] += 1
                continue
            if forward and source in TRANSPORTS:
                if not product or uvek_signs != {1}:
                    raise ValueError("Modal transport target reference convention changed; repeat the review.")
                prior = previous.get(resource["name"], {}).get(rule["id"], {})
                original_review = prior_recipe_review(resource, rule, prior)
                decisions[rule["id"]] = transport_decision(
                    rule,
                    eco_rows,
                    modal_transport_inventories,
                    rule_binding(rule),
                    original_review,
                    prior,
                )
                counts["approved_modal_transport_proxy"] += 1
                continue
            if (
                not forward
                and source == NATURAL_GAS_FLARE_TARGET
                and rule.get("mapping_method") == NATURAL_GAS_FLARE_REVERSE_METHOD
            ):
                decisions[rule["id"]] = natural_gas_flare_reverse_decision(
                    rule, eco_rows, natural_gas_flare_inventories, native_row, uvek_signs, rule_binding(rule)
                )
                counts["already_correct"] += 1
                continue
            if forward and source in EXACT_PATCH_STOCKS:
                prior = previous.get(resource["name"], {}).get(rule["id"], {})
                original_review = prior_recipe_review(resource, rule, prior)
                decisions[rule["id"]] = exact_patch_stock_decision(
                    rule,
                    eco_rows,
                    cutoff_stock_inventories[source],
                    exact_patch_boundary_inventories[source],
                    rule_binding(rule),
                    original_review,
                )
                counts["approved_cutoff_stock_recipe"] += 1
                continue
            if forward and source in COMPLETE_PATCH_STOCKS:
                prior = previous.get(resource["name"], {}).get(rule["id"], {})
                original_review = prior_recipe_review(resource, rule, prior)
                decisions[rule["id"]] = complete_patch_stock_decision(
                    rule,
                    eco_rows,
                    cutoff_stock_inventories[source],
                    complete_patch_boundary_inventories[source],
                    rule_binding(rule),
                    original_review,
                )
                counts["approved_cutoff_stock_recipe"] += 1
                continue
            if forward and source in STOCKS:
                prior = previous.get(resource["name"], {}).get(rule["id"], {})
                original_review = prior_recipe_review(resource, rule, prior)
                decisions[rule["id"]] = stock_decision(
                    rule, eco_rows, cutoff_stock_inventories[source], rule_binding(rule), original_review
                )
                counts["approved_cutoff_stock_recipe"] += 1
                continue
            if forward and source in CUTOFF_SLAG_PRODUCTS:
                if not product or uvek_signs != {1}:
                    raise ValueError("Cut-off slag prior product target changed; repeat the independent review.")
                prior = previous.get(resource["name"], {}).get(rule["id"], {})
                decisions[rule["id"]] = cutoff_slag_product_decision(
                    rule,
                    eco_rows,
                    [(row["label"], row["inventory_sha256"]) for row in cutoff_slag_product_inventories[source]],
                    rule_binding(rule),
                    prior_recipe_review(resource, rule, prior),
                )
                counts["approved_cutoff_slag_product_recipe"] += 1
                continue
            if forward and source in PROCESSING:
                approval = PROCESSING[source]
                if not {"PRODUCT_FLOW": product, "WASTE_FLOW": waste}.get(
                    approval["previous_native_flow_type"]
                ) or uvek_signs != {1}:
                    raise ValueError(
                        "Processing-composition prior target reference changed; repeat the scientific review."
                    )
                prior = previous.get(resource["name"], {}).get(rule["id"], {})
                original_review = prior_recipe_review(resource, rule, prior)
                decisions[rule["id"]] = processing_decision(
                    rule,
                    eco_rows,
                    processing_composition_inventories[source],
                    rule_binding(rule),
                    original_review,
                )
                counts["approved_processing_composition"] += 1
                continue
            if forward and source in COMPOSITIONS:
                approval = COMPOSITIONS[source]
                if not {"PRODUCT_FLOW": product, "WASTE_FLOW": waste}.get(
                    approval["previous_native_flow_type"]
                ) or uvek_signs != {1}:
                    raise ValueError("Market-composition prior target reference changed; repeat the scientific review.")
                prior = previous.get(resource["name"], {}).get(rule["id"], {})
                original_review = prior_recipe_review(resource, rule, prior)
                decisions[rule["id"]] = composition_decision(
                    rule,
                    eco_rows,
                    market_composition_inventories[source],
                    rule_binding(rule),
                    original_review,
                )
                counts["approved_market_composition"] += 1
                continue
            if forward and source in MARKETS:
                approval = MARKETS[source]
                if not {"PRODUCT_FLOW": product, "WASTE_FLOW": waste}.get(
                    approval["previous_native_flow_type"]
                ) or uvek_signs != {1}:
                    raise ValueError("Gas-market prior target reference changed; repeat the scientific review.")
                prior = previous.get(resource["name"], {}).get(rule["id"], {})
                original_review = prior_recipe_review(resource, rule, prior)
                decisions[rule["id"]] = market_decision(
                    rule,
                    eco_rows,
                    gas_market_inventories,
                    rule_binding(rule),
                    original_review,
                )
                counts["approved_gas_market_recipe"] += 1
                continue
            if forward and source in GAS_SOURCES:
                approval = GAS_SOURCES[source]
                if not {"PRODUCT_FLOW": product, "WASTE_FLOW": waste}.get(
                    approval["previous_native_flow_type"]
                ) or uvek_signs != {1}:
                    raise ValueError("Industrial-gas prior target reference changed; repeat the review.")
                for primitive in PRIMITIVES:
                    if {sign for _, sign in observations["uvek"].get(primitive, set())} != {1}:
                        raise ValueError("Industrial-gas combustion primitive reference changed; repeat the review.")
                prior = previous.get(resource["name"], {}).get(rule["id"], {})
                original_review = prior_recipe_review(resource, rule, prior)
                decisions[rule["id"]] = gas_decision(
                    rule,
                    eco_rows,
                    industrial_gas_inventories[source],
                    industrial_gas_inventories,
                    rule_binding(rule),
                    original_review,
                )
                counts["approved_industrial_gas_recipe"] += 1
                continue
            if forward and source in RECLAMATION_SOURCES:
                if not product or uvek_signs != {1}:
                    raise ValueError("Reclamation prior target reference changed; repeat the review.")
                for expected_target in (
                    DISPOSAL_TARGET,
                    ELECTRICITY_TARGET,
                    REFRIGERANT_TARGET,
                ):
                    if {sign for _, sign in observations["uvek"].get(identity(expected_target), set())} != {1}:
                        raise ValueError("Reclamation proxy target reference changed; repeat the review.")
                prior = previous.get(resource["name"], {}).get(rule["id"], {})
                original_review = prior_recipe_review(resource, rule, prior)
                decisions[rule["id"]] = reclamation_decision(
                    rule,
                    eco_rows,
                    reclamation_inventories[source],
                    rule_binding(rule),
                    original_review,
                )
                counts[
                    (
                        "approved_reclamation_recipe"
                        if RECLAMATION_SOURCES[source]["kind"] == "service"
                        else "approved_zero_burden_recipe"
                    )
                ] += 1
                continue
            if forward and source in DISPOSAL_SOURCES:
                prior = previous.get(resource["name"], {}).get(rule["id"], {})
                approval = DISPOSAL_SOURCES[source]
                original_review = prior_recipe_review(resource, rule, prior)
                disposal_target = identity(DISPOSAL_TARGET)
                target_native = native.get(disposal_target, {})
                if (
                    not {"PRODUCT_FLOW": product, "WASTE_FLOW": waste}.get(approval["previous_native_flow_type"])
                    or uvek_signs != {1}
                    or {sign for _, sign in observations["uvek"].get(disposal_target, set())} != {1}
                    or target_native.get("native_flow_type") != "WASTE_FLOW"
                    or target_native.get("native_reference_is_input") is not True
                ):
                    raise ValueError("Disposal proxy reference changed; repeat the scientific review.")
                decisions[rule["id"]] = disposal_decision(
                    rule,
                    eco_rows,
                    disposal_inventories[source],
                    rule_binding(rule),
                    original_review,
                    prior,
                )
                counts["approved_disposal_recipe"] += 1
                continue
            if forward and source in VENTING_SOURCES:
                if not product or uvek_signs != {1}:
                    raise ValueError("Original venting proxy reference changed; repeat the review.")
                prior = previous.get(resource["name"], {}).get(rule["id"], {})
                original_review = prior_recipe_review(resource, rule, prior)
                decisions[rule["id"]] = venting_decision(
                    rule,
                    eco_rows,
                    venting_inventories[source],
                    rule_binding(rule),
                    original_review,
                )
                counts["approved_venting_recipe"] += 1
                continue
            treatment_approval = APPROVED_WASTE_TREATMENTS.get(source) if forward else None
            profiled_approval = APPROVED_PROFILED_TREATMENTS.get(source) if forward else None
            profiled_omission = forward and source in APPROVED_PROFILED_OMISSIONS
            credit = APPROVED_RECOVERY_CREDITS.get(source) if forward else None
            omission = (APPROVED_TECHNOSPHERE_OMISSIONS if forward else APPROVED_REVERSE_OMISSIONS).get(source)
            if omission:
                expected = (
                    {
                        (f"ecoinvent-{version}-{model}", -1)
                        for version in omission["source_versions"]
                        for model in ("cutoff", "consequential")
                    }
                    if forward
                    else {("ecoinvent-3.12-cutoff", -1)}
                )
                prior_target = omission.get(
                    "previous_target",
                    OMISSION_PREVIOUS_TARGET if forward else BIOWASTE_PREVIOUS_TARGET,
                )
                prior_role = omission.get(
                    "previous_native_flow_type",
                    "WASTE_FLOW" if forward else "PRODUCT_FLOW",
                )
                if (
                    target != prior_target
                    or eco_rows != expected
                    or not {"WASTE_FLOW": waste, "PRODUCT_FLOW": product}.get(prior_role, False)
                    or uvek_signs != {1}
                ):
                    raise ValueError("Approved omission inventory or prior supplier changed; repeat the review.")
                if forward and source in APPROVED_ANTIFOULING_OMISSIONS:
                    validate_antifouling_inventories(source, eco_rows, antifouling_inventories[source])
                if forward and source in APPROVED_WWT_RESIDUE_OMISSIONS:
                    validate_wwt_residue_inventories(source, eco_rows, wwt_residue_inventories[source])
            treatment = treatment_approval is not None
            reverse_service = (
                REVERSE_GAS_SERVICES.get(source)
                if not forward and rule.get("mapping_method") == REVERSE_GAS_METHOD
                else None
            )
            if reverse_service and (
                target != identity(reverse_service["target"])
                or eco_rows != {("ecoinvent-3.12-cutoff", -1)}
                or uvek_signs != {1}
                or not product
            ):
                raise ValueError("Reverse gas service reference conventions changed; repeat the independent review.")
            # Native PRODUCT_FLOW can describe a service. Accept this only for
            # exact inventory-inspected approvals; preserve all other role blocks.
            approved_product_service = (treatment and treatment_approval.get("native_product_service", False)) or bool(
                reverse_service
            )
            reason = ""
            if len(eco_signs) != 1 or uvek_signs != {1} or not (waste or product):
                reason = "Reference-flow sign is missing or differs between source profiles; no sign is inferred from labels."
            elif eco_signs == {-1} and not (waste or (approved_product_service and product)):
                reason = "The mapping changes a waste-treatment reference into a product reference or vice versa; sign correction alone cannot establish functional equivalence."
            if not reason and eco_signs == {1} and product and not treatment and not profiled_omission:
                counts["product_unchanged"] += 1
                continue
            original = rule.get("conversion_factor", 1)
            previous_decision = previous.get(resource["name"], {}).get(rule["id"])
            # An approved supplier change can retain an earlier sign correction.
            # Keep its original factor bound to the inspected prior supplier too.
            if previous_decision and previous_decision["binding"] in (
                rule_binding(rule),
                original_binding,
            ):
                if original not in (
                    previous_decision["original_factor"],
                    previous_decision["factor"],
                ):
                    raise ValueError(f"Stale reference-flow sign factor: {rule['id']}")
                original = previous_decision["original_factor"]
            opposite_signs = eco_signs == {-1}
            if treatment and (
                eco_signs != {-1} or not (product if approved_product_service else waste) or uvek_signs != {1}
            ):
                raise ValueError("Waste-treatment reference conventions changed; repeat the scientific review.")
            if treatment and "source_versions" in treatment_approval:
                expected = {
                    (f"ecoinvent-{version}-{model}", -1)
                    for version in treatment_approval["source_versions"]
                    for model in ("cutoff", "consequential")
                }
                if eco_rows != expected:
                    raise ValueError("Approved-treatment profile coverage changed; repeat the scientific review.")
            rationale = (
                "Ecoinvent's negative waste reference and UVEK's positive treatment-service reference require opposite exchange signs."
                if opposite_signs
                else "Both imported reference production amounts are positive; preserve their quantity direction despite differing native flow classifications. This does not establish scientific equivalence."
            )
            if approved_product_service:
                function = (
                    treatment_approval.get("reference_function", "wastewater-treatment service")
                    if treatment
                    else "refinery-gas flaring service"
                )
                rationale = f"For this exact inspected and approved correspondence, UVEK's native PRODUCT_FLOW output represents a {function}, not recovered material. Reference production -1/+1 requires opposite exchange signs; boundary and treatment-model limitations are independently disclosed."
            review = {
                "status": "unresolved" if reason else "verified",
                "reason": reason or rationale,
                "evidence": [f"{REVIEW_PATH}#{resource['name']}/{rule['id']}"],
            }
            discharge = forward and source in APPROVED_DISCHARGE_TREATMENTS
            if discharge:
                review.update(
                    status="unresolved",
                    profiles=discharge_profiles(source, eco_rows, discharge_inventories, review["evidence"]),
                    reason="Select only an exact inspected source profile for the approved produced-water discharge proxy; missing and patch profiles remain blocked.",
                )
            if forward and source in FLARES:
                review.update(
                    status="unresolved",
                    profiles=flare_profiles(source, eco_rows, flare_inventories, review["evidence"]),
                    reason="Select only an exact inspected source profile for the approved refinery-flaring proxy; missing and patch profiles remain blocked.",
                )
            if forward and source in NATURAL_GAS_FLARES:
                review.update(
                    status="unresolved",
                    profiles=natural_gas_flare_profiles(
                        source, eco_rows, natural_gas_flare_inventories, review["evidence"]
                    ),
                    reason="Select only an exact inspected modern sweet-gas flare profile; older emission boundaries, sour gas and patch contexts remain pending.",
                )
            if forward and source in LANDFILLS:
                review.update(
                    status="unresolved",
                    profiles=landfill_profiles(source, eco_rows, eaf_landfill_inventories, review["evidence"]),
                    reason="Select only an exact inspected source profile for the approved legacy EAF landfill proxy; other and patch profiles remain blocked.",
                )
            if forward and source in LCD_TREATMENTS:
                review.update(
                    status="unresolved",
                    profiles=lcd_profiles(source, eco_rows, lcd_treatment_inventories, review["evidence"]),
                    reason="Select only an exact inspected source profile for the dedicated LCD treatment proxy; missing and patch profiles remain blocked.",
                )
            if forward and source in DISPOSALS:
                review.update(
                    status="unresolved",
                    profiles=dedicated_disposal_profiles(
                        source,
                        eco_rows,
                        dedicated_disposal_inventories,
                        review["evidence"],
                    ),
                    reason="Select only an exact inspected source profile for the dedicated material/vehicle disposal proxy; missing and patch profiles remain blocked.",
                )
            if forward and len(eco_signs) > 1:
                previous_profiles = previous_decision["review"].get("profiles", []) if previous_decision else []
                scoped = model_specific_review(eco_rows, review["evidence"], previous_profiles)
                if scoped is not None:
                    review["profiles"] = scoped
                    review["reason"] = (
                        "Reference signs are recorded separately for each inspected source version and system model. Supplier substitutions remain pending separate scientific approval."
                    )
            if profiled_approval:
                if target != profiled_approval["target"] or not waste or uvek_signs != {1} or original != 1:
                    raise ValueError(
                        "Profile-specific treatment supplier or reference role changed; repeat the review."
                    )
                review["profiles"] = profiled_treatment_review(profiled_approval, eco_rows, review["evidence"])
                review["reason"] = (
                    "The treatment proxy is independently approved for the enumerated exact source profiles. Select their verified factors; no global factor or missing-profile fallback is approved."
                )
            coal_tar = forward and source == COAL_TAR_SOURCE
            if coal_tar:
                if target != COAL_TAR_TARGET or not product or uvek_signs != {1} or original != 1:
                    raise ValueError(
                        "Coal-tar supplier identity, native reference role or quantity factor changed after approval."
                    )
                review["profiles"] = coal_tar_review(eco_rows, review["evidence"])
                review["reason"] = (
                    "The production proxy is approved only for the ten inspected positive-reference profiles. Four older consequential profiles remain blocked."
                )
            if profiled_omission:
                if target != PROFILE_OMISSION_TARGETS[source] or not product or uvek_signs != {1} or original != 1:
                    raise ValueError("Profile omission supplier, role or factor changed; repeat the review.")
                review["profiles"] = profile_omission_profiles(
                    source,
                    eco_rows,
                    review["evidence"],
                    (previous_decision or {}).get("review", {}).get("profiles", []),
                )
                review["status"] = "unresolved"
                if source in APPROVED_STEEL_RECOVERY_OMISSIONS:
                    review["reason"] = (
                        "The waste-to-recyclable handoff is independently approved for omission only in the enumerated exact source profiles. Missing profiles remain blocked."
                    )
                elif source in APPROVED_FEED_SUBSTITUTION_OMISSIONS:
                    review["reason"] = (
                        "Only the enumerated consequential profiles approve omission of nutritional feed-substitution contributions. Positive-reference product supply and missing/unreviewed profiles receive no omission approval."
                    )
                elif source in APPROVED_NUTRIENT_REFERENCE_OMISSIONS:
                    review["reason"] = (
                        "Only enumerated exact profiles approve omission of this normalized nutritional reference. Positive reference coordinates do not validate the former non-nutritional provider; no missing-profile fallback is permitted."
                    )
                else:
                    review["reason"] = (
                        "Select only an exact reviewed source profile: older consequential combustion treatment is omitted with a warning; approved production profiles retain factor +1. Missing profiles remain blocked."
                    )
            if credit:
                if not product or uvek_signs != {1} or target != credit["target"] or original != 1:
                    raise ValueError("Recovery-credit target, native role or prior factor changed; repeat the review.")
                review["profiles"] = recovery_credit_profiles(
                    credit, eco_rows, credit_quantities[source], review["evidence"]
                )
                review["reason"] = (
                    "Only the exact inspected profiles independently approve a waste-reference to recovered-product credit conversion. Preserve its incoming sign with the reviewed positive quantity factor; no missing-profile fallback is permitted."
                )
            if not reason:
                review.update(
                    source_sign=-1 if forward and opposite_signs else 1,
                    target_sign=-1 if not forward and opposite_signs else 1,
                )
            factor = original if reason else (-1 if opposite_signs else 1) * abs(original)
            if treatment and "target_reference_mass_kg" in treatment_approval:
                factor = reviewed_reference_mass_factor(
                    source,
                    target,
                    original,
                    treatment_approval["target_reference_mass_kg"],
                )
            if forward and source in NATURAL_GAS_FLARES:
                if original != 1 or source[3] != "megajoule" or target[3] != "cubic meter":
                    raise ValueError("Natural-gas flare physical reference basis changed; repeat the review.")
                factor = -NATURAL_GAS_VOLUME_PER_MJ
            decisions[rule["id"]] = {
                "binding": rule_binding(rule),
                "original_factor": original,
                "factor": factor,
                "source_observations": [list(row) for row in sorted(eco_rows)],
                "uvek_reference_sign": (next(iter(uvek_signs)) if len(uvek_signs) == 1 else None),
                "uvek_native_flow_type": native_row.get("native_flow_type"),
                "review": review,
            }
            if profiled_omission:
                decisions[rule["id"]]["profile_omission_review"] = deepcopy(APPROVED_PROFILED_OMISSIONS[source])
            if omission:
                decisions[rule["id"]].update(
                    operation="delete",
                    original_rule=deepcopy(
                        previous_decision["original_rule"]
                        if previous_decision and "original_rule" in previous_decision
                        else rule
                    ),
                    omission_review=deepcopy(omission["review"]),
                )
                if "review_ref" in rule:
                    decisions[rule["id"]]["original_review"] = deepcopy(
                        previous_decision["original_review"]
                        if previous_decision and "original_review" in previous_decision
                        else rule_review(resource, rule)
                    )
            if coal_tar:
                decision = decisions[rule["id"]]
                decision["supplier_proxy_review"] = {
                    "rationale": "Approved on 2026-11-04: use Tar, at coke plant (GLO, kilogram) as a disclosed approximate production proxy for market for coal tar only in the inspected positive-reference source profiles, with factor +1. Older negative-reference consequential profiles are not approved.",
                    "limitations": [COAL_TAR_LIMITATIONS],
                    "evidence": [
                        *review["evidence"],
                        "docs/workflows/ecoinvent-to-uvek.rst; approved coal-tar production proxy",
                    ],
                }
                if previous_decision and "original_review" in previous_decision:
                    decision["original_review"] = previous_decision["original_review"]
                    decision["original_information_loss"] = previous_decision["original_information_loss"]
                else:
                    decision["original_review"] = rule_review(resource, rule) if "review_ref" in rule else None
                    decision["original_information_loss"] = rule.get("technosphere_information_loss")
            if treatment or profiled_approval:
                approval = treatment_approval or profiled_approval
                decision = decisions[rule["id"]]
                if treatment:
                    decision["original_target"] = dict(
                        zip(
                            FIELDS,
                            treatment_approval.get("previous_target", BATTERY_PREVIOUS_TARGET),
                            strict=True,
                        )
                    )
                decision["supplier_proxy_review"] = {
                    "rationale": approval.get("rationale")
                    or f"Approved on 2026-11-04: replace the graphite-anode manufacturing proxy for the exact used-Li-ion-battery waste reference with the GLO, kilogram {approval['technology']} disposal supplier. Apply factor -1 to convert the inspected negative ecoinvent waste reference to UVEK's positive service reference. {approval['excluded_references']}",
                    "limitations": [approval["limitations"]],
                    "evidence": [
                        *review["evidence"],
                        *(
                            approval.get("inventory_evidence")
                            or [
                                f"Inspected ecoinvent 3.12 cut-off activity {treatment_approval['ecoinvent_code']}: Waste reference, production -1 kg.",
                                f"Inspected UVEK 2025 activity {treatment_approval['uvek_code']}: production +1 kg; 100% allocation to disposal; verified native WASTE_FLOW input reference.",
                                f"docs/workflows/ecoinvent-to-uvek.rst; approved Li-ion battery {technology} treatment",
                            ]
                        ),
                    ],
                }
                if previous_decision and "original_review" in previous_decision:
                    decision["original_review"] = previous_decision["original_review"]
                    decision["original_information_loss"] = previous_decision["original_information_loss"]
                else:
                    decision["original_review"] = rule_review(resource, rule) if "review_ref" in rule else None
                    decision["original_information_loss"] = rule.get("technosphere_information_loss")
            if credit:
                decision = decisions[rule["id"]]
                decision["original_target"] = dict(zip(FIELDS, credit["previous_target"], strict=True))
                decision["supplier_proxy_review"] = deepcopy(credit["review"])
                decision["recovery_credit_observations"] = [
                    {"source_profile": label, **record} for label, record in sorted(credit_quantities[source])
                ]
                decision["original_review"] = deepcopy(
                    (previous_decision["original_review"] or None)
                    if previous_decision and "original_review" in previous_decision
                    else rule_review(resource, rule) if "review_ref" in rule else None
                )
                decision["original_information_loss"] = (
                    previous_decision.get("original_information_loss")
                    if previous_decision and "original_review" in previous_decision
                    else rule.get("technosphere_information_loss")
                )
            counts[
                (
                    "partially_approved_natural_gas_flare"
                    if forward and source in NATURAL_GAS_FLARES
                    else (
                        "approved_discharge_proxy"
                        if discharge
                        else (
                            "approved_recovery_credit"
                            if credit
                            else (
                                "omitted"
                                if omission
                                else (
                                    "approved_by_profile"
                                    if profiled_approval
                                    else (
                                        (
                                            "partially_approved_with_profile_omissions"
                                            if any(
                                                entry["review"]["status"] == "unresolved"
                                                for entry in review["profiles"]
                                            )
                                            else "approved_with_profile_omissions"
                                        )
                                        if profiled_omission
                                        else (
                                            "unresolved"
                                            if reason
                                            else (
                                                "corrected"
                                                if original != factor
                                                else (
                                                    "already_correct"
                                                    if opposite_signs
                                                    else "same_sign_service_unchanged"
                                                )
                                            )
                                        )
                                    )
                                )
                            )
                        )
                    )
                )
            ] += 1
        resources[resource["name"]] = decisions
        summary[filename] = dict(sorted(counts.items()))
    provenance = [
        {
            **{
                key: value
                for key, value in profile.items()
                if key
                not in (
                    "references",
                    "pp_uncontrolled_source_inventories",
                    "ghana_uncontrolled_inventories",
                    "refrigerant_patch_formula_inventories",
                    "digester_stock_inventories",
                    "swiss_cement_market_inventories",
                    "copper_ash_inventories",
                    "waste_concrete_inert_inventories",
                    "older_pet_whole_freight_inventories",
                    "metalliferous_stock_inventories",
                    "metalliferous_stock_market_inventories",
                    "precious_anode_inventories",
                    "recovery_credits",
                    "venting_inventories",
                    "disposal_inventories",
                    "reclamation_inventories",
                    "cutoff_stock_inventories",
                    "discharge_inventories",
                    "flare_inventories",
                    "natural_gas_flare_inventories",
                    "cutoff_feed_product_inventories",
                    "eaf_landfill_inventories",
                    "lcd_treatment_inventories",
                    "dedicated_disposal_inventories",
                    "modal_transport_inventories",
                    "antifouling_inventories",
                    "industrial_gas_inventories",
                    "wwt_residue_inventories",
                    "reverse_gas_inventories",
                    "gas_market_inventories",
                    "market_composition_inventories",
                    "esterquat_market_component_inventories",
                    "reverse_ash_inventories",
                    "processing_composition_inventories",
                    "cutoff_slag_product_inventories",
                    "sewer_grid_inventories",
                    "slag_landfill_component_inventories",
                    "ash_market_inventories",
                    "bottom_ash_inventories",
                    "stainless_stock_inventories",
                    "residual_landfill_inventories",
                    "resolved_child_market_inventories",
                    "sewer_throughput_inventories",
                    "packaging_paper_landfarming_inventories",
                    "steel_residue_market_inventories",
                    "industrial_landfarming_inventories",
                    "industrial_waste_market_inventories",
                    "additional_cutoff_waste_market_inventories",
                    "uncontrolled_disposal_inventories",
                    "pet_waste_market_inventories",
                    "five_simple_waste_market_inventories",
                    "swiss_pet_fluwa_inventories",
                    "slf_landfarming_inventories",
                    "average_sludge_landfarming_inventories",
                    "exact_patch_boundary_inventories",
                    "complete_patch_boundary_inventories",
                    "cable_rail_inventories",
                    "pet_market_group_inventories",
                    "copper_stock_inventories",
                    "copper_cathode_inventories",
                    "pam_frontier_stock_inventories",
                    "regional_freight_inventories",
                    "older_regional_road_inventories",
                    "chemical_heat_inventories",
                    "whole_freight_inventories",
                    "packed_wool_inventories",
                    "sewage_digestion_inventories",
                )
            },
            **(
                {
                    "sewer_grid_inventory_count": len(profile["sewer_grid_inventories"]),
                    "sewer_grid_inventory_sha256": hashlib.sha256(
                        json.dumps(profile["sewer_grid_inventories"], sort_keys=True).encode()
                    ).hexdigest(),
                }
                if profile.get("sewer_grid_inventories")
                else {}
            ),
            **(
                {
                    "slag_landfill_component_inventory_count": len(profile["slag_landfill_component_inventories"]),
                    "slag_landfill_component_inventory_sha256": hashlib.sha256(
                        json.dumps(profile["slag_landfill_component_inventories"], sort_keys=True).encode()
                    ).hexdigest(),
                }
                if profile.get("slag_landfill_component_inventories")
                else {}
            ),
            **(
                {
                    "ash_market_inventory_count": len(profile["ash_market_inventories"]),
                    "ash_market_inventory_sha256": hashlib.sha256(
                        json.dumps(profile["ash_market_inventories"], sort_keys=True).encode()
                    ).hexdigest(),
                }
                if profile.get("ash_market_inventories")
                else {}
            ),
            **(
                {
                    "bottom_ash_inventory_count": len(profile["bottom_ash_inventories"]),
                    "bottom_ash_inventory_sha256": hashlib.sha256(
                        json.dumps(profile["bottom_ash_inventories"], sort_keys=True).encode()
                    ).hexdigest(),
                }
                if profile.get("bottom_ash_inventories")
                else {}
            ),
            **(
                {
                    "stainless_stock_inventory_count": len(profile["stainless_stock_inventories"]),
                    "stainless_stock_inventory_sha256": hashlib.sha256(
                        json.dumps(profile["stainless_stock_inventories"], sort_keys=True).encode()
                    ).hexdigest(),
                }
                if profile.get("stainless_stock_inventories")
                else {}
            ),
            **(
                {
                    "residual_landfill_inventory_count": len(profile["residual_landfill_inventories"]),
                    "residual_landfill_inventory_sha256": hashlib.sha256(
                        json.dumps(profile["residual_landfill_inventories"], sort_keys=True).encode()
                    ).hexdigest(),
                }
                if profile.get("residual_landfill_inventories")
                else {}
            ),
            **(
                {
                    "resolved_child_market_inventory_count": len(profile["resolved_child_market_inventories"]),
                    "resolved_child_market_inventory_sha256": hashlib.sha256(
                        json.dumps(profile["resolved_child_market_inventories"], sort_keys=True).encode()
                    ).hexdigest(),
                }
                if profile.get("resolved_child_market_inventories")
                else {}
            ),
            **(
                {
                    "sewer_throughput_inventory_count": len(profile["sewer_throughput_inventories"]),
                    "sewer_throughput_inventory_sha256": hashlib.sha256(
                        json.dumps(profile["sewer_throughput_inventories"], sort_keys=True).encode()
                    ).hexdigest(),
                }
                if profile.get("sewer_throughput_inventories")
                else {}
            ),
            **(
                {
                    "packaging_paper_landfarming_inventory_count": len(
                        profile["packaging_paper_landfarming_inventories"]
                    ),
                    "packaging_paper_landfarming_inventory_sha256": hashlib.sha256(
                        json.dumps(profile["packaging_paper_landfarming_inventories"], sort_keys=True).encode()
                    ).hexdigest(),
                }
                if profile.get("packaging_paper_landfarming_inventories")
                else {}
            ),
            **(
                {
                    "steel_residue_market_inventory_count": len(profile["steel_residue_market_inventories"]),
                    "steel_residue_market_inventory_sha256": hashlib.sha256(
                        json.dumps(profile["steel_residue_market_inventories"], sort_keys=True).encode()
                    ).hexdigest(),
                }
                if profile.get("steel_residue_market_inventories")
                else {}
            ),
            **(
                {
                    "industrial_landfarming_inventory_count": len(profile["industrial_landfarming_inventories"]),
                    "industrial_landfarming_inventory_sha256": hashlib.sha256(
                        json.dumps(profile["industrial_landfarming_inventories"], sort_keys=True).encode()
                    ).hexdigest(),
                }
                if profile.get("industrial_landfarming_inventories")
                else {}
            ),
            **(
                {
                    "industrial_waste_market_inventory_count": len(profile["industrial_waste_market_inventories"]),
                    "industrial_waste_market_inventory_sha256": hashlib.sha256(
                        json.dumps(profile["industrial_waste_market_inventories"], sort_keys=True).encode()
                    ).hexdigest(),
                }
                if profile.get("industrial_waste_market_inventories")
                else {}
            ),
            **(
                {
                    "additional_cutoff_waste_market_inventory_count": len(
                        profile["additional_cutoff_waste_market_inventories"]
                    ),
                    "additional_cutoff_waste_market_inventory_sha256": hashlib.sha256(
                        json.dumps(profile["additional_cutoff_waste_market_inventories"], sort_keys=True).encode()
                    ).hexdigest(),
                }
                if profile.get("additional_cutoff_waste_market_inventories")
                else {}
            ),
            **(
                {
                    "uncontrolled_disposal_inventory_count": len(profile["uncontrolled_disposal_inventories"]),
                    "uncontrolled_disposal_inventory_sha256": hashlib.sha256(
                        json.dumps(profile["uncontrolled_disposal_inventories"], sort_keys=True).encode()
                    ).hexdigest(),
                }
                if profile.get("uncontrolled_disposal_inventories")
                else {}
            ),
            **(
                {
                    "pet_waste_market_inventory_count": len(profile["pet_waste_market_inventories"]),
                    "pet_waste_market_inventory_sha256": hashlib.sha256(
                        json.dumps(profile["pet_waste_market_inventories"], sort_keys=True).encode()
                    ).hexdigest(),
                }
                if profile.get("pet_waste_market_inventories")
                else {}
            ),
            **(
                {
                    "five_simple_waste_market_inventory_count": len(profile["five_simple_waste_market_inventories"]),
                    "five_simple_waste_market_inventory_sha256": hashlib.sha256(
                        json.dumps(profile["five_simple_waste_market_inventories"], sort_keys=True).encode()
                    ).hexdigest(),
                }
                if profile.get("five_simple_waste_market_inventories")
                else {}
            ),
            **(
                {
                    "swiss_pet_fluwa_inventory_count": len(profile["swiss_pet_fluwa_inventories"]),
                    "swiss_pet_fluwa_inventory_sha256": hashlib.sha256(
                        json.dumps(profile["swiss_pet_fluwa_inventories"], sort_keys=True).encode()
                    ).hexdigest(),
                }
                if profile.get("swiss_pet_fluwa_inventories")
                else {}
            ),
            **(
                {
                    "slf_landfarming_inventory_count": len(profile["slf_landfarming_inventories"]),
                    "slf_landfarming_inventory_sha256": hashlib.sha256(
                        json.dumps(profile["slf_landfarming_inventories"], sort_keys=True).encode()
                    ).hexdigest(),
                }
                if profile.get("slf_landfarming_inventories")
                else {}
            ),
            **(
                {
                    "average_sludge_landfarming_inventory_count": len(
                        profile["average_sludge_landfarming_inventories"]
                    ),
                    "average_sludge_landfarming_inventory_sha256": hashlib.sha256(
                        json.dumps(profile["average_sludge_landfarming_inventories"], sort_keys=True).encode()
                    ).hexdigest(),
                }
                if profile.get("average_sludge_landfarming_inventories")
                else {}
            ),
            **(
                {
                    "exact_patch_boundary_inventory_count": len(profile["exact_patch_boundary_inventories"]),
                    "exact_patch_boundary_inventory_sha256": hashlib.sha256(
                        json.dumps(profile["exact_patch_boundary_inventories"], sort_keys=True).encode()
                    ).hexdigest(),
                }
                if profile.get("exact_patch_boundary_inventories")
                else {}
            ),
            **(
                {
                    "complete_patch_boundary_inventory_count": len(profile["complete_patch_boundary_inventories"]),
                    "complete_patch_boundary_inventory_sha256": hashlib.sha256(
                        json.dumps(profile["complete_patch_boundary_inventories"], sort_keys=True).encode()
                    ).hexdigest(),
                }
                if profile.get("complete_patch_boundary_inventories")
                else {}
            ),
            **(
                {
                    "cable_rail_inventory_count": len(profile["cable_rail_inventories"]),
                    "cable_rail_inventory_sha256": hashlib.sha256(
                        json.dumps(profile["cable_rail_inventories"], sort_keys=True).encode()
                    ).hexdigest(),
                }
                if profile.get("cable_rail_inventories")
                else {}
            ),
            **(
                {
                    "pet_market_group_inventory_count": len(profile["pet_market_group_inventories"]),
                    "pet_market_group_inventory_sha256": hashlib.sha256(
                        json.dumps(profile["pet_market_group_inventories"], sort_keys=True).encode()
                    ).hexdigest(),
                }
                if profile.get("pet_market_group_inventories")
                else {}
            ),
            **(
                {
                    "packed_wool_inventory_count": len(profile["packed_wool_inventories"]),
                    "packed_wool_inventory_sha256": hashlib.sha256(
                        json.dumps(profile["packed_wool_inventories"], sort_keys=True).encode()
                    ).hexdigest(),
                }
                if profile.get("packed_wool_inventories")
                else {}
            ),
            **(
                {
                    "whole_freight_inventory_count": len(profile["whole_freight_inventories"]),
                    "whole_freight_inventory_sha256": hashlib.sha256(
                        json.dumps(profile["whole_freight_inventories"], sort_keys=True).encode()
                    ).hexdigest(),
                }
                if profile.get("whole_freight_inventories")
                else {}
            ),
            **(
                {
                    "chemical_heat_inventory_count": len(profile["chemical_heat_inventories"]),
                    "chemical_heat_inventory_sha256": hashlib.sha256(
                        json.dumps(profile["chemical_heat_inventories"], sort_keys=True).encode()
                    ).hexdigest(),
                }
                if profile.get("chemical_heat_inventories")
                else {}
            ),
            **(
                {
                    "pam_frontier_stock_inventory_count": len(profile["pam_frontier_stock_inventories"]),
                    "pam_frontier_stock_inventory_sha256": hashlib.sha256(
                        json.dumps(profile["pam_frontier_stock_inventories"], sort_keys=True).encode()
                    ).hexdigest(),
                }
                if profile.get("pam_frontier_stock_inventories")
                else {}
            ),
            **(
                {
                    "regional_freight_inventory_count": len(profile["regional_freight_inventories"]),
                    "regional_freight_inventory_sha256": hashlib.sha256(
                        json.dumps(profile["regional_freight_inventories"], sort_keys=True).encode()
                    ).hexdigest(),
                }
                if profile.get("regional_freight_inventories")
                else {}
            ),
            **(
                {
                    "older_regional_road_inventory_count": len(profile["older_regional_road_inventories"]),
                    "older_regional_road_inventory_sha256": hashlib.sha256(
                        json.dumps(profile["older_regional_road_inventories"], sort_keys=True).encode()
                    ).hexdigest(),
                }
                if profile.get("older_regional_road_inventories")
                else {}
            ),
            **(
                {
                    "copper_cathode_inventory_count": len(profile["copper_cathode_inventories"]),
                    "copper_cathode_inventory_sha256": hashlib.sha256(
                        json.dumps(profile["copper_cathode_inventories"], sort_keys=True).encode()
                    ).hexdigest(),
                }
                if profile.get("copper_cathode_inventories")
                else {}
            ),
            **(
                {
                    "copper_stock_inventory_count": len(profile["copper_stock_inventories"]),
                    "copper_stock_inventory_sha256": hashlib.sha256(
                        json.dumps(profile["copper_stock_inventories"], sort_keys=True).encode()
                    ).hexdigest(),
                }
                if profile.get("copper_stock_inventories")
                else {}
            ),
            **(
                {
                    "sewage_digestion_inventory_count": len(profile["sewage_digestion_inventories"]),
                    "sewage_digestion_inventory_sha256": hashlib.sha256(
                        json.dumps(profile["sewage_digestion_inventories"], sort_keys=True).encode()
                    ).hexdigest(),
                }
                if profile.get("sewage_digestion_inventories")
                else {}
            ),
            **(
                {
                    "cutoff_slag_product_inventory_count": len(profile["cutoff_slag_product_inventories"]),
                    "cutoff_slag_product_inventory_sha256": hashlib.sha256(
                        json.dumps(profile["cutoff_slag_product_inventories"], sort_keys=True).encode()
                    ).hexdigest(),
                }
                if profile.get("cutoff_slag_product_inventories")
                else {}
            ),
            **(
                {
                    "processing_composition_inventory_count": len(profile["processing_composition_inventories"]),
                    "processing_composition_inventory_sha256": hashlib.sha256(
                        json.dumps(
                            profile["processing_composition_inventories"],
                            sort_keys=True,
                        ).encode()
                    ).hexdigest(),
                }
                if profile.get("processing_composition_inventories")
                else {}
            ),
            **(
                {
                    "reverse_ash_inventory_count": len(profile["reverse_ash_inventories"]),
                    "reverse_ash_inventory_sha256": hashlib.sha256(
                        json.dumps(profile["reverse_ash_inventories"], sort_keys=True).encode()
                    ).hexdigest(),
                }
                if profile.get("reverse_ash_inventories")
                else {}
            ),
            **(
                {
                    "market_composition_inventory_count": len(profile["market_composition_inventories"]),
                    "market_composition_inventory_sha256": hashlib.sha256(
                        json.dumps(profile["market_composition_inventories"], sort_keys=True).encode()
                    ).hexdigest(),
                }
                if profile.get("market_composition_inventories")
                else {}
            ),
            **(
                {
                    "gas_market_inventory_count": len(profile["gas_market_inventories"]),
                    "gas_market_inventory_sha256": hashlib.sha256(
                        json.dumps(profile["gas_market_inventories"], sort_keys=True).encode()
                    ).hexdigest(),
                }
                if profile.get("gas_market_inventories")
                else {}
            ),
            **(
                {
                    "reverse_gas_inventory_count": len(profile["reverse_gas_inventories"]),
                    "reverse_gas_inventory_sha256": hashlib.sha256(
                        json.dumps(profile["reverse_gas_inventories"], sort_keys=True).encode()
                    ).hexdigest(),
                }
                if profile.get("reverse_gas_inventories")
                else {}
            ),
            **(
                {
                    "wwt_residue_inventory_count": len(profile["wwt_residue_inventories"]),
                    "wwt_residue_inventory_sha256": hashlib.sha256(
                        json.dumps(profile["wwt_residue_inventories"], sort_keys=True).encode()
                    ).hexdigest(),
                }
                if profile.get("wwt_residue_inventories")
                else {}
            ),
            **(
                {
                    "industrial_gas_inventory_count": len(profile["industrial_gas_inventories"]),
                    "industrial_gas_inventory_sha256": hashlib.sha256(
                        json.dumps(profile["industrial_gas_inventories"], sort_keys=True).encode()
                    ).hexdigest(),
                }
                if profile.get("industrial_gas_inventories")
                else {}
            ),
            **(
                {
                    "antifouling_inventory_count": len(profile["antifouling_inventories"]),
                    "antifouling_inventory_sha256": hashlib.sha256(
                        json.dumps(profile["antifouling_inventories"], sort_keys=True).encode()
                    ).hexdigest(),
                }
                if profile.get("antifouling_inventories")
                else {}
            ),
            **(
                {
                    "discharge_inventory_count": len(profile["discharge_inventories"]),
                    "discharge_inventory_sha256": hashlib.sha256(
                        json.dumps(profile["discharge_inventories"], sort_keys=True).encode()
                    ).hexdigest(),
                }
                if profile.get("discharge_inventories")
                else {}
            ),
            **(
                {
                    "reclamation_inventory_count": len(profile["reclamation_inventories"]),
                    "reclamation_inventory_sha256": hashlib.sha256(
                        json.dumps(profile["reclamation_inventories"], sort_keys=True).encode()
                    ).hexdigest(),
                }
                if profile.get("reclamation_inventories")
                else {}
            ),
            **(
                {
                    "disposal_inventory_count": len(profile["disposal_inventories"]),
                    "disposal_inventory_sha256": hashlib.sha256(
                        json.dumps(profile["disposal_inventories"], sort_keys=True).encode()
                    ).hexdigest(),
                }
                if profile.get("disposal_inventories")
                else {}
            ),
            **(
                {
                    "venting_inventory_count": len(profile["venting_inventories"]),
                    "venting_inventory_sha256": hashlib.sha256(
                        json.dumps(profile["venting_inventories"], sort_keys=True).encode()
                    ).hexdigest(),
                }
                if profile.get("venting_inventories")
                else {}
            ),
            **(
                {
                    "recovery_credit_count": len(profile["recovery_credits"]),
                    "recovery_credit_sha256": hashlib.sha256(
                        json.dumps(profile["recovery_credits"], sort_keys=True).encode()
                    ).hexdigest(),
                }
                if profile.get("recovery_credits")
                else {}
            ),
            **(
                {
                    "cutoff_stock_inventory_count": len(profile["cutoff_stock_inventories"]),
                    "cutoff_stock_inventory_sha256": hashlib.sha256(
                        json.dumps(profile["cutoff_stock_inventories"], sort_keys=True).encode()
                    ).hexdigest(),
                }
                if profile.get("cutoff_stock_inventories")
                else {}
            ),
            "reference_count": len(profile["references"]),
            **(
                {
                    "modal_transport_inventory_count": len(profile["modal_transport_inventories"]),
                    "modal_transport_inventory_sha256": hashlib.sha256(
                        json.dumps(profile["modal_transport_inventories"], sort_keys=True).encode()
                    ).hexdigest(),
                }
                if profile.get("modal_transport_inventories")
                else {}
            ),
            **(
                {
                    "lcd_treatment_inventory_count": len(profile["lcd_treatment_inventories"]),
                    "lcd_treatment_inventory_sha256": hashlib.sha256(
                        json.dumps(profile["lcd_treatment_inventories"], sort_keys=True).encode()
                    ).hexdigest(),
                }
                if profile.get("lcd_treatment_inventories")
                else {}
            ),
            **(
                {
                    "dedicated_disposal_inventory_count": len(profile["dedicated_disposal_inventories"]),
                    "dedicated_disposal_inventory_sha256": hashlib.sha256(
                        json.dumps(profile["dedicated_disposal_inventories"], sort_keys=True).encode()
                    ).hexdigest(),
                }
                if profile.get("dedicated_disposal_inventories")
                else {}
            ),
            **(
                {
                    "eaf_landfill_inventory_count": len(profile["eaf_landfill_inventories"]),
                    "eaf_landfill_inventory_sha256": hashlib.sha256(
                        json.dumps(profile["eaf_landfill_inventories"], sort_keys=True).encode()
                    ).hexdigest(),
                }
                if profile.get("eaf_landfill_inventories")
                else {}
            ),
            **(
                {
                    "flare_inventory_count": len(profile["flare_inventories"]),
                    "flare_inventory_sha256": hashlib.sha256(
                        json.dumps(profile["flare_inventories"], sort_keys=True).encode()
                    ).hexdigest(),
                }
                if profile.get("flare_inventories")
                else {}
            ),
            **(
                {
                    "natural_gas_flare_inventory_count": len(profile["natural_gas_flare_inventories"]),
                    "natural_gas_flare_inventory_sha256": hashlib.sha256(
                        json.dumps(profile["natural_gas_flare_inventories"], sort_keys=True).encode()
                    ).hexdigest(),
                }
                if profile.get("natural_gas_flare_inventories")
                else {}
            ),
            **(
                {
                    "cutoff_feed_product_inventory_count": len(profile["cutoff_feed_product_inventories"]),
                    "cutoff_feed_product_inventories_sha256": hashlib.sha256(
                        json.dumps(profile["cutoff_feed_product_inventories"], sort_keys=True).encode()
                    ).hexdigest(),
                }
                if profile.get("cutoff_feed_product_inventories")
                else {}
            ),
            **(
                {
                    "esterquat_market_component_inventory_count": len(
                        profile["esterquat_market_component_inventories"]
                    ),
                    "esterquat_market_component_inventories_sha256": hashlib.sha256(
                        json.dumps(profile["esterquat_market_component_inventories"], sort_keys=True).encode()
                    ).hexdigest(),
                }
                if profile.get("esterquat_market_component_inventories")
                else {}
            ),
            "reference_sign_sha256": hashlib.sha256(
                json.dumps(profile["references"], sort_keys=True).encode()
            ).hexdigest(),
            "negative_reference_count": sum(row[-1] < 0 for row in profile["references"]),
        }
        for profile in profiles
    ]
    return {
        "schema_version": 1,
        "generator": "scripts/review_uvek_reference_signs.py",
        "native_reference_sha256": hashlib.sha256(native_bytes).hexdigest(),
        "provenance": provenance,
        "limitations": [
            "Reference signs establish coordinates, not scientific equivalence or equal LCIA results.",
            "Shared identities with conflicting signs require profile-specific scientific review.",
            "An unapproved waste/product role mismatch remains unresolved even under permissive policy; exact inspected service-output approvals are recorded separately.",
        ],
        "summary": summary,
        "resources": resources,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--evidence", type=Path)
    parser.add_argument("--brightway-root", type=Path)
    args = parser.parse_args()
    path = args.root / REVIEW_PATH
    if args.evidence or args.brightway_root:
        if args.evidence and args.brightway_root:
            parser.error("Choose --evidence or --brightway-root, not both.")
        profiles = (
            json.loads(args.evidence.read_text(encoding="utf-8"))
            if args.evidence
            else collect_reference_profiles(
                args.brightway_root.resolve(),
                json.loads(path.read_text(encoding="utf-8"))["provenance"],
            )
        )
        ledger = build_review(args.root, profiles)
        path.write_text(dumps_review(ledger), encoding="utf-8", newline="\n")
    else:
        ledger = json.loads(path.read_text(encoding="utf-8"))
    for filename in (FORWARD, REVERSE):
        resource_path = args.root / "brightpath/data/migrations/uvek" / filename
        resource = json.loads(resource_path.read_text(encoding="utf-8"))
        apply_sign_review(resource, ledger)
        resource_path.write_text(dumps_migration_resource(resource), encoding="utf-8", newline="\n")
    print(json.dumps(ledger["summary"], indent=2))


def dumps_review(ledger):
    """Keep sparse audit decisions independently reviewable, one rule per line."""
    metadata = {key: value for key, value in ledger.items() if key != "resources"}
    lines = [json.dumps(metadata, indent=2, sort_keys=True)[:-2] + ',\n  "resources": {']
    resources = sorted(ledger["resources"].items())
    for resource_index, (name, decisions) in enumerate(resources):
        lines.append(f"    {json.dumps(name)}: {{")
        entries = sorted(decisions.items())
        for decision_index, (rule_id, decision) in enumerate(entries):
            suffix = "," if decision_index + 1 < len(entries) else ""
            lines.append(f"      {json.dumps(rule_id)}: {json.dumps(decision, sort_keys=True)}{suffix}")
        lines.append("    }" + ("," if resource_index + 1 < len(resources) else ""))
    lines.extend(["  }", "}", ""])
    return "\n".join(lines)


if __name__ == "__main__":
    main()
