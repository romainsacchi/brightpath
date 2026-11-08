"""Unsupported industrial CRT wastewater residues are omitted with a complete audit."""

import json
import math
from collections import Counter
from copy import deepcopy

import pytest

from brightpath import (
    DATA_DIR,
    BackgroundContext,
    BiosphereProfile,
    BrightwayInventory,
    FormatProfile,
    InventoryContext,
    TechnosphereProfile,
)
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.migrations.resources import load_uvek_technosphere_resource
from scripts.review_uvek_reference_signs import APPROVED_CRT_WASTEWATER_RESIDUE_OMISSIONS

FIELDS = ("name", "reference product", "location", "unit")
SOURCES = list(APPROVED_CRT_WASTEWATER_RESIDUE_OMISSIONS)
PROFILES = [
    profile
    for profile in [(v, m) for v in ("3.9", "3.10", "3.11", "3.12") for m in ("cutoff", "consequential")]
    if profile[1] == "cutoff"
]


def context(family, version, model="cutoff"):
    return InventoryContext(
        FormatProfile("brightway_excel"),
        BackgroundContext(TechnosphereProfile(family, version, model), BiosphereProfile("ecoinvent", "3.10")),
    )


def consumer(amount):
    output = {"name": "CRT residue omission consumer", "reference product": "output", "location": "CH", "unit": "unit"}
    exchanges = [{**output, "type": "production", "amount": 1}]
    for source in SOURCES:
        supplier = dict(zip(FIELDS, source, strict=True))
        exchanges.extend(
            [
                {
                    **supplier,
                    "type": "technosphere",
                    "amount": amount,
                    "formula": "crt_residue_mass",
                    "uncertainty type": 2,
                    "loc": math.log(abs(amount)) if amount else 0,
                    "negative": amount < 0,
                    "scale": 0.2,
                    "audit_note": "original",
                },
                {
                    **supplier,
                    "type": "substitution",
                    "amount": amount,
                    "formula": "crt_residue_mass",
                    "uncertainty type": 3,
                    "loc": amount,
                    "minimum": amount - 1,
                    "maximum": amount + 1,
                    "scale": 0.2,
                    "audit_note": "original",
                },
            ]
        )
    exchanges.append(
        {
            "name": "Carbon dioxide, fossil",
            "categories": ["air"],
            "unit": "kilogram",
            "amount": 0.5,
            "type": "biosphere",
        }
    )
    return [{**output, "exchanges": exchanges}]


@pytest.mark.parametrize("version,model", PROFILES)
@pytest.mark.parametrize("release", ["2025", "2026"])
@pytest.mark.parametrize("amount", [-10, 0, 10])
def test_crt_residue_omissions_warn_preserve_original_exchanges_and_keep_foreground_carbon(
    version, model, release, amount
):
    data = consumer(amount)
    before = deepcopy(data)
    inventory = BrightwayInventory.from_data(data, context=context("ecoinvent", version, model))
    policy = MigrationPolicy(
        on_heuristic_mapping=PolicyAction.WARN, on_deletion=PolicyAction.ERROR, on_information_loss=PolicyAction.ERROR
    )
    result = inventory.migrate_background(context("uvek", release).background, policy=policy)
    assert result.data[0]["exchanges"] == [before[0]["exchanges"][0], before[0]["exchanges"][-1]]
    report = result.last_migration_report
    warnings = [i for i in report.issues if i.code == "migration.technosphere_omission"]
    losses = [loss for loss in report.losses if loss.code == "migration.technosphere_omission"]
    assert len(warnings) == len(losses) == 36
    assert all(i.severity.value == "warning" for i in warnings)
    assert all("score effects are method-dependent and unquantified" in i.message for i in warnings)
    assert all("Any associated credits are excluded" in i.message for i in warnings)
    original_by_path = {
        f"datasets[0].exchanges[{i}]": e
        for i, e in enumerate(before[0]["exchanges"])
        if 0 < i < len(before[0]["exchanges"]) - 1
    }
    for loss in losses:
        assert loss.details["source_exchange"] == original_by_path[loss.path]
        assert loss.details["approved_omission"] is True
    assert not any(i.code == "migration.reference_flow_sign_unresolved" for i in report.issues)
    assert inventory.data == data == before


def test_crt_residue_omission_evidence_retains_both_prior_native_roles_and_all_routes():
    resource = load_uvek_technosphere_resource()
    ledger = json.loads((DATA_DIR / "export/uvek_reference_flow_sign_review.json").read_text())
    rules = [
        r
        for r in resource["delete"]
        if tuple(r["source"][f] for f in FIELDS) in APPROVED_CRT_WASTEWATER_RESIDUE_OMISSIONS
    ]
    assert len(rules) == 18
    assert sum(a["approval_scope"] == "explicit" for a in APPROVED_CRT_WASTEWATER_RESIDUE_OMISSIONS.values()) == 2
    assert {a["route"] for a in APPROVED_CRT_WASTEWATER_RESIDUE_OMISSIONS.values()} == {
        "sludge landfill",
        "sludge incineration",
        "sludge landfarming",
        "bottom-ash landfill",
        "residue landfill",
    }
    roles = Counter()
    historical_factors = Counter()
    observations = 0
    for rule in rules:
        source = tuple(rule["source"][f] for f in FIELDS)
        approval = APPROVED_CRT_WASTEWATER_RESIDUE_OMISSIONS[source]
        decision = ledger["resources"][resource["name"]][rule["id"]]
        assert decision["operation"] == "delete"
        assert tuple(decision["original_rule"]["target"][f] for f in FIELDS) == approval["previous_target"]
        # Preserve all historical evidence, independently of executable scope.
        assert decision["source_observations"] == sorted(
            [[f"ecoinvent-{v}-{m}", -1] for v in ("3.9", "3.10", "3.11", "3.12") for m in ("cutoff", "consequential")]
        )
        roles[decision["uvek_native_flow_type"]] += 1
        historical_factors[decision["original_rule"].get("conversion_factor", 1)] += 1
        observations += len(decision["source_observations"])
    assert roles == {"WASTE_FLOW": 14, "PRODUCT_FLOW": 4}
    assert historical_factors == {-1: 14, 1: 4}
    assert observations == 144


def test_all_matching_foreground_crt_residue_suppliers_remain_intact():
    data = consumer(-10)
    for source in SOURCES:
        supplier = dict(zip(FIELDS, source, strict=True))
        data.append({**supplier, "exchanges": [{**supplier, "amount": 1, "type": "production"}]})
    before = deepcopy(data)
    result = BrightwayInventory.from_data(data, context=context("ecoinvent", "3.12")).migrate_background(
        context("uvek", "2025").background, policy=MigrationPolicy.permissive()
    )
    assert result.data == data == before
    assert not any(i.code == "migration.technosphere_omission" for i in result.last_migration_report.issues)
