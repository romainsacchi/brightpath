"""Reviewed plastic-incineration proxies preserve mass and disclose composition loss."""

import json
import math
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
from brightpath.background.reference_signs import select_profile_sign_rules
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource
from scripts.review_uvek_reference_signs import APPROVED_INCINERATION_COMPOSITION_PROXIES, PET_DUST_SOURCE

FIELDS = ("name", "reference product", "location", "unit")
CASES = [
    (source, version, model)
    for source, approval in APPROVED_INCINERATION_COMPOSITION_PROXIES.items()
    for version in approval["source_versions"]
    for model in ("cutoff",)
]


def context(family, version, model="cutoff"):
    return InventoryContext(
        FormatProfile("brightway_excel"),
        BackgroundContext(TechnosphereProfile(family, version, model), BiosphereProfile("ecoinvent", "3.10")),
    )


def consumer(source, amount):
    output = {"name": "Composition consumer", "reference product": "output", "location": "CH", "unit": "unit"}
    supplier = dict(zip(FIELDS, source, strict=True))
    return [
        {
            **output,
            "exchanges": [
                {**output, "amount": 1, "type": "production"},
                {
                    **supplier,
                    "amount": amount,
                    "type": "technosphere",
                    "formula": "waste_mass",
                    "uncertainty type": 2,
                    "loc": math.log(abs(amount)) if amount else 0,
                    "negative": amount < 0,
                    "scale": 0.2,
                },
                {
                    **supplier,
                    "amount": amount,
                    "type": "substitution",
                    "formula": "waste_mass",
                    "uncertainty type": 3,
                    "loc": amount,
                    "minimum": amount - 1,
                    "maximum": amount + 1,
                    "scale": 0.2,
                },
            ],
        }
    ]


@pytest.mark.parametrize("source,version,model", CASES)
@pytest.mark.parametrize("release", ["2025", "2026"])
@pytest.mark.parametrize("amount", [-10, 0, 10])
def test_incineration_composition_proxies_preserve_mass_uncertainty_and_formulas(
    source, version, model, release, amount
):
    data = consumer(source, amount)
    before = deepcopy(data)
    inventory = BrightwayInventory.from_data(data, context=context("ecoinvent", version, model))
    result = inventory.migrate_background(context("uvek", release).background, policy=MigrationPolicy.permissive())
    assert len(result.data[0]["exchanges"]) == 3
    assert result.data[0]["exchanges"][0] == before[0]["exchanges"][0]
    for original, exchange in zip(before[0]["exchanges"][1:], result.data[0]["exchanges"][1:], strict=True):
        assert tuple(exchange[field] for field in FIELDS) == APPROVED_INCINERATION_COMPOSITION_PROXIES[source]["target"]
        assert exchange["amount"] == -amount
        assert exchange["type"] == original["type"]
        assert exchange["formula"] == "(waste_mass) * (-1.0)"
        assert exchange["scale"] == original["scale"]
        if original["uncertainty type"] == 2:
            assert exchange["loc"] == original["loc"]
            if amount:
                assert exchange["negative"] is (amount > 0)
        else:
            assert exchange["loc"] == -amount
            assert exchange["minimum"] == -amount - 1
            assert exchange["maximum"] == -amount + 1
    report = result.last_migration_report
    warnings = [i for i in report.issues if i.code == "migration.technosphere_information_loss"]
    assert len(warnings) == 2 and all(i.severity.value == "warning" for i in warnings)
    source_facts = (
        ("2.7027%", "37.1 MJ/kg", "23.13 MJ/kg")
        if source == PET_DUST_SOURCE
        else (
            "13.775%",
            "38.79 MJ/kg",
            "42.82 MJ/kg",
            "100% LDPE",
        )
    )
    for warning in warnings:
        assert all(fact in warning.message for fact in source_facts)
        assert "without heating-value or moisture scaling" in warning.message
        assert "consequential electricity and heat substitution credits are not reproduced" in warning.message
        assert "RER is approximated by CH" in warning.message
    assert len([loss for loss in report.losses if loss.code == "migration.technosphere_information_loss"]) == 2
    assert inventory.data == data == before


@pytest.mark.parametrize("source", APPROVED_INCINERATION_COMPOSITION_PROXIES)
def test_composition_proxies_require_explicit_information_loss_acceptance(source):
    data = consumer(source, -10)
    inventory = BrightwayInventory.from_data(data, context=context("ecoinvent", "3.12"))
    policy = MigrationPolicy(on_heuristic_mapping=PolicyAction.WARN, on_information_loss=PolicyAction.ERROR)
    with pytest.raises(MigrationError) as caught:
        inventory.migrate_background(context("uvek", "2025").background, policy=policy)
    assert "migration.technosphere_information_loss" in {i.code for i in caught.value.report.issues}
    assert inventory.data == data


@pytest.mark.parametrize("source", APPROVED_INCINERATION_COMPOSITION_PROXIES)
def test_composition_proxy_audit_retains_original_suppliers_and_exact_profile_evidence(source):
    resource = load_uvek_technosphere_resource()
    rule = next(r for r in resource["replace"] if tuple(r["source"][f] for f in FIELDS) == source)
    ledger = json.loads((DATA_DIR / "export/uvek_reference_flow_sign_review.json").read_text())
    decision = ledger["resources"][resource["name"]][rule["id"]]
    approval = APPROVED_INCINERATION_COMPOSITION_PROXIES[source]
    assert tuple(decision["original_target"][f] for f in FIELDS) == approval["previous_target"]
    assert decision["factor"] == -1 and decision["original_factor"] == 1
    assert decision["uvek_native_flow_type"] == "WASTE_FLOW"
    assert decision["source_observations"] == sorted(
        [
            [f"ecoinvent-{version}-{model}", -1]
            for version in approval["source_versions"]
            for model in ("cutoff", "consequential")
        ]
    )


@pytest.mark.parametrize("issue", ["lcd_ash", "sludge", "ash"])
def test_composition_approval_does_not_redirect_independent_residue_treatments(issue):
    resource = load_uvek_technosphere_resource()
    selected = select_profile_sign_rules(resource, TechnosphereProfile("ecoinvent", "3.12", "cutoff"))
    names = {
        "lcd_ash": "treatment of bottom ash, MSWI-WWT, WW from LCD backlight production, slag compartment",
        "sludge": "treatment of sewage sludge, 70% water, WWT, WW from medium density board production, municipal incineration",
        "ash": "treatment of bottom ash, MSWI, LDPE in waste fresh milk carton (chilled), post-industrial, slag compartment",
    }
    location = "GLO" if issue == "lcd_ash" else "RER"
    rule = next(
        r
        for r in selected["replace"] + selected["delete"]
        if r["source"]["name"] == names[issue]
        and r["source"]["location"] == location
        and r["source"]["unit"] == "kilogram"
    )
    source = tuple(rule["source"][f] for f in FIELDS)
    assert source not in APPROVED_INCINERATION_COMPOSITION_PROXIES
    data = consumer(tuple(rule["source"][f] for f in FIELDS), -10)
    inventory = BrightwayInventory.from_data(data, context=context("ecoinvent", "3.12"))
    if rule.get("reference_flow_sign_review", {}).get("status") == "unresolved":
        with pytest.raises(MigrationError) as caught:
            inventory.migrate_background(context("uvek", "2025").background, policy=MigrationPolicy.permissive())
        assert "migration.reference_flow_sign_unresolved" in {i.code for i in caught.value.report.issues}
    else:
        # Independent residue review may approve its own service or omission;
        # a polymer-incineration composition approval must never redirect ash.
        result = inventory.migrate_background(context("uvek", "2025").background, policy=MigrationPolicy.permissive())
        targets = {a["target"] for a in APPROVED_INCINERATION_COMPOSITION_PROXIES.values()}
        assert all(tuple(e.get(f) for f in FIELDS) not in targets for e in result.data[0]["exchanges"][1:])
        if rule.get("approved_omission"):
            assert len(result.data[0]["exchanges"]) == 1
            assert any(i.code == "migration.technosphere_omission" for i in result.last_migration_report.issues)
    assert inventory.data == data


def test_recovered_energy_references_are_not_redirected_to_mass_treatment_services():
    resource = load_uvek_technosphere_resource()
    rows = [
        r
        for r in resource["replace"]
        if r["source"]["name"] == PET_DUST_SOURCE[0] and r["source"]["unit"] != "kilogram"
    ]
    assert len(rows) == 2
    assert {(r["source"]["unit"], r["target"]["unit"]) for r in rows} == {
        ("kilowatt hour", "kilowatt hour"),
        ("megajoule", "megajoule"),
    }
    assert all("conversion_factor" not in r for r in rows)
