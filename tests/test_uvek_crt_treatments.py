"""CRT treatment proxies require exact waste identities and disclosed losses."""

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
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource
from scripts.review_uvek_reference_signs import (
    APPROVED_CRT_GLASS_TREATMENTS,
    APPROVED_CRT_INCINERATION_TREATMENTS,
    APPROVED_CRT_LANDFILL_TREATMENTS,
    APPROVED_CRT_MANUAL_TREATMENTS,
    APPROVED_CRT_MECHANICAL_TREATMENTS,
    APPROVED_CRT_RESIDUE_MARKETS,
    CRT_COATING_PRODUCT,
    CRT_COATING_TARGET,
    CRT_CULLET_LANDFILL_TARGET,
    CRT_FRIT_LANDFILL_TARGET,
    CRT_FRIT_PRODUCT,
    CRT_GLASS_TARGET,
    CRT_MANUAL_TARGET,
    CRT_MECHANICAL_SOURCE,
    CRT_MECHANICAL_TARGET,
    CRT_RESIDUE_TARGET,
    CRT_SCREEN_MARKET_SOURCE,
    CRT_SCREEN_MIX_TARGET,
    CRT_ZINC_PREVIOUS_TARGET,
    reviewed_reference_mass_factor,
)

FIELDS = ("name", "reference product", "location", "unit")
CRT_APPROVALS = {
    **APPROVED_CRT_INCINERATION_TREATMENTS,
    **APPROVED_CRT_GLASS_TREATMENTS,
    **APPROVED_CRT_MANUAL_TREATMENTS,
    **APPROVED_CRT_MECHANICAL_TREATMENTS,
    **APPROVED_CRT_RESIDUE_MARKETS,
    **APPROVED_CRT_LANDFILL_TREATMENTS,
}
PROFILES = [
    profile
    for profile in [
        (version, model)
        for version in ("3.6", "3.7", "3.8", "3.9", "3.10", "3.11", "3.12")
        for model in ("cutoff", "consequential")
    ]
    if profile[1] == "cutoff"
]


def context(family, version, model="cutoff"):
    return InventoryContext(
        FormatProfile("brightway_excel"),
        BackgroundContext(TechnosphereProfile(family, version, model), BiosphereProfile("ecoinvent", "3.10")),
    )


def consumer(sources, amount):
    output = {"name": "CRT proxy consumer", "reference product": "output", "location": "CH", "unit": "unit"}
    exchanges = [{**output, "type": "production", "amount": 1}]
    for source in sources:
        supplier = dict(zip(FIELDS, source, strict=True))
        exchanges.extend(
            [
                {
                    **supplier,
                    "type": "technosphere",
                    "amount": amount,
                    "formula": "waste_mass",
                    "uncertainty type": 2,
                    "loc": math.log(abs(amount)) if amount else 0,
                    "negative": amount < 0,
                    "scale": 0.2,
                },
                {
                    **supplier,
                    "type": "substitution",
                    "amount": amount,
                    "formula": "waste_mass",
                    "uncertainty type": 3,
                    "loc": amount,
                    "minimum": amount - 1,
                    "maximum": amount + 1,
                    "scale": 0.2,
                },
            ]
        )
    return [{**output, "exchanges": exchanges}]


@pytest.mark.parametrize("version,model", PROFILES)
@pytest.mark.parametrize("release", ["2025", "2026"])
@pytest.mark.parametrize("amount", [-10, 0, 10])
def test_all_inspected_crt_profiles_preserve_mass_uncertainty_and_report_losses(version, model, release, amount):
    sources = [source for source, approval in CRT_APPROVALS.items() if version in approval["source_versions"]]
    data = consumer(sources, amount)
    before = deepcopy(data)
    inventory = BrightwayInventory.from_data(data, context=context("ecoinvent", version, model))
    result = inventory.migrate_background(context("uvek", release).background, policy=MigrationPolicy.permissive())
    assert result.data[0]["exchanges"][0] == before[0]["exchanges"][0]
    assert len(result.data[0]["exchanges"]) == 1 + 2 * len(sources)
    for original, exchange in zip(before[0]["exchanges"][1:], result.data[0]["exchanges"][1:], strict=True):
        source = tuple(original[f] for f in FIELDS)
        factor = -1 / CRT_APPROVALS[source].get("target_reference_mass_kg", 1)
        if source in APPROVED_CRT_LANDFILL_TREATMENTS:
            target = CRT_FRIT_LANDFILL_TARGET if source[1] == CRT_FRIT_PRODUCT else CRT_CULLET_LANDFILL_TARGET
        elif source == CRT_SCREEN_MARKET_SOURCE:
            target = CRT_SCREEN_MIX_TARGET
        elif source == CRT_MECHANICAL_SOURCE:
            target = CRT_MECHANICAL_TARGET
        elif source in APPROVED_CRT_MANUAL_TREATMENTS:
            target = CRT_MANUAL_TARGET
        elif source[1] == CRT_COATING_PRODUCT:
            target = CRT_COATING_TARGET
        elif source in APPROVED_CRT_GLASS_TREATMENTS:
            target = CRT_GLASS_TARGET
        else:
            target = CRT_RESIDUE_TARGET
        assert tuple(exchange[f] for f in FIELDS) == target
        assert exchange["amount"] == pytest.approx(amount * factor) and exchange["type"] == original["type"]
        assert exchange["formula"] == f"(waste_mass) * ({float(factor)!r})"
        if original["uncertainty type"] == 2:
            assert exchange["loc"] == pytest.approx(original["loc"] + math.log(abs(factor)))
            assert exchange["scale"] == 0.2
            if amount:
                assert exchange["negative"] is (amount > 0)
        else:
            assert exchange["loc"] == pytest.approx(amount * factor)
            assert exchange["minimum"] == pytest.approx((amount + 1) * factor)
            assert exchange["maximum"] == pytest.approx((amount - 1) * factor)
            assert exchange["scale"] == pytest.approx(0.2 * abs(factor))
        if source == CRT_SCREEN_MARKET_SOURCE:
            assert exchange["unit"] == "unit"
            assert exchange["amount"] * 19.9 == pytest.approx(-amount)
    report = result.last_migration_report
    warnings = [i for i in report.issues if i.code == "migration.technosphere_information_loss"]
    assert len(warnings) == 2 * len(sources) and all(i.severity.value == "warning" for i in warnings)
    originals_by_path = {f"datasets[0].exchanges[{i}]": row for i, row in enumerate(before[0]["exchanges"]) if i}
    for warning in warnings:
        original = originals_by_path[warning.path]
        assert "including toxicity impacts" in warning.message and "metal speciation" in warning.message
        source = tuple(original[f] for f in FIELDS)
        if source in APPROVED_CRT_LANDFILL_TREATMENTS:
            assert "without composition or leaching-based scaling" in warning.message
        elif source == CRT_SCREEN_MARKET_SOURCE:
            assert "one unit per 19.9 kg" in warning.message and "23% manual / 77% mechanical" in warning.message
        elif source in APPROVED_CRT_MANUAL_TREATMENTS or source == CRT_MECHANICAL_SOURCE:
            assert "Preserve the original waste mass" in warning.message
        else:
            assert "without heating-value or moisture scaling" in warning.message
        if original["name"].startswith("market"):
            assert "Additional market-level freight is omitted" in warning.message
            if source in APPROVED_CRT_LANDFILL_TREATMENTS:
                function = (
                    "100% residual-material landfill of the same frit"
                    if source[1] == CRT_FRIT_PRODUCT
                    else "100% landfill of the same cullet"
                )
            elif source == CRT_SCREEN_MARKET_SOURCE:
                function = "23% manual / 77% mechanical"
            elif source in APPROVED_CRT_RESIDUE_MARKETS:
                function = "100% incineration of the same mixed shredder residues"
            elif source in APPROVED_CRT_GLASS_TREATMENTS:
                function = "100% CRT-glass processing"
            else:
                function = "100% incineration of the same coating"
            assert function in warning.message
        else:
            assert "Additional market-level freight is omitted" not in warning.message
        if source in APPROVED_CRT_LANDFILL_TREATMENTS:
            if source[1] == CRT_FRIT_PRODUCT:
                assert "separate from the inert-cullet model" in warning.message
            else:
                assert "excludes direct leachate emissions" in warning.message
                assert "not a reconstruction of this cullet's lead or other metal content" in warning.message
        elif original["reference product"] == CRT_COATING_PRODUCT:
            assert "inconsistent LCD-use wording" in warning.message
        elif source in APPROVED_CRT_MECHANICAL_TREATMENTS:
            assert "double-count" in warning.message and "manual dismantling" in warning.message
        elif source in APPROVED_CRT_MANUAL_TREATMENTS:
            assert "transfer coefficients" in warning.message
        elif tuple(original[f] for f in FIELDS) in APPROVED_CRT_GLASS_TREATMENTS:
            assert "100% allocation to disposal" in warning.message
        elif source in APPROVED_CRT_RESIDUE_MARKETS:
            assert "mixed shredder residues" in warning.message
        else:
            assert "Mixed shredder residues" in warning.message
    assert len([loss for loss in report.losses if loss.code == "migration.technosphere_information_loss"]) == len(
        warnings
    )
    assert inventory.data == data == before


def test_crt_audit_retains_all_historical_suppliers_and_exact_profile_pairs():
    resource = load_uvek_technosphere_resource()
    ledger = json.loads((DATA_DIR / "export/uvek_reference_flow_sign_review.json").read_text())
    assert len(APPROVED_CRT_INCINERATION_TREATMENTS) == 12
    assert len(APPROVED_CRT_GLASS_TREATMENTS) == 2
    assert len(APPROVED_CRT_MANUAL_TREATMENTS) == 2
    assert len(APPROVED_CRT_MECHANICAL_TREATMENTS) == 2
    assert len(APPROVED_CRT_RESIDUE_MARKETS) == 2
    assert len(APPROVED_CRT_LANDFILL_TREATMENTS) == 10
    assert sum(a["approval_scope"] == "explicit" for a in APPROVED_CRT_LANDFILL_TREATMENTS.values()) == 2
    assert sum(a["approval_scope"] == "explicit" for a in APPROVED_CRT_INCINERATION_TREATMENTS.values()) == 2
    observations = []
    for source, approval in CRT_APPROVALS.items():
        rule = next(r for r in resource["replace"] if tuple(r["source"][f] for f in FIELDS) == source)
        decision = ledger["resources"][resource["name"]][rule["id"]]
        assert tuple(rule["target"][f] for f in FIELDS) == approval["target"]
        assert tuple(decision["original_target"][f] for f in FIELDS) == approval["previous_target"]
        assert decision["factor"] == pytest.approx(-1 / approval.get("target_reference_mass_kg", 1))
        assert decision["original_factor"] == 1
        assert decision["uvek_native_flow_type"] == "WASTE_FLOW"
        expected = sorted(
            [[f"ecoinvent-{v}-{m}", -1] for v in approval["source_versions"] for m in ("cutoff", "consequential")]
        )
        assert decision["source_observations"] == expected
        observations.extend(expected)
    assert len(observations) == 324
    zinc = [
        a for a in APPROVED_CRT_INCINERATION_TREATMENTS.values() if a["previous_target"] == CRT_ZINC_PREVIOUS_TARGET
    ]
    assert len(zinc) == 1 and zinc[0]["target"] == CRT_RESIDUE_TARGET


@pytest.mark.parametrize("mass", [0, -19.9, True, float("inf"), float("nan"), "19.9"])
def test_reviewed_screen_reference_mass_rejects_invalid_quantity_bases(mass):
    with pytest.raises(ValueError, match="reference-mass conversion changed"):
        reviewed_reference_mass_factor(CRT_SCREEN_MARKET_SOURCE, CRT_SCREEN_MIX_TARGET, 1, mass)


@pytest.mark.parametrize("field", ["source_unit", "target_unit", "original_factor"])
def test_reviewed_screen_reference_mass_rejects_changed_units_or_factor(field):
    source, target, original = CRT_SCREEN_MARKET_SOURCE, CRT_SCREEN_MIX_TARGET, 1
    if field == "source_unit":
        source = (*source[:3], "liter")
    elif field == "target_unit":
        target = (*target[:3], "kilogram")
    else:
        original = 2
    with pytest.raises(ValueError, match="reference-mass conversion changed"):
        reviewed_reference_mass_factor(source, target, original, 19.9)


def test_crt_approximations_require_loss_acceptance_and_unrelated_pending_review_rolls_back(monkeypatch):
    sources = [s for s, a in CRT_APPROVALS.items() if "3.12" in a["source_versions"]]
    data = consumer(sources, -10)
    inventory = BrightwayInventory.from_data(data, context=context("ecoinvent", "3.12"))
    policy = MigrationPolicy(on_heuristic_mapping=PolicyAction.WARN, on_information_loss=PolicyAction.ERROR)
    with pytest.raises(MigrationError) as caught:
        inventory.migrate_background(context("uvek", "2025").background, policy=policy)
    assert "migration.technosphere_information_loss" in {i.code for i in caught.value.report.issues}
    assert inventory.data == data
    resource = load_uvek_technosphere_resource()
    glass = next(
        r["source"]
        for r in resource["replace"]
        if r["source"]["name"]
        == "treatment of glass cullet, lead containing, from used cathode ray tube, 0% water, inert material landfill"
        and r["source"]["unit"] == "kilogram"
    )
    data[0]["exchanges"].append({**glass, "amount": -1, "type": "technosphere"})
    # The pending-review fixture remains stable when real supplier approvals change.
    resource = deepcopy(resource)
    pending = next(r for r in resource["replace"] if r["source"] == glass)
    pending["reference_flow_sign_review"] = {
        "status": "unresolved",
        "reason": "Synthetic pending supplier review for transactional rollback.",
        "evidence": ["Synthetic test evidence; the real approved correspondence is unchanged."],
    }
    monkeypatch.setattr("brightpath.background.migration.load_uvek_technosphere_resource", lambda: resource)
    monkeypatch.setattr("brightpath.background.execution.load_uvek_technosphere_resource", lambda: resource)
    before = deepcopy(data)
    inventory = BrightwayInventory.from_data(data, context=context("ecoinvent", "3.12"))
    with pytest.raises(MigrationError) as caught:
        inventory.migrate_background(context("uvek", "2025").background, policy=MigrationPolicy.permissive())
    assert "migration.reference_flow_sign_unresolved" in {i.code for i in caught.value.report.issues}
    assert inventory.data == data == before and caught.value.report.metadata["committed"] is False


@pytest.mark.parametrize("source", [s for s in CRT_APPROVALS if s[2] == "GLO" and not s[0].startswith("market")])
def test_matching_foreground_crt_suppliers_are_preserved(source):
    data = consumer([source], -10)
    supplier = dict(zip(FIELDS, source, strict=True))
    data.append({**supplier, "exchanges": [{**supplier, "type": "production", "amount": 1}]})
    before = deepcopy(data)
    result = BrightwayInventory.from_data(data, context=context("ecoinvent", "3.12")).migrate_background(
        context("uvek", "2025").background, policy=MigrationPolicy.permissive()
    )
    assert result.data == data == before
    assert not any(i.code == "migration.technosphere_information_loss" for i in result.last_migration_report.issues)
