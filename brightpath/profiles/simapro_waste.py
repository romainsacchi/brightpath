"""Conservative, auditable SimaPro waste inference from inventory classifications.

ISIC describes activities; CPC describes products. Neither is a SimaPro category.
These rules deliberately leave conflicting or incomplete evidence unresolved.
"""

from __future__ import annotations

import math
import re
from collections.abc import Mapping
from dataclasses import dataclass
from numbers import Real

from brightpath.profiles.simapro_categories import split_simapro_category
from brightpath.units import normalize_unit


@dataclass(frozen=True)
class WasteResolution:
    """Waste status and the rule/evidence used; ``None`` requires review."""

    waste: bool | None
    rule: str
    isic: tuple[str, ...] = ()
    cpc: tuple[str, ...] = ()


_ENERGY_UNITS = frozenset(
    {"watt hour", "kilowatt hour", "megawatt hour", "joule", "kilojoule", "megajoule", "gigajoule"}
)
_MASS_UNITS = frozenset({"gram", "kilogram", "ton"})
_VOLUME_UNITS = frozenset({"cubic meter", "normal cubic meter", "standard cubic meter", "litre"})


def _recovered_product_evidence(reference: str, unit: str, cpc: str) -> str | None:
    """Return a reviewed product role only when name, CPC and dimension agree.

    These rules identify products of allocated treatment datasets, not the
    treatment activity's sector. Unlisted recovered materials remain unresolved.
    """
    if cpc.startswith("17100") and (reference == "electricity" or reference.startswith("electricity, ")):
        return "energy" if unit in _ENERGY_UNITS else None
    if cpc.startswith("17300") and (reference == "heat" or reference.startswith("heat, ")):
        return "energy" if unit in _ENERGY_UNITS else None
    if unit in _MASS_UNITS:
        if cpc.startswith("3465") and re.fullmatch(
            r"(?:organic )?(?:nitrogen|phosphorus|potassium) fertili[sz]er(?:, as .+)?", reference
        ):
            return "fertiliser"
        if cpc.startswith("391") and reference == "compost":
            return "compost"
        if cpc.startswith("34654") and reference == "poultry manure, dried":
            return "dried_manure"
    if cpc.startswith("17200") and reference == "biogas" and unit in _VOLUME_UNITS:
        return "biogas"
    return None


def resolve_classified_waste(activity: Mapping) -> WasteResolution:
    """Resolve one activity without modifying it or consulting a name catalog.

    Explicit production SimaPro categories take precedence. Only ISIC revision 4
    (including ecoinvent extensions) and CPC are interpreted. A negative reference
    quantity supports waste inference, but never determines it on its own.
    """
    productions = [e for e in activity.get("exchanges", ()) if e.get("type") == "production"]
    if len(productions) != 1:
        return WasteResolution(None, "reference_exchange_unresolved")
    production = productions[0]
    if production.get("simapro category"):
        kind, _ = split_simapro_category(production["simapro category"])
        return WasteResolution(kind == "waste treatment", "explicit_category")

    codes = {"isic": set(), "cpc": set()}
    for entry in activity.get("classifications") or ():
        if not isinstance(entry, (tuple, list)) or len(entry) != 2:
            continue
        scheme, value = entry
        target = {"ISIC rev.4 ecoinvent": "isic", "ISIC rev.4": "isic", "CPC": "cpc"}.get(scheme)
        match = re.match(r"^\s*(\d+)(?=\D|$)", str(value))
        if target and match:
            codes[target].add(match.group(1))
    isic, cpc = tuple(sorted(codes["isic"])), tuple(sorted(codes["cpc"]))

    def result(waste, rule):
        return WasteResolution(waste, rule, isic, cpc)

    if len(isic) > 1 or len(cpc) > 1:
        return result(None, "multiple_classifications")
    sector = isic[0] if isic else ""
    product = cpc[0] if cpc else ""
    if sector and len(sector) < 4:
        return result(None, "incomplete_isic")
    amount = production.get("amount")
    if not isinstance(amount, Real) or not math.isfinite(amount) or amount == 0 or production.get("formula"):
        return result(None, "reference_quantity_unresolved")
    reference = str(activity.get("reference product") or "").strip().lower()
    exchange_reference = str(production.get("reference product") or production.get("product") or "").strip().lower()
    unit = normalize_unit(str(activity.get("unit") or "").strip().lower())
    exchange_unit = normalize_unit(str(production.get("unit") or "").strip().lower())
    if (
        not reference
        or not unit
        or (exchange_reference and exchange_reference != reference)
        or (exchange_unit and exchange_unit != unit)
    ):
        return result(None, "reference_identity_unresolved")
    name = str(activity.get("name", "")).lower().strip()
    # Reviewed Premise inventory: positive reference represents effluent
    # treatment, not a recovered product. Keep this exception identity-specific.
    if (
        name == "treatment of effluent from nitrogen trifluoride production, wastewater treatment, class 3"
        and reference == "effluent from nitrogen trifluoride production"
        and unit == "cubic meter"
        and str(activity.get("location", "")).strip() == "CH"
        and sector == "3700"
        and product == "39990"
    ):
        return result(True, "reviewed_nf3_effluent_treatment")
    market = name.startswith(("market for ", "market group for "))
    treatment = name.startswith("treatment of ")
    waste_sector = sector.startswith(("382", "383")) or sector == "3700"
    service = product.startswith(("941", "943"))
    recovered = _recovered_product_evidence(reference, unit, product)
    if product.startswith(("17100", "17300")) and recovered != "energy":
        return result(None, "energy_classification_conflicts_with_reference")
    if recovered:
        if amount < 0:
            return result(None, "negative_recovered_product")
        if waste_sector:
            return result(False, "recovered_" + recovered + "_product")
    if service and sector and not waste_sector:
        return result(None, "conflicting_service_and_sector")
    if waste_sector or service:
        if amount < 0:
            return result(True, "waste_sector_negative_reference")
        if market:
            return result(None, "positive_waste_market")
        if sector.startswith("383") and not service:
            return result(None, "positive_material_recovery")
        # A positive quantity and a treatment name do not identify which
        # allocated product is supplied. Require explicit review, even for
        # CPC service codes, rather than infer signs from the activity name.
        return result(None, "positive_treatment_reference_unresolved")
    if amount < 0:
        return result(None, "negative_reference_outside_waste_sector")
    if sector.startswith(("381", "390")) or treatment:
        return result(None, "other_waste_activity_role")
    if sector:
        return result(False, "non_waste_sector")
    return result(None, "missing_activity_classification")
