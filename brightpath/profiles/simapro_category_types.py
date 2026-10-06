"""Conservative SimaPro category types independent of ecoinvent release numbers.

Waste status, the non-waste category type, and folder placement are distinct
questions. ISIC describes the producer; the reference product and CPC describe
what the process supplies. Unknown service/product roles require review.
"""

from collections.abc import Mapping
from dataclasses import dataclass

from brightpath.profiles.simapro_waste import WasteResolution
from brightpath.units import normalize_unit

_ENERGY_UNITS = frozenset(
    {"watt hour", "kilowatt hour", "megawatt hour", "joule", "kilojoule", "megajoule", "gigajoule"}
)
_TRANSPORT_UNITS = frozenset(
    {
        "ton kilometer",
        "ton-kilometer",
        "person kilometer",
        "person-kilometer",
        "vehicle kilometer",
        "vehicle-kilometer",
        "passenger-kilometer",
    }
)
_PHYSICAL_UNITS = frozenset(
    {"kilogram", "gram", "ton", "cubic meter", "normal cubic meter", "litre", "meter", "square meter", "unit"}
)


@dataclass(frozen=True)
class CategoryTypeResolution:
    category_type: str | None
    rule: str


def resolve_category_type(activity: Mapping, waste: WasteResolution) -> CategoryTypeResolution:
    """Infer a type from product evidence after waste status has been resolved."""
    if waste.waste is True:
        return CategoryTypeResolution("waste treatment", waste.rule)
    if waste.waste is None:
        return CategoryTypeResolution(None, "waste_status_unresolved")
    reference = str(activity.get("reference product") or "").casefold().strip()
    unit = normalize_unit(activity.get("unit", ""))
    cpc = waste.cpc[0] if len(waste.cpc) == 1 else ""
    isic = waste.isic[0] if len(waste.isic) == 1 else ""
    if unit in _ENERGY_UNITS and (
        cpc.startswith(("171", "173"))
        or reference.startswith(("electricity", "heat", "steam"))
        or "burned" in reference
    ):
        return CategoryTypeResolution("energy", "energy_product_and_unit")
    if unit in _TRANSPORT_UNITS and (cpc.startswith(("64", "65")) or reference.startswith("transport,")):
        return CategoryTypeResolution("transport", "transport_product_and_unit")
    if reference.startswith(("operation,", "operation of ", "use of ", "use,")) and cpc.startswith(
        ("5", "6", "7", "8", "9")
    ):
        return CategoryTypeResolution("use", "operation_service_product")
    if cpc.startswith(("5", "8", "9")):
        return CategoryTypeResolution("processing", "service_product_classification")
    if unit in _PHYSICAL_UNITS and cpc[:1] in {"0", "1", "2", "3", "4"}:
        return CategoryTypeResolution("material", "physical_goods_product")
    if not cpc and unit in _PHYSICAL_UNITS and isic[:2].isdigit() and 1 <= int(isic[:2]) <= 32:
        return CategoryTypeResolution("material", "physical_goods_producer")
    return CategoryTypeResolution(None, "non_waste_product_role_unresolved")
