from __future__ import annotations

from copy import deepcopy

from .models import InventoryDocument


def normalize_inventory(document: InventoryDocument) -> InventoryDocument:
    """Return a normalized copy without modifying the source document."""

    data = document.data
    _promote_legacy_product_fields(data)
    _normalize_category_sequences(data)
    _synchronize_production_exchanges(data)
    return document.replace(data=data)


def _promote_legacy_product_fields(data: list[dict]) -> None:
    for activity in data:
        if not str(activity.get("reference product") or "").strip():
            legacy_product = str(activity.get("product") or "").strip()
            if legacy_product:
                activity["reference product"] = legacy_product

        for exchange in activity.get("exchanges", []):
            if exchange.get("type") not in {"production", "technosphere"}:
                continue
            if not str(exchange.get("reference product") or "").strip():
                legacy_product = str(exchange.get("product") or "").strip()
                if legacy_product:
                    exchange["reference product"] = legacy_product
            if "reference product" in exchange:
                exchange["product"] = exchange["reference product"]


def _normalize_category_sequences(data: list[dict]) -> None:
    for activity in data:
        for exchange in activity.get("exchanges", []):
            categories = exchange.get("categories")
            if isinstance(categories, str):
                exchange["categories"] = tuple(part.strip() for part in categories.split("::") if part.strip())
            elif isinstance(categories, list):
                exchange["categories"] = tuple(deepcopy(categories))


def _synchronize_production_exchanges(data: list[dict]) -> None:
    for activity in data:
        identity = {
            "name": activity.get("name"),
            "reference product": activity.get("reference product"),
            "location": activity.get("location"),
            "unit": activity.get("unit"),
        }
        for exchange in activity.get("exchanges", []):
            if exchange.get("type") != "production":
                continue
            for field, value in identity.items():
                if value not in (None, ""):
                    exchange[field] = value
            if identity["reference product"] not in (None, ""):
                exchange["product"] = identity["reference product"]
