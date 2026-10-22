"""Database labels shared by the Brightway syntax writers."""

from __future__ import annotations

from brightpath.exceptions import ExcelSerializationError
from brightpath.models import InventoryDocument


def labelled_brightway_data(document: InventoryDocument) -> list[dict]:
    """Label exported exchanges without changing the canonical inventory."""

    data = document.data
    foreground = document.database_name or "brightpath-inventory"
    profile = document.context.background.technosphere
    background = "-".join(part for part in (profile.family, profile.version, profile.system_model) if part)
    identities = {_identity(activity) for activity in data if isinstance(activity, dict)}
    for activity in data:
        if not isinstance(activity, dict):
            raise ExcelSerializationError("Every inventory dataset must be a dictionary.")
        exchanges = activity.get("exchanges", [])
        if not isinstance(exchanges, list):
            raise ExcelSerializationError(f"Activity {activity.get('name')!r} exchanges must be a list.")
        for exchange in exchanges:
            if not isinstance(exchange, dict):
                raise ExcelSerializationError(f"Activity {activity.get('name')!r} contains a non-dictionary exchange.")
            kind = exchange.get("type")
            if kind == "production" or (kind in {"technosphere", "substitution"} and _identity(exchange) in identities):
                exchange["database"] = foreground
            elif not exchange.get("database"):
                source_key = exchange.get("input")
                if isinstance(source_key, (list, tuple)) and len(source_key) == 2 and source_key[0]:
                    exchange["database"] = source_key[0]
                elif kind == "biosphere":
                    exchange["database"] = "biosphere"
                elif kind in {"technosphere", "substitution"} and background:
                    exchange["database"] = background
    return data


def _identity(value: dict) -> tuple[str, str, str, str]:
    return tuple(
        str(field or "")
        for field in (
            value.get("name"),
            value.get("reference product") or value.get("product"),
            value.get("location"),
            value.get("unit"),
        )
    )
