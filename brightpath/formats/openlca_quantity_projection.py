"""One reviewed water accounting projection; never a generic density conversion."""

from __future__ import annotations

import hashlib
import json
from copy import deepcopy
from functools import lru_cache

from brightpath import DATA_DIR
from brightpath.migrations.scaling import scale_exchange

IDENTITY = ("Water", ("water", "fossil well"), "cubic meter")
SOURCE_UUID = "2256a142-8242-4b4f-b9aa-a167803989ca"
NATIVE_UUID = "7c98e916-5edf-3cc3-984d-3278fa1015cf"
PROPERTY_UUID = "93a60a56-a3c8-11da-a746-0800200b9a66"
UNIT_UUID = "20aadc24-a391-41cf-b340-3e4529f44bde"
PROJECTION_ID = "uvek2025-fossilwater-volume-to-native-mass-v1"
FACTOR = 1000.0
PROFILE = {
    "technosphere": {"family": "uvek", "version": "2025", "system_model": "cutoff"},
    "biosphere": {"family": "ecoinvent", "version": "3.10"},
}
PENDING_FACTOR = "_brightpath_native_quantity_factor"


@lru_cache(maxsize=1)
def reviewed_mapping():
    directory = DATA_DIR / "export/openlca_references"
    path = directory / "native_quantity_projections.json"
    raw = path.read_bytes()
    manifest = json.loads((directory / "RESOURCE_MANIFEST.json").read_bytes())
    entry = manifest.get("native_quantity_projections", {})
    if (
        entry.get("file") != path.name
        or entry.get("size") != len(raw)
        or entry.get("sha256") != hashlib.sha256(raw).hexdigest()
        or entry.get("rows") != 1
    ):
        raise ValueError("Native quantity projection resource integrity/count mismatch.")
    payload = json.loads(raw)
    if payload.get("schema_version") != 1 or payload.get("profile") != PROFILE or entry.get("profile") != PROFILE:
        raise ValueError("Native quantity projection profile/schema mismatch.")
    if len(payload.get("mappings", [])) != 1:
        raise ValueError("Only the one reviewed native quantity projection is supported.")
    mapping = payload["mappings"][0]
    source, native = mapping.get("source", {}), mapping.get("native", {})
    expected_source = {
        "flow_id": SOURCE_UUID,
        "name": IDENTITY[0],
        "categories": list(IDENTITY[1]),
        "unit": IDENTITY[2],
    }
    expected_native = {
        "flow_ref_id": NATIVE_UUID,
        "flow_name": "Water",
        "category_path": ["emissions to water", "fossilwater"],
        "flow_type": "ELEMENTARY_FLOW",
        "flow_property_ref_id": PROPERTY_UUID,
        "flow_property_name": "Mass",
        "unit_ref_id": UNIT_UUID,
        "unit": "kg",
        "unit_group_ref_id": "93a60a57-a4c8-11da-a746-0800200c9a66",
        "reference_unit_factor": 1.0,
    }
    if (
        source != expected_source
        or native != expected_native
        or mapping.get("amount_factor") != FACTOR
        or isinstance(mapping.get("amount_factor"), bool)
        or mapping.get("id") != PROJECTION_ID
        or payload.get("native_source")
        != {
            "database_build": "UVEK 2025 Version 2 openLCA, 2026-03-09",
            "archive_sha256": "28294c2201a4ac9a336ca52a31cbd02fa028c49d04ecb8455a87965d3d7aa89d",
        }
    ):
        raise ValueError("Native quantity projection is outside the reviewed exact identity/unit/build scope.")
    return payload


def catalog_projection_factor(row, profile):
    """Validate every non-unit factor against the exact resource, never a fuzzy name."""
    factor = row.get("amount_factor", 1.0)
    if isinstance(factor, bool):
        raise ValueError("Native quantity factor must be numeric and explicit.")
    if factor == 1 and not row.get("quantity_projection_id"):
        return 1.0
    bundle = reviewed_mapping()
    mapping = bundle["mappings"][0]
    expected = {
        "name": "Water",
        "categories": list(IDENTITY[1]),
        "unit": "cubic meter",
        "source_flow_id": SOURCE_UUID,
        "flow_id": NATIVE_UUID,
        "flow_name": "Water",
        "flow_property_id": PROPERTY_UUID,
        "flow_property_name": "Mass",
        "unit_id": UNIT_UUID,
        "unit_name": "kg",
        "amount_factor": FACTOR,
        "native_categories": ["emissions to water", "fossilwater"],
        "quantity_projection_id": PROJECTION_ID,
    }
    if profile != bundle["profile"] or any(row.get(k) != value for k, value in expected.items()):
        raise ValueError("Catalog quantity projection conflicts with its reviewed exact correspondence.")
    return mapping["amount_factor"]


def scale_detached(exchange, factor, *, inverse=False):
    """Reuse the common signed marginal/formula transform on a detached exchange."""
    result = deepcopy(exchange)
    aliases = {}
    for alias, canonical in (("min", "minimum"), ("max", "maximum")):
        if alias in result:
            if canonical in result and result[alias] != result[canonical]:
                raise ValueError("Conflicting bound aliases cannot be quantity-projected.")
            aliases[alias] = canonical in result
            result[canonical] = result.pop(alias)
    original_formula = result.get("formula")
    scale_exchange(result, 1 / factor if inverse else factor)
    if inverse and original_formula:
        suffix = f") * ({float(factor)!r})"
        if original_formula.startswith("(") and original_formula.endswith(suffix):
            # Undo only the exact wrapper produced by scale_exchange. Never
            # replace a changed native formula with a preserved source value.
            result["formula"] = original_formula[1 : -len(suffix)]
    for alias, had_canonical in aliases.items():
        canonical = "minimum" if alias == "min" else "maximum"
        result[alias] = result[canonical]
        if not had_canonical:
            result.pop(canonical)
    return result


def restore_package_quantities(package, context, *, decode_native_uncertainty=True):
    """Decode native graphs on the actual kg wire basis, then invert the factor."""
    if not any(PENDING_FACTOR in e for d in package.data for e in d.get("exchanges", [])):
        return package
    from brightpath.models import InventoryDocument

    from .openlca_native_uncertainty import decode

    document = InventoryDocument(
        data=package.data,
        context=context,
        metadata=package.metadata,
        database_name=package.database_name,
        database_parameters=package.database_parameters,
        project_parameters=package.project_parameters,
    )
    if decode_native_uncertainty:
        document = decode(document)
    elif any(
        PENDING_FACTOR in e and "bp_olca262_" in str(e.get("formula", ""))
        for d in document.data
        for e in d.get("exchanges", [])
    ):
        raise ValueError(
            "Canonical projected quantities require native uncertainty graph decoding; inspect raw ZIP for literal kg graph."
        )
    data = document.data
    for dataset in data:
        for index, exchange in enumerate(dataset.get("exchanges", [])):
            factor = exchange.pop(PENDING_FACTOR, 1.0)
            if factor != 1:
                dataset["exchanges"][index] = scale_detached(exchange, factor, inverse=True)
    from dataclasses import replace

    return replace(
        package,
        data=data,
        database_parameters=document.database_parameters,
        project_parameters=document.project_parameters,
    )
