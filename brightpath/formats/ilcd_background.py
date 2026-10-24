"""Build used ILCD reference definitions from exact background identity catalogs."""

from __future__ import annotations

import hashlib
import json
from copy import deepcopy
from functools import lru_cache
from uuid import UUID

from brightpath import DATA_DIR
from brightpath.background.catalogs import CatalogNotFoundError, catalog_provider_from_environment
from brightpath.units import normalize_unit

from .ilcd_package import fail
from .ilcd_references import ILCDReferenceCatalog, identity
from .openlca_references import load_openlca_reference_catalog


def openlca_key(*parts: str) -> str:
    """Reproduce openLCA KeyGen's Java nameUUIDFromBytes convention."""
    raw = "/".join(part.strip() for part in parts).lower().encode("utf-8")
    return str(UUID(bytes=hashlib.md5(raw, usedforsecurity=False).digest(), version=3))


@lru_cache(maxsize=1)
def _quantities() -> dict:
    directory = DATA_DIR / "export" / "ilcd_quantities"
    manifest = json.loads((directory / "RESOURCE_MANIFEST.json").read_text())
    raw = (directory / "openlca_2_6_2.json").read_bytes()
    if manifest.get("size") != len(raw) or manifest.get("sha256") != hashlib.sha256(raw).hexdigest():
        fail("quantity_catalog_invalid", "ILCD quantity definitions do not match their integrity manifest.")
    payload = json.loads(raw)
    if payload.get("schema_version") != 1:
        fail("quantity_catalog_invalid", "Unsupported ILCD quantity reference schema.")
    return payload["units"]


def quantity_definition(unit: str, reference=None) -> dict:
    """Resolve a supported unit and, when supplied, verify exact target UUIDs."""
    quantities = _quantities()
    if reference is None:
        result = quantities.get(normalize_unit(unit))
    else:
        result = next(
            (
                row
                for row in quantities.values()
                if row["property_id"] == reference.flow_property_id and row["unit_id"] == reference.unit_id
            ),
            None,
        )
    if result is None:
        fail("unit_mapping_required", f"No verified ILCD background quantity definition for {unit!r}.")
    return deepcopy(result)


def automatic_references(document, supplied: ILCDReferenceCatalog, *, dialect: str, catalog_provider=None):
    """Fill missing references without replacing supplied or preserved ILCD evidence.

    Only used external exchanges are resolved. The exact catalog provider supplies
    ecoinvent UUID pairs; UVEK uses its separately verified openLCA reference build.
    No background inventories are copied or synthesized.
    """
    supplied.check_context(document.context.background)
    if dialect != "openlca":
        return supplied
    # Import rendering primitives lazily to keep provider lookup out of XML parsing.
    from .ilcd import _context_dict, _merge, _new_flow

    context = document.context
    background = _context_dict(context.background)
    data = document.data
    foreground = {identity(dataset, "technosphere") for dataset in data}
    rows = {kind: list(getattr(supplied, kind)) for kind in ("technosphere", "biosphere")}
    present = {kind: {identity(row["identity"], kind) for row in records} for kind, records in rows.items()}
    resources = dict(supplied.resources)
    provider = catalog_provider
    loaded = {}
    target = None
    target_loaded = False
    for dataset in data:
        for exchange in dataset.get("exchanges", []):
            kind = exchange.get("type")
            if kind not in rows:
                continue
            key = identity(exchange, kind)
            if key in present[kind] or (kind == "technosphere" and key in foreground):
                continue
            preserved = exchange.get("ilcd", {})
            if (
                dataset.get("ilcd", {}).get("background_context") == background
                and preserved.get("flow_id")
                and json.loads(json.dumps(preserved.get("identity", []))) == json.loads(json.dumps(key))
            ):
                continue
            if not target_loaded:
                target = load_openlca_reference_catalog(context)
                target_loaded = True
            target_row = getattr(target, kind).get(key) if target is not None else None
            if target is not None and target_row is None:
                # A known target build with partial coverage must not acquire lookalike flows.
                continue
            if target_row is not None:
                flow_id = target_row.flow_id
                process_id = target_row.process_id if kind == "technosphere" else ""
                quantity = quantity_definition(exchange["unit"], target_row)
            else:
                profile = getattr(context.background, kind)
                if profile.family != "ecoinvent":
                    continue
                if kind not in loaded:
                    provider = provider or catalog_provider_from_environment()
                    try:
                        loaded[kind] = getattr(provider, f"load_{kind}")(profile)
                    except CatalogNotFoundError as error:
                        fail("reference_catalog_unavailable", str(error))
                    if loaded[kind].profile != profile:
                        fail(
                            "reference_context_conflict",
                            "The catalog provider returned a different background profile.",
                        )
                pair = loaded[kind].references.get(key)
                if pair is None:
                    continue
                if kind == "technosphere":
                    activity_id, product_id = pair
                    process_id = openlca_key(activity_id, product_id)
                    # openLCA 2.6.2 keeps the ecoSpold intermediate-flow UUID.
                    flow_id = product_id
                else:
                    process_id, flow_id = "", pair
                quantity = quantity_definition(exchange["unit"])
            row = {
                "identity": {
                    k: deepcopy(exchange[k])
                    for k in ("name", "reference product", "location", "unit", "categories")
                    if k in exchange
                },
                "flow_id": flow_id,
                "flow_version": "01.00.000",
                "amount_factor": quantity["factor"],
                "reference_unit": normalize_unit(quantity["reference_unit"]),
            }
            if process_id:
                row["process_id"] = process_id
            name = exchange.get("reference product", exchange["name"]) if kind == "technosphere" else exchange["name"]
            _merge(
                resources,
                _new_flow(
                    flow_id,
                    name,
                    row["reference_unit"],
                    row["flow_version"],
                    categories=tuple(exchange.get("categories", ())) if kind == "biosphere" else (),
                    quantity=quantity,
                ),
            )
            rows[kind].append(row)
            present[kind].add(key)
    return ILCDReferenceCatalog(background, tuple(rows["technosphere"]), tuple(rows["biosphere"]), resources)
