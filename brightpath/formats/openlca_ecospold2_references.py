"""Used native openLCA 2.6 EcoSpold2 references from exact paired catalogs.

Only reference metadata is built. External process IDs follow native KeyGen;
product and elementary-flow IDs retain their EcoSpold UUIDs. No inventories,
amount conversions, synthetic geographic IDs or catalog-wide exports are made.
"""

from __future__ import annotations

from brightpath.background.catalogs import CatalogNotFoundError, catalog_provider_from_environment
from brightpath.exceptions import SerializationError

from .openlca_references import (
    OpenLCABiosphereReference,
    OpenLCAReferenceCatalog,
    OpenLCATechnosphereReference,
)

_SOURCE = "openLCA 2.6 native EcoSpold2 IDs; used exact catalog references only"


def _paired(context):
    if context is None:
        return False
    tech, bio = context.background.technosphere, context.background.biosphere
    return tech.family == bio.family == "ecoinvent" and tech.version == bio.version


def _tech_identity(row):
    return (
        str(row.get("name") or ""),
        str(row.get("reference product") or row.get("product") or ""),
        str(row.get("location") or ""),
        str(row.get("unit") or ""),
    )


def _bio_identity(row):
    return str(row.get("name") or ""), tuple(row.get("categories") or ()), str(row.get("unit") or "")


def _load(provider, kind, profile):
    catalog = getattr(provider, f"load_{kind}")(profile)
    if catalog.profile != profile:
        raise ValueError("External openLCA catalog profile conflicts with the exact declared context.")
    return catalog


def _quantity(unit, profile):
    # Shared integrity-checked native metadata; avoid loading ILCD until needed.
    from .ilcd_background import quantity_definition

    label = f"{profile.family} {profile.version} {profile.system_model}"
    try:
        quantity = quantity_definition(unit, profile=profile)
    except (ValueError, OSError) as error:
        raise SerializationError(f"No verified native openLCA unit {unit!r} for {label}.") from error
    selected = [row for row in quantity["units"] if row["id"] == quantity["unit_id"]]
    if len(selected) != 1 or not isinstance(selected[0].get("name"), str) or not selected[0]["name"]:
        raise SerializationError(f"No exact native openLCA unit definition for {unit!r} in {label}.")
    return quantity, selected[0]["name"]


def _process_id(activity, product):
    from .ilcd_background import openlca_key

    return openlca_key(activity, product)


def _tech_reference(identity, pair, profile, *, process_id=None):
    quantity, unit_name = _quantity(identity[3], profile)
    return OpenLCATechnosphereReference(
        process_id=process_id or _process_id(*pair),
        process_name=identity[0],
        flow_id=pair[1],
        flow_name=identity[1],
        flow_property_id=quantity["property_id"],
        flow_property_name=quantity["property_name"],
        unit_id=quantity["unit_id"],
        unit_name=unit_name,
        location_id="",
        location=identity[2],
        category="",
    )


def _bio_reference(identity, flow_id, profile):
    quantity, unit_name = _quantity(identity[2], profile)
    return OpenLCABiosphereReference(
        flow_id=flow_id,
        flow_name=identity[0],
        flow_property_id=quantity["property_id"],
        flow_property_name=quantity["property_name"],
        unit_id=quantity["unit_id"],
        unit_name=unit_name,
    )


def references_for_document(document, *, catalog_provider=None, missing_foreground=()):
    """Build references for used exact external identities in a paired EI context.

    Local foreground suppliers and explicitly missing foreground identities are
    excluded. Native exchange unit UUIDs carry conversion factors: source amounts,
    uncertainty parameters and formulas are never rescaled by this helper.
    Other background contexts return ``None`` for their existing dispatch paths.
    """
    if not _paired(document.context):
        return None
    data = document.data
    foreground = {_tech_identity(row) for row in data}
    missing = {tuple(row) for row in missing_foreground}
    if any(len(row) != 4 for row in missing):
        raise ValueError("Missing foreground identities must contain name, product, location and unit.")
    used = {"technosphere": set(), "biosphere": set()}
    for dataset in data:
        for exchange in dataset.get("exchanges", []):
            kind = exchange.get("type")
            if kind == "technosphere":
                key = _tech_identity(exchange)
                if key not in foreground and key not in missing:
                    used[kind].add(key)
            elif kind == "biosphere":
                used[kind].add(_bio_identity(exchange))
    references = {"technosphere": {}, "biosphere": {}}
    provider = catalog_provider
    tech = document.context.background.technosphere
    for kind, identities in used.items():
        if not identities:
            continue
        provider = provider or catalog_provider_from_environment()
        try:
            catalog = _load(provider, kind, getattr(document.context.background, kind))
        except CatalogNotFoundError as error:
            raise SerializationError(f"Native openLCA reference catalog unavailable: {error}") from error
        for key in sorted(identities):
            value = catalog.references.get(key)
            if key not in catalog.identities or value is None:
                raise SerializationError(f"No exact native openLCA {kind} reference for {key!r}.")
            references[kind][key] = (
                _tech_reference(key, value, tech) if kind == "technosphere" else _bio_reference(key, value, tech)
            )
    return OpenLCAReferenceCatalog(**references, source=_SOURCE)


def references_for_package(raw_entities, context, *, catalog_provider=None):
    """Resolve only absent external definitions by native IDs and quantity IDs.

    Self-contained legacy packages and other contexts retain their existing
    reader paths. Unknown or mutated external references in a supported catalog
    are rejected; labels never substitute for a native identifier.
    """
    if not _paired(context):
        return None
    defined = {row.get("@id") for row in raw_entities.get("flows", [])}
    wanted = []
    for process in raw_entities.get("processes", []):
        for exchange in process.get("exchanges", []):
            flow = (exchange.get("flow") or {}).get("@id")
            if flow in defined:
                continue
            wanted.append(
                (
                    flow,
                    (exchange.get("defaultProvider") or {}).get("@id"),
                    (exchange.get("unit") or {}).get("@id"),
                    (exchange.get("flowProperty") or {}).get("@id"),
                )
            )
    if not wanted:
        return None
    provider = catalog_provider or catalog_provider_from_environment()
    tech = context.background.technosphere
    wanted_products = {flow for flow, process, _unit, _quantity in wanted if process}
    wanted_pairs = {(flow, process) for flow, process, _unit, _quantity in wanted if process}
    wanted_biosphere = {flow for flow, process, _unit, _quantity in wanted if not process}
    candidates = {}
    try:
        if wanted_products:
            catalog = _load(provider, "technosphere", tech)
            for identity, pair in catalog.references.items():
                # Important for large catalogs: filter product IDs before MD5.
                if pair[1] not in wanted_products:
                    continue
                process_id = _process_id(*pair)
                if (pair[1], process_id) not in wanted_pairs:
                    continue
                reference = _tech_reference(identity, pair, tech, process_id=process_id)
                key = (pair[1], process_id, reference.unit_id, reference.flow_property_id)
                candidates.setdefault(key, []).append(("technosphere", identity, reference))
        if wanted_biosphere:
            catalog = _load(provider, "biosphere", context.background.biosphere)
            for identity, flow in catalog.references.items():
                if flow not in wanted_biosphere:
                    continue
                reference = _bio_reference(identity, flow, tech)
                key = (flow, None, reference.unit_id, reference.flow_property_id)
                candidates.setdefault(key, []).append(("biosphere", identity, reference))
    except CatalogNotFoundError:
        # Unknown contexts have no native reference contract to dispatch here.
        return None
    references = {"technosphere": {}, "biosphere": {}}
    for key in wanted:
        matches = candidates.get(key, [])
        if len(matches) != 1:
            raise ValueError(
                "External openLCA reference has missing flow or unknown/ambiguous native "
                f"provider, flow or unit identifiers: {key!r}."
            )
        kind, identity, reference = matches[0]
        references[kind][identity] = reference
    return OpenLCAReferenceCatalog(**references, source=_SOURCE)
