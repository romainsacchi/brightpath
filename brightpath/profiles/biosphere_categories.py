"""Compartment vocabulary at software boundaries, independent of flow migration."""

from collections import defaultdict
from functools import lru_cache

UVEK_ROOTS = frozenset(
    {
        "economic issues",
        "emissions to air",
        "emissions to soil",
        "emissions to water",
        "non material emissions",
        "resources",
    }
)
CANONICAL_ROOTS = frozenset({"natural resource", "air", "water", "soil", "inventory indicator"})
STRUCTURAL_BIOSPHERE_ROOTS = CANONICAL_ROOTS | UVEK_ROOTS
_WIRE_ROOTS = {
    "emissions to air": "air",
    "emissions to soil": "soil",
    "emissions to water": "water",
    "resources": "natural resource",
}


def wire_categories(categories):
    """Return format compartment labels without changing the source identity."""
    values = tuple(categories or ())
    return (_WIRE_ROOTS.get(values[0], values[0]), *values[1:]) if values else ()


def is_native_uvek_biosphere(profile):
    return profile is not None and (profile.family, profile.version) == ("uvek", "2026")


def _wire_key(categories):
    values = wire_categories(categories)
    return values[:1] if len(values) == 2 and values[1] == "unspecified" else values


@lru_cache(maxsize=1)
def _native_identities():
    from brightpath.background import PackageCatalogProvider
    from brightpath.core.context import BiosphereProfile

    return PackageCatalogProvider().load_biosphere(BiosphereProfile("uvek", "2026")).identities


def native_wire_identities(native_identities):
    """Return native flow identities indexed by reversible format labels."""
    identities = defaultdict(set)
    for name, categories, unit in native_identities:
        identities[(name, _wire_key(categories), unit)].add(categories)
    return identities


def restore_native_categories(data, profile, *, identities=None):
    """Restore only unique exact catalog identities after a format parse, in place."""
    if not is_native_uvek_biosphere(profile):
        return
    native_identities = _native_identities() if identities is None else frozenset(identities)
    wire_identities = native_wire_identities(native_identities)
    for dataset in data:
        for exchange in dataset.get("exchanges", []):
            if exchange.get("type") != "biosphere":
                continue
            categories = tuple(exchange.get("categories") or ())
            exact = exchange.get("name"), categories, exchange.get("unit")
            if exact in native_identities:
                continue
            key = exchange.get("name"), _wire_key(categories), exchange.get("unit")
            matches = wire_identities.get(key, ())
            if len(matches) > 1:
                raise ValueError(f"Ambiguous native UVEK biosphere compartment for {key!r}.")
            if matches:
                exchange["categories"] = next(iter(matches))
