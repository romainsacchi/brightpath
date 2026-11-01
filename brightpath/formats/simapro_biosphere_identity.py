"""Exact native SimaPro literals for reviewed older ecoinvent identities.

The literals are witnessed in the licensed 3.9.1 / SimaPro9.5 CSV reference.
Their ecoinvent IDs are independently witnessed in native ElementaryExchanges
metadata. Platform and ecoinvent UUID namespaces are never equated.
"""

SUPPORTED_VERSIONS = frozenset({"3.6", "3.7", "3.8", "3.9.1"})
NATIVE_IDENTITIES = {
    ("Hydrochloric acid", ("air",), "kilogram"): "c9a8073a-8a19-5b9b-a120-7d549563b67b",
    ("Lithium", ("natural resource", "in ground"), "kilogram"): "7d2c1cdd-a64a-5936-a577-5b82db0c0d1b",
    ("Copper", ("natural resource", "in ground"), "kilogram"): "a9ac40a0-9bea-4c48-afa7-66aa6eb90624",
    (
        "Copper, Cu 5.2E-2%, Pt 4.8E-4%, Pd 2.0E-4%, Rh 2.4E-5%, Ni 3.7E-2% in ore",
        ("natural resource", "in ground"),
        "kilogram",
    ): "8508a83c-6a37-4159-93cc-21a2645390ab",
    ("Iron", ("natural resource", "in ground"), "kilogram"): "8ce3ff02-7a1e-48e3-881e-3248b944f28a",
}
UNVERIFIED_DISTINCT_IDENTITIES = {
    "c941d6d0-a56c-4e6c-95de-ac685635218d": (("air",), "kilogram"),
    "a9ad523f-b721-4f07-ad9f-584053f3454d": (("natural resource", "in ground"), "kilogram"),
    "f77aacc3-2c22-4bda-99ab-fe1110a1b891": (("natural resource", "in ground"), "kilogram"),
}


class NativeBiosphereNames:
    """One exact injected catalogue; full context and UUID must match uniquely."""

    def __init__(self, catalog):
        self.catalog = catalog
        self.active = catalog.profile.family == "ecoinvent" and catalog.profile.version in SUPPORTED_VERSIONS
        self.candidates = {}
        if self.active:
            for wire, flow_id in NATIVE_IDENTITIES.items():
                self.candidates[wire] = [
                    identity
                    for identity, value in catalog.references.items()
                    if value == flow_id and identity[1:] == wire[1:]
                ]

    def identity_from_wire(self, exchange, wire_name):
        """Resolve the literal, never an original name recovered from comments."""
        if not self.active or exchange.get("type", "biosphere") != "biosphere":
            return None
        key = wire_name, tuple(exchange.get("categories") or ()), exchange["unit"]
        if key not in self.candidates:
            return None
        matches = self.candidates[key]
        if len(matches) != 1:
            raise ValueError(f"Missing or ambiguous same-UUID native SimaPro identity for {key!r}.")
        return matches[0]

    def label_for_identity(self, exchange, default_label):
        """Render exact native vocabulary; do not collapse an unverified grade."""
        if not self.active or exchange.get("type", "biosphere") != "biosphere":
            return default_label
        identity = exchange["name"], tuple(exchange.get("categories") or ()), exchange["unit"]
        flow_id = self.catalog.references.get(identity)
        if UNVERIFIED_DISTINCT_IDENTITIES.get(flow_id) == identity[1:]:
            raise ValueError(f"No verified distinct native SimaPro label for {identity!r}.")
        for wire, native_id in NATIVE_IDENTITIES.items():
            if flow_id == native_id and identity[1:] == wire[1:]:
                target = self.identity_from_wire(exchange, wire[0])
                if target != identity:
                    raise ValueError("Native SimaPro identity differs from the source catalogue identity.")
                return wire[0]
        for label in (exchange["name"], default_label):
            key = label, identity[1], identity[2]
            if key in NATIVE_IDENTITIES and flow_id != NATIVE_IDENTITIES[key]:
                raise ValueError("Source catalogue UUID conflicts with the reviewed native SimaPro literal.")
        return default_label
