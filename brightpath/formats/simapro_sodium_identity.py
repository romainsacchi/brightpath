"""Exact native aqueous-sodium labels; distinct generic flows stay refused."""

from brightpath.background.catalogs import CatalogNotFoundError, catalog_provider_from_environment
from brightpath.models import Issue
from brightpath.profiles.simapro_biosphere import resolve_ecoinvent_flow_name
from brightpath.units import normalize_unit

# SimaPro9.4 -> ecoinventEFv3.7 exact (=), kg, factor1 mappings. The
# SimaPro substance CAS17341-25-2/Na+ independently identifies ionic sodium.
# River/lake map to the surface-water UUID; other compartments stay distinct.
_IONIC_WATER_UUIDS = {
    ("water",): "5ac961f0-8e53-42ff-b13f-1a7fb9133868",
    ("water", "ground-"): "ee9fdd7c-5e29-4bdb-852a-3258f06ec2ab",
    ("water", "ground-, long-term"): "fb83d61e-6fe3-4da7-8eb2-b0e2dc85a76d",
    ("water", "ocean"): "9310f497-3205-4717-aaa8-1a94c321f7d8",
    ("water", "surface water"): "1fc409bc-b8e7-48b2-92d5-2ced4aa7bae2",
}
_CATALOG_NAMES = frozenset({"Sodium", "Sodium I", "Sodium, ion"})


def remember_sodium_wire_labels(data):
    """Save actual CSV labels before bw2io's generic name strategies run."""
    for dataset in data:
        for exchange in dataset.get("exchanges", []):
            if exchange.get("type") == "biosphere" and exchange.get("name") == "Sodium":
                exchange["simapro name"] = exchange["name"]


def _water_categories(exchange):
    categories = tuple(exchange.get("categories") or ())
    if categories in {("water", ""), ("water", "unspecified")}:
        return ("water",)
    return categories


class SodiumIdentityGuard:
    """Render/parse proven ionic UUIDs and refuse colliding generic identities.

    All lookups use the exact declared biosphere profile and injected provider.
    No quantity, formula or uncertainty field is interpreted or changed.
    """

    def __init__(self, profile, names, catalog_provider=None):
        self.profile = profile
        self.names = names
        self.provider = catalog_provider
        self.catalog = None

    def _eligible(self, exchange):
        return (
            self.profile.family == "ecoinvent"
            and exchange.get("type") == "biosphere"
            and _water_categories(exchange) in _IONIC_WATER_UUIDS
            and normalize_unit(exchange.get("unit", "")) == "kilogram"
        )

    def _catalog(self):
        if self.catalog is None:
            provider = self.provider if self.provider is not None else catalog_provider_from_environment()
            catalog = provider.load_biosphere(self.profile)
            if catalog.profile != self.profile:
                raise ValueError("Sodium catalogue does not match the exact declared biosphere profile.")
            self.catalog = catalog
        return self.catalog

    def _ionic_identity(self, categories):
        flow_id = _IONIC_WATER_UUIDS[categories]
        matches = [
            identity
            for identity, value in self._catalog().references.items()
            if value == flow_id and identity[1:] == (categories, "kilogram")
        ]
        if len(matches) != 1 or matches[0][0] not in _CATALOG_NAMES:
            raise ValueError(f"No unique verified ionic Sodium identity for {flow_id} in {self.profile.label()}.")
        return matches[0]

    def issue(self, exchange, path):
        """Return a structured preflight error without modifying an exchange."""
        if not self._eligible(exchange) or exchange.get("name") not in _CATALOG_NAMES:
            return None
        if (
            exchange.get("amount") == 0
            and not exchange.get("formula")
            and exchange.get("uncertainty type", 0) in (None, 0, 1)
        ):
            return None
        try:
            categories = _water_categories(exchange)
            identity = exchange["name"], categories, "kilogram"
            source_uuid = self._catalog().references.get(identity)
            ionic_uuid = _IONIC_WATER_UUIDS[categories]
            if exchange["name"] == "Sodium" and source_uuid != ionic_uuid:
                # An absent generic identity remains ordinary background validation's
                # responsibility. Never recreate generic A in newer catalogues.
                if source_uuid is None:
                    return None
                label = self._label(identity)
                if label != "Sodium":
                    return None
                return Issue(
                    severity="error",
                    code="simapro_biosphere_identity_ambiguous",
                    message=(
                        f"SimaPro label {label!r} cannot preserve the distinction between "
                        f"aqueous 'Sodium' ({source_uuid}) and ionic sodium ({ionic_uuid}) "
                        f"in biosphere {self.profile.version}. Exporting the generic flow would silently "
                        "change its identity on reimport. Use another export format or a verified distinct mapping."
                    ),
                    path=path,
                    suggested_fix="Use Brightway, openLCA JSON-LD or ILCD for this inventory.",
                )
            if source_uuid != ionic_uuid or self._ionic_identity(categories) != identity:
                raise ValueError("The declared Sodium identity does not match the verified ionic UUID/context/unit.")
        except (CatalogNotFoundError, ValueError) as error:
            return Issue(severity="error", code="simapro_biosphere_unresolved", message=str(error), path=path)
        return None

    def wire_name(self, exchange, fallback):
        """Use stock native Sodium only for the exact proven ionic identity."""
        if not self._eligible(exchange) or exchange.get("name") not in _CATALOG_NAMES:
            return fallback
        categories = _water_categories(exchange)
        identity = exchange["name"], categories, "kilogram"
        if self._catalog().references.get(identity) != _IONIC_WATER_UUIDS[categories]:
            return fallback
        if self._ionic_identity(categories) != identity:
            raise ValueError("Ambiguous native Sodium wire identity.")
        return "Sodium"

    def restore_wire_identity(self, exchange, raw_name, *, allowed_flows=None):
        """Resolve an actual native wire label to the same UUID in the selected catalogue.

        This must run before any unversioned alias or forward-label inverse.
        It never recovers source-original values from comments or metadata.
        """
        if raw_name != "Sodium" or self.profile.family != "ecoinvent":
            return False
        if not tuple(exchange.get("categories") or ()) or exchange["categories"][0] != "water":
            return False
        if not self._eligible(exchange):
            raise ValueError("No verified native Sodium identity for this water compartment/unit.")
        if self.provider is None:
            raise ValueError("An explicit UUID-bearing catalogue provider is required to decode native Sodium.")
        name, categories, unit = self._ionic_identity(_water_categories(exchange))
        key = name, categories[0], categories[1] if len(categories) > 1 else "unspecified"
        if allowed_flows is not None and key not in allowed_flows:
            raise ValueError("The verified ionic Sodium identity is excluded by the supplied biosphere flowset.")
        exchange.update(name=name, categories=categories, unit=unit)
        return True

    def _label(self, identity):
        name, categories, unit = identity
        return resolve_ecoinvent_flow_name(
            {"name": name, "categories": categories, "unit": unit}, self.names.get(name, name)
        )
