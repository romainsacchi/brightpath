"""Prevent a known many-to-one aqueous-sodium SimaPro representation."""

from brightpath.background.catalogs import CatalogNotFoundError, catalog_provider_from_environment
from brightpath.models import Issue
from brightpath.profiles.simapro_biosphere import resolve_ecoinvent_flow_name
from brightpath.units import normalize_unit

_GENERIC_SODIUM = "723f56f9-c68e-5692-a969-0fb2a70bce20"
_IONIC_SODIUM = "5ac961f0-8e53-42ff-b13f-1a7fb9133868"


class SodiumIdentityGuard:
    """Check the exact catalogue and rendered labels without changing exchanges.

    The two UUIDs identify distinct ecoinvent flows, not interchangeable aliases.
    A single native ``Sodium`` label cannot carry their distinction. Checking
    the native label also covers releases that rename the ionic flow Sodium I.
    """

    def __init__(self, profile, names, catalog_provider=None):
        self.profile = profile
        self.names = names
        self.provider = catalog_provider
        self.catalog = None

    def issue(self, exchange, path):
        """Return a structured error for an active, identity-losing export."""
        categories = tuple(exchange.get("categories") or ())
        if (
            self.profile.family != "ecoinvent"
            or exchange.get("type") != "biosphere"
            or exchange.get("name") != "Sodium"
            or categories not in {("water",), ("water", ""), ("water", "unspecified")}
            or normalize_unit(exchange.get("unit", "")) != "kilogram"
        ):
            return None
        if (
            exchange.get("amount") == 0
            and not exchange.get("formula")
            and exchange.get("uncertainty type", 0) in (None, 0, 1)
        ):
            return None
        try:
            if self.catalog is None:
                provider = self.provider if self.provider is not None else catalog_provider_from_environment()
                self.catalog = provider.load_biosphere(self.profile)
        except (CatalogNotFoundError, ValueError) as error:
            return Issue(severity="error", code="simapro_biosphere_unresolved", message=str(error), path=path)
        identity = ("Sodium", ("water",), "kilogram")
        if self.catalog.references.get(identity) != _GENERIC_SODIUM:
            return None
        generic_label = self._label(identity)
        for peer, flow_id in self.catalog.references.items():
            # bw2io interprets native "Sodium" as ionic sodium even when a newer
            # catalogue calls that UUID "Sodium I" and its forward label differs.
            if (
                flow_id == _IONIC_SODIUM
                and peer[1:] == identity[1:]
                and (generic_label == "Sodium" or self._label(peer) == generic_label)
            ):
                return Issue(
                    severity="error",
                    code="simapro_biosphere_identity_ambiguous",
                    message=(
                        f"SimaPro label {generic_label!r} cannot preserve the distinction between "
                        f"aqueous 'Sodium' ({_GENERIC_SODIUM}) and {peer[0]!r} ({_IONIC_SODIUM}) "
                        f"in biosphere {self.profile.version}. Exporting the generic flow would silently "
                        "change its identity on reimport. Use another export format or a verified distinct mapping."
                    ),
                    path=path,
                    suggested_fix="Use Brightway, openLCA JSON-LD or ILCD for this inventory.",
                )
        return None

    def _label(self, identity):
        name, categories, unit = identity
        return resolve_ecoinvent_flow_name(
            {"name": name, "categories": categories, "unit": unit}, self.names.get(name, name)
        )
