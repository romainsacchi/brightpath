"""Verified native aqueous-ion labels; no migration or quantity changes.

Exact SP94 factor-one UUID mappings and native95 label/context witnesses are
recorded in tests/fixtures/simapro_water_ion_evidence.json. For Calcium and the qualified generic Vanadium label,
stock CAS/formula differ although the unique catalog UUID is ionic;
that stock metadata discrepancy is not a generic-to-ionic substitution rule.
The three additional modern Cs/Mo/Sr labels use a separately reviewed
chemical-definition correspondence, not legacy stock-ID continuity. Source
CAS discrepancies remain documented; no mass/stoichiometry conversion occurs.
Chromium III/VI use qualified modern labels; valid legacy bare labels and
older3.6-3.8 wire behavior are not declared generally invalid.
"""

from brightpath.background.catalogs import catalog_provider_from_environment
from brightpath.units import normalize_unit

# Exact display names of these SAME publisher UUIDs in the two earlier
# modern catalogs; not a general generic-to-ionic name substitution.
_PROFILE_NAMES = {
    ("Caesium I", "3.9"): "Caesium",
    ("Caesium I", "3.9.1"): "Caesium",
    ("Strontium II", "3.9"): "Strontium",
    ("Strontium II", "3.9.1"): "Strontium",
}

_VERSIONS = frozenset({"3.9", "3.9.1", "3.10", "3.10.1", "3.11", "3.12"})
_WIRE = {
    "Aluminium III": "Aluminium (III)",
    "Barium II": "Barium (II)",
    "Cadmium II": "Cadmium (II)",
    "Calcium II": "Calcium",
    "Cobalt II": "Cobalt (II)",
    "Lead II": "Lead (II)",
    "Lithium I": "Lithium (I)",
    "Manganese II": "Manganese (II)",
    "Mercury II": "Mercury (II)",
    "Nickel II": "Nickel (II)",
    "Potassium I": "Potassium (I)",
    "Silver I": "Silver (I)",
    "Zinc II": "Zinc (II)",
    "Vanadium V": "Vanadium",
    "Caesium I": "Cesium (I)",
    "Molybdenum VI": "Molybdenum (VI)",
    "Strontium II": "Strontium (II)",
    "Chromium III": "Chromium (III)",
    "Chromium VI": "Chromium (VI)",
}
_UUIDS = {
    ("water",): {
        "Aluminium III": "7880e65c-0321-4295-ba55-3186ad36e489",
        "Barium II": "0e1c17ae-8144-4714-a7cd-786a5480f423",
        "Cadmium II": "2712fe5f-0d3c-4c8e-8bc4-26c604413849",
        "Calcium II": "08151735-9d1b-4b1c-a8b1-f341ade54f2f",
        "Cobalt II": "33627701-feca-434d-a192-674a450eaaf7",
        "Lead II": "31c4d160-b21e-41b3-b9c1-3d7a72377653",
        "Lithium I": "eb2bfdf1-1881-40e8-bebb-d674b2a79149",
        "Manganese II": "10cbd2f7-c41b-4bb4-b636-d8fc15cf0282",
        "Mercury II": "9325957a-bee5-4f2d-8bd1-458729c5e49e",
        "Nickel II": "6ec6cdaa-5a01-46d4-b716-1905e56690ea",
        "Potassium I": "130bbacf-e7c7-4ffd-b109-4d576f1d02c1",
        "Silver I": "6aa8673f-f7e3-4270-93dc-e97b745fced7",
        "Zinc II": "2c7be02c-791f-4fef-b231-b90321bbeb30",
        "Vanadium V": "20839113-fbf2-4d2f-bab8-66861f86bc60",
        "Caesium I": "59570d74-fac9-446d-88a7-5c5b0a6139a7",
        "Molybdenum VI": "fdaaef05-0d10-4032-b2e7-85cae3c7bd5c",
        "Strontium II": "b26b758d-85cc-41b8-872c-3683caa638d1",
        "Chromium III": "30f484ee-dec4-47ae-8e92-d00d4b93fe05",
        "Chromium VI": "a9ec0c9b-982a-4329-99ae-dab1f9fb62d3",
    },
    ("water", "surface water"): {
        "Aluminium III": "97e498ec-f323-4ec6-bcc0-d8a4c853bae3",
        "Barium II": "2c872773-0a29-4831-93b9-d49b116fa7d5",
        "Cadmium II": "af83b42f-a4e6-4457-be74-46a87798f82a",
        "Calcium II": "ac066c02-b403-407b-a1f0-b29ad0f8188f",
        "Cobalt II": "d4291dd5-dae8-47fa-bf06-466fcecbc210",
        "Lead II": "b3ebdcc3-c588-4997-95d2-9785b26b34e1",
        "Lithium I": "e98084d6-88a2-48cf-9ad5-6462fe9cf857",
        "Manganese II": "f532985c-90b7-46fc-aac9-b039b40e22f1",
        "Mercury II": "66bfb434-78ab-4183-b1a7-7f87d08974fa",
        "Nickel II": "9798359e-a3ee-4362-a038-23a188582c6e",
        "Potassium I": "1653bf60-f682-4088-b02d-6dc44eae2786",
        "Silver I": "af9793ba-25a1-4928-a14a-4bcf7d5bd3f7",
        "Zinc II": "541b633c-17a3-4047-bce6-0c0e4fdb7c10",
        "Vanadium V": "a46a250e-297d-43e9-b1c4-052cdcfb79c5",
        "Caesium I": "96f0a692-ddb9-4a4c-8236-7c943929653d",
        "Molybdenum VI": "442511cc-a98b-4242-9229-5736cb9a9399",
        "Strontium II": "4295ed5b-9824-4bbf-97a4-fc4cabd87f0d",
        "Chromium III": "e34d3da4-a3d5-41be-84b5-458afe32c990",
        "Chromium VI": "8216fc31-15a1-4d33-858f-e09650b14c63",
    },
    ("water", "ground-"): {
        "Aluminium III": "2e0c9efa-d55c-49db-8be3-0376d5921ff4",
        "Barium II": "d68f41b3-6887-49a8-8efa-96812f8ec9f6",
        "Cadmium II": "3937f5a5-b4a4-434d-9fc3-1d29064fc3f8",
        "Calcium II": "1d0f4392-8c91-4d01-8a5d-f8fe1f5b05c9",
        "Cobalt II": "f0d1c319-249f-4a89-b0e7-3a5a90a54036",
        "Lead II": "c864aa15-0abc-41ad-9889-348106e485d8",
        "Lithium I": "4569494e-085e-413a-8fef-63a3c419e0cf",
        "Manganese II": "44f28e11-061d-44fd-8f25-dc62a9f1fa57",
        "Mercury II": "e1aa92d7-9297-452e-ade0-e5e2acc52ad7",
        "Nickel II": "56815b4f-6138-4e0b-9fac-c94fd6b102b3",
        "Potassium I": "c21a1397-82dc-427a-a6cb-c790ba2626f4",
        "Silver I": "71cfe87c-ab80-421e-adba-afb7d5bd0861",
        "Zinc II": "7e769b3e-03fb-4b53-be15-85a910dcfea9",
        "Vanadium V": "ed1fd27e-014a-41cf-842f-7b2022b0be79",
        "Caesium I": "a5e13836-93f3-4a63-8e5e-944ac627bced",
        "Molybdenum VI": "3822809c-cc03-469d-ac2a-9f85842f8440",
        "Strontium II": "070f9c6e-584d-40ed-8b07-6b1f7f75dd3f",
        "Chromium III": "1c87de06-e58f-4684-a54c-d29f1a251a87",
        "Chromium VI": "9bbae55a-35de-4eff-a219-814a9fec5ac2",
    },
    ("water", "ground-, long-term"): {
        "Aluminium III": "01056d4b-f9b0-4dfc-b8d9-8407c8376efb",
        "Barium II": "3184c902-c587-4fd7-bd89-769d3cf30072",
        "Cadmium II": "4ce23896-a82f-49f1-a8e3-8848734805a3",
        "Calcium II": "ae28c923-a4a3-4f00-b862-1ae6e748efb9",
        "Cobalt II": "1e904267-caa3-40e6-a3a0-fc104489770b",
        "Lead II": "0ff15165-d761-48c9-9814-2d1dcdd56857",
        "Manganese II": "835d0907-a668-4d92-b6b6-7b56fa343713",
        "Mercury II": "40f89b03-64f8-466d-9480-bca47d8a435c",
        "Nickel II": "9f69cb8e-51fe-447b-a1ac-4da74de8ebe4",
        "Potassium I": "b93fc4df-91b2-4ac4-a315-89147515ddfb",
        "Silver I": "94426df9-63b1-4dc6-ad1b-3509e22f1154",
        "Zinc II": "bdce3a7c-e008-4391-be26-285f71a02453",
        "Vanadium V": "41dbbe1f-06e1-4a34-8497-5986be8f9e51",
        "Caesium I": "2267ffd7-fe74-441e-873b-dd66c26a5e9f",
        "Molybdenum VI": "7d3f7498-3596-478a-9fa1-72201ef94082",
        "Strontium II": "8f1b69d9-fc00-4b46-99f9-7a57fd71dad2",
        "Chromium III": "d9c23a8d-e7bf-4f70-8d22-b9ad8a653204",
        "Chromium VI": "ef475b50-9cd0-4e7c-9c59-07868fe16e91",
    },
    ("water", "ocean"): {
        "Aluminium III": "0ace7678-a181-48e8-8728-d912a8b56c57",
        "Barium II": "95ad40ec-c34b-49fc-a7dc-0e13172ac105",
        "Cadmium II": "62fcd384-906d-474f-a84b-9f944b78f804",
        "Calcium II": "55bf893f-0d89-429c-be59-2f99b33ddb11",
        "Cobalt II": "caf066cf-40ce-47a8-ba4c-969fcd6019c7",
        "Lead II": "e9e856c2-868e-435e-8425-8df1c9297cfc",
        "Lithium I": "cf88e72e-13bc-416c-ac41-7d4b24bda86a",
        "Manganese II": "c91fd4bf-ed70-4e2f-a34f-159773f981b6",
        "Mercury II": "fd61d155-3b34-46d5-8769-efb3e806f545",
        "Nickel II": "23cb10a4-6228-4ce4-9fb5-7043bc31faec",
        "Potassium I": "297cc04f-e215-433c-ae3b-d1e34464c785",
        "Silver I": "21c257b2-57ce-4a6e-b4bd-40f0f75f310f",
        "Zinc II": "2195d86f-e4c8-4e86-9f18-b5b065b1df7e",
        "Vanadium V": "dbdc3ea8-cc2b-4dbe-82b9-e954e333281d",
        "Caesium I": "e189eb4e-0f6a-4054-aaf8-47df28c87ec4",
        "Molybdenum VI": "163926e2-b4cc-42bb-ba7d-401028140985",
        "Strontium II": "836493d4-5c50-4713-b2a8-7979b830c1a5",
        "Chromium III": "d98f0ec0-bf19-4c90-a457-a8593dec497f",
        "Chromium VI": "81b3321a-c9d8-4104-a974-5460f1bc5029",
    },
}


def _categories(exchange):
    value = tuple(exchange.get("categories") or ())
    return ("water",) if value in {("water", ""), ("water", "unspecified")} else value


class WaterIonIdentityGuard:
    """Use only exact declared-profile, context, kilogram and publisher UUIDs."""

    def __init__(self, profile, catalog_provider=None):
        self.profile = profile
        self.provider = catalog_provider
        self.catalog = None

    def _scope(self, exchange):
        return (
            self.profile.family == "ecoinvent"
            and self.profile.version in _VERSIONS
            and exchange.get("type") == "biosphere"
            and _categories(exchange) in _UUIDS
            and normalize_unit(exchange.get("unit", "")) == "kilogram"
        )

    def _identity(self, canonical, categories):
        if self.catalog is None:
            provider = self.provider if self.provider is not None else catalog_provider_from_environment()
            self.catalog = provider.load_biosphere(self.profile)
        if self.catalog.profile != self.profile:
            raise ValueError("Water-ion catalog does not match the exact declared biosphere profile.")
        expected = _UUIDS[categories].get(canonical)
        if expected is None:
            raise ValueError("No verified native water-ion literal for this UUID/context pair.")
        name = _PROFILE_NAMES.get((canonical, self.profile.version), canonical)
        identity = name, categories, "kilogram"
        literal = _WIRE[canonical], categories, "kilogram"
        if (
            literal != identity
            and literal in self.catalog.identities
            and self.catalog.references.get(literal) != expected
        ):
            raise ValueError("The native water-ion wire label collides with a distinct catalog UUID.")
        matches = [
            key
            for key, uid in self.catalog.references.items()
            if uid == expected and key[1:] == (categories, "kilogram")
        ]
        if matches != [identity] or identity not in self.catalog.identities:
            raise ValueError("No unique verified water-ion UUID/context/unit identity in the declared catalog.")
        return identity

    def wire_name(self, exchange, fallback):
        """Return a verified stock label without modifying the source exchange."""
        if (
            self.profile.family != "ecoinvent"
            or self.profile.version not in _VERSIONS
            or _categories(exchange)[:1] != ("water",)
        ):
            return fallback
        name = exchange.get("name")
        family = (
            name
            if name in _WIRE
            else next(
                (
                    family
                    for (family, version), profile_name in _PROFILE_NAMES.items()
                    if version == self.profile.version and profile_name == name
                ),
                None,
            )
        )
        if family is None:
            return fallback
        if not self._scope(exchange):
            raise ValueError("No verified water-ion compartment/unit for this source identity.")
        identity = self._identity(family, _categories(exchange))
        if identity[0] != name:
            raise ValueError("The water-ion source name does not match its exact declared catalog.")
        return _WIRE[family]

    def restore_wire_identity(self, exchange, raw_name, *, allowed_flows=None):
        """Decode a proven stock label using an explicit UUID-bearing provider."""
        inverse = {wire: canonical for canonical, wire in _WIRE.items()}
        if (
            raw_name not in inverse
            or self.profile.family != "ecoinvent"
            or self.profile.version not in _VERSIONS
            or _categories(exchange)[:1] != ("water",)
        ):
            return False
        if not self._scope(exchange):
            raise ValueError("No verified water-ion compartment/unit for this native wire label.")
        if self.provider is None:
            raise ValueError("An explicit UUID-bearing catalog provider is required to decode water ions.")
        identity = self._identity(inverse[raw_name], _categories(exchange))
        name, categories, unit = identity
        key = name, categories[0], categories[1] if len(categories) > 1 else "unspecified"
        if allowed_flows is not None and key not in allowed_flows:
            raise ValueError("The verified water-ion identity is excluded by the supplied biosphere flowset.")
        exchange.update(name=name, categories=categories, unit=unit)
        return True


def remember_water_ion_wire_labels(data, profile):
    """Retain only these stock wire labels before bw2io generic name strategies."""
    if profile.family != "ecoinvent" or profile.version not in _VERSIONS:
        return
    labels = frozenset(_WIRE.values())
    for dataset in data:
        for exchange in dataset.get("exchanges", []):
            if exchange.get("type") == "biosphere" and exchange.get("name") in labels:
                exchange["simapro name"] = exchange["name"]
