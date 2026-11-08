"""Keep chemical heat in MJ and retain source make-up water and physical releases."""

import hashlib
import json
import pickle
import sqlite3
from pathlib import Path

try:
    from review_uvek_market_compositions import composition_decision, composition_recipe, validate_composition_bindings
    from review_uvek_whole_freight import inventory_record
except ModuleNotFoundError:
    from scripts.review_uvek_market_compositions import (
        composition_decision,
        composition_recipe,
        validate_composition_bindings,
    )
    from scripts.review_uvek_whole_freight import inventory_record

FIELDS = ("name", "reference product", "location", "unit")
ROOT = Path(__file__).resolve().parents[1]
REVIEW_PATH = "brightpath/data/export/uvek_chemical_heat_review.json"
APPROVAL = json.loads((ROOT / REVIEW_PATH).read_text())
REVIEW_SHA256 = "4a41555ab81c4429f8addfdc14f7f9826d2b305bfd04393691f391f925b55a69"
VERSIONS = {"3.6", "3.7", "3.8", "3.9", "3.9.1", "3.10", "3.10.1", "3.11", "3.12"}
NAMES = {
    "market for heat, from steam, in chemical industry",
    "steam production, as energy carrier, in chemical industry",
}
EXPECTED_SOURCES = {
    (name, "heat, from steam, in chemical industry", location, "megajoule")
    for name in NAMES
    for location in ("RER", "RoW")
}
TARGET = (
    "Heat, unspecific, in chemical plant",
    "Heat, unspecific, in chemical plant",
    "RER",
    "megajoule",
)
LIMITATIONS = APPROVAL["limitations"]
SOURCES = {tuple(row["source"][f] for f in FIELDS): row for row in APPROVAL["rules"]}
EVIDENCE_SCOPE = {}
for row in APPROVAL["inventories"]:
    EVIDENCE_SCOPE.setdefault(tuple(row["label"].split("-")), set()).add(tuple(row["source"][f] for f in FIELDS))


def identity(row):
    return tuple(row[f] for f in FIELDS)


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()


def validate_review():
    """Do not inherit this heat-energy decision for physical steam or another profile."""
    if (
        digest(APPROVAL) != REVIEW_SHA256
        or set(SOURCES) != EXPECTED_SOURCES
        or APPROVAL["authorized_system_model"] != "cutoff"
        or APPROVAL["source_count"] != 4
        or APPROVAL["source_profile_count"] != 36
        or APPROVAL["inventory_count"] != 141
        or not APPROVAL["scientific_boundary"]["native_heat_water_excluded"]
    ):
        raise ValueError("Chemical-heat inventory or boundary changed; repeat the scientific review.")
    expected_labels = {f"ecoinvent-{v}-cutoff" for v in VERSIONS}
    for row in SOURCES.values():
        if {p["label"] for p in row["profiles"]} != expected_labels or len(row["profiles"]) != 9:
            raise ValueError("Chemical-heat profile scope changed; repeat the scientific review.")
        for profile in row["profiles"]:
            heat = [e for e in profile["inputs"] if e["type"] == "technosphere" and e["unit"] == "megajoule"]
            water = [e for e in profile["inputs"] if e["type"] == "technosphere" and e["unit"] == "kilogram"]
            releases = [e for e in profile["inputs"] if e["type"] == "biosphere"]
            if (
                profile["source_sign"] != 1
                or len(heat) != 1
                or identity(heat[0]) != TARGET
                or heat[0]["amount"] != 1
                or not water
                or any(not e["name"].startswith("Tap water,") or e["amount"] <= 0 for e in water)
                or len(releases) != 2
                or {tuple(e["categories"]) for e in releases} != {("air",), ("water",)}
                or any(e["name"] != "Water" or e["unit"] != "cubic meter" for e in releases)
                or len(profile["inputs"]) != 1 + len(water) + len(releases)
            ):
                raise ValueError(
                    "Chemical-heat functional unit or water boundary changed; repeat the scientific review."
                )


def collect_extra_inventories(brightway_root):
    """Read the two actual patch databases, never inherit their water shares from a base release."""
    validate_review()
    result = []
    for entry in APPROVAL["extra_profiles"]:
        scope = EVIDENCE_SCOPE[tuple(entry["profile"])]
        path = brightway_root / entry["project"] / "lci/databases.db"
        found = set()
        with sqlite3.connect(path.as_uri() + "?mode=ro", uri=True) as connection:
            for name in sorted({s[0] for s in scope}):
                for (raw,) in connection.execute(
                    "SELECT data FROM activitydataset WHERE database=? AND name=?", (entry["database"], name)
                ):
                    metadata = pickle.loads(raw)
                    source = identity(metadata)
                    if source not in scope:
                        continue
                    blobs = [
                        b
                        for b, in connection.execute(
                            "SELECT data FROM exchangedataset WHERE output_database=? AND output_code=?",
                            (entry["database"], metadata["code"]),
                        )
                    ]
                    result.append(
                        {
                            **inventory_record(
                                source, blobs, metadata.get("comment"), metadata.get("product_information")
                            ),
                            "label": "-".join(entry["profile"]),
                        }
                    )
                    found.add(source)
        if found != scope:
            raise ValueError("Chemical-heat exact patch evidence changed; repeat the scientific review.")
    return result


def validate_inventories(inventories, observations, native):
    """Require all complete heat, water, separate mass-steam and native supplier fingerprints."""
    validate_review()
    expected = {
        json.dumps({**r, "source": list(identity(r["source"]))}, sort_keys=True) for r in APPROVAL["inventories"]
    }
    actual = {json.dumps({**r, "source": list(s)}, sort_keys=True) for s, rows in inventories.items() for r in rows}
    if actual != expected or len(expected) != 141 or sum(map(len, inventories.values())) != 141:
        raise ValueError("Chemical-heat inventory or description changed; repeat the scientific review.")
    for source, row in SOURCES.items():
        observed = {
            (label, sign) for label, sign in observations["ecoinvent"].get(source, set()) if label.endswith("-cutoff")
        }
        if observed != {(p["label"], 1) for p in row["profiles"]}:
            raise ValueError("Chemical-heat positive MJ reference changed; repeat the scientific review.")
    for role in APPROVAL["native_roles"]:
        source = identity(role["source"])
        if observations["uvek"].get(source) != {("uvek-2025-cutoff", 1)} or any(
            native.get(source, {}).get(f) != role[f]
            for f in ("native_flow_type", "native_reference_is_input", "flow_property_name", "unit_name")
        ):
            raise ValueError("Chemical-heat or retained-water native reference changed; repeat the scientific review.")


def validate_bindings(resource):
    """Keep exact source water shares and the original biosphere receiving contexts."""
    validate_review()
    validate_composition_bindings(resource, approval=APPROVAL, label="Chemical heat")
    catalog = json.loads((ROOT / "brightpath/data/export/reference_catalogs/ecoinvent__3.10__cutoff.json").read_text())
    by_uuid = {r["flow_id"]: r for r in catalog["biosphere"]}
    if len(APPROVAL["canonical_biosphere_definitions"]) != 2:
        raise ValueError("Chemical-heat physical release scope changed; repeat the scientific review.")
    for definition in APPROVAL["canonical_biosphere_definitions"]:
        if {k: by_uuid.get(definition["flow_id"], {}).get(k) for k in definition} != definition:
            raise ValueError("Chemical-heat physical water identity changed; repeat the scientific review.")


def heat_recipe(source, profile):
    """Compile a complete per-MJ heat proxy plus every original make-up-water flow."""
    validate_review()
    if source not in SOURCES or profile not in SOURCES[source]["profiles"]:
        raise ValueError("Chemical-heat profile is unreviewed; repeat the scientific review.")
    return composition_recipe(
        source, profile, rationale=LIMITATIONS, review_path=REVIEW_PATH, namespace="chemical-heat-water"
    )


def forward_decision(rule, observations, inventories, binding, original_review):
    """Author an explicitly scoped +1 MJ helper without inferring a reverse correspondence."""
    validate_review()
    source = identity(rule["source"])
    if source not in SOURCES:
        raise ValueError("Chemical-heat source is unreviewed; repeat the scientific review.")
    decision = composition_decision(
        rule,
        {(label, sign) for label, sign in observations if label.endswith("-cutoff")},
        [(r["label"], r["inventory_sha256"]) for r in inventories],
        binding,
        original_review,
        approval_row=SOURCES[source],
        rationale=LIMITATIONS,
        review_path=REVIEW_PATH,
        namespace="chemical-heat-water",
        mapping_method="reviewed_chemical_heat_with_retained_water",
        profile_reason="Only the 36 exact inspected cut-off heat-energy profiles and water balances are reviewed.",
    )
    decision["source_observations"] = [list(row) for row in sorted(observations)]
    return decision
