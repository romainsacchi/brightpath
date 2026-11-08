"""Author exact post-incineration bottom-ash landfill services with retained releases."""

import hashlib
import json
import pickle
import sqlite3
from copy import deepcopy
from pathlib import Path

try:
    from review_uvek_market_compositions import composition_decision, validate_composition_bindings
except ModuleNotFoundError:
    from scripts.review_uvek_market_compositions import composition_decision, validate_composition_bindings

FIELDS = ("name", "reference product", "location", "unit")
ROOT = Path(__file__).resolve().parents[1]
REVIEW_PATH = "brightpath/data/export/uvek_bottom_ash_review.json"
APPROVAL = json.loads((ROOT / REVIEW_PATH).read_text())
SOURCES = {tuple(row["source"][f] for f in FIELDS): row for row in APPROVAL["rules"]}
if (
    len(SOURCES) != 49
    or APPROVAL["authorized_system_model"] != "cutoff"
    or APPROVAL["source_count"] != 49
    or APPROVAL["source_profile_count"] != 200
    or APPROVAL["inventory_count"] != 200
    or any(
        not source[0].startswith("treatment of bottom ash,")
        or not source[0].endswith("slag compartment")
        or source[3] != "kilogram"
        for source in SOURCES
    )
    or sum(len(row["profiles"]) for row in SOURCES.values()) != 200
):
    raise ValueError("Bottom-ash service review scope changed; repeat the scientific review.")
LIMITATIONS = (
    "Reviewed post-incineration bottom-ash slag-compartment landfill helper per one kg incoming ash. "
    "Retain each waste stream's inspected short-term surface-water and long-term groundwater releases, "
    "other direct emissions and source quantities without substituting a generic waste composition. "
    "Retain one kg of the inspected regional operating mixture and the original 1.7778e-9 facility "
    "units per kg, using the independently reviewed Swiss UVEK slag operation and 375,000 m3 slag "
    "compartment. Preserve source regional weights without renormalization. Swiss energy, material, "
    "heat, water, geography, freight and vintage approximations remain disclosed by the component "
    "reviews. The source excludes collection and incoming-waste transport; target-facility construction "
    "freight is inherited, not separately added or removed. This service starts with ash already "
    "generated: do not add municipal incineration, upstream wastewater treatment, whole original-waste "
    "disposal or recovery credits. MSWI[F] and WWT-derived ashes retain their own release inventory. "
    "The source reference is -1 kg and the helper reference +1 kg: apply factor -1 to caller ash demand, "
    "preserving genuine signed quantities; no absolute value, moisture, dry-mass or carbon scaling is "
    "authorized. Retain reported waste/carbon inventory indicators where supported; otherwise omit "
    "only those explicitly marked indicators with warnings under the standing policy. Actual emissions "
    "are not omitted. Native UVEK water proxies preserve signed kg with explicit per-flow chemical and "
    "receiving-water loss reports, as approved separately; named anion masses are not converted to "
    "elemental mass. LCIA equivalence is not established. Internal background uncertainty and parameter "
    "formulas are not reconstructed. Preserve caller uncertainty, formulas, metadata, foreground "
    "suppliers, production and separately listed emissions. Score and export all returned helpers. "
    "Only the enumerated exact cut-off profiles are approved; no patch, model or inverse fallback is "
    "inferred. Whole-ash treatment is not the inverse of generic UVEK operating burdens."
)
EVIDENCE_SCOPE = {}
for row in APPROVAL["inventories"]:
    profile = tuple(row["label"].split("-"))
    if profile not in {
        ("ecoinvent", version, "cutoff") for version in ("3.9", "3.9.1", "3.10", "3.10.1", "3.11", "3.12")
    }:
        raise ValueError("Unreviewed bottom-ash evidence profile.")
    EVIDENCE_SCOPE.setdefault(profile, set()).add(tuple(row["source"][f] for f in FIELDS))


def inventory_record(source, blobs, comment, product_information):
    """Pin actual treatment vectors and boundaries without retaining source inventories."""
    return {
        "source": list(source),
        "inventory_sha256": hashlib.sha256(b"".join(sorted(blobs))).hexdigest(),
        "activity_comment_sha256": hashlib.sha256(str(comment or "").encode()).hexdigest(),
        "product_information_sha256": hashlib.sha256(str(product_information or "").encode()).hexdigest(),
    }


def validate_inventories(inventories, observations):
    """Require all 200 exact inventories and their negative one-kg coordinate signs."""
    expected = {
        json.dumps({**row, "source": list(tuple(row["source"][f] for f in FIELDS))}, sort_keys=True)
        for row in APPROVAL["inventories"]
    }
    actual = {
        json.dumps({**row, "source": list(source)}, sort_keys=True)
        for source, records in inventories.items()
        for row in records
    }
    if actual != expected or len(expected) != 200 or sum(map(len, inventories.values())) != 200:
        raise ValueError("Bottom-ash inventory or boundary metadata changed; repeat the scientific review.")
    for source, row in SOURCES.items():
        expected_signs = {(p["label"], -1) for p in row["profiles"]}
        actual_signs = {(label, sign) for label, sign in observations.get(source, set()) if label.endswith("-cutoff")}
        if actual_signs != expected_signs:
            raise ValueError("Bottom-ash exact reference or profile changed; repeat the scientific review.")


def validate_bindings(resource):
    """Keep construction, operation and used native elementary-flow approvals independent."""
    validate_composition_bindings(resource, approval=APPROVAL, label="Bottom-ash")
    native = json.loads(
        (ROOT / "brightpath/data/migrations/uvek/ecoinvent-3.10-biosphere-to-uvek-2026-biosphere.json").read_text()
    )

    def identity(row):
        return row["name"], tuple(row["categories"]), row["unit"]

    rules = {identity(row["source"]): row for row in native["replace"]}
    if len(APPROVAL["biosphere_bindings"]) != 84:
        raise ValueError("Bottom-ash biosphere scope changed; repeat the scientific review.")
    for entry in APPROVAL["biosphere_bindings"]:
        digest = hashlib.sha256(json.dumps(rules.get(identity(entry["source"])), sort_keys=True).encode()).hexdigest()
        if digest != entry["binding_sha256"]:
            raise ValueError("Bottom-ash native biosphere binding changed; repeat the scientific review.")


def forward_decision(rule, observations, inventories, binding, original_review):
    """Represent only the source ash service, retaining every waste-specific release."""
    source = tuple(rule["source"][f] for f in FIELDS)
    cf = {(label, sign) for label, sign in observations if label.endswith("-cutoff")}
    decision = composition_decision(
        rule,
        cf,
        [(row["label"], row["inventory_sha256"]) for row in inventories],
        binding,
        deepcopy(original_review),
        approval_row=SOURCES[source],
        rationale=LIMITATIONS,
        review_path=REVIEW_PATH,
        namespace="bottom-ash-landfill",
        mapping_method="reviewed_bottom_ash_landfill_service",
        profile_reason="Select only an exact independently inspected cut-off bottom-ash service profile; no unreviewed patch, waste type, composition or model fallback is approved.",
    )

    decision["source_observations"] = [list(pair) for pair in sorted(observations)]
    return decision


def collect_extra_inventories(brightway_root):
    """Read exact ash-service fingerprints and negative references from original patch databases."""
    records = []
    for entry in APPROVAL["extra_profiles"]:
        scope = EVIDENCE_SCOPE[tuple(entry["profile"])]
        path = brightway_root / entry["project"] / "lci/databases.db"
        with sqlite3.connect(path.as_uri() + "?mode=ro", uri=True) as connection:
            found = set()
            for name in sorted({source[0] for source in scope}):
                for (raw,) in connection.execute(
                    "SELECT data FROM activitydataset WHERE database=? AND name=?", (entry["database"], name)
                ):
                    metadata = pickle.loads(raw)
                    source = tuple(metadata[f] for f in FIELDS)
                    if source not in scope:
                        continue
                    blobs = [
                        b
                        for b, in connection.execute(
                            "SELECT data FROM exchangedataset WHERE output_database=? AND output_code=?",
                            (entry["database"], metadata["code"]),
                        )
                    ]
                    exchanges = [pickle.loads(b) for b in blobs]
                    (reference,) = [e for e in exchanges if e["type"] == "production"]
                    if reference["amount"] != -1 or tuple(reference["input"]) != (entry["database"], metadata["code"]):
                        raise ValueError("Bottom-ash patch reference changed; repeat the scientific review.")
                    records.append(
                        {
                            **inventory_record(
                                source, blobs, metadata.get("comment"), metadata.get("product_information")
                            ),
                            "label": "-".join(entry["profile"]),
                            "reference_amount": reference["amount"],
                        }
                    )
                    found.add(source)
            if found != scope:
                raise ValueError("Bottom-ash patch scope changed; repeat the scientific review.")
    return records
