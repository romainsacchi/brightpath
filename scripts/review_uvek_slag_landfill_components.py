"""Keep slag-landfill operation and construction separate from sanitary landfill."""

import hashlib
import json
import pickle
import sqlite3
from copy import deepcopy
from pathlib import Path

FIELDS = ("name", "reference product", "location", "unit")
REVIEW_PATH = "brightpath/data/export/uvek_slag_landfill_component_review.json"
APPROVAL = json.loads((Path(__file__).resolve().parents[1] / REVIEW_PATH).read_text())
SOURCES = {tuple(row["source"][f] for f in FIELDS): row for row in APPROVAL["rules"]}
EXPECTED = {
    (name, "process-specific burdens, slag landfill", location, "kilogram")
    for name in ("process-specific burdens, slag landfill", "market for process-specific burdens, slag landfill")
    for location in ("CH", "Europe without Switzerland", "RoW")
} | {
    ("market for slag landfill", "slag landfill", "GLO", "unit"),
    ("slag landfill construction", "slag landfill", "CH", "unit"),
    ("slag landfill construction", "slag landfill", "RoW", "unit"),
}
if (
    set(SOURCES) != EXPECTED
    or APPROVAL["authorized_system_model"] != "cutoff"
    or APPROVAL["source_count"] != 9
    or APPROVAL["source_profile_count"] != 81
    or APPROVAL["inventory_count"] != 85
):
    raise ValueError("Slag-landfill component review scope changed; repeat the scientific review.")
for source, row in SOURCES.items():
    operation = source[3] == "kilogram"
    name = "Process-specific burdens, slag compartment" if operation else "Slag compartment"
    if (
        row["kind"] != ("operation" if operation else "construction")
        or row["target"] != dict(zip(FIELDS, (name, name, "CH", source[3]), strict=True))
        or {p["label"] for p in row["profiles"]}
        != {f"ecoinvent-{v}-cutoff" for v in ("3.6", "3.7", "3.8", "3.9", "3.9.1", "3.10", "3.10.1", "3.11", "3.12")}
        or len(row["profiles"]) != 9
    ):
        raise ValueError("Slag-landfill component function or exact-profile scope changed.")
LIMITATIONS = (
    "Reviewed cut-off slag-landfill component proxy: use the dedicated Swiss slag compartment, "
    "rather than the former sanitary-landfill facility or operation. Construction means one complete "
    "375,000 m3 slag compartment at 1,500 kg/m3; the source documents construction/use/aftercare "
    "phases of 5/30/75 years. "
    "Do not use the former 1,800,000 m3 sanitary-landfill facility or introduce capacity rescaling. "
    "Operating burdens are per signed kg of waste, separately from facility demand and material-specific "
    "releases. The UVEK operation retains 0.027 MJ diesel, 0.00001 kWh low-voltage electricity, land use "
    "and 0.00107 MJ oil heat; source Swiss operation uses 0.0010058 MJ other heat. UVEK adds 0.000036 MJ "
    "waste heat. Construction material quantities largely match the Swiss source, but UVEK has explicit "
    "road/rail freight and lacks the source facility's direct 104.55 m3 water-to-air and 592.45 m3 "
    "water-to-water releases. These are disclosed differences in the substituted background inventory; "
    "caller-owned emissions remain unchanged. Swiss supply replaces RoW and Europe-without-Switzerland "
    "mixtures where applicable. Heat, electricity, freight, water, geography, technology and vintage "
    "differ; equal LCIA scores are not established. Both imported references are positive, including "
    "the UVEK operation's native WASTE_FLOW input: preserve quantity direction with factor +1. No "
    "treatment sign reversal, material production, recovery credit, omission, additional helper or "
    "inferred inverse is authorized. Preserve caller signed quantities, supported uncertainty, formulas, "
    "foreground suppliers and separate emissions. Only the nine exact inspected cut-off releases "
    "3.6 through 3.12, including 3.9.1 and 3.10.1, are reviewed; further patches and consequential conversion are not approved."
)
EVIDENCE_SCOPE = {}
for row in APPROVAL["inventories"]:
    EVIDENCE_SCOPE.setdefault(tuple(row["label"].split("-")), set()).add(tuple(row["source"][f] for f in FIELDS))


def inventory_record(source, blobs, comment, product_information):
    """Pin inventories and their descriptions without redistributing full source inventories."""
    return {
        "source": list(source),
        "inventory_sha256": hashlib.sha256(b"".join(sorted(blobs))).hexdigest(),
        "activity_comment_sha256": hashlib.sha256(str(comment or "").encode()).hexdigest(),
        "product_information_sha256": hashlib.sha256(str(product_information or "").encode()).hexdigest(),
    }


def validate_inventories(inventories, observations, native):
    """Require all exact construction, operation, market and former-supplier boundaries."""
    expected = {
        json.dumps({**row, "source": [row["source"][f] for f in FIELDS]}, sort_keys=True)
        for row in APPROVAL["inventories"]
    }
    actual = {
        json.dumps({**row, "source": list(source)}, sort_keys=True)
        for source, rows in inventories.items()
        for row in rows
    }
    if actual != expected or len(expected) != 85 or sum(map(len, inventories.values())) != 85:
        raise ValueError("Slag-landfill component inventory or metadata changed; repeat the scientific review.")
    for profile, sources in EVIDENCE_SCOPE.items():
        for source in sources:
            if ("-".join(profile), 1) not in observations[profile[0]].get(source, set()):
                raise ValueError("Slag-landfill component reference changed; repeat the scientific review.")
            if profile[0] == "uvek":
                expected_waste = source[3] == "kilogram"
                row = native.get(source, {})
                if (
                    row.get("native_flow_type") != ("WASTE_FLOW" if expected_waste else "PRODUCT_FLOW")
                    or row.get("native_reference_is_input") is not expected_waste
                ):
                    raise ValueError("Slag-landfill component native role changed; repeat the scientific review.")

    for source, row in SOURCES.items():
        if {
            (label, sign) for label, sign in observations["ecoinvent"].get(source, set()) if label.endswith("-cutoff")
        } != {(p["label"], 1) for p in row["profiles"]}:
            raise ValueError("Slag-landfill component exact profile changed; repeat the scientific review.")


def approved_component_rule(rule):
    """Correct only the nine independently reviewed component suppliers."""
    row = SOURCES.get(tuple(rule["source"][f] for f in FIELDS))
    if row is None:
        return rule
    if rule["target"] not in (row["previous_target"], row["target"]) or rule.get("conversion_factor", 1) != 1:
        raise ValueError("Slag-landfill component correspondence changed; repeat the scientific review.")
    return {**rule, "target": deepcopy(row["target"])}


def component_decision(rule, observations, uvek_signs, native, binding, previous, original_review):
    """Disclose the independently inspected function proxy without changing signed units."""
    row = SOURCES[tuple(rule["source"][f] for f in FIELDS)]
    waste = row["kind"] == "operation"
    if (
        rule["target"] != row["target"]
        or rule.get("conversion_factor", 1) != 1
        or {(label, sign) for label, sign in observations if label.endswith("-cutoff")}
        != {(p["label"], 1) for p in row["profiles"]}
        or uvek_signs != {1}
        or native.get("native_flow_type") != ("WASTE_FLOW" if waste else "PRODUCT_FLOW")
        or native.get("native_reference_is_input") is not waste
    ):
        raise ValueError("Slag-landfill component basis or role changed; repeat the scientific review.")
    return {
        "binding": binding,
        "original_factor": 1,
        "factor": 1,
        "source_observations": [list(pair) for pair in sorted(observations)],
        "uvek_reference_sign": 1,
        "uvek_native_flow_type": native["native_flow_type"],
        "original_target": deepcopy(row["previous_target"]),
        "original_review": deepcopy(previous.get("original_review", original_review)),
        "original_information_loss": previous.get(
            "original_information_loss", rule.get("technosphere_information_loss")
        ),
        "supplier_proxy_review": {
            "rationale": "Auto-approved under standing independently inspected matching-function, material, unit and geographic-proxy approvals. "
            + LIMITATIONS,
            "limitations": [LIMITATIONS],
            "evidence": [REVIEW_PATH],
        },
        "review": {
            "status": "unresolved",
            "reason": "Only the nine exact inspected cut-off component profiles are approved.",
            "evidence": [REVIEW_PATH],
            "profiles": [
                {
                    "source_profile": dict(
                        zip(("family", "version", "system_model"), p["label"].split("-"), strict=True)
                    ),
                    "conversion_factor": 1,
                    "review": {
                        "status": "verified",
                        "source_sign": 1,
                        "target_sign": 1,
                        "reason": LIMITATIONS,
                        "evidence": [REVIEW_PATH],
                    },
                }
                for p in row["profiles"]
            ],
        },
    }


def collect_extra_inventories(brightway_root):
    """Read positive reference and exact inventories from the two original patch databases."""
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
                    (reference,) = [pickle.loads(b) for b in blobs if pickle.loads(b)["type"] == "production"]
                    if reference["amount"] != 1 or tuple(reference["input"]) != (entry["database"], metadata["code"]):
                        raise ValueError("Slag component patch reference changed; repeat the scientific review.")
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
                raise ValueError("Slag component patch scope changed; repeat the scientific review.")
    return records
