"""Author exact-profile refinery flaring proxies after independent inventory inspection."""

import json
from pathlib import Path

FIELDS = ("name", "reference product", "location", "unit")
REVIEW_PATH = "brightpath/data/export/uvek_refinery_flare_review.json"
APPROVAL = json.loads((Path(__file__).resolve().parents[1] / REVIEW_PATH).read_text())
TARGET = tuple(APPROVAL["target"]["target"][f] for f in FIELDS)
LIMITATIONS = (
    "Approved matching-service refinery-flaring screening proxy: preserve the incoming MJ quantity with "
    "factor -1, converting the negative ecoinvent waste reference to UVEK's positive combustion-service "
    "reference. Both inventories cover direct emissions from refinery flares and exclude flare infrastructure; "
    "neither includes fuel production, purchased fuel, recovered energy or electricity/heat substitution credits. "
    "UVEK replaces the source emission inventory: nine active source biosphere exchanges versus ten in UVEK. "
    "Emission factors, pollutant coverage, speciation and underlying plant/geographic representation differ. "
    "LCIA equivalence, including toxicity and climate scores, is not established or quantified. The source "
    "market supplies exactly one MJ of this flaring service without freight or other active inputs/emissions. "
    "Preserve caller-owned uncertainty, formulas, foreground suppliers and separately listed emissions; "
    "overlapping foreground releases remain the caller's responsibility. Only enumerated exact profiles are "
    "approved. Natural-gas flares, gas turbines and refinery-gas production are outside this approval. "
    "No reverse reconstruction is inferred."
)
FLARES = {}
for row in APPROVAL["rules"]:
    source = tuple(row["source"][f] for f in FIELDS)
    if source in FLARES or tuple(row["previous_target"][f] for f in FIELDS) != TARGET:
        raise ValueError("Duplicate or changed refinery-flare approval.")
    FLARES[source] = {
        "target": TARGET,
        "previous_target": TARGET,
        "source_versions": ("3.6", "3.7", "3.8", "3.9", "3.10", "3.11", "3.12"),
        "native_product_service": True,
        "reference_function": "refinery-gas flaring service",
        "profiles": row["profiles"],
        "rationale": (
            "Auto-approved under the independently inspected matching-material/function/unit treatment-service "
            "proxy authorization: map this exact refinery-gas flaring reference directly to its matching "
            "GLO/MJ UVEK flare with factor -1. No helper or heating-value conversion is needed. " + LIMITATIONS
        ),
        "limitations": LIMITATIONS,
        "inventory_evidence": [
            f"{REVIEW_PATH}: 28 exact source/profile fingerprints and one independently inspected UVEK supplier "
            "fingerprint. Single self-linked source references -1 MJ versus target +1 MJ; native PRODUCT_FLOW "
            "output describes combustion as a service. Fourteen source-market inventories contain only -1 MJ "
            "of the corresponding direct flaring service. Sparse evidence only; no complete inventories published."
        ],
    }
if (
    len(FLARES) != APPROVAL["source_count"]
    or sum(len(r["profiles"]) for r in FLARES.values()) != APPROVAL["source_profile_count"]
):
    raise ValueError("Refinery-flare approval scope disagrees with its sparse evidence.")


def flare_profiles(source, observations, inventories, evidence):
    """Reject changed inventory fingerprints, references or exact source-profile coverage."""
    expected = sorted((p["label"], p["inventory_sha256"]) for p in FLARES[source]["profiles"])
    if (
        sorted(observations) != [(label, -1) for label, _ in expected]
        or sorted(inventories[source]) != expected
        or inventories[TARGET] != [("uvek-2025-cutoff", APPROVAL["target"]["inventory_sha256"])]
    ):
        raise ValueError("Refinery-flare inventory or profile changed; repeat the scientific review.")
    return [
        {
            "source_profile": dict(zip(("family", "version", "system_model"), label.split("-"), strict=True)),
            "conversion_factor": -1,
            "review": {
                "status": "verified",
                "source_sign": -1,
                "target_sign": 1,
                "reason": LIMITATIONS,
                "evidence": [*evidence, f"{REVIEW_PATH}#{label}/{digest}"],
            },
        }
        for label, digest in expected
    ]
