"""Author exact-profile legacy EAF-waste landfill proxies with material-specific warnings."""

import json
from pathlib import Path

FIELDS = ("name", "reference product", "location", "unit")
REVIEW_PATH = "brightpath/data/export/uvek_eaf_landfill_review.json"
APPROVAL = json.loads((Path(__file__).resolve().parents[1] / REVIEW_PATH).read_text())
TARGETS = {tuple(row["target"][f] for f in FIELDS): row for row in APPROVAL["targets"]}
LANDFILLS = {}
for row in APPROVAL["rules"]:
    source = tuple(row["source"][f] for f in FIELDS)
    target = tuple(row["target"][f] for f in FIELDS)
    if (
        source in LANDFILLS
        or target not in TARGETS
        or row["moisture"]
        not in (
            "dust-105410-ppm-vs-15.4-percent",
            "source-water-unavailable-vs-0-percent",
        )
    ):
        raise ValueError("Duplicate or unrecognized legacy EAF landfill approval.")
    limitations = (
        "Approved matching-material Swiss residual-material-landfill screening proxy: replace the former "
        "allocated slag-production supplier with the dedicated disposal service for this exact waste type. "
        "Apply factor -1 to preserve incoming waste mass against the positive UVEK treatment reference. "
        + (
            "Source dust documents H2O 105410 ppm (10.541%); UVEK names specify 15.4% water. "
            if row["moisture"] == "dust-105410-ppm-vs-15.4-percent"
            else "Source slag documentation reports H2O unavailable; UVEK names specify 0% water. "
        )
        + "Preserve the original kg without moisture or dry-mass scaling; equal composition is not established. "
        + (
            "Both alloyed-dust processes include cement stabilization and hydrated-cement disposal. UVEK adds "
            "road and rail freight absent from the source service; its broader freight boundary may overlap "
            "separately represented foreground transport, which remains unchanged. "
            if row["alloyed_dust"]
            else "The proxy substitutes its own landfill operation and infrastructure inventory. "
        )
        + "Metal concentrations, speciation, leaching, short- and long-term water releases, cement handling, "
        "background supplies and allocation models can differ; toxicity and other LCIA equivalence are not "
        "established or quantified. No recovered-material production or substitution credit is inferred. "
        "Preserve caller uncertainty, formulas, production, foreground suppliers and separate emissions. "
        "Only the enumerated exact ecoinvent 3.6 profiles are approved. Other versions, patch profiles, "
        "alloy grades, waste types, markets and reverse routes require independent review."
    )
    LANDFILLS[source] = {
        "target": target,
        "previous_target": tuple(row["previous_target"][f] for f in FIELDS),
        "source_versions": ("3.6",),
        "profiles": row["profiles"],
        "rationale": (
            "Auto-approved under the independently inspected matching-material/function/unit disposal-service "
            "proxy authorization: use the corresponding CH/kg UVEK landfill service for the exact legacy EAF "
            "dust or slag treatment, preserving incoming wet waste mass with factor -1. No helper is needed. "
            + limitations
        ),
        "limitations": limitations,
        "inventory_evidence": [
            f"{REVIEW_PATH}: six independently inspected ecoinvent 3.6 cut-off/consequential source inventories "
            "and three UVEK supplier fingerprints. Each source has single self-linked production -1 kg, active "
            "landfill inputs and releases. Each target has single self-linked production +1 kg and independently "
            "verified native WASTE_FLOW input reference for the corresponding disposal function. Full licensed "
            "inventories are not included."
        ],
    }
if (
    len(LANDFILLS) != APPROVAL["source_count"]
    or sum(len(r["profiles"]) for r in LANDFILLS.values()) != APPROVAL["source_profile_count"]
):
    raise ValueError("Legacy EAF landfill approval scope disagrees with its evidence.")


def landfill_profiles(source, observations, inventories, evidence):
    """Require unchanged inventories and exact source-profile coverage before replay."""
    approval = LANDFILLS[source]
    target = approval["target"]
    expected = sorted((p["label"], p["inventory_sha256"]) for p in approval["profiles"])
    if (
        sorted(observations) != [(label, -1) for label, _ in expected]
        or sorted(inventories[source]) != expected
        or inventories[target] != [("uvek-2025-cutoff", TARGETS[target]["inventory_sha256"])]
    ):
        raise ValueError("Legacy EAF landfill inventory or profile changed; repeat the scientific review.")
    return [
        {
            "source_profile": dict(zip(("family", "version", "system_model"), label.split("-"), strict=True)),
            "conversion_factor": -1,
            "review": {
                "status": "verified",
                "source_sign": -1,
                "target_sign": 1,
                "reason": approval["limitations"],
                "evidence": [*evidence, f"{REVIEW_PATH}#{label}/{digest}"],
            },
        }
        for label, digest in expected
    ]
