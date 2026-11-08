"""Author dedicated LCD treatment proxies under inspected display-service approvals."""

import json
from pathlib import Path

FIELDS = ("name", "reference product", "location", "unit")
REVIEW_PATH = "brightpath/data/export/uvek_lcd_treatment_review.json"
APPROVAL = json.loads((Path(__file__).resolve().parents[1] / REVIEW_PATH).read_text())
TARGETS = {tuple(row["target"][f] for f in FIELDS): row for row in APPROVAL["targets"]}
LCD_TREATMENTS = {}
for row in APPROVAL["rules"]:
    source = tuple(row["source"][f] for f in FIELDS)
    target = tuple(row["target"][f] for f in FIELDS)
    if (
        source in LCD_TREATMENTS
        or target not in TARGETS
        or row["kind"]
        not in (
            "manual",
            "mechanical",
            "shredder-incineration",
        )
    ):
        raise ValueError("Duplicate or unrecognized dedicated LCD treatment approval.")
    boundary = {
        "manual": (
            "Use the dedicated manual LCD dismantling service, not generic industrial-device dismantling "
            "or an empty recovered-aluminium fraction. The source and proxy both describe manual separation "
            "of used LCD screens into material fractions. The inspected CH service represents RoW where applicable. "
        ),
        "mechanical": (
            "Use the dedicated manual-plus-mechanical LCD dismantling service instead of mercury production. "
            "Ecoinvent 3.6--3.9 descriptions include manual dismantling before shredding/separation. Inspected "
            "3.10--3.12 descriptions specify a four-step mechanical procedure and no separate manual-service "
            "input; the proxy's broader manual-plus-mechanical boundary follows the independently approved "
            "similar display-treatment approximation. It can add manual-treatment burdens or overlap with "
            "separately modelled manual dismantling, which remains unchanged. "
        ),
        "shredder-incineration": (
            "Use the dedicated LCD shredder-residue municipal-incineration service instead of methane "
            "production from biological methanation. Both descriptions identify LCD mechanical-treatment "
            "residues with 1.95% rubber and predominantly electronic plastics. Whole LCD modules, glass "
            "and CRT coatings remain separate functions. The CH service represents RoW where applicable. "
        ),
    }[row["kind"]]
    limitations = (
        "Approved dedicated LCD treatment screening proxy: "
        + boundary
        + "Preserve incoming wet kg with factor -1 and no moisture, heating-value or device-count scaling. "
        "Regional fractions, yields, waste composition, emission controls, fly-ash extraction, downstream "
        "disposal, metal speciation, leaching, energy recovery, allocation and consequential product-credit "
        "models can differ. The source-specific credit model is not reconstructed. Toxicity and other LCIA "
        "equivalence are not established or quantified. Preserve caller-owned signed quantities, uncertainty, "
        "formulas, production, matching foreground suppliers, separate credits and foreground emissions. "
        "Only enumerated exact profiles are approved; missing and patch profiles remain blocked. "
        "No reverse correspondence is inferred."
    )
    LCD_TREATMENTS[source] = {
        "target": target,
        "previous_target": tuple(row["previous_target"][f] for f in FIELDS),
        "source_versions": tuple(
            sorted(
                {p["label"].split("-")[1] for p in row["profiles"]},
                key=lambda v: tuple(int(x) for x in v.split(".")),
            )
        ),
        "profiles": row["profiles"],
        "rationale": (
            "Auto-approved under the independently inspected similar display-dismantling, broader "
            "manual-plus-mechanical boundary and dedicated display-residue incineration authorizations: "
            f"map the exact {source[0]} / {source[2]} / kg reference to its dedicated LCD service with "
            "factor -1. This does not authorize CRT material substitutions or arbitrary WEEE proxies. " + limitations
        ),
        "limitations": limitations,
        "inventory_evidence": [
            f"{REVIEW_PATH}: five exact LCD references, 50 source/profile fingerprints and three independently "
            "inspected UVEK suppliers. Sources have self-linked production -1 kg; targets have +1 kg and native "
            "WASTE_FLOW input references. Source dismantling services have no active direct biosphere flows; "
            "residue incinerators have active treatment inputs and emissions. Exact mechanical profiles record "
            "manual-stage documentation and absence of a separate manual supplier. No full licensed inventory "
            "is included."
        ],
    }
if (
    len(LCD_TREATMENTS) != APPROVAL["source_count"]
    or sum(len(a["profiles"]) for a in LCD_TREATMENTS.values()) != APPROVAL["source_profile_count"]
):
    raise ValueError("Dedicated LCD treatment approval scope disagrees with its evidence.")


def lcd_profiles(source, observations, inventories, evidence):
    """Require unchanged inventories and exact-profile coverage before issuing a decision."""
    approval = LCD_TREATMENTS[source]
    target = approval["target"]
    expected = sorted((p["label"], p["inventory_sha256"]) for p in approval["profiles"])
    if (
        sorted(observations) != [(label, -1) for label, _ in expected]
        or sorted(inventories[source]) != expected
        or inventories[target] != [("uvek-2025-cutoff", TARGETS[target]["inventory_sha256"])]
    ):
        raise ValueError("LCD treatment inventory or profile changed; repeat the scientific review.")
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
