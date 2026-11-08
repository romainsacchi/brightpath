"""Author matching-material regional landfill and legacy vehicle dismantling proxies."""

import json
from pathlib import Path

FIELDS = ("name", "reference product", "location", "unit")
REVIEW_PATH = "brightpath/data/export/uvek_dedicated_disposal_review.json"
APPROVAL = json.loads((Path(__file__).resolve().parents[1] / REVIEW_PATH).read_text())
TARGETS = {tuple(r["target"][f] for f in FIELDS): r for r in APPROVAL["targets"]}
BOUNDARIES = {
    "alloyed-dust": (
        "Use the dedicated alloyed/chromium EAF dust residual-material landfill. Source H2O is 105410 ppm "
        "(10.541%); the target specifies 15.4% water. Both include cement solidification. The target also "
        "includes freight which can overlap separately represented transport. "
    ),
    "unalloyed-dust": (
        "Use the dedicated unalloyed EAF dust residual-material landfill, preserving its distinction from "
        "alloyed dust. Source H2O is 105410 ppm (10.541%); the target specifies 15.4% water. "
    ),
    "unalloyed-slag": (
        "Use the dedicated unalloyed electric-steel slag residual-material landfill. Source H2O is "
        "unavailable; the target specifies 0% water. Do not substitute a slag-production coproduct. "
    ),
    "pet-landfill": (
        "Use the dedicated sanitary-landfill service for PET, including the published legacy 'xx' name, "
        "instead of polyester-fleece production. Both specify PET with 0.2% water. UVEK's CH landfill "
        "approximates RoW climate, gas capture/utilisation, leachate treatment and long-term emissions. "
        "The source's electricity and heat recovery-credit model is not reconstructed. "
    ),
    "scooter-glider": (
        "Use the dedicated shredding service for a scooter glider without battery and drivetrain. "
        "Both inventories describe the same approximately 80 kg example glider but their references "
        "are per kg: do not multiply or divide by the example device weight. "
    ),
    "scooter-powertrain": (
        "Use the dedicated manual dismantling service for the scooter drivetrain comprising a motor, "
        "controller and charger; remaining electronics go to a smelter. The reference is per kg, "
        "not per drivetrain or component. The source's metal-recovery credits are not reconstructed. "
    ),
    "passenger-car-engine": (
        "Use the dedicated shredding service for the used internal-combustion engine of a passenger car. "
        "Both describe an approximately 275 kg example engine but their references are per kg: no "
        "device-weight scaling applies. The source's metal-recovery credits are not reconstructed. "
    ),
    "formox-carrier": (
        "Use the dedicated residual-material-landfill service for the spent Formox catalyst carrier "
        "after metal recycling from formaldehyde (CH2O) production. This is the unrecycled matrix, not "
        "catalyst manufacture, metal recovery or steam production. The CH service approximates RER. "
        "Source moisture is unavailable versus the target's 0% label; preserve original wet mass. "
        "Both functions exclude additional cement solidification. The source has 24 active biosphere "
        "releases versus eight in UVEK: waste-specific ions/metals, speciation, leaching, hydrology, "
        "time horizons and infrastructure differ; no emission-coverage or toxicity equivalence is claimed. "
    ),
}
DISPOSALS = {}
for row in APPROVAL["rules"]:
    source = tuple(row["source"][f] for f in FIELDS)
    target = tuple(row["target"][f] for f in FIELDS)
    if source in DISPOSALS or target not in TARGETS or row["kind"] not in BOUNDARIES:
        raise ValueError("Duplicate or unrecognized dedicated-disposal approval.")
    limitations = (
        "Approved matching-material/function regional disposal screening proxy: "
        + BOUNDARIES[row["kind"]]
        + "Apply factor -1 and preserve incoming wet kg without moisture, dry-mass or heating-value scaling. "
        "The inspected CH/RER service represents RoW/GLO where applicable. Plant technology, electricity, "
        "freight, composition, metal speciation, leaching, short- and long-term releases, recovery yields "
        "and allocation models can differ; scientific and LCIA equivalence are not established. Dedicated "
        "disposal replaces neither recovered-material production nor source-specific substitution credits. "
        "Preserve caller-owned signed quantities, uncertainty, formulas, production, matching foreground "
        "suppliers, separate credits, transport and foreground emissions. Only enumerated exact profiles "
        "are approved; missing and patch profiles remain blocked. No reverse correspondence is inferred."
    )
    DISPOSALS[source] = {
        "target": target,
        "previous_target": tuple(row["previous_target"][f] for f in FIELDS),
        "source_versions": tuple(sorted({p["label"].split("-")[1] for p in row["profiles"]})),
        "profiles": row["profiles"],
        "rationale": row.get(
            "approval_basis",
            "Auto-approved under the independently inspected matching-material/function/unit disposal, "
            "vehicle-dismantling and geographic-proxy authorizations. Use the dedicated disposal service "
            "instead of material or equipment manufacture. ",
        )
        + limitations,
        "limitations": limitations,
        "inventory_evidence": row.get(
            "inventory_evidence",
            [
                f"{REVIEW_PATH}: seven exact treatment identities, 32 independently inspected source/profile "
                "fingerprints and seven independently inspected UVEK supplier fingerprints. Every source has "
                "single self-linked production -1 kg and an active treatment inventory; each target has "
                "+1 kg, active treatment inputs and a separately verified native WASTE_FLOW input reference. "
                "Regional legacy EAF dust/slag are limited to 3.6; legacy vehicle processes to 3.6 and 3.7; "
                "PET landfill to inspected 3.6--3.12 profiles. Full licensed inventories are not included."
            ],
        ),
    }
if (
    len(DISPOSALS) != APPROVAL["source_count"]
    or sum(len(r["profiles"]) for r in DISPOSALS.values()) != APPROVAL["source_profile_count"]
):
    raise ValueError("Dedicated-disposal approval scope disagrees with its evidence.")


def dedicated_disposal_profiles(source, observations, inventories, evidence):
    """Require unchanged source and target inventories and exact source-profile coverage."""
    approval = DISPOSALS[source]
    expected = sorted((p["label"], p["inventory_sha256"]) for p in approval["profiles"])
    target = approval["target"]
    if (
        sorted(observations) != [(label, -1) for label, _ in expected]
        or sorted(inventories[source]) != expected
        or inventories[target] != [("uvek-2025-cutoff", TARGETS[target]["inventory_sha256"])]
    ):
        raise ValueError("Dedicated-disposal inventory or profile changed; repeat the scientific review.")
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
