"""Author inventory-bound, exact-profile produced-water discharge proxies."""

import json
from pathlib import Path

FIELDS = ("name", "reference product", "location", "unit")
REVIEW_PATH = "brightpath/data/export/uvek_produced_water_review.json"
APPROVAL = json.loads((Path(__file__).resolve().parents[1] / REVIEW_PATH).read_text())
APPROVED_DISCHARGE_TREATMENTS = {}
DISCHARGE_TARGETS = {tuple(row["target"][f] for f in FIELDS): row for row in APPROVAL["targets"]}
for row in APPROVAL["rules"]:
    source = tuple(row["source"][f] for f in FIELDS)
    target = tuple(row["previous_target"][f] for f in FIELDS)
    onshore = source[1].endswith("onshore")
    limitations = (
        "Approved produced-water discharge screening proxy: preserve discharged water mass with factor -1, "
        "without concentration-based scaling. UVEK substitutes its own discharge-emission inventory. "
        "Ecoinvent 3.10--3.12 has dissolved organic carbon 0.02003 kg/kg versus UVEK 0.000295 kg/kg; "
        "UVEK uses chromium VI where ecoinvent 3.9--3.12 uses chromium III. "
        "Older ecoinvent 3.6--3.8 air and water-balance exchanges are absent from the proxy. "
        + (
            "UVEK extrapolates offshore measurements to onshore surface-water discharge. "
            "Its chloride inventory is 0.071775 kg/kg versus ecoinvent 3.10--3.12 onshore 1.2e-7 kg/kg. "
            if onshore
            else "UVEK OCE represents the source global offshore ocean-discharge service. "
        )
        + "Pollutant concentrations, speciation, geography and emission coverage differ; LCIA equivalence, "
        "including toxicity scores, is not established or quantified. This represents discharge after "
        "pre-treatment; no additional wastewater-treatment plant or recovered-product credit is inferred. "
        "Caller-owned production, matching foreground suppliers and separately listed emissions are preserved. "
        "Only the enumerated exact profiles are approved; no reverse reconstruction is inferred."
    )
    APPROVED_DISCHARGE_TREATMENTS[source] = {
        "target": target,
        "previous_target": target,
        "source_versions": ("3.6", "3.7", "3.8", "3.9", "3.10", "3.11", "3.12"),
        "native_product_service": True,
        "reference_function": "produced-water discharge service",
        "profiles": row["profiles"],
        "rationale": (
            "Approved by the user: map this exact produced-water discharge reference directly to the "
            "corresponding UVEK onshore/offshore discharge service with factor -1, converting the source "
            "negative waste reference to the positive service reference. No helper dataset is needed. " + limitations
        ),
        "limitations": limitations,
        "inventory_evidence": [
            f"{REVIEW_PATH}: 56 exact source/profile fingerprints and two inspected UVEK supplier fingerprints. "
            "All 28 source-market inventories supply only -1 kg of the corresponding direct discharge "
            "service, with no other active non-production exchanges. Direct services have -1 kg "
            "reference production. The UVEK services have +1 kg reference production, 55 active "
            "water-emission exchanges and no technosphere inputs. Native PRODUCT_FLOW output metadata "
            "describes a physical discharge service. Sparse evidence only; no full inventories are published."
        ],
    }


def discharge_profiles(source, observations, inventories, evidence):
    """Reject altered source/target inventories before issuing exact-profile decisions."""
    approval = APPROVED_DISCHARGE_TREATMENTS[source]
    expected = sorted((p["label"], p["inventory_sha256"]) for p in approval["profiles"])
    target = approval["target"]
    if (
        sorted(observations) != [(label, -1) for label, _ in expected]
        or sorted(inventories[source]) != expected
        or inventories[target] != [("uvek-2025-cutoff", DISCHARGE_TARGETS[target]["inventory_sha256"])]
    ):
        raise ValueError("Produced-water discharge inventory or profile changed; repeat the scientific review.")
    profiles = []
    for label, digest in expected:
        family, version, model = label.split("-")
        profiles.append(
            {
                "source_profile": {"family": family, "version": version, "system_model": model},
                "conversion_factor": -1,
                "review": {
                    "status": "verified",
                    "source_sign": -1,
                    "target_sign": 1,
                    "reason": approval["limitations"],
                    "evidence": [*evidence, f"{REVIEW_PATH}#{label}/{digest}"],
                },
            }
        )
    return profiles
