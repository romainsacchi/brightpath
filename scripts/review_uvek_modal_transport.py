"""Author independently inspected rail/barge proxies without changing signed tkm."""

import json
from copy import deepcopy
from pathlib import Path

FIELDS = ("name", "reference product", "location", "unit")
REVIEW_PATH = "brightpath/data/export/uvek_modal_transport_review.json"
APPROVAL = json.loads((Path(__file__).resolve().parents[1] / REVIEW_PATH).read_text())
TARGETS = {tuple(row["target"][f] for f in FIELDS): row for row in APPROVAL["targets"]}
TRANSPORTS = {tuple(row["source"][f] for f in FIELDS): row for row in APPROVAL["rules"]}
for source, row in TRANSPORTS.items():
    expected = {"barge": "Transport, barge", "rail": "Transport, freight, rail"}.get(row["mode"])
    if (
        expected is None
        or row["target"]["name"] != expected
        or row["target"]["reference product"] != expected
        or row["target"]["location"] != "RER"
        or row["target"]["unit"] != source[3]
        or source[3] != "ton kilometer"
    ):
        raise ValueError("Modal transport review must retain the inspected mode, location and tkm basis.")
LIMITATIONS = (
    "Reviewed same-mode freight screening proxy: rail transport uses the whole-life RER rail service, "
    "and diesel inland-barge transport uses the whole-life RER barge service. Both source and target have "
    "positive one-ton-kilometer references; factor +1 preserves signed transport demand and genuine avoided "
    "transport credits without an absolute-value operation. Global fleet mixes are approximated by RER. "
    "Country weights, fuels, traction electricity, vehicle loading, infrastructure, fleet age, emissions and "
    "geographic representation differ; LCIA equivalence is not established or quantified. Preserve caller "
    "uncertainty, formulas, foreground suppliers and separately listed emissions. Only the enumerated exact "
    "profiles are approved; no missing-profile fallback or reverse correspondence is inferred."
)
if (
    len(TRANSPORTS) != APPROVAL["source_count"]
    or sum(len(a["profiles"]) for a in TRANSPORTS.values()) != APPROVAL["source_profile_count"]
):
    raise ValueError("Modal transport approval scope disagrees with its evidence.")


def approved_transport_rule(rule):
    """Project only an independently reviewed exact positive-reference modal correction."""
    source = tuple(rule["source"][f] for f in FIELDS)
    row = TRANSPORTS.get(source)
    if row is None:
        return rule
    if rule["target"] not in (row["previous_target"], row["target"]):
        raise ValueError("Modal transport supplier changed; repeat its scientific review.")
    return {**rule, "target": deepcopy(row["target"])}


def transport_decision(rule, observations, inventories, binding, original_review, prior=None):
    """Bind quantity-preserving modal corrections to exact source and target fingerprints."""
    prior = prior or {}
    source = tuple(rule["source"][f] for f in FIELDS)
    row = TRANSPORTS[source]
    target = tuple(row["target"][f] for f in FIELDS)
    expected = sorted((p["label"], p["inventory_sha256"]) for p in row["profiles"])
    if (
        rule["target"] != row["target"]
        or rule.get("conversion_factor", 1) != 1
        or sorted(observations) != [(label, 1) for label, _ in expected]
        or sorted(inventories[source]) != expected
        or inventories[target] != [("uvek-2025-cutoff", TARGETS[target]["inventory_sha256"])]
    ):
        raise ValueError("Modal transport inventory or exact profile changed; repeat the scientific review.")
    evidence = [f"{REVIEW_PATH}: exact source/target inventory and positive-reference evidence."]
    return {
        "binding": binding,
        "original_target": deepcopy(row["previous_target"]),
        "original_factor": 1,
        "factor": 1,
        "source_observations": [list(x) for x in sorted(observations)],
        "uvek_reference_sign": 1,
        "uvek_native_flow_type": "PRODUCT_FLOW",
        "original_review": deepcopy(prior.get("original_review", original_review)),
        "original_information_loss": prior.get("original_information_loss", rule.get("technosphere_information_loss")),
        "supplier_proxy_review": {
            "rationale": (
                f"Auto-approved under independently inspected matching-function/quantity and geographic service "
                f"proxy authorization: correct {source[0]} / {source[2]} to its matching {row['mode']} service. "
                + LIMITATIONS
            ),
            "limitations": [LIMITATIONS],
            "evidence": evidence,
        },
        "review": {
            "status": "unresolved",
            "reason": "Select only an exact inspected source profile for the same-mode transport proxy.",
            "evidence": evidence,
            "profiles": [
                {
                    "source_profile": dict(zip(("family", "version", "system_model"), label.split("-"), strict=True)),
                    "conversion_factor": 1,
                    "review": {
                        "status": "verified",
                        "source_sign": 1,
                        "target_sign": 1,
                        "reason": LIMITATIONS,
                        "evidence": [*evidence, f"{REVIEW_PATH}#{label}/{digest}"],
                    },
                }
                for label, digest in expected
            ],
        },
    }
