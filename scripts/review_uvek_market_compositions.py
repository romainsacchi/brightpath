"""Compose exact inspected markets without inventing a new component correspondence."""

import hashlib
import json
from copy import deepcopy
from pathlib import Path

from brightpath import TechnosphereProfile
from brightpath.background.reference_signs import select_profile_sign_rules

FIELDS = ("name", "reference product", "location", "unit")
REVIEW_PATH = "brightpath/data/export/uvek_market_composition_review.json"
APPROVAL = json.loads((Path(__file__).resolve().parents[1] / REVIEW_PATH).read_text())
# These original decisions remain in the historical ledger. Their exact cut-off
# inventories now have complete waste-specific treatment/freight reconstructions
# in review_uvek_steel_residue_markets; consequential profiles are excluded.
SUPERSEDED_STEEL_MARKETS = {
    ("market for basic oxygen furnace dust", "basic oxygen furnace dust", "IN", "kilogram"),
    ("market for blast furnace sludge", "blast furnace sludge", "IN", "kilogram"),
    ("market for blast furnace sludge", "blast furnace sludge", "CH", "kilogram"),
}
# These material markets now bind their positive allocated stock to a dedicated
# production-only supplier. Keep their original fingerprints for historical replay;
# review_uvek_digester_stock validates the corrected stock and retained freight.
SUPERSEDED_DIGESTER_MARKETS = {
    ("market for digester sludge", "digester sludge", location, "kilogram") for location in ("CH", "GLO", "RoW")
}
# Retain the complete historical collection scope for fingerprint replay.
COMPOSITIONS = {tuple(r["source"][f] for f in FIELDS): r for r in APPROVAL["rules"]}

LIMITATIONS = (
    "Reviewed market-composition helper: retain the inspected active supply mix and freight quantities, "
    "using only the existing resolved directional component mappings for this exact source profile. "
    "The source has a unit-magnitude reference, active technosphere inputs only and no direct biosphere "
    "exchanges. Apply each component's reference-sign factor independently: negative inputs to negative "
    "treatment references are treatment demand, whereas negative inputs to positive product references "
    "are recovery credits. No component omission, empty allocation or missing-provider fallback is used. "
    "These helpers inherit the component correspondences' heuristic, technology, composition, geography "
    "and allocation approximations; resolved signs do not establish scientific or LCIA equivalence. "
    "Internal component uncertainty and parameter formulas are not reconstructed. Preserve caller-owned "
    "signed quantities, uncertainty, formulas, foreground suppliers, production and separate emissions. "
    "Score and export all returned helpers. Missing or changed exact profiles require a new review; "
    "no reverse correspondence is inferred."
)


def composition_recipe(
    source, profile, *, rationale=LIMITATIONS, review_path=REVIEW_PATH, namespace="market-composition"
):
    """Represent a market by its already mapped target components, without normalization."""
    rationale = profile.get("rationale", rationale)
    namespace = profile.get("recipe_namespace", namespace)
    if not isinstance(namespace, str) or not namespace or not namespace.strip():
        raise ValueError("A reviewed recipe namespace must be a nonempty string.")
    inputs = deepcopy(profile["inputs"])
    if not inputs:
        raise ValueError("An unreviewed empty market cannot inherit composition approval.")
    identity = [source, inputs]
    if "source_input_omissions" in profile:
        identity.append(profile["source_input_omissions"])
    digest = hashlib.sha256(json.dumps(identity, sort_keys=True).encode()).hexdigest()[:20]
    recipe = {
        "name": f"{source[0]}, composed from resolved suppliers, {source[2]}",
        "reference product": source[1],
        "location": source[2],
        "unit": source[3],
        "inputs": inputs,
        "rationale": rationale,
        "evidence": [
            f"{review_path}: exact source inventory and component-mapping fingerprints; derived target coefficients only.",
            *profile.get("additional_evidence", []),
        ],
    }
    if "source_input_omissions" in profile:
        recipe["source_input_omissions"] = deepcopy(profile["source_input_omissions"])
    return f"{namespace}-{digest}", recipe


def validate_composition_bindings(resource, *, approval=APPROVAL, label="Market-composition"):
    """Freeze existing component rules, including exact-profile decisions and loss policies."""
    for key, digest in approval["recipe_dependencies"].items():
        if hashlib.sha256(json.dumps(resource["recipes"].get(key), sort_keys=True).encode()).hexdigest() != digest:
            raise ValueError(f"{label} recipe dependency changed; repeat the scientific review.")
    by_label = {}
    for row in approval["rules"]:
        if approval is APPROVAL and tuple(row["source"][f] for f in FIELDS) in (
            SUPERSEDED_STEEL_MARKETS | SUPERSEDED_DIGESTER_MARKETS
        ):
            continue
        for profile in row["profiles"]:
            by_label.setdefault(profile["label"], {}).update(
                {tuple(b["source"]): b["binding_sha256"] for b in profile["bindings"]}
            )
    for profile_label, bindings in by_label.items():
        selected = select_profile_sign_rules(resource, TechnosphereProfile(*profile_label.split("-")))
        rules = {tuple(r["source"][f] for f in FIELDS): r for r in selected["replace"]}
        for source, digest in bindings.items():
            if hashlib.sha256(json.dumps(rules.get(source), sort_keys=True).encode()).hexdigest() != digest:
                raise ValueError(f"{label} component binding changed; repeat the scientific review.")


def composition_decision(
    rule,
    observations,
    inventories,
    binding,
    original_review,
    *,
    approval_row=None,
    rationale=LIMITATIONS,
    review_path=REVIEW_PATH,
    namespace="market-composition",
    mapping_method="reviewed_market_composition",
    profile_reason="Select an exact inspected market profile; no unreviewed component or profile fallback is approved.",
):
    """Compile only complete, independently inspected exact-profile market compositions."""
    source = tuple(rule["source"][f] for f in FIELDS)
    approval = COMPOSITIONS[source] if approval_row is None else approval_row
    if approval.get("authorized_system_model") == "cutoff":
        observations = {(label, sign) for label, sign in observations if label.endswith("-cutoff")}
        inventories = [(label, sha) for label, sha in inventories if label.endswith("-cutoff")]
    original_factor = approval.get("original_factor", 1)
    all_profiles = [*approval["profiles"], *approval.get("pending_profiles", [])]
    expected = sorted((p["label"], p["inventory_sha256"]) for p in all_profiles)
    expected_signs = sorted((p["label"], p["source_sign"]) for p in all_profiles)
    if (
        rule["target"] != approval["previous_target"]
        or rule.get("conversion_factor", 1) != original_factor
        or sorted(observations) != expected_signs
        or sorted(inventories) != expected
    ):
        raise ValueError("Market-composition supplier, inventory or profile changed; repeat the scientific review.")
    profiles, recipes = [], {}
    for p in approval["profiles"]:
        family, version, model = p["label"].split("-")
        recipe_id, recipe = composition_recipe(
            source, p, rationale=rationale, review_path=review_path, namespace=namespace
        )
        recipes[recipe_id] = recipe
        profiles.append(
            {
                "source_profile": {"family": family, "version": version, "system_model": model},
                "target": {"recipe_ref": recipe_id},
                "conversion_factor": p["source_sign"],
                "review": {
                    "status": "verified",
                    "source_sign": p["source_sign"],
                    "target_sign": 1,
                    "reason": p.get("rationale", rationale),
                    "evidence": [f"{review_path}#{p['label']}/{p['inventory_sha256']}"],
                },
            }
        )
    for p in approval.get("pending_profiles", []):
        profiles.append(
            {
                "source_profile": dict(zip(("family", "version", "system_model"), p["label"].split("-"), strict=True)),
                "review": {
                    "status": "unresolved",
                    "source_sign": p["source_sign"],
                    "target_sign": 1,
                    "reason": "This source inventory/profile has unreviewed components, direct biosphere flows or an unapproved allocation/treatment boundary; no inferred counterpart is approved.",
                    "evidence": [f"{review_path}#{p['label']}/{p['inventory_sha256']}"],
                },
            }
        )
    if not approval["profiles"]:
        raise ValueError("An assembly needs at least one independently approved exact profile.")
    review_rationale = approval.get("review_rationale", rationale)
    factor = profiles[0]["conversion_factor"]
    return {
        "operation": "recipe",
        "binding": binding,
        "original_rule": deepcopy(rule),
        "original_review": deepcopy(original_review),
        "original_factor": original_factor,
        "factor": factor,
        "source_observations": [list(r) for r in sorted(observations)],
        "uvek_reference_sign": 1,
        "uvek_native_flow_type": approval.get("generated_flow_type", "GENERATED_SERVICE"),
        "original_native_flow_type": approval["previous_native_flow_type"],
        "target": deepcopy(profiles[0]["target"]),
        "recipes": recipes,
        "recipe_review": {"rationale": review_rationale, "limitations": [review_rationale], "evidence": [review_path]},
        "recipe_information_loss": review_rationale,
        "mapping_method": mapping_method,
        "review": {
            "status": "unresolved",
            "reason": profile_reason,
            "evidence": [review_path],
            "profiles": profiles,
        },
    }
