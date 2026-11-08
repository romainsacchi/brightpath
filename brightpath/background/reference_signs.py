"""Select independently reviewed reference coordinates for an exact source profile."""

from brightpath.core.context import TechnosphereProfile


def select_profile_sign_rules(resource: dict, source_profile: TechnosphereProfile) -> dict:
    """Return operation-local rules without changing cached resources or guessing profiles."""
    declared_models = {profile["system_model"] for profile in resource.get("source_profiles", [])}
    if declared_models and source_profile.system_model not in declared_models:
        raise ValueError(
            "The resource does not support the requested source system model; no fallback across system models is permitted."
        )
    selected_rules = []
    deletions = list(resource.get("delete", []))
    expected = {
        "family": source_profile.family,
        "version": source_profile.version,
        "system_model": source_profile.system_model,
    }
    for rule in resource.get("replace", []):
        review = rule.get("reference_flow_sign_review", {})
        if "profiles" not in review:
            selected_rules.append(rule)
            continue
        selected = next((entry for entry in review["profiles"] if entry["source_profile"] == expected), None)
        if selected is not None and selected.get("operation") == "delete":
            deletions.append(
                {
                    "id": rule["id"],
                    "source": rule["source"],
                    "approved_omission": True,
                    "review_ref": selected["review_ref"],
                }
            )
            continue
        replacement = dict(rule)
        if selected is None:
            replacement["reference_flow_sign_review"] = {
                "status": "unresolved",
                "reason": f"No reference-flow sign decision for the exact source profile {expected}; no profile fallback is permitted.",
                "evidence": list(review["evidence"]),
            }
        else:
            replacement["reference_flow_sign_review"] = dict(selected["review"])
            if selected["review"]["status"] == "verified":
                replacement["conversion_factor"] = selected["conversion_factor"]
                if "target" in selected:
                    replacement["target"] = dict(selected["target"])
        selected_rules.append(replacement)
    return {**resource, "replace": selected_rules, "delete": deletions}
