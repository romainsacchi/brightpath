"""Content-bound approvals for individual compatibility-resource operations."""

from dataclasses import asdict
import hashlib
import json

from brightpath.core.context import TechnosphereProfile
from brightpath.migrations.schema import rule_review


def compatibility_rule_fingerprint(resource, operation, rule, source, target):
    """Bind approval to an exact profile pair, operation, rule and shared review.

    Computing this fingerprint does not establish scientific approval. Callers
    must obtain their allowed fingerprints from independently reviewed evidence.
    Rule IDs alone are insufficient: suppliers, factors or reviews can change.
    """
    if operation not in {"replace", "disaggregate", "delete"}:
        raise ValueError("Unsupported compatibility-rule operation.")
    if not isinstance(source, TechnosphereProfile) or not isinstance(target, TechnosphereProfile):
        raise TypeError("Compatibility-rule scopes require exact TechnosphereProfile values.")
    payload = {
        "resource": resource["name"],
        "axis": resource["axis"],
        "operation": operation,
        "source": asdict(source),
        "target": asdict(target),
        "rule": rule,
        "review": rule_review(resource, rule),
    }
    targets = [rule.get("target", {}), *rule.get("targets", ())]
    pending = [target["recipe_ref"] for target in targets if "recipe_ref" in target]
    recipes = {}
    while pending:
        recipe_id = pending.pop()
        if recipe_id in recipes:
            continue
        recipe = resource["recipes"][recipe_id]
        recipes[recipe_id] = recipe
        pending.extend(exchange["recipe"] for exchange in recipe["inputs"] if "recipe" in exchange)
    if recipes:
        payload["recipes"] = recipes
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)
    return hashlib.sha256(raw.encode()).hexdigest()
