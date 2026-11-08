"""Content-bound approvals for individual compatibility-resource operations."""

from dataclasses import asdict
import hashlib
import json

from brightpath.core.context import TechnosphereProfile
from brightpath.migrations.schema import rule_review


def _references(value, key):
    if isinstance(value, dict):
        for field, item in value.items():
            if field == key:
                yield item
            else:
                yield from _references(item, key)
    elif isinstance(value, (list, tuple)):
        for item in value:
            yield from _references(item, key)


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
    # Profile-specific target overrides are part of the approval too.
    pending = list(_references(rule, "recipe_ref"))
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
    references = set(_references(rule, "review_ref")) | set(_references(recipes, "review_ref"))
    references.discard(rule.get("review_ref"))
    if references:
        payload["referenced_reviews"] = {key: resource["reviews"][key] for key in sorted(references)}
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)
    return hashlib.sha256(raw.encode()).hexdigest()
