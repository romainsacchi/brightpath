"""Replay the user-reviewed direct European grid in wool sorting."""

import hashlib
import json
from copy import deepcopy
from pathlib import Path

from brightpath.migrations.schema import intern_review, rule_review

ROOT = Path(__file__).resolve().parents[1]
REVIEW_PATH = "brightpath/data/export/uvek_wool_europe_direct_grid_review.json"
REVIEW_SHA256 = "0a90ec411e28a1f2a6c7b54c26fbec53bc36fc0f2f5221ee92784ccc212d8be8"


def _digest(value):
    raw = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(raw.encode()).hexdigest()


def apply_wool_europe_direct_grid_review(resource, root=ROOT):
    """Replace one reviewed recipe edge; reject unrelated source changes."""
    raw = (root / REVIEW_PATH).read_bytes()
    if hashlib.sha256(raw).hexdigest() != REVIEW_SHA256:
        raise ValueError("Direct European wool grid evidence changed.")
    review = json.loads(raw)
    owner = review["owner_recipe_id"]
    retired = review["retired_recipe_id"]
    current = resource["recipes"][owner]
    if current == review["compiled_recipe"]:
        if retired in resource["recipes"]:
            raise ValueError("Retired wool grid helper remains after simplification.")
        for row in review["rules"]:
            rule = next(r for r in resource["replace"] if r["id"] == row["rule_id"])
            if rule != row["compiled_rule"] or rule_review(resource, rule) != row["review"]:
                raise ValueError("Direct European wool parent changed after review.")
        return
    if (
        _digest(current) != review["original_recipe_sha256"]
        or _digest(resource["recipes"][retired]) != review["retired_recipe_sha256"]
        or review["native_reference_amount"] != 1
    ):
        raise ValueError("European wool recipe or grid reference changed before review.")
    before = deepcopy(current)
    after = deepcopy(review["compiled_recipe"])
    old_input = before["inputs"][review["input_index"]]
    new_input = after["inputs"][review["input_index"]]
    old_stats = {key: value for key, value in old_input.items() if key != "recipe"}
    new_stats = {
        key: value for key, value in new_input.items() if key not in review["native_supplier"] and key != "type"
    }
    if (
        old_input.get("recipe") != retired
        or new_stats != old_stats
        or any(new_input.get(k) != v for k, v in review["native_supplier"].items())
        or before["inputs"][: review["input_index"]] != after["inputs"][: review["input_index"]]
        or before["inputs"][review["input_index"] + 1 :] != after["inputs"][review["input_index"] + 1 :]
    ):
        raise ValueError("Direct European wool grid changes source quantities or other inputs.")
    parents = []
    for row in review["rules"]:
        rule = next(r for r in resource["replace"] if r["id"] == row["rule_id"])
        if (
            _digest(rule) != row["original_rule_sha256"]
            or _digest(rule_review(resource, rule)) != row["original_review_sha256"]
        ):
            raise ValueError("European wool parent changed before grid simplification.")
        parents.append((rule, row))
    resource["recipes"][owner] = after
    del resource["recipes"][retired]
    for rule, row in parents:
        if intern_review(resource, row["review"]) != row["compiled_rule"]["review_ref"]:
            raise ValueError("Direct European wool review identity changed.")
        rule.clear()
        rule.update(deepcopy(row["compiled_rule"]))
