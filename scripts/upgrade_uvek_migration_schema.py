"""Upgrade the two historical UVEK technosphere layouts to the shared v3 schema.

This is a maintainer data-upgrade utility, not a runtime compatibility layer.
"""

from __future__ import annotations

import argparse
import json
from copy import deepcopy
from pathlib import Path

from brightpath.migrations.schema import (
    MIGRATION_SCHEMA,
    dumps_migration_resource,
    intern_review,
    normalize_migration_resource,
)


def upgrade_resource(payload: dict) -> dict:
    """Return a shared-schema resource without changing existing mapping evidence."""
    if payload.get("schema") == MIGRATION_SCHEMA or payload.get("schema_version", 1) == 1:
        return normalize_migration_resource(payload)
    if payload.get("schema_version") != 2:
        raise ValueError("Expected a historical UVEK v2 resource.")
    resource = deepcopy(payload)
    resource["schema_version"] = 1
    if "rules" in resource:
        resource["replace"] = resource.pop("rules")
        resource["reviews"] = {}
        for rule in resource["replace"]:
            if "factor" in rule:
                rule["conversion_factor"] = rule.pop("factor")
            if "recipe" in rule:
                rule["target"] = {"recipe_ref": rule.pop("recipe")}
            review = {key: rule.pop(key) for key in ("rationale", "evidence")}
            rule["review_ref"] = intern_review(resource, review)
        resource["coverage"]["mapped_identities"] = resource["coverage"].pop("approved_identities")
    else:
        targets = resource.pop("targets")
        reviews = resource.pop("reviews")
        for rule in resource["replace"]:
            rule["target"] = deepcopy(targets[rule["target"]])
            if "review_provenance" in rule:
                rule["review_provenance"] = deepcopy(reviews[rule["review_provenance"]])
    return normalize_migration_resource(resource)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", type=Path)
    parser.add_argument("destination", type=Path)
    args = parser.parse_args()
    resource = upgrade_resource(json.loads(args.source.read_text(encoding="utf-8")))
    args.destination.write_text(dumps_migration_resource(resource), encoding="utf-8")


if __name__ == "__main__":
    main()
