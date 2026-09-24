"""Explicit identity-preserving patch routes, with exact target validation."""

import json
from functools import lru_cache

from brightpath import DATA_DIR
from brightpath.migrations.resources import _load_resource_manifest, _verify_manifest_entry


@lru_cache(maxsize=1)
def compatibility_resource():
    path = DATA_DIR / "migrations/compatibility/patch-links.json"
    raw = path.read_bytes()
    resource = json.loads(raw)
    _verify_manifest_entry(path, raw, resource, _load_resource_manifest())
    if resource.get("schema_version") != 1 or resource.get("patches") != {"3.9": "3.9.1", "3.10": "3.10.1"}:
        raise ValueError("Invalid patch compatibility resource.")
    seen = set()
    for rule in resource.get("reverse_proxies", []):
        source = rule["source"]
        target = rule["target"]
        fields = {"name", "reference product", "location", "unit"}
        key = (rule["source_version"], rule["target_version"], tuple(sorted(source.items())))
        if set(source) != fields or set(target) != fields or key in seen:
            raise ValueError("Malformed or duplicate reverse proxy.")
        if source["unit"] != target["unit"] or not rule["rationale"] or not rule["evidence"]:
            raise ValueError("Reverse proxies require unchanged units and documented evidence.")
        seen.add(key)
    return resource


PATCH_VERSIONS = {"3.9": "3.9.1", "3.10": "3.10.1"}


def patch_pair(source, target):
    compatibility_resource()
    return PATCH_VERSIONS.get(source) == target or PATCH_VERSIONS.get(target) == source


def patch_resource(source, target, axis):
    if not patch_pair(source, target):
        raise ValueError("No reviewed patch route for these exact versions.")
    return {
        "name": f"ecoinvent-{source}-to-{target}-{axis}-identity-validation",
        "replace": [],
        "disaggregate": [],
        "delete": [],
    }
