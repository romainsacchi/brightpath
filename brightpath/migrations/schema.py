"""Shared migration-resource schema, semantic validation and serialization."""

from __future__ import annotations

import hashlib
import json
import math
import re
from collections import Counter
from copy import deepcopy
from functools import lru_cache
from itertools import product

from jsonschema import Draft202012Validator

from brightpath import DATA_DIR
from brightpath.exceptions import MigrationError

MIGRATION_SCHEMA = "brightpath.migration"
MIGRATION_SCHEMA_VERSION = 3
SCHEMA_PATH = DATA_DIR / "schemas" / "migration-v3.schema.json"
IDENTITY_FIELDS = ("name", "reference product", "location", "unit")
_PROFILE_ID = re.compile(r"^(ecoinvent|uvek)-(\d+(?:\.\d+)*)-(cutoff|consequential|biosphere)$")


@lru_cache(maxsize=1)
def _validator():
    definition = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
    Draft202012Validator.check_schema(definition)
    return Draft202012Validator(definition)


def rule_review(resource: dict, rule: dict) -> dict:
    """Return an independent copy of a rule's optional shared review record."""
    return deepcopy(resource["reviews"][rule["review_ref"]]) if "review_ref" in rule else {}


def intern_review(resource: dict, review: dict) -> str:
    """Store a review once and return its deterministic content identifier."""
    canonical = json.dumps(review, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)
    reference = "r-" + hashlib.sha256(canonical.encode()).hexdigest()[:16]
    reviews = resource.setdefault("reviews", {})
    if reference in reviews and reviews[reference] != review:
        raise MigrationError("Migration review identifier collision.")
    reviews[reference] = deepcopy(review)
    return reference


def rule_identifier(resource_name: str, source: dict) -> str:
    """Return a stable rule identifier derived from its resource and source identity."""
    fields = IDENTITY_FIELDS if "reference product" in source else ("name", "categories", "unit")
    identity = {field: source[field] for field in fields}
    value = json.dumps([resource_name, identity], sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return "bp-" + hashlib.sha256(value.encode()).hexdigest()[:16]


def profile_list(profile: dict) -> list[dict]:
    """Expand a legacy multi-version profile into explicit profile declarations."""
    if not isinstance(profile, dict) or not isinstance(profile.get("family"), str) or not profile["family"]:
        raise MigrationError("A migration profile needs a background family.")
    versions = profile.get("versions", [profile.get("version")])
    models = profile.get("system_models", [profile.get("system_model")])
    if not isinstance(versions, list) or not versions or not isinstance(models, list) or not models:
        raise MigrationError("Migration profile versions and system models must be nonempty lists.")
    return [
        {
            "family": "uvek" if profile["family"].lower() == "bafu" else profile["family"].lower(),
            "version": version,
            **({"system_model": model} if model is not None else {}),
        }
        for version, model in product(versions, models)
    ]


def _id_profile(identifier: str) -> tuple[dict, str]:
    match = _PROFILE_ID.fullmatch(identifier)
    if not match:
        raise MigrationError(f"Cannot infer migration profile from {identifier!r}.")
    family, version, kind = match.groups()
    return {"family": family, "version": version, **({"system_model": kind} if kind != "biosphere" else {})}, (
        "biosphere" if kind == "biosphere" else "technosphere"
    )


def normalize_migration_resource(payload: dict) -> dict:
    """Copy and validate a v3 resource or normalize a legacy ecoinvent-style resource.

    Legacy resources keep their upstream metadata and rule bodies. Compact UVEK
    v2 storage is intentionally not a runtime format of this schema.

    :param payload: A migration resource, independent of migration direction.
    :returns: A validated schema-version-3 copy, preserving unknown metadata.
    :raises MigrationError: If its schema, profiles, rules or references are invalid.
    """
    if not isinstance(payload, dict):
        raise MigrationError("Migration resource must contain an object.")
    resource = deepcopy(payload)
    if resource.get("schema") == MIGRATION_SCHEMA:
        validate_migration_resource(resource)
        return resource
    if (
        "schema" in resource
        or resource.get("schema_version", 1) != 1
        or isinstance(resource.get("schema_version"), bool)
    ):
        raise MigrationError("Unsupported migration resource schema.")
    try:
        return _normalize_legacy_resource(resource)
    except MigrationError:
        raise
    except (KeyError, TypeError, AttributeError) as error:
        raise MigrationError(f"Malformed legacy migration resource: {error}") from error


def _normalize_legacy_resource(resource: dict) -> dict:
    if "source_profile" in resource:
        resource["source_profiles"] = profile_list(resource.pop("source_profile"))
    elif "source_profiles" not in resource:
        source, axis = _id_profile(str(resource.get("source_id", "")))
        resource["source_profiles"] = [source]
        resource.setdefault("axis", axis)
    if "target_profile" not in resource:
        target, axis = _id_profile(str(resource.get("target_id", "")))
        resource["target_profile"] = target
        if resource.get("axis") != axis:
            raise MigrationError("Migration source and target axes differ.")
    else:
        target_profiles = profile_list(resource["target_profile"])
        if len(target_profiles) != 1:
            raise MigrationError("A migration resource needs one target profile.")
        resource["target_profile"] = target_profiles[0]
    resource.update(schema=MIGRATION_SCHEMA, schema_version=MIGRATION_SCHEMA_VERSION)
    resource.setdefault("status", "active")
    families = {p["family"] for p in resource["source_profiles"]} | {resource["target_profile"]["family"]}
    resource.setdefault(
        "reverse_policy",
        "explicit_only" if len(families) > 1 or resource.get("quality") == "heuristic" else "infer_with_policy",
    )
    for key, default in (
        ("replace", []),
        ("disaggregate", []),
        ("delete", []),
        ("recipes", {}),
        ("reviews", {}),
        ("coverage", {}),
    ):
        resource.setdefault(key, default)
    for operation in ("replace", "disaggregate", "delete"):
        for rule in resource[operation]:
            if "review_provenance" in rule:
                review = rule.pop("review_provenance")
                if "reason" in review and "rationale" not in review:
                    review["rationale"] = review.pop("reason")
                rule["review_ref"] = intern_review(resource, review)
            if len(families) > 1:
                rule.setdefault("id", rule_identifier(resource["name"], rule["source"]))
    coverage = resource["coverage"]
    if coverage and "mapped_identities" in coverage:
        coverage.setdefault("unresolved_identities", coverage["source_identities"] - coverage["mapped_identities"])
    validate_migration_resource(resource)
    return resource


def _finite_numbers(value):
    if isinstance(value, float) and not math.isfinite(value):
        raise MigrationError("Migration values must be finite.")
    if isinstance(value, dict):
        for item in value.values():
            _finite_numbers(item)
    elif isinstance(value, list):
        for item in value:
            _finite_numbers(item)


def _identity(record: dict, axis: str):
    if axis == "technosphere":
        if not all(isinstance(record.get(field), str) and record[field] for field in IDENTITY_FIELDS):
            raise MigrationError("Incomplete technosphere identity.")
        return tuple(record[field] for field in IDENTITY_FIELDS)
    if not record.get("name") or not record.get("unit") or not record.get("categories"):
        raise MigrationError("Incomplete biosphere source identity.")
    return record["name"], tuple(record["categories"]), record["unit"]


def validate_migration_resource(resource: dict) -> None:
    """Validate the named JSON schema, factors, references, recipes and coverage.

    :param resource: A canonical schema-version-3 migration resource.
    :raises MigrationError: If structural or semantic validation fails.
    """
    error = next(_validator().iter_errors(resource), None)
    if error is not None:
        path = ".".join(str(value) for value in error.absolute_path) or "resource"
        raise MigrationError(f"Invalid migration schema at {path}: {error.message}")
    _finite_numbers(resource)
    axis = resource["axis"]
    profiles = resource["source_profiles"]
    encoded_profiles = [json.dumps(profile, sort_keys=True) for profile in profiles]
    if len(set(encoded_profiles)) != len(profiles):
        raise MigrationError("Duplicate migration source profile.")
    for profile in [*profiles, resource["target_profile"]]:
        if axis == "technosphere" and not profile.get("system_model"):
            raise MigrationError("Technosphere profiles need an explicit system model.")
        if axis == "biosphere" and "system_model" in profile:
            raise MigrationError("Biosphere profiles must be independent of system models.")
    for role, expected in (("source", profiles), ("target", [resource["target_profile"]])):
        identifier = resource.get(role + "_id")
        if isinstance(identifier, str) and _PROFILE_ID.fullmatch(identifier):
            declared, declared_axis = _id_profile(identifier)
            if declared_axis != axis or declared not in expected:
                raise MigrationError(f"Migration {role} identifier conflicts with its declared profile.")
    families = {p["family"] for p in profiles} | {resource["target_profile"]["family"]}
    if len(families) > 1 and resource["reverse_policy"] != "explicit_only":
        raise MigrationError("Cross-family resources require an explicit reverse resource.")
    seen_ids = set()
    seen_sources = set()
    for operation in ("replace", "disaggregate", "delete"):
        operation_sources = set()
        for rule in resource[operation]:
            source = _identity(rule["source"], axis)
            if source in operation_sources:
                raise MigrationError(f"Duplicate {operation} source identity: {source}")
            operation_sources.add(source)
            seen_sources.add(source)
            if "id" in rule:
                if rule["id"] in seen_ids:
                    raise MigrationError("Duplicate migration rule identifier.")
                seen_ids.add(rule["id"])
            if "review_ref" in rule and rule["review_ref"] not in resource["reviews"]:
                raise MigrationError("Unknown migration review reference.")
            if operation == "replace":
                target = rule["target"]
                if "recipe_ref" in target:
                    if set(target) != {"recipe_ref"}:
                        raise MigrationError("A recipe target cannot also contain a background identity.")
                    if axis != "technosphere" or target["recipe_ref"] not in resource["recipes"]:
                        raise MigrationError("Unknown migration recipe reference.")
                    if resource["recipes"][target["recipe_ref"]]["unit"] != rule["source"]["unit"]:
                        raise MigrationError("Recipe output unit differs from source unit.")
                elif axis == "technosphere":
                    _identity(target, axis)
                if rule.get("classification") == "proxy":
                    review = rule_review(resource, rule)
                    if not review.get("rationale") or not review.get("evidence"):
                        raise MigrationError("A documented proxy needs a rationale and evidence.")
            if operation == "disaggregate":
                for target in rule["targets"]:
                    if axis == "technosphere":
                        _identity(target, axis)
                if sum(target.get("allocation", 1) for target in rule["targets"]) <= 0:
                    raise MigrationError("Disaggregation needs a positive total allocation.")
    if resource.get("quality") == "reviewed":
        for rule in resource["replace"]:
            review = rule_review(resource, rule)
            if (
                not rule.get("id")
                or "classification" not in rule
                or not review.get("rationale")
                or not review.get("evidence")
            ):
                raise MigrationError("Reviewed rules need an identifier, classification, rationale and evidence.")
    visiting, visited = set(), set()

    def visit(recipe_id):
        if recipe_id in visiting:
            raise MigrationError("Cyclic recipe dependency.")
        if recipe_id in visited:
            return
        visiting.add(recipe_id)
        recipe = resource["recipes"][recipe_id]
        _identity(recipe, "technosphere")
        for exchange in recipe["inputs"]:
            if "recipe" in exchange:
                if exchange["recipe"] not in resource["recipes"]:
                    raise MigrationError("Unknown dependent recipe.")
                visit(exchange["recipe"])
            else:
                _identity(exchange, exchange["type"])
            if "minimum" in exchange and "maximum" in exchange and exchange["minimum"] > exchange["maximum"]:
                raise MigrationError("Recipe uncertainty minimum exceeds maximum.")
        visiting.remove(recipe_id)
        visited.add(recipe_id)

    for recipe_id in resource["recipes"]:
        visit(recipe_id)
    coverage = resource["coverage"]
    if coverage:
        count = len(seen_sources)
        if (
            coverage["mapped_identities"] != count
            or coverage["source_identities"] != count + coverage["unresolved_identities"]
        ):
            raise MigrationError("Catalog disposition count does not match coverage.")
        rules = resource["replace"]
        methods = Counter(rule["mapping_method"] for rule in rules if "mapping_method" in rule)
        if "mapping_methods" in coverage and coverage["mapping_methods"] != dict(methods):
            raise MigrationError("Mapping method counts do not match coverage.")
        if "dispositions" in coverage:
            dispositions = Counter(
                "recipe" if "recipe_ref" in rule["target"] else rule.get("classification", "unclassified")
                for rule in rules
            )
            if coverage["unresolved_identities"]:
                dispositions["unresolved"] = coverage["unresolved_identities"]
            if coverage["dispositions"] != dict(dispositions):
                raise MigrationError("Catalog dispositions do not match coverage.")
        scores = [rule["confidence"] for rule in rules if "confidence" in rule]
        if "mean_confidence" in coverage and (
            not scores or coverage["mean_confidence"] != round(sum(scores) / len(scores), 6)
        ):
            raise MigrationError("Mean confidence does not match existing scores.")


def dumps_migration_resource(resource: dict) -> str:
    """Serialize a canonical resource deterministically with compact rule entries."""
    validate_migration_resource(resource)
    fields = []
    for key in sorted(k for k in resource if k != "_path"):
        value = resource[key]
        if key in ("replace", "disaggregate", "delete") and value:
            rendered = (
                "[\n"
                + ",\n".join(
                    "    " + json.dumps(rule, sort_keys=True, ensure_ascii=False, allow_nan=False) for rule in value
                )
                + "\n  ]"
            )
        else:
            rendered = json.dumps(value, indent=2, sort_keys=True, ensure_ascii=False, allow_nan=False).replace(
                "\n", "\n  "
            )
        fields.append("  " + json.dumps(key) + ": " + rendered)
    return "{\n" + ",\n".join(fields) + "\n}\n"
