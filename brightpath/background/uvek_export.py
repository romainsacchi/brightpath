"""Reviewed, directional UVEK exports with explicit foreground recipes."""

from __future__ import annotations

import hashlib
import json
import math
from copy import deepcopy
from functools import lru_cache

from brightpath import DATA_DIR, __version__
from brightpath.core.policies import PolicyAction
from brightpath.core.reports import Change, Issue, Loss, Severity, StageKind, StageReport
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import _load_resource_manifest, _verify_manifest_entry

RESOURCE_NAME = "uvek-2025-to-ecoinvent-3.12-cutoff.json"
IDENTITY_FIELDS = ("name", "reference product", "location", "unit")
STAGE = StageKind.BACKGROUND_MIGRATION


def identity(record):
    """Return the exact technosphere identity used by catalog validation."""
    return tuple(record.get(field, "") for field in IDENTITY_FIELDS)


def migration_resource_fingerprint() -> str:
    """Identify the migration implementation and all packaged resource revisions."""
    manifest = (DATA_DIR / "migrations/RESOURCE_MANIFEST.json").read_bytes()
    catalogs = (DATA_DIR / "export/reference_catalogs/RESOURCE_MANIFEST.json").read_bytes()
    return hashlib.sha256(
        __version__.encode() + b"uvek-export-v2-composed-patch-routes" + manifest + catalogs
    ).hexdigest()


@lru_cache(maxsize=1)
def load_uvek_export_resource():
    """Load and integrity-check the reviewed UVEK-to-ecoinvent resource."""
    path = DATA_DIR / "migrations/uvek" / RESOURCE_NAME
    try:
        raw = path.read_bytes()
        resource = json.loads(raw)
        if not isinstance(resource, dict):
            raise ValueError("Expected a migration resource object.")
        _verify_manifest_entry(path, raw, resource, _load_resource_manifest())
        validate_resource(resource)
    except (OSError, ValueError, TypeError, KeyError) as error:
        raise MigrationError(f"Invalid directional UVEK resource: {error}") from error
    return resource


def validate_resource(resource):
    """Reject conflicting rules, malformed coefficients, and cyclic recipes."""
    if not isinstance(resource, dict):
        raise ValueError("Expected a migration resource object.")
    if resource.get("schema_version") != 1 or resource.get("quality") != "reviewed":
        raise ValueError("Expected a reviewed schema-version-1 resource.")
    if resource.get("source_profile") != {"family": "uvek", "version": "2025", "system_model": "cutoff"}:
        raise ValueError("Unexpected source profile.")
    if resource.get("target_profile") != {"family": "ecoinvent", "version": "3.12", "system_model": "cutoff"}:
        raise ValueError("Unexpected target profile.")
    if resource.get("status") != "active" or resource.get("axis") != "technosphere":
        raise ValueError("Expected an active technosphere resource.")
    seen = set()
    rule_ids = set()
    recipes = resource["recipes"]
    for rule in resource["rules"]:
        source = identity(rule["source"])
        if not all(isinstance(value, str) and value for value in source) or source in seen:
            raise ValueError(f"Missing or duplicate source identity: {source}")
        seen.add(source)
        if not rule.get("id") or rule["id"] in rule_ids:
            raise ValueError("Missing or duplicate rule identifier.")
        rule_ids.add(rule["id"])
        if rule.get("classification") not in {"correspondence", "proxy"}:
            raise ValueError("A rule must distinguish correspondences and proxies.")
        if not rule.get("rationale") or not rule.get("evidence"):
            raise ValueError("A rule needs evidence and a rationale.")
        if ("recipe" in rule) == ("target" in rule):
            raise ValueError("A rule needs exactly one target or recipe.")
        if "factor" in rule:
            _finite_factor(rule["factor"])
        if "recipe" in rule:
            if rule["recipe"] not in recipes:
                raise ValueError("Missing recipe.")
            if recipes[rule["recipe"]]["unit"] != source[3]:
                raise ValueError("Recipe output unit differs from source unit.")
        else:
            if not all(identity(rule["target"])):
                raise ValueError("Incomplete target identity.")
            _finite_factor(rule["factor"])
    for entry in resource["unresolved"]:
        source = identity(entry["source"])
        if not all(source) or source in seen or not entry.get("reason"):
            raise ValueError("Missing, duplicate, or conflicting unresolved entry.")
        seen.add(source)
    if len(seen) != resource["coverage"]["source_identities"]:
        raise ValueError("Catalog disposition count does not match coverage.")
    visiting, visited = set(), set()

    def visit(recipe_id):
        if recipe_id in visiting:
            raise ValueError("Cyclic recipe dependency.")
        if recipe_id in visited:
            return
        visiting.add(recipe_id)
        recipe = recipes[recipe_id]
        if not recipe.get("unit") or not recipe.get("inputs") or not recipe.get("evidence"):
            raise ValueError("Recipe lacks output unit, inputs, or evidence.")
        for exchange in recipe["inputs"]:
            amount = exchange["amount"]
            if isinstance(amount, bool) or not isinstance(amount, (float, int)) or not math.isfinite(amount):
                raise ValueError("Recipe coefficient must be finite.")
            if "recipe" in exchange:
                if exchange["recipe"] not in recipes:
                    raise ValueError("Missing dependent recipe.")
                visit(exchange["recipe"])
            elif exchange.get("type") == "technosphere":
                if not all(identity(exchange)):
                    raise ValueError("Incomplete recipe supplier.")
            elif exchange.get("type") == "biosphere":
                if not exchange.get("name") or not exchange.get("unit") or not exchange.get("categories"):
                    raise ValueError("Incomplete recipe biosphere flow.")
            else:
                raise ValueError("Invalid recipe exchange type.")
        visiting.remove(recipe_id)
        visited.add(recipe_id)

    for recipe_id in recipes:
        visit(recipe_id)


def _finite_factor(value):
    if isinstance(value, bool) or not isinstance(value, (float, int)) or not math.isfinite(value) or value == 0:
        raise ValueError("Conversion factor must be finite and nonzero.")
    return float(value)


def scale_exchange(exchange, factor):
    """Scale supported uncertainties and expressions together with their amount."""
    factor = _finite_factor(factor)
    if factor == 1:
        return
    uncertainty = exchange.get("uncertainty type", 0)
    if uncertainty not in (0, 1, 2, 3, 4):
        raise ValueError(f"Cannot safely scale uncertainty type {uncertainty}.")
    amount = float(exchange["amount"])
    if not math.isfinite(amount):
        raise ValueError("Cannot scale a non-finite amount.")
    exchange["amount"] = amount * factor
    if "loc" in exchange:
        exchange["loc"] = (
            float(exchange["loc"]) + math.log(abs(factor)) if uncertainty == 2 else float(exchange["loc"]) * factor
        )
    if uncertainty == 2:
        exchange["negative"] = bool(exchange.get("negative", amount < 0)) != (factor < 0)
    elif "negative" in exchange:
        exchange["negative"] = exchange["amount"] < 0
    if uncertainty == 3 and "scale" in exchange:
        exchange["scale"] = float(exchange["scale"]) * abs(factor)
    bounds = {field: float(exchange[field]) * factor for field in ("minimum", "maximum") if field in exchange}
    for field in ("minimum", "maximum"):
        exchange.pop(field, None)
    for field, value in bounds.items():
        target = {"minimum": "maximum", "maximum": "minimum"}[field] if factor < 0 else field
        exchange[target] = value
    if exchange.get("formula"):
        exchange["formula"] = f"({exchange['formula']}) * ({factor!r})"


def apply_uvek_export(data, policy, foreground_targets=(), *, resource=None, database_name="brightpath-inventory"):
    """Transform external suppliers only; return a report and target-native helpers."""
    resource = resource if resource is not None else load_uvek_export_resource()
    rules = {identity(rule["source"]): rule for rule in resource["rules"]}
    unresolved = {identity(entry["source"]): entry for entry in resource["unresolved"]}
    foreground = {identity(dataset) for dataset in data} | set(foreground_targets)
    codes = {dataset.get("code") for dataset in data if dataset.get("code")}
    issues, changes, losses = [], [], []
    helpers = {}
    recipe_signatures = {}
    matched = 0

    def recipe_signature(recipe_id):
        if recipe_id not in recipe_signatures:
            recipe = resource["recipes"][recipe_id]
            dependencies = {
                exchange["recipe"]: recipe_signature(exchange["recipe"])
                for exchange in recipe["inputs"]
                if "recipe" in exchange
            }
            recipe_signatures[recipe_id] = hashlib.sha256(
                json.dumps(
                    {"recipe": recipe, "dependencies": dependencies, "target": resource["target_profile"]},
                    sort_keys=True,
                    separators=(",", ":"),
                    ensure_ascii=False,
                ).encode()
            ).hexdigest()[:24]
        return recipe_signatures[recipe_id]

    def helper(recipe_id):
        if recipe_id in helpers:
            return helpers[recipe_id]
        recipe = resource["recipes"][recipe_id]
        signature = recipe_signature(recipe_id)
        code = f"brightpath-uvek-{signature}"
        dataset = {
            "name": f"{recipe['name']} [BrightPath UVEK conversion {signature[:8]}]",
            "reference product": recipe["reference product"],
            "location": recipe["location"],
            "unit": recipe["unit"],
            "code": code,
            "database": database_name,
            "type": "process",
            "production amount": 1,
            "comment": recipe["rationale"],
            "source": "; ".join(recipe["evidence"]),
            "brightpath_conversion": {
                "origin": "generated",
                "recipe_id": recipe_id,
                "resource": resource["name"],
                "evidence": recipe["evidence"],
            },
            "exchanges": [],
        }
        if code in codes or identity(dataset) in foreground:
            raise ValueError("Generated helper collides with a supplied foreground dataset.")
        codes.add(code)
        foreground.add(identity(dataset))
        helpers[recipe_id] = dataset
        dataset["exchanges"].append(
            {**dict(zip(IDENTITY_FIELDS, identity(dataset), strict=True)), "type": "production", "amount": 1}
        )
        for original in recipe["inputs"]:
            exchange = deepcopy(original)
            exchange.pop("input", None)
            exchange.pop("database", None)
            if "recipe" in exchange:
                dependency = helper(exchange.pop("recipe"))
                exchange.update(dict(zip(IDENTITY_FIELDS, identity(dependency), strict=True)))
                exchange["type"] = "technosphere"
            dataset["exchanges"].append(exchange)
        return dataset

    for dataset_index, dataset in enumerate(data):
        for exchange_index, exchange in enumerate(dataset.get("exchanges", [])):
            if exchange.get("type") not in {"technosphere", "substitution"} or identity(exchange) in foreground:
                continue
            source = identity(exchange)
            path = f"datasets[{dataset_index}].exchanges[{exchange_index}]"
            rule = rules.get(source)
            if rule is None:
                reason = unresolved.get(source, {}).get("reason", "No reviewed source correspondence is packaged.")
                issues.append(
                    Issue(
                        Severity.ERROR,
                        "migration.uvek_supplier_unresolved",
                        reason,
                        STAGE,
                        path=path,
                        details={"source": dict(zip(IDENTITY_FIELDS, source, strict=True))},
                    )
                )
                continue
            details = {
                "rule_id": rule["id"],
                "source": rule["source"],
                "classification": rule["classification"],
                "rationale": rule["rationale"],
                "evidence": rule["evidence"],
                "resource": resource["name"],
            }
            if rule["classification"] == "proxy":
                severity = Severity.ERROR if policy.on_information_loss == PolicyAction.ERROR else Severity.WARNING
                issues.append(
                    Issue(severity, "migration.documented_proxy", rule["rationale"], STAGE, path=path, details=details)
                )
                losses.append(Loss("migration.documented_proxy", rule["rationale"], STAGE, path=path, details=details))
            before = deepcopy(exchange)
            try:
                target = helper(rule["recipe"]) if "recipe" in rule else rule["target"]
                scale_exchange(exchange, rule.get("factor", 1))
            except (ValueError, KeyError, TypeError, OverflowError) as error:
                issues.append(
                    Issue(
                        Severity.ERROR,
                        "migration.uvek_transform_invalid",
                        str(error),
                        STAGE,
                        path=path,
                        details=details,
                    )
                )
                continue
            exchange.update(dict(zip(IDENTITY_FIELDS, identity(target), strict=True)))
            if "product" in exchange:
                exchange["product"] = target["reference product"]
            exchange.pop("input", None)
            exchange.pop("database", None)
            note = f"BrightPath {rule['id']}: {rule['rationale']}"
            exchange["comment"] = "\n".join(filter(None, (exchange.get("comment"), note)))
            details.update(
                target=dict(zip(IDENTITY_FIELDS, identity(target), strict=True)), factor=rule.get("factor", 1)
            )
            changes.append(
                Change(
                    "migration.uvek_supplier",
                    note,
                    STAGE,
                    path=path,
                    before=before,
                    after=deepcopy(exchange),
                    details=details,
                )
            )
            matched += 1
    generated = sorted(helpers.values(), key=lambda dataset: dataset["code"])
    report = StageReport(
        STAGE,
        label="reviewed UVEK export",
        issues=tuple(issues),
        changes=tuple(changes),
        losses=tuple(losses),
        metrics={
            "matched_external_exchanges": matched,
            "generated_dataset_codes": [dataset["code"] for dataset in generated],
            "resource": resource["name"],
            "fingerprint": migration_resource_fingerprint(),
        },
    )
    return report, generated
