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
from brightpath.migrations.resources import load_migration_resource
from brightpath.migrations.schema import SCHEMA_PATH, normalize_migration_resource, rule_review

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
        __version__.encode() + b"migration-schema-v3" + manifest + catalogs + SCHEMA_PATH.read_bytes()
    ).hexdigest()


@lru_cache(maxsize=1)
def load_uvek_export_resource():
    """Load and integrity-check the reviewed UVEK-to-ecoinvent resource."""
    resource = load_migration_resource(DATA_DIR / "migrations/uvek" / RESOURCE_NAME)
    _validate_export_binding(resource)
    return resource


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


def _validate_export_binding(resource):
    """Check the supported route binding after direction-independent schema validation."""
    if (
        resource.get("axis") != "technosphere"
        or resource.get("quality") != "reviewed"
        or resource["source_profiles"] != [{"family": "uvek", "version": "2025", "system_model": "cutoff"}]
        or resource["target_profile"] != {"family": "ecoinvent", "version": "3.12", "system_model": "cutoff"}
    ):
        raise MigrationError("The reviewed UVEK export requires its registered source and target profiles.")
    if resource["disaggregate"] or resource["delete"]:
        raise MigrationError("The UVEK recipe exporter supports replacement operations only.")


def apply_uvek_export(data, policy, foreground_targets=(), *, resource=None, database_name="brightpath-inventory"):
    """Transform external suppliers only; return a report and target-native helpers."""
    if resource is None:
        resource = load_uvek_export_resource()
    else:
        resource = normalize_migration_resource(resource)
        _validate_export_binding(resource)
    rules = {identity(rule["source"]): rule for rule in resource["replace"]}
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
                issues.append(
                    Issue(
                        Severity.ERROR,
                        "migration.uvek_supplier_unresolved",
                        "No reviewed directional correspondence; similarity and reversed forward rules are not approval.",
                        STAGE,
                        path=path,
                        details={"source": dict(zip(IDENTITY_FIELDS, source, strict=True))},
                    )
                )
                continue
            review = rule_review(resource, rule)
            details = {
                "rule_id": rule["id"],
                "source": rule["source"],
                "classification": rule["classification"],
                "rationale": review["rationale"],
                "evidence": review["evidence"],
                "resource": resource["name"],
            }
            if rule["classification"] == "proxy":
                severity = Severity.ERROR if policy.on_information_loss == PolicyAction.ERROR else Severity.WARNING
                issues.append(
                    Issue(
                        severity, "migration.documented_proxy", review["rationale"], STAGE, path=path, details=details
                    )
                )
                losses.append(
                    Loss("migration.documented_proxy", review["rationale"], STAGE, path=path, details=details)
                )
            before = deepcopy(exchange)
            try:
                target = helper(rule["target"]["recipe_ref"]) if "recipe_ref" in rule["target"] else rule["target"]
                scale_exchange(exchange, rule.get("conversion_factor", 1))
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
            note = f"BrightPath {rule['id']}: {review['rationale']}"
            exchange["comment"] = "\n".join(filter(None, (exchange.get("comment"), note)))
            details.update(
                target=dict(zip(IDENTITY_FIELDS, identity(target), strict=True)),
                factor=rule.get("conversion_factor", 1),
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
