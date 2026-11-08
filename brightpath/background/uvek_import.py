"""Materialize reviewed forward UVEK recipe targets without altering other links."""

import hashlib
import json
from copy import deepcopy
from dataclasses import asdict

from brightpath.background.catalogs import CatalogIntegrityError, CatalogNotFoundError
from brightpath.background.reference_signs import select_profile_sign_rules
from brightpath.core.policies import PolicyAction
from brightpath.core.reports import Change, Issue, Loss, Severity, StageKind, StageReport
from brightpath.migrations.resources import load_uvek_technosphere_resource
from brightpath.migrations.scaling import scale_exchange
from brightpath.migrations.schema import rule_review

FIELDS = ("name", "reference product", "location", "unit")
STAGE = StageKind.BACKGROUND_MIGRATION


def identity(record):
    return tuple(record.get(f, "") for f in FIELDS)


def _recipe_signature(resource, recipe_id, target_biosphere, signatures):
    """Bind a generated identity to the complete packaged recipe and its dependencies."""
    if recipe_id not in signatures:
        recipe = resource["recipes"][recipe_id]
        dependencies = {
            e["recipe"]: _recipe_signature(resource, e["recipe"], target_biosphere, signatures)
            for e in recipe["inputs"]
            if "recipe" in e
        }
        signatures[recipe_id] = hashlib.sha256(
            json.dumps(
                {
                    "recipe": recipe,
                    "dependencies": dependencies,
                    "resource": resource["name"],
                    "target_biosphere": target_biosphere,
                },
                sort_keys=True,
                ensure_ascii=False,
            ).encode()
        ).hexdigest()[:24]
    return signatures[recipe_id]


def apply_reviewed_recipe_biosphere_bindings(data, owners, target_profile, target_flows, policy):
    """Replay packaged contextual bindings only inside an identifiable generated helper.

    ``data`` contains exchange-only projections; ``owners`` supplies the unchanged
    foreground identities. Never take a flow correspondence from caller metadata.
    A stale helper signature requires regeneration rather than inheriting a new review.
    """
    issues, changes, losses = [], [], []
    marked = [
        (di, d)
        for di, d in enumerate(owners)
        if isinstance(d.get("brightpath_conversion"), dict)
        and "biosphere_binding_profile" in d["brightpath_conversion"]
    ]
    if not marked:
        return StageReport(STAGE, label="reviewed helper biosphere bindings")
    resource = load_uvek_technosphere_resource()
    signatures = {}
    for di, owner in marked:
        metadata = owner["brightpath_conversion"]
        recipe_id = metadata.get("recipe_id")
        path = f"datasets[{di}]"
        try:
            recipe = resource["recipes"][recipe_id]
            original_profile = metadata["biosphere_binding_profile"]
            profile_key = json.dumps(original_profile, sort_keys=True)
            signature = _recipe_signature(resource, recipe_id, original_profile, signatures.setdefault(profile_key, {}))
            expected = {
                **{f: recipe[f] for f in FIELDS},
                "name": f"{recipe['name']} [BrightPath UVEK conversion {signature[:8]}]",
            }
            valid = (
                metadata.get("origin") == "generated"
                and metadata.get("resource") == resource["name"]
                and owner.get("code") == f"brightpath-uvek-import-{signature}"
                and identity(owner) == identity(expected)
            )
        except (KeyError, TypeError, ValueError):
            valid = False
        if not valid:
            issues.append(
                Issue(
                    Severity.ERROR,
                    "migration.recipe_biosphere_review_stale",
                    "Generated helper no longer matches its packaged biosphere review; regenerate it.",
                    STAGE,
                    path=path,
                )
            )
            continue
        bindings = {}
        for original in recipe["inputs"]:
            for binding in original.get("biosphere_targets", []):
                if binding["profile"] == target_profile:
                    key = (original["name"], tuple(original["categories"]), original["unit"])
                    if key in bindings and bindings[key][1] != binding:
                        raise ValueError("Conflicting reviewed helper biosphere bindings.")
                    bindings[key] = original, binding
        optional = {
            (e["name"], tuple(e["categories"]), e["unit"]): e
            for e in recipe["inputs"]
            if e.get("approved_omission_if_unavailable")
        }
        omitted = set()
        for ei, exchange in enumerate(data[di].get("exchanges", [])):
            if exchange.get("type") != "biosphere":
                continue
            key = (exchange.get("name"), tuple(exchange.get("categories", ())), exchange.get("unit"))
            if key in optional and key not in target_flows:
                original = optional[key]
                path = f"datasets[{di}].exchanges[{ei}]"
                details = {
                    "resource": resource["name"],
                    "recipe_id": recipe_id,
                    "source_exchange": deepcopy(exchange),
                    "reviewed_recipe_input": original,
                    "target_biosphere": target_profile,
                }
                severity = Severity.ERROR if policy.on_information_loss is PolicyAction.ERROR else Severity.WARNING
                issues.append(
                    Issue(
                        severity,
                        "migration.recipe_input_omission",
                        original["omission_rationale"],
                        STAGE,
                        path=path,
                        details=details,
                    )
                )
                losses.append(
                    Loss(
                        "migration.recipe_input_omission",
                        original["omission_rationale"],
                        STAGE,
                        path=path,
                        details=details,
                    )
                )
                if severity is not Severity.ERROR:
                    omitted.add(ei)
                    changes.append(
                        Change(
                            "migration.recipe_input_omission",
                            original["omission_rationale"],
                            STAGE,
                            path=path,
                            before=exchange,
                            after=None,
                            details=details,
                        )
                    )
                continue
            if key not in bindings:
                continue
            original, binding = bindings[key]
            destination = binding["target"]
            path = f"datasets[{di}].exchanges[{ei}]"
            target_key = (destination["name"], tuple(destination["categories"]), destination["unit"])
            if target_key not in target_flows:
                issues.append(
                    Issue(
                        Severity.ERROR,
                        "migration.recipe_biosphere_target_unavailable",
                        "The exact reviewed helper biosphere target is unavailable.",
                        STAGE,
                        path=path,
                    )
                )
                continue
            details = {
                "resource": resource["name"],
                "recipe_id": recipe_id,
                "source_exchange": deepcopy(exchange),
                "reviewed_recipe_input": original,
                "target": destination,
            }
            if binding.get("information_loss"):
                severity = Severity.ERROR if policy.on_information_loss is PolicyAction.ERROR else Severity.WARNING
                issues.append(
                    Issue(
                        severity,
                        "migration.recipe_biosphere_approximation",
                        binding["information_loss"],
                        STAGE,
                        path=path,
                        details=details,
                    )
                )
                losses.append(
                    Loss(
                        "migration.recipe_biosphere_approximation",
                        binding["information_loss"],
                        STAGE,
                        path=path,
                        details=details,
                    )
                )
                if severity is Severity.ERROR:
                    continue
            before = deepcopy(exchange)
            exchange.update(deepcopy(destination))
            exchange.pop("input", None)
            exchange.pop("database", None)
            changes.append(
                Change(
                    "migration.recipe_biosphere_binding",
                    binding["reason"],
                    STAGE,
                    path=path,
                    before=before,
                    after=exchange,
                    details=details,
                )
            )
        if omitted:
            data[di]["exchanges"] = [e for ei, e in enumerate(data[di]["exchanges"]) if ei not in omitted]
    return StageReport(
        STAGE,
        label="reviewed helper biosphere bindings",
        issues=tuple(issues),
        changes=tuple(changes),
        losses=tuple(losses),
    )


def apply_uvek_import_recipes(
    data, source_profile, target_biosphere, provider, foreground_targets=(), *, database_name, policy
):
    """Return generated helpers and immutable findings for exact reviewed recipes.

    Callers provide an operation-local data copy and roll back the complete
    operation on any error, including unrelated later mapping failures.
    """
    resource = select_profile_sign_rules(load_uvek_technosphere_resource(), source_profile)
    rules = {identity(r["source"]): r for r in resource["replace"] if "recipe_ref" in r["target"]}
    foreground = set(foreground_targets) | {identity(d) for d in data}
    codes = {d.get("code") for d in data if d.get("code")}
    helpers, issues, changes, losses = {}, [], [], []
    # A missing target catalog must not trigger an approved optional omission.
    # Ordinary final-target validation retains responsibility for that finding.
    try:
        target_flows = provider.load_biosphere(target_biosphere).identities
    except (CatalogIntegrityError, CatalogNotFoundError):
        target_flows = None

    signatures = {}

    def recipe_signature(recipe_id):
        return _recipe_signature(resource, recipe_id, asdict(target_biosphere), signatures)

    zero_stock_ids = {}

    def zero_suppliers(recipe_id):
        if recipe_id not in zero_stock_ids:
            recipe = resource["recipes"][recipe_id]
            zero_stock_ids[recipe_id] = (
                {recipe_id}
                if recipe.get("zero_burden")
                else set().union(*(zero_suppliers(e["recipe"]) for e in recipe["inputs"] if "recipe" in e))
            )
        return zero_stock_ids[recipe_id]

    def helper(recipe_id):
        if recipe_id in helpers:
            return helpers[recipe_id]
        recipe = resource["recipes"][recipe_id]
        signature = recipe_signature(recipe_id)
        code = f"brightpath-uvek-import-{signature}"
        dataset = {
            **{f: recipe[f] for f in FIELDS},
            "name": f"{recipe['name']} [BrightPath UVEK conversion {signature[:8]}]",
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
        if recipe.get("zero_burden"):
            dataset["brightpath_conversion"].update(zero_burden=True, zero_burden_reason=recipe["zero_burden_reason"])
        if any(e.get("biosphere_targets") or e.get("approved_omission_if_unavailable") for e in recipe["inputs"]):
            dataset["brightpath_conversion"]["biosphere_binding_profile"] = asdict(target_biosphere)
        if "parameters" in recipe:
            dataset["parameters"] = deepcopy(recipe["parameters"])
        if code in codes or identity(dataset) in foreground:
            raise ValueError("Generated helper collides with a supplied foreground dataset.")
        codes.add(code)
        foreground.add(identity(dataset))
        helpers[recipe_id] = dataset
        dataset["exchanges"].append(
            {**dict(zip(FIELDS, identity(dataset), strict=True)), "type": "production", "amount": 1}
        )

        # Apply contextual targets immediately only if the whole helper already
        # belongs to the target biosphere. Otherwise keep it canonical until its
        # explicit biosphere migration reaches that destination.
        def ready(exchange):
            if exchange.get("type") != "biosphere":
                return True
            if exchange.get("approved_omission_if_unavailable"):
                return True
            binding = next(
                (b for b in exchange.get("biosphere_targets", []) if b["profile"] == asdict(target_biosphere)), None
            )
            destination = binding["target"] if binding else exchange
            return (destination["name"], tuple(destination["categories"]), destination["unit"]) in target_flows

        bindings_ready = target_flows is not None and all(ready(e) for e in recipe["inputs"])
        for original in recipe["inputs"]:
            exchange = deepcopy(original)
            exchange.pop("zero_burden_supplier", None)
            bindings = exchange.pop("biosphere_targets", [])
            binding = next((b for b in bindings if b["profile"] == asdict(target_biosphere)), None)
            if binding and not bindings_ready:
                destination = binding["target"]
                destination_identity = (destination["name"], tuple(destination["categories"]), destination["unit"])
                if target_flows is None or destination_identity not in target_flows:
                    raise ValueError("The exact reviewed recipe biosphere target is unavailable.")
            if binding and bindings_ready:
                destination = binding["target"]
                destination_identity = (destination["name"], tuple(destination["categories"]), destination["unit"])
                if target_flows is None or destination_identity not in target_flows:
                    raise ValueError("The exact reviewed recipe biosphere target is unavailable.")
                exchange.update(deepcopy(destination))
                if binding.get("information_loss"):
                    path = f"helpers[{recipe_id}].inputs[{len(dataset['exchanges']) - 1}]"
                    details = {
                        "resource": resource["name"],
                        "recipe_id": recipe_id,
                        "source_exchange": original,
                        "target": destination,
                    }
                    issues.append(
                        Issue(
                            Severity.WARNING,
                            "migration.recipe_biosphere_approximation",
                            binding["information_loss"],
                            STAGE,
                            path=path,
                            details=details,
                        )
                    )
                    losses.append(
                        Loss(
                            "migration.recipe_biosphere_approximation",
                            binding["information_loss"],
                            STAGE,
                            path=path,
                            details=details,
                        )
                    )
            optional = exchange.pop("approved_omission_if_unavailable", False)
            rationale = exchange.pop("omission_rationale", "")
            flow = (exchange.get("name", ""), tuple(exchange.get("categories", ())), exchange.get("unit", ""))
            if optional and target_flows is not None and flow not in target_flows:
                path = f"helpers[{recipe_id}].inputs[{len(dataset['exchanges']) - 1}]"
                details = {
                    "resource": resource["name"],
                    "recipe_id": recipe_id,
                    "source_exchange": original,
                    "target_biosphere": asdict(target_biosphere),
                }
                issues.append(
                    Issue(
                        Severity.WARNING,
                        "migration.recipe_input_omission",
                        rationale,
                        STAGE,
                        path=path,
                        details=details,
                    )
                )
                losses.append(Loss("migration.recipe_input_omission", rationale, STAGE, path=path, details=details))
                continue
            exchange.pop("input", None)
            exchange.pop("database", None)
            if "recipe" in exchange:
                dependency = helper(exchange.pop("recipe"))
                exchange.update(dict(zip(FIELDS, identity(dependency), strict=True)))
                exchange["type"] = "technosphere"
            dataset["exchanges"].append(exchange)
        return dataset

    for di, dataset in enumerate(data):
        for ei, exchange in enumerate(dataset.get("exchanges", [])):
            if exchange.get("type") not in {"technosphere", "substitution"} or identity(exchange) in foreground:
                continue
            rule = rules.get(identity(exchange))
            if rule is None:
                continue
            path = f"datasets[{di}].exchanges[{ei}]"
            if rule.get("reference_flow_sign_review", {}).get("status") != "verified":
                issues.append(
                    Issue(
                        Severity.ERROR,
                        "migration.reference_flow_sign_unresolved",
                        rule["reference_flow_sign_review"]["reason"],
                        STAGE,
                        path=path,
                    )
                )
                continue
            before = deepcopy(exchange)
            information_loss = rule.get("technosphere_information_loss")
            if information_loss:
                severity = Severity.ERROR if policy.on_information_loss is PolicyAction.ERROR else Severity.WARNING
                details = {
                    "rule_id": rule["id"],
                    "resource": resource["name"],
                    "source_exchange": before,
                    "review": rule_review(resource, rule),
                    "policy_action": policy.on_information_loss.value,
                }
                issues.append(
                    Issue(
                        severity,
                        "migration.technosphere_information_loss",
                        information_loss,
                        STAGE,
                        path=path,
                        details=details,
                    )
                )
                losses.append(
                    Loss("migration.technosphere_information_loss", information_loss, STAGE, path=path, details=details)
                )
                if severity is Severity.ERROR:
                    continue
            try:
                target = helper(rule["target"]["recipe_ref"])
                scale_exchange(exchange, rule["conversion_factor"])
            except (ValueError, TypeError, KeyError, OverflowError) as error:
                issues.append(Issue(Severity.ERROR, "migration.recipe_transform_invalid", str(error), STAGE, path=path))
                continue
            exchange.update(dict(zip(FIELDS, identity(target), strict=True)))
            if "product" in exchange:
                exchange["product"] = target["reference product"]
            exchange.pop("input", None)
            exchange["database"] = database_name
            exchange["input"] = (database_name, target["code"])
            review = rule_review(resource, rule)
            details = {
                "rule_id": rule["id"],
                "resource": resource["name"],
                "recipe_id": rule["target"]["recipe_ref"],
                "factor": rule["conversion_factor"],
                "source_exchange": before,
                "review": review,
            }
            recipe = resource["recipes"][rule["target"]["recipe_ref"]]
            if recipe.get("zero_burden"):
                target["brightpath_conversion"].update(
                    zero_burden=True, zero_burden_reason=recipe["zero_burden_reason"]
                )
                issues.append(
                    Issue(
                        Severity.WARNING,
                        "migration.zero_burden_supplier",
                        recipe["zero_burden_reason"],
                        STAGE,
                        path=path,
                        details=details,
                    )
                )
            else:
                for stock_id in sorted(zero_suppliers(rule["target"]["recipe_ref"])):
                    issues.append(
                        Issue(
                            Severity.WARNING,
                            "migration.zero_burden_supplier",
                            resource["recipes"][stock_id]["zero_burden_reason"],
                            STAGE,
                            path=path,
                            details={**details, "root_recipe_id": details["recipe_id"], "recipe_id": stock_id},
                        )
                    )
            message = f"Apply reviewed physical recipe {rule['id']}: {review['rationale']}"
            exchange["comment"] = "\n".join(filter(None, (exchange.get("comment"), message)))
            issues.append(
                Issue(Severity.INFO, "migration.foreground_recipe", message, STAGE, path=path, details=details)
            )
            changes.append(
                Change(
                    "migration.foreground_recipe",
                    message,
                    STAGE,
                    path=path,
                    before=before,
                    after=exchange,
                    details=details,
                )
            )
    generated = sorted(helpers.values(), key=lambda d: d["code"])
    return (
        StageReport(
            STAGE,
            label="reviewed UVEK import recipes",
            issues=tuple(issues),
            changes=tuple(changes),
            losses=tuple(losses),
            metrics={
                "generated_dataset_codes": [d["code"] for d in generated],
                "matched_external_exchanges": len(changes),
            },
        ),
        generated,
    )
