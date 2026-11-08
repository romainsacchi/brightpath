"""Audited exchange omissions shared by both migration directions."""

from copy import deepcopy

from brightpath.core.policies import PolicyAction
from brightpath.core.reports import Loss, StageKind
from brightpath.migrations.engine import _technosphere_matches
from brightpath.migrations.schema import rule_review
from brightpath.models import Issue

FIELDS = ("name", "reference product", "location", "unit")


def _apply_technosphere_deletions(data, resource, policy, report, step_index, *, foreground_targets=()):
    """Omit external exchanges and retain their original data in warning/loss reports."""
    if not resource.get("delete"):
        return []
    foreground = set(foreground_targets) | {tuple(dataset.get(field, "") for field in FIELDS) for dataset in data}
    pending = []
    losses = []
    for dataset_index, dataset in enumerate(data):
        for exchange_index, exchange in enumerate(dataset.get("exchanges", [])):
            if exchange.get("type") not in {"technosphere", "substitution"}:
                continue
            if tuple(exchange.get(field, "") for field in FIELDS) in foreground:
                continue
            for rule in resource.get("delete", []):
                if not _technosphere_matches(exchange, rule["source"]):
                    continue
                approved = rule.get("approved_omission", False)
                action = PolicyAction.WARN if approved else policy.on_deletion
                review = rule_review(resource, rule)
                message = (
                    f"Omitted exchange under rule {rule.get('id', '')}: "
                    f"{review.get('rationale', 'The migration resource removes this source exchange.')} "
                    "Its impacts are not represented in the target dataset."
                )
                path = f"datasets[{dataset_index}].exchanges[{exchange_index}]"
                code = "migration_technosphere_omission" if approved else "migration_technosphere_deletion"
                severity = {PolicyAction.ERROR: "error", PolicyAction.WARN: "warning", PolicyAction.ALLOW: "info"}[
                    action
                ]
                report.issues.append(Issue(severity=severity, code=code, message=message, path=path))
                losses.append(
                    Loss(
                        code=code.replace("migration_", "migration.", 1),
                        message=message,
                        stage=StageKind.BACKGROUND_MIGRATION,
                        path=path,
                        details={
                            "rule_id": rule.get("id", ""),
                            "resource": report.resource_name,
                            "source_exchange": deepcopy(exchange),
                            "review": review,
                            "approved_omission": approved,
                            "policy_action": action.value,
                            "step_index": step_index,
                        },
                    )
                )
                pending.append((dataset_index, exchange_index))
                break
    if not any(issue.severity == "error" for issue in report.issues):
        removed = set(pending)
        for dataset_index, dataset in enumerate(data):
            if "exchanges" in dataset:
                dataset["exchanges"] = [
                    exchange
                    for exchange_index, exchange in enumerate(dataset["exchanges"])
                    if (dataset_index, exchange_index) not in removed
                ]
        report.technosphere_deletions += len(pending)
    return losses
