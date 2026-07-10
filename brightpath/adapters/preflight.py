"""Pure target-format representability checks for inventory conversion.

This module describes what the current syntax writers can preserve before a
conversion changes an inventory's format context or writes an artifact.  It
does not mutate the source document and does not infer a target format.
"""

from __future__ import annotations

import math
from collections.abc import Iterable, Mapping
from numbers import Real
from typing import Any

from brightpath.core.policies import ConversionPolicy, PolicyAction
from brightpath.core.reports import Change, Issue, Loss, Severity, StageKind, StageReport
from brightpath.formats.simapro_csv import (
    _ACTIVITY_METADATA_FIELDS,
    _simapro_uncertainty_type,
    is_simapro_final_waste_flow,
    render_simapro_rows,
)
from brightpath.models import InventoryDocument
from brightpath.profiles import format_simapro_technosphere_name
from brightpath.utils import (
    convert_sd_to_sd2,
    get_simapro_biosphere,
    get_simapro_subcompartments,
    get_simapro_units,
    get_subcategory,
    is_a_waste_treatment,
    is_activity_waste_treatment,
    is_blacklisted,
    round_floats_in_string,
)

from .base import FormatDescriptor, coerce_format_descriptor

_STAGE = StageKind.CONVERSION_PREFLIGHT
_BRIGHTWAY_FORMATS = frozenset({"brightway_excel", "brightway_csv", "brightway_tsv"})
_SUPPORTED_FORMATS = _BRIGHTWAY_FORMATS | {"simapro_csv"}
_SIMAPRO_BIOSPHERE_CATEGORIES = frozenset({"natural resource", "air", "water", "soil"})
_STRICT_CONVERSION_POLICY = ConversionPolicy.strict()

_SIMAPRO_DOCUMENT_METADATA = frozenset({"Project", "system description", "literature reference"})
_SIMAPRO_ACTIVITY_METADATA = frozenset(field.lower() for field in _ACTIVITY_METADATA_FIELDS)
_SIMAPRO_DATASET_FIELDS = frozenset(
    {
        "name",
        "reference product",
        "location",
        "unit",
        "exchanges",
        "parameters",
        "type",
        "comment",
        "source",
        "simapro name",
        "simapro metadata",
        *_SIMAPRO_ACTIVITY_METADATA,
    }
)
_SIMAPRO_METADATA_FIELDS = frozenset({"Process name", "Category type", *_ACTIVITY_METADATA_FIELDS})
_SIMAPRO_PARAMETER_FIELDS = frozenset(
    {
        "name",
        "amount",
        "formula",
        "loc",
        "uncertainty type",
        "scale",
        "shape",
        "minimum",
        "maximum",
        "min",
        "max",
        "hidden",
        "comment",
    }
)
_SIMAPRO_EXCHANGE_COMMON_FIELDS = frozenset(
    {
        "type",
        "name",
        "unit",
        "amount",
        "comment",
        "formula",
        "uncertainty type",
        "loc",
        "scale",
        "shape",
        "minimum",
        "maximum",
        "min",
        "max",
        "simapro name",
    }
)
_UNCERTAINTY_FIELDS = frozenset({"uncertainty type", "loc", "scale", "shape", "minimum", "maximum", "min", "max"})


def preflight_conversion(
    document: InventoryDocument,
    target_format: FormatDescriptor | object | str,
    policy: ConversionPolicy = _STRICT_CONVERSION_POLICY,
) -> StageReport:
    """Inspect whether *document* is representable in *target_format*.

    :param document: Immutable-boundary inventory document to inspect.
    :param target_format: Explicit adapter descriptor, format profile, enum, or
        format identifier.
    :param policy: Severity decisions for unsupported features, information
        loss, and invalid target data.
    :return: An immutable conversion-preflight stage report.
    """

    if not isinstance(document, InventoryDocument):
        raise TypeError("document must be an InventoryDocument.")
    if not isinstance(policy, ConversionPolicy):
        raise TypeError("policy must be a ConversionPolicy.")
    descriptor = coerce_format_descriptor(target_format)

    if descriptor.format_id in _BRIGHTWAY_FORMATS:
        findings = _preflight_brightway(document, policy)
    elif descriptor.format_id == "simapro_csv":
        findings = _preflight_simapro(document, policy)
    else:
        findings = _Findings()
        findings.add_condition(
            code="conversion.target_format_unsupported",
            message=f"No representability contract is available for target format {descriptor.label()}.",
            path="context.format",
            action=policy.on_invalid_target,
            details={"supported_formats": sorted(_SUPPORTED_FORMATS)},
            suggested_fix="Choose a target format with a registered writer and representability contract.",
        )

    return StageReport(
        _STAGE,
        label=f"{descriptor.label()} representability",
        issues=tuple(findings.issues),
        changes=tuple(findings.changes),
        losses=tuple(findings.losses),
        metrics={
            "datasets": len(document.data),
            "policy": policy.to_dict(),
            "target_format": {
                "format_id": descriptor.format_id,
                "version": descriptor.version,
                "dialect": descriptor.dialect,
            },
        },
    )


class _Findings:
    """Mutable local collector whose values are frozen by :class:`StageReport`."""

    def __init__(self) -> None:
        self.issues: list[Issue] = []
        self.changes: list[Change] = []
        self.losses: list[Loss] = []

    def add_loss(
        self,
        *,
        code: str,
        message: str,
        path: str,
        action: PolicyAction,
        category: str,
        details: Mapping[str, Any] | None = None,
        recoverable: bool = False,
        suggested_fix: str = "",
    ) -> None:
        loss = Loss(
            code=code,
            message=message,
            stage=_STAGE,
            path=path,
            recoverable=recoverable,
            details=dict(details or {}),
        )
        self.losses.append(loss)
        self.add_condition(
            code=f"conversion.{category}",
            message=message,
            path=path,
            action=action,
            details={"loss_code": code, "policy_action": action.value, **dict(details or {})},
            suggested_fix=suggested_fix or _policy_suggested_fix(category),
        )

    def add_condition(
        self,
        *,
        code: str,
        message: str,
        path: str,
        action: PolicyAction,
        details: Mapping[str, Any] | None = None,
        suggested_fix: str = "",
    ) -> None:
        self.issues.append(
            Issue(
                severity=_policy_severity(action),
                code=code,
                message=message,
                stage=_STAGE,
                path=path,
                details=dict(details or {}),
                suggested_fix=suggested_fix,
            )
        )

    def add_change(
        self,
        *,
        code: str,
        message: str,
        path: str,
        before: Any = None,
        after: Any = None,
        details: Mapping[str, Any] | None = None,
    ) -> None:
        self.changes.append(
            Change(
                code=code,
                message=message,
                stage=_STAGE,
                path=path,
                before=before,
                after=after,
                details=dict(details or {}),
            )
        )


def _policy_severity(action: PolicyAction) -> Severity:
    if action is PolicyAction.ERROR:
        return Severity.ERROR
    if action is PolicyAction.WARN:
        return Severity.WARNING
    return Severity.INFO


def _policy_suggested_fix(category: str) -> str:
    if category == "unsupported_feature":
        return "Remove the unsupported feature, choose another target, or explicitly allow it in the policy."
    return "Choose a lossless target or explicitly allow this information loss in the policy."


def _preflight_brightway(document: InventoryDocument, policy: ConversionPolicy) -> _Findings:
    findings = _Findings()
    for dataset_index, dataset in enumerate(document.data):
        if not isinstance(dataset, Mapping):
            continue
        for exchange_index, exchange in enumerate(dataset.get("exchanges", ())):
            if not isinstance(exchange, Mapping):
                continue
            fields = sorted({"input", "output"}.intersection(exchange))
            if not fields:
                continue
            path = f"datasets[{dataset_index}].exchanges[{exchange_index}]"
            findings.add_loss(
                code="brightway_exchange_link_fields_omitted",
                message=f"Brightway writers omit exchange link field(s): {', '.join(fields)}.",
                path=path,
                action=policy.on_information_loss,
                category="information_loss",
                details={"fields": fields},
                suggested_fix="Remove transient input/output keys or retain the source artifact alongside the export.",
            )
    return findings


def _preflight_simapro(document: InventoryDocument, policy: ConversionPolicy) -> _Findings:
    findings = _Findings()
    data = document.data

    render_result = None
    try:
        render_result = render_simapro_rows(document)
    except Exception as error:  # The public preflight always returns a report.
        findings.add_condition(
            code="conversion.preflight_failed",
            message=str(error) or type(error).__name__,
            path="",
            action=policy.on_invalid_target,
            details={"exception_type": type(error).__name__, "target_format": "simapro_csv"},
            suggested_fix="Correct the inventory data rejected by the SimaPro renderer.",
        )
    else:
        _translate_renderer_issues(render_result.issues, findings, policy)

    _inspect_document_metadata(document.metadata, findings, policy)
    _inspect_parameter_collection(document.database_parameters, "database_parameters", findings, policy)
    _inspect_parameter_collection(document.project_parameters, "project_parameters", findings, policy)

    for dataset_index, dataset in enumerate(data):
        if not isinstance(dataset, Mapping):
            continue
        _inspect_simapro_dataset(document, dataset, dataset_index, findings, policy)

    _inspect_latin1(document, data, getattr(render_result, "rows", ()), findings, policy)

    if render_result is not None and not any(issue.severity == "error" for issue in render_result.issues):
        _record_simapro_representation_changes(document, data, findings, policy)
    return findings


def _translate_renderer_issues(issues: Iterable[object], findings: _Findings, policy: ConversionPolicy) -> None:
    for legacy_issue in issues:
        code = str(getattr(legacy_issue, "code", "simapro_serialization_failed"))
        if code == "simapro_exchange_unused":
            # Exchange paths and specific omission reasons are recorded below.
            continue
        findings.add_condition(
            code=code,
            message=str(getattr(legacy_issue, "message", legacy_issue)),
            path=_canonical_path(str(getattr(legacy_issue, "path", ""))),
            action=policy.on_invalid_target,
            suggested_fix=str(getattr(legacy_issue, "suggested_fix", "")),
            details={"policy_action": policy.on_invalid_target.value, "source": "simapro_renderer"},
        )


def _inspect_document_metadata(metadata: Mapping[str, Any], findings: _Findings, policy: ConversionPolicy) -> None:
    unsupported = sorted(set(metadata) - _SIMAPRO_DOCUMENT_METADATA)
    invalid_nested = sorted(
        field
        for field in ("system description", "literature reference")
        if field in metadata and not isinstance(metadata[field], Mapping)
    )
    unsupported.extend(field for field in invalid_nested if field not in unsupported)
    if unsupported:
        _add_unsupported_fields(findings, policy, "metadata", "metadata", unsupported)


def _inspect_simapro_dataset(
    document: InventoryDocument,
    dataset: Mapping[str, Any],
    dataset_index: int,
    findings: _Findings,
    policy: ConversionPolicy,
) -> None:
    path = f"datasets[{dataset_index}]"
    unsupported = set(dataset) - _SIMAPRO_DATASET_FIELDS
    if dataset.get("product") in (None, "", dataset.get("reference product")):
        unsupported.discard("product")
    _add_unsupported_fields(findings, policy, path, "dataset", sorted(unsupported))

    simapro_metadata = dataset.get("simapro metadata")
    if simapro_metadata is not None:
        if isinstance(simapro_metadata, Mapping):
            _add_unsupported_fields(
                findings,
                policy,
                f"{path}.simapro_metadata",
                "dataset metadata",
                sorted(set(simapro_metadata) - _SIMAPRO_METADATA_FIELDS),
            )
        else:
            _add_unsupported_fields(findings, policy, path, "dataset", ["simapro metadata"])

    _inspect_parameter_collection(dataset.get("parameters"), f"{path}.parameters", findings, policy)

    try:
        waste_activity = is_activity_waste_treatment(dict(dataset), document.background_profile.family)
    except (KeyError, TypeError, ValueError):
        waste_activity = False
    exchanges = dataset.get("exchanges", ())
    if not isinstance(exchanges, Iterable) or isinstance(exchanges, (str, bytes, Mapping)):
        return
    for exchange_index, exchange in enumerate(exchanges):
        if not isinstance(exchange, Mapping):
            continue
        _inspect_simapro_exchange(
            document,
            exchange,
            f"{path}.exchanges[{exchange_index}]",
            waste_activity,
            findings,
            policy,
        )


def _inspect_simapro_exchange(
    document: InventoryDocument,
    exchange: Mapping[str, Any],
    path: str,
    waste_activity: bool,
    findings: _Findings,
    policy: ConversionPolicy,
) -> None:
    exchange_type = str(exchange.get("type") or "")
    supported = set(_SIMAPRO_EXCHANGE_COMMON_FIELDS)
    if exchange_type in {"production", "technosphere", "substitution"}:
        supported.update({"reference product", "location"})
    if exchange_type == "production":
        supported.add("simapro category")
    if exchange_type == "biosphere":
        supported.add("categories")
    if is_simapro_final_waste_flow(dict(exchange)):
        supported.add("categories")

    unsupported = set(exchange) - supported
    if exchange.get("product") in (None, "", exchange.get("reference product")):
        unsupported.discard("product")
    _add_unsupported_fields(findings, policy, path, "exchange", sorted(unsupported))

    if exchange.get("formula") not in (None, ""):
        findings.add_loss(
            code="simapro_exchange_formula_unsupported",
            message="SimaPro CSV exchange rows do not preserve canonical exchange formulas.",
            path=f"{path}.formula",
            action=policy.on_unsupported_feature,
            category="unsupported_feature",
            details={"formula": str(exchange["formula"])},
        )

    status = _simapro_exchange_status(document, exchange)
    if status == "final_waste":
        findings.add_loss(
            code="simapro_final_waste_exchange_unsupported",
            message="The current SimaPro writer does not preserve Final waste flows exchange semantics.",
            path=path,
            action=policy.on_unsupported_feature,
            category="unsupported_feature",
            details={"exchange_type": exchange_type},
        )
    elif status == "substitution":
        findings.add_loss(
            code="simapro_substitution_exchange_unsupported",
            message="The current SimaPro writer cannot preserve a canonical substitution exchange as such.",
            path=path,
            action=policy.on_unsupported_feature,
            category="unsupported_feature",
            details={"exchange_type": exchange_type},
        )
    elif status == "blacklisted":
        findings.add_loss(
            code="simapro_exchange_blacklisted",
            message=f"Blacklisted exchange {exchange.get('name')!r} is omitted from SimaPro output.",
            path=path,
            action=policy.on_information_loss,
            category="information_loss",
            details={"exchange_type": exchange_type},
        )
    elif status == "unused":
        findings.add_loss(
            code="simapro_exchange_unused",
            message=f"Exchange {exchange.get('name')!r} is not represented in SimaPro output.",
            path=path,
            action=policy.on_information_loss,
            category="information_loss",
            details={"exchange_type": exchange_type},
        )

    if status != "used":
        return

    amount = exchange.get("amount")
    effective_amount = _effective_simapro_amount(
        document,
        exchange,
        amount,
        exchange_type=exchange_type,
        waste_activity=waste_activity,
    )
    if _is_finite_number(amount) and _is_finite_number(effective_amount) and float(amount) != float(effective_amount):
        if not _is_water_conversion(exchange):
            findings.add_loss(
                code="simapro_exchange_sign_normalized",
                message="SimaPro normalizes the sign of this waste-related exchange amount.",
                path=f"{path}.amount",
                action=policy.on_information_loss,
                category="information_loss",
                details={"before": float(amount), "after": float(effective_amount)},
            )

    _add_rounding_loss(
        findings,
        policy,
        path=f"{path}.amount",
        value=effective_amount,
        precision=".3E",
        code="simapro_exchange_amount_rounded",
    )
    _add_uncertainty_loss(exchange, path, ".3E", findings, policy)


def _simapro_exchange_status(document: InventoryDocument, exchange: Mapping[str, Any]) -> str:
    amount = exchange.get("amount")
    if _is_number(amount) and float(amount) == 0:
        return "zero"
    exchange_type = str(exchange.get("type") or "")
    if exchange_type == "production":
        return "used"
    if is_simapro_final_waste_flow(dict(exchange)):
        return "final_waste"
    if exchange_type == "substitution":
        return "substitution"
    if exchange_type not in {"technosphere", "biosphere"}:
        return "unused"
    try:
        if is_blacklisted(dict(exchange), document.background_profile.family):
            return "blacklisted"
    except (KeyError, TypeError):
        return "unused"
    if exchange_type == "biosphere":
        categories = exchange.get("categories")
        if not isinstance(categories, (tuple, list)) or not categories:
            return "unused"
        if str(categories[0]) not in _SIMAPRO_BIOSPHERE_CATEGORIES:
            return "unused"
    return "used"


def _effective_simapro_amount(
    document: InventoryDocument,
    exchange: Mapping[str, Any],
    amount: Any,
    *,
    exchange_type: str,
    waste_activity: bool,
) -> Any:
    if not _is_number(amount):
        return amount
    result = float(amount)
    if exchange_type == "production" and waste_activity and result < 0:
        result = abs(result)
    elif exchange_type == "technosphere":
        try:
            waste_exchange = is_a_waste_treatment(str(exchange.get("name") or ""), document.background_profile.family)
        except (KeyError, TypeError, ValueError):
            waste_exchange = False
        if waste_exchange or (waste_activity and result < 0):
            result = abs(result)
    if _is_water_conversion(exchange):
        result *= 1000
    return result


def _is_water_conversion(exchange: Mapping[str, Any]) -> bool:
    categories = exchange.get("categories")
    return (
        exchange.get("type") == "biosphere"
        and isinstance(categories, (tuple, list))
        and bool(categories)
        and categories[0] != "natural resource"
        and str(exchange.get("name") or "").lower() == "water"
    )


def _inspect_parameter_collection(
    parameters: Any,
    path: str,
    findings: _Findings,
    policy: ConversionPolicy,
) -> None:
    if not isinstance(parameters, list):
        return
    for index, parameter in enumerate(parameters):
        if not isinstance(parameter, Mapping):
            continue
        parameter_path = f"{path}[{index}]"
        unsupported = set(parameter) - _SIMAPRO_PARAMETER_FIELDS
        if parameter.get("formula") and "amount" in parameter:
            unsupported.add("amount")
        _add_unsupported_fields(findings, policy, parameter_path, "parameter", sorted(unsupported))

        if parameter.get("formula"):
            ignored = sorted(set(parameter).intersection({"uncertainty type", *_UNCERTAINTY_FIELDS, "hidden"}))
            _add_unsupported_fields(findings, policy, parameter_path, "calculated parameter", ignored)
            continue

        amount = parameter.get("amount", parameter.get("loc"))
        _add_rounding_loss(
            findings,
            policy,
            path=f"{parameter_path}.amount",
            value=amount,
            precision=".12g",
            code="simapro_parameter_amount_rounded",
        )
        _add_uncertainty_loss(parameter, parameter_path, ".12g", findings, policy)


def _add_uncertainty_loss(
    value: Mapping[str, Any],
    path: str,
    precision: str,
    findings: _Findings,
    policy: ConversionPolicy,
) -> None:
    present = sorted(set(value).intersection(_UNCERTAINTY_FIELDS))
    if not present:
        return

    transformations: dict[str, Any] = {}
    uncertainty_type = value.get("uncertainty type")
    rendered_type = _simapro_uncertainty_type(uncertainty_type)
    if "uncertainty type" in value and uncertainty_type not in (None, 0, 1):
        transformations["uncertainty_type"] = {"before": _json_scalar(uncertainty_type), "after": rendered_type}
    if uncertainty_type not in (None, 0, 1, 2, 3, 4, 5):
        transformations["unsupported_uncertainty_type"] = str(uncertainty_type)

    if "scale" in value and _is_finite_number(value.get("scale")):
        converted_scale = convert_sd_to_sd2(float(value["scale"]), rendered_type)
        rendered_scale = _formatted_number(converted_scale, precision)
        if float(value["scale"]) != rendered_scale:
            transformations["scale"] = {"before": float(value["scale"]), "after": rendered_scale}
    for field in ("loc", "shape"):
        if field in value and value[field] is not None:
            transformations[field] = {"before": _json_scalar(value[field]), "after": None}
    for canonical, alias in (("minimum", "min"), ("maximum", "max")):
        if canonical in value and alias in value and value[canonical] != value[alias]:
            transformations[canonical] = {
                "before": _json_scalar(value[canonical]),
                "after": _json_scalar(value[alias]),
            }
        selected = value.get(alias, value.get(canonical))
        if _is_finite_number(selected):
            rendered = _formatted_number(selected, precision)
            if float(selected) != rendered:
                transformations[f"{canonical}_rounding"] = {"before": float(selected), "after": rendered}

    if transformations:
        findings.add_loss(
            code="simapro_uncertainty_transformed",
            message="SimaPro transforms or omits part of this uncertainty representation.",
            path=path,
            action=policy.on_information_loss,
            category="information_loss",
            details={"fields": present, "transformations": transformations},
        )


def _add_rounding_loss(
    findings: _Findings,
    policy: ConversionPolicy,
    *,
    path: str,
    value: Any,
    precision: str,
    code: str,
) -> None:
    if not _is_finite_number(value):
        return
    rendered = _formatted_number(value, precision)
    numeric = float(value)
    if numeric == rendered:
        return
    findings.add_loss(
        code=code,
        message=f"SimaPro numeric precision changes {numeric!r} to {rendered!r}.",
        path=path,
        action=policy.on_information_loss,
        category="information_loss",
        details={"before": numeric, "after": rendered, "precision": precision},
    )


def _formatted_number(value: Any, precision: str) -> float:
    return float(format(float(value), precision))


def _add_unsupported_fields(
    findings: _Findings,
    policy: ConversionPolicy,
    path: str,
    object_kind: str,
    fields: Iterable[str],
) -> None:
    selected = sorted(set(fields))
    if not selected:
        return
    findings.add_loss(
        code=f"simapro_{object_kind.replace(' ', '_')}_fields_unsupported",
        message=f"SimaPro CSV does not preserve {object_kind} field(s): {', '.join(selected)}.",
        path=path,
        action=policy.on_unsupported_feature,
        category="unsupported_feature",
        details={"fields": selected, "object_kind": object_kind},
    )


def _inspect_latin1(
    document: InventoryDocument,
    data: list[dict],
    rows: Iterable[Iterable[Any]],
    findings: _Findings,
    policy: ConversionPolicy,
) -> None:
    source_values = list(_simapro_rendered_source_strings(document, data))
    failing_paths = sorted({path for path, value in source_values if not _is_latin1(value)})

    # Mapping tables can introduce output text not present verbatim in source.
    rendered_failures = []
    for row_index, row in enumerate(rows):
        for column_index, value in enumerate(row):
            if isinstance(value, str) and not _is_latin1(value):
                rendered_failures.append(f"rendered_rows[{row_index}][{column_index}]")
    if not failing_paths and not rendered_failures:
        return

    paths = sorted(set(failing_paths + rendered_failures))
    findings.add_loss(
        code="simapro_latin1_encoding_unsupported",
        message=f"SimaPro CSV Latin-1 encoding cannot represent text at {len(paths)} path(s).",
        path=paths[0] if len(paths) == 1 else "",
        action=policy.on_unsupported_feature,
        category="unsupported_feature",
        details={"paths": paths},
        suggested_fix="Replace unsupported Unicode characters or choose a Unicode-capable target format.",
    )


def _simapro_rendered_source_strings(
    document: InventoryDocument,
    data: list[dict],
) -> Iterable[tuple[str, str]]:
    yield from _walk_strings(document.database_name, "database_name")
    for key in _SIMAPRO_DOCUMENT_METADATA.intersection(document.metadata):
        yield from _walk_strings(document.metadata[key], f"metadata.{_path_key(key)}")
    for scope, parameters in (
        ("database_parameters", document.database_parameters),
        ("project_parameters", document.project_parameters),
    ):
        yield from _parameter_strings(parameters, scope)
    for dataset_index, dataset in enumerate(data):
        if not isinstance(dataset, Mapping):
            continue
        path = f"datasets[{dataset_index}]"
        for key in _SIMAPRO_DATASET_FIELDS.intersection(dataset):
            if key in {"exchanges", "parameters", "simapro metadata", "product", "simapro name"}:
                continue
            yield from _walk_strings(dataset[key], f"{path}.{_path_key(key)}")
        metadata = dataset.get("simapro metadata")
        if isinstance(metadata, Mapping):
            for key in _SIMAPRO_METADATA_FIELDS.intersection(metadata):
                yield from _walk_strings(metadata[key], f"{path}.simapro_metadata.{_path_key(key)}")
        yield from _parameter_strings(dataset.get("parameters"), f"{path}.parameters")
        exchanges = dataset.get("exchanges")
        if not isinstance(exchanges, list):
            continue
        for exchange_index, exchange in enumerate(exchanges):
            if not isinstance(exchange, Mapping):
                continue
            if _simapro_exchange_status(document, exchange) not in {"used", "zero"}:
                continue
            exchange_path = f"{path}.exchanges[{exchange_index}]"
            exchange_type = exchange.get("type")
            rendered_fields = {"name", "unit", "comment"}
            if exchange_type in {"production", "technosphere"}:
                rendered_fields.update({"reference product", "location"})
            if exchange_type == "production":
                rendered_fields.add("simapro category")
            if exchange_type == "biosphere":
                rendered_fields.add("categories")
            for key in rendered_fields.intersection(exchange):
                yield from _walk_strings(exchange[key], f"{exchange_path}.{_path_key(key)}")


def _parameter_strings(parameters: Any, path: str) -> Iterable[tuple[str, str]]:
    if not isinstance(parameters, list):
        return
    for index, parameter in enumerate(parameters):
        if not isinstance(parameter, Mapping):
            continue
        for key in {"name", "formula", "comment"}.intersection(parameter):
            yield from _walk_strings(parameter[key], f"{path}[{index}].{key}")


def _walk_strings(value: Any, path: str) -> Iterable[tuple[str, str]]:
    if isinstance(value, str):
        yield path, value
    elif isinstance(value, Mapping):
        for key in sorted(value, key=str):
            yield from _walk_strings(str(key), f"{path}.keys")
            yield from _walk_strings(value[key], f"{path}.{_path_key(str(key))}")
    elif isinstance(value, (tuple, list)):
        for index, item in enumerate(value):
            yield from _walk_strings(item, f"{path}[{index}]")


def _record_simapro_representation_changes(
    document: InventoryDocument,
    data: list[dict],
    findings: _Findings,
    policy: ConversionPolicy,
) -> None:
    findings.add_change(
        code="simapro_date_generated",
        message="SimaPro header and process dates are generated when rows are rendered.",
        path="metadata.generated_date",
        before=None,
        after="generated_at_render_time",
        details={"process_count": len(data)},
    )

    try:
        units = get_simapro_units()
        biosphere_names = get_simapro_biosphere()
        subcompartments = get_simapro_subcompartments()
    except (OSError, TypeError, ValueError):
        return

    for dataset_index, dataset in enumerate(data):
        if not isinstance(dataset, Mapping):
            continue
        dataset_path = f"datasets[{dataset_index}]"
        formatted_name = _format_technosphere(document, dataset)
        if formatted_name is not None:
            findings.add_change(
                code="simapro_technosphere_name_formatted",
                message="Canonical dataset identity is rendered using SimaPro technosphere naming.",
                path=f"{dataset_path}.name",
                before=str(dataset.get("simapro name") or dataset.get("name") or ""),
                after=formatted_name,
            )

        exchanges = dataset.get("exchanges")
        if not isinstance(exchanges, list):
            continue
        for exchange_index, exchange in enumerate(exchanges):
            if not isinstance(exchange, Mapping) or _simapro_exchange_status(document, exchange) != "used":
                continue
            exchange_path = f"{dataset_path}.exchanges[{exchange_index}]"
            exchange_type = exchange.get("type")
            if exchange_type == "technosphere":
                formatted_exchange = _format_technosphere(document, exchange)
                if formatted_exchange is not None:
                    findings.add_change(
                        code="simapro_technosphere_name_formatted",
                        message="Canonical exchange identity is rendered using SimaPro technosphere naming.",
                        path=f"{exchange_path}.name",
                        before=str(exchange.get("simapro name") or exchange.get("name") or ""),
                        after=formatted_exchange,
                    )
            elif exchange_type == "biosphere":
                rendered_name = biosphere_names.get(exchange.get("name"), exchange.get("name"))
                if rendered_name != exchange.get("name"):
                    findings.add_change(
                        code="simapro_biosphere_name_mapped",
                        message="Canonical biosphere flow name is mapped to its SimaPro name.",
                        path=f"{exchange_path}.name",
                        before=str(exchange.get("name") or ""),
                        after=str(rendered_name or ""),
                    )

            unit = exchange.get("unit")
            rendered_unit = units.get(unit)
            if rendered_unit is not None and rendered_unit != unit:
                after: Any = rendered_unit
                if _is_water_conversion(exchange):
                    after = units.get("kilogram", "kg")
                findings.add_change(
                    code="simapro_unit_mapped",
                    message="Canonical unit is rendered using SimaPro unit syntax.",
                    path=f"{exchange_path}.unit",
                    before=str(unit or ""),
                    after=str(after),
                    details={"water_mass_conversion": _is_water_conversion(exchange)},
                )
            if _is_water_conversion(exchange) and _is_finite_number(exchange.get("amount")):
                findings.add_change(
                    code="simapro_biosphere_water_mass_converted",
                    message="SimaPro emission water is rendered as mass using a deterministic factor.",
                    path=f"{exchange_path}.amount",
                    before=float(exchange["amount"]),
                    after=float(exchange["amount"]) * 1000,
                    details={"factor": 1000, "source_unit": str(unit or ""), "target_unit": "kg"},
                )

            if exchange_type == "production" and exchange.get("simapro category"):
                category = str(exchange["simapro category"])
                findings.add_change(
                    code="simapro_category_mapped",
                    message="Production category is split into SimaPro category and subcategory fields.",
                    path=f"{exchange_path}.simapro_category",
                    before=category,
                    after={"category": category.split("/")[0], "subcategory": get_subcategory(category)},
                )
            elif exchange_type == "biosphere":
                categories = exchange.get("categories")
                if isinstance(categories, (tuple, list)) and categories:
                    subcompartment = subcompartments.get(categories[1], "") if len(categories) > 1 else ""
                    findings.add_change(
                        code="simapro_category_mapped",
                        message="Biosphere categories are rendered as a SimaPro section and subcompartment.",
                        path=f"{exchange_path}.categories",
                        before=[str(item) for item in categories],
                        after={"section": str(categories[0]), "subcompartment": str(subcompartment)},
                    )

        comment = str(dataset.get("comment") or "")
        normalized_comment = round_floats_in_string(comment).replace("\n", " ")
        if comment and normalized_comment != comment:
            findings.add_loss(
                code="simapro_comment_normalized",
                message="SimaPro rendering normalizes line breaks or decimal text in the dataset comment.",
                path=f"{dataset_path}.comment",
                action=policy.on_information_loss,
                category="information_loss",
                details={"before": comment, "after": normalized_comment},
            )


def _format_technosphere(document: InventoryDocument, value: Mapping[str, Any]) -> str | None:
    try:
        return format_simapro_technosphere_name(
            name=str(value["name"]),
            reference_product=str(value["reference product"]),
            location=str(value.get("location") or "GLO"),
            unit=str(value["unit"]),
            profile=document.background_profile,
        )
    except (KeyError, TypeError, ValueError):
        return None


def _canonical_path(path: str) -> str:
    if path.startswith("activity["):
        return "datasets[" + path[len("activity[") :]
    return path


def _path_key(value: str) -> str:
    return value.replace(" ", "_").replace("-", "_")


def _is_number(value: Any) -> bool:
    return isinstance(value, Real) and not isinstance(value, bool)


def _is_finite_number(value: Any) -> bool:
    return _is_number(value) and math.isfinite(float(value))


def _json_scalar(value: Any) -> Any:
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if _is_finite_number(value):
        return float(value)
    return str(value)


def _is_latin1(value: str) -> bool:
    try:
        value.encode("latin-1")
    except UnicodeEncodeError:
        return False
    return True
