"""Dependency-injected inventory operations composed from focused services.

The pipeline is an orchestration layer. Format adapters remain responsible for
syntax, structural and background validators remain independent, and format
conversion never changes the selected background context.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from brightpath.adapters.base import ArtifactKind, FormatAdapter, FormatDescriptor, coerce_format_descriptor
from brightpath.adapters.registry import AdapterRegistry, DetectionReport
from brightpath.background.catalogs import CatalogProvider
from brightpath.background.validation import validate_background_links
from brightpath.exceptions import SerializationError
from brightpath.formats.simapro_csv import render_simapro_rows
from brightpath.models import InventoryDocument
from brightpath.normalization import normalize_inventory
from brightpath.validation.brightway import validate_brightway_inventory

from .audit import digest_artifact, write_report_sidecar
from .context import ContextHint, FormatProfile, InventoryContext
from .policies import ConversionPolicy, PolicyAction
from .reports import (
    Change,
    Issue,
    Loss,
    OperationKind,
    OperationReport,
    OperationResult,
    Severity,
    StageKind,
    StageReport,
)

_EMPTY_CONTEXT_HINT = ContextHint()
_STRICT_CONVERSION_POLICY = ConversionPolicy.strict()


@dataclass(frozen=True)
class InventoryPipeline:
    """Compose format, normalization, validation, and write services.

    The adapter registry and catalog provider are immutable injected
    dependencies. Construct application defaults at the application boundary;
    this module intentionally owns no process-global registry or provider.

    :param registry: Adapters available to this pipeline instance.
    :param catalog_provider: Exact background catalogs used during validation.
    """

    registry: AdapterRegistry
    catalog_provider: CatalogProvider

    def __post_init__(self) -> None:
        if not isinstance(self.registry, AdapterRegistry):
            raise TypeError("registry must be an AdapterRegistry.")
        if not isinstance(self.catalog_provider, CatalogProvider):
            raise TypeError("catalog_provider must implement CatalogProvider.")

    def detect(
        self,
        artifact: object,
        *,
        artifact_kind: ArtifactKind | str = ArtifactKind.FILE,
        explicit_format: object | None = None,
        minimum_confidence: float = 0.5,
        tie_tolerance: float = 1e-9,
    ) -> OperationResult[FormatDescriptor | None]:
        """Detect an artifact format without guessing when evidence is tied.

        Detection candidates and probe failures are retained in the stage
        metrics and issues. An absent, low-confidence, or ambiguous match is a
        normal error result with a ``None`` value.
        """

        detection = self.registry.detect(
            artifact,
            artifact_kind=artifact_kind,
            explicit_format=explicit_format,
            minimum_confidence=minimum_confidence,
            tie_tolerance=tie_tolerance,
        )
        stage = _detection_stage(detection)
        report = OperationReport(
            OperationKind.ANALYZE,
            stages=(stage,),
            metadata={"artifact_kind": ArtifactKind(artifact_kind).value},
        )
        return OperationResult(detection.detected_format, report)

    def read(
        self,
        artifact: object,
        *,
        hint: ContextHint = _EMPTY_CONTEXT_HINT,
        explicit_format: object | None = None,
        artifact_kind: ArtifactKind | str = ArtifactKind.FILE,
        adapter_kwargs: Mapping[str, Any] | None = None,
    ) -> OperationResult[InventoryDocument | None]:
        """Detect and parse an inventory artifact into an inventory document.

        A complete background hint is forwarded with the selected source
        format as an exact :class:`InventoryContext`. Reader failures are
        represented by a parse-stage issue.
        """

        if not isinstance(hint, ContextHint):
            raise TypeError("hint must be a ContextHint.")
        kwargs = _copy_adapter_kwargs(adapter_kwargs)
        selected_explicit = explicit_format
        if selected_explicit is None and hint.format is not None:
            selected_explicit = _registered_profile_descriptor(self.registry, hint.format)

        detection_result = self.detect(
            artifact,
            artifact_kind=artifact_kind,
            explicit_format=selected_explicit,
        )
        stages = list(detection_result.report.stages)
        descriptor = detection_result.value
        if descriptor is None:
            return OperationResult(
                None,
                OperationReport(
                    OperationKind.READ,
                    stages=tuple(stages),
                    metadata={"artifact_kind": ArtifactKind(artifact_kind).value},
                ),
            )

        format_conflict = _format_hint_conflict(hint.format, descriptor)
        if format_conflict is not None:
            stages.append(StageReport(StageKind.PARSE, label="adapter read", issues=(format_conflict,)))
            return OperationResult(None, OperationReport(OperationKind.READ, stages=tuple(stages)))

        try:
            adapter = self.registry.get(descriptor)
            if hint.background is not None:
                source_format = hint.format or _format_profile(descriptor)
                kwargs.setdefault("context", InventoryContext(format=source_format, background=hint.background))
            document = adapter.read(artifact, **kwargs)
            if not isinstance(document, InventoryDocument):
                raise TypeError(
                    f"The {descriptor.label()} adapter returned {type(document).__name__}, not InventoryDocument."
                )
            if document.context.format.format_id != descriptor.format_id:
                raise ValueError(
                    "Parsed inventory context format {!r} does not match selected adapter {!r}.".format(
                        document.context.format.format_id,
                        descriptor.format_id,
                    )
                )
        except Exception as error:
            stages.append(
                StageReport(
                    StageKind.PARSE,
                    label="adapter read",
                    issues=(
                        Issue(
                            Severity.ERROR,
                            "parse.failed",
                            str(error) or type(error).__name__,
                            StageKind.PARSE,
                            details={"format": _descriptor_details(descriptor)},
                        ),
                    ),
                )
            )
            return OperationResult(
                None,
                OperationReport(
                    OperationKind.READ,
                    stages=tuple(stages),
                    metadata={"source_format": descriptor.format_id},
                ),
            )

        stages.append(
            StageReport(
                StageKind.PARSE,
                label="adapter read",
                metrics={
                    "datasets": len(document.data),
                    "format": _format_profile_details(document.context.format),
                },
            )
        )
        return OperationResult(
            document,
            OperationReport(
                OperationKind.READ,
                stages=tuple(stages),
                metadata={"source_format": descriptor.format_id},
            ),
        )

    def normalize(self, document: InventoryDocument) -> OperationResult[InventoryDocument | None]:
        """Return a safely normalized copy of *document*."""

        _require_document(document)
        before = document.data
        try:
            normalized = normalize_inventory(document)
            after = normalized.data
        except Exception as error:
            stage = StageReport(
                StageKind.NORMALIZE,
                issues=(
                    Issue(
                        Severity.ERROR,
                        "normalization.failed",
                        str(error) or type(error).__name__,
                        StageKind.NORMALIZE,
                    ),
                ),
            )
            return OperationResult(None, OperationReport(OperationKind.NORMALIZE, stages=(stage,)))

        changes: tuple[Change, ...] = ()
        if before != after:
            changes = (
                Change(
                    "inventory.normalized",
                    "Canonical inventory fields were normalized.",
                    StageKind.NORMALIZE,
                    before=_inventory_summary(before),
                    after=_inventory_summary(after),
                ),
            )
        stage = StageReport(
            StageKind.NORMALIZE,
            changes=changes,
            metrics={"datasets": len(after), "changed": bool(changes)},
        )
        return OperationResult(normalized, OperationReport(OperationKind.NORMALIZE, stages=(stage,)))

    def validate(
        self,
        document: InventoryDocument,
        *,
        check_background_links: bool = True,
        additional_foreground_targets: Iterable[tuple[str, str, str, str]] = (),
    ) -> OperationResult[InventoryDocument]:
        """Validate structure and, independently, exact background links."""

        _require_document(document)
        targets = tuple(additional_foreground_targets)
        stages = [self._structural_validation_stage(document)]
        if check_background_links:
            try:
                background_stage = validate_background_links(
                    document.data,
                    document.context.background,
                    self.catalog_provider,
                    foreground_technosphere_targets=targets,
                )
            except TypeError:
                raise
            except Exception as error:
                background_stage = StageReport(
                    StageKind.BACKGROUND_VALIDATION,
                    label="background links",
                    issues=(
                        Issue(
                            Severity.ERROR,
                            "background.validation_failed",
                            str(error) or type(error).__name__,
                            StageKind.BACKGROUND_VALIDATION,
                        ),
                    ),
                )
            stages.append(background_stage)
        return OperationResult(document, OperationReport(OperationKind.VALIDATE, stages=tuple(stages)))

    def convert(
        self,
        document: InventoryDocument,
        target_format: object,
        *,
        policy: ConversionPolicy = _STRICT_CONVERSION_POLICY,
    ) -> OperationResult[InventoryDocument | None]:
        """Preflight a format conversion and change only the format context."""

        _require_document(document)
        _require_conversion_policy(policy)
        descriptor = coerce_format_descriptor(target_format)
        converted, _adapter, stages = self._prepare_conversion(document, descriptor, policy)
        return OperationResult(
            converted,
            OperationReport(
                OperationKind.CONVERT,
                stages=stages,
                metadata={
                    "source_format": document.context.format.format_id,
                    "target_format": descriptor.format_id,
                },
            ),
        )

    def write(
        self,
        document: InventoryDocument,
        artifact: str | Path,
        *,
        target_format: object | None = None,
        policy: ConversionPolicy = _STRICT_CONVERSION_POLICY,
        sidecar: str | Path | bool | None = None,
        adapter_kwargs: Mapping[str, Any] | None = None,
    ) -> OperationResult[Path | None]:
        """Preflight and write an inventory, optionally with an audit sidecar.

        A ``True`` *sidecar* value writes ``<artifact>.brightpath.json``. A
        path value selects the sidecar explicitly. Serialization and filesystem
        failures are reported rather than raised.
        """

        _require_document(document)
        _require_conversion_policy(policy)
        _validate_sidecar_argument(sidecar)
        kwargs = _copy_adapter_kwargs(adapter_kwargs)
        descriptor = (
            coerce_format_descriptor(target_format)
            if target_format is not None
            else _registered_profile_descriptor(self.registry, document.context.format)
        )
        converted, adapter, prepared_stages = self._prepare_conversion(document, descriptor, policy)
        stages = list(prepared_stages)
        metadata = {
            "source_format": document.context.format.format_id,
            "target_format": descriptor.format_id,
            "sidecar": bool(sidecar),
        }
        if converted is None or adapter is None:
            return OperationResult(None, OperationReport(OperationKind.WRITE, stages=tuple(stages), metadata=metadata))

        try:
            adapter_result = adapter.write(converted, artifact, **kwargs)
            output, returned_render = _adapter_output(adapter_result)
        except Exception as error:
            stage_kind = StageKind.SERIALIZATION if isinstance(error, SerializationError) else StageKind.WRITE
            code = "serialization.failed" if stage_kind is StageKind.SERIALIZATION else "write.failed"
            stages.append(
                StageReport(
                    stage_kind,
                    label="adapter write",
                    issues=(
                        Issue(
                            Severity.ERROR,
                            code,
                            str(error) or type(error).__name__,
                            stage_kind,
                            details={"format": _descriptor_details(descriptor)},
                        ),
                    ),
                )
            )
            return OperationResult(None, OperationReport(OperationKind.WRITE, stages=tuple(stages), metadata=metadata))

        serialization_issues = _returned_render_issues(returned_render)
        stages.append(
            StageReport(
                StageKind.SERIALIZATION,
                label="adapter serialization",
                issues=serialization_issues,
                metrics={"format": descriptor.format_id},
            )
        )
        stages.append(
            StageReport(
                StageKind.WRITE,
                label="artifact write",
                metrics={"output": str(output)},
            )
        )
        report = OperationReport(OperationKind.WRITE, stages=tuple(stages), metadata=metadata)

        sidecar_path = _sidecar_path(sidecar, output)
        if sidecar_path is not None:
            try:
                digest = digest_artifact(output, role="output")
                write_report_sidecar(report, sidecar_path, artifacts=(digest,))
            except Exception as error:
                stages.append(
                    StageReport(
                        StageKind.WRITE,
                        label="audit sidecar",
                        issues=(
                            Issue(
                                Severity.ERROR,
                                "write.sidecar_failed",
                                str(error) or type(error).__name__,
                                StageKind.WRITE,
                                path=str(sidecar_path),
                            ),
                        ),
                    )
                )
                report = OperationReport(OperationKind.WRITE, stages=tuple(stages), metadata=metadata)
        return OperationResult(output, report)

    def _structural_validation_stage(self, document: InventoryDocument) -> StageReport:
        try:
            legacy = validate_brightway_inventory(document, check_background_links=False)
            issues = tuple(
                _legacy_issue(issue, StageKind.STRUCTURAL_VALIDATION) for issue in getattr(legacy, "issues", ())
            )
        except Exception as error:
            issues = (
                Issue(
                    Severity.ERROR,
                    "structural_validation.failed",
                    str(error) or type(error).__name__,
                    StageKind.STRUCTURAL_VALIDATION,
                ),
            )
        return StageReport(
            StageKind.STRUCTURAL_VALIDATION,
            label="canonical inventory structure",
            issues=issues,
            metrics={"datasets": len(document.data)},
        )

    def _prepare_conversion(
        self,
        document: InventoryDocument,
        descriptor: FormatDescriptor,
        policy: ConversionPolicy,
    ) -> tuple[InventoryDocument | None, FormatAdapter | None, tuple[StageReport, ...]]:
        adapter, capability_issues = self._writable_adapter(descriptor)
        preflight_issues = list(capability_issues)
        losses: list[Loss] = []

        if adapter is not None and descriptor.format_id == "simapro_csv":
            try:
                rendered = render_simapro_rows(document)
            except Exception as error:
                preflight_issues.append(
                    Issue(
                        Severity.ERROR,
                        "conversion.preflight_failed",
                        str(error) or type(error).__name__,
                        StageKind.CONVERSION_PREFLIGHT,
                        details={"format": _descriptor_details(descriptor)},
                    )
                )
            else:
                for legacy_issue in rendered.issues:
                    if legacy_issue.code == "simapro_exchange_unused":
                        loss = Loss(
                            "simapro_exchange_unused",
                            legacy_issue.message,
                            StageKind.CONVERSION_PREFLIGHT,
                            path=str(getattr(legacy_issue, "path", "")),
                            details={"target_format": descriptor.format_id},
                        )
                        losses.append(loss)
                        policy_issue = _loss_policy_issue(loss, policy.on_information_loss)
                        if policy_issue is not None:
                            preflight_issues.append(policy_issue)
                    else:
                        preflight_issues.append(_legacy_issue(legacy_issue, StageKind.CONVERSION_PREFLIGHT))

        preflight = StageReport(
            StageKind.CONVERSION_PREFLIGHT,
            label=f"{descriptor.label()} representability",
            issues=tuple(preflight_issues),
            losses=tuple(losses),
            metrics={"target_format": _descriptor_details(descriptor)},
        )
        if preflight.has_errors or adapter is None:
            return None, adapter, (preflight,)

        source_format = document.context.format
        target_profile = _target_format_profile(source_format, descriptor)
        changes: tuple[Change, ...] = ()
        if target_profile != source_format:
            changes = (
                Change(
                    "format.context_changed",
                    "Inventory format context changed without changing its background context.",
                    StageKind.FORMAT_CONVERSION,
                    path="context.format",
                    before=_format_profile_details(source_format),
                    after=_format_profile_details(target_profile),
                ),
            )
        try:
            context = InventoryContext(format=target_profile, background=document.context.background)
            converted = document.replace(context=context)
        except Exception as error:
            conversion = StageReport(
                StageKind.FORMAT_CONVERSION,
                issues=(
                    Issue(
                        Severity.ERROR,
                        "conversion.failed",
                        str(error) or type(error).__name__,
                        StageKind.FORMAT_CONVERSION,
                    ),
                ),
            )
            return None, adapter, (preflight, conversion)

        conversion = StageReport(
            StageKind.FORMAT_CONVERSION,
            label="format context",
            changes=changes,
            metrics={"background_unchanged": converted.context.background == document.context.background},
        )
        return converted, adapter, (preflight, conversion)

    def _writable_adapter(
        self,
        descriptor: FormatDescriptor,
    ) -> tuple[FormatAdapter | None, tuple[Issue, ...]]:
        try:
            adapter = self.registry.get(descriptor)
        except LookupError as error:
            return None, (
                Issue(
                    Severity.ERROR,
                    "conversion.target_adapter_unavailable",
                    str(error),
                    StageKind.CONVERSION_PREFLIGHT,
                    details={"format": _descriptor_details(descriptor)},
                ),
            )
        if not adapter.capabilities.supports_write(ArtifactKind.FILE):
            return None, (
                Issue(
                    Severity.ERROR,
                    "conversion.target_not_writable",
                    f"The {descriptor.label()} adapter cannot write file artifacts.",
                    StageKind.CONVERSION_PREFLIGHT,
                    details={"format": _descriptor_details(descriptor)},
                ),
            )
        return adapter, ()


def _detection_stage(report: DetectionReport) -> StageReport:
    issues = tuple(
        Issue(
            severity=_severity(issue.severity),
            code=issue.code,
            message=issue.message,
            stage=StageKind.FORMAT_DETECTION,
            details={"format_id": issue.format_id} if issue.format_id else {},
        )
        for issue in report.issues
    )
    candidates = [
        {
            "format": _descriptor_details(candidate.descriptor),
            "confidence": candidate.confidence,
            "evidence": list(candidate.evidence),
        }
        for candidate in report.candidates
    ]
    return StageReport(
        StageKind.FORMAT_DETECTION,
        label="adapter evidence",
        issues=issues,
        metrics={
            "candidates": candidates,
            "selected": (_descriptor_details(report.selected.descriptor) if report.selected is not None else None),
            "explicit": (_descriptor_details(report.explicit_format) if report.explicit_format is not None else None),
        },
    )


def _legacy_issue(value: object, stage: StageKind) -> Issue:
    path = str(getattr(value, "path", ""))
    if path.startswith("activity["):
        path = "datasets[" + path[len("activity[") :]
    return Issue(
        severity=_severity(getattr(value, "severity", "error")),
        code=str(getattr(value, "code", "operation.failed")),
        message=str(getattr(value, "message", value)),
        stage=stage,
        path=path,
        suggested_fix=str(getattr(value, "suggested_fix", "")),
    )


def _severity(value: object) -> Severity:
    try:
        return Severity(str(getattr(value, "value", value)).lower())
    except ValueError:
        return Severity.ERROR


def _loss_policy_issue(loss: Loss, action: PolicyAction) -> Issue | None:
    if action is PolicyAction.ALLOW:
        return None
    severity = Severity.ERROR if action is PolicyAction.ERROR else Severity.WARNING
    return Issue(
        severity,
        "conversion.information_loss",
        loss.message,
        StageKind.CONVERSION_PREFLIGHT,
        path=loss.path,
        details={"loss_code": loss.code, "policy_action": action.value},
        suggested_fix="Choose a lossless target or explicitly use a policy that permits this loss.",
    )


def _format_hint_conflict(hint: FormatProfile | None, descriptor: FormatDescriptor) -> Issue | None:
    if hint is None:
        return None
    expected = coerce_format_descriptor(hint)
    if (
        expected.format_id == descriptor.format_id
        and (not expected.version or not descriptor.version or expected.version == descriptor.version)
        and (not expected.dialect or not descriptor.dialect or expected.dialect == descriptor.dialect)
    ):
        return None
    return Issue(
        Severity.ERROR,
        "parse.format_hint_conflict",
        f"Context format {expected.label()} conflicts with selected adapter {descriptor.label()}.",
        StageKind.PARSE,
        details={"hint": _descriptor_details(expected), "selected": _descriptor_details(descriptor)},
    )


def _target_format_profile(source: FormatProfile, descriptor: FormatDescriptor) -> FormatProfile:
    if descriptor.format_id == source.format_id and not descriptor.version and not descriptor.dialect:
        return source
    encoding = "latin-1" if descriptor.format_id == "simapro_csv" else ""
    return FormatProfile(
        descriptor.format_id,
        format_version=descriptor.version,
        dialect=descriptor.dialect,
        encoding=encoding,
    )


def _format_profile(descriptor: FormatDescriptor) -> FormatProfile:
    return _target_format_profile(FormatProfile(descriptor.format_id), descriptor)


def _registered_profile_descriptor(registry: AdapterRegistry, profile: FormatProfile) -> FormatDescriptor:
    descriptor = coerce_format_descriptor(profile)
    if registry.matching(descriptor):
        return descriptor
    return FormatDescriptor(profile.format_id)


def _descriptor_details(descriptor: FormatDescriptor) -> dict[str, str]:
    return {
        "format_id": descriptor.format_id,
        "version": descriptor.version,
        "dialect": descriptor.dialect,
    }


def _format_profile_details(profile: FormatProfile) -> dict[str, str]:
    return {
        "format_id": profile.format_id,
        "format_version": profile.format_version,
        "dialect": profile.dialect,
        "encoding": profile.encoding,
    }


def _inventory_summary(data: list[dict]) -> dict[str, object]:
    serialized = json.dumps(
        data, ensure_ascii=False, allow_nan=False, default=str, sort_keys=True, separators=(",", ":")
    )
    return {
        "datasets": len(data),
        "exchanges": sum(len(dataset.get("exchanges", ())) for dataset in data),
        "sha256": hashlib.sha256(serialized.encode("utf-8")).hexdigest(),
    }


def _adapter_output(value: object) -> tuple[Path, object | None]:
    render_result = None
    if isinstance(value, tuple):
        if not value:
            raise TypeError("Adapter write returned an empty tuple.")
        output = value[0]
        render_result = value[1] if len(value) > 1 else None
    else:
        output = value
    if not isinstance(output, (str, Path)):
        raise TypeError("Adapter write must return a path or a tuple whose first item is a path.")
    return Path(output).expanduser().resolve(), render_result


def _returned_render_issues(render_result: object | None) -> tuple[Issue, ...]:
    if render_result is None:
        return ()
    issues = []
    for issue in getattr(render_result, "issues", ()):
        if getattr(issue, "code", "") == "simapro_exchange_unused":
            continue
        issues.append(_legacy_issue(issue, StageKind.SERIALIZATION))
    return tuple(issues)


def _sidecar_path(sidecar: str | Path | bool | None, output: Path) -> Path | None:
    if sidecar in (None, False):
        return None
    if sidecar is True:
        return Path(f"{output}.brightpath.json")
    if not isinstance(sidecar, (str, Path)):
        raise TypeError("sidecar must be a path, boolean, or None.")
    return Path(sidecar).expanduser().resolve()


def _validate_sidecar_argument(sidecar: object) -> None:
    if sidecar is not None and not isinstance(sidecar, (str, Path, bool)):
        raise TypeError("sidecar must be a path, boolean, or None.")


def _copy_adapter_kwargs(value: Mapping[str, Any] | None) -> dict[str, Any]:
    if value is None:
        return {}
    if not isinstance(value, Mapping):
        raise TypeError("adapter_kwargs must be a mapping or None.")
    return dict(value)


def _require_document(document: object) -> None:
    if not isinstance(document, InventoryDocument):
        raise TypeError("document must be an InventoryDocument.")


def _require_conversion_policy(policy: object) -> None:
    if not isinstance(policy, ConversionPolicy):
        raise TypeError("policy must be a ConversionPolicy.")
