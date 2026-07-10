"""Immutable adapter registry and evidence-based format detection."""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Mapping

from .base import (
    AdapterCapabilities,
    ArtifactKind,
    DetectionCandidate,
    FormatAdapter,
    FormatDescriptor,
    coerce_format_descriptor,
)


@dataclass(frozen=True)
class DetectionIssue:
    """Structured problem encountered while detecting an artifact format."""

    severity: str
    code: str
    message: str
    format_id: str = ""


@dataclass(frozen=True)
class DetectionReport:
    """Complete, non-guessing result of format detection."""

    candidates: tuple[DetectionCandidate, ...] = ()
    selected: DetectionCandidate | None = None
    issues: tuple[DetectionIssue, ...] = ()
    explicit_format: FormatDescriptor | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "candidates", tuple(self.candidates))
        object.__setattr__(self, "issues", tuple(self.issues))

    @property
    def is_ambiguous(self) -> bool:
        """Whether two or more candidates are tied for the best confidence."""

        return any(issue.code == "format_detection_ambiguous" for issue in self.issues)

    @property
    def has_errors(self) -> bool:
        """Whether detection encountered an error that needs caller action."""

        return any(issue.severity == "error" for issue in self.issues)

    @property
    def detected_format(self) -> FormatDescriptor | None:
        """Selected format descriptor, if detection was decisive or explicit."""

        return self.selected.descriptor if self.selected else None


def _descriptor_for_adapter(adapter: FormatAdapter) -> FormatDescriptor:
    try:
        return coerce_format_descriptor(adapter.descriptor)
    except AttributeError as error:
        raise TypeError("Every adapter must expose a descriptor.") from error


def _capabilities_for_adapter(adapter: FormatAdapter) -> AdapterCapabilities:
    try:
        capabilities = adapter.capabilities
    except AttributeError as error:
        raise TypeError("Every adapter must expose capabilities.") from error
    if not isinstance(capabilities, AdapterCapabilities):
        raise TypeError("Adapter capabilities must be an AdapterCapabilities instance.")
    return capabilities


@dataclass(frozen=True)
class AdapterRegistry:
    """An immutable, dependency-injected collection of format adapters.

    Construct a registry at an application boundary and pass it to operations
    that need adapters. There is intentionally no process-global registry and
    no mutating ``register`` method.
    """

    adapters: tuple[FormatAdapter, ...] = ()
    _by_descriptor: Mapping[FormatDescriptor, FormatAdapter] = field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        adapters = tuple(self.adapters)
        by_descriptor: dict[FormatDescriptor, FormatAdapter] = {}
        for adapter in adapters:
            descriptor = _descriptor_for_adapter(adapter)
            _capabilities_for_adapter(adapter)
            if descriptor in by_descriptor:
                raise ValueError(f"Duplicate adapter descriptor: {descriptor.label()}.")
            by_descriptor[descriptor] = adapter

        object.__setattr__(self, "adapters", adapters)
        object.__setattr__(self, "_by_descriptor", MappingProxyType(by_descriptor))

    @property
    def descriptors(self) -> tuple[FormatDescriptor, ...]:
        """Descriptors registered in deterministic insertion order."""

        return tuple(self._by_descriptor)

    def matching(self, format_value: object) -> tuple[FormatAdapter, ...]:
        """Return adapters using exact, generic-fallback, then family matching.

        An exact qualified descriptor always wins. If it is absent, a single
        unqualified descriptor for the same format family is the conservative
        fallback. Only an unqualified request with no generic adapter can
        return multiple qualified adapters and require caller disambiguation.
        """

        descriptor = coerce_format_descriptor(format_value)
        exact = self._by_descriptor.get(descriptor)
        if exact is not None:
            return (exact,)

        generic = self._by_descriptor.get(FormatDescriptor(descriptor.format_id))
        if generic is not None:
            return (generic,)
        if descriptor.version or descriptor.dialect:
            return ()
        return tuple(
            adapter
            for registered_descriptor, adapter in self._by_descriptor.items()
            if registered_descriptor.format_id == descriptor.format_id
        )

    def get(self, format_value: object) -> FormatAdapter:
        """Return the sole matching adapter.

        An exact descriptor or registered generic fallback is decisive. When
        only multiple qualified versions or dialects are registered, callers
        must provide a complete descriptor.
        """

        matches = self.matching(format_value)
        descriptor = coerce_format_descriptor(format_value)
        if not matches:
            raise LookupError(f"No adapter is registered for {descriptor.label()}.")
        if len(matches) > 1:
            raise LookupError(
                f"Multiple adapters are registered for {descriptor.format_id}; specify a version or dialect."
            )
        return matches[0]

    def supports_read(self, format_value: object, artifact_kind: ArtifactKind | str) -> bool:
        """Whether one matching registered adapter can read the artifact kind."""

        return any(adapter.capabilities.supports_read(artifact_kind) for adapter in self.matching(format_value))

    def supports_write(self, format_value: object, artifact_kind: ArtifactKind | str) -> bool:
        """Whether one matching registered adapter can write the artifact kind."""

        return any(adapter.capabilities.supports_write(artifact_kind) for adapter in self.matching(format_value))

    def readable_formats(self, artifact_kind: ArtifactKind | str) -> tuple[FormatDescriptor, ...]:
        """Return registered descriptors that can read ``artifact_kind``."""

        return tuple(
            descriptor
            for descriptor, adapter in self._by_descriptor.items()
            if adapter.capabilities.supports_read(artifact_kind)
        )

    def writable_formats(self, artifact_kind: ArtifactKind | str) -> tuple[FormatDescriptor, ...]:
        """Return registered descriptors that can write ``artifact_kind``."""

        return tuple(
            descriptor
            for descriptor, adapter in self._by_descriptor.items()
            if adapter.capabilities.supports_write(artifact_kind)
        )

    def detect(
        self,
        artifact: object,
        *,
        artifact_kind: ArtifactKind | str = ArtifactKind.FILE,
        explicit_format: object | None = None,
        minimum_confidence: float = 0.5,
        tie_tolerance: float = 1e-9,
    ) -> DetectionReport:
        """Collect adapter evidence and select a format only when unambiguous.

        All adapters declaring detection support for ``artifact_kind`` are
        probed, even when a format was supplied explicitly. An explicit format
        wins after its registration and read capability have been checked.
        Without one, the leading candidate must meet ``minimum_confidence`` and
        must not tie another candidate within ``tie_tolerance``.
        """

        kind = ArtifactKind(artifact_kind)
        if not 0.0 <= minimum_confidence <= 1.0:
            raise ValueError("minimum_confidence must be between 0.0 and 1.0.")
        if tie_tolerance < 0.0:
            raise ValueError("tie_tolerance must not be negative.")

        candidates: list[DetectionCandidate] = []
        issues: list[DetectionIssue] = []
        for descriptor, adapter in self._by_descriptor.items():
            if not adapter.capabilities.supports_detection(kind):
                continue
            try:
                candidate = adapter.detect(artifact, artifact_kind=kind)
            except Exception as error:  # Detection reports bad probes without hiding other evidence.
                issues.append(
                    DetectionIssue(
                        severity="warning",
                        code="format_detection_probe_failed",
                        message=f"The {descriptor.label()} probe failed: {error}",
                        format_id=descriptor.format_id,
                    )
                )
                continue
            if candidate is None:
                continue
            if not isinstance(candidate, DetectionCandidate):
                issues.append(
                    DetectionIssue(
                        severity="warning",
                        code="format_detection_invalid_candidate",
                        message=f"The {descriptor.label()} probe returned an invalid detection candidate.",
                        format_id=descriptor.format_id,
                    )
                )
                continue
            if candidate.descriptor != descriptor:
                issues.append(
                    DetectionIssue(
                        severity="warning",
                        code="format_detection_descriptor_mismatch",
                        message=(
                            f"The {descriptor.label()} probe claimed {candidate.descriptor.label()}; "
                            "its candidate was ignored."
                        ),
                        format_id=descriptor.format_id,
                    )
                )
                continue
            candidates.append(candidate)

        candidates.sort(key=lambda item: (-item.confidence, item.descriptor))

        explicit_descriptor = coerce_format_descriptor(explicit_format) if explicit_format is not None else None
        if explicit_descriptor is not None:
            matches = self.matching(explicit_descriptor)
            if not matches:
                issues.append(
                    DetectionIssue(
                        severity="error",
                        code="explicit_format_unavailable",
                        message=f"No adapter is registered for explicit format {explicit_descriptor.label()}.",
                        format_id=explicit_descriptor.format_id,
                    )
                )
                return DetectionReport(tuple(candidates), issues=tuple(issues), explicit_format=explicit_descriptor)
            if len(matches) > 1:
                issues.append(
                    DetectionIssue(
                        severity="error",
                        code="explicit_format_ambiguous",
                        message=(
                            f"Multiple adapters are registered for explicit format {explicit_descriptor.format_id}; "
                            "specify a version or dialect."
                        ),
                        format_id=explicit_descriptor.format_id,
                    )
                )
                return DetectionReport(tuple(candidates), issues=tuple(issues), explicit_format=explicit_descriptor)

            adapter = matches[0]
            registered_descriptor = _descriptor_for_adapter(adapter)
            if not adapter.capabilities.supports_read(kind):
                issues.append(
                    DetectionIssue(
                        severity="error",
                        code="explicit_format_unsupported_artifact",
                        message=f"The {registered_descriptor.label()} adapter cannot read {kind.value} artifacts.",
                        format_id=registered_descriptor.format_id,
                    )
                )
                return DetectionReport(tuple(candidates), issues=tuple(issues), explicit_format=explicit_descriptor)

            probed = next((item for item in candidates if item.descriptor == registered_descriptor), None)
            evidence = ("Format selected explicitly by the caller.",)
            if probed is not None:
                evidence += probed.evidence
            selected = DetectionCandidate(registered_descriptor, 1.0, evidence)
            return DetectionReport(
                candidates=tuple(candidates),
                selected=selected,
                issues=tuple(issues),
                explicit_format=explicit_descriptor,
            )

        if not candidates:
            issues.append(
                DetectionIssue(
                    severity="error",
                    code="format_detection_no_match",
                    message=f"No registered adapter recognized the {kind.value} artifact.",
                )
            )
            return DetectionReport(tuple(candidates), issues=tuple(issues))

        leading = candidates[0]
        if leading.confidence < minimum_confidence:
            issues.append(
                DetectionIssue(
                    severity="error",
                    code="format_detection_low_confidence",
                    message=(
                        f"Best format match {leading.descriptor.label()} has confidence {leading.confidence:.3f}, "
                        f"below the required {minimum_confidence:.3f}."
                    ),
                    format_id=leading.descriptor.format_id,
                )
            )
            return DetectionReport(tuple(candidates), issues=tuple(issues))

        tied = [
            candidate
            for candidate in candidates[1:]
            if math.isclose(candidate.confidence, leading.confidence, rel_tol=0.0, abs_tol=tie_tolerance)
        ]
        if tied:
            tied_labels = ", ".join(item.descriptor.label() for item in (leading, *tied))
            issues.append(
                DetectionIssue(
                    severity="error",
                    code="format_detection_ambiguous",
                    message=f"Format detection is ambiguous between: {tied_labels}.",
                )
            )
            return DetectionReport(tuple(candidates), issues=tuple(issues))

        return DetectionReport(tuple(candidates), selected=leading, issues=tuple(issues))
