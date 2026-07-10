"""Protocols and registry infrastructure for software-format adapters."""

from .base import (
    AdapterCapabilities,
    ArtifactKind,
    DetectionCandidate,
    FormatAdapter,
    FormatDescriptor,
    coerce_format_descriptor,
)
from .registry import AdapterRegistry, DetectionIssue, DetectionReport

__all__ = (
    "AdapterCapabilities",
    "AdapterRegistry",
    "ArtifactKind",
    "DetectionCandidate",
    "DetectionIssue",
    "DetectionReport",
    "FormatAdapter",
    "FormatDescriptor",
    "coerce_format_descriptor",
)
