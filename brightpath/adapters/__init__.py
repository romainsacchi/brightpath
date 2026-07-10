"""Protocols and registry infrastructure for software-format adapters."""

from .base import (
    AdapterCapabilities,
    ArtifactKind,
    DetectionCandidate,
    FormatAdapter,
    FormatDescriptor,
    coerce_format_descriptor,
)
from .builtins import (
    BrightwayDelimitedAdapter,
    BrightwayExcelAdapter,
    SimaProCSVAdapter,
    default_adapter_registry,
)
from .preflight import preflight_conversion, validate_adapter_format
from .registry import AdapterRegistry, DetectionIssue, DetectionReport

__all__ = (
    "AdapterCapabilities",
    "AdapterRegistry",
    "ArtifactKind",
    "BrightwayDelimitedAdapter",
    "BrightwayExcelAdapter",
    "DetectionCandidate",
    "DetectionIssue",
    "DetectionReport",
    "FormatAdapter",
    "FormatDescriptor",
    "SimaProCSVAdapter",
    "coerce_format_descriptor",
    "default_adapter_registry",
    "preflight_conversion",
    "validate_adapter_format",
)
