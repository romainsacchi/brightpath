"""Built-in adapters for BrightPath's currently supported exchange formats."""

from __future__ import annotations

import csv
import zipfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from brightpath.core.context import BiosphereProfile, InventoryContext
from brightpath.formats.brightway_delimited import load_brightway_delimited, write_brightway_delimited
from brightpath.formats.brightway_excel import load_brightway_excel, write_brightway_excel
from brightpath.formats.simapro_csv import (
    SimaProRenderResult,
    load_simapro_csv,
    write_simapro_csv,
)
from brightpath.models import BackgroundProfile, InventoryDocument, InventoryFormat

from .base import AdapterCapabilities, ArtifactKind, DetectionCandidate, FormatDescriptor
from .registry import AdapterRegistry

_FILE_CAPABILITIES = AdapterCapabilities(
    read_artifact_kinds={ArtifactKind.FILE},
    write_artifact_kinds={ArtifactKind.FILE},
    detection_artifact_kinds={ArtifactKind.FILE},
)

_MAX_ZIP_ENTRIES = 20_000
_MAX_PACKAGE_PART_BYTES = 2 * 1024 * 1024
_MAX_XML_PROBE_BYTES = 16 * 1024 * 1024
_MAX_WORKSHEETS_TO_PROBE = 16
_MAX_TEXT_PROBE_BYTES = 1024 * 1024
_XLSX_WORKBOOK_CONTENT_TYPE = b"application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"
_BRIGHTWAY_MARKERS = frozenset({"activity", "database", "exchanges"})

_SIMAPRO_PROCESS_MARKERS = frozenset({"process", "category type", "process name"})
_SIMAPRO_SECTION_MARKERS = frozenset(
    {
        "products",
        "materials/fuels",
        "electricity/heat",
        "resources",
        "emissions to air",
        "emissions to water",
        "emissions to soil",
        "final waste flows",
        "waste to treatment",
        "end",
    }
)


def _file_path(artifact: object) -> Path | None:
    """Return a concrete file path without resolving or modifying it."""

    try:
        path = Path(artifact)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
    try:
        return path if path.is_file() else None
    except OSError:
        return None


def _read_package_part(archive: zipfile.ZipFile, name: str, *, maximum: int) -> bytes:
    """Read a small package part after applying explicit decompression limits."""

    try:
        info = archive.getinfo(name)
    except KeyError:
        return b""
    if info.flag_bits & 0x1 or info.file_size > maximum:
        return b""
    with archive.open(info) as handle:
        contents = handle.read(maximum + 1)
    return contents if len(contents) <= maximum else b""


def _has_xlsx_workbook_structure(archive: zipfile.ZipFile) -> bool:
    """Check mandatory OOXML workbook parts without trusting the extension."""

    if len(archive.infolist()) > _MAX_ZIP_ENTRIES:
        return False
    content_types = _read_package_part(
        archive,
        "[Content_Types].xml",
        maximum=_MAX_PACKAGE_PART_BYTES,
    )
    workbook = _read_package_part(
        archive,
        "xl/workbook.xml",
        maximum=_MAX_PACKAGE_PART_BYTES,
    )
    if _XLSX_WORKBOOK_CONTENT_TYPE not in content_types.lower() or not workbook:
        return False
    normalized = workbook.lower()
    return b"workbook" in normalized and b"<sheet" in normalized


def _collect_xml_markers(
    archive: zipfile.ZipFile,
    name: str,
    *,
    expected: frozenset[str],
) -> set[str]:
    """Stream bounded XML text nodes and return the expected labels found."""

    try:
        info = archive.getinfo(name)
    except KeyError:
        return set()
    if info.flag_bits & 0x1:
        return set()

    if info.file_size > _MAX_XML_PROBE_BYTES:
        return set()
    try:
        with archive.open(info) as handle:
            contents = handle.read(_MAX_XML_PROBE_BYTES + 1)
    except (OSError, RuntimeError, zipfile.BadZipFile):
        return set()
    if len(contents) > _MAX_XML_PROBE_BYTES:
        return set()
    normalized = contents.lower()
    return {marker for marker in expected if marker.encode("ascii") in normalized}


def _brightway_workbook_markers(path: Path) -> set[str]:
    """Return Brightway block-layout labels from a safe, bounded XLSX probe."""

    try:
        with zipfile.ZipFile(path) as archive:
            if not _has_xlsx_workbook_structure(archive):
                return set()
            names = set(archive.namelist())
            parts = []
            if "xl/sharedStrings.xml" in names:
                parts.append("xl/sharedStrings.xml")
            parts.extend(
                sorted(name for name in names if name.startswith("xl/worksheets/") and name.endswith(".xml"))[
                    :_MAX_WORKSHEETS_TO_PROBE
                ]
            )
            found: set[str] = set()
            for name in parts:
                found.update(_collect_xml_markers(archive, name, expected=_BRIGHTWAY_MARKERS - found))
                if found == _BRIGHTWAY_MARKERS:
                    break
            return found
    except (OSError, zipfile.BadZipFile, zipfile.LargeZipFile):
        return set()


def _decoded_text_prefix(path: Path) -> str:
    """Decode a bounded text prefix using SimaPro's supported encodings."""

    try:
        with path.open("rb") as handle:
            contents = handle.read(_MAX_TEXT_PROBE_BYTES)
    except OSError:
        return ""
    if not contents:
        return ""
    try:
        return contents.decode("utf-8-sig")
    except UnicodeDecodeError:
        return contents.decode("latin-1")


def _simapro_rows(path: Path) -> tuple[str, ...]:
    """Return normalized leading SimaPro row labels from a bounded prefix."""

    text = _decoded_text_prefix(path)
    if not text or "\x00" in text:
        return ()
    try:
        rows = csv.reader(text.splitlines(), delimiter=";")
        return tuple(row[0].strip().casefold() for row in rows if row and row[0].strip())
    except csv.Error:
        return ()


@dataclass(frozen=True)
class BrightwayExcelAdapter:
    """Read, detect, and write Brightway block-layout Excel workbooks."""

    descriptor: FormatDescriptor = field(
        default_factory=lambda: FormatDescriptor(InventoryFormat.BRIGHTWAY_EXCEL.value)
    )
    capabilities: AdapterCapabilities = _FILE_CAPABILITIES

    def detect(
        self,
        artifact: object,
        *,
        artifact_kind: ArtifactKind,
    ) -> DetectionCandidate | None:
        """Probe XLSX package structure and Brightway worksheet labels."""

        if ArtifactKind(artifact_kind) is not ArtifactKind.FILE:
            return None
        path = _file_path(artifact)
        if path is None:
            return None
        markers = _brightway_workbook_markers(path)
        if not {"activity", "exchanges"}.issubset(markers):
            return None

        marker_evidence = ", ".join(sorted(marker.title() for marker in markers))
        confidence = 0.98 if markers == _BRIGHTWAY_MARKERS else 0.92
        return DetectionCandidate(
            descriptor=self.descriptor,
            confidence=confidence,
            evidence=(
                "Valid XLSX workbook package structure.",
                f"Brightway workbook markers: {marker_evidence}.",
            ),
        )

    def read(
        self,
        artifact: object,
        *,
        background_profile: BackgroundProfile | None = None,
        biosphere_profile: BiosphereProfile | None = None,
        context: InventoryContext | None = None,
    ) -> InventoryDocument:
        """Load a workbook while forwarding the caller's exact context."""

        return load_brightway_excel(
            artifact,  # type: ignore[arg-type]
            background_profile=background_profile,
            biosphere_profile=biosphere_profile,
            context=context,
        )

    def write(self, document: object, artifact: object, **kwargs: Any) -> Path:
        """Delegate Brightway Excel serialization to the syntax writer."""

        return write_brightway_excel(document, artifact, **kwargs)  # type: ignore[arg-type]


@dataclass(frozen=True)
class SimaProCSVAdapter:
    """Read, detect, and write SimaPro process CSV exports."""

    descriptor: FormatDescriptor = field(default_factory=lambda: FormatDescriptor(InventoryFormat.SIMAPRO_CSV.value))
    capabilities: AdapterCapabilities = _FILE_CAPABILITIES

    def detect(
        self,
        artifact: object,
        *,
        artifact_kind: ArtifactKind,
    ) -> DetectionCandidate | None:
        """Probe SimaPro signatures, process labels, and exchange sections."""

        if ArtifactKind(artifact_kind) is not ArtifactKind.FILE:
            return None
        path = _file_path(artifact)
        if path is None:
            return None
        rows = _simapro_rows(path)
        if not rows:
            return None

        row_set = set(rows)
        signature = next((row for row in rows[:10] if row.startswith("{simapro ")), "")
        process_markers = row_set.intersection(_SIMAPRO_PROCESS_MARKERS)
        section_markers = row_set.intersection(_SIMAPRO_SECTION_MARKERS)
        header_markers = {
            row for row in rows[:30] if row.startswith("{csv format version:") or row.startswith("{csv separator:")
        }

        if signature and len(header_markers) >= 2 and process_markers == _SIMAPRO_PROCESS_MARKERS:
            confidence = 0.99
        elif signature and (header_markers or process_markers):
            confidence = 0.9
        elif process_markers == _SIMAPRO_PROCESS_MARKERS and len(section_markers) >= 2:
            confidence = 0.78
        else:
            return None

        evidence = []
        if signature:
            evidence.append("SimaPro export signature found in the leading rows.")
        if header_markers:
            evidence.append("SimaPro CSV format and separator declarations found.")
        if process_markers:
            evidence.append("SimaPro process field rows found.")
        if section_markers:
            evidence.append("SimaPro exchange section rows found.")
        return DetectionCandidate(self.descriptor, confidence, tuple(evidence))

    def read(
        self,
        artifact: object,
        *,
        background_profile: BackgroundProfile | None = None,
        biosphere_profile: BiosphereProfile | None = None,
        context: InventoryContext | None = None,
        database_name: str | None = None,
    ) -> InventoryDocument:
        """Load a SimaPro CSV while forwarding the caller's exact context."""

        return load_simapro_csv(
            artifact,  # type: ignore[arg-type]
            background_profile=background_profile,
            biosphere_profile=biosphere_profile,
            context=context,
            database_name=database_name,
        )

    def write(
        self,
        document: object,
        artifact: object,
        **kwargs: Any,
    ) -> tuple[Path, SimaProRenderResult]:
        """Delegate SimaPro CSV serialization to the syntax writer."""

        return write_simapro_csv(document, artifact, **kwargs)  # type: ignore[arg-type]


@dataclass(frozen=True)
class BrightwayDelimitedAdapter:
    """Read, detect, and write one Brightway block-layout text dialect."""

    descriptor: FormatDescriptor
    delimiter: str
    capabilities: AdapterCapabilities = _FILE_CAPABILITIES

    def __post_init__(self) -> None:
        if self.descriptor.format_id not in {
            InventoryFormat.BRIGHTWAY_CSV.value,
            InventoryFormat.BRIGHTWAY_TSV.value,
        }:
            raise ValueError("Brightway delimited adapters require a brightway_csv or brightway_tsv descriptor.")
        expected = "," if self.descriptor.format_id == InventoryFormat.BRIGHTWAY_CSV.value else "\t"
        if self.delimiter != expected:
            raise ValueError("Brightway delimited adapter delimiter conflicts with its descriptor.")

    def detect(
        self,
        artifact: object,
        *,
        artifact_kind: ArtifactKind,
    ) -> DetectionCandidate | None:
        """Probe bounded rows for the Brightway block grammar."""

        if ArtifactKind(artifact_kind) is not ArtifactKind.FILE:
            return None
        path = _file_path(artifact)
        if path is None:
            return None
        text = _decoded_text_prefix(path)
        if not text or "\x00" in text:
            return None
        try:
            rows = list(csv.reader(text.splitlines(), delimiter=self.delimiter))
        except csv.Error:
            return None
        labels = {row[0].strip().casefold() for row in rows if row and row[0].strip()}
        markers = labels.intersection(_BRIGHTWAY_MARKERS)
        has_values = any(len(row) > 1 for row in rows)
        if not {"activity", "exchanges"}.issubset(markers) or not has_values:
            return None
        confidence = 0.96 if markers == _BRIGHTWAY_MARKERS else 0.9
        return DetectionCandidate(
            self.descriptor,
            confidence,
            (f"Brightway block markers using the {self.descriptor.format_id} delimiter.",),
        )

    def read(
        self,
        artifact: object,
        *,
        background_profile: BackgroundProfile | None = None,
        biosphere_profile: BiosphereProfile | None = None,
        context: InventoryContext | None = None,
    ) -> InventoryDocument:
        """Load a Brightway delimited file with an exact context."""

        return load_brightway_delimited(
            artifact,  # type: ignore[arg-type]
            delimiter=self.delimiter,
            background_profile=background_profile,
            biosphere_profile=biosphere_profile,
            context=context,
        )

    def write(self, document: object, artifact: object, **kwargs: Any) -> Path:
        """Delegate deterministic text serialization to the format codec."""

        return write_brightway_delimited(  # type: ignore[arg-type]
            document,
            artifact,  # type: ignore[arg-type]
            delimiter=self.delimiter,
            **kwargs,
        )


def default_adapter_registry() -> AdapterRegistry:
    """Return a fresh immutable registry of production-ready file adapters."""

    return AdapterRegistry(
        (
            BrightwayExcelAdapter(),
            BrightwayDelimitedAdapter(FormatDescriptor(InventoryFormat.BRIGHTWAY_CSV.value), ","),
            BrightwayDelimitedAdapter(FormatDescriptor(InventoryFormat.BRIGHTWAY_TSV.value), "\t"),
            SimaProCSVAdapter(),
        )
    )
