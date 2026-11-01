"""Exact, opt-in membership checks for a declared SimaPro substance catalogue.

Stock membership is separate from background identity, scientific equivalence,
CSV grammar, and native application acceptance. No catalogue is selected by default.
"""

from __future__ import annotations

import csv
import hashlib
import io
import re
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from types import MappingProxyType
from typing import Iterable, Mapping

from brightpath.core.reports import Issue, Severity, StageKind, StageReport

SUBSTANCE_SECTIONS = frozenset(
    {
        "Resources",
        "Emissions to air",
        "Emissions to water",
        "Emissions to soil",
        "Final waste flows",
        "Non material emissions",
        "Social issues",
        "Economic issues",
    }
)
_GLAD_CONTEXTS = {
    "Airborne emissions": "Emissions to air",
    "Waterborne emissions": "Emissions to water",
    "Emissions to soil": "Emissions to soil",
    "Raw materials": "Resources",
    **{key: key for key in SUBSTANCE_SECTIONS - {"Resources", "Emissions to air", "Emissions to water"}},
}


@dataclass(frozen=True)
class SimaProStockSubstance:
    """One native stock identity; ``None`` means subcompartment not supplied."""

    name: str
    main_compartment: str
    unit: str
    stock_id: str
    subcompartment: str | None = None

    def __post_init__(self):
        for name in ("name", "main_compartment", "unit", "stock_id"):
            if not isinstance(getattr(self, name), str) or not getattr(self, name):
                raise ValueError(f"Stock substance {name} must be a non-empty string.")
        if self.main_compartment not in SUBSTANCE_SECTIONS:
            raise ValueError(f"Unknown rendered SimaPro substance section: {self.main_compartment!r}.")
        if self.subcompartment is not None and not isinstance(self.subcompartment, str):
            raise TypeError("subcompartment must be a string or None.")


@dataclass(frozen=True)
class SimaProStockCatalog:
    """Injected exact-version stock snapshot with explicit provenance and coverage.

    ``complete`` authorizes absence conclusions only for ``covered_sections``.
    It is an assertion by the caller supplying the catalogue, never inferred
    from a filename, row count or a GLAD nomenclature token.
    """

    stock_version: str
    substances: tuple[SimaProStockSubstance, ...]
    source: str
    source_sha256: str
    complete: bool = False
    covered_sections: frozenset[str] = SUBSTANCE_SECTIONS
    subcompartments: bool = False
    _index: Mapping = field(init=False, repr=False, compare=False)

    def __post_init__(self):
        if (
            not isinstance(self.stock_version, str)
            or not self.stock_version
            or self.stock_version.strip() != self.stock_version
        ):
            raise ValueError("stock_version must be an exact, non-empty version string.")
        if not isinstance(self.source, str) or not self.source:
            raise ValueError("Catalogue source provenance is required.")
        if not isinstance(self.source_sha256, str) or re.fullmatch(r"[0-9a-f]{64}", self.source_sha256) is None:
            raise ValueError("Catalogue source_sha256 must be a lowercase SHA-256 digest.")
        if type(self.complete) is not bool or type(self.subcompartments) is not bool:
            raise TypeError("Catalogue coverage flags must be booleans.")
        records = tuple(self.substances)
        sections = frozenset(self.covered_sections)
        if not sections or not sections <= SUBSTANCE_SECTIONS:
            raise ValueError("covered_sections must identify supported substance sections.")
        if self.complete and not records:
            raise ValueError("An empty catalogue cannot declare complete stock coverage.")
        index = defaultdict(set)
        for record in records:
            if not isinstance(record, SimaProStockSubstance):
                raise TypeError("substances must contain SimaProStockSubstance records.")
            if record.main_compartment not in sections:
                raise ValueError("A stock record lies outside the declared section coverage.")
            if self.subcompartments and record.subcompartment is None:
                raise ValueError("Declared subcompartment coverage requires explicit values on every record.")
            key = (record.name, record.main_compartment, record.unit)
            if self.subcompartments:
                key += (record.subcompartment,)
            index[key].add(record.stock_id)
        object.__setattr__(self, "substances", records)
        object.__setattr__(self, "covered_sections", sections)
        object.__setattr__(self, "_index", MappingProxyType({key: frozenset(value) for key, value in index.items()}))

    @classmethod
    def from_glad_rows(
        cls, rows: Iterable[Mapping], *, stock_version: str, source: str, source_sha256: str, complete: bool = False
    ) -> SimaProStockCatalog:
        """Read GLAD dictionaries filtered by the exact ``Professional VERSION`` token.

        The published list has no subcompartment relation. No chemistry, name,
        unit or case alias is introduced. Rows in other nomenclatures are ignored.
        """
        token = f"Professional {stock_version}"
        substances = []
        for row in rows:
            if token not in {value.strip() for value in str(row.get("SupportedNomenclatures") or "").split(";")}:
                continue
            context = row.get("Context")
            if context not in _GLAD_CONTEXTS:
                raise ValueError(f"Unsupported GLAD stock context: {context!r}.")
            substances.append(
                SimaProStockSubstance(
                    row.get("Flowable"), _GLAD_CONTEXTS[context], row.get("Unit"), row.get("FlowUUID")
                )
            )
        if not substances:
            raise ValueError(f"No rows with the exact supported nomenclature {token!r}.")
        return cls(stock_version, tuple(substances), source, source_sha256, complete=complete)

    @classmethod
    def load_glad_xlsx(
        cls, path: str | Path, *, stock_version: str, complete: bool = False, sheet_name: str | None = None
    ) -> SimaProStockCatalog:
        """Load an external GLAD workbook using the existing openpyxl dependency.

        Nothing is downloaded or redistributed; the complete source file is
        hashed before and after read-only parsing. Completeness stays explicit.
        """
        from openpyxl import load_workbook

        path = Path(path)
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        book = load_workbook(path, read_only=True, data_only=True)
        try:
            sheet = book[sheet_name] if sheet_name is not None else book.worksheets[0]
            iterator = sheet.iter_rows(values_only=True)
            headers = next(iterator, ())
            required = {"Flowable", "Context", "Unit", "FlowUUID", "SupportedNomenclatures"}
            if not required <= set(headers) or len(headers) != len(set(headers)):
                raise ValueError("GLAD workbook is missing required unique column headers.")
            result = cls.from_glad_rows(
                (dict(zip(headers, values, strict=True)) for values in iterator),
                stock_version=stock_version,
                source=str(path),
                source_sha256=digest,
                complete=complete,
            )
        finally:
            book.close()
        if hashlib.sha256(path.read_bytes()).hexdigest() != digest:
            raise ValueError("GLAD source workbook changed during loading.")
        return result


def validate_simapro_stock_rows(
    rows: Iterable[Iterable],
    *,
    stock_catalog: SimaProStockCatalog,
    stock_version: str,
    check_subcompartments: bool = False,
) -> StageReport:
    """Validate every rendered substance row without changing any cell.

    Renderer rows are checked after the writer's final Latin-1 cell transform.
    Process/product/technosphere/parameter sections are not substance stock.
    Unknown results are errors: absence of errors means every checked membership
    was witnessed, not that unrequested dimensions or native application import
    were validated. The CSV grammar remains a separate validation stage.
    """
    return _validate_rows(
        rows,
        stock_catalog=stock_catalog,
        stock_version=stock_version,
        check_subcompartments=check_subcompartments,
        rendered=True,
    )


def _validate_rows(rows, *, stock_catalog, stock_version, check_subcompartments, rendered):
    from brightpath.formats.simapro_csv import _latin1_safe_cell

    if not isinstance(stock_catalog, SimaProStockCatalog):
        raise TypeError("stock_catalog must be an explicit SimaProStockCatalog.")
    if not isinstance(stock_version, str) or not stock_version:
        raise ValueError("An exact stock_version is required.")
    if type(check_subcompartments) is not bool:
        raise TypeError("check_subcompartments must be a boolean.")
    stage = StageKind.FORMAT_VALIDATION
    issues, matches = [], []
    section = None
    process = -1
    recognized_sections = 0
    counts = {"matched": 0, "unmatched": 0, "unknown": 0}
    profile_matches = stock_version == stock_catalog.stock_version
    if not profile_matches:
        issues.append(
            Issue(
                Severity.ERROR,
                "simapro_stock_version_mismatch",
                "Requested SimaPro stock version does not match the supplied catalogue.",
                stage,
                details={"requested_version": stock_version, "catalogue_version": stock_catalog.stock_version},
            )
        )
    for number, original in enumerate(rows):
        # Actual saved CSV cells have already passed the writer's transform.
        row = [_latin1_safe_cell(value) for value in original] if rendered else list(original)
        if not row:
            section = None
            continue
        if row == ["Process"]:
            process += 1
        if len(row) == 1 and section not in SUBSTANCE_SECTIONS:
            section = row[0]
            recognized_sections += section in SUBSTANCE_SECTIONS
            continue
        stock_like = isinstance(section, str) and section.strip().casefold() in {
            s.casefold() for s in SUBSTANCE_SECTIONS
        }
        if section not in SUBSTANCE_SECTIONS and not stock_like:
            continue
        details = {
            "row": number,
            "process": process,
            "name": row[0],
            "main_compartment": section,
            "subcompartment": row[1] if len(row) > 1 else None,
            "unit": row[2] if len(row) > 2 else None,
        }
        path = f"rows[{number}]"
        state, code = "unknown", "simapro_stock_coverage_unknown"
        ids = frozenset()
        if len(row) < 3 or any(not isinstance(row[i], str) or not row[i] for i in (0, 2)):
            code = "simapro_stock_row_invalid"
        elif not profile_matches:
            code = "simapro_stock_version_mismatch"
        elif section not in stock_catalog.covered_sections:
            code = "simapro_stock_coverage_unknown"
        elif check_subcompartments and not stock_catalog.subcompartments:
            code = "simapro_stock_subcompartment_unknown"
        else:
            key = (row[0], section, row[2])
            if check_subcompartments:
                ids = stock_catalog._index.get(key + (row[1],), frozenset())
            elif stock_catalog.subcompartments:
                ids = frozenset(
                    uid for record, values in stock_catalog._index.items() if record[:3] == key for uid in values
                )
            else:
                ids = stock_catalog._index.get(key, frozenset())
            if len(ids) == 1:
                state, code = "matched", "simapro_stock_matched"
            elif len(ids) > 1:
                code = "simapro_stock_ambiguous"
            elif stock_catalog.complete:
                state, code = "unmatched", "simapro_stock_unmatched"
        counts[state] += 1
        details.update(match_state=state, candidate_stock_ids=sorted(ids))
        matches.append(details)
        if state != "matched":
            issues.append(
                Issue(
                    Severity.ERROR,
                    code,
                    f"SimaPro stock membership is {state} for {row[0]!r} in {section!r}.",
                    stage,
                    path=path,
                    details=details,
                )
            )
    if not sum(counts.values()) and not recognized_sections:
        issues.append(
            Issue(
                Severity.ERROR,
                "simapro_stock_no_substances",
                "No recognized substance sections were available for stock membership validation.",
                stage,
            )
        )
    metrics = {
        "stock_version": stock_version,
        "catalogue_version": stock_catalog.stock_version,
        "catalogue_source": stock_catalog.source,
        "catalogue_source_sha256": stock_catalog.source_sha256,
        "catalogue_complete": stock_catalog.complete,
        "covered_sections": sorted(stock_catalog.covered_sections),
        "checked_dimensions": ["name", "main_compartment", "unit"]
        + (["subcompartment"] if check_subcompartments else []),
        "subcompartment_coverage_available": stock_catalog.subcompartments,
        "subcompartments_checked": check_subcompartments and stock_catalog.subcompartments,
        "substance_rows_checked": sum(counts.values()),
        **counts,
        "matches": matches,
        "recognized_substance_sections": recognized_sections,
        "stock_membership_validated": profile_matches and bool(sum(counts.values())) and not issues,
        "stock_state": (
            "unknown"
            if counts["unknown"] or not profile_matches or issues and not sum(counts.values())
            else "unmatched" if counts["unmatched"] else "matched" if sum(counts.values()) else "not_applicable"
        ),
        "scientific_identity_or_native_application_compatibility_certified": False,
    }
    return StageReport(stage, label="SimaPro stock membership", issues=tuple(issues), metrics=metrics)


def validate_simapro_stock_csv(
    content: bytes, *, stock_catalog: SimaProStockCatalog, stock_version: str, check_subcompartments: bool = False
) -> StageReport:
    """Check existing Latin-1, semicolon SimaPro CSV bytes without reserializing."""
    if not isinstance(content, bytes):
        raise TypeError("content must be actual CSV bytes.")
    try:
        rows = list(csv.reader(io.StringIO(content.decode("latin-1"), newline=""), delimiter=";", strict=True))
    except csv.Error as error:
        return StageReport(
            StageKind.FORMAT_VALIDATION,
            label="SimaPro stock membership",
            issues=(Issue(Severity.ERROR, "simapro_stock_csv_invalid", str(error), StageKind.FORMAT_VALIDATION),),
            metrics={"stock_membership_validated": False, "stock_state": "unknown", "substance_rows_checked": 0},
        )
    return _validate_rows(
        rows,
        stock_catalog=stock_catalog,
        stock_version=stock_version,
        check_subcompartments=check_subcompartments,
        rendered=False,
    )
