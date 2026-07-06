from __future__ import annotations

from dataclasses import dataclass, field


def _normalize_family(value: str | None) -> str:
    return (value or "").strip().lower()


def _normalize_system_model(value: str | None) -> str:
    normalized = (value or "").strip().lower()
    if normalized in {"cut-off", "cutoff"}:
        return "cutoff"
    return normalized


@dataclass(frozen=True)
class BackgroundProfile:
    family: str = ""
    version: str = ""
    system_model: str = ""

    def normalized(self) -> "BackgroundProfile":
        return BackgroundProfile(
            family=_normalize_family(self.family),
            version=(self.version or "").strip(),
            system_model=_normalize_system_model(self.system_model),
        )


@dataclass
class Issue:
    severity: str
    code: str
    message: str
    path: str = ""
    suggested_fix: str = ""


@dataclass
class CandidateSummary:
    index: int
    name: str = ""
    reference_product: str = ""
    location: str = ""
    unit: str = ""
    description_hint: str = ""
    source_hint: str = ""
    issues: list[Issue] = field(default_factory=list)


@dataclass
class AnalysisResult:
    detected_software: str
    detected_format: str
    source_profile: BackgroundProfile = field(default_factory=BackgroundProfile)
    file_issues: list[Issue] = field(default_factory=list)
    candidates: list[CandidateSummary] = field(default_factory=list)
    inventory_data: list[dict] = field(default_factory=list)

    @property
    def has_errors(self) -> bool:
        if any(issue.severity == "error" for issue in self.file_issues):
            return True
        return any(
            issue.severity == "error"
            for candidate in self.candidates
            for issue in candidate.issues
        )
