from __future__ import annotations

from dataclasses import dataclass, field

from brightpath.models import BackgroundProfile, Issue


@dataclass
class MigrationStepReport:
    """Audit counts and issues for one edge in a migration route."""

    source_version: str
    target_version: str
    direction: str
    resource_name: str
    dataset_replacements: int = 0
    technosphere_replacements: int = 0
    technosphere_disaggregations: int = 0
    technosphere_aggregations: int = 0
    biosphere_replacements: int = 0
    biosphere_deletions: int = 0
    issues: list[Issue] = field(default_factory=list)

    @property
    def changed(self) -> bool:
        """Whether this step changed at least one identity or exchange."""

        return any(
            (
                self.dataset_replacements,
                self.technosphere_replacements,
                self.technosphere_disaggregations,
                self.technosphere_aggregations,
                self.biosphere_replacements,
                self.biosphere_deletions,
            )
        )


@dataclass
class MigrationReport:
    """Audit report for a complete background-profile migration."""

    source_profile: BackgroundProfile
    target_profile: BackgroundProfile
    steps: list[MigrationStepReport] = field(default_factory=list)
    issues: list[Issue] = field(default_factory=list)

    @property
    def all_issues(self) -> list[Issue]:
        """Return route-level and step-level issues in reporting order."""

        return [
            *self.issues,
            *(issue for step in self.steps for issue in step.issues),
        ]

    @property
    def has_errors(self) -> bool:
        """Whether any migration or target-validation issue is an error."""

        return any(issue.severity == "error" for issue in self.all_issues)

    @property
    def changed(self) -> bool:
        """Whether any migration step changed the inventory."""

        return any(step.changed for step in self.steps)
