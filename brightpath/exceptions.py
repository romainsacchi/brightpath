from __future__ import annotations

from .models import ValidationReport


class BrightPathError(Exception):
    """Base exception for BrightPath domain errors."""


class InventoryValidationError(BrightPathError, ValueError):
    def __init__(self, report: ValidationReport) -> None:
        self.report = report
        messages = [issue.message for issue in report.issues if issue.severity == "error"]
        detail = "\n".join(messages) or "Unknown inventory validation error."
        super().__init__(f"Inventory validation failed:\n{detail}")


class MigrationError(BrightPathError, ValueError):
    """Base exception for background migration failures."""


class MigrationUnavailableError(MigrationError):
    """Raised when no supported migration route exists."""


class ExcelSerializationError(BrightPathError, ValueError):
    """Raised when inventory values cannot be represented in Brightway Excel."""


class SimaProSerializationError(BrightPathError, ValueError):
    """Raised when canonical inventory data cannot be represented in SimaPro CSV."""
