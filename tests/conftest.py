from copy import deepcopy
from datetime import datetime

import pytest


@pytest.fixture
def fixed_simapro_clock(monkeypatch):
    """Keep export timestamps constant when comparing repeated renders."""

    class FixedDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            return cls(2026, 1, 1, tzinfo=tz)

    monkeypatch.setattr("brightpath.formats.simapro_csv.datetime.datetime", FixedDatetime)


@pytest.fixture
def migration_resource():
    """Provide an independent synthetic shared-schema resource."""
    from brightpath.migrations.schema import intern_review

    target = {"name": "Target", "reference product": "product", "location": "CH", "unit": "kilogram"}
    result = {
        "schema": "brightpath.migration",
        "schema_version": 3,
        "name": "synthetic-crosswalk",
        "axis": "technosphere",
        "status": "active",
        "reverse_policy": "explicit_only",
        "source_profiles": [{"family": "ecoinvent", "version": "3.12", "system_model": "cutoff"}],
        "target_profile": {"family": "uvek", "version": "2025", "system_model": "cutoff"},
        "replace": [],
        "disaggregate": [],
        "delete": [],
        "recipes": {},
        "reviews": {},
        "coverage": {"source_identities": 3, "mapped_identities": 2, "unresolved_identities": 1},
        "custom_metadata": {"preserved": True},
    }
    review = intern_review(result, {"rationale": "Synthetic review", "limitations": ["Geographic proxy"]})
    rule = {
        "id": "synthetic-a",
        "source": {**target, "name": "Source", "custom_field": [1, 2]},
        "target": target,
        "confidence": 0.5,
        "mapping_method": "review",
        "review_ref": review,
        "conversion_factor": 0.1,
    }
    result["replace"] = [rule, {**deepcopy(rule), "id": "synthetic-b", "source": {**rule["source"], "location": "RER"}}]
    return result
