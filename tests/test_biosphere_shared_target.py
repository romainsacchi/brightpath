from copy import deepcopy

import pytest

from brightpath.migrations.engine import _apply_biosphere_rules, _shared_catalog_target
from brightpath.migrations.models import MigrationStepReport


def test_shared_biosphere_target_accepts_list_categories_without_mutation():
    exchange = {"name": "merged", "categories": ["air"], "unit": "kilogram", "amount": 2}
    target = {"name": "older", "categories": ["air", "low population density"], "unit": "kilogram"}
    matches = [{"source": {**target, "uuid": identifier}} for identifier in ("first", "second")]
    original = deepcopy((exchange, matches))
    catalog = {("older", ("air", "low population density"), "kilogram")}
    assert _shared_catalog_target(exchange, matches, "source", catalog) == target
    assert (exchange, matches) == original


@pytest.mark.parametrize("different_field", ["categories", "formula"])
def test_shared_biosphere_target_still_refuses_distinct_candidates(different_field):
    exchange = {"name": "merged", "categories": ["air"], "unit": "kilogram"}
    target = {"name": "older", "categories": ["air"], "unit": "kilogram", "formula": "CO2"}
    different = {**target, different_field: ["water"] if different_field == "categories" else "CO"}
    matches = [{"source": target}, {"source": different}]
    catalog = {("older", ("air",), "kilogram"), ("older", ("water",), "kilogram")}
    assert _shared_catalog_target(exchange, matches, "source", catalog) is None


def test_reverse_biosphere_rules_resolve_shared_list_target_without_guessing():
    source = {"name": "older", "categories": ["air"], "unit": "kilogram"}
    target = {"name": "merged", "categories": ["air"], "unit": "kilogram"}
    rules = [
        {"source": {**source, "uuid": identifier}, "target": {**target, "uuid": "merged-id"}}
        for identifier in ("first", "second")
    ]
    exchange = {**target, "type": "biosphere", "amount": 2, "uncertainty type": 3, "loc": 2, "scale": 0.2}
    original = deepcopy(exchange)
    report = MigrationStepReport(
        source_version="3.12", target_version="3.11", direction="backward", resource_name="synthetic"
    )
    data = [{"exchanges": [exchange]}]
    _apply_biosphere_rules(
        data, {"replace": rules}, "backward", report, target_biosphere_identities={("older", ("air",), "kilogram")}
    )
    assert exchange["name"] == "older"
    assert "uuid" not in exchange
    assert not any("ambiguous" in issue.code for issue in report.issues)
    for field in ("amount", "uncertainty type", "loc", "scale", "categories", "unit"):
        assert exchange[field] == original[field]
