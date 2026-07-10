from __future__ import annotations

from copy import deepcopy

import pytest

from brightpath.adapters.base import FormatDescriptor
from brightpath.adapters.preflight import preflight_conversion
from brightpath.core import BackgroundContext, BiosphereProfile, FormatProfile, InventoryContext, TechnosphereProfile
from brightpath.core.policies import ConversionPolicy, PolicyAction
from brightpath.core.reports import Severity, StageKind
from brightpath.formats.simapro_csv import SimaProRenderResult
from brightpath.models import InventoryDocument
from brightpath.models import Issue as LegacyIssue


def _context(format_id: str = "brightway_excel") -> InventoryContext:
    return InventoryContext(
        format=FormatProfile(format_id),
        background=BackgroundContext(
            technosphere=TechnosphereProfile("ecoinvent", "3.9", "cutoff"),
            biosphere=BiosphereProfile("ecoinvent", "3.9"),
        ),
    )


def _activity(*, extra_exchanges=(), **overrides):
    activity = {
        "name": "test process",
        "reference product": "test product",
        "location": "GLO",
        "unit": "kilogram",
        "exchanges": [
            {
                "type": "production",
                "name": "test process",
                "reference product": "test product",
                "product": "test product",
                "location": "GLO",
                "unit": "kilogram",
                "amount": 1.0,
                "simapro category": "Materials/Test",
            },
            *extra_exchanges,
        ],
    }
    activity.update(overrides)
    return activity


def _document(
    *,
    data=None,
    metadata=None,
    database_parameters=None,
    project_parameters=None,
    format_id="brightway_excel",
):
    return InventoryDocument(
        data=data if data is not None else [_activity()],
        context=_context(format_id),
        database_name="preflight-test",
        metadata=metadata,
        database_parameters=database_parameters,
        project_parameters=project_parameters,
    )


def _loss_codes(report):
    return {loss.code for loss in report.losses}


def test_clean_simapro_conversion_has_only_deterministic_changes():
    report = preflight_conversion(_document(), FormatDescriptor("simapro_csv"))

    assert report.stage is StageKind.CONVERSION_PREFLIGHT
    assert not report.issues
    assert not report.losses
    assert not report.has_errors
    assert {
        "simapro_category_mapped",
        "simapro_date_generated",
        "simapro_technosphere_name_formatted",
        "simapro_unit_mapped",
    }.issubset({change.code for change in report.changes})
    assert report.metrics["target_format"]["format_id"] == "simapro_csv"


def test_unknown_simapro_fields_are_aggregated_and_follow_policy():
    activity = _activity(custom_dataset={"nested": True}, parameters=[{"name": "p", "amount": 1, "custom": 2}])
    activity["exchanges"][0]["custom exchange"] = "value"
    document = _document(data=[activity], metadata={"custom metadata": "value"})

    strict = preflight_conversion(document, "simapro_csv")
    permissive = preflight_conversion(document, "simapro_csv", ConversionPolicy.permissive())
    allowed = preflight_conversion(
        document,
        "simapro_csv",
        ConversionPolicy(on_unsupported_feature=PolicyAction.ALLOW),
    )

    expected = {
        "simapro_dataset_fields_unsupported",
        "simapro_exchange_fields_unsupported",
        "simapro_metadata_fields_unsupported",
        "simapro_parameter_fields_unsupported",
    }
    assert expected.issubset(_loss_codes(strict))
    assert all(issue.severity is Severity.ERROR for issue in strict.issues)
    assert all(issue.severity is Severity.WARNING for issue in permissive.issues)
    assert all(issue.severity is Severity.INFO for issue in allowed.issues)
    assert not allowed.has_errors
    dataset_loss = next(loss for loss in strict.losses if loss.code == "simapro_dataset_fields_unsupported")
    assert dataset_loss.path == "datasets[0]"
    assert dataset_loss.details["fields"] == ("custom_dataset",)


def test_exchange_formula_and_rounding_are_explicit_losses():
    technosphere = {
        "type": "technosphere",
        "name": "market for product",
        "reference product": "product",
        "location": "CH",
        "unit": "kilogram",
        "amount": 1.23456,
        "formula": "demand * 2",
    }

    report = preflight_conversion(_document(data=[_activity(extra_exchanges=(technosphere,))]), "simapro_csv")

    assert {
        "simapro_exchange_amount_rounded",
        "simapro_exchange_formula_unsupported",
    }.issubset(_loss_codes(report))
    formula = next(loss for loss in report.losses if loss.code == "simapro_exchange_formula_unsupported")
    rounding = next(loss for loss in report.losses if loss.code == "simapro_exchange_amount_rounded")
    assert formula.path == "datasets[0].exchanges[1].formula"
    assert rounding.details["after"] == pytest.approx(1.235)


def test_substitution_blacklist_final_waste_and_unused_are_distinguished():
    exchanges = (
        {
            "type": "substitution",
            "name": "substituted product",
            "reference product": "product",
            "location": "GLO",
            "unit": "kilogram",
            "amount": -1.0,
        },
        {
            "type": "biosphere",
            "name": "Oxygen",
            "categories": ("air", "urban air close to ground"),
            "unit": "kilogram",
            "amount": 1.0,
        },
        {
            "type": "technosphere",
            "name": "final waste",
            "reference product": "waste",
            "location": "GLO",
            "unit": "kilogram",
            "amount": 1.0,
            "categories": "Final waste flows",
        },
        {"type": "custom", "name": "unhandled", "unit": "kilogram", "amount": 1.0},
    )

    report = preflight_conversion(_document(data=[_activity(extra_exchanges=exchanges)]), "simapro_csv")

    assert {
        "simapro_exchange_blacklisted",
        "simapro_exchange_unused",
        "simapro_final_waste_exchange_unsupported",
        "simapro_substitution_exchange_unsupported",
    }.issubset(_loss_codes(report))


def test_sign_and_uncertainty_transformations_are_reported():
    technosphere = {
        "type": "technosphere",
        "name": "market for product",
        "reference product": "product",
        "location": "CH",
        "unit": "kilogram",
        "amount": -2.0,
        "uncertainty type": 3,
        "scale": 0.2,
    }
    activity = _activity(
        name="treatment of waste",
        type="waste treatment",
        extra_exchanges=(technosphere,),
    )

    report = preflight_conversion(_document(data=[activity]), "simapro_csv")

    assert {
        "simapro_exchange_sign_normalized",
        "simapro_uncertainty_transformed",
    }.issubset(_loss_codes(report))
    sign = next(loss for loss in report.losses if loss.code == "simapro_exchange_sign_normalized")
    assert sign.details == {"after": 2.0, "before": -2.0}


def test_latin1_failure_is_reported_before_write():
    report = preflight_conversion(
        _document(data=[_activity(comment="unsupported snowman: \u2603")]),
        "simapro_csv",
    )

    assert "simapro_latin1_encoding_unsupported" in _loss_codes(report)
    loss = next(loss for loss in report.losses if loss.code == "simapro_latin1_encoding_unsupported")
    assert "datasets[0].comment" in loss.details["paths"]
    assert report.has_errors


@pytest.mark.parametrize("target", ["brightway_excel", "brightway_csv", "brightway_tsv"])
def test_brightway_preserves_unknown_fields_but_omits_link_keys(target):
    activity = _activity(custom_dataset={"nested": [1, {"two": 2}]})
    activity["exchanges"][0].update(
        {
            "custom exchange": {"nested": True},
            "input": ("background", "provider"),
            "output": ("foreground", "activity"),
        }
    )

    report = preflight_conversion(_document(data=[activity]), target)

    assert _loss_codes(report) == {"brightway_exchange_link_fields_omitted"}
    assert report.losses[0].details["fields"] == ("input", "output")
    assert report.losses[0].path == "datasets[0].exchanges[0]"


def test_brightway_unknown_tagged_fields_without_links_are_lossless():
    activity = _activity(custom_dataset={"nested": [1, {"two": 2}]})
    activity["exchanges"][0]["custom exchange"] = {"nested": True}

    report = preflight_conversion(_document(data=[activity]), "brightway_excel")

    assert not report.losses
    assert not report.issues


def test_renderer_errors_are_translated_to_core_stage_and_policy(monkeypatch):
    legacy = LegacyIssue(
        severity="error",
        code="simapro_category_missing",
        message="missing category",
        path="activity[0].exchanges[0]",
    )
    monkeypatch.setattr(
        "brightpath.adapters.preflight.render_simapro_rows",
        lambda _document: SimaProRenderResult(rows=[], issues=[legacy]),
    )

    strict = preflight_conversion(_document(), "simapro_csv")
    permissive = preflight_conversion(_document(), "simapro_csv", ConversionPolicy.permissive())

    assert strict.issues[0].code == "simapro_category_missing"
    assert strict.issues[0].stage is StageKind.CONVERSION_PREFLIGHT
    assert strict.issues[0].path == "datasets[0].exchanges[0]"
    assert strict.issues[0].severity is Severity.ERROR
    assert permissive.issues[0].severity is Severity.WARNING


def test_unexpected_renderer_failure_is_structured(monkeypatch):
    def fail(_document):
        raise ValueError("renderer exploded")

    monkeypatch.setattr("brightpath.adapters.preflight.render_simapro_rows", fail)

    report = preflight_conversion(_document(), "simapro_csv")

    assert report.has_errors
    assert [issue.code for issue in report.issues] == ["conversion.preflight_failed"]
    assert report.issues[0].details["exception_type"] == "ValueError"


def test_preflight_does_not_mutate_caller_or_document_data():
    data = [_activity(comment="value 1.234", custom_dataset={"nested": [1, 2]})]
    source = deepcopy(data)
    document = _document(data=data, metadata={"owner": {"name": "BrightPath"}})
    before = document.data

    first = preflight_conversion(document, "simapro_csv", ConversionPolicy.permissive())
    second = preflight_conversion(document, "simapro_csv", ConversionPolicy.permissive())

    assert data == source
    assert document.data == before
    assert first == second
