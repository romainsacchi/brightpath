"""Versioned stock membership is explicit, exact and independent of background links."""

import csv
import hashlib
import io
from copy import deepcopy
from dataclasses import replace

import pytest
from openpyxl import Workbook

from brightpath import BackgroundProfile, SimaProInventory
from brightpath.background.catalogs import BiosphereCatalog, InMemoryCatalogProvider
from brightpath.core.context import BiosphereProfile
from brightpath.core.reports import StageReport
from brightpath.exceptions import InventoryValidationError
from brightpath.formats.simapro_csv import _latin1_safe_cell, write_simapro_csv
from brightpath.formats.simapro_stock import (
    SimaProStockCatalog,
    SimaProStockSubstance,
    validate_simapro_stock_csv,
    validate_simapro_stock_rows,
)


def catalog(*, name="Probe compound", main="Emissions to air", unit="kg", complete=True, **kwargs):
    return SimaProStockCatalog(
        "10.2",
        (SimaProStockSubstance(name, main, unit, "synthetic-stock-id"),),
        "synthetic test catalogue",
        "a" * 64,
        complete=complete,
        **kwargs,
    )


def rows(name="Probe compound", main="Emissions to air", unit="kg", sub=""):
    return [["Process"], [], [main], [name, sub, unit, 2.5, "Undefined"], [], ["End"]]


def csv_bytes(values):
    stream = io.StringIO(newline="")
    csv.writer(stream, delimiter=";").writerows(values)
    return stream.getvalue().encode("latin-1")


def inventory(name="Probe compound"):
    return SimaProInventory.from_data(
        [
            {
                "name": "stock test",
                "reference product": "product",
                "location": "GLO",
                "unit": "kilogram",
                "comment": "Synthetic stock-membership validation fixture.",
                "exchanges": [
                    {
                        "type": "production",
                        "name": "stock test",
                        "reference product": "product",
                        "location": "GLO",
                        "unit": "kilogram",
                        "amount": 1,
                        "simapro category": "Materials/Test",
                    },
                    {
                        "type": "biosphere",
                        "name": name,
                        "categories": ("air", "unspecified"),
                        "unit": "kilogram",
                        "amount": 2.5,
                        "uncertainty type": 3,
                        "loc": 2.5,
                        "scale": 0.2,
                        "formula": "rate",
                    },
                ],
                "parameters": [{"name": "rate", "amount": 2.5}],
            }
        ],
        background_profile=BackgroundProfile("ecoinvent", "3.12", "cutoff"),
    )


def test_exact_membership_immutable_report_and_caller_nonmutation():
    before = rows()
    saved = deepcopy(before)
    report = validate_simapro_stock_rows(before, stock_catalog=catalog(), stock_version="10.2")
    assert isinstance(report, StageReport) and not report.has_errors
    assert report.metrics["matched"] == report.metrics["substance_rows_checked"] == 1
    assert report.metrics["stock_membership_validated"] is True
    assert report.metrics["subcompartments_checked"] is False
    assert before == saved
    with pytest.raises(TypeError):
        report.metrics["matched"] = 0
    snapshot = report.to_dict()
    snapshot["metrics"]["matches"][0]["name"] = "changed"
    assert report.metrics["matches"][0]["name"] == "Probe compound"
    assert report == validate_simapro_stock_rows(before, stock_catalog=catalog(), stock_version="10.2")


@pytest.mark.parametrize(
    "kwargs", [{"name": "Missing"}, {"name": "probe compound"}, {"main": "Emissions to water"}, {"unit": "g"}]
)
def test_exact_missing_wrongcase_main_or_unit_is_unmatched(kwargs):
    report = validate_simapro_stock_rows(rows(**kwargs), stock_catalog=catalog(), stock_version="10.2")
    assert report.has_errors and report.metrics["unmatched"] == 1
    assert report.metrics["stock_membership_validated"] is False
    assert report.issues[0].code == "simapro_stock_unmatched"


def test_wrong_version_never_falls_back_or_resolves_patch_series():
    report = validate_simapro_stock_rows(rows(), stock_catalog=catalog(), stock_version="10.2.0")
    assert report.has_errors and report.metrics["unknown"] == 1
    assert any(issue.code == "simapro_stock_version_mismatch" for issue in report.issues)


def test_partial_miss_is_unknown_and_partial_positive_is_witnessed():
    stock = catalog(complete=False)
    positive = validate_simapro_stock_rows(rows(), stock_catalog=stock, stock_version="10.2")
    assert not positive.has_errors and positive.metrics["catalogue_complete"] is False
    missing = validate_simapro_stock_rows(rows(name="Absent"), stock_catalog=stock, stock_version="10.2")
    assert missing.has_errors and missing.metrics["unknown"] == 1 and missing.metrics["unmatched"] == 0


def test_ambiguous_native_IDs_are_not_chosen():
    stock = catalog()
    stock = replace(stock, substances=stock.substances + (replace(stock.substances[0], stock_id="different-id"),))
    report = validate_simapro_stock_rows(rows(), stock_catalog=stock, stock_version="10.2")
    assert report.has_errors and report.metrics["unknown"] == 1
    assert report.issues[0].code == "simapro_stock_ambiguous"


def test_missing_subcompartment_dimension_is_optional_but_never_claimed():
    stock = catalog()
    report = validate_simapro_stock_rows(rows(sub="low. pop."), stock_catalog=stock, stock_version="10.2")
    assert not report.has_errors and not report.metrics["subcompartments_checked"]
    report = validate_simapro_stock_rows(
        rows(sub="low. pop."), stock_catalog=stock, stock_version="10.2", check_subcompartments=True
    )
    assert report.has_errors and report.issues[0].code == "simapro_stock_subcompartment_unknown"


def test_explicit_subcompartments_match_exactly():
    stock = replace(
        catalog(), substances=(replace(catalog().substances[0], subcompartment="low. pop."),), subcompartments=True
    )
    good = validate_simapro_stock_rows(
        rows(sub="low. pop."), stock_catalog=stock, stock_version="10.2", check_subcompartments=True
    )
    bad = validate_simapro_stock_rows(
        rows(sub="high. pop."), stock_catalog=stock, stock_version="10.2", check_subcompartments=True
    )
    assert not good.has_errors and good.metrics["subcompartments_checked"]
    assert bad.has_errors and bad.metrics["unmatched"] == 1


def test_technosphere_and_parameter_sections_are_excluded():
    values = rows() + [
        [],
        ["Materials/fuels"],
        ["Missing supplier", "kg", 100],
        [],
        ["Input parameters"],
        ["Not a substance", 4, "Undefined"],
    ]
    report = validate_simapro_stock_rows(values, stock_catalog=catalog(), stock_version="10.2")
    assert not report.has_errors and report.metrics["substance_rows_checked"] == 1


def test_uncovered_section_is_unknown_even_for_otherwise_complete_catalogue():
    stock = replace(catalog(), covered_sections=frozenset({"Emissions to air"}))
    report = validate_simapro_stock_rows(rows(main="Emissions to water"), stock_catalog=stock, stock_version="10.2")
    assert report.has_errors and report.metrics["unknown"] == 1


def test_zero_empty_and_malformed_input_do_not_certify_stock():
    empty = validate_simapro_stock_rows(
        [["Process"], [], ["Resources"], [], ["End"]], stock_catalog=catalog(), stock_version="10.2"
    )
    assert not empty.has_errors and empty.metrics["stock_state"] == "not_applicable"
    assert empty.metrics["stock_membership_validated"] is False
    malformed = validate_simapro_stock_rows(
        rows()[:-2] + [["Broken row"]], stock_catalog=catalog(), stock_version="10.2"
    )
    assert malformed.has_errors
    unknown = validate_simapro_stock_csv(
        b"not a native substance section\n", stock_catalog=catalog(), stock_version="10.2"
    )
    assert unknown.has_errors and not unknown.metrics["stock_membership_validated"]


def test_csv_bytes_are_not_transformed_again(monkeypatch):
    content = csv_bytes(rows(name="=Exact native cell"))
    stock = catalog(name="=Exact native cell")
    monkeypatch.setattr(
        "brightpath.formats.simapro_csv._latin1_safe_cell", lambda _: pytest.fail("Saved cells transformed twice")
    )
    report = validate_simapro_stock_csv(content, stock_catalog=stock, stock_version="10.2")
    assert not report.has_errors


def test_render_rows_use_the_actual_latin1_and_spreadsheet_cell_transform():
    name = "=Probe\u2009compound"
    stock = catalog(name=str(_latin1_safe_cell(name)))
    report = validate_simapro_stock_rows(rows(name=name), stock_catalog=stock, stock_version="10.2")
    assert not report.has_errors


def test_malformed_quoted_csv_returns_structured_issue():
    report = validate_simapro_stock_csv(b'"unterminated', stock_catalog=catalog(), stock_version="10.2")
    assert report.has_errors and report.issues[0].code == "simapro_stock_csv_invalid"


def test_real_renderer_facade_and_written_CSV_agree(tmp_path):
    source = inventory()
    before = source.data
    rendered = source.render()
    report = source.validate_stock(stock_catalog=catalog(), stock_version="10.2")
    assert not rendered.has_errors and not report.has_errors
    path = source.write_csv(tmp_path / "real", validate=False, stock_catalog=catalog(), stock_version="10.2")
    saved = validate_simapro_stock_csv(path.read_bytes(), stock_catalog=catalog(), stock_version="10.2")
    assert saved.metrics["matches"] == report.metrics["matches"]
    assert source.data == before
    assert b"Probe compound;;kg;rate" in path.read_bytes()


def test_low_level_guard_runs_before_directory_or_existing_file_write(tmp_path):
    source = inventory()
    target = tmp_path / "not-created" / "rejected.csv"
    with pytest.raises(InventoryValidationError) as error:
        write_simapro_csv(source._document, target, stock_catalog=catalog(name="Other"), stock_version="10.2")
    assert not target.parent.exists()
    assert error.value.report.stages[0].metrics["unmatched"] == 1
    existing = tmp_path / "existing.csv"
    existing.write_bytes(b"keep existing")
    with pytest.raises(InventoryValidationError):
        source.write_csv(existing, validate=False, stock_catalog=catalog(name="Other"), stock_version="10.2")
    assert existing.read_bytes() == b"keep existing"


@pytest.mark.parametrize("low_level", [False, True])
def test_writer_given_version_requires_matching_catalogue(tmp_path, low_level):
    with pytest.raises(ValueError, match="requires a matching stock_catalog"):
        if low_level:
            write_simapro_csv(inventory()._document, tmp_path / "none.csv", stock_version="10.2")
        else:
            inventory().write_csv(tmp_path / "none.csv", validate=False, stock_version="10.2")
    assert not (tmp_path / "none.csv").exists()


@pytest.mark.parametrize("low_level", [False, True])
def test_writer_without_version_ignores_supplied_catalogue(monkeypatch, tmp_path, low_level):
    import brightpath.formats.simapro_stock as stock_module

    def unexpected(*args, **kwargs):
        pytest.fail("No substance-list check should run without a target version.")

    monkeypatch.setattr(stock_module, "validate_simapro_stock_rows", unexpected)
    source = inventory()
    before = source.data
    target = tmp_path / "unchecked.csv"
    if low_level:
        write_simapro_csv(source._document, target, stock_catalog=catalog(name="Other"), check_subcompartments=True)
    else:
        source.write_csv(target, validate=False, stock_catalog=catalog(name="Other"), check_subcompartments=True)
    assert b"Probe compound;;kg;rate" in target.read_bytes()
    assert source.data == before


def test_legacy_programmatic_write_does_not_select_any_stock(tmp_path):
    assert inventory().write_csv(tmp_path / "legacy.csv", validate=False).exists()


def test_injected_background_provider_reaches_facade_render_validation(monkeypatch):
    profile = BiosphereProfile("ecoinvent", "3.9.1")
    key = ("Sodium I", ("water",), "kilogram")
    biosphere = BiosphereCatalog(profile, frozenset({key}), references={key: "5ac961f0-8e53-42ff-b13f-1a7fb9133868"})
    provider = InMemoryCatalogProvider(biosphere=[biosphere])
    data = inventory().data
    data[0]["exchanges"][1].update(name="Sodium I", categories=("water",))
    source = SimaProInventory.from_data(data, background_profile=BackgroundProfile("ecoinvent", "3.9.1", "cutoff"))
    for module in ("simapro_csv", "simapro_sodium_identity", "simapro_water_ions"):
        # A syntax-only codec may use only the injected provider and own no factory.
        monkeypatch.setattr(
            f"brightpath.formats.{module}.catalog_provider_from_environment",
            lambda: pytest.fail("Injected provider ignored"),
            raising=False,
        )
    report = source.validate(check_background_links=False, check_simapro_rendering=True, catalog_provider=provider)
    assert not report.has_errors, report.issues


def glad_row(name="Probe compound", token="Professional 10.2", context="Airborne emissions"):
    return {
        "Flowable": name,
        "Unit": "kg",
        "Context": context,
        "FlowUUID": "synthetic-guid",
        "SupportedNomenclatures": token,
    }


def test_GLAD_filters_exact_token_and_declares_main_only_coverage():
    source = [
        glad_row(token="SimaPro EF 3.1.1; Professional 10.2"),
        glad_row(name="Not selected", token="Professional 10.20"),
    ]
    before = deepcopy(source)
    stock = SimaProStockCatalog.from_glad_rows(source, stock_version="10.2", source="fixture", source_sha256="a" * 64)
    assert len(stock.substances) == 1 and stock.substances[0].main_compartment == "Emissions to air"
    assert stock.complete is False and stock.subcompartments is False
    assert source == before
    with pytest.raises(ValueError, match="No rows"):
        SimaProStockCatalog.from_glad_rows(source, stock_version="10.4", source="fixture", source_sha256="a" * 64)


def test_external_GLAD_xlsx_loader_pins_read_only_source(tmp_path):
    path = tmp_path / "external.xlsx"
    book = Workbook()
    sheet = book.active
    keys = tuple(glad_row())
    sheet.append(keys)
    sheet.append(tuple(glad_row().values()))
    book.save(path)
    book.close()
    before = path.read_bytes()
    stock = SimaProStockCatalog.load_glad_xlsx(path, stock_version="10.2", complete=True)
    assert stock.source_sha256 == hashlib.sha256(before).hexdigest()
    assert stock.complete and path.read_bytes() == before
    assert not validate_simapro_stock_rows(rows(), stock_catalog=stock, stock_version="10.2").has_errors
