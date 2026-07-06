import json
from pathlib import Path
from types import SimpleNamespace

from brightpath import BrightwayConverter
from brightpath.analysis import (
    SOURCE_FORMAT_BRIGHTWAY_EXCEL,
    SOURCE_FORMAT_SIMAPRO_CSV,
    analyze_inventory,
    infer_source_format,
)
from brightpath.models import BackgroundProfile
from brightpath.simaproconverter import SimaproConverter


def minimal_activity(extra_exchanges=None, **overrides):
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
                "location": "GLO",
                "unit": "kilogram",
                "amount": 1.0,
                "simapro category": "Materials/Test",
            }
        ],
    }
    activity.update(overrides)
    if extra_exchanges:
        activity["exchanges"].extend(extra_exchanges)
    return activity


def make_brightway_excel(tmp_path, data, db_name="analysis_db"):
    converter = SimaproConverter.__new__(SimaproConverter)
    converter.i = SimpleNamespace(data=data, db_name=db_name)
    converter.db_name = db_name
    return converter.write_brightway_excel(tmp_path / "inventory")


def make_simapro_csv(tmp_path, data, filename="inventory.csv"):
    converter = BrightwayConverter(data=data, export_dir=tmp_path)
    converter.convert_to_simapro(filename=filename)
    return tmp_path / filename


def write_catalog(tmp_path, *, family, version, system_model, technosphere, biosphere):
    directory = tmp_path / "reference_catalogs"
    directory.mkdir(exist_ok=True)
    path = directory / f"{family}__{version}__{system_model}.json"
    path.write_text(
        json.dumps(
            {
                "profile": {
                    "family": family,
                    "version": version,
                    "system_model": system_model,
                },
                "technosphere": technosphere,
                "biosphere": biosphere,
            }
        ),
        encoding="utf-8",
    )
    return directory


def test_infer_source_format_supports_xlsx_and_csv():
    assert infer_source_format("inventory.xlsx") == SOURCE_FORMAT_BRIGHTWAY_EXCEL
    assert infer_source_format("inventory.csv") == SOURCE_FORMAT_SIMAPRO_CSV


def test_analyze_brightway_excel_returns_candidate_summaries(tmp_path):
    workbook = make_brightway_excel(tmp_path, [minimal_activity()])

    result = analyze_inventory(
        path=workbook,
        source_profile=BackgroundProfile(
            family="ecoinvent",
            version="3.10",
            system_model="cut-off",
        ),
    )

    assert result.detected_software == "brightway"
    assert result.detected_format == SOURCE_FORMAT_BRIGHTWAY_EXCEL
    assert result.source_profile.family == "ecoinvent"
    assert result.source_profile.system_model == "cutoff"
    assert result.file_issues == []
    assert len(result.candidates) == 1
    assert result.candidates[0].name == "test process"
    assert result.candidates[0].reference_product == "test product"
    assert result.candidates[0].issues == []


def test_analyze_brightway_excel_ignores_missing_simapro_category(tmp_path):
    invalid_activity = minimal_activity()
    del invalid_activity["exchanges"][0]["simapro category"]
    workbook = make_brightway_excel(tmp_path, [invalid_activity])

    result = analyze_inventory(path=workbook, source_format=SOURCE_FORMAT_BRIGHTWAY_EXCEL)

    assert result.file_issues == []
    assert len(result.candidates) == 1
    assert result.candidates[0].issues == []


def test_analyze_brightway_excel_infers_background_profile_from_catalogs(tmp_path, monkeypatch):
    directory = write_catalog(
        tmp_path,
        family="ecoinvent",
        version="3.10",
        system_model="cutoff",
        technosphere=[
            {
                "name": "market for steel",
                "reference_product": "steel",
                "location": "GLO",
                "unit": "kilogram",
            }
        ],
        biosphere=[],
    )
    monkeypatch.setenv("BRIGHTPATH_REFERENCE_DIR", str(directory))
    workbook = make_brightway_excel(
        tmp_path,
        [
            minimal_activity(
                extra_exchanges=[
                    {
                        "type": "technosphere",
                        "name": "market for steel",
                        "reference product": "steel",
                        "location": "GLO",
                        "unit": "kilogram",
                        "amount": 2.0,
                    }
                ]
            )
        ],
    )

    result = analyze_inventory(path=workbook, source_format=SOURCE_FORMAT_BRIGHTWAY_EXCEL)

    assert result.source_profile == BackgroundProfile(
        family="ecoinvent",
        version="3.10",
        system_model="cutoff",
    )
    assert any(issue.code == "background_profile_inferred" for issue in result.file_issues)


def test_analyze_brightway_excel_flags_missing_background_targets(tmp_path, monkeypatch):
    directory = write_catalog(
        tmp_path,
        family="ecoinvent",
        version="3.10",
        system_model="cutoff",
        technosphere=[],
        biosphere=[],
    )
    monkeypatch.setenv("BRIGHTPATH_REFERENCE_DIR", str(directory))
    workbook = make_brightway_excel(
        tmp_path,
        [
            minimal_activity(
                extra_exchanges=[
                    {
                        "type": "technosphere",
                        "name": "missing market",
                        "reference product": "missing product",
                        "location": "CH",
                        "unit": "kilogram",
                        "amount": 2.0,
                    },
                    {
                        "type": "biosphere",
                        "name": "mystery flow",
                        "categories": ("air", "urban air close to ground"),
                        "unit": "kilogram",
                        "amount": 1.0,
                    },
                ]
            )
        ],
    )

    result = analyze_inventory(
        path=workbook,
        source_profile=BackgroundProfile(
            family="ecoinvent",
            version="3.10",
            system_model="cutoff",
        ),
    )

    issue_codes = [issue.code for issue in result.candidates[0].issues]
    assert "unknown_technosphere_target" in issue_codes
    assert "unknown_biosphere_flow" in issue_codes


def test_analyze_brightway_excel_accepts_uvek_catalog_with_external_biosphere_reference(
    tmp_path, monkeypatch
):
    directory = write_catalog(
        tmp_path,
        family="uvek",
        version="2025",
        system_model="cutoff",
        technosphere=[
            {
                "name": "market for steel",
                "reference_product": "steel",
                "location": "CH",
                "unit": "kilogram",
            }
        ],
        biosphere=[
            {
                "name": "Carbon dioxide, fossil",
                "categories": ["air"],
                "unit": "kilogram",
            }
        ],
    )
    monkeypatch.setenv("BRIGHTPATH_REFERENCE_DIR", str(directory))
    workbook = make_brightway_excel(
        tmp_path,
        [
            minimal_activity(
                extra_exchanges=[
                    {
                        "type": "technosphere",
                        "name": "market for steel",
                        "reference product": "steel",
                        "location": "CH",
                        "unit": "kilogram",
                        "amount": 2.0,
                    },
                    {
                        "type": "biosphere",
                        "name": "Carbon dioxide, fossil",
                        "categories": ("air",),
                        "unit": "kilogram",
                        "amount": 1.0,
                    },
                ]
            )
        ],
    )

    result = analyze_inventory(
        path=workbook,
        source_profile=BackgroundProfile(
            family="uvek",
            version="2025",
            system_model="cutoff",
        ),
    )

    assert result.file_issues == []
    assert result.candidates[0].issues == []


def test_analyze_brightway_excel_keeps_structural_validation_errors_blocking(tmp_path):
    invalid_activity = minimal_activity()
    invalid_activity["exchanges"][0]["unit"] = "made-up unit"
    workbook = make_brightway_excel(tmp_path, [invalid_activity])

    result = analyze_inventory(path=workbook, source_format=SOURCE_FORMAT_BRIGHTWAY_EXCEL)

    assert result.file_issues == []
    assert len(result.candidates) == 1
    assert len(result.candidates[0].issues) == 1
    assert result.candidates[0].issues[0].severity == "error"
    assert result.candidates[0].issues[0].code == "inventory_validation_error"
    assert "unknown exchange unit" in result.candidates[0].issues[0].message


def test_analyze_simapro_csv_returns_candidate_summaries(tmp_path):
    filepath = make_simapro_csv(tmp_path, [minimal_activity()])

    result = analyze_inventory(
        path=filepath,
        source_profile=BackgroundProfile(
            family="ecoinvent",
            version="3.9",
            system_model="cutoff",
        ),
    )

    assert result.detected_software == "simapro"
    assert result.detected_format == SOURCE_FORMAT_SIMAPRO_CSV
    assert result.file_issues == []
    assert len(result.candidates) == 1
    assert result.candidates[0].name == "Test process"
    assert result.candidates[0].reference_product == "test product"
    assert result.candidates[0].location == "GLO"


def test_analyze_simapro_csv_attaches_duplicate_identity_errors(tmp_path):
    filepath = make_simapro_csv(tmp_path, [minimal_activity(), minimal_activity()], filename="duplicates.csv")

    result = analyze_inventory(path=filepath, source_format=SOURCE_FORMAT_SIMAPRO_CSV)

    assert result.file_issues == []
    assert len(result.candidates) == 2
    assert all(candidate.issues for candidate in result.candidates)
    assert all(candidate.issues[0].code == "duplicate_dataset_identity" for candidate in result.candidates)


def test_analyze_inventory_reports_missing_file_as_file_error(tmp_path):
    missing = tmp_path / "missing.xlsx"

    result = analyze_inventory(path=missing, source_format=SOURCE_FORMAT_BRIGHTWAY_EXCEL)

    assert len(result.file_issues) == 1
    assert result.file_issues[0].code == "brightway_excel_parse_failed"
    assert "could not be found" in result.file_issues[0].message
