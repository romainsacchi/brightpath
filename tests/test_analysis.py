import csv
import json
from pathlib import Path
from types import SimpleNamespace

from openpyxl import load_workbook

from brightpath import BrightwayConverter
from brightpath.analysis import (
    InventoryValidationError,
    SOURCE_FORMAT_BRIGHTWAY_CSV,
    SOURCE_FORMAT_BRIGHTWAY_EXCEL,
    SOURCE_FORMAT_BRIGHTWAY_TSV,
    SOURCE_FORMAT_SIMAPRO_CSV,
    analyze_inventory,
    infer_source_format,
    validate_inventory,
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


def make_brightway_delimited(tmp_path, data, *, delimiter=",", suffix=".csv", db_name="analysis_db"):
    workbook = make_brightway_excel(tmp_path, data, db_name=db_name)
    sheet = load_workbook(workbook, read_only=True, data_only=False).active
    path = tmp_path / f"inventory{suffix}"
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle, delimiter=delimiter)
        for row in sheet.iter_rows(values_only=True):
            writer.writerow(["" if cell is None else cell for cell in row])
    return path


def _normalize_technosphere_entries(entries):
    normalized = []
    for row in entries:
        if isinstance(row, dict):
            normalized.append(
                (
                    str(row["name"]),
                    str(row["reference_product"]),
                    str(row["location"]),
                    str(row["unit"]),
                )
            )
            continue
        normalized.append(tuple(row))
    return normalized


def _normalize_biosphere_entries(entries):
    normalized = []
    for row in entries:
        if isinstance(row, dict):
            normalized.append(
                (
                    str(row["name"]),
                    tuple(str(item) for item in row["categories"]),
                    str(row["unit"]),
                )
            )
            continue
        name, categories, unit = row
        normalized.append((str(name), tuple(str(item) for item in categories), str(unit)))
    return normalized


def write_catalog(tmp_path, *, family, version, system_model, technosphere, biosphere):
    directory = tmp_path / "reference_catalogs"
    directory.mkdir(exist_ok=True)
    normalized_technosphere = _normalize_technosphere_entries(technosphere)
    normalized_biosphere = _normalize_biosphere_entries(biosphere)
    path = directory / f"{family}__{version}__{system_model}.json"
    path.write_text(
        json.dumps(
            {
                "profile": {
                    "family": family,
                    "version": version,
                    "system_model": system_model,
                },
                "technosphere": [
                    {
                        "name": name,
                        "reference_product": reference_product,
                        "location": location,
                        "unit": unit,
                    }
                    for name, reference_product, location, unit in normalized_technosphere
                ],
                "biosphere": [
                    {
                        "name": name,
                        "categories": list(categories),
                        "unit": unit,
                    }
                    for name, categories, unit in normalized_biosphere
                ],
            }
        ),
        encoding="utf-8",
    )
    return directory


def test_infer_source_format_supports_xlsx_and_csv():
    assert infer_source_format("inventory.xlsx") == SOURCE_FORMAT_BRIGHTWAY_EXCEL
    assert infer_source_format("inventory.tsv") == SOURCE_FORMAT_BRIGHTWAY_TSV
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
    assert result.candidates[0].description_hint == ""
    assert result.candidates[0].source_hint == ""
    assert result.candidates[0].issues == []


def test_analyze_brightway_excel_extracts_description_and_source_hints(tmp_path):
    workbook = make_brightway_excel(
        tmp_path,
        [
            minimal_activity(
                comment="Dataset comment",
                source="Journal article",
            )
        ],
    )

    result = analyze_inventory(path=workbook, source_format=SOURCE_FORMAT_BRIGHTWAY_EXCEL)

    assert len(result.candidates) == 1
    assert result.candidates[0].description_hint == "Dataset comment"
    assert result.candidates[0].source_hint == "Journal article"


def test_analyze_brightway_excel_extracts_trailing_source_from_comment(tmp_path):
    workbook = make_brightway_excel(
        tmp_path,
        [
            minimal_activity(
                comment=(
                    "This dataset models a foreground process. "
                    "Source: Foteinis et al. (2023). https://doi.org/example"
                )
            )
        ],
    )

    result = analyze_inventory(path=workbook, source_format=SOURCE_FORMAT_BRIGHTWAY_EXCEL)

    assert len(result.candidates) == 1
    assert result.candidates[0].description_hint == "This dataset models a foreground process."
    assert (
        result.candidates[0].source_hint
        == "Foteinis et al. (2023). https://doi.org/example"
    )


def test_analyze_brightway_excel_keeps_comment_when_source_marker_is_not_trailing(tmp_path):
    workbook = make_brightway_excel(
        tmp_path,
        [
            minimal_activity(
                comment=(
                    "Source: internal screening assumptions. "
                    "This dataset models a foreground process."
                )
            )
        ],
    )

    result = analyze_inventory(path=workbook, source_format=SOURCE_FORMAT_BRIGHTWAY_EXCEL)

    assert len(result.candidates) == 1
    assert (
        result.candidates[0].description_hint
        == "Source: internal screening assumptions. This dataset models a foreground process."
    )
    assert result.candidates[0].source_hint == ""


def test_analyze_brightway_excel_ignores_missing_simapro_category(tmp_path):
    invalid_activity = minimal_activity()
    del invalid_activity["exchanges"][0]["simapro category"]
    workbook = make_brightway_excel(tmp_path, [invalid_activity])

    result = analyze_inventory(path=workbook, source_format=SOURCE_FORMAT_BRIGHTWAY_EXCEL)

    assert result.file_issues == []
    assert len(result.candidates) == 1
    assert result.candidates[0].issues == []


def test_analyze_brightway_csv_returns_candidate_summaries(tmp_path):
    csv_path = make_brightway_delimited(tmp_path, [minimal_activity()], suffix=".csv")

    result = analyze_inventory(
        path=csv_path,
        source_format=SOURCE_FORMAT_BRIGHTWAY_CSV,
        source_profile=BackgroundProfile(
            family="ecoinvent",
            version="3.10",
            system_model="cut-off",
        ),
    )

    assert result.detected_software == "brightway"
    assert result.detected_format == SOURCE_FORMAT_BRIGHTWAY_CSV
    assert result.source_profile.system_model == "cutoff"
    assert result.file_issues == []
    assert len(result.candidates) == 1
    assert result.candidates[0].name == "test process"
    assert result.candidates[0].issues == []


def test_analyze_brightway_tsv_returns_candidate_summaries(tmp_path):
    tsv_path = make_brightway_delimited(tmp_path, [minimal_activity()], delimiter="\t", suffix=".tsv")

    result = analyze_inventory(
        path=tsv_path,
        source_format=SOURCE_FORMAT_BRIGHTWAY_TSV,
    )

    assert result.detected_software == "brightway"
    assert result.detected_format == SOURCE_FORMAT_BRIGHTWAY_TSV
    assert result.file_issues == []
    assert len(result.candidates) == 1
    assert result.candidates[0].name == "test process"


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


def test_analyze_brightway_excel_prefers_latest_cutoff_when_profile_matches_are_tied(
    tmp_path, monkeypatch
):
    directory = tmp_path / "reference_catalogs"
    directory.mkdir(exist_ok=True)
    for version in ("3.9", "3.10"):
        for system_model in ("cutoff", "consequential"):
            write_catalog(
                tmp_path,
                family="ecoinvent",
                version=version,
                system_model=system_model,
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
    assert len(result.file_issues) == 1
    assert result.file_issues[0].severity == "warning"
    assert result.file_issues[0].code == "background_profile_assumed"
    assert "selected ecoinvent 3.10 cutoff" in result.file_issues[0].message
    assert "override" in result.file_issues[0].suggested_fix


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


def test_analyze_brightway_excel_groups_unknown_technosphere_targets_per_candidate(
    tmp_path, monkeypatch
):
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
                        "name": "missing market a",
                        "reference product": "missing product a",
                        "location": "CH",
                        "unit": "kilogram",
                        "amount": 2.0,
                    },
                    {
                        "type": "technosphere",
                        "name": "missing market b",
                        "reference product": "missing product b",
                        "location": "RER",
                        "unit": "megajoule",
                        "amount": 3.0,
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

    technosphere_issues = [
        issue
        for issue in result.candidates[0].issues
        if issue.code == "unknown_technosphere_target"
    ]
    assert len(technosphere_issues) == 1
    assert "missing market a | missing product a | CH | kilogram" in technosphere_issues[0].message
    assert "missing market b | missing product b | RER | megajoule" in technosphere_issues[0].message
    assert technosphere_issues[0].suggested_fix


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


def test_validate_inventory_raises_for_unknown_background_targets(tmp_path, monkeypatch):
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
                    }
                ]
            )
        ],
    )

    try:
        validate_inventory(
            path=workbook,
            source_profile=BackgroundProfile(
                family="ecoinvent",
                version="3.10",
                system_model="cutoff",
            ),
        )
    except InventoryValidationError as exc:
        assert exc.result.candidates[0].issues[0].code == "unknown_technosphere_target"
        assert "Technosphere exchanges do not match" in str(exc)
    else:
        raise AssertionError("InventoryValidationError was not raised")


def test_validate_inventory_raises_when_background_catalog_is_missing(tmp_path, monkeypatch):
    directory = tmp_path / "reference_catalogs"
    directory.mkdir()
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

    try:
        validate_inventory(
            path=workbook,
            source_profile=BackgroundProfile(
                family="ecoinvent",
                version="3.5",
                system_model="cutoff",
            ),
        )
    except InventoryValidationError as exc:
        assert exc.result.file_issues[0].code == "background_catalog_missing"
        assert "No local reference catalog is available" in str(exc)
    else:
        raise AssertionError("InventoryValidationError was not raised")


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
