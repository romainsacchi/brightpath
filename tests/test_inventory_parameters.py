import csv
from io import BytesIO

import pytest
from openpyxl import load_workbook
from preservation_helpers import Profile, export_snapshots, synthetic_snapshot

from brightpath import BackgroundContext, BiosphereProfile, FormatProfile, InventoryContext, TechnosphereProfile
from brightpath.analysis import analyze_inventory

PROFILE = Profile("uvek", "2025")
SUPPLIER = {
    "name": "Electricity, low voltage, at grid",
    "reference product": "Electricity, low voltage, at grid",
    "location": "CH",
    "unit": "kilowatt hour",
}


def test_excel_formulas_are_literal_text_not_executable_cells():
    artifact = export_snapshots([synthetic_snapshot(SUPPLIER)], PROFILE, "brightway")
    workbook = load_workbook(BytesIO(artifact.content), data_only=False)
    cells = [cell for sheet in workbook for row in sheet for cell in row]
    assert any(cell.value == "shared * efficiency" for cell in cells)
    assert not any(cell.data_type == "f" for cell in cells)


@pytest.mark.parametrize(
    "delimiter,suffix,format_id", [(",", ".csv", "brightway_csv"), ("\t", ".tsv", "brightway_tsv")]
)
def test_delimited_analysis_retains_shared_parameter_scopes(tmp_path, delimiter, suffix, format_id):
    source = synthetic_snapshot(SUPPLIER)
    artifact = export_snapshots([source], PROFILE, "brightway")
    workbook = load_workbook(BytesIO(artifact.content), data_only=False)
    path = tmp_path / f"inventory{suffix}"
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle, delimiter=delimiter)
        for row in workbook.active.iter_rows(values_only=True):
            writer.writerow(["" if value is None else value for value in row])
    context = InventoryContext(
        FormatProfile(format_id),
        BackgroundContext(TechnosphereProfile("uvek", "2025", "cutoff"), BiosphereProfile("ecoinvent", "3.10")),
    )
    result = analyze_inventory(path=path, source_context=context)
    assert not result.has_errors, result.file_issues
    assert result.database_parameters == source["database parameters"]
    assert result.project_parameters == source["project parameters"]
