import csv
from copy import deepcopy

import pytest

from brightpath.core import (
    BackgroundContext,
    BiosphereProfile,
    FormatProfile,
    InventoryContext,
    TechnosphereProfile,
)
from brightpath.formats.brightway_delimited import _TSVExtractor, load_brightway_delimited, write_brightway_delimited
from brightpath.models import InventoryDocument


def context(format_id):
    return InventoryContext(
        format=FormatProfile(format_id, format_version="1.2", dialect="bw2io", encoding="utf-8"),
        background=BackgroundContext(
            technosphere=TechnosphereProfile("ecoinvent", "3.10.1", "cutoff"),
            biosphere=BiosphereProfile("ecoinvent", "3.10.2"),
        ),
    )


def inventory_data():
    return [
        {
            "name": "foreground service",
            "reference product": "service",
            "location": "GLO",
            "unit": "unit",
            "custom metadata": {"nested": [1, None, {"source": "test"}]},
            "parameters": [
                {
                    "name": "efficiency",
                    "amount": 0.9,
                    "formula": "=base_efficiency",
                    "group": "foreground",
                    "provenance": {"kind": "measured"},
                }
            ],
            "exchanges": [
                {
                    "name": "foreground service",
                    "reference product": "service",
                    "location": "GLO",
                    "unit": "unit",
                    "amount": 1.0,
                    "type": "production",
                    "custom_none": None,
                    "custom_mapping": {"labels": ["a", "b"]},
                }
            ],
        }
    ]


def document(format_id):
    return InventoryDocument(
        data=inventory_data(),
        context=context(format_id),
        database_name="round-trip",
        metadata={"owner": {"name": "BrightPath"}},
        database_parameters=[{"name": "database_parameter", "amount": 3.0}],
        project_parameters=[{"name": "project_parameter", "amount": 4.0}],
    )


@pytest.mark.parametrize(
    ("suffix", "delimiter", "format_id"),
    [
        (".csv", ",", "brightway_csv"),
        (".tsv", "\t", "brightway_tsv"),
    ],
)
def test_round_trip_preserves_exact_context_unknown_values_and_parameters(tmp_path, suffix, delimiter, format_id):
    source = document(format_id)

    output = write_brightway_delimited(source, tmp_path / f"inventory{suffix}", delimiter)
    loaded = load_brightway_delimited(output)

    assert output == (tmp_path / f"inventory{suffix}").resolve()
    assert loaded.context == source.context
    assert loaded.database_name == "round-trip"
    assert loaded.metadata["owner"] == {"name": "BrightPath"}
    assert loaded.data[0]["custom metadata"] == {"nested": [1, None, {"source": "test"}]}
    assert loaded.data[0]["parameters"][0]["formula"] == "=base_efficiency"
    assert loaded.data[0]["parameters"][0]["provenance"] == {"kind": "measured"}
    assert loaded.data[0]["exchanges"][0]["custom_none"] is None
    assert loaded.data[0]["exchanges"][0]["custom_mapping"] == {"labels": ["a", "b"]}
    assert loaded.database_parameters == [{"name": "database_parameter", "amount": 3.0}]
    assert loaded.project_parameters == [{"name": "project_parameter", "amount": 4.0}]


def test_writer_adds_suffix_selected_by_explicit_delimiter(tmp_path):
    output = write_brightway_delimited(document("brightway_tsv"), tmp_path / "inventory", "\t")

    assert output == (tmp_path / "inventory.tsv").resolve()
    assert load_brightway_delimited(output).context.format.format_id == "brightway_tsv"


def test_tsv_extractor_accepts_bw2io_sheet_name_keyword(tmp_path):
    source = tmp_path / "inventory.tsv"
    source.write_text("Database\ttest\n", encoding="utf-8")

    filename, rows = _TSVExtractor.extract(source, sheet_name=None)

    assert filename == source.name
    assert rows == [["Database", "test"]]


def test_writer_is_deterministic_and_does_not_mutate_input(tmp_path):
    data = inventory_data()
    original = deepcopy(data)
    source = InventoryDocument(data=data, context=context("brightway_csv"))
    document_before = source.data

    first = write_brightway_delimited(source, tmp_path / "first.csv")
    second = write_brightway_delimited(source, tmp_path / "second.csv")

    assert first.read_bytes() == second.read_bytes()
    assert data == original
    assert source.data == document_before


def test_writer_tags_formula_like_text_and_loader_restores_it(tmp_path):
    data = inventory_data()
    data[0]["comment"] = '  =HYPERLINK("https://example.com")'
    source = InventoryDocument(data=data, context=context("brightway_csv"), database_name="+formula")

    output = write_brightway_delimited(source, tmp_path / "safe.csv")

    with output.open(encoding="utf-8", newline="") as handle:
        cells = [cell for row in csv.reader(handle) for cell in row]
    assert not any(cell.lstrip().startswith(("=", "+", "-", "@")) for cell in cells)
    loaded = load_brightway_delimited(output)
    assert loaded.database_name == "+formula"
    assert loaded.data[0]["comment"] == '  =HYPERLINK("https://example.com")'


def test_explicit_context_must_match_suffix(tmp_path):
    output = write_brightway_delimited(document("brightway_csv"), tmp_path / "inventory.csv")

    with pytest.raises(ValueError, match="brightway_csv"):
        load_brightway_delimited(output, context=context("brightway_tsv"))


@pytest.mark.parametrize("suffix", [".xlsx", ".txt"])
def test_invalid_suffix_is_rejected(tmp_path, suffix):
    source = tmp_path / f"inventory{suffix}"
    source.write_text("Database,test\n", encoding="utf-8")

    with pytest.raises(ValueError, match=r"\.csv or \.tsv"):
        load_brightway_delimited(source)
    with pytest.raises(ValueError, match=r"\.csv or \.tsv"):
        write_brightway_delimited(document("brightway_csv"), source)


def test_suffix_and_delimiter_must_agree(tmp_path):
    with pytest.raises(ValueError, match="requires the comma"):
        write_brightway_delimited(document("brightway_csv"), tmp_path / "inventory.csv", "\t")

    source = tmp_path / "inventory.tsv"
    source.write_text("Database\ttest\n", encoding="utf-8")
    with pytest.raises(ValueError, match="requires the tab"):
        load_brightway_delimited(source, delimiter=",")


def test_only_comma_and_tab_delimiters_are_supported(tmp_path):
    with pytest.raises(ValueError, match="only comma or tab"):
        write_brightway_delimited(document("brightway_csv"), tmp_path / "inventory.csv", ";")
