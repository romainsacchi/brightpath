"""Regression for numeric source comments reaching ILCD's string-length guard."""

from copy import deepcopy

import pytest
from test_ilcd import context, dataset, document, profile

from brightpath.formats.ilcd import load_ilcd, validate_ilcd_package, write_ilcd


@pytest.mark.parametrize("comment", [0.006202616822429906, 17, "Complete qualification — " * 40])
def test_comment_text_conversion_does_not_change_science_or_source(tmp_path, comment):
    record = dataset()
    record["parameters"] = [{"name": "dose", "amount": 100}]
    record["exchanges"][0].update(
        {"comment": comment, "formula": "dose", "uncertainty type": 3, "loc": 100, "scale": 2}
    )
    before = deepcopy(record)
    source = document(record)
    source_before = source.data
    output = write_ilcd(source, tmp_path / "numeric.zip", metadata_profile=profile(), dialect="openlca")
    assert validate_ilcd_package(output) == ()
    restored = load_ilcd(output, context=context()).data[0]
    exchange = restored["exchanges"][0]
    assert exchange["comment"] == str(comment)
    for field in (
        "amount",
        "loc",
        "scale",
        "formula",
        "uncertainty type",
        "type",
        "name",
        "reference product",
        "location",
        "unit",
    ):
        assert exchange[field] == before["exchanges"][0][field]
    for field in ("name", "reference product", "location", "unit"):
        assert restored[field] == before[field]
    text_record = deepcopy(before)
    text_record["exchanges"][0]["comment"] = str(comment)
    control = write_ilcd(document(text_record), tmp_path / "text.zip", metadata_profile=profile(), dialect="openlca")
    assert output.read_bytes() == control.read_bytes()
    assert source.data == source_before
    assert record == before
