"""Numeric-looking profile labels must survive Brightway's numeric strategies."""

from copy import deepcopy

import pytest

from brightpath import BackgroundContext, BiosphereProfile, FormatProfile, InventoryContext, TechnosphereProfile
from brightpath.formats.brightway_delimited import load_brightway_delimited, write_brightway_delimited
from brightpath.formats.brightway_excel import load_brightway_excel, write_brightway_excel
from brightpath.models import InventoryDocument


@pytest.mark.parametrize(
    "format_id,suffix", [("brightway_excel", ".xlsx"), ("brightway_csv", ".csv"), ("brightway_tsv", ".tsv")]
)
@pytest.mark.parametrize(
    "biosphere", [("uvek", "2026"), ("uvek", "2026.0"), ("ecoinvent", "3.10"), ("ecoinvent", "3.10.1")]
)
def test_exact_biosphere_and_format_versions_survive_without_explicit_overrides(tmp_path, format_id, suffix, biosphere):
    activity = {"name": "Foreground", "reference product": "output", "location": "CH", "unit": "kilogram"}
    context = InventoryContext(
        FormatProfile(format_id, format_version="1.10", dialect="bw2io", encoding="utf-8"),
        BackgroundContext(TechnosphereProfile("uvek", "2026", "cutoff"), BiosphereProfile(*biosphere)),
    )
    document = InventoryDocument(
        data=[
            {
                **activity,
                "comment": "Exact profile fixture.",
                "exchanges": [
                    {**activity, "type": "production", "amount": 1},
                ],
            }
        ],
        context=context,
    )
    before = deepcopy(document.data)
    writer, reader = (
        (write_brightway_excel, load_brightway_excel)
        if format_id == "brightway_excel"
        else (write_brightway_delimited, load_brightway_delimited)
    )
    for iteration in range(2):
        path = writer(document, tmp_path / f"profile-{iteration}{suffix}")
        restored = reader(path)
        assert restored.context == context
        assert document.data == before
        document = restored
        before = deepcopy(restored.data)
