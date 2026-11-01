"""Case-colliding included recipes keep distinct labels and independent samples."""

import base64
import csv
import json
import math
from copy import deepcopy

import pytest

from brightpath.background.catalogs import PackageCatalogProvider
from brightpath.formats.simapro_csv import (
    _restore_activity_identity,
    load_simapro_csv,
    render_simapro_rows,
)
from brightpath.formats.simapro_native_uncertainty import prepare, render
from brightpath.models import BackgroundProfile, InventoryDocument, InventoryFormat

FIELDS = ("name", "reference product", "location", "unit")


def sample(reverse=False):
    records = []
    for chemical, amount in [("HFC-152a", 10.0), ("hfc-152a", 20.0)]:
        head = {
            "name": f"1,1-difluoroethane production, {chemical}",
            "reference product": f"1,1-difluoroethane, {chemical}",
            "location": "US",
            "unit": "kilogram",
            "comment": "Synthetic case-distinct foreground fixture.",
        }
        records.append(
            {
                **head,
                "exchanges": [
                    {**head, "type": "production", "amount": 1.0, "simapro category": "Materials/Other"},
                    {
                        "name": "Carbon dioxide, fossil",
                        "type": "biosphere",
                        "categories": ("air",),
                        "unit": "kilogram",
                        "amount": amount,
                        "uncertainty type": 2,
                        "loc": math.log(amount * 1.1),
                        "scale": 0.2,
                    },
                ],
            }
        )
    consumer = {
        "name": "Synthetic dual recipe consumer",
        "reference product": "synthetic output",
        "location": "GLO",
        "unit": "kilogram",
        "comment": "Synthetic consumer retaining separate recipe links.",
    }
    records.append(
        {
            **consumer,
            "parameters": [{"name": "dose", "amount": 0.25}],
            "exchanges": [
                {**consumer, "type": "production", "amount": 1.0, "simapro category": "Materials/Other"},
                *[
                    {**{k: r[k] for k in FIELDS}, "type": "technosphere", "amount": q, "formula": f"{factor}*dose"}
                    for r, q, factor in zip(records, (0.25, 0.5), (1, 2), strict=True)
                ],
            ],
        }
    )
    return list(reversed(records)) if reverse else records


@pytest.mark.parametrize("version", ["3.8", "3.12"])
@pytest.mark.parametrize("reverse", [False, True])
def test_distinct_recipes_and_foreground_links_roundtrip_without_consolidation(tmp_path, version, reverse):
    source = InventoryDocument(
        data=sample(reverse),
        background_profile=BackgroundProfile("ecoinvent", version, "cutoff"),
        inventory_format=InventoryFormat.BRIGHTWAY_EXCEL,
    )
    before = deepcopy(source.data)
    prepared, specs = prepare(source)
    assert len(specs) == 2 and specs[0].latent != specs[1].latent
    rows = render(prepared, specs)
    production_labels = [rows[i + 1][0] for i, row in enumerate(rows) if row == ["Products"]]
    assert len(production_labels) == 3 and len({n.casefold() for n in production_labels}) == 3
    assert sum("[BrightPath fg " in n for n in production_labels) == 2
    path = tmp_path / "case-only.csv"
    with path.open("w", encoding="latin-1", newline="") as stream:
        csv.writer(stream, delimiter=";").writerows(rows)
    loaded = load_simapro_csv(
        path, background_profile=source.background_profile, catalog_provider=PackageCatalogProvider()
    )
    by_identity = {tuple(a[k] for k in FIELDS): a for a in loaded.data}
    assert len(by_identity) == 3
    for original in before:
        actual = by_identity[tuple(original[k] for k in FIELDS)]
        for expected in original["exchanges"]:
            if expected["type"] == "biosphere":
                match = next(e for e in actual["exchanges"] if e["type"] == "biosphere")
                assert match["amount"] == expected["amount"]
                assert match["loc"] == pytest.approx(expected["loc"], rel=1e-12)
                assert match["scale"] == pytest.approx(expected["scale"], rel=1e-12)
            elif expected["type"] == "technosphere":
                matches = [
                    e
                    for e in actual["exchanges"]
                    if e["type"] == "technosphere" and tuple(e[k] for k in FIELDS) == tuple(expected[k] for k in FIELDS)
                ]
                assert len(matches) == 1 and matches[0]["amount"] == expected["amount"]
                assert matches[0]["formula"] == expected["formula"]
    assert source.data == before


def test_single_foreground_keeps_original_native_label():
    source = InventoryDocument(
        data=sample()[:1],
        background_profile=BackgroundProfile("ecoinvent", "3.12", "cutoff"),
        inventory_format=InventoryFormat.BRIGHTWAY_EXCEL,
    )
    result = render_simapro_rows(source)
    assert not result.has_errors
    assert not any("[BrightPath fg " in str(c) for r in result.rows for c in r)


def test_same_folded_name_with_different_units_is_still_refused():
    records = sample()[:2]
    records[1]["unit"] = "cubic meter"
    records[1]["exchanges"][0]["unit"] = "cubic meter"
    source = InventoryDocument(
        data=records,
        background_profile=BackgroundProfile("ecoinvent", "3.12", "cutoff"),
        inventory_format=InventoryFormat.BRIGHTWAY_EXCEL,
    )
    result = render_simapro_rows(source)
    assert result.has_errors and result.rows == []


def test_alias_marker_identity_and_unit_tampering_is_refused():
    source = InventoryDocument(
        data=sample(),
        background_profile=BackgroundProfile("ecoinvent", "3.12", "cutoff"),
        inventory_format=InventoryFormat.BRIGHTWAY_EXCEL,
    )
    result = render_simapro_rows(source)
    assert not result.has_errors
    label = next(r[0] for r in result.rows if len(r) >= 7 and "[BrightPath fg " in str(r[0]))
    original = sample()[0]
    identity = {k: original[k] for k in FIELDS}

    def metadata(value):
        payload = base64.b64encode(json.dumps(value).encode()).decode()
        return f"[BrightPath activity v1:{payload}]"

    valid = {"simapro name": label, "unit": "kilogram", "comment": metadata(identity)}
    assert _restore_activity_identity(deepcopy(valid), source.background_profile)
    for tampered in [
        {**valid, "simapro name": label.replace("BrightPath fg ", "BrightPath fg x")},
        {**valid, "comment": metadata({**identity, "reference product": "another product"})},
        {**valid, "unit": "gram"},
    ]:
        with pytest.raises(ValueError, match="conflicts"):
            _restore_activity_identity(tampered, source.background_profile)
