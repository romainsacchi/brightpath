from copy import deepcopy

import pytest
from bw2io import ExcelImporter
from bw2io.strategies import link_iterable_by_fields

from brightpath.core import BackgroundContext, BiosphereProfile, FormatProfile, InventoryContext, TechnosphereProfile
from brightpath.formats.brightway_delimited import load_brightway_delimited, write_brightway_delimited
from brightpath.formats.brightway_excel import load_brightway_excel, write_brightway_excel
from brightpath.models import InventoryDocument


def database_document(profile):
    supplier = {
        "name": "foreground supplier",
        "reference product": "service",
        "location": "CH",
        "unit": "unit",
    }
    background = {**supplier, "name": "background supplier"}
    biosphere = {"name": "Synthetic emission", "categories": ("air",), "unit": "kilogram"}
    exchanges = [
        {**supplier, "type": "production", "amount": 1, "database": "old-foreground"},
        {**background, "type": "technosphere", "amount": 2},
        {**biosphere, "type": "biosphere", "amount": 3},
        {**background, "type": "technosphere", "amount": 4, "database": "custom-background"},
        {**biosphere, "type": "biosphere", "amount": 5, "database": "biosphere3"},
        {**background, "type": "technosphere", "amount": 6, "input": ("key-background", "code")},
    ]
    consumer = {**supplier, "name": "foreground consumer"}
    return InventoryDocument(
        data=[
            {**supplier, "exchanges": exchanges},
            {
                **consumer,
                "exchanges": [
                    {**consumer, "type": "production", "amount": 1},
                    {**supplier, "type": "technosphere", "amount": 7, "database": "old-foreground"},
                    {**supplier, "type": "substitution", "amount": -1},
                ],
            },
        ],
        context=InventoryContext(
            format=FormatProfile("brightway_excel"),
            background=BackgroundContext(profile, BiosphereProfile("ecoinvent", "3.10.1")),
        ),
        database_name="clic-test-package",
    )


@pytest.mark.parametrize("suffix", [".xlsx", ".csv", ".tsv"])
@pytest.mark.parametrize(
    "profile",
    [
        TechnosphereProfile("ecoinvent", "3.12", "cutoff"),
        TechnosphereProfile("ecoinvent", "3.10.1", "cutoff"),
        TechnosphereProfile("ecoinvent", "3.11", "consequential"),
        TechnosphereProfile("uvek", "2025", "cutoff"),
    ],
)
def test_brightway_database_labels_round_trip_without_mutation(tmp_path, suffix, profile):
    document = database_document(profile)
    original = deepcopy(document.data)
    writer = write_brightway_excel if suffix == ".xlsx" else write_brightway_delimited
    reader = load_brightway_excel if suffix == ".xlsx" else load_brightway_delimited

    first = reader(writer(document, tmp_path / f"inventory{suffix}"))
    second = reader(writer(first, tmp_path / f"again{suffix}"))

    assert document.data == original
    assert [exchange["database"] for activity in first.data for exchange in activity["exchanges"]] == [
        exchange["database"] for activity in second.data for exchange in activity["exchanges"]
    ]
    assert first.context.background == document.context.background
    assert [exchange["database"] for exchange in first.data[0]["exchanges"]] == [
        "clic-test-package",
        f"{profile.family}-{profile.version}-{profile.system_model}",
        "biosphere",
        "custom-background",
        "biosphere3",
        "key-background",
    ]
    assert {exchange["database"] for exchange in first.data[1]["exchanges"]} == {"clic-test-package"}
    assert [exchange["amount"] for exchange in first.data[0]["exchanges"]] == [1, 2, 3, 4, 5, 6]


def test_excel_importer_can_link_all_labelled_dependencies(tmp_path):
    document = database_document(TechnosphereProfile("ecoinvent", "3.12", "cutoff"))
    path = write_brightway_excel(document, tmp_path / "inventory.xlsx")
    importer = ExcelImporter(path)
    importer.apply_strategies()
    targets = []
    for activity in document.data:
        for exchange in activity["exchanges"]:
            if exchange["type"] == "production" or exchange["name"].startswith("foreground"):
                continue
            database = exchange.get("database") or (
                exchange["input"][0]
                if exchange.get("input")
                else "biosphere" if exchange["type"] == "biosphere" else "ecoinvent-3.12-cutoff"
            )
            targets.append({**exchange, "database": database, "code": f"target-{exchange['amount']}"})
    importer.data = link_iterable_by_fields(
        importer.data,
        other=targets,
        fields=["name", "reference product", "location", "unit", "categories", "database"],
    )

    assert not list(importer.unlinked)
    consumer_link = importer.data[1]["exchanges"][1]
    assert consumer_link["input"][0] == "clic-test-package"
