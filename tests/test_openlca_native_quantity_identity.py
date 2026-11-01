"""One actual native flow can have multiple exact canonical unit contracts."""

import json
from copy import deepcopy
from zipfile import ZipFile

import pytest

from brightpath import BackgroundContext, BiosphereProfile, FormatProfile, InventoryContext, TechnosphereProfile
from brightpath.formats.openlca_jsonld import load_openlca_jsonld, write_openlca_jsonld
from brightpath.models import InventoryDocument

FLOW = "f2d84834-d0b3-42e5-b41a-f04cc80337a4"
PROPERTY = "f6811440-ee37-11de-8a39-0800200c9a66"
UNITS = {
    "megajoule": ("52765a6c-3896-43c2-b2f4-c679acf13efe", "MJ"),
    "kilowatt hour": ("86ad2244-1f0e-4912-af53-7865283103e4", "kWh"),
}


def document(reverse=False):
    selected = InventoryContext(
        FormatProfile("openlca_jsonld"),
        BackgroundContext(TechnosphereProfile("uvek", "2026", "cutoff"), BiosphereProfile("uvek", "2026")),
    )
    activity = {
        "name": "Synthetic two-unit native heat control",
        "reference product": "synthetic output",
        "location": "GLO",
        "unit": "kilogram",
    }
    rows = [
        {
            "type": "biosphere",
            "name": "Heat, waste",
            "categories": ("emissions to air", "unspecified"),
            "unit": unit,
            "amount": amount,
            "formula": parameter,
            "uncertainty type": 3,
            "loc": amount,
            "scale": spread,
        }
        for unit, amount, parameter, spread in (
            ("megajoule", 2.0, "heat_mj", 0.1),
            ("kilowatt hour", 3.0, "heat_kwh", 0.2),
        )
    ]
    if reverse:
        rows.reverse()
    return InventoryDocument(
        data=[
            {
                **activity,
                "comment": "Synthetic foreground; native metadata only, no background vectors.",
                "parameters": [{"name": "heat_mj", "amount": 2.0}, {"name": "heat_kwh", "amount": 3.0}],
                "exchanges": [{**activity, "type": "production", "amount": 1.0}, *rows],
            }
        ],
        context=selected,
        database_name="synthetic-native-quantity-identity",
    )


def entities(path):
    with ZipFile(path) as archive:
        return {name: json.loads(archive.read(name)) for name in archive.namelist() if name.endswith(".json")}


def heat_rows(raw):
    process = next(value for name, value in raw.items() if name.startswith("processes/"))
    return [row for row in process["exchanges"] if row["flow"]["@id"] == FLOW]


@pytest.mark.parametrize("reverse", [False, True])
def test_actual_native_heat_flow_resolves_mj_and_kwh_by_exact_unit_and_property_ids(tmp_path, reverse):
    doc = document(reverse)
    before = deepcopy(doc.data)
    path = write_openlca_jsonld(doc, tmp_path / "two-unit-heat.zip")
    raw = entities(path)
    rows = heat_rows(raw)
    assert len(rows) == 2
    assert {row["unit"]["@id"] for row in rows} == {value[0] for value in UNITS.values()}
    assert all(row["flowProperty"]["@id"] == PROPERTY for row in rows)
    assert f"flows/{FLOW}.json" not in raw
    assert sorted(row["amount"] for row in rows) == [2.0, 3.0]
    restored = load_openlca_jsonld(path, context=doc.context).data[0]
    actual = {row["unit"]: row for row in restored["exchanges"] if row["type"] == "biosphere"}
    for row in before[0]["exchanges"][1:]:
        found = actual[row["unit"]]
        for field in ("name", "unit", "categories", "amount", "formula", "uncertainty type", "loc", "scale"):
            assert found[field] == row[field]
    assert doc.data == before


@pytest.mark.parametrize("mutation", ["unknown_unit", "wrong_unit", "wrong_property", "no_unit_id", "no_property_id"])
def test_ambiguous_native_flow_is_not_selected_by_names_or_missing_quantity_ids(tmp_path, mutation):
    doc = document()
    path = write_openlca_jsonld(doc, tmp_path / "changed-quantity.zip")
    raw = entities(path)
    row = next(row for row in heat_rows(raw) if row["unit"]["name"] == "kWh")
    if mutation == "unknown_unit":
        row["unit"]["@id"] = "00000000-0000-4000-8000-000000000001"
    elif mutation == "wrong_unit":
        row["unit"]["@id"] = UNITS["megajoule"][0]  # Correct native UUID, conflicting kWh label.
    elif mutation == "wrong_property":
        row["flowProperty"]["@id"] = "93a60a56-a3c8-11da-a746-0800200b9a66"  # Mass, not Energy.
    elif mutation == "no_unit_id":
        row["unit"].pop("@id")
    else:
        row["flowProperty"].pop("@id")
    with ZipFile(path, "w") as archive:
        for name, value in raw.items():
            archive.writestr(name, json.dumps(value))
    with pytest.raises(ValueError, match="Unknown or ambiguous|unit.*conflicts|quantity.*conflicts"):
        load_openlca_jsonld(path, context=doc.context)
