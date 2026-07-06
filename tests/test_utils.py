import copy
import csv
import math

import pytest
from voluptuous import MultipleInvalid

from brightpath import utils


def activity_with_exchanges(*exchanges):
    return {
        "name": "activity",
        "reference product": "product",
        "location": "GLO",
        "unit": "kilogram",
        "exchanges": list(exchanges),
    }


def production_exchange(**overrides):
    exchange = {
        "type": "production",
        "name": "activity",
        "reference product": "product",
        "location": "GLO",
        "unit": "kilogram",
        "amount": 1.0,
        "simapro category": "Materials/Test",
    }
    exchange.update(overrides)
    return exchange


def technosphere_exchange(**overrides):
    exchange = {
        "type": "technosphere",
        "name": "market for product",
        "reference product": "product",
        "location": "GLO",
        "unit": "kilogram",
        "amount": 2.0,
    }
    exchange.update(overrides)
    return exchange


def biosphere_exchange(**overrides):
    exchange = {
        "type": "biosphere",
        "name": "Water",
        "categories": ("air", "urban air close to ground"),
        "unit": "cubic meter",
        "amount": 3.0,
    }
    exchange.update(overrides)
    return exchange


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("=SUM(A1:A2)", "'=SUM(A1:A2)"),
        ("+SUM(A1:A2)", "'+SUM(A1:A2)"),
        ("@SUM(A1:A2)", "'@SUM(A1:A2)"),
        ("-CMD()", "'-CMD()"),
        (" -CMD()", "' -CMD()"),
        ("-1.23", "-1.23"),
        ("plain text", "plain text"),
        (1.25, 1.25),
    ],
)
def test_escape_spreadsheet_formula(value, expected):
    assert utils.escape_spreadsheet_formula(value) == expected


def test_search_for_forbidden_units_returns_updated_copy():
    row = ["transport", "min", "kilogram"]

    updated = utils.search_for_forbidden_units(row)

    assert updated == ["transport", "minute", "kilogram"]
    assert row == ["transport", "min", "kilogram"]


def test_check_simapro_inventory_returns_source_when_no_changes(tmp_path):
    source = tmp_path / "inventory.csv"
    source.write_text("Name;Unit\nTransport;kilogram\n", encoding="latin-1")

    result = utils.check_simapro_inventory(source)

    assert result == source


def test_check_simapro_inventory_writes_explicit_output(tmp_path):
    source = tmp_path / "inventory.csv"
    output = tmp_path / "cleaned" / "inventory.csv"
    source.write_text("Name;Unit\nTransport;min\n", encoding="latin-1")

    result = utils.check_simapro_inventory(source, output_path=output)

    assert result == output
    with open(output, encoding="latin-1", newline="") as handle:
        rows = list(csv.reader(handle, delimiter=";"))
    assert rows == [["Name", "Unit"], ["Transport", "minute"]]


def test_load_inventory_metadata_validates_schema_and_suffix(tmp_path):
    metadata = tmp_path / "metadata.yaml"
    metadata.write_text(
        "\n".join(
            [
                "system description:",
                "  name: system",
                "literature reference:",
                "  name: reference",
                "  documentation link: https://example.com/doc",
            ]
        ),
        encoding="utf-8",
    )

    assert utils.load_inventory_metadata(metadata)["system description"]["name"] == "system"

    txt = tmp_path / "metadata.txt"
    txt.write_text("system description: {}", encoding="utf-8")
    with pytest.raises(ValueError, match="\\.yaml"):
        utils.load_inventory_metadata(txt)


def test_load_inventory_metadata_rejects_missing_required_sections(tmp_path):
    metadata = tmp_path / "metadata.yaml"
    metadata.write_text("system description:\n  name: system\n", encoding="utf-8")

    with pytest.raises(MultipleInvalid):
        utils.load_inventory_metadata(metadata)


def test_check_inventories_accepts_valid_inventory():
    data = [
        activity_with_exchanges(
            production_exchange(),
            technosphere_exchange(),
            biosphere_exchange(),
        )
    ]

    utils.check_inventories(data)


def test_check_inventories_rejects_missing_exchange_fields():
    data = [activity_with_exchanges(technosphere_exchange(**{"reference product": None}))]
    del data[0]["exchanges"][0]["reference product"]

    with pytest.raises(ValueError, match="missing required exchange field"):
        utils.check_inventories(data)


def test_validate_brightway_inventory_rejects_structural_errors():
    with pytest.raises(ValueError, match="must be a list"):
        utils.validate_brightway_inventory({"not": "a list"})

    data = [
        activity_with_exchanges(
            production_exchange(),
            {
                "type": "substitution",
                "name": "substitution",
                "reference product": "product",
                "location": "GLO",
                "unit": "kilogram",
                "amount": 1.0,
            },
        )
    ]

    with pytest.raises(ValueError, match="unsupported exchange type"):
        utils.validate_brightway_inventory(data)


def test_validate_brightway_inventory_rejects_bad_amount_and_categories():
    data = [
        activity_with_exchanges(
            production_exchange(amount="1"),
            biosphere_exchange(categories=("ocean",), amount=1.0),
        )
    ]

    with pytest.raises(ValueError, match="amount.*number"):
        utils.validate_brightway_inventory(data)


def test_inspect_brightway_inventory_can_relax_simapro_category_requirement():
    data = [activity_with_exchanges(production_exchange(**{"simapro category": ""}))]

    errors, warnings = utils.inspect_brightway_inventory(
        data,
        require_simapro_category=False,
    )

    assert errors == []
    assert warnings == []

    utils.validate_brightway_inventory(
        data,
        require_simapro_category=False,
    )


def test_inspect_brightway_inventory_accepts_biosphere_kilo_becquerel_unit():
    data = [
        activity_with_exchanges(
            production_exchange(),
            biosphere_exchange(
                name="Hydrogen-3, Tritium",
                categories=("air", "non-urban air or from high stacks"),
                unit="kilo Becquerel",
            ),
        )
    ]

    errors, warnings = utils.inspect_brightway_inventory(
        data,
        require_simapro_category=False,
    )

    assert errors == []
    assert warnings == []


def test_find_production_exchange_returns_exchange_or_raises():
    production = production_exchange()
    assert utils.find_production_exchange(activity_with_exchanges(production)) is production

    with pytest.raises(ValueError, match="production exchange"):
        utils.find_production_exchange({"name": "activity", "exchanges": []})


def test_exchange_filters_skip_zero_amounts_and_validate_categories():
    tech = technosphere_exchange(amount=2.0)
    zero_tech = technosphere_exchange(amount=0.0)
    air = biosphere_exchange(categories=("air", "urban air close to ground"), amount=3.0)
    water = biosphere_exchange(categories=("water", "surface water"), amount=4.0)
    activity = activity_with_exchanges(tech, zero_tech, air, water)

    assert utils.get_technosphere_exchanges(activity) == [tech]
    assert utils.get_biosphere_exchanges(activity, "air") == [air]

    with pytest.raises(ValueError, match="missing categories"):
        utils.get_biosphere_exchanges(activity_with_exchanges(biosphere_exchange(categories=())), "air")


def test_format_exchange_name_for_ecoinvent_and_uvek():
    assert (
        utils.format_exchange_name("market for electricity", "electricity", "CH", "kilowatt hour", "ecoinvent")
        == "Electricity {CH}| market for electricity | Cut-off, U"
    )
    assert (
        utils.format_exchange_name("custom process", "custom product", "GLO", "kilogram", "ecoinvent")
        == "Custom product {GLO}| Custom process | Cut-off, U"
    )
    assert utils.format_exchange_name("custom process", "product", "CH", "kilogram", "uvek") == "custom process/CH U"

    with pytest.raises(ValueError, match="non-empty"):
        utils.format_exchange_name("", "product", "GLO", "kilogram", "ecoinvent")


def test_uncertainty_helpers_cover_known_and_unknown_types(caplog):
    assert utils.get_simapro_uncertainty_type(2) == "Lognormal"
    assert utils.get_simapro_uncertainty_type(999) == "not defined"
    assert math.isclose(utils.convert_sd_to_sd2(0.5, "Lognormal"), math.exp(0.5) ** 2)
    assert utils.convert_sd_to_sd2(0.5, "Normal") == 0.25
    assert utils.convert_sd_to_sd2(0.5, "not defined") == 0

    assert utils.convert_sd_to_sd2(0.5, "Triangular") == 0
    assert "No SimaPro uncertainty scale conversion" in caplog.text


def test_string_and_category_helpers():
    assert utils.round_floats_in_string("values 1.234 and -2.345") == "values 1.23 and -2.35"
    assert utils.get_subcategory("Materials/Metals/Aluminium") == "Metals\\Aluminium"
    assert utils.get_subcategory("Materials") == ""
    assert utils.lower_cap_first_letter("NASA component") == "NASA component"
    assert utils.lower_cap_first_letter("Component") == "component"


def test_flag_exchanges_marks_all_exchanges_unused():
    activity = activity_with_exchanges(production_exchange(used=True), technosphere_exchange(used=True))

    returned = utils.flag_exchanges(activity)

    assert returned is activity
    assert [exchange["used"] for exchange in activity["exchanges"]] == [False, False]


def test_blacklist_and_waste_detection():
    assert utils.is_blacklisted({"name": "Oxygen"}, "ecoinvent") is True
    assert utils.is_blacklisted({"name": "not blacklisted"}, "ecoinvent") is False
    assert utils.is_a_waste_treatment("treatment of municipal waste", "ecoinvent") is True
    assert utils.is_a_waste_treatment("treatment of aluminium scrap", "ecoinvent") is False
    assert utils.is_activity_waste_treatment({"name": "anything", "type": "process"}, "ecoinvent") is False
    assert utils.is_activity_waste_treatment({"name": "anything", "type": "waste treatment"}, "ecoinvent") is True


def test_check_exchanges_for_conversion_applies_uvek_factor():
    exchange = technosphere_exchange(
        name="market for heat, from steam, in chemical industry",
        amount=10.0,
        unit="megajoule",
    )

    result = utils.check_exchanges_for_conversion([exchange], "uvek")

    assert result[0]["amount"] == pytest.approx(3.6)
    assert result[0]["unit"] == "kilogram"
    assert exchange["amount"] == 10.0
    assert exchange["unit"] == "megajoule"


def test_add_distri_transport_returns_copy_with_added_transport(monkeypatch):
    activity = activity_with_exchanges(
        production_exchange(),
        technosphere_exchange(name="shipped product", amount=1000.0, unit="kilogram"),
    )

    monkeypatch.setattr(utils, "fetch_transport_distance", lambda name, location: (10.0, 20.0, 0.0))
    result = utils.add_distri_transport(activity)

    assert result is not activity
    assert len(result["exchanges"]) == len(activity["exchanges"]) + 2
    assert len(activity["exchanges"]) == 2
    assert [exchange["amount"] for exchange in result["exchanges"][-2:]] == [10.0, 20.0]


def test_transport_distance_unknown_item_returns_zeros():
    assert utils.fetch_transport_distance("not in table", "CH") == (0.0, 0.0, 0.0)


def test_ensure_unique_datasets_uses_name_product_location():
    datasets = [
        {"name": "same", "reference product": "product", "location": "CH"},
        {"name": "same", "reference product": "product", "location": "DE"},
    ]

    assert utils.ensure_unique_datasets(datasets) == datasets

    duplicate = copy.deepcopy(datasets[0])
    with pytest.raises(ValueError, match="Duplicate datasets"):
        utils.ensure_unique_datasets(datasets + [duplicate])
