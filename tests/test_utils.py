import csv
import math

import pytest

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


def test_inspect_brightway_inventory_can_relax_simapro_category_requirement():
    data = [activity_with_exchanges(production_exchange(**{"simapro category": ""}))]

    errors, warnings = utils.inspect_brightway_inventory(
        data,
        require_simapro_category=False,
    )

    assert errors == []
    assert warnings == []


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


def test_inspect_brightway_inventory_accepts_technosphere_hectare_alias_unit():
    data = [
        activity_with_exchanges(
            production_exchange(),
            technosphere_exchange(
                name="market for sowing",
                **{"reference product": "sowing"},
                unit="ha",
            ),
        )
    ]

    errors, warnings = utils.inspect_brightway_inventory(
        data,
        require_simapro_category=False,
    )

    assert errors == []
    assert warnings == []


def test_inspect_brightway_inventory_accepts_person_kilometer_unit_alias():
    data = [
        activity_with_exchanges(
            production_exchange(),
            technosphere_exchange(
                name="transport, tram",
                **{"reference product": "transport, tram"},
                unit="person kilometer",
            ),
        )
    ]

    errors, warnings = utils.inspect_brightway_inventory(
        data,
        require_simapro_category=False,
    )

    assert errors == []
    assert warnings == []


def test_inspect_brightway_inventory_warns_on_water_resource_intake_without_release():
    data = [
        activity_with_exchanges(
            production_exchange(),
            biosphere_exchange(
                name="Water, river",
                categories=("natural resource", "in water"),
                unit="cubic meter",
                amount=2.0,
            ),
        )
    ]

    errors, warnings = utils.inspect_brightway_inventory(
        data,
        require_simapro_category=False,
    )

    assert errors == []
    assert len(warnings) == 1
    assert "no water release flows were found" in warnings[0]


def test_inspect_brightway_inventory_warns_on_incomplete_land_transformation_pair():
    data = [
        activity_with_exchanges(
            production_exchange(),
            biosphere_exchange(
                name="Transformation, from forest, extensive",
                categories=("natural resource", "land"),
                unit="square meter",
                amount=10.0,
            ),
        )
    ]

    errors, warnings = utils.inspect_brightway_inventory(
        data,
        require_simapro_category=False,
    )

    assert errors == []
    assert len(warnings) == 1
    assert "'transformation to' exchange" in warnings[0]


def test_inspect_brightway_inventory_warns_on_combustion_fuel_co2_mismatch():
    activity = activity_with_exchanges(
        production_exchange(),
        technosphere_exchange(
            name="market for natural gas, high pressure",
            reference_product="natural gas, high pressure",
            location="CH",
            unit="megajoule",
            amount=10.0,
        ),
    )
    activity["name"] = "heat production, natural gas boiler"
    data = [activity]

    errors, warnings = utils.inspect_brightway_inventory(
        data,
        require_simapro_category=False,
    )

    assert errors == []
    assert len(warnings) == 1
    assert "detected fossil fuel inputs suggest about" in warnings[0]


def test_inspect_brightway_inventory_skips_combustion_warning_when_co2_is_consistent():
    activity = activity_with_exchanges(
        production_exchange(),
        technosphere_exchange(
            name="market for natural gas, high pressure",
            reference_product="natural gas, high pressure",
            location="CH",
            unit="megajoule",
            amount=10.0,
        ),
        biosphere_exchange(
            name="Carbon dioxide, fossil",
            categories=("air",),
            unit="kilogram",
            amount=0.56,
        ),
    )
    activity["name"] = "heat production, natural gas boiler"
    data = [activity]

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


def test_uncertainty_scale_conversion(caplog):
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
