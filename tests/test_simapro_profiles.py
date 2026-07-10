import pytest

from brightpath import BackgroundProfile
from brightpath.formats.simapro_csv import format_biosphere_exchange
from brightpath.profiles import format_simapro_technosphere_name, parse_simapro_technosphere_name
from brightpath.utils import load_simapro_brightway_biosphere_mapping


def ecoinvent_profile(version="3.9", system_model="cutoff"):
    return BackgroundProfile("ecoinvent", version, system_model)


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        (
            "Electricity {CH}| market for | Cut-off, U",
            ("market for electricity", "electricity", "CH"),
        ),
        (
            "electricity {WECC, US only}| market for | Cut-off, U",
            ("market for electricity", "electricity", "US-WECC"),
        ),
        (
            "Aluminium, primary| production | Cut-off, U",
            ("aluminium production, primary", "aluminium, primary", "GLO"),
        ),
        (
            "waste plastic {GLO}| treatment of | Cut-off, U",
            ("treatment of waste plastic", "waste plastic", "GLO"),
        ),
        (
            "Copper {French Guiana}| production | Cut-off, U",
            ("copper production", "copper", "FG"),
        ),
    ],
)
def test_parse_ecoinvent_technosphere_names(text, expected):
    assert parse_simapro_technosphere_name(text, profile=ecoinvent_profile()) == expected


def test_parse_ecoinvent_name_preserves_semantic_case_from_writer():
    assert parse_simapro_technosphere_name(
        "Test product {GLO}| Test process | Cut-off, U",
        profile=ecoinvent_profile(),
    ) == ("test process", "test product", "GLO")
    assert parse_simapro_technosphere_name(
        "PV module {GLO}| PV module production | Cut-off, U",
        profile=ecoinvent_profile(),
    ) == ("PV module production", "PV module", "GLO")


def test_parse_ecoinvent_name_rejects_malformed_values():
    with pytest.raises(ValueError, match="empty"):
        parse_simapro_technosphere_name("", profile=ecoinvent_profile())
    with pytest.raises(ValueError, match="too many fields"):
        parse_simapro_technosphere_name("a|b|c|d|e", profile=ecoinvent_profile())
    with pytest.raises(ValueError, match="malformed.*braces"):
        parse_simapro_technosphere_name(
            "Electricity {CH| market for | Cut-off, U",
            profile=ecoinvent_profile(),
        )
    with pytest.raises(ValueError, match="empty location"):
        parse_simapro_technosphere_name(
            "Electricity {}| market for | Cut-off, U",
            profile=ecoinvent_profile(),
        )


def test_parse_ecoinvent_name_normalizes_ei310_location():
    assert parse_simapro_technosphere_name(
        "Hard coal {Europe, without Russia and Turkey}| market for | Cut-off, U",
        profile=ecoinvent_profile("3.10"),
    ) == (
        "market for hard coal",
        "hard coal",
        "Europe, without Russia and T\u00fcrkiye",
    )


def test_format_ecoinvent_names_supports_system_models_and_market_exception():
    assert (
        format_simapro_technosphere_name(
            name="market for electricity",
            reference_product="electricity",
            location="CH",
            unit="kilowatt hour",
            profile=ecoinvent_profile(),
        )
        == "Electricity {CH}| market for electricity | Cut-off, U"
    )
    assert (
        format_simapro_technosphere_name(
            name="market for ethylene vinyl acetate copolymer",
            reference_product="ethylene vinyl acetate copolymer",
            location="GLO",
            unit="kilogram",
            profile=ecoinvent_profile(),
        )
        == "Ethylene vinyl acetate copolymer {GLO}| market for | Cut-off, U"
    )
    assert format_simapro_technosphere_name(
        name="electricity production",
        reference_product="electricity",
        location="CH",
        unit="kilowatt hour",
        profile=ecoinvent_profile(system_model="consequential"),
    ).endswith("| Consequential, U")


def test_uvek_names_use_mapping_and_have_catalog_free_fallback(monkeypatch):
    profile = BackgroundProfile("uvek", "2025", "cutoff")
    mapped = format_simapro_technosphere_name(
        name="Bentonite, at processing",
        reference_product="Bentonite, at processing",
        location="DE",
        unit="kilogram",
        profile=profile,
    )
    assert mapped == "Bentonite, at processing/DE U"
    assert parse_simapro_technosphere_name(mapped, unit="kilogram", profile=profile) == (
        "Bentonite, at processing",
        "Bentonite, at processing",
        "DE",
    )

    from brightpath.profiles import simapro as simapro_profile

    def missing_catalog(_profile):
        raise FileNotFoundError

    monkeypatch.setattr(simapro_profile, "load_background_catalog", missing_catalog)
    assert parse_simapro_technosphere_name("foreground service/GLO U", profile=profile) == (
        "foreground service",
        "foreground service",
        "GLO",
    )


def test_technosphere_names_reject_unknown_background_family():
    profile = BackgroundProfile("unknown", "1", "cutoff")
    with pytest.raises(ValueError, match="Unsupported background family"):
        parse_simapro_technosphere_name("activity/GLO U", profile=profile)
    with pytest.raises(ValueError, match="Unsupported background family"):
        format_simapro_technosphere_name(
            name="activity",
            reference_product="product",
            location="GLO",
            unit="kilogram",
            profile=profile,
        )


def test_format_biosphere_exchange_normalizes_water_and_in_ground_flows():
    water = {
        "name": "Water, lake",
        "categories": ("natural resource", "unspecified"),
    }
    result = format_biosphere_exchange(
        water,
        "3.9",
        [("Water, lake", "natural resource", "in water")],
        {},
    )
    assert result["categories"] == ("natural resource", "in water")

    cadmium = {
        "name": "Cadmium, in ground",
        "categories": ("natural resource", "unspecified"),
    }
    result = format_biosphere_exchange(
        cadmium,
        "3.9",
        [("Cadmium", "natural resource", "in ground")],
        {},
    )
    assert result["name"] == "Cadmium"
    assert result["categories"] == ("natural resource", "in ground")


def test_format_biosphere_exchange_is_copy_on_write_and_validates_categories():
    exchange = {
        "name": "Water, lake",
        "categories": ("natural resource", "unspecified"),
    }
    result = format_biosphere_exchange(
        exchange,
        "3.9",
        [("Water, lake", "natural resource", "in water")],
        {},
    )

    assert result is not exchange
    assert result["categories"] == ("natural resource", "in water")
    assert exchange["categories"] == ("natural resource", "unspecified")
    with pytest.raises(ValueError, match="missing categories"):
        format_biosphere_exchange({"name": "Flow"}, "3.9", [], {})
    with pytest.raises(ValueError, match="unsupported category"):
        format_biosphere_exchange({"name": "Flow", "categories": ("ocean",)}, "3.9", [], {})


def test_format_biosphere_exchange_applies_correspondence_mapping():
    result = format_biosphere_exchange(
        {
            "name": "Old flow",
            "categories": ("air", "urban air close to ground"),
        },
        "3.9",
        [("New flow", "air", "urban air close to ground")],
        {"air": {"Old flow": "New flow"}},
    )
    assert result["name"] == "New flow"


@pytest.mark.parametrize(
    ("name", "categories", "expected_name", "expected_categories"),
    [
        ("Mercury (II)", ("air", "urban air close to ground"), "Mercury II", ("air", "urban air close to ground")),
        (
            "BOD5 (Biological Oxygen Demand)",
            ("water", "surface water"),
            "BOD5, Biological Oxygen Demand",
            ("water", "surface water"),
        ),
        ("Water/m3, RER", ("air",), "Water", ("air",)),
        ("Argon-40/kg", ("natural resource", "in air"), "Argon", ("natural resource", "in air")),
        ("Strontium (II)", ("water", "surface water"), "Strontium II", ("water", "surface water")),
        ("AOX, Adsorbable Organic Halogen", ("water",), "AOX, Adsorbable Organic Halides", ("water",)),
        ("Ammonium ion", ("water", "surface water"), "Ammonium", ("water", "surface water")),
        ("Chromium, ion", ("air",), "Chromium VI", ("air",)),
        ("Benzene, hexachloro-", ("air",), "Hexachlorobenzene", ("air",)),
    ],
)
def test_format_biosphere_exchange_applies_ei310_name_normalizers(
    name,
    categories,
    expected_name,
    expected_categories,
):
    result = format_biosphere_exchange(
        {"name": name, "categories": categories},
        "3.10",
        [],
        {"water": {"Strontium (II)": "Strontium"}},
        version_mapping=load_simapro_brightway_biosphere_mapping("3.10"),
    )

    assert result["name"] == expected_name
    assert result["categories"] == expected_categories
