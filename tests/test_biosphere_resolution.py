from brightpath.bwconverter import BrightwayConverter


def make_converter():
    """Create a converter with controllable biosphere mapping."""

    converter = BrightwayConverter.__new__(BrightwayConverter)
    converter.simapro_biosphere = {}
    return converter


def test_resolve_returns_original_name_when_missing():
    converter = make_converter()
    exchange = {"name": "Water, river", "location": "CH"}

    assert (
        converter._resolve_biosphere_flow_name(exchange, activity_location="GLO")
        == "Water, river"
    )


def test_resolve_uses_direct_string_mapping():
    converter = make_converter()
    converter.simapro_biosphere = {"Water": "Water, resource"}
    exchange = {"name": "Water", "location": "CH"}

    assert (
        converter._resolve_biosphere_flow_name(exchange, activity_location=None)
        == "Water, resource"
    )


def test_resolve_prefers_exact_exchange_location():
    converter = make_converter()
    converter.simapro_biosphere = {
        "Water": {"CH": "Water, CH", "GLO": "Water, GLO"}
    }
    exchange = {"name": "Water", "location": "CH"}

    assert (
        converter._resolve_biosphere_flow_name(exchange, activity_location="DE")
        == "Water, CH"
    )


def test_resolve_supports_hierarchical_locations():
    converter = make_converter()
    converter.simapro_biosphere = {
        "Water": {"CH": "Water, CH", "GLO": "Water, GLO"}
    }
    exchange = {"name": "Water", "location": "CH-01"}

    assert (
        converter._resolve_biosphere_flow_name(exchange, activity_location=None)
        == "Water, CH"
    )


def test_resolve_falls_back_to_activity_location():
    converter = make_converter()
    converter.simapro_biosphere = {
        "Water": {"BR": "Water, BR", "GLO": "Water, GLO"}
    }
    exchange = {"name": "Water"}

    assert (
        converter._resolve_biosphere_flow_name(exchange, activity_location="BR")
        == "Water, BR"
    )


def test_resolve_defaults_to_global_and_none():
    converter = make_converter()
    converter.simapro_biosphere = {
        "Water": {"GLO": "Water, GLO", None: "Water"}
    }
    exchange = {"name": "Water"}

    assert (
        converter._resolve_biosphere_flow_name(exchange, activity_location=None)
        == "Water, GLO"
    )

    converter.simapro_biosphere["Water"].pop("GLO")

    assert (
        converter._resolve_biosphere_flow_name(exchange, activity_location=None)
        == "Water"
    )
