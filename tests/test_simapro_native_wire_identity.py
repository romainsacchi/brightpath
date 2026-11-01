"""Native forward wire identity takes precedence over stale reverse aliases."""

import pytest

from brightpath import BackgroundProfile
from brightpath.background import PackageCatalogProvider
from brightpath.core import TechnosphereProfile
from brightpath.profiles import simapro


@pytest.mark.parametrize("version", ["2025", "2026"])
@pytest.mark.parametrize("unit", ["kg", "kilogram"])
def test_actual_chromium_material_wire_does_not_select_rolling_service(version, unit):
    native = PackageCatalogProvider().load_technosphere(TechnosphereProfile("uvek", version, "cutoff"))
    before = native.identities
    name = "Chromium steel 18/8, at plant"
    profile = BackgroundProfile("uvek", version, "cutoff")
    wire = simapro.format_simapro_technosphere_name(
        name=name, reference_product=name, location="RER", unit=unit, profile=profile
    )
    assert wire == "Chromium steel 18/8, at plant/RER U"
    assert ("Sheet rolling, chromium steel", "Sheet rolling, chromium steel", "RER", "kilogram") in native.identities
    assert simapro.parse_simapro_technosphere_name(
        wire, unit=unit, profile=profile, technosphere_identities=native.identities
    ) == (name, name, "RER")
    assert native.identities == before


def test_two_actual_forward_emitters_remain_ambiguous(monkeypatch):
    identities = frozenset({("Wire", "Product A", "GLO", "kilogram"), ("Wire", "Product B", "GLO", "kilogram")})
    monkeypatch.setattr(simapro, "_uvek_simapro_mappings", lambda: ({}, {}))
    with pytest.raises(ValueError, match="Ambiguous UVEK reference product"):
        simapro.parse_simapro_technosphere_name(
            "Wire/GLO U",
            unit="kg",
            profile=BackgroundProfile("uvek", "2025", "cutoff"),
            technosphere_identities=identities,
        )


@pytest.mark.parametrize("unit", ["", " ", None, "megajoule"])
def test_native_wire_requires_its_declared_unit(unit):
    identities = frozenset({("Wire", "Product", "GLO", "kilogram")})
    with pytest.raises(ValueError, match="unit"):
        simapro.parse_simapro_technosphere_name(
            "Wire/GLO U",
            unit=unit,
            profile=BackgroundProfile("uvek", "2025", "cutoff"),
            technosphere_identities=identities,
        )


def test_immutable_catalog_index_reused_without_rescanning(monkeypatch):
    class CountedFrozenSet(frozenset):
        iterations = 0

        def __iter__(self):
            self.iterations += 1
            return super().__iter__()

    identities = CountedFrozenSet({("Wire", "Product", "GLO", "kilogram")})
    forward = {}
    monkeypatch.setattr(simapro, "_uvek_simapro_mappings", lambda: (forward, {}))
    for _ in range(5):
        assert simapro.parse_simapro_technosphere_name(
            "Wire/GLO U",
            unit="kg",
            profile=BackgroundProfile("uvek", "2025", "cutoff"),
            technosphere_identities=identities,
        ) == ("Wire", "Product", "GLO")
    assert identities.iterations == 1


def test_mutable_injected_catalog_is_not_served_a_stale_index(monkeypatch):
    identities = {("Wire", "Product A", "GLO", "kilogram")}
    forward = {}
    monkeypatch.setattr(simapro, "_uvek_simapro_mappings", lambda: (forward, {}))
    kwargs = {
        "unit": "kg",
        "profile": BackgroundProfile("uvek", "2025", "cutoff"),
        "technosphere_identities": identities,
    }
    assert simapro.parse_simapro_technosphere_name("Wire/GLO U", **kwargs) == ("Wire", "Product A", "GLO")
    identities.add(("Wire", "Product B", "GLO", "kilogram"))
    with pytest.raises(ValueError, match="Ambiguous UVEK"):
        simapro.parse_simapro_technosphere_name("Wire/GLO U", **kwargs)


def test_legacy_alias_with_wrong_unit_is_not_invented_as_another_identity(monkeypatch):
    identities = frozenset({("Native name", "Product", "GLO", "kilogram")})
    forward = {("Native name", "GLO", "kilogram", "Product"): "Native name/GLO U"}
    reverse = {"Old alias/GLO U": (("Native name", "GLO", "kilogram", "Product"),)}
    monkeypatch.setattr(simapro, "_uvek_simapro_mappings", lambda: (forward, reverse))
    with pytest.raises(ValueError, match="unit match"):
        simapro.parse_simapro_technosphere_name(
            "Old alias/GLO U",
            unit="megajoule",
            profile=BackgroundProfile("uvek", "2025", "cutoff"),
            technosphere_identities=identities,
        )
