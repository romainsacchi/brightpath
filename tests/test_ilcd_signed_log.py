"""Signed native log-normal sampling without nominal/formula recentering."""

import math
import zipfile

import pytest
from lxml import etree
from test_ilcd import FLOW, context, dataset, document, electricity_document, profile
from test_ilcd_independent_centres import process_exchange

from brightpath.background import InMemoryCatalogProvider
from brightpath.background.catalogs import BiosphereCatalog
from brightpath.formats.ilcd import (
    _write_uncertainty,
    load_ilcd,
    render_ilcd_package,
    validate_ilcd_package,
    write_ilcd,
)
from brightpath.formats.ilcd_package import NS, ILCDError, parse_xml, q, xml_bytes


def signed_electricity(scale):
    source, provider = electricity_document()
    data = source.data
    data[0]["exchanges"][1].update(
        {
            "amount": -2,
            "formula": "-(dose * 2)",
            "uncertainty type": 2,
            "loc": math.log(3),
            "scale": scale,
            "negative": True,
        }
    )
    return source.replace(data=data), provider


@pytest.mark.parametrize("scale", [math.log(1.5), 0.2])
def test_signed_log_native_quantity_formula_and_actual_rounded_spread(tmp_path, scale):
    source, provider = signed_electricity(scale)
    before = source.data
    warnings = []
    path = write_ilcd(
        source,
        tmp_path / "signed.zip",
        metadata_profile=profile(),
        dialect="openlca",
        catalog_provider=provider,
        uncertainty_relative_tolerance=0.005,
        uncertainty_rounding_warnings=warnings,
    )
    assert not validate_ilcd_package(path)
    field = round(math.exp(scale), 3)
    sigma = math.log(field)
    element = process_exchange(path, 1)
    assert float(element.get(q("olca", "amount"))) == pytest.approx(-math.exp(math.log(3 * 3.6) + 0.5 * sigma**2))
    assert float(element.findtext("p:resultingAmount", namespaces=NS)) == -7.2
    assert element.get(q("olca", "unitId")) is None
    assert element.get(q("olca", "propertyId")) is None
    assert element.get(q("olca", "defaultProvider"))
    result = load_ilcd(path, context=context()).data[0]["exchanges"][1]
    assert result["amount"] == -2
    assert result["loc"] == pytest.approx(math.log(3), abs=1e-14)
    assert result["scale"] == sigma
    assert result["negative"] is True
    assert result["formula"] == "-(dose * 2)"
    assert result["unit"] == "kilowatt hour"
    assert bool(warnings) == (scale == 0.2)
    if warnings:
        assert warnings[0]["original_amount"] == -2
        assert warnings[0]["original_uncertainty"]["negative"] is True
    repeated = write_ilcd(load_ilcd(path, context=context()), tmp_path / "again.zip")
    assert load_ilcd(repeated, context=context()).data[0]["exchanges"][1]["negative"] is True
    assert source.data == before


@pytest.mark.parametrize("scale", [math.log(1.5), 0.2])
@pytest.mark.parametrize("include_negative", [False, True])
def test_signed_log_foreground_provider_identity_and_formula(tmp_path, scale, include_negative):
    supplier = dataset("supplier", "Supplier", "Supplier product")
    consumer = dataset("consumer", "Consumer", "Consumer product", 1)
    consumer["parameters"] = [{"name": "dose", "amount": 2}]
    consumer["exchanges"].append(
        {
            **{key: supplier[key] for key in ("name", "reference product", "location", "unit")},
            "type": "technosphere",
            "amount": -2,
            "formula": "-dose",
            "uncertainty type": 2,
            "loc": math.log(3),
            "scale": scale,
            "negative": True,
        }
    )
    if not include_negative:
        consumer["exchanges"][1].pop("negative")
    source = document(supplier, consumer)
    before = source.data
    path = write_ilcd(
        source,
        tmp_path / "linked.zip",
        metadata_profile=profile(),
        dialect="openlca",
        uncertainty_relative_tolerance=0.005,
    )
    assert not validate_ilcd_package(path)
    restored = {row["name"]: row for row in load_ilcd(path, context=context()).data}
    result = restored["Consumer"]["exchanges"][1]
    assert tuple(result[key] for key in ("name", "reference product", "location", "unit")) == tuple(
        supplier[key] for key in ("name", "reference product", "location", "unit")
    )
    assert result["amount"] == -2
    assert result["formula"] == "-dose"
    assert result["negative"] is True
    assert result["loc"] == pytest.approx(math.log(3), abs=1e-14)
    assert result["scale"] == math.log(round(math.exp(scale), 3))
    assert restored["Supplier"]["exchanges"][0]["amount"] == 100
    assert source.data == before


@pytest.mark.parametrize("scale", [0.0099, 0.0001, 1e-14])
@pytest.mark.parametrize("tolerance", [0.0, 0.005])
def test_signed_log_precision_and_positive_to_zero_guards_remain(scale, tolerance):
    source, provider = signed_electricity(scale)
    with pytest.raises(ILCDError) as error:
        render_ilcd_package(
            source,
            metadata_profile=profile(),
            dialect="openlca",
            catalog_provider=provider,
            uncertainty_relative_tolerance=tolerance,
        )
    assert error.value.code == "ilcd.uncertainty_precision_unsupported"


@pytest.mark.parametrize("amount,negative", [(-2, True), (2, True), (-2, False), (0, True), (0, False)])
def test_core_signed_and_inconsistent_native_sign_guards(amount, negative):
    exchange = {
        "amount": amount,
        "uncertainty type": 2,
        "loc": math.log(3),
        "scale": math.log(1.5),
        "negative": negative,
    }
    with pytest.raises(ILCDError) as error:
        _write_uncertainty(etree.Element(q("p", "exchange")), exchange, dialect="core")
    assert error.value.code == "ilcd.uncertainty_unsupported"
    if amount != -2 or not negative:
        with pytest.raises(ILCDError) as error:
            _write_uncertainty(etree.Element(q("p", "exchange")), exchange, dialect="openlca")
        assert error.value.code == "ilcd.uncertainty_unsupported"


@pytest.mark.parametrize("kind", [2, 3])
@pytest.mark.parametrize("reverse_direction", [False, True])
def test_partial_amount_reader_reflects_biosphere_uncertainty_with_direction(tmp_path, kind, reverse_direction):
    record = dataset()
    key = ("Synthetic emission", ("air",), "kilogram")
    record["parameters"] = [{"name": "dose", "amount": -2}]
    record["exchanges"].append(
        {
            "name": key[0],
            "categories": key[1],
            "unit": key[2],
            "type": "biosphere",
            "amount": -2,
            "formula": "dose",
            "uncertainty type": kind,
            "loc": math.log(3) if kind == 2 else -3,
            "scale": math.log(1.5) if kind == 2 else 0.125,
            "negative": True,
        }
    )
    provider = InMemoryCatalogProvider(
        biosphere=[BiosphereCatalog(context().background.biosphere, frozenset([key]), references={key: FLOW})]
    )
    source = document(record)
    before = source.data
    resources = render_ilcd_package(source, metadata_profile=profile(), dialect="openlca", catalog_provider=provider)
    path = tmp_path / "biosphere.zip"
    with zipfile.ZipFile(path, "w") as archive:
        for name, raw in resources.items():
            if name.startswith("processes/") and reverse_direction:
                root = parse_xml(raw)
                root.findall("p:exchanges/p:exchange", NS)[1].find(q("p", "exchangeDirection")).text = "Input"
                raw = xml_bytes(root)
            archive.writestr(name, raw)
    result = load_ilcd(path, context=context()).data[0]["exchanges"][1]
    assert result["amount"] == (2 if reverse_direction else -2)
    assert result["loc"] == pytest.approx(math.log(3) if kind == 2 else (3 if reverse_direction else -3), abs=1e-14)
    if kind == 2:
        assert result.get("negative", False) == (not reverse_direction)
    assert result["formula"] == ("-(dose)" if reverse_direction else "dose")
    assert source.data == before


def test_foreign_native_positive_distribution_negative_nominal_returns_explicit_false(tmp_path):
    source, provider = signed_electricity(math.log(1.5))
    resources = render_ilcd_package(source, metadata_profile=profile(), dialect="openlca", catalog_provider=provider)
    path = tmp_path / "foreign-independent-sign.zip"
    with zipfile.ZipFile(path, "w") as archive:
        for name, raw in resources.items():
            if name.startswith("processes/"):
                root = parse_xml(raw)
                element = root.findall("p:exchanges/p:exchange", NS)[1]
                element.set(q("olca", "amount"), str(abs(float(element.get(q("olca", "amount"))))))
                raw = xml_bytes(root)
            archive.writestr(name, raw)
    result = load_ilcd(path, context=context()).data[0]["exchanges"][1]
    assert result["amount"] == -2
    assert result["loc"] == pytest.approx(math.log(3), abs=1e-14)
    assert result["negative"] is False
