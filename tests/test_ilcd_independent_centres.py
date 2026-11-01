"""Native partial amounts separate distribution parameters from nominal quantities."""

import math
import zipfile

import pytest
from test_ilcd import context, dataset, document, electricity_document, profile

from brightpath.formats.ilcd import load_ilcd, render_ilcd_package, validate_ilcd_package, write_ilcd
from brightpath.formats.ilcd_package import NS, ILCDError, parse_xml, q


def production_source(kind, nominal=100, centre=150):
    record = dataset(amount=nominal)
    record["parameters"] = [{"name": "dose", "amount": nominal}]
    record["exchanges"][0].update(
        {
            "formula": "dose",
            "uncertainty type": kind,
            "loc": math.log(centre) if kind == 2 else centre,
            "scale": math.log(1.5) if kind == 2 else 0.125,
        }
    )
    return document(record)


def process_exchange(path, number):
    with zipfile.ZipFile(path) as archive:
        name = next(name for name in archive.namelist() if name.startswith("processes/"))
        root = parse_xml(archive.read(name))
    return root.find(f"p:exchanges/p:exchange[@dataSetInternalID='{number}']", NS)


@pytest.mark.parametrize("kind", [2, 3])
def test_native_partial_amount_retains_nominal_formula_and_independent_centre(tmp_path, kind):
    source = production_source(kind)
    before = source.data
    path = write_ilcd(source, tmp_path / "independent.zip", metadata_profile=profile(), dialect="openlca")
    assert not validate_ilcd_package(path)
    element = process_exchange(path, 0)
    expected_native_mean = 150 * math.exp(0.5 * math.log(1.5) ** 2) if kind == 2 else 150
    assert float(element.get(q("olca", "amount"))) == pytest.approx(expected_native_mean)
    assert element.get(q("olca", "unitId")) is None
    assert element.get(q("olca", "propertyId")) is None
    assert element.findtext("p:resultingAmount", namespaces=NS) == "100.0"
    assert element.findtext("p:meanAmount", namespaces=NS) == "1"
    assert element.findtext("p:referenceToVariable", namespaces=NS)
    result = load_ilcd(path, context=context()).data[0]["exchanges"][0]
    assert result["amount"] == 100
    assert result["formula"] == "dose"
    assert result["loc"] == pytest.approx(before[0]["exchanges"][0]["loc"], abs=1e-14)
    assert result["scale"] == before[0]["exchanges"][0]["scale"]
    assert source.data == before
    repeated = write_ilcd(load_ilcd(path, context=context()), tmp_path / "again.zip")
    assert load_ilcd(repeated, context=context()).data[0]["exchanges"][0]["loc"] == result["loc"]


@pytest.mark.parametrize("kind", [2, 3])
def test_independent_centre_is_converted_to_native_property_units_with_formula(tmp_path, kind):
    source, provider = electricity_document()
    data = source.data
    exchange = data[0]["exchanges"][1]
    exchange.update(
        {
            "uncertainty type": kind,
            "loc": math.log(3) if kind == 2 else 3,
            "scale": math.log(1.5) if kind == 2 else 0.125,
        }
    )
    source = source.replace(data=data)
    before = source.data
    path = write_ilcd(
        source, tmp_path / "converted.zip", metadata_profile=profile(), dialect="openlca", catalog_provider=provider
    )
    assert not validate_ilcd_package(path)
    element = process_exchange(path, 1)
    expected_native_mean = 3 * 3.6 * (math.exp(0.5 * math.log(1.5) ** 2) if kind == 2 else 1)
    assert float(element.get(q("olca", "amount"))) == pytest.approx(expected_native_mean)
    assert float(element.findtext("p:resultingAmount", namespaces=NS)) == 2 * 3.6
    assert element.get(q("olca", "defaultProvider"))
    assert element.get(q("olca", "unitId")) is None
    assert element.get(q("olca", "propertyId")) is None
    result = load_ilcd(path, context=context()).data[0]["exchanges"][1]
    assert result["unit"] == "kilowatt hour"
    assert result["amount"] == 2
    assert result["formula"] == "dose * 2"
    assert result["loc"] == pytest.approx(exchange["loc"])
    assert result["scale"] == exchange["scale"]
    assert source.data == before


@pytest.mark.parametrize("nominal,centre", [(100, -150), (-100, -150), (-100, 150)])
def test_normal_independent_signed_centre_roundtrip_including_waste_coordinate_change(tmp_path, nominal, centre):
    source = production_source(3, nominal, centre)
    before = source.data
    path = write_ilcd(source, tmp_path / "signed-normal.zip", metadata_profile=profile(), dialect="openlca")
    result = load_ilcd(path, context=context()).data[0]["exchanges"][0]
    assert result["amount"] == nominal
    assert result["loc"] == centre
    assert result["scale"] == 0.125
    assert result["formula"] == "dose"
    assert source.data == before


@pytest.mark.parametrize("kind", [2, 3])
def test_independent_centres_remain_guarded_in_core_ilcd(kind):
    with pytest.raises(ILCDError) as error:
        render_ilcd_package(production_source(kind), metadata_profile=profile(), dialect="core")
    assert error.value.code == "ilcd.uncertainty_unsupported"


@pytest.mark.parametrize("loc", [1000, -1000])
def test_native_log_centre_overflow_and_underflow_are_controlled(loc):
    source = production_source(2)
    data = source.data
    data[0]["exchanges"][0]["loc"] = loc
    with pytest.raises(ILCDError) as error:
        render_ilcd_package(source.replace(data=data), metadata_profile=profile(), dialect="openlca")
    assert error.value.code == "ilcd.uncertainty_invalid"


def test_native_reader_takes_actual_partial_amount_not_audit_metadata(tmp_path):
    source = production_source(3)
    resources = render_ilcd_package(source, metadata_profile=profile(), dialect="openlca")
    path = tmp_path / "modified.zip"
    with zipfile.ZipFile(path, "w") as archive:
        for name, raw in resources.items():
            if name.startswith("processes/"):
                root = parse_xml(raw)
                root.attrib.pop(q("bp", "dialect"))
                root.find("p:exchanges/p:exchange", NS).set(q("olca", "amount"), "175")
                from brightpath.formats.ilcd_package import xml_bytes

                raw = xml_bytes(root)
            archive.writestr(name, raw)
    result = load_ilcd(path, context=context()).data[0]["exchanges"][0]
    assert result["amount"] == 100
    assert result["loc"] == 175
    assert result["formula"] == "dose"


@pytest.mark.parametrize("centre", [100, 150])
def test_native_log_correction_uses_exported_rounded_spread(tmp_path, centre):
    source = production_source(2, centre=centre)
    data = source.data
    data[0]["exchanges"][0]["scale"] = 0.2
    source = source.replace(data=data)
    before = source.data
    warnings = []
    path = write_ilcd(
        source,
        tmp_path / "rounded.zip",
        metadata_profile=profile(),
        dialect="openlca",
        uncertainty_relative_tolerance=0.005,
        uncertainty_rounding_warnings=warnings,
    )
    element = process_exchange(path, 0)
    sigma = math.log(1.221)
    assert float(element.get(q("olca", "amount"))) == math.exp(math.log(centre) + 0.5 * sigma**2)
    result = load_ilcd(path, context=context()).data[0]["exchanges"][0]
    assert result["amount"] == 100
    assert result["formula"] == "dose"
    assert result["scale"] == sigma
    assert result["loc"] == pytest.approx(math.log(centre), abs=1e-14)
    assert warnings[0]["original_uncertainty"]["loc"] == math.log(centre)
    assert source.data == before


def test_foreign_native_log_without_partial_amount_reads_actual_sampling_centre(tmp_path):
    source = production_source(2)
    resources = render_ilcd_package(source, metadata_profile=profile(), dialect="openlca")
    path = tmp_path / "foreign-native.zip"
    with zipfile.ZipFile(path, "w") as archive:
        for name, raw in resources.items():
            if name.startswith("processes/"):
                root = parse_xml(raw)
                root.find("p:exchanges/p:exchange", NS).attrib.pop(q("olca", "amount"))
                from brightpath.formats.ilcd_package import xml_bytes

                raw = xml_bytes(root)
            archive.writestr(name, raw)
    result = load_ilcd(path, context=context()).data[0]["exchanges"][0]
    assert result["amount"] == 100
    assert result["loc"] == math.log(100) - 0.5 * math.log(1.5) ** 2
