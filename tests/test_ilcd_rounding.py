"""Explicit bounded export rounding; native readback is always authoritative."""

import math
import zipfile

import pytest
from test_ilcd import context, dataset, document, electricity_document, profile

from brightpath.formats.ilcd import load_ilcd, render_ilcd_package, validate_ilcd_package, write_ilcd
from brightpath.formats.ilcd_package import NS, ILCDError, parse_xml


def uncertain(kind=2, scale=0.2, amount=100):
    record = dataset(amount=amount)
    record["parameters"] = [{"name": "dose", "amount": amount}]
    record["exchanges"][0].update(
        {
            "formula": "dose",
            "uncertainty type": kind,
            "loc": math.log(abs(amount)) if kind == 2 else amount,
            "scale": scale,
        }
    )
    return record


@pytest.mark.parametrize("kind,scale,readback", [(2, 0.2, math.log(1.221)), (3, 2.123456, 2.123)])
def test_explicit_rounding_preserves_source_and_reimports_actual_field(tmp_path, kind, scale, readback):
    record = uncertain(kind, scale)
    record["exchanges"][0]["comment"] = "Qualifications. " * 60
    source = document(record)
    before = source.data
    with pytest.raises(ILCDError):
        render_ilcd_package(source, metadata_profile=profile(), dialect="openlca")
    warnings = []
    output = write_ilcd(
        source,
        tmp_path / "rounded.zip",
        metadata_profile=profile(),
        dialect="openlca",
        uncertainty_relative_tolerance=0.005,
        uncertainty_rounding_warnings=warnings,
    )
    assert not validate_ilcd_package(output)
    assert len(warnings) == 1
    row = warnings[0]
    assert row["dataset_code"] == record["code"]
    assert row["exchange_index"] == 0
    assert row["original_uncertainty"] == {
        key: record["exchanges"][0][key] for key in ("uncertainty type", "loc", "scale")
    }
    assert row["original_amount"] == 100
    assert row["original_unit"] == "kilogram"
    assert row["original_scale"] == row["native_original_scale"] == scale
    assert row["exported_scale"] == row["native_exported_scale"] == readback
    assert row["relative_error"] == abs(readback - scale) / scale <= 0.005
    restored = load_ilcd(output, context=context())
    result = restored.data[0]["exchanges"][0]
    assert result["scale"] == readback != scale
    assert result["amount"] == 100
    assert result["loc"] == record["exchanges"][0]["loc"]
    assert result["formula"] == "dose"
    assert result["comment"] == record["exchanges"][0]["comment"]
    assert restored.data[0]["comment"] == record["comment"]
    assert restored.data[0]["ilcd"]["uncertainty_rounding_warnings"] == warnings
    with zipfile.ZipFile(output) as package:
        name = next(name for name in package.namelist() if name.startswith("processes/"))
        root = parse_xml(package.read(name))
        comment = root.findtext("p:processInformation/p:dataSetInformation/c:generalComment", namespaces=NS)
        assert "ILCD uncertainty rounding warnings" in comment
        assert '"original_scale"' in comment
    again_warnings = []
    again = write_ilcd(restored, tmp_path / "again.zip", uncertainty_rounding_warnings=again_warnings)
    repeated = load_ilcd(again, context=context())
    assert repeated.data[0]["exchanges"][0]["scale"] == readback
    assert repeated.data[0]["ilcd"]["uncertainty_rounding_warnings"] == warnings
    assert not again_warnings
    assert source.data == before


@pytest.mark.parametrize("kind,scale", [(2, 0.0099), (2, 0.0001), (2, 1e-14), (3, 0.0001), (3, 0.0009)])
@pytest.mark.parametrize("tolerance", [0.0, 0.005])
def test_over_budget_or_positive_to_zero_is_rejected(kind, scale, tolerance):
    source = document(uncertain(kind, scale))
    before = source.data
    warnings = []
    with pytest.raises(ILCDError) as error:
        render_ilcd_package(
            source,
            metadata_profile=profile(),
            dialect="openlca",
            uncertainty_relative_tolerance=tolerance,
            uncertainty_rounding_warnings=warnings,
        )
    assert error.value.code == "ilcd.uncertainty_precision_unsupported"
    assert warnings == []
    assert source.data == before


@pytest.mark.parametrize("kind,scale", [(2, 0.2), (3, 1.0004)])
def test_inclusive_exact_error_boundary_and_next_float_below(kind, scale):
    field = round(math.exp(scale) if kind == 2 else scale, 3)
    readback = math.log(field) if kind == 2 else field
    boundary = abs(readback - scale) / scale
    assert 0 < boundary < 0.005
    source = document(uncertain(kind, scale))
    render_ilcd_package(source, metadata_profile=profile(), dialect="openlca", uncertainty_relative_tolerance=boundary)
    with pytest.raises(ILCDError) as error:
        render_ilcd_package(
            source,
            metadata_profile=profile(),
            dialect="openlca",
            uncertainty_relative_tolerance=math.nextafter(boundary, 0),
        )
    assert error.value.code == "ilcd.uncertainty_precision_unsupported"


@pytest.mark.parametrize("kind,scale", [(2, math.log(1.5)), (3, 2.123), (3, 0)])
def test_already_representable_has_no_rounding_audit(kind, scale):
    warnings = []
    source = document(uncertain(kind, scale))
    strict = render_ilcd_package(source, metadata_profile=profile(), dialect="openlca")
    opted_in = render_ilcd_package(
        source,
        metadata_profile=profile(),
        dialect="openlca",
        uncertainty_relative_tolerance=0.005,
        uncertainty_rounding_warnings=warnings,
    )
    assert strict == opted_in
    assert not warnings


@pytest.mark.parametrize(
    "tolerance", [-0.001, math.nextafter(0.02, math.inf), 0.02000000001, math.nan, math.inf, True, None, "0.005"]
)
def test_tolerance_validation_is_finite_numeric_and_capped(tolerance):
    with pytest.raises(ILCDError) as error:
        render_ilcd_package(
            document(), metadata_profile=profile(), dialect="openlca", uncertainty_relative_tolerance=tolerance
        )
    assert error.value.code == "ilcd.uncertainty_tolerance_invalid"


def test_core_tolerance_is_not_enabled_and_collector_type_is_checked():
    with pytest.raises(ILCDError, match="only for dialect"):
        render_ilcd_package(
            document(), metadata_profile=profile(), dialect="core", uncertainty_relative_tolerance=0.005
        )
    with pytest.raises(ILCDError, match="list or None"):
        render_ilcd_package(document(), metadata_profile=profile(), uncertainty_rounding_warnings={})


@pytest.mark.parametrize("kind,field,value", [(2, "negative", True), (6, "scale", 0.2)])
def test_tolerance_does_not_relax_unsupported_signs_or_distributions(kind, field, value):
    record = uncertain(2 if kind == 6 else kind)
    record["exchanges"][0]["uncertainty type"] = kind
    record["exchanges"][0][field] = value
    with pytest.raises(ILCDError) as error:
        render_ilcd_package(
            document(record), metadata_profile=profile(), dialect="openlca", uncertainty_relative_tolerance=0.005
        )
    assert error.value.code == "ilcd.uncertainty_unsupported"


def test_rounding_occurs_after_native_quantity_conversion(tmp_path):
    source, provider = electricity_document()
    data = source.data
    data[0]["exchanges"][1]["scale"] = 2.123456
    source = source.replace(data=data)
    before = source.data
    warnings = []
    output = write_ilcd(
        source,
        tmp_path / "native-units.zip",
        metadata_profile=profile(),
        dialect="openlca",
        catalog_provider=provider,
        uncertainty_relative_tolerance=0.005,
        uncertainty_rounding_warnings=warnings,
    )
    row = warnings[0]
    assert row["exchange_index"] == 1
    assert row["original_unit"] == "kilowatt hour"
    assert row["native_unit"] == "megajoule"
    assert row["original_scale"] == 2.123456
    assert row["native_original_scale"] == 2.123456 * 3.6
    assert row["native_rounded_dispersion"] == row["native_exported_scale"] == 7.644
    assert row["exported_scale"] == 7.644 * (1 / 3.6)
    result = load_ilcd(output, context=context()).data[0]["exchanges"][1]
    assert result["scale"] == row["exported_scale"]
    assert result["amount"] == result["loc"] == 2
    assert result["formula"] == "dose * 2"
    assert source.data == before


def test_canonical_unit_error_boundary_is_inclusive_after_conversion():
    source, provider = electricity_document()
    data = source.data
    original_scale = 1.0004
    data[0]["exchanges"][1]["scale"] = original_scale
    source = source.replace(data=data)
    readback = round(original_scale * 3.6, 3) * (1 / 3.6)
    boundary = abs(readback - original_scale) / original_scale
    warnings = []
    render_ilcd_package(
        source,
        metadata_profile=profile(),
        dialect="openlca",
        catalog_provider=provider,
        uncertainty_relative_tolerance=boundary,
        uncertainty_rounding_warnings=warnings,
    )
    assert warnings[0]["relative_error"] == boundary
    with pytest.raises(ILCDError):
        render_ilcd_package(
            source,
            metadata_profile=profile(),
            dialect="openlca",
            catalog_provider=provider,
            uncertainty_relative_tolerance=math.nextafter(boundary, 0),
        )


@pytest.mark.parametrize("tolerance", [0.0, 0.005])
def test_native_quantity_underflow_cannot_erase_positive_normal_spread(tolerance):
    from brightpath.background import InMemoryCatalogProvider
    from brightpath.background.catalogs import TechnosphereCatalog

    record = dataset()
    key = ("Synthetic gram supplier", "Gram product", "CH", "gram")
    record["exchanges"].append(
        {
            **dict(zip(("name", "reference product", "location", "unit"), key, strict=True)),
            "type": "technosphere",
            "amount": 1,
            "uncertainty type": 3,
            "loc": 1,
            "scale": math.nextafter(0, 1),
        }
    )
    provider = InMemoryCatalogProvider(
        technosphere=[
            TechnosphereCatalog(
                context().background.technosphere,
                frozenset([key]),
                references={key: ("11111111-1111-4111-8111-111111111111", "22222222-2222-4222-8222-222222222222")},
            )
        ]
    )
    with pytest.raises(ILCDError, match="positive spread"):
        render_ilcd_package(
            document(record),
            metadata_profile=profile(),
            dialect="openlca",
            catalog_provider=provider,
            uncertainty_relative_tolerance=tolerance,
        )


def test_collector_retains_all_present_original_uncertainty_fields():
    record = uncertain(3, 2.123456)
    record["exchanges"][0].update({"shape": None, "minimum": None, "maximum": None, "negative": False})
    source = document(record)
    before = source.data
    warnings = []
    render_ilcd_package(
        source,
        metadata_profile=profile(),
        dialect="openlca",
        uncertainty_relative_tolerance=0.005,
        uncertainty_rounding_warnings=warnings,
    )
    assert warnings[0]["original_uncertainty"] == {
        "uncertainty type": 3,
        "loc": 100,
        "scale": 2.123456,
        "shape": None,
        "minimum": None,
        "maximum": None,
        "negative": False,
    }
    warnings[0]["original_uncertainty"]["scale"] = 999
    assert source.data == before


def test_waste_sign_conversion_and_original_index_before_sorting(tmp_path):
    treatment = dataset("treatment", "Treatment", "Waste", -100)
    consumer = dataset("consumer", "Consumer", "Product", 1)
    production = consumer["exchanges"][0]
    production.update({"uncertainty type": 3, "loc": 1, "scale": 2.123456})
    waste = {
        **{key: treatment[key] for key in ("name", "reference product", "unit", "location")},
        "type": "technosphere",
        "amount": -3,
        "loc": -3,
        "uncertainty type": 3,
        "scale": 2.123456,
    }
    consumer["exchanges"] = [waste, production]
    source = document(treatment, consumer)
    before = source.data
    warnings = []
    path = write_ilcd(
        source,
        tmp_path / "waste.zip",
        metadata_profile=profile(),
        dialect="openlca",
        uncertainty_relative_tolerance=0.005,
        uncertainty_rounding_warnings=warnings,
    )
    assert [row["exchange_index"] for row in warnings] == [1, 0]
    assert all(row["dataset_code"] == "consumer" for row in warnings)
    assert warnings[1]["original_amount"] == warnings[1]["original_uncertainty"]["loc"] == -3
    assert warnings[1]["native_amount"] == 3
    by_name = {record["name"]: record for record in load_ilcd(path, context=context()).data}
    result = by_name["Consumer"]["exchanges"][1]
    assert result["amount"] == result["loc"] == -3
    assert result["scale"] == warnings[1]["exported_scale"] == 2.123
    assert source.data == before


def test_failed_render_and_failed_publication_do_not_append_partial_warnings(tmp_path):
    first = uncertain()
    second = uncertain()
    second["code"] = "second"
    second["name"] = "Second"
    second["exchanges"][0]["negative"] = True
    warnings = [{"existing": True}]
    with pytest.raises(ILCDError):
        render_ilcd_package(
            document(first, second),
            metadata_profile=profile(),
            dialect="openlca",
            uncertainty_relative_tolerance=0.005,
            uncertainty_rounding_warnings=warnings,
        )
    assert warnings == [{"existing": True}]
    with pytest.raises(ILCDError):
        write_ilcd(
            document(first),
            tmp_path / "missing" / "out.zip",
            metadata_profile=profile(),
            dialect="openlca",
            uncertainty_relative_tolerance=0.005,
            uncertainty_rounding_warnings=warnings,
        )
    assert warnings == [{"existing": True}]


def test_preflight_reports_rounding_without_mutating_export_collector():
    from brightpath.adapters import ILCDAdapter
    from brightpath.core.policies import ConversionPolicy

    warnings = []
    result = ILCDAdapter().preflight_conversion(
        document(uncertain()),
        policy=ConversionPolicy.strict(),
        metadata_profile=profile(),
        dialect="openlca",
        uncertainty_relative_tolerance=0.005,
        uncertainty_rounding_warnings=warnings,
    )
    assert not result.has_errors
    assert warnings == []
    assert len(result.issues) == 1
    issue = result.issues[0]
    assert issue.code == "ilcd.uncertainty_rounded"
    assert issue.to_dict()["severity"] == "warning"
    assert issue.details["original_scale"] == 0.2
    assert issue.details["exported_scale"] == math.log(1.221)


@pytest.mark.parametrize("tolerance", [0.005, 0.00500000001, 0.01, 0.015, 0.02])
def test_explicit_opt_in_values_through_two_percent_are_valid(tolerance):
    render_ilcd_package(
        document(), metadata_profile=profile(), dialect="openlca", uncertainty_relative_tolerance=tolerance
    )


def test_two_percent_accepts_observed_worst_logspread_and_preserves_independent_centre(tmp_path):
    scale = math.sqrt(0.0002)
    record = uncertain(2, scale)
    record["exchanges"][0]["loc"] = math.log(150)
    source = document(record)
    before = source.data
    for tolerance in (0.0, 0.005, 0.015):
        with pytest.raises(ILCDError) as error:
            render_ilcd_package(
                source, metadata_profile=profile(), dialect="openlca", uncertainty_relative_tolerance=tolerance
            )
        assert error.value.code == "ilcd.uncertainty_precision_unsupported"
    warnings = []
    output = write_ilcd(
        source,
        tmp_path / "two-percent.zip",
        metadata_profile=profile(),
        dialect="openlca",
        uncertainty_relative_tolerance=0.02,
        uncertainty_rounding_warnings=warnings,
    )
    assert not validate_ilcd_package(output)
    result = load_ilcd(output, context=context()).data[0]["exchanges"][0]
    sigma = math.log(1.014)
    assert result["amount"] == 100
    assert result["formula"] == "dose"
    assert result["unit"] == "kilogram"
    assert result["loc"] == pytest.approx(math.log(150), abs=1e-14)
    assert result["scale"] == sigma != scale
    assert not result["negative"]
    (row,) = warnings
    assert row["original_scale"] == scale
    assert row["exported_scale"] == sigma
    assert row["relative_error"] == abs(sigma - scale) / scale
    assert 0.015 < row["relative_error"] < row["tolerance"] == 0.02
    assert row["original_uncertainty"]["loc"] == math.log(150)
    assert source.data == before


@pytest.mark.parametrize("negative", [False, True])
def test_two_percent_logspread_preserves_native_units_nominal_sign_formula_and_centre(tmp_path, negative):
    source, provider = electricity_document()
    data = source.data
    original = data[0]["exchanges"][1]
    original.update(
        {
            "amount": -2 if negative else 2,
            "formula": "-(dose * 2)" if negative else "dose * 2",
            "uncertainty type": 2,
            "loc": math.log(3),
            "scale": math.sqrt(0.0002),
            "negative": negative,
        }
    )
    source = source.replace(data=data)
    before = source.data
    warnings = []
    path = write_ilcd(
        source,
        tmp_path / "converted-log.zip",
        metadata_profile=profile(),
        dialect="openlca",
        catalog_provider=provider,
        uncertainty_relative_tolerance=0.02,
        uncertainty_rounding_warnings=warnings,
    )
    result = load_ilcd(path, context=context()).data[0]["exchanges"][1]
    for key in ("amount", "formula", "unit", "negative"):
        assert result[key] == original[key]
    assert result["loc"] == pytest.approx(original["loc"], abs=1e-14)
    assert result["scale"] == math.log(1.014)
    (row,) = warnings
    assert row["exchange_index"] == 1
    assert row["native_amount"] == (-7.2 if negative else 7.2)
    assert row["native_unit"] == "megajoule"
    assert row["original_unit"] == "kilowatt hour"
    assert row["original_uncertainty"]["negative"] is negative
    assert row["original_uncertainty"]["loc"] == math.log(3)
    assert source.data == before


def test_two_percent_normalspread_is_checked_in_original_units_after_conversion(tmp_path):
    source, provider = electricity_document()
    data = source.data
    original_scale = 0.001 / (3.6 * 1.017)
    data[0]["exchanges"][1].update({"scale": original_scale, "loc": 3})
    source = source.replace(data=data)
    before = source.data
    warnings = []
    path = write_ilcd(
        source,
        tmp_path / "converted-normal.zip",
        metadata_profile=profile(),
        dialect="openlca",
        catalog_provider=provider,
        uncertainty_relative_tolerance=0.02,
        uncertainty_rounding_warnings=warnings,
    )
    result = load_ilcd(path, context=context()).data[0]["exchanges"][1]
    (row,) = warnings
    assert row["original_scale"] == original_scale
    assert row["native_original_scale"] == original_scale * 3.6
    assert row["native_rounded_dispersion"] == 0.001
    assert row["exported_scale"] == result["scale"] == 0.001 * (1 / 3.6)
    assert 0.015 < row["relative_error"] < 0.02
    assert result["amount"] == 2
    assert result["loc"] == pytest.approx(3, abs=1e-14)
    assert result["formula"] == "dose * 2"
    assert result["unit"] == "kilowatt hour"
    assert source.data == before


@pytest.mark.parametrize("kind,scale", [(2, 0.00204), (3, 0.00098)])
def test_two_percent_still_rejects_spread_error_just_over_budget(kind, scale):
    source = document(uncertain(kind, scale))
    before = source.data
    warnings = []
    with pytest.raises(ILCDError) as error:
        render_ilcd_package(
            source,
            metadata_profile=profile(),
            dialect="openlca",
            uncertainty_relative_tolerance=0.02,
            uncertainty_rounding_warnings=warnings,
        )
    assert error.value.code == "ilcd.uncertainty_precision_unsupported"
    assert not warnings
    assert source.data == before


@pytest.mark.parametrize("kind,scale", [(2, 0.0001), (2, 1e-14), (3, 0.0001)])
def test_two_percent_cannot_replace_positive_spread_with_zero(kind, scale):
    with pytest.raises(ILCDError) as error:
        render_ilcd_package(
            document(uncertain(kind, scale)),
            metadata_profile=profile(),
            dialect="openlca",
            uncertainty_relative_tolerance=0.02,
        )
    assert error.value.code == "ilcd.uncertainty_precision_unsupported"


def test_two_percent_does_not_enable_approximation_in_core():
    with pytest.raises(ILCDError, match="only for dialect"):
        render_ilcd_package(
            document(uncertain()), metadata_profile=profile(), dialect="core", uncertainty_relative_tolerance=0.02
        )


def test_high_error_inclusive_boundary_and_next_float_below_remain_strict():
    scale = math.sqrt(0.0002)
    boundary = abs(math.log(1.014) - scale) / scale
    assert 0.015 < boundary < 0.02
    source = document(uncertain(2, scale))
    render_ilcd_package(source, metadata_profile=profile(), dialect="openlca", uncertainty_relative_tolerance=boundary)
    with pytest.raises(ILCDError) as error:
        render_ilcd_package(
            source,
            metadata_profile=profile(),
            dialect="openlca",
            uncertainty_relative_tolerance=math.nextafter(boundary, 0),
        )
    assert error.value.code == "ilcd.uncertainty_precision_unsupported"
