"""Independent ILCD fixtures and process-package conversion regressions."""

from __future__ import annotations

import hashlib
import json
import math
import shutil
import zipfile
from copy import deepcopy

import pytest
from lxml import etree

from brightpath.adapters import ArtifactKind, ILCDAdapter, default_adapter_registry
from brightpath.analysis import analyze_inventory, infer_source_format
from brightpath.background import InMemoryCatalogProvider
from brightpath.background.catalogs import BiosphereCatalog, TechnosphereCatalog, catalog_provider_from_environment
from brightpath.core import (
    BackgroundContext,
    BiosphereProfile,
    ContextHint,
    FormatProfile,
    InventoryContext,
    TechnosphereProfile,
)
from brightpath.core.pipeline import InventoryPipeline
from brightpath.core.policies import ConversionPolicy
from brightpath.formats import ilcd_package
from brightpath.formats.ilcd import (
    load_ilcd,
    load_ilcd_package,
    render_ilcd_package,
    validate_ilcd_package,
    validate_ilcd_xml,
    write_ilcd,
)
from brightpath.formats.ilcd_background import automatic_references, openlca_key
from brightpath.formats.ilcd_package import NS, SCHEMA_DIRECTORY, ILCDError, ILCDPackage, parse_xml, q, schema
from brightpath.formats.ilcd_parameters import evaluate, parameter_values
from brightpath.formats.ilcd_references import ILCDReferenceCatalog
from brightpath.formats.openlca_references import load_openlca_reference_catalog
from brightpath.models import InventoryDocument

PROCESS = "11111111-1111-4111-8111-111111111111"
FLOW = "22222222-2222-4222-8222-222222222222"
PROPERTY = "33333333-3333-4333-8333-333333333333"
UNIT = "44444444-4444-4444-8444-444444444444"


def context(format_id="ilcd", dialect=""):
    return InventoryContext(
        FormatProfile(format_id, format_version="1.1" if format_id == "ilcd" else "", dialect=dialect),
        BackgroundContext(
            TechnosphereProfile("ecoinvent", "3.10.1", "cutoff"), BiosphereProfile("ecoinvent", "3.10.1")
        ),
    )


def profile():
    return {
        "schema_version": 1,
        "defaults": {
            "dataset_version": "01.00.000",
            "reference_year": 2020,
            "valid_until": 2025,
            "process_type": "Unit process, single operation",
        },
    }


def dataset(code="foreground-1", name="Process A", product="Product A", amount=100):
    return {
        "name": name,
        "reference product": product,
        "location": "CH",
        "unit": "kilogram",
        "code": code,
        "comment": "Synthetic process",
        "custom metadata": {"keep": [1, 2]},
        "exchanges": [
            {
                "name": name,
                "reference product": product,
                "location": "CH",
                "unit": "kilogram",
                "type": "production",
                "amount": amount,
            }
        ],
    }


def document(*data):
    return InventoryDocument(data=list(data or [dataset()]), context=context("brightway_excel"))


def electricity_document():
    record = dataset()
    record["parameters"] = [{"name": "dose", "amount": 1}]
    record["exchanges"].append(
        {
            "type": "technosphere",
            "name": "Synthetic electricity provider",
            "reference product": "Synthetic electricity",
            "location": "CH",
            "unit": "kilowatt hour",
            "amount": 2,
            "formula": "dose * 2",
            "uncertainty type": 3,
            "loc": 2,
            "scale": 0.2,
        }
    )
    key = ("Synthetic electricity provider", "Synthetic electricity", "CH", "kilowatt hour")
    provider = InMemoryCatalogProvider(
        technosphere=[
            TechnosphereCatalog(
                context().background.technosphere,
                frozenset([key]),
                references={key: (PROCESS, FLOW)},
            )
        ]
    )
    return document(record), provider


def test_automatic_reference_ids_match_native_openlca_keygen():
    assert openlca_key(PROCESS, FLOW) == "bf62d289-bb74-3403-b803-b8f82e4bdedd"


def test_automatic_references_convert_quantities_formulas_uncertainty_and_roundtrip(tmp_path):
    source, provider = electricity_document()
    before = source.data
    output = write_ilcd(
        source, tmp_path / "auto.zip", metadata_profile=profile(), dialect="openlca", catalog_provider=provider
    )
    assert not validate_ilcd_package(output)
    with zipfile.ZipFile(output) as archive:
        names = archive.namelist()
        assert sum(name.startswith("processes/") for name in names) == 1
        root = parse_xml(archive.read(next(name for name in names if name.startswith("processes/"))))
        exchange = root.findall("p:exchanges/p:exchange", NS)[1]
        assert exchange.get(q("olca", "defaultProvider")) == openlca_key(PROCESS, FLOW)
        assert exchange.find("p:referenceToFlowDataSet", NS).get("refObjectId") == FLOW
        assert float(exchange.findtext("p:resultingAmount", namespaces=NS)) == pytest.approx(7.2)
        assert float(exchange.findtext("p:relativeStandardDeviation95In", namespaces=NS)) == pytest.approx(0.72)
    restored = load_ilcd(output, context=context())
    exchange = restored.data[0]["exchanges"][1]
    assert exchange["unit"] == "kilowatt hour"
    assert exchange["amount"] == pytest.approx(2)
    assert exchange["loc"] == pytest.approx(2)
    assert exchange["scale"] == pytest.approx(0.2)
    assert exchange["formula"] == "dose * 2"
    second = write_ilcd(restored, tmp_path / "again.zip", catalog_provider=InMemoryCatalogProvider())
    assert load_ilcd(second, context=context()).data[0]["exchanges"][1]["amount"] == pytest.approx(2)
    assert source.data == before
    assert render_ilcd_package(
        source, metadata_profile=profile(), dialect="openlca", catalog_provider=provider
    ) == render_ilcd_package(source, metadata_profile=profile(), dialect="openlca", catalog_provider=provider)


def test_pipeline_uses_injected_reference_provider(tmp_path):
    source, provider = electricity_document()
    pipeline = InventoryPipeline(default_adapter_registry(), provider)
    result = pipeline.write(
        source,
        tmp_path / "pipeline.zip",
        target_format="ilcd",
        adapter_options={
            "metadata_profile": profile(),
            "dialect": "openlca",
        },
    )
    assert not result.report.has_errors, result.report.to_dict()


def test_missing_exact_reference_is_not_invented(tmp_path):
    source, _provider = electricity_document()
    key = ("Unrelated", "Product", "CH", "kilowatt hour")
    provider = InMemoryCatalogProvider(
        technosphere=[
            TechnosphereCatalog(
                context().background.technosphere,
                frozenset([key]),
                references={key: (PROCESS, FLOW)},
            )
        ]
    )
    with pytest.raises(ILCDError, match="No exact ILCD technosphere mapping"):
        write_ilcd(
            source, tmp_path / "missing.zip", metadata_profile=profile(), dialect="openlca", catalog_provider=provider
        )
    assert not (tmp_path / "missing.zip").exists()


def test_automatic_references_reject_wrong_profile_from_provider(monkeypatch):
    source, provider = electricity_document()
    key = ("Synthetic electricity provider", "Synthetic electricity", "CH", "kilowatt hour")
    wrong = TechnosphereCatalog(
        TechnosphereProfile("ecoinvent", "3.10", "cutoff"), frozenset([key]), references={key: (PROCESS, FLOW)}
    )
    monkeypatch.setattr(provider, "load_technosphere", lambda _profile: wrong)
    with pytest.raises(ILCDError, match="different background profile"):
        automatic_references(source, ILCDReferenceCatalog(), dialect="openlca", catalog_provider=provider)


def test_automatic_references_reject_unknown_quantity_definitions():
    record = dataset()
    key = ("Synthetic provider", "Synthetic product", "CH", "unknown unit")
    record["exchanges"].append(
        {
            **dict(zip(("name", "reference product", "location", "unit"), key, strict=True)),
            "type": "technosphere",
            "amount": 2,
        }
    )
    provider = InMemoryCatalogProvider(
        technosphere=[
            TechnosphereCatalog(
                context().background.technosphere,
                frozenset([key]),
                references={key: (PROCESS, FLOW)},
            )
        ]
    )
    with pytest.raises(ILCDError, match="No verified ILCD background quantity definition"):
        automatic_references(document(record), ILCDReferenceCatalog(), dialect="openlca", catalog_provider=provider)


def test_automatic_biosphere_lookup_does_not_require_a_technosphere_catalog():
    record = dataset()
    key = ("Synthetic emission", ("air",), "kilogram")
    record["exchanges"].append(
        {"name": key[0], "categories": key[1], "unit": key[2], "type": "biosphere", "amount": 0.5}
    )
    provider = InMemoryCatalogProvider(
        biosphere=[
            BiosphereCatalog(
                context().background.biosphere,
                frozenset([key]),
                references={key: FLOW},
            )
        ]
    )
    resources = render_ilcd_package(
        document(record), metadata_profile=profile(), dialect="openlca", catalog_provider=provider
    )
    assert f"flows/{FLOW}.xml" in resources


@pytest.mark.parametrize(
    "version,model", [("3.6", "cutoff"), ("3.10.1", "cutoff"), ("3.12", "cutoff"), ("3.12", "consequential")]
)
def test_packaged_ecoinvent_references_are_used_without_configuration(version, model):
    provider = catalog_provider_from_environment()
    background = BackgroundContext(
        TechnosphereProfile("ecoinvent", version, model), BiosphereProfile("ecoinvent", version)
    )
    catalog = provider.load_technosphere(background.technosphere)
    key = next(key for key in sorted(catalog.references) if key[-1] == "kilogram")
    record = dataset()
    record["exchanges"].append(
        dict(zip(("name", "reference product", "location", "unit"), key, strict=True), type="technosphere", amount=1)
    )
    flow_catalog = provider.load_biosphere(background.biosphere)
    flow_key = next(key for key in sorted(flow_catalog.references) if key[1][0] == "air" and key[-1] == "kilogram")
    record["exchanges"].append(
        {"name": flow_key[0], "categories": flow_key[1], "unit": flow_key[2], "type": "biosphere", "amount": 0.5}
    )
    source = InventoryDocument(data=[record], context=InventoryContext(FormatProfile("brightway_excel"), background))
    refs = automatic_references(source, ILCDReferenceCatalog(), dialect="openlca")
    assert len(refs.technosphere) == len(refs.biosphere) == 1
    assert refs.technosphere[0]["flow_id"] == catalog.references[key][1]
    assert refs.biosphere[0]["flow_id"] == flow_catalog.references[flow_key]
    resources = render_ilcd_package(source, metadata_profile=profile(), dialect="openlca")
    assert len([path for path in resources if path.startswith("processes/")]) == 1


def test_packaged_uvek_references_use_verified_target_uuids_and_partial_coverage():
    background = BackgroundContext(TechnosphereProfile("uvek", "2025", "cutoff"), BiosphereProfile("ecoinvent", "3.10"))
    source_context = InventoryContext(FormatProfile("brightway_excel"), background)
    target = load_openlca_reference_catalog(source_context)
    key = next(key for key in sorted(target.technosphere) if key[-1] == "kilowatt hour")
    record = dataset()
    record["exchanges"].append(
        dict(zip(("name", "reference product", "location", "unit"), key, strict=True), type="technosphere", amount=2)
    )
    source = InventoryDocument(data=[record], context=source_context)
    refs = automatic_references(source, ILCDReferenceCatalog(), dialect="openlca")
    assert refs.technosphere[0]["process_id"] == target.technosphere[key].process_id
    assert refs.technosphere[0]["flow_id"] == target.technosphere[key].flow_id
    assert refs.technosphere[0]["amount_factor"] == 3.6
    render_ilcd_package(source, metadata_profile=profile(), dialect="openlca")
    # A flow absent from this exact openLCA build must not fall back to an ecoSpold UUID.
    biosphere = catalog_provider_from_environment().load_biosphere(background.biosphere)
    missing = next(key for key in sorted(biosphere.references) if key not in target.biosphere)
    record["exchanges"].append(
        {"name": missing[0], "categories": missing[1], "unit": missing[2], "type": "biosphere", "amount": 1}
    )
    source = source.replace(data=[record])
    with pytest.raises(ILCDError, match="No exact ILCD biosphere mapping"):
        render_ilcd_package(source, metadata_profile=profile(), dialect="openlca")


@pytest.mark.parametrize("factor", [0, -1, True, float("nan"), float("inf")])
def test_reference_conversion_factors_are_validated(factor):
    with pytest.raises(ILCDError, match="finite and positive"):
        ILCDReferenceCatalog.from_mapping(
            {
                "biosphere": [
                    {
                        "identity": {"name": "Synthetic flow", "categories": ["air"], "unit": "kilogram"},
                        "flow_id": FLOW,
                        "flow_version": "01.00.000",
                        "amount_factor": factor,
                        "reference_unit": "kilogram",
                    }
                ]
            }
        )


@pytest.fixture
def native_resources():
    """Hand-authored XML, independent of BrightPath's writer."""
    return {
        f"processes/{PROCESS}.xml": f"""<processDataSet xmlns="{NS['p']}" xmlns:c="{NS['c']}" xmlns:xml="{NS['xml']}" version="1.1" locations="../ILCDLocations.xml">
  <processInformation><dataSetInformation><c:UUID>{PROCESS}</c:UUID><name><baseName xml:lang="en">Synthetic process</baseName><baseName xml:lang="fr">Procédé synthétique</baseName></name><c:generalComment xml:lang="en">Independent fixture</c:generalComment></dataSetInformation>
    <quantitativeReference type="Reference flow(s)"><referenceToReferenceFlow>9</referenceToReferenceFlow></quantitativeReference>
    <time><c:referenceYear>2020</c:referenceYear><c:dataSetValidUntil>2025</c:dataSetValidUntil></time>
    <geography><locationOfOperationSupplyOrProduction location="CH"/></geography>
  </processInformation>
  <modellingAndValidation><LCIMethodAndAllocation><typeOfDataSet>Unit process, single operation</typeOfDataSet></LCIMethodAndAllocation></modellingAndValidation>
  <administrativeInformation><publicationAndOwnership><c:dataSetVersion>01.00.000</c:dataSetVersion></publicationAndOwnership></administrativeInformation>
  <exchanges><exchange dataSetInternalID="9"><referenceToFlowDataSet type="flow data set" refObjectId="{FLOW}" version="01.00.000" uri="../flows/{FLOW}.xml"/><exchangeDirection>Output</exchangeDirection><meanAmount>100</meanAmount><resultingAmount>100</resultingAmount><generalComment xml:lang="en">Production</generalComment></exchange></exchanges>
</processDataSet>""",
        f"flows/{FLOW}.xml": f"""<flowDataSet xmlns="{NS['f']}" xmlns:c="{NS['c']}" version="1.1">
 <flowInformation><dataSetInformation><c:UUID>{FLOW}</c:UUID><name><baseName xml:lang="en">Synthetic product</baseName></name></dataSetInformation><quantitativeReference><referenceToReferenceFlowProperty>5</referenceToReferenceFlowProperty></quantitativeReference></flowInformation>
 <modellingAndValidation><LCIMethod><typeOfDataSet>Product flow</typeOfDataSet></LCIMethod></modellingAndValidation>
 <administrativeInformation><publicationAndOwnership><c:dataSetVersion>01.00.000</c:dataSetVersion></publicationAndOwnership></administrativeInformation>
 <flowProperties><flowProperty dataSetInternalID="5"><referenceToFlowPropertyDataSet type="flow property data set" refObjectId="{PROPERTY}" version="01.00.000" uri="../flowproperties/{PROPERTY}.xml"/><meanValue>1</meanValue></flowProperty></flowProperties>
</flowDataSet>""",
        f"flowproperties/{PROPERTY}.xml": f"""<flowPropertyDataSet xmlns="{NS['fp']}" xmlns:c="{NS['c']}" version="1.1">
 <flowPropertiesInformation><dataSetInformation><c:UUID>{PROPERTY}</c:UUID><c:name xml:lang="en">Mass</c:name></dataSetInformation><quantitativeReference><referenceToReferenceUnitGroup type="unit group data set" refObjectId="{UNIT}" version="01.00.000" uri="../unitgroups/{UNIT}.xml"/></quantitativeReference></flowPropertiesInformation>
 <administrativeInformation><publicationAndOwnership><c:dataSetVersion>01.00.000</c:dataSetVersion></publicationAndOwnership></administrativeInformation>
</flowPropertyDataSet>""",
        f"unitgroups/{UNIT}.xml": f"""<unitGroupDataSet xmlns="{NS['u']}" xmlns:c="{NS['c']}" version="1.1">
 <unitGroupInformation><dataSetInformation><c:UUID>{UNIT}</c:UUID><c:name xml:lang="en">Mass units</c:name></dataSetInformation><quantitativeReference><referenceToReferenceUnit>7</referenceToReferenceUnit></quantitativeReference></unitGroupInformation>
 <administrativeInformation><publicationAndOwnership><c:dataSetVersion>01.00.000</c:dataSetVersion></publicationAndOwnership></administrativeInformation>
 <units><unit dataSetInternalID="7"><name>kg</name><meanValue>1</meanValue></unit></units>
</unitGroupDataSet>""",
        "ILCDLocations.xml": '<ILCDLocations xmlns="http://lca.jrc.it/ILCD/Locations"><location value="CH">Switzerland</location></ILCDLocations>',
    }


def archive(tmp_path, resources, filename="inventory.zip", prefix=""):
    path = tmp_path / filename
    with zipfile.ZipFile(path, "w") as output:
        for name, value in resources.items():
            output.writestr(prefix + name, value)
    return path


def test_independent_fixture_resolves_quantities_versions_and_metadata(tmp_path, native_resources):
    path = archive(tmp_path, native_resources, prefix="ILCD/")
    assert not validate_ilcd_package(path)
    parsed = load_ilcd_package(path)
    record = parsed.data[0]
    assert record["code"] == PROCESS
    assert record["reference product"] == "Synthetic product"
    assert record["unit"] == "kilogram"
    assert record["exchanges"][0]["amount"] == 100
    assert record["ilcd"]["metadata"]["dataset_version"] == "01.00.000"
    assert "Procédé synthétique" in record["ilcd"]["resources"][f"processes/{PROCESS}.xml"]


def test_native_roundtrip_from_json_snapshot_without_original_archive(tmp_path, native_resources):
    path = archive(tmp_path, native_resources)
    parsed = load_ilcd(path, context=context())
    restored = InventoryDocument(data=json.loads(json.dumps(parsed.data)), context=context())
    output = write_ilcd(restored, tmp_path / "roundtrip.zip")
    second = load_ilcd(output, context=context())
    assert second.data[0]["name"] == parsed.data[0]["name"]
    assert second.data[0]["exchanges"][0]["amount"] == 100
    assert second.data[0]["ilcd"]["process_id"] == PROCESS
    assert not validate_ilcd_package(output)


def test_writer_preserves_nonunit_production_unknown_fields_and_input(tmp_path):
    source = document()
    before = source.data
    first = write_ilcd(source, tmp_path / "first.zip", metadata_profile=profile())
    second = write_ilcd(source, tmp_path / "second.zip", metadata_profile=profile())
    assert first.read_bytes() == second.read_bytes()
    assert source.data == before
    loaded = load_ilcd(first, context=context())
    assert loaded.data[0]["exchanges"][0]["amount"] == 100
    assert loaded.data[0]["custom metadata"] == {"keep": [1, 2]}
    assert loaded.context.background.technosphere.version == "3.10.1"


def test_foreground_links_survive_openlca_dialect_and_snapshot_split(tmp_path):
    producer = dataset()
    consumer = dataset("foreground-2", "Process B", "Product B", 1)
    consumer["exchanges"].append(
        {
            **{k: producer[k] for k in ("name", "reference product", "unit", "location")},
            "type": "technosphere",
            "amount": -2,
        }
    )
    source = document(producer, consumer)
    with pytest.raises(ILCDError, match="selected supplier"):
        render_ilcd_package(source, metadata_profile=profile())
    output = write_ilcd(source, tmp_path / "linked.zip", metadata_profile=profile(), dialect="openlca")
    parsed = load_ilcd(output, context=context(dialect="openlca"))
    consumer = next(d for d in parsed.data if d["name"] == "Process B")
    assert consumer["exchanges"][1]["name"] == "Process A"
    assert consumer["exchanges"][1]["amount"] == -2
    assert not consumer["ilcd"]["issues"]
    single = InventoryDocument(data=[consumer], context=context(dialect="openlca"))
    output = write_ilcd(single, tmp_path / "split.zip")
    assert len(load_ilcd(output, context=context()).data) == 1


@pytest.mark.parametrize(
    "uncertainty",
    [
        {"uncertainty type": 3, "loc": 100, "scale": 2},
        {"uncertainty type": 4, "minimum": 90, "maximum": 110},
        {"uncertainty type": 2, "loc": math.log(100), "scale": math.log(1.5) / 2},
    ],
)
def test_uncertainty_numeric_semantics(tmp_path, uncertainty):
    data = dataset()
    data["exchanges"][0].update(uncertainty)
    path = write_ilcd(document(data), tmp_path / "uncertain.zip", metadata_profile=profile())
    result = load_ilcd(path, context=context()).data[0]["exchanges"][0]
    for key, expected in uncertainty.items():
        assert result[key] == pytest.approx(expected)


def test_supplier_dialect_does_not_override_standard_uncertainty():
    data = dataset(amount=1)
    data["exchanges"][0].update({"uncertainty type": 3, "loc": 1.0, "scale": 0.1})
    rendered = render_ilcd_package(document(data), metadata_profile=profile(), dialect="openlca")
    process = parse_xml(next(value for name, value in rendered.items() if name.startswith("processes/")))
    exchange = process.find("p:exchanges/p:exchange", NS)
    assert exchange.findtext("p:uncertaintyDistributionType", namespaces=NS) == "normal"
    assert exchange.get(q("olca", "amount")) is None
    assert exchange.get(q("olca", "unitId")) is None
    assert exchange.get(q("olca", "propertyId")) is None


@pytest.mark.parametrize("dialect,expected_wire", [("core", "0.2"), ("openlca", "0.1")])
def test_normal_uncertainty_conventions_are_explicit(tmp_path, dialect, expected_wire):
    data = dataset(amount=1)
    data["exchanges"][0].update({"uncertainty type": 3, "loc": 1.0, "scale": 0.1})
    rendered = render_ilcd_package(document(data), metadata_profile=profile(), dialect=dialect)
    process = parse_xml(next(value for name, value in rendered.items() if name.startswith("processes/")))
    assert process.get(q("bp", "dialect")) == dialect
    assert process.findtext("p:exchanges/p:exchange/p:relativeStandardDeviation95In", namespaces=NS) == expected_wire
    parsed = load_ilcd(archive(tmp_path, rendered), context=context())
    assert parsed.context.format.dialect == dialect
    assert parsed.data[0]["exchanges"][0]["scale"] == pytest.approx(0.1)


def test_qualified_target_dialect_is_used_in_preflight_and_writing(tmp_path):
    pipeline = InventoryPipeline(default_adapter_registry(), InMemoryCatalogProvider())
    result = pipeline.write(
        document(),
        tmp_path / "qualified.zip",
        target_format=FormatProfile("ilcd", format_version="1.1", dialect="openlca"),
        adapter_options={"metadata_profile": profile()},
    )
    assert result.succeeded, result.report.to_dict()
    assert load_ilcd(result.value, context=context()).context.format.dialect == "openlca"
    with pytest.raises(ValueError, match="conflict"):
        pipeline.convert(document(), FormatProfile("ilcd", dialect="core"), adapter_options={"dialect": "openlca"})


@pytest.mark.parametrize("dialect,scale", [("core", math.log(1.5) / 2), ("openlca", math.log(1.5))])
def test_log_normal_dialect_conventions_preserve_scale(tmp_path, dialect, scale):
    data = dataset(amount=100)
    data["exchanges"][0].update({"uncertainty type": 2, "loc": math.log(100), "scale": scale})
    rendered = render_ilcd_package(document(data), metadata_profile=profile(), dialect=dialect)
    root = parse_xml(next(value for name, value in rendered.items() if name.startswith("processes/")))
    assert root.findtext("p:exchanges/p:exchange/p:relativeStandardDeviation95In", namespaces=NS) == "1.5"
    parsed = load_ilcd(archive(tmp_path, rendered), context=context())
    assert parsed.data[0]["exchanges"][0]["scale"] == pytest.approx(scale)


def test_unmarked_native_dialect_inference_is_reported(tmp_path):
    data = dataset(amount=1)
    data["exchanges"][0].update({"uncertainty type": 3, "loc": 1, "scale": 0.1})
    rendered = render_ilcd_package(document(data), metadata_profile=profile(), dialect="openlca")
    name = next(name for name in rendered if name.startswith("processes/"))
    root = parse_xml(rendered[name])
    root.attrib.pop(q("bp", "dialect"))
    exchange = root.find("p:exchanges/p:exchange", NS)
    for key, value in {"amount": "1", "unitId": UNIT, "propertyId": PROPERTY}.items():
        exchange.set(q("olca", key), value)
    rendered[name] = etree.tostring(root)
    parsed = load_ilcd(archive(tmp_path, rendered), context=context())
    assert parsed.context.format.dialect == "openlca"
    assert parsed.data[0]["exchanges"][0]["scale"] == pytest.approx(0.1)
    assert parsed.data[0]["ilcd"]["issues"][0]["code"] == "ilcd.dialect_inferred"


def test_formula_and_parameter_dependencies(tmp_path):
    data = dataset(amount=10)
    data["parameters"] = [{"name": "x", "amount": 2}, {"name": "y", "formula": "x * 5", "amount": 10}]
    data["exchanges"][0]["formula"] = "y"
    path = write_ilcd(document(data), tmp_path / "parameters.zip", metadata_profile=profile(), dialect="openlca")
    result = load_ilcd(path, context=context()).data[0]
    assert result["exchanges"][0]["formula"] == "y"
    assert result["exchanges"][0]["amount"] == 10
    assert parameter_values(result["parameters"])["y"] == 10


@pytest.mark.parametrize("formula", ["__import__('os')", "x.real", "[x]", "1e1000", "2 ** 10000000", "1 / 0"])
def test_unsafe_or_unbounded_formulas_are_rejected(formula):
    with pytest.raises(ILCDError):
        evaluate(formula, {"x": 2})


def test_cyclic_and_inconsistent_parameters_fail():
    with pytest.raises(ILCDError, match="cycle"):
        parameter_values([{"name": "x", "formula": "y"}, {"name": "y", "formula": "x"}])
    with pytest.raises(ILCDError, match="Cached"):
        parameter_values([{"name": "x", "formula": "2 + 2", "amount": 5}])


def test_reference_resolution_rejects_ambiguous_versions_and_conflicting_uri(native_resources):
    package = ILCDPackage.from_resources(native_resources)
    owner, process = next(v for k, v in package.entities.items() if k[0] == "Process")
    reference = process.find("p:exchanges/p:exchange/p:referenceToFlowDataSet", NS)
    reference.set("uri", "../flows/wrong.xml")
    with pytest.raises(ILCDError, match="disagrees"):
        package.resolve(reference, owner)
    resources = deepcopy(native_resources)
    resources[f"flows/{FLOW}_v2.xml"] = resources[f"flows/{FLOW}.xml"].replace("01.00.000", "02.00.000")
    package = ILCDPackage.from_resources(resources)
    reference.attrib.pop("version")
    reference.attrib.pop("uri")
    with pytest.raises(ILCDError) as error:
        package.resolve(reference, owner)
    assert error.value.code == "ilcd.reference_ambiguous"


def test_missing_quantity_reference_and_duplicate_entity_fail(tmp_path, native_resources):
    missing = dict(native_resources)
    missing.pop(f"unitgroups/{UNIT}.xml")
    with pytest.raises(ILCDError) as error:
        load_ilcd_package(archive(tmp_path, missing))
    assert error.value.code == "ilcd.reference_missing"
    duplicated = dict(native_resources)
    duplicated["flows/duplicate.xml"] = duplicated[f"flows/{FLOW}.xml"]
    with pytest.raises(ILCDError, match="Duplicate"):
        ILCDPackage.from_resources(duplicated)


def test_schema_validation_and_unsupported_quantitative_reference(tmp_path, native_resources):
    resources = deepcopy(native_resources)
    process = parse_xml(resources[f"processes/{PROCESS}.xml"].encode())
    exchanges = process.find("p:exchanges", NS)
    second = deepcopy(exchanges[0])
    second.set("dataSetInternalID", "8")
    exchanges.append(second)
    reference = etree.SubElement(
        process.find("p:processInformation/p:quantitativeReference", NS), q("p", "referenceToReferenceFlow")
    )
    reference.text = "8"
    resources[f"processes/{PROCESS}.xml"] = etree.tostring(process).decode()
    with pytest.raises(ILCDError) as error:
        load_ilcd_package(archive(tmp_path, resources))
    assert error.value.code == "ilcd.multiple_reference_flows"
    assert validate_ilcd_xml(b"<invalid/>")


@pytest.mark.parametrize("name", ["../process.xml", "/process.xml", "a\\process.xml"])
def test_unsafe_zip_members_are_rejected(tmp_path, name):
    with pytest.raises(ILCDError):
        load_ilcd_package(archive(tmp_path, {name: "<invalid/>"}))


def test_dtd_is_rejected():
    with pytest.raises(ILCDError, match="DTD"):
        parse_xml(b'<!DOCTYPE a [<!ENTITY x "unsafe">]><a>&x;</a>')


def test_failed_export_preserves_destination_and_input(tmp_path):
    source = document()
    before = source.data
    destination = tmp_path / "existing.zip"
    destination.write_bytes(b"unchanged")
    with pytest.raises(ILCDError, match="requires"):
        write_ilcd(source, destination)
    assert destination.read_bytes() == b"unchanged"
    assert source.data == before


def test_registry_pipeline_and_candidate_analysis(tmp_path, native_resources):
    source = archive(tmp_path, native_resources)
    assert infer_source_format(source) == "ilcd"
    pipeline = InventoryPipeline(default_adapter_registry(), InMemoryCatalogProvider())
    parsed = pipeline.read(source, hint=ContextHint(format=FormatProfile("ilcd"), background=context().background))
    assert parsed.succeeded
    assert parsed.value.context.background == context().background
    result = analyze_inventory(path=source, source_context=context(), catalog_provider=InMemoryCatalogProvider())
    assert result.detected_format == "ilcd"
    assert result.candidates[0].name == "Synthetic process"
    assert result.inventory_data[0]["ilcd"]["resources"]
    exported = pipeline.write(parsed.value, tmp_path / "pipeline.zip")
    assert exported.succeeded, exported.report.to_dict()


def test_directory_roundtrip_and_strict_preflight(tmp_path):
    source = document()
    path = write_ilcd(source, tmp_path / "package", metadata_profile=profile())
    assert (
        default_adapter_registry().detect(path, artifact_kind=ArtifactKind.DIRECTORY).detected_format.format_id
        == "ilcd"
    )
    assert len(load_ilcd(path, context=context()).data) == 1
    report = ILCDAdapter().preflight_conversion(source, policy=ConversionPolicy.permissive())
    assert report.has_errors
    assert report.issues[0].code == "ilcd.required_metadata_missing"


def test_schema_manifest_and_offline_load():
    manifest = json.loads((SCHEMA_DIRECTORY / "RESOURCE_MANIFEST.json").read_text())
    for resource in manifest["resources"]:
        assert hashlib.sha256((SCHEMA_DIRECTORY / resource["path"]).read_bytes()).hexdigest() == resource["sha256"]
    assert schema("Process") is not None


@pytest.mark.parametrize("single_slash", [False, True])
def test_schema_file_uris_resolve_offline_with_encoded_paths(tmp_path, monkeypatch, single_slash):
    directory = tmp_path / "schemas with spaces"
    shutil.copytree(SCHEMA_DIRECTORY, directory)
    monkeypatch.setattr(ilcd_package, "SCHEMA_DIRECTORY", directory)
    uri = (directory / "ILCD_ProcessDataSet.xsd").as_uri()
    if single_slash:
        uri = uri.replace("file:///", "file:/", 1)
    parser = etree.XMLParser(resolve_entities=False, no_network=True, load_dtd=False)
    parser.resolvers.add(ilcd_package._SchemaResolver())
    assert etree.XMLSchema(etree.parse(uri, parser)) is not None


@pytest.mark.parametrize(
    "uri",
    [
        "https://example.invalid/schema.xsd",
        "file://remote/schema.xsd",
        "file:///outside/schema.xsd",
        "file:///outside/schema.xsd?query=1",
        "file:///outside/schema.xsd#fragment",
    ],
)
def test_schema_resolver_rejects_external_uris(uri):
    with pytest.raises(OSError, match="External schema resolution is disabled"):
        ilcd_package._SchemaResolver().resolve(uri, None, None)


def test_standalone_process_with_explicit_reference_directory(tmp_path, native_resources):
    for name, text in native_resources.items():
        path = tmp_path / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
    result = load_ilcd_package(tmp_path / f"processes/{PROCESS}.xml", reference_directory=tmp_path)
    assert result.data[0]["exchanges"][0]["amount"] == 100


def test_missing_external_metadata_is_reported_and_retained(tmp_path, native_resources):
    resources = deepcopy(native_resources)
    root = parse_xml(resources[f"processes/{PROCESS}.xml"].encode())
    other = etree.SubElement(root, q("c", "other"))
    reference = etree.SubElement(
        other,
        q("bp", "provenance"),
        type="source data set",
        refObjectId="55555555-5555-4555-8555-555555555555",
        version="01.00.000",
    )
    reference.set("uri", "../sources/external.xml")
    resources[f"processes/{PROCESS}.xml"] = etree.tostring(root).decode()
    parsed = load_ilcd(archive(tmp_path, resources), context=context())
    finding = parsed.data[0]["ilcd"]["issues"][0]
    assert finding["code"] == "ilcd.auxiliary_reference_external"
    assert finding["severity"] == "warning"
    assert not ILCDAdapter().validate_format(parsed).has_errors


def test_external_biosphere_requires_exact_mapping_and_context(tmp_path, native_resources):
    from brightpath.formats.ilcd import _new_flow
    from brightpath.formats.ilcd_references import ILCDReferenceCatalog

    emission_id = "66666666-6666-4666-8666-666666666666"
    resources = _new_flow(
        emission_id, "Carbon dioxide, fossil", "kilogram", "01.00.000", categories=("air", "urban air close to ground")
    )
    row = {
        "identity": {
            "name": "Carbon dioxide, fossil",
            "categories": ["air", "urban air close to ground"],
            "unit": "kilogram",
        },
        "flow_id": emission_id,
        "flow_version": "01.00.000",
    }
    catalog = {
        "schema_version": 1,
        "context": {
            "technosphere": {"family": "ecoinvent", "version": "3.10.1", "system_model": "cutoff"},
            "biosphere": {"family": "ecoinvent", "version": "3.10.1"},
        },
        "biosphere": [row],
        "resources": resources,
    }
    data = dataset()
    data["exchanges"].append({**row["identity"], "type": "biosphere", "amount": 0.5})
    with pytest.raises(ILCDError, match="No exact"):
        render_ilcd_package(document(data), metadata_profile=profile())
    path = write_ilcd(document(data), tmp_path / "emission.zip", metadata_profile=profile(), reference_catalog=catalog)
    loaded = load_ilcd(path, context=context())
    assert loaded.data[0]["exchanges"][1]["amount"] == 0.5
    assert tuple(loaded.data[0]["exchanges"][1]["categories"]) == ("air", "urban air close to ground")
    bad = deepcopy(catalog)
    bad["context"]["biosphere"]["version"] = "3.10"
    with pytest.raises(ILCDError, match="exact background"):
        render_ilcd_package(document(data), metadata_profile=profile(), reference_catalog=bad)
    duplicated = deepcopy(catalog)
    duplicated["biosphere"].append(row)
    with pytest.raises(ILCDError, match="Duplicate"):
        ILCDReferenceCatalog.from_mapping(duplicated)


def test_cross_format_conversion_preserves_background_and_inventory(tmp_path, native_resources):
    source = load_ilcd(archive(tmp_path, native_resources), context=context())
    pipeline = InventoryPipeline(default_adapter_registry(), InMemoryCatalogProvider())
    result = pipeline.write(source, tmp_path / "brightway.xlsx", target_format="brightway_excel")
    assert result.succeeded, result.report.to_dict()
    from brightpath.formats.brightway_excel import load_brightway_excel

    restored = load_brightway_excel(result.value, context=context("brightway_excel"))
    assert restored.context.background == source.context.background
    assert restored.data[0]["exchanges"][0]["amount"] == 100


def test_cli_dry_run_uses_the_ilcd_renderer(tmp_path, capsys):
    from brightpath import cli
    from brightpath.formats.brightway_excel import write_brightway_excel

    source = write_brightway_excel(document(), tmp_path / "source.xlsx")
    capsys.readouterr()
    metadata = tmp_path / "metadata.json"
    metadata.write_text(json.dumps(profile()))
    target = tmp_path / "target.zip"
    args = [
        "convert-format",
        str(source),
        str(target),
        "--target-format",
        "ilcd",
        "--ilcd-metadata",
        str(metadata),
        "--dry-run",
        "--json",
        "--source-technosphere-family",
        "ecoinvent",
        "--source-technosphere-version",
        "3.10.1",
        "--source-technosphere-system-model",
        "cutoff",
        "--source-biosphere-family",
        "ecoinvent",
        "--source-biosphere-version",
        "3.10.1",
    ]
    assert cli.main(args) == cli.EXIT_SUCCESS
    assert json.loads(capsys.readouterr().out)["success"]
    assert not target.exists()


def test_log_normal_precision_loss_is_explicit():
    data = dataset()
    data["exchanges"][0].update({"uncertainty type": 2, "loc": math.log(100), "scale": 0.2})
    report = ILCDAdapter().preflight_conversion(
        document(data), policy=ConversionPolicy.permissive(), metadata_profile=profile()
    )
    assert report.has_errors
    assert report.issues[0].code == "ilcd.uncertainty_precision_unsupported"


def test_stale_review_claims_block_inventory_edits(tmp_path, native_resources):
    resources = deepcopy(native_resources)
    root = parse_xml(resources[f"processes/{PROCESS}.xml"].encode())
    review = etree.SubElement(
        etree.SubElement(root.find("p:modellingAndValidation", NS), q("p", "validation")), q("p", "review")
    )
    review.set("type", "Dependent internal review")
    resources[f"processes/{PROCESS}.xml"] = etree.tostring(root).decode()
    parsed = load_ilcd(archive(tmp_path, resources), context=context())
    changed = parsed.data
    changed[0]["exchanges"][0]["amount"] = 200
    with pytest.raises(ILCDError) as error:
        render_ilcd_package(parsed.replace(data=changed))
    assert error.value.code == "ilcd.stale_claims"
    assert parsed.data[0]["exchanges"][0]["amount"] == 100


def test_source_format_validation_checks_preserved_unsupported_features(tmp_path, native_resources):
    parsed = load_ilcd(archive(tmp_path, native_resources), context=context())
    changed = parsed.data
    stored = changed[0]["ilcd"]
    root = parse_xml(stored["resources"][stored["process_path"]].encode())
    etree.SubElement(root.find("p:exchanges/p:exchange", NS), q("p", "allocations"))
    stored["resources"][stored["process_path"]] = etree.tostring(root).decode()
    report = ILCDAdapter().validate_format(parsed.replace(data=changed))
    assert report.has_errors
    assert report.issues[0].code == "ilcd.allocation_unsupported"


def test_missing_references_have_read_reports(tmp_path, native_resources):
    from brightpath.core.reports import OperationKind, StageKind

    resources = dict(native_resources)
    resources.pop(f"flows/{FLOW}.xml")
    with pytest.raises(ILCDError) as error:
        load_ilcd_package(archive(tmp_path, resources))
    assert error.value.report.operation == OperationKind.READ
    assert error.value.report.stages[0].stage == StageKind.PARSE
    assert json.loads(error.value.report.to_json())["operation"] == "read"


def test_reference_catalog_normalizes_legacy_uvek_alias_without_mutation():
    from brightpath.formats.ilcd_references import ILCDReferenceCatalog

    original = {
        "context": {
            "technosphere": {"family": "BAFU", "version": "2025.0", "system_model": "cut-off"},
            "biosphere": {"family": "ecoinvent", "version": "3.10.1"},
        }
    }
    before = deepcopy(original)
    catalog = ILCDReferenceCatalog.from_mapping(original)
    assert catalog.context["technosphere"] == {"family": "uvek", "version": "2025", "system_model": "cutoff"}
    assert catalog.context["biosphere"]["version"] == "3.10.1"
    assert original == before


def test_explicit_license_metadata_is_preserved(tmp_path):
    metadata = profile()
    metadata["defaults"].update({"license_type": "Other", "access_restrictions": "CC-BY-4.0", "copyright": True})
    path = write_ilcd(document(), tmp_path / "licensed.zip", metadata_profile=metadata)
    result = load_ilcd(path, context=context()).data[0]
    assert result["ilcd"]["metadata"]["access_restrictions"] == "CC-BY-4.0"
    assert result["ilcd"]["metadata"]["copyright"] is True


def test_unknown_metadata_and_fractional_years_are_rejected():
    metadata = profile()
    metadata["defaults"]["invented_author"] = "Not a supported structured field"
    with pytest.raises(ILCDError, match="Unsupported ILCD metadata"):
        render_ilcd_package(document(), metadata_profile=metadata)
    metadata = profile()
    metadata["defaults"]["reference_year"] = 2020.5
    with pytest.raises(ILCDError, match="integers"):
        render_ilcd_package(document(), metadata_profile=metadata)


def test_biosphere_direction_changes_transform_bounds_and_formula(tmp_path):
    from brightpath.formats.ilcd import _new_flow

    emission_id = "66666666-6666-4666-8666-666666666666"
    mapping = {"name": "Carbon dioxide, fossil", "categories": ["air", "urban air close to ground"], "unit": "kilogram"}
    catalog = {
        "context": {
            "technosphere": {"family": "ecoinvent", "version": "3.10.1", "system_model": "cutoff"},
            "biosphere": {"family": "ecoinvent", "version": "3.10.1"},
        },
        "biosphere": [{"identity": mapping, "flow_id": emission_id, "flow_version": "01.00.000"}],
        "resources": _new_flow(emission_id, mapping["name"], "kilogram", "01.00.000", categories=mapping["categories"]),
    }
    data = dataset()
    data["parameters"] = [{"name": "dose", "amount": 0.5}]
    data["exchanges"].append(
        {
            **mapping,
            "type": "biosphere",
            "amount": 0.5,
            "formula": "dose",
            "uncertainty type": 4,
            "minimum": 0,
            "maximum": 1,
        }
    )
    resources = render_ilcd_package(document(data), metadata_profile=profile(), reference_catalog=catalog)
    process_name = next(name for name in resources if name.startswith("processes/"))
    root = parse_xml(resources[process_name])
    root.findall("p:exchanges/p:exchange", NS)[1].find("p:exchangeDirection", NS).text = "Input"
    resources[process_name] = etree.tostring(root)
    parsed = load_ilcd(archive(tmp_path, resources), context=context()).data[0]
    emission = parsed["exchanges"][1]
    assert emission["amount"] == -0.5
    assert (emission["minimum"], emission["maximum"]) == (-1, 0)
    assert evaluate(emission["formula"], parameter_values(parsed["parameters"])) == -0.5
