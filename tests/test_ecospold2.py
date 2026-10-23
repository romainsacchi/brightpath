from __future__ import annotations

import zipfile
from copy import deepcopy
from datetime import datetime, timezone

import pytest
from lxml import etree

from brightpath import cli
from brightpath.adapters import ArtifactKind, EcoSpold2Adapter, default_adapter_registry
from brightpath.background import InMemoryCatalogProvider
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
from brightpath.formats.ecospold2 import (
    ECOSPOLD2_NAMESPACE,
    load_ecospold2,
    render_ecospold2_package,
    validate_ecospold2_package,
    validate_ecospold2_xml,
    write_ecospold2,
)
from brightpath.formats.ecospold2_metadata import EcoSpold2ExportProfile, EcoSpold2ReferenceCatalog
from brightpath.models import BackgroundProfile, InventoryDocument


def _context(format_id: str = "brightway_excel") -> InventoryContext:
    return InventoryContext(
        format=FormatProfile(format_id),
        background=BackgroundContext(
            technosphere=TechnosphereProfile("ecoinvent", "3.10", "cutoff"),
            biosphere=BiosphereProfile("ecoinvent", "3.10"),
        ),
    )


def _dataset(**overrides):
    value = {
        "name": "synthetic process",
        "reference product": "synthetic product",
        "location": "CH",
        "unit": "kilogram",
        "comment": "This narrative mentions an author, but is not parsed as metadata.",
        "code": "foreground-1",
        "exchanges": [
            {
                "type": "production",
                "name": "synthetic product",
                "reference product": "synthetic product",
                "location": "CH",
                "unit": "kilogram",
                "amount": 1.0,
            },
            {
                "type": "biosphere",
                "name": "Water",
                "categories": ("natural resource", "in water"),
                "unit": "cubic meter",
                "amount": 0.5,
            },
            {
                "type": "biosphere",
                "name": "Carbon dioxide, fossil",
                "categories": ("air", "urban air close to ground"),
                "unit": "kilogram",
                "amount": 0.25,
            },
        ],
    }
    value.update(overrides)
    return value


def _document(*datasets, format_id: str = "brightway_excel") -> InventoryDocument:
    return InventoryDocument(
        data=list(datasets or (_dataset(),)), context=_context(format_id), database_name="synthetic"
    )


def _profile(**default_overrides) -> dict:
    defaults = {
        "time_period": {
            "start_date": "2020-01-01",
            "end_date": "2020-12-31",
            "data_valid_for_entire_period": True,
        },
        "macroeconomic_scenario": {
            "name": "Business as usual",
            "id": "11111111-1111-4111-8111-111111111111",
        },
        "data_entry_person": {
            "name": "Jane Smith",
            "email": "jane@example.org",
            "id": "22222222-2222-4222-8222-222222222222",
        },
        "data_generator_person": {
            "name": "Jane Smith",
            "email": "jane@example.org",
            "id": "22222222-2222-4222-8222-222222222222",
        },
        "copyright": False,
        "data_published_in": 2,
        "published_source_id": "55555555-5555-4555-8555-555555555555",
        "published_source_year": "2020",
        "published_source_first_author": "Smith J.",
    }
    defaults.update(default_overrides)
    return {"schema_version": 1, "defaults": defaults}


def _references() -> dict:
    return {
        "schema_version": 1,
        "biosphere": [
            {
                "identity": {
                    "name": "Water",
                    "categories": ["natural resource", "in water"],
                    "unit": "cubic meter",
                },
                "flow_id": "33333333-3333-4333-8333-333333333333",
            },
            {
                "identity": {
                    "name": "Carbon dioxide, fossil",
                    "categories": ["air", "urban air close to ground"],
                    "unit": "kilogram",
                },
                "flow_id": "44444444-4444-4444-8444-444444444444",
            },
        ],
    }


def test_rendered_package_is_xsd_valid_and_uses_correct_exchange_groups():
    package = render_ecospold2_package(
        _document(),
        metadata_profile=_profile(),
        reference_catalog=_references(),
        generated_at=datetime(2024, 1, 1, tzinfo=timezone.utc),
    )

    datasets = [contents for name, contents in package.items() if name.endswith(".spold")]
    assert len(datasets) == 1
    assert validate_ecospold2_xml(datasets[0]) == ()
    root = etree.fromstring(datasets[0])
    namespace = {"es": ECOSPOLD2_NAMESPACE}
    groups = [
        (
            exchange.findtext("es:name", namespaces=namespace),
            exchange.findtext("es:inputGroup", namespaces=namespace),
            exchange.findtext("es:outputGroup", namespaces=namespace),
        )
        for exchange in root.findall(".//es:intermediateExchange", namespaces=namespace)
        + root.findall(".//es:elementaryExchange", namespaces=namespace)
    ]
    assert groups == [
        ("synthetic product", None, "0"),
        ("Water", "4", None),
        ("Carbon dioxide, fossil", None, "4"),
    ]
    assert "MasterData/IntermediateExchanges.xml" in package


def test_zip_roundtrip_preserves_metadata_parameters_formulas_and_uncertainty(tmp_path):
    dataset = _dataset()
    dataset["parameters"] = [
        {"name": "distance", "description": "Transport distance", "unit": "kilometer", "amount": 12.0}
    ]
    dataset["exchanges"][2].update(
        {
            "formula": "distance / 48",
            "variable name": "co2",
            "uncertainty type": 3,
            "loc": 0.25,
            "scale": 0.1,
            "ecospold2": {
                "source_id": "66666666-6666-4666-8666-666666666666",
                "source_year": "2019",
                "source_first_author": "Miller A.",
                "page_numbers": "12-14",
            },
        }
    )
    path = write_ecospold2(
        _document(dataset),
        tmp_path / "inventory.zip",
        metadata_profile=_profile(),
        reference_catalog=_references(),
        generated_at="2024-01-01T00:00:00+00:00",
    )

    loaded = load_ecospold2(path, background_profile=BackgroundProfile("ecoinvent", "3.10", "cutoff"))

    assert validate_ecospold2_package(path) == ()
    assert loaded.context.format.format_id == "ecospold2"
    assert loaded.data[0]["reference product"] == "synthetic product"
    assert loaded.data[0]["ecospold2"]["data_entry_person"]["email"] == "jane@example.org"
    assert loaded.data[0]["ecospold2"]["published_source_first_author"] == "Smith J."
    assert loaded.data[0]["parameters"][0]["name"] == "distance"
    emission = next(exchange for exchange in loaded.data[0]["exchanges"] if exchange["name"].startswith("Carbon"))
    assert emission["formula"] == "distance / 48"
    assert emission["uncertainty type"] == 3
    assert emission["ecospold2"]["source_first_author"] == "Miller A."


def test_rendering_is_deterministic_and_does_not_mutate_source():
    document = _document()
    before = document.data
    options = {
        "metadata_profile": _profile(),
        "reference_catalog": _references(),
        "generated_at": "2024-01-01T00:00:00+00:00",
    }

    first = render_ecospold2_package(document, **options)
    second = render_ecospold2_package(document, **options)

    assert first == second
    assert document.data == before


def test_description_never_satisfies_required_metadata():
    report = EcoSpold2Adapter().preflight_conversion(
        _document(),
        policy=ConversionPolicy.permissive(),
        reference_catalog=_references(),
    )

    assert report.has_errors
    assert {issue.code for issue in report.issues} == {"ecospold2.required_metadata_missing"}
    assert any(issue.path.endswith("data_entry_person.name") for issue in report.issues)


def test_unresolved_external_uuid_is_a_hard_error():
    report = EcoSpold2Adapter().preflight_conversion(
        _document(),
        policy=ConversionPolicy.permissive(),
        metadata_profile=_profile(),
    )

    assert report.has_errors
    assert {issue.code for issue in report.issues} == {"ecospold2.package_unrepresentable"}
    assert "elementary-flow UUID" in report.issues[0].message


def test_packaged_uuid_catalog_is_selected_from_document_context():
    dataset = _dataset()
    dataset["exchanges"] = [dataset["exchanges"][0], dataset["exchanges"][2]]

    package = render_ecospold2_package(
        _document(dataset),
        metadata_profile=_profile(),
        generated_at="2024-01-01T00:00:00+00:00",
    )

    contents = next(value for name, value in package.items() if name.endswith(".spold"))
    root = etree.fromstring(contents)
    exchange = root.find(".//es:elementaryExchange", namespaces={"es": ECOSPOLD2_NAMESPACE})
    assert exchange is not None
    assert exchange.get("elementaryExchangeId") == "f9749677-9c9f-4678-ab55-c607dfdc2cb9"


def test_internal_foreground_links_do_not_require_reference_catalog():
    provider = _dataset(name="provider", code="provider", comment="")
    provider["reference product"] = "input"
    provider["location"] = "GLO"
    provider["exchanges"] = [
        {
            "type": "production",
            "name": "input",
            "reference product": "input",
            "location": "GLO",
            "unit": "kilogram",
            "amount": 1.0,
        }
    ]
    consumer = deepcopy(provider)
    consumer.update(
        {
            "name": "consumer",
            "reference product": "output",
            "location": "CH",
            "code": "consumer",
        }
    )
    consumer["exchanges"] = [
        {
            "type": "production",
            "name": "output",
            "reference product": "output",
            "location": "CH",
            "unit": "kilogram",
            "amount": 1.0,
        },
        {
            "type": "technosphere",
            "name": "provider",
            "reference product": "input",
            "location": "GLO",
            "unit": "kilogram",
            "amount": 2.0,
        },
    ]

    package = render_ecospold2_package(_document(provider, consumer), metadata_profile=_profile())

    assert len([name for name in package if name.endswith(".spold")]) == 2


def test_multi_output_allocation_and_transfer_coefficients_are_hard_errors():
    dataset = _dataset()
    dataset["exchanges"].append({"type": "production", "name": "co-product", "unit": "kilogram", "amount": 0.2})
    dataset["ecospold2"] = {
        **_profile()["defaults"],
        "raw": {"allocation_count": 1, "transfer_coefficient_count": 1},
    }

    report = EcoSpold2Adapter().validate_format(_document(dataset, format_id="ecospold2"))

    assert report.has_errors
    assert {issue.code for issue in report.issues} >= {
        "ecospold2.multiple_outputs_unsupported",
        "ecospold2.allocation_unsupported",
        "ecospold2.transfer_coefficients_unsupported",
    }


def test_pipeline_writes_zip_and_reads_package_directory(tmp_path):
    pipeline = InventoryPipeline(default_adapter_registry(), InMemoryCatalogProvider())
    options = {
        "metadata_profile": _profile(),
        "reference_catalog": _references(),
        "generated_at": datetime(2024, 1, 1, tzinfo=timezone.utc),
    }
    written = pipeline.write(
        _document(),
        tmp_path / "inventory.zip",
        target_format="ecospold2",
        adapter_options=options,
    )
    directory = write_ecospold2(
        _document(),
        tmp_path / "package",
        package_mode="directory",
        **options,
    )
    read = pipeline.read(
        directory,
        artifact_kind=ArtifactKind.DIRECTORY,
        hint=ContextHint(format=FormatProfile("ecospold2"), background=_context().background),
    )

    assert written.succeeded
    assert written.value is not None
    assert validate_ecospold2_package(written.value) == ()
    assert read.succeeded
    assert read.value is not None
    assert read.value.data[0]["name"] == "synthetic process"


def test_adapter_detects_disguised_zip_and_directory(tmp_path):
    archive = write_ecospold2(
        _document(),
        tmp_path / "inventory.zip",
        metadata_profile=_profile(),
        reference_catalog=_references(),
    ).rename(tmp_path / "inventory.data")
    directory = write_ecospold2(
        _document(),
        tmp_path / "package",
        metadata_profile=_profile(),
        reference_catalog=_references(),
        package_mode="directory",
    )
    adapter = EcoSpold2Adapter()

    assert adapter.detect(archive, artifact_kind=ArtifactKind.FILE) is not None
    assert adapter.detect(directory, artifact_kind=ArtifactKind.DIRECTORY) is not None


def test_zip_path_traversal_is_rejected(tmp_path):
    path = tmp_path / "unsafe.zip"
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr("../datasets/activity.spold", b"<ecoSpold/>")

    errors = validate_ecospold2_package(path)

    assert errors
    assert "Unsafe ZIP member path" in errors[0]


def test_bw2io_importer_writes_generated_package_to_isolated_project(tmp_path):
    bw2io = pytest.importorskip("bw2io")
    bw2data = pytest.importorskip("bw2data")
    dataset = _dataset()
    dataset["exchanges"] = dataset["exchanges"][:1]
    package = write_ecospold2(
        _document(dataset),
        tmp_path / "package",
        metadata_profile=_profile(),
        package_mode="directory",
    )
    data_dir = tmp_path / "brightway-data"
    logs_dir = tmp_path / "brightway-logs"
    data_dir.mkdir()
    logs_dir.mkdir()
    projects = bw2data.projects
    previous = (projects._base_data_dir, projects._base_logs_dir, projects.current)
    try:
        projects.change_base_directories(data_dir, logs_dir, project_name="ecospold2-integration")
        importer = bw2io.SingleOutputEcospold2Importer(
            str(package / "datasets"),
            "foreground",
            use_mp=False,
            add_product_information=True,
        )
        importer.apply_strategies()
        assert importer.statistics() == (1, 1, 0, 0)
        importer.write_database()
        database = bw2data.Database("foreground")
        assert len(database) == 1
        assert next(iter(database))["name"] == "synthetic process"
    finally:
        projects.change_base_directories(previous[0], previous[1], project_name=previous[2])


def test_cli_exports_with_structured_metadata_and_reference_profiles(tmp_path, monkeypatch, capsys):
    registry = default_adapter_registry()
    source = registry.get("brightway_excel").write(_document(), tmp_path / "source.xlsx")
    destination = tmp_path / "target.zip"
    metadata = tmp_path / "metadata.yaml"
    references = tmp_path / "references.yaml"
    metadata.write_text(
        """schema_version: 1
defaults:
  time_period:
    start_date: 2020-01-01
    end_date: 2020-12-31
  macroeconomic_scenario:
    name: Business as usual
  data_entry_person:
    name: Jane Smith
    email: jane@example.org
  data_generator_person:
    name: Jane Smith
    email: jane@example.org
""",
        encoding="utf-8",
    )
    references.write_text(
        """schema_version: 1
biosphere:
  - identity:
      name: Water
      categories: [natural resource, in water]
      unit: cubic meter
    flow_id: 33333333-3333-4333-8333-333333333333
  - identity:
      name: Carbon dioxide, fossil
      categories: [air, urban air close to ground]
      unit: kilogram
    flow_id: 44444444-4444-4444-8444-444444444444
""",
        encoding="utf-8",
    )
    provider = InMemoryCatalogProvider()
    pipeline = InventoryPipeline(registry, provider)
    monkeypatch.setattr(cli, "_application_services", lambda: (pipeline, provider))

    exit_code = cli.main(
        [
            "convert-format",
            str(source),
            str(destination),
            "--source-format",
            "brightway_excel",
            "--source-technosphere-family",
            "ecoinvent",
            "--source-technosphere-version",
            "3.10",
            "--source-technosphere-system-model",
            "cutoff",
            "--source-biosphere-family",
            "ecoinvent",
            "--source-biosphere-version",
            "3.10",
            "--target-format",
            "ecospold2",
            "--ecospold2-metadata",
            str(metadata),
            "--ecospold2-references",
            str(references),
            "--json",
        ]
    )

    assert exit_code == cli.EXIT_SUCCESS, capsys.readouterr().out
    assert destination.is_file()
    assert validate_ecospold2_package(destination) == ()


def test_metadata_and_reference_files_are_schema_validated(tmp_path):
    metadata = tmp_path / "metadata.yaml"
    references = tmp_path / "references.yaml"
    metadata.write_text("schema_version: 2\ndefaults: {}\n", encoding="utf-8")
    references.write_text("schema_version: 2\n", encoding="utf-8")

    with pytest.raises(ValueError, match="schema_version"):
        EcoSpold2ExportProfile.from_file(metadata)
    with pytest.raises(ValueError, match="schema_version"):
        EcoSpold2ReferenceCatalog.from_file(references)
