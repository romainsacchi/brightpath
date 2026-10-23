from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone

import pytest
from lxml import etree

from brightpath import cli
from brightpath.adapters import ArtifactKind, EcoSpold1Adapter, default_adapter_registry
from brightpath.analysis import analyze_inventory, infer_source_format
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
from brightpath.formats.ecospold1 import (
    ECOSPOLD1_NAMESPACE,
    load_ecospold1,
    render_ecospold1,
    validate_ecospold1_xml,
    write_ecospold1,
)
from brightpath.formats.ecospold1_metadata import EcoSpold1ExportProfile
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
        "comment": "This narrative mentions Smith (2020), but is not parsed as metadata.",
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
                "type": "technosphere",
                "name": "market for input",
                "reference product": "input",
                "location": "GLO",
                "unit": "kilogram",
                "amount": 2.0,
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
        "classification": {"category": "Materials", "subcategory": "Synthetic"},
        "time_period": {
            "start_date": "2020-01-01",
            "end_date": "2020-12-31",
            "data_valid_for_entire_period": True,
        },
        "source": {
            "first_author": "Smith J.",
            "year": 2020,
            "title": "Synthetic inventory source",
            "place_of_publication": "Zurich",
        },
        "data_entry_person": {
            "name": "Jane Smith",
            "address": "Example Institute",
            "company_code": "EXAMPLE",
            "country_code": "CH",
        },
        "data_generator_person": {
            "name": "Jane Smith",
            "address": "Example Institute",
            "company_code": "EXAMPLE",
            "country_code": "CH",
        },
        "copyright": False,
    }
    defaults.update(default_overrides)
    return {"schema_version": 1, "defaults": defaults}


def test_public_analyzer_dispatches_ecospold1(tmp_path):
    dataset = _dataset()
    dataset["exchanges"] = dataset["exchanges"][:1]
    path = write_ecospold1(
        _document(dataset),
        tmp_path / "inventory.xml",
        metadata_profile=_profile(),
        generated_at="2024-01-01T00:00:00+00:00",
    )

    assert infer_source_format(path) == "ecospold1"
    result = analyze_inventory(path=path, source_context=_context("ecospold1"))

    assert result.detected_software == "ecospold"
    assert result.detected_format == "ecospold1"
    assert len(result.candidates) == 1
    assert result.candidates[0].name == dataset["name"]
    issues = [*result.file_issues, *(issue for candidate in result.candidates for issue in candidate.issues)]
    assert not [issue for issue in issues if issue.severity == "error"]


def test_render_is_xsd_valid_and_uses_correct_exchange_groups():
    contents = render_ecospold1(
        _document(),
        metadata_profile=_profile(),
        generated_at=datetime(2024, 1, 1, tzinfo=timezone.utc),
    )

    assert validate_ecospold1_xml(contents) == ()
    root = etree.fromstring(contents)
    namespace = {"es": ECOSPOLD1_NAMESPACE}
    groups = [
        (
            exchange.get("name"),
            exchange.findtext("es:inputGroup", namespaces=namespace),
            exchange.findtext("es:outputGroup", namespaces=namespace),
        )
        for exchange in root.findall(".//es:exchange", namespaces=namespace)
    ]
    assert groups == [
        ("synthetic product", None, "0"),
        ("market for input", "5", None),
        ("Water", "4", None),
        ("Carbon dioxide, fossil", None, "4"),
    ]


def test_write_read_roundtrip_preserves_structured_provenance(tmp_path):
    source = _document()
    path = write_ecospold1(
        source,
        tmp_path / "inventory.xml",
        metadata_profile=_profile(),
        generated_at="2024-01-01T00:00:00+00:00",
    )

    loaded = load_ecospold1(
        path,
        background_profile=BackgroundProfile("ecoinvent", "3.10", "cutoff"),
        biosphere_profile=BiosphereProfile("ecoinvent", "3.10"),
    )

    assert loaded.context.format.format_id == "ecospold1"
    assert loaded.data[0]["reference product"] == "synthetic product"
    assert loaded.data[0]["ecospold1"]["sources"][0]["first_author"] == "Smith J."
    assert loaded.data[0]["ecospold1"]["data_entry_person"]["address"] == "Example Institute"
    assert loaded.data[0]["comment"] == source.data[0]["comment"]


def test_free_form_description_never_satisfies_required_metadata():
    adapter = EcoSpold1Adapter()
    report = adapter.preflight_conversion(_document(), policy=ConversionPolicy.permissive())

    assert report.has_errors
    assert {issue.code for issue in report.issues} == {"ecospold1.required_metadata_missing"}
    assert any(issue.path.endswith("sources") for issue in report.issues)


def test_dataset_override_wins_over_preserved_metadata_and_defaults():
    dataset = _dataset(ecospold1=_profile()["defaults"])
    profile = deepcopy(_profile())
    profile["defaults"]["source"]["title"] = "Default title"
    profile["datasets"] = [
        {
            "selector": {"code": "foreground-1"},
            "metadata": {"source": {"title": "Explicit override title"}},
        }
    ]

    resolved = EcoSpold1ExportProfile.from_mapping(profile).metadata_for(dataset)

    assert resolved.sources[0].title == "Explicit override title"
    assert resolved.sources[0].first_author == "Smith J."


def test_multiple_single_output_datasets_are_supported():
    second = _dataset(name="second process", code="foreground-2")
    contents = render_ecospold1(
        _document(_dataset(), second),
        metadata_profile=_profile(),
        generated_at="2024-01-01T00:00:00+00:00",
    )

    root = etree.fromstring(contents)
    assert len(root.findall(f"{{{ECOSPOLD1_NAMESPACE}}}dataset")) == 2


def test_multi_output_and_allocation_metadata_are_hard_errors():
    dataset = _dataset()
    dataset["exchanges"].append(
        {
            "type": "production",
            "name": "co-product",
            "unit": "kilogram",
            "amount": 0.2,
        }
    )
    dataset["ecospold1"] = {**_profile()["defaults"], "raw": {"allocation_count": 1}}

    report = EcoSpold1Adapter().validate_format(_document(dataset, format_id="ecospold1"))

    assert report.has_errors
    assert {issue.code for issue in report.issues} >= {
        "ecospold1.multiple_outputs_unsupported",
        "ecospold1.allocation_unsupported",
    }


def test_pipeline_dry_run_and_write_use_the_same_metadata_profile(tmp_path):
    pipeline = InventoryPipeline(default_adapter_registry(), InMemoryCatalogProvider())
    options = {
        "metadata_profile": _profile(),
        "generated_at": datetime(2024, 1, 1, tzinfo=timezone.utc),
    }

    conversion = pipeline.convert(_document(), "ecospold1", adapter_options=options)
    written = pipeline.write(
        _document(),
        tmp_path / "inventory.xml",
        target_format="ecospold1",
        adapter_options=options,
    )

    assert conversion.succeeded
    assert written.succeeded
    assert written.value is not None
    assert validate_ecospold1_xml(written.value) == ()


def test_adapter_detects_content_without_relying_on_suffix(tmp_path):
    path = write_ecospold1(
        _document(),
        tmp_path / "inventory.xml",
        metadata_profile=_profile(),
        generated_at="2024-01-01T00:00:00+00:00",
    )
    disguised = path.rename(tmp_path / "inventory.data")
    unrelated = tmp_path / "not-ecospold.xml"
    unrelated.write_text("<ecoSpold xmlns='http://example.com'/>", encoding="utf-8")

    adapter = EcoSpold1Adapter()
    assert adapter.detect(disguised, artifact_kind=ArtifactKind.FILE) is not None
    assert adapter.detect(unrelated, artifact_kind=ArtifactKind.FILE) is None


def test_safe_parser_does_not_expand_external_entities(tmp_path):
    payload = b"""<?xml version='1.0'?>
<!DOCTYPE ecoSpold [<!ENTITY secret SYSTEM 'file:///etc/passwd'>]>
<ecoSpold xmlns='http://www.EcoInvent.org/EcoSpold01' validationStatus='&secret;'/>
"""
    path = tmp_path / "entity.xml"
    path.write_bytes(payload)

    errors = validate_ecospold1_xml(path)

    assert errors
    assert all("root:" not in error for error in errors)


def test_pipeline_reads_ecospold1_with_explicit_background_hint(tmp_path):
    path = write_ecospold1(
        _document(),
        tmp_path / "inventory.xml",
        metadata_profile=_profile(),
        generated_at="2024-01-01T00:00:00+00:00",
    )
    pipeline = InventoryPipeline(default_adapter_registry(), InMemoryCatalogProvider())

    result = pipeline.read(
        path,
        hint=ContextHint(format=FormatProfile("ecospold1"), background=_context().background),
    )

    assert result.succeeded
    assert result.value is not None
    assert result.value.data[0]["name"] == "synthetic process"


def test_pipeline_converts_imported_single_output_ecospold1_to_brightway(tmp_path):
    path = write_ecospold1(
        _document(),
        tmp_path / "inventory.xml",
        metadata_profile=_profile(),
        generated_at="2024-01-01T00:00:00+00:00",
    )
    loaded = load_ecospold1(
        path,
        background_profile=BackgroundProfile("ecoinvent", "3.10", "cutoff"),
        biosphere_profile=BiosphereProfile("ecoinvent", "3.10"),
    )
    pipeline = InventoryPipeline(default_adapter_registry(), InMemoryCatalogProvider())

    converted = pipeline.convert(loaded, "brightway_excel")

    assert converted.succeeded
    assert converted.value is not None
    assert converted.value.context.format.format_id == "brightway_excel"
    assert converted.value.data[0]["ecospold1"]["sources"][0]["title"] == "Synthetic inventory source"


def test_cli_exports_with_structured_metadata_profile(tmp_path, monkeypatch, capsys):
    registry = default_adapter_registry()
    source = registry.get("brightway_excel").write(_document(), tmp_path / "source.xlsx")
    destination = tmp_path / "target.xml"
    metadata = tmp_path / "metadata.yaml"
    metadata.write_text(
        """schema_version: 1
defaults:
  classification:
    category: Materials
    subcategory: Synthetic
  time_period:
    start_date: 2020-01-01
    end_date: 2020-12-31
  source:
    first_author: Smith J.
    year: 2020
    title: Synthetic inventory source
    place_of_publication: Zurich
  data_entry_person:
    name: Jane Smith
    address: Example Institute
    company_code: EXAMPLE
    country_code: CH
  data_generator_person:
    name: Jane Smith
    address: Example Institute
    company_code: EXAMPLE
    country_code: CH
  copyright: false
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
            "ecospold1",
            "--ecospold1-metadata",
            str(metadata),
            "--json",
        ]
    )

    assert exit_code == cli.EXIT_SUCCESS, capsys.readouterr().out
    assert destination.is_file()
    assert validate_ecospold1_xml(destination) == ()


def test_metadata_profile_file_is_schema_validated(tmp_path):
    path = tmp_path / "metadata.yaml"
    path.write_text("schema_version: 2\ndefaults: {}\n", encoding="utf-8")

    with pytest.raises(ValueError, match="schema_version"):
        EcoSpold1ExportProfile.from_file(path)


def test_metadata_profile_accepts_unquoted_yaml_dates(tmp_path):
    path = tmp_path / "metadata.yaml"
    path.write_text(
        "schema_version: 1\n"
        "defaults:\n"
        "  time_period:\n"
        "    start_date: 2020-01-01\n"
        "    end_date: 2020-12-31\n",
        encoding="utf-8",
    )

    profile = EcoSpold1ExportProfile.from_file(path)

    assert profile.defaults["time_period"]["start_date"] == "2020-01-01"
