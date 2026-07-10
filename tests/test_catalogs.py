import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from brightpath.background import (
    BiosphereCatalog,
    CatalogIntegrityError,
    CatalogProvider,
    TechnosphereCatalog,
)
from brightpath.catalogs import (
    available_catalog_profiles,
    collect_biosphere_catalog_entries,
    collect_technosphere_catalog_entries,
    load_background_catalog,
    write_background_catalog,
)
from brightpath.core import BiosphereProfile, TechnosphereProfile
from brightpath.models import BackgroundProfile

SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "generate_reference_catalogs.py"
SCRIPT_SPEC = importlib.util.spec_from_file_location("generate_reference_catalogs", SCRIPT_PATH)
SCRIPT_MODULE = importlib.util.module_from_spec(SCRIPT_SPEC)
assert SCRIPT_SPEC.loader is not None
SCRIPT_SPEC.loader.exec_module(SCRIPT_MODULE)


class RecordingProvider(CatalogProvider):
    def __init__(self, technosphere, biosphere):
        self.technosphere = technosphere
        self.biosphere = biosphere
        self.calls = []

    def load_technosphere(self, profile):
        self.calls.append(("technosphere", profile))
        return self.technosphere

    def load_biosphere(self, profile):
        self.calls.append(("biosphere", profile))
        return self.biosphere

    def technosphere_profiles(self):
        return (self.technosphere.profile,)

    def biosphere_profiles(self):
        return (self.biosphere.profile,)


def test_collect_technosphere_catalog_entries_uses_dataset_identities():
    inventory_data = [
        {
            "name": "dataset a",
            "reference product": "product a",
            "location": "CH",
            "unit": "kilogram",
            "exchanges": [
                {
                    "type": "technosphere",
                    "name": "external supplier",
                    "reference product": "external product",
                    "location": "RER",
                    "unit": "kilogram",
                }
            ],
        },
        {
            "name": "dataset b",
            "reference product": "product b",
            "location": "GLO",
            "unit": "megajoule",
            "exchanges": [],
        },
    ]

    assert collect_technosphere_catalog_entries(inventory_data) == frozenset(
        {
            ("dataset a", "product a", "CH", "kilogram"),
            ("dataset b", "product b", "GLO", "megajoule"),
        }
    )


def test_collect_biosphere_catalog_entries_collects_unique_flow_keys():
    inventory_data = [
        {
            "name": "dataset",
            "reference product": "product",
            "location": "CH",
            "unit": "kilogram",
            "exchanges": [
                {
                    "type": "biosphere",
                    "name": "Carbon dioxide, fossil",
                    "categories": ("air",),
                    "unit": "kilogram",
                },
                {
                    "type": "biosphere",
                    "name": "Carbon dioxide, fossil",
                    "categories": ("air",),
                    "unit": "kilogram",
                },
            ],
        }
    ]

    assert collect_biosphere_catalog_entries(inventory_data) == frozenset(
        {
            ("Carbon dioxide, fossil", ("air",), "kilogram"),
        }
    )


def test_write_background_catalog_round_trips(tmp_path, monkeypatch):
    profile = BackgroundProfile(family="ecoinvent", version="3.10", system_model="cutoff")
    monkeypatch.setenv("BRIGHTPATH_REFERENCE_DIR", str(tmp_path))

    write_background_catalog(
        profile,
        technosphere={
            ("dataset a", "product a", "CH", "kilogram"),
        },
        biosphere={
            ("Carbon dioxide, fossil", ("air",), "kilogram"),
        },
        output_dir=tmp_path,
    )

    loaded = load_background_catalog(profile)

    assert loaded.profile == profile
    assert loaded.technosphere == frozenset({("dataset a", "product a", "CH", "kilogram")})
    assert loaded.biosphere == frozenset({("Carbon dioxide, fossil", ("air",), "kilogram")})


def test_load_background_catalog_uses_independent_exact_provider_axes(monkeypatch):
    profile = BackgroundProfile("ecoinvent", "3.10.1", "cutoff")
    technosphere_profile = TechnosphereProfile("ecoinvent", "3.10.1", "cutoff")
    biosphere_profile = BiosphereProfile("ecoinvent", "3.10.1")
    provider = RecordingProvider(
        TechnosphereCatalog(
            technosphere_profile,
            {("market", "product", "GLO", "kilogram")},
        ),
        BiosphereCatalog(
            biosphere_profile,
            {("Carbon dioxide, fossil", ("air",), "kilogram")},
        ),
    )
    monkeypatch.setattr("brightpath.catalogs.catalog_provider_from_environment", lambda: provider)

    loaded = load_background_catalog(profile)

    assert provider.calls == [
        ("technosphere", technosphere_profile),
        ("biosphere", biosphere_profile),
    ]
    assert loaded.profile == profile
    assert loaded.technosphere == provider.technosphere.identities
    assert loaded.biosphere == provider.biosphere.identities


def test_load_uvek_background_uses_documented_ecoinvent_biosphere(monkeypatch):
    profile = BackgroundProfile("BAFU", "2025.0", "cut-off")
    technosphere_profile = TechnosphereProfile("uvek", "2025", "cutoff")
    biosphere_profile = BiosphereProfile("ecoinvent", "3.10")
    provider = RecordingProvider(
        TechnosphereCatalog(technosphere_profile, {("uvek activity", "product", "CH", "kilogram")}),
        BiosphereCatalog(biosphere_profile, {("Carbon dioxide, fossil", ("air",), "kilogram")}),
    )
    monkeypatch.setattr("brightpath.catalogs.catalog_provider_from_environment", lambda: provider)

    loaded = load_background_catalog(profile)

    assert loaded.profile == BackgroundProfile("uvek", "2025", "cutoff")
    assert provider.calls == [
        ("technosphere", technosphere_profile),
        ("biosphere", biosphere_profile),
    ]


@pytest.mark.parametrize("wrong_axis", ["technosphere", "biosphere"])
def test_load_background_catalog_rejects_provider_profile_mismatch(monkeypatch, wrong_axis):
    profile = BackgroundProfile("ecoinvent", "3.10", "cutoff")
    technosphere_profile = TechnosphereProfile("ecoinvent", "3.10", "cutoff")
    biosphere_profile = BiosphereProfile("ecoinvent", "3.10")
    returned_technosphere = (
        TechnosphereProfile("ecoinvent", "3.9", "cutoff") if wrong_axis == "technosphere" else technosphere_profile
    )
    returned_biosphere = BiosphereProfile("ecoinvent", "3.9") if wrong_axis == "biosphere" else biosphere_profile
    provider = RecordingProvider(
        TechnosphereCatalog(returned_technosphere, set()),
        BiosphereCatalog(returned_biosphere, set()),
    )
    monkeypatch.setattr("brightpath.catalogs.catalog_provider_from_environment", lambda: provider)

    with pytest.raises(CatalogIntegrityError, match=rf"requested {wrong_axis} profile"):
        load_background_catalog(profile)


def test_available_catalog_profiles_delegates_to_provider_and_normalizes_alias(monkeypatch):
    provider = RecordingProvider(
        TechnosphereCatalog(TechnosphereProfile("BAFU", "2025", "cutoff"), set()),
        BiosphereCatalog(BiosphereProfile("BAFU", "2025"), set()),
    )
    monkeypatch.setattr("brightpath.catalogs.catalog_provider_from_environment", lambda: provider)

    assert available_catalog_profiles(family="BAFU") == [BackgroundProfile("uvek", "2025", "cutoff")]
    assert available_catalog_profiles(family="ecoinvent") == []


def test_legacy_loader_uses_directory_provider_manifest_integrity(tmp_path, monkeypatch):
    profile = BackgroundProfile("ecoinvent", "3.10", "cutoff")
    catalog_path = write_background_catalog(profile, technosphere=set(), biosphere=set(), output_dir=tmp_path)
    payload = json.loads(catalog_path.read_text(encoding="utf-8"))
    (tmp_path / "RESOURCE_MANIFEST.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "resources": [
                    {
                        "file": catalog_path.name,
                        "sha256": "0" * 64,
                        "size": catalog_path.stat().st_size,
                        "schema_version": 1,
                        "profile": payload["profile"],
                        "technosphere_identities": 0,
                        "biosphere_identities": 0,
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("BRIGHTPATH_REFERENCE_DIR", str(tmp_path))

    with pytest.raises(CatalogIntegrityError, match="digest or size"):
        load_background_catalog(profile)


def test_clean_credential_value_strips_rtf_delimiters():
    assert SCRIPT_MODULE.clean_credential_value("Romain.Sacchi\\") == "Romain.Sacchi"
    assert SCRIPT_MODULE.clean_credential_value("!052388.Dream}") == "!052388.Dream"


def test_load_credentials_prefers_environment_variables(monkeypatch):
    monkeypatch.setenv("ECOINVENT_USERNAME", "user-from-env")
    monkeypatch.setenv("ECOINVENT_PASSWORD", "pass-from-env")

    username, password = SCRIPT_MODULE.load_credentials(
        SimpleNamespace(
            username="",
            password="",
            credentials_file="",
        )
    )

    assert username == "user-from-env"
    assert password == "pass-from-env"
