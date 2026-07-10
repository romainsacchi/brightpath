import importlib.util
from pathlib import Path
from types import SimpleNamespace

from brightpath.catalogs import (
    collect_biosphere_catalog_entries,
    collect_technosphere_catalog_entries,
    load_background_catalog,
    write_background_catalog,
)
from brightpath.models import BackgroundProfile

SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "generate_reference_catalogs.py"
SCRIPT_SPEC = importlib.util.spec_from_file_location("generate_reference_catalogs", SCRIPT_PATH)
SCRIPT_MODULE = importlib.util.module_from_spec(SCRIPT_SPEC)
assert SCRIPT_SPEC.loader is not None
SCRIPT_SPEC.loader.exec_module(SCRIPT_MODULE)


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
    profile = BackgroundProfile(family="uvek", version="2025", system_model="cutoff")
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
