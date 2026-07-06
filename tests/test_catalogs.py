from brightpath.catalogs import (
    collect_biosphere_catalog_entries,
    collect_technosphere_catalog_entries,
    load_background_catalog,
    write_background_catalog,
)
from brightpath.models import BackgroundProfile


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
