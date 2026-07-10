import pytest

from brightpath.background import BiosphereCatalog, InMemoryCatalogProvider
from brightpath.core import BiosphereProfile
from brightpath.formats.simapro_csv import normalize_simapro_import_data
from brightpath.models import BackgroundProfile


def _raw_inventory():
    return [
        {
            "name": "Product {GLO}| market for | Cut-off, U",
            "unit": "kilogram",
            "simapro metadata": {"Category type": "Materials"},
            "exchanges": [
                {
                    "type": "production",
                    "name": "Product {GLO}| market for | Cut-off, U",
                    "unit": "kilogram",
                    "amount": 1.0,
                    "categories": ("Materials",),
                },
                {
                    "type": "biosphere",
                    "name": "Release-specific flow",
                    "categories": ("air", "urban air close to ground"),
                    "unit": "kilogram",
                    "amount": 2.0,
                },
            ],
        }
    ]


def test_simapro_normalization_uses_the_exact_injected_biosphere_catalog():
    profile = BiosphereProfile("ecoinvent", "3.11")
    provider = InMemoryCatalogProvider(
        biosphere=(
            BiosphereCatalog(
                profile,
                {
                    (
                        "Release-specific flow",
                        ("air", "urban air close to ground"),
                        "kilogram",
                    )
                },
                source="test exact catalog",
            ),
        )
    )

    normalized = normalize_simapro_import_data(
        _raw_inventory(),
        background_profile=BackgroundProfile("ecoinvent", "3.11", "cutoff"),
        biosphere_profile=profile,
        database_name="test",
        catalog_provider=provider,
        biosphere_correspondence={},
        version_mapping={},
    )

    exchange = normalized[0]["exchanges"][1]
    assert exchange["name"] == "Release-specific flow"
    assert exchange["categories"] == ("air", "urban air close to ground")


def test_simapro_normalization_never_falls_back_to_a_fixed_biosphere_release():
    with pytest.raises(TypeError, match="catalog_provider or biosphere_flows"):
        normalize_simapro_import_data(
            _raw_inventory(),
            background_profile=BackgroundProfile("ecoinvent", "3.11", "cutoff"),
            biosphere_profile=BiosphereProfile("ecoinvent", "3.11"),
            database_name="test",
            biosphere_correspondence={},
            version_mapping={},
        )
