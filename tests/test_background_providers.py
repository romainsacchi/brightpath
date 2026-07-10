import json

import pytest

from brightpath.background import (
    BiosphereCatalog,
    CatalogIntegrityError,
    CompositeCatalogProvider,
    DirectoryCatalogProvider,
    InMemoryCatalogProvider,
    PackageCatalogProvider,
    TechnosphereCatalog,
)
from brightpath.core.context import BiosphereProfile, TechnosphereProfile


def technosphere(version="3.10", model="cutoff"):
    return TechnosphereProfile("ecoinvent", version, model)


def biosphere(version="3.10"):
    return BiosphereProfile("ecoinvent", version)


def test_in_memory_provider_keeps_background_axes_independent():
    provider = InMemoryCatalogProvider(
        technosphere=[TechnosphereCatalog(technosphere(), {("market", "product", "GLO", "kilogram")})],
        biosphere=[BiosphereCatalog(biosphere(), {("Carbon dioxide, fossil", ("air",), "kilogram")})],
    )

    assert provider.load_technosphere(technosphere()).profile.system_model == "cutoff"
    assert provider.load_biosphere(biosphere()).profile == biosphere()


def test_directory_provider_rejects_embedded_profile_mismatch(tmp_path):
    path = tmp_path / "ecoinvent__3.10__cutoff.json"
    path.write_text(
        json.dumps(
            {
                "profile": {"family": "ecoinvent", "version": "3.9", "system_model": "cutoff"},
                "technosphere": [],
                "biosphere": [],
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(CatalogIntegrityError, match="not requested"):
        DirectoryCatalogProvider(tmp_path).load_technosphere(technosphere())


def test_directory_provider_requires_consistent_biosphere_across_models(tmp_path):
    for model, flow in (("cutoff", "flow a"), ("consequential", "flow b")):
        (tmp_path / f"ecoinvent__3.10__{model}.json").write_text(
            json.dumps(
                {
                    "profile": {"family": "ecoinvent", "version": "3.10", "system_model": model},
                    "technosphere": [],
                    "biosphere": [{"name": flow, "categories": ["air"], "unit": "kilogram"}],
                }
            ),
            encoding="utf-8",
        )

    with pytest.raises(CatalogIntegrityError, match="disagree"):
        DirectoryCatalogProvider(tmp_path).load_biosphere(biosphere())


def test_composite_provider_uses_first_matching_catalog():
    first = InMemoryCatalogProvider(
        technosphere=[TechnosphereCatalog(technosphere(), {("first", "product", "GLO", "kilogram")})]
    )
    second = InMemoryCatalogProvider(
        technosphere=[TechnosphereCatalog(technosphere(), {("second", "product", "GLO", "kilogram")})]
    )

    catalog = CompositeCatalogProvider((first, second)).load_technosphere(technosphere())

    assert next(iter(catalog.identities))[0] == "first"


def test_package_provider_loads_exact_profiles_and_digests():
    provider = PackageCatalogProvider()

    tech = provider.load_technosphere(technosphere())
    bio = provider.load_biosphere(biosphere())

    assert tech.identities
    assert bio.identities
    assert len(tech.digest) == 64
    assert len(bio.digest) == 64
