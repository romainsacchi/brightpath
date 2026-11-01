"""Native SimaPro literal regressions across exact older ecoinvent catalogues."""

from copy import deepcopy

import pytest

from brightpath.background import BiosphereCatalog, InMemoryCatalogProvider
from brightpath.background.catalogs import PackageCatalogProvider
from brightpath.core import BiosphereProfile
from brightpath.exceptions import SimaProSerializationError
from brightpath.formats.simapro_biosphere_identity import NATIVE_IDENTITIES, UNVERIFIED_DISTINCT_IDENTITIES
from brightpath.formats.simapro_csv import load_simapro_csv, write_simapro_csv
from brightpath.models import BackgroundProfile, InventoryDocument, InventoryFormat

VERSIONS = ["3.6", "3.7", "3.8", "3.9.1"]


@pytest.fixture(scope="module")
def provider():
    return PackageCatalogProvider()


def document(version, exchange):
    activity = {
        "name": "Synthetic native literal control",
        "reference product": "synthetic output",
        "location": "GLO",
        "unit": "kilogram",
        "comment": "Synthetic native literal regression fixture.",
    }
    return InventoryDocument(
        data=[
            {
                **activity,
                "exchanges": [
                    {**activity, "type": "production", "amount": 1, "simapro category": "Materials/Test"},
                    exchange,
                ],
            }
        ],
        background_profile=BackgroundProfile("ecoinvent", version, "cutoff"),
        inventory_format=InventoryFormat.SIMAPRO_CSV,
    )


@pytest.mark.parametrize("version", VERSIONS)
@pytest.mark.parametrize("wire,flow_id", list(NATIVE_IDENTITIES.items()))
def test_actual_native_literal_write_read_UUID_and_scalar_preservation(tmp_path, provider, version, wire, flow_id):
    catalog = provider.load_biosphere(BiosphereProfile("ecoinvent", version))
    identity = next(i for i, uid in catalog.references.items() if uid == flow_id and i[1:] == wire[1:])
    exchange = dict(
        type="biosphere",
        name=identity[0],
        categories=identity[1],
        unit=identity[2],
        amount=0.25,
        **{"uncertainty type": 5, "loc": 0.25, "minimum": 0, "maximum": 0.3},
    )
    source = document(version, exchange)
    before = deepcopy(source.data)
    path, _ = write_simapro_csv(source, tmp_path / "native.csv", catalog_provider=provider)
    assert any(row.startswith(wire[0] + ";") for row in path.read_text(encoding="latin1").splitlines())
    loaded = load_simapro_csv(path, background_profile=source.background_profile, catalog_provider=provider)
    bio = next(e for e in loaded.data[0]["exchanges"] if e["type"] == "biosphere")
    assert (bio["name"], tuple(bio["categories"]), bio["unit"]) == identity
    assert catalog.references[identity] == flow_id
    assert bio["amount"] == 0.25
    assert bio["uncertainty type"] == 5
    assert bio["loc"] == 0.25
    assert bio["minimum"] == 0
    assert bio["maximum"] == 0.3
    assert source.data == before


@pytest.mark.parametrize("version", VERSIONS)
@pytest.mark.parametrize("flow_id", list(UNVERIFIED_DISTINCT_IDENTITIES))
def test_unverified_distinct_grade_directions_refuse_without_changing_source(tmp_path, provider, version, flow_id):
    catalog = provider.load_biosphere(BiosphereProfile("ecoinvent", version))
    identities = [i for i, uid in catalog.references.items() if uid == flow_id]
    if not identities:
        pytest.skip("This distinct UUID is absent from this exact selected catalogue")
    identity = identities[0]
    source = document(
        version, dict(type="biosphere", name=identity[0], categories=identity[1], unit=identity[2], amount=1)
    )
    before = deepcopy(source.data)
    with pytest.raises(SimaProSerializationError, match="No verified distinct"):
        write_simapro_csv(source, tmp_path / "unsupported.csv", catalog_provider=provider)
    assert source.data == before
    assert not (tmp_path / "unsupported.csv").exists()


def test_writer_honors_injected_provider_and_does_not_fall_back_to_packaged_catalog(tmp_path, monkeypatch):
    import brightpath.formats.simapro_csv as codec

    def forbidden_default():
        raise AssertionError("Explicit catalogue provider must be honored")

    monkeypatch.setattr(codec, "catalog_provider_from_environment", forbidden_default)
    identity = ("Hydrochloric acid", ("air",), "kilogram")
    custom = InMemoryCatalogProvider(
        biosphere=[
            BiosphereCatalog(
                BiosphereProfile("ecoinvent", "3.6"),
                {identity},
                references={identity: "c941d6d0-a56c-4e6c-95de-ac685635218d"},
            )
        ]
    )
    source = document(
        "3.6", dict(type="biosphere", name=identity[0], categories=identity[1], unit=identity[2], amount=1)
    )
    with pytest.raises(SimaProSerializationError):
        write_simapro_csv(source, tmp_path / "custom.csv", catalog_provider=custom)


@pytest.mark.parametrize("version", VERSIONS)
def test_PV_unknown_to_unspecified_remains_a_different_UUID_loss(tmp_path, provider, version):
    source = document(
        version,
        dict(
            type="biosphere",
            name="Transformation, from unknown",
            categories=("natural resource", "land"),
            unit="square meter",
            amount=0.25,
        ),
    )
    before = deepcopy(source.data)
    path, _ = write_simapro_csv(source, tmp_path / "PV.csv", catalog_provider=provider)
    loaded = load_simapro_csv(path, background_profile=source.background_profile, catalog_provider=provider)
    bio = next(e for e in loaded.data[0]["exchanges"] if e["type"] == "biosphere")
    assert bio["name"] == "Transformation, from unspecified"
    assert bio["amount"] == 0.25
    catalog = provider.load_biosphere(BiosphereProfile("ecoinvent", version))
    assert (
        catalog.references[("Transformation, from unknown", ("natural resource", "land"), "square meter")]
        != catalog.references[(bio["name"], tuple(bio["categories"]), bio["unit"])]
    )
    assert source.data == before
