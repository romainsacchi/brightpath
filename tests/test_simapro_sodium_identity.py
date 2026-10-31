"""Regression coverage for distinct sodium UUIDs collapsed by SimaPro labels."""

from copy import deepcopy

import pytest

from brightpath.background.catalogs import BiosphereCatalog, InMemoryCatalogProvider, PackageCatalogProvider
from brightpath.core import BiosphereProfile
from brightpath.exceptions import SimaProSerializationError
from brightpath.formats.simapro_csv import load_simapro_csv, render_simapro_rows, write_simapro_csv
from brightpath.formats.simapro_sodium_identity import SodiumIdentityGuard
from brightpath.models import BackgroundProfile, InventoryDocument, InventoryFormat
from brightpath.utils import get_simapro_biosphere

GENERIC = ("Sodium", ("water",), "kilogram")
IONIC = ("Sodium, ion", ("water",), "kilogram")
GENERIC_UUID = "723f56f9-c68e-5692-a969-0fb2a70bce20"
IONIC_UUID = "5ac961f0-8e53-42ff-b13f-1a7fb9133868"


def document(version, name="Sodium", **overrides):
    exchange = dict(type="biosphere", name=name, categories=("water",), unit="kilogram", amount=0.25)
    exchange.update(overrides)
    activity = dict(
        name="Sodium identity control",
        **{"reference product": "test output"},
        location="GLO",
        unit="kilogram",
        comment="Synthetic sodium regression fixture.",
    )
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


@pytest.mark.parametrize("version", ["3.6", "3.7", "3.8"])
@pytest.mark.parametrize("categories", [("water",), ("water", "unspecified")])
def test_generic_sodium_refused_before_touching_destination(tmp_path, version, categories):
    source = document(version, categories=categories)
    before = deepcopy(source.data)
    result = render_simapro_rows(source)
    assert not result.rows
    issue = next(i for i in result.issues if i.code == "simapro_biosphere_identity_ambiguous")
    assert issue.path == "activity[0].exchanges[1]"
    assert GENERIC_UUID in issue.message and IONIC_UUID in issue.message
    destination = tmp_path / "existing.csv"
    destination.write_bytes(b"keep original file")
    with pytest.raises(SimaProSerializationError, match="cannot preserve the distinction"):
        write_simapro_csv(source, destination)
    assert destination.read_bytes() == b"keep original file"
    assert source.data == before


@pytest.mark.parametrize("version", ["3.6", "3.7", "3.8"])
def test_ionic_sodium_actual_roundtrip_preserves_uuid_amount_and_source(tmp_path, version):
    provider = PackageCatalogProvider()
    source = document(version, name="Sodium, ion")
    before = deepcopy(source.data)
    path, result = write_simapro_csv(source, tmp_path / "ionic.csv")
    assert not result.has_errors
    loaded = load_simapro_csv(path, background_profile=source.background_profile, catalog_provider=provider)
    exchange = next(e for e in loaded.data[0]["exchanges"] if e["type"] == "biosphere")
    identity = exchange["name"], tuple(exchange["categories"]), exchange["unit"]
    catalog = provider.load_biosphere(BiosphereProfile("ecoinvent", version))
    assert identity == IONIC
    assert catalog.references[identity] == IONIC_UUID
    assert exchange["amount"] == 0.25
    assert source.data == before


@pytest.mark.parametrize(
    "fields",
    [{"amount": 0, "formula": "x"}, {"amount": 0, "uncertainty type": 3, "loc": 0, "scale": 1}, {"amount": -1}],
)
def test_active_zero_and_negative_exchanges_are_not_exempt(fields):
    source = document("3.8", **fields)
    guard = SodiumIdentityGuard(source.context.background.biosphere, get_simapro_biosphere())
    issue = guard.issue(source.data[0]["exchanges"][1], "exchange")
    assert issue.code == "simapro_biosphere_identity_ambiguous"


def test_inactive_zero_does_not_load_catalogue():
    class ForbiddenProvider:
        def load_biosphere(self, profile):
            raise AssertionError("No catalogue required for an omitted deterministic zero")

    source = document("3.8", amount=0)
    guard = SodiumIdentityGuard(source.context.background.biosphere, get_simapro_biosphere(), ForbiddenProvider())
    assert guard.issue(source.data[0]["exchanges"][1], "exchange") is None


@pytest.mark.parametrize(
    "fields",
    [
        {"categories": ("soil", "agricultural")},
        {"unit": "cubic meter"},
        {"type": "technosphere"},
        {"name": "Sodium, ion"},
    ],
)
def test_other_contexts_not_reclassified(fields):
    source = document("3.8", **fields)
    guard = SodiumIdentityGuard(source.context.background.biosphere, get_simapro_biosphere())
    assert guard.issue(source.data[0]["exchanges"][1], "exchange") is None


def test_injected_catalogue_is_honored_and_distinct_labels_allowed():
    profile = BiosphereProfile("ecoinvent", "3.8")
    catalog = BiosphereCatalog(profile, {GENERIC, IONIC}, references={GENERIC: GENERIC_UUID, IONIC: IONIC_UUID})
    provider = InMemoryCatalogProvider(biosphere=[catalog])
    source = document("3.8").data[0]["exchanges"][1]
    assert SodiumIdentityGuard(profile, {"Sodium, ion": "Sodium"}, provider).issue(source, "x") is not None
    assert (
        SodiumIdentityGuard(profile, {"Sodium": "Distinct verified generic label"}, provider).issue(source, "x") is None
    )
    other = BiosphereCatalog(profile, {GENERIC, IONIC}, references={GENERIC: IONIC_UUID, IONIC: GENERIC_UUID})
    other_provider = InMemoryCatalogProvider(biosphere=[other])
    assert SodiumIdentityGuard(profile, {"Sodium, ion": "Sodium"}, other_provider).issue(source, "x") is None


def test_missing_exact_catalogue_is_a_structured_error():
    profile = BiosphereProfile("ecoinvent", "3.8")
    guard = SodiumIdentityGuard(profile, get_simapro_biosphere(), InMemoryCatalogProvider())
    issue = guard.issue(document("3.8").data[0]["exchanges"][1], "x")
    assert issue.code == "simapro_biosphere_unresolved"
    assert issue.path == "x"


@pytest.mark.parametrize("version", ["3.9", "3.9.1"])
def test_newer_ionic_name_does_not_make_generic_native_label_lossless(tmp_path, version):
    # The native reader still normalizes plain Sodium to ionic sodium, even
    # when the exact catalogue has renamed that peer Sodium I.
    source = document(version)
    before = deepcopy(source.data)
    result = render_simapro_rows(source)
    assert any(i.code == "simapro_biosphere_identity_ambiguous" for i in result.issues)
    with pytest.raises(SimaProSerializationError, match="cannot preserve the distinction"):
        write_simapro_csv(source, tmp_path / "generic.csv")
    assert not (tmp_path / "generic.csv").exists()
    assert source.data == before


@pytest.mark.parametrize("version", ["3.10", "3.11", "3.12"])
def test_guard_does_not_reintroduce_retired_generic_identity(version):
    # Later releases already merge this flow via explicit background migration.
    # An invalid source declaring the retired generic identity must instead be
    # handled by ordinary exact-background validation, not this format guard.
    provider = PackageCatalogProvider()
    profile = BiosphereProfile("ecoinvent", version)
    catalog = provider.load_biosphere(profile)
    assert GENERIC not in catalog.identities
    assert GENERIC_UUID not in catalog.references.values()
    guard = SodiumIdentityGuard(profile, get_simapro_biosphere(), provider)
    assert guard.issue(document(version).data[0]["exchanges"][1], "x") is None
