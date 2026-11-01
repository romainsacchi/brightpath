"""Observed native land labels must not collapse distinct source classes."""

import csv
import hashlib
from copy import deepcopy

import pytest
from test_simapro_catalog_identity import document

from brightpath import DATA_DIR, BackgroundProfile, __version__
from brightpath.background import migration_resource_fingerprint
from brightpath.background.catalogs import PackageCatalogProvider
from brightpath.core import BiosphereProfile
from brightpath.formats.simapro_csv import load_simapro_csv, write_simapro_csv
from brightpath.migrations.schema import SCHEMA_PATH
from brightpath.models import InventoryDocument
from brightpath.profiles.simapro_biosphere import resolve_ecoinvent_flow_name, validate_ecoinvent_flow

FLOWS = [
    ("Occupation, mineral extraction site", "square meter-year", "m2a", "379ba5c9-5c3a-43d0-8e2d-605ad9c39e46"),
    ("Transformation, to mineral extraction site", "square meter", "m2", "626915e9-2424-4059-8b6a-fae47161acdf"),
    ("Transformation, from annual crop", "square meter", "m2", "f05cca02-ec18-4acc-9939-59658ff9a554"),
    (
        "Occupation, annual crop, irrigated, intensive",
        "square meter-year",
        "m2a",
        "9fd128fe-d8c5-476f-af42-2795d5f5d227",
    ),
    (
        "Occupation, annual crop, non-irrigated, intensive",
        "square meter-year",
        "m2a",
        "e063ee9c-9850-42b5-b01e-4cc9b5ad7152",
    ),
]


@pytest.mark.parametrize("version", ["3.9.1", "3.12"])
@pytest.mark.parametrize("amount", [2.5, -2.5])
@pytest.mark.parametrize("name,unit,wire_unit,flow_id", FLOWS)
def test_native_label_preserves_exact_land_uuid_and_sampling_fields(
    tmp_path, version, amount, name, unit, wire_unit, flow_id
):
    profile = BackgroundProfile("ecoinvent", version, "cutoff")
    provider = PackageCatalogProvider()
    categories = ("natural resource", "land")
    source = document(profile, {"type": "biosphere", "name": name, "categories": categories, "unit": unit})
    data = source.data
    data[0]["exchanges"][1].update(
        amount=amount, loc=amount, scale=0.3, formula=f"({amount}) * 1", **{"uncertainty type": 3}
    )
    source = InventoryDocument(data=data, context=source.context)
    before = deepcopy(source.data)
    path, _ = write_simapro_csv(source, tmp_path / "native-land.csv")
    with path.open(encoding="latin-1", newline="") as stream:
        rows = list(csv.reader(stream, delimiter=";"))
    (wire,) = [r for r in rows if len(r) == 9 and r[0] == name]
    assert wire[:3] == [name, "land", wire_unit]
    assert wire[4:6] == ["Normal", "0.6"]
    for _ in range(2):
        restored = load_simapro_csv(path, background_profile=profile, catalog_provider=provider)
        exchange = next(e for e in restored.data[0]["exchanges"] if e["type"] == "biosphere")
        identity = exchange["name"], tuple(exchange["categories"]), exchange["unit"]
        assert identity == (name, categories, unit)
        assert provider.load_biosphere(BiosphereProfile("ecoinvent", version)).references[identity] == flow_id
        assert exchange["amount"] == exchange["loc"] == amount
        assert exchange["scale"] == 0.3 and exchange["formula"] == f"({amount}) * 1"
        write_simapro_csv(restored, path)
    assert source.data == before


@pytest.mark.parametrize("name,unit,wire_unit,flow_id", FLOWS)
def test_native_land_rule_is_limited_to_observed_units_and_compartment(name, unit, wire_unit, flow_id):
    exchange = {"name": name, "categories": ("natural resource", "land"), "unit": unit}
    assert resolve_ecoinvent_flow_name(exchange, "legacy alias") == name
    assert (
        resolve_ecoinvent_flow_name({**exchange, "categories": ("soil", "agricultural")}, "legacy alias")
        == "legacy alias"
    )
    with pytest.raises(ValueError, match="requires"):
        validate_ecoinvent_flow({**exchange, "unit": "kilogram"})


def test_native_land_labels_invalidate_preceding_export_cache():
    previous = hashlib.sha256(
        __version__.encode()
        + b"migration-schema-v3-native-source-format-preservation-v4"
        + b"simapro-normal-two-sigma-v1"
        + (DATA_DIR / "migrations/RESOURCE_MANIFEST.json").read_bytes()
        + (DATA_DIR / "export/reference_catalogs/RESOURCE_MANIFEST.json").read_bytes()
        + SCHEMA_PATH.read_bytes()
    ).hexdigest()
    assert migration_resource_fingerprint() != previous
