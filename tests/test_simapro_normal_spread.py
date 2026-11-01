"""SimaPro CSV normal parameter1 is doubled SD for exchanges and parameters."""

import csv
import hashlib
from copy import deepcopy

import pytest
from bw2io.extractors.simapro_csv import SimaProCSVExtractor
from test_simapro_inventory import minimal_activity, profile

from brightpath import DATA_DIR, SimaProInventory, __version__
from brightpath.background import migration_resource_fingerprint
from brightpath.formats.simapro_csv import _ExpressionPreservingSimaProExtractor
from brightpath.migrations.schema import SCHEMA_PATH


def make_source(amount=2, sigma=0.3, *, unit="kilogram", formula=False):
    exchange = {
        "name": "Water" if unit == "cubic meter" else "Carbon dioxide, fossil",
        "type": "biosphere",
        "categories": ("air",),
        "unit": unit,
        "amount": amount,
        "uncertainty type": 3,
        "loc": amount,
        "scale": sigma,
    }
    if formula:
        exchange["formula"] = f"({amount!r}) * 1"
    activity = minimal_activity(extra_exchanges=[exchange])
    return SimaProInventory.from_data([activity], background_profile=profile("3.12"))


@pytest.mark.parametrize("amount", [2, -2])
@pytest.mark.parametrize("formula", [False, True])
def test_sigma_point_three_native_field_and_scalar_formula_roundtrip(tmp_path, amount, formula):
    source = make_source(amount, formula=formula)
    before = source.data
    path = source.write_csv(tmp_path / "normal.csv")
    with path.open(encoding="latin-1", newline="") as stream:
        rows = list(csv.reader(stream, delimiter=";"))
    (wire,) = [row for row in rows if len(row) == 9 and row[4] == "Normal"]
    assert wire[4:8] == ["Normal", "0.6", "0", "0"]
    # Independent numeric interpretation of the documented native 2*SD field.
    assert float(wire[5]) / 2 == 0.3
    loaded = SimaProInventory.from_csv(path, background_profile=source.background_profile)
    result = next(e for e in loaded.data[0]["exchanges"] if e["type"] == "biosphere")
    assert result["amount"] == result["loc"] == amount
    assert result["scale"] == 0.3
    assert result.get("formula", "") == before[0]["exchanges"][1].get("formula", "")
    assert source.data == before


@pytest.mark.parametrize("scope", ["process", "database", "project"])
def test_normal_parameter_rows_use_two_sigma_and_native_values_override_comments(tmp_path, scope):
    parameter = {"name": "normal_parameter", "amount": 2, "uncertainty type": 3, "loc": 2, "scale": 0.3}
    activity = minimal_activity()
    kwargs = {}
    if scope == "process":
        activity["parameters"] = [parameter]
    else:
        kwargs[f"{scope}_parameters"] = [parameter]
    caller = deepcopy([activity])
    original_parameter = deepcopy(parameter)
    source = SimaProInventory.from_data([activity], background_profile=profile("3.12"), **kwargs)
    before = source.data
    path = source.write_csv(tmp_path / "parameter.csv")
    with path.open(encoding="latin-1", newline="") as stream:
        rows = list(csv.reader(stream, delimiter=";"))
    (wire,) = [row for row in rows if row and row[0] == "normal_parameter"]
    assert wire[1:6] == ["2", "Normal", "0.6", "0", "0"]
    # Remove metadata to prove the native field, rather than a comment, carries SD.
    wire[7] = ""
    with path.open("w", encoding="latin-1", newline="") as stream:
        csv.writer(stream, delimiter=";").writerows(rows)
    loaded = SimaProInventory.from_csv(path, background_profile=source.background_profile)
    parameters = loaded.data[0]["parameters"] if scope == "process" else getattr(loaded, f"{scope}_parameters")
    (result,) = [row for row in parameters if row["name"] == "normal_parameter"]
    assert result["amount"] == result["loc"] == 2
    assert result["scale"] == 0.3
    assert [activity] == caller and parameter == original_parameter and source.data == before


def test_water_volume_scales_sd_before_native_two_sigma():
    source = make_source(0.002, 0.0003, unit="cubic meter")
    before = source.data
    (wire,) = [row for row in source.render().rows if len(row) == 9 and row[4] == "Normal"]
    assert wire[2] == "kg" and wire[3] == "2" and wire[5] == "0.6"
    assert float(wire[5]) / 2 == 0.0003 * 1000
    assert source.data == before


def test_local_normal_reader_does_not_monkeypatch_upstream_or_other_distributions():
    assert _ExpressionPreservingSimaProExtractor.create_distribution("2", "Normal", "0.6", "0", "0")["scale"] == 0.3
    assert SimaProCSVExtractor.create_distribution("2", "Normal", "0.6", "0", "0")["scale"] == pytest.approx(0.6**0.5)
    for kind, fields in [
        ("Lognormal", ("1.5", "0", "0")),
        ("Triangle", ("0", "1", "3")),
        ("Uniform", ("0", "1", "3")),
        ("Undefined", ("0", "0", "0")),
    ]:
        assert _ExpressionPreservingSimaProExtractor.create_distribution(
            "2", kind, *fields
        ) == SimaProCSVExtractor.create_distribution("2", kind, *fields)


def test_normal_wire_remains_tied_to_nominal_no_unrelated_centre_rewrite():
    source = make_source()
    data = source.data
    data[0]["exchanges"][1]["loc"] = 3
    source = SimaProInventory.from_data(data, background_profile=source.background_profile)
    before = source.data
    (wire,) = [row for row in source.render().rows if len(row) == 9 and row[4] == "Normal"]
    assert wire[3] == "2" and wire[5] == "0.6"
    assert source.data == before and before[0]["exchanges"][1]["loc"] == 3


def test_normal_semantic_marker_changes_exact_predecessor_fingerprint():
    prefix = __version__.encode() + b"migration-schema-v3-native-source-format-preservation-v4"
    suffix = (
        (DATA_DIR / "migrations/RESOURCE_MANIFEST.json").read_bytes()
        + (DATA_DIR / "export/reference_catalogs/RESOURCE_MANIFEST.json").read_bytes()
        + SCHEMA_PATH.read_bytes()
    )
    previous = hashlib.sha256(prefix + suffix).hexdigest()
    current = migration_resource_fingerprint()
    # Later format fixes can append semantic markers without restoring the
    # pre-normal-encoding cache key.
    assert current != previous
    assert migration_resource_fingerprint() == current
