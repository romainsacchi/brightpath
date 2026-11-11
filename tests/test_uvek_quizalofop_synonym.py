"""One chemical rename must not authorize other releases or compartments."""

import hashlib
import json
from copy import deepcopy

import pytest

from brightpath import DATA_DIR
from brightpath.migrations.canonical_biosphere import select_canonical_biosphere_rules
from brightpath.migrations.resources import load_uvek_biosphere_resource
from scripts.review_uvek_quizalofop_synonym import BASELINE_SHA256, build_extended_biosphere_resource


def test_successor_preserves_baseline_and_every_unreviewed_decision():
    path = DATA_DIR / "migrations/uvek/ecoinvent-to-ecoinvent-3.10-biosphere.json"
    assert hashlib.sha256(path.read_bytes()).hexdigest() == BASELINE_SHA256
    base = json.loads(path.read_text())
    before = deepcopy(base)
    active = build_extended_biosphere_resource(base)
    assert base == before
    rid = "bp-c31afda84ba5fe78"
    assert [r for r in active["replace"] if r["id"] != rid] == base["replace"]
    assert active["unresolved"] == [r for r in base["unresolved"] if r["id"] != rid]
    loaded = load_uvek_biosphere_resource()
    loaded.pop("_path")
    assert loaded == active


@pytest.mark.parametrize("version", ["3.6", "3.7", "3.8"])
def test_exact_release_only(version):
    selected = select_canonical_biosphere_rules(load_uvek_biosphere_resource(), version)
    rid = "bp-c31afda84ba5fe78"
    mapped = [r for r in selected["replace"] if r["id"] == rid]
    unresolved = [r for r in selected["unresolved"] if r["id"] == rid]
    assert bool(mapped) == (version == "3.8")
    assert bool(unresolved) == (version != "3.8")
    if mapped:
        assert mapped[0]["target"]["flow_id"] == "9ae11925-3df9-5fde-b7af-1627c0818347"
        assert mapped[0]["target"]["categories"] == ["soil", "agricultural"]
        assert mapped[0]["target"]["unit"] == "kilogram"


@pytest.mark.parametrize("amount", [-0.25, 0.25])
def test_signed_chemical_mass_uncertainty_and_formula_survive(amount):
    from test_uvek_canonical_biosphere_identity import exchange, execute

    source = {"name": "Quizalofop ethyl ester", "categories": ["soil", "agricultural"], "unit": "kilogram"}
    bio = exchange(source, amount, formula="dose", **{"uncertainty type": 3, "loc": amount, "scale": 0.1})
    result, _ = execute("3.8", [bio])
    assert result.succeeded, result.report
    actual = result.value.data[0]["exchanges"][1]
    assert actual["name"] == "Quizalofop-ethyl"
    for key in ("amount", "formula", "uncertainty type", "loc", "scale", "categories", "unit"):
        assert actual[key] == bio[key]
