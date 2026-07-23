import importlib.util
import json
import sys
from pathlib import Path

from brightpath import DATA_DIR
from brightpath.migrations.engine import _biosphere_match_specification, _biosphere_matches

SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "enrich_biosphere_migration_sources.py"
SCRIPT_SPEC = importlib.util.spec_from_file_location("enrich_biosphere_migration_sources", SCRIPT_PATH)
SCRIPT_MODULE = importlib.util.module_from_spec(SCRIPT_SPEC)
assert SCRIPT_SPEC.loader is not None
sys.modules[SCRIPT_SPEC.name] = SCRIPT_MODULE
SCRIPT_SPEC.loader.exec_module(SCRIPT_MODULE)


def biosphere_resource_paths() -> list[Path]:
    standard = sorted((DATA_DIR / "migrations" / "ecoinvent" / "biosphere").glob("*.json"))
    return [*standard, DATA_DIR / "migrations" / "uvek" / "ecoinvent-to-ecoinvent-3.10-biosphere.json"]


def source_rules(payload: dict):
    for verb in SCRIPT_MODULE.RULE_VERBS:
        for rule in payload.get(verb, ()):
            yield rule["source"]


def test_every_packaged_biosphere_source_has_a_unique_uuid_independent_identity():
    standard_directory = DATA_DIR / "migrations" / "ecoinvent" / "biosphere"

    for path in biosphere_resource_paths():
        payload = json.loads(path.read_text(encoding="utf-8"))
        identities = []
        for source in source_rules(payload):
            assert isinstance(source.get("name"), str) and source["name"]
            assert isinstance(source.get("categories"), list) and source["categories"]
            assert all(isinstance(category, str) and category for category in source["categories"])
            assert isinstance(source.get("unit"), str) and source["unit"]
            if path.parent == standard_directory:
                assert isinstance(source.get("uuid"), str) and source["uuid"]
            identities.append((source["name"], tuple(source["categories"]), source["unit"]))

        assert identities
        assert len(identities) == len(set(identities))


def test_enriched_source_uses_brightway_identity_and_preserves_provenance_fields():
    source = {"name": "Example flow", "unit": "kg", "uuid": "flow-uuid", "formula": "X"}
    flow = {
        "name": "Example flow",
        "categories": ("air", "urban air close to ground"),
        "unit": "kilogram",
    }

    enriched = SCRIPT_MODULE.enriched_source(source, flow)

    assert enriched == {
        "name": "Example flow",
        "categories": ["air", "urban air close to ground"],
        "unit": "kilogram",
        "uuid": "flow-uuid",
        "formula": "X",
    }


def test_complete_biosphere_tuple_matches_without_requiring_uuid():
    specification = {
        "name": "Example flow",
        "categories": ["air"],
        "unit": "kilogram",
        "uuid": "resource-provenance-uuid",
    }
    uuid_less_exchange = {
        "name": "Example flow",
        "categories": ("air",),
        "unit": "kg",
        "type": "biosphere",
    }
    exchange_with_unrelated_uuid = {**uuid_less_exchange, "uuid": "foreground-uuid"}

    assert _biosphere_matches(uuid_less_exchange, specification)
    assert _biosphere_matches(exchange_with_unrelated_uuid, specification)
    assert not _biosphere_matches({**uuid_less_exchange, "categories": ("water",)}, specification)


def test_reverse_rule_inherits_unchanged_biosphere_identity_fields():
    rule = {
        "source": {
            "name": "Particulates, < 2.5 um",
            "categories": ["air", "urban air close to ground"],
            "unit": "kilogram",
            "uuid": "source-uuid",
        },
        "target": {
            "name": "Particulate Matter, < 2.5 um",
            "uuid": "target-uuid",
        },
    }
    exchange = {
        "name": "Particulate Matter, < 2.5 um",
        "categories": ("air", "urban air close to ground"),
        "unit": "kilogram",
        "type": "biosphere",
    }

    specification = _biosphere_match_specification(rule, "target")

    assert specification["categories"] == ["air", "urban air close to ground"]
    assert specification["unit"] == "kilogram"
    assert _biosphere_matches(exchange, specification)
