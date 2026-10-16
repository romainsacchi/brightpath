import hashlib
import json
from copy import deepcopy
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator

from brightpath import DATA_DIR
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_biosphere_resources, load_technosphere_resources
from brightpath.migrations.schema import (
    SCHEMA_PATH,
    dumps_migration_resource,
    intern_review,
    normalize_migration_resource,
    rule_review,
    validate_migration_resource,
)


@pytest.mark.parametrize("confidence", [0, 1, 0.414702])
def test_confidence_preserves_its_exact_value(migration_resource, confidence):
    migration_resource["replace"][0]["confidence"] = confidence
    restored = normalize_migration_resource(migration_resource)
    assert restored["replace"][0]["confidence"] == confidence
    assert json.loads(dumps_migration_resource(restored))["replace"][0]["confidence"] == confidence


def test_missing_confidence_stays_missing_and_mean_uses_existing_scores(migration_resource):
    migration_resource["replace"][0]["confidence"] = 0
    migration_resource["replace"][1].pop("confidence")
    migration_resource["coverage"]["mean_confidence"] = 0
    restored = normalize_migration_resource(migration_resource)
    assert restored["replace"][0]["confidence"] == 0
    assert "confidence" not in restored["replace"][1]
    migration_resource["replace"][0].pop("confidence")
    with pytest.raises(MigrationError, match="confidence"):
        validate_migration_resource(migration_resource)


@pytest.mark.parametrize(
    "field,value",
    [
        ("confidence", -0.01),
        ("confidence", 1.01),
        ("confidence", True),
        ("confidence", None),
        ("confidence", float("nan")),
        ("confidence", float("inf")),
        ("conversion_factor", 0),
        ("conversion_factor", True),
        ("conversion_factor", None),
        ("conversion_factor", float("nan")),
        ("conversion_factor", float("inf")),
        ("mapping_method", ""),
        ("classification", "equivalent"),
        ("review_ref", "missing"),
        ("target", "t-legacy-reference"),
        ("target", {"name": "Incomplete"}),
        ("target", {"recipe_ref": "missing"}),
        ("factor", 1),
        ("recipe", "legacy"),
        ("review_provenance", {}),
    ],
)
def test_invalid_rule_values_are_rejected_without_mutating_inputs(migration_resource, field, value):
    migration_resource["replace"][0][field] = value
    before = repr(migration_resource)
    with pytest.raises(MigrationError):
        normalize_migration_resource(migration_resource)
    assert repr(migration_resource) == before


@pytest.mark.parametrize(
    "key",
    [
        "schema",
        "schema_version",
        "name",
        "source_profiles",
        "target_profile",
        "replace",
        "disaggregate",
        "delete",
        "recipes",
        "reviews",
        "coverage",
        "reverse_policy",
    ],
)
def test_missing_required_fields_are_rejected(migration_resource, key):
    migration_resource.pop(key)
    with pytest.raises(MigrationError):
        validate_migration_resource(migration_resource)


@pytest.mark.parametrize(
    "change",
    [
        "duplicate_source",
        "duplicate_id",
        "duplicate_profile",
        "unsupported_version",
        "foreign_schema",
        "reverse_policy",
        "invalid_profile",
        "old_layout",
        "mapped_count",
        "source_count",
        "negative_count",
        "method_counts",
        "dispositions",
        "nonfinite_metadata",
    ],
)
def test_semantic_conflicts_are_rejected(migration_resource, change):
    if change == "duplicate_source":
        migration_resource["replace"][1]["source"] = deepcopy(migration_resource["replace"][0]["source"])
    elif change == "duplicate_id":
        migration_resource["replace"][1]["id"] = migration_resource["replace"][0]["id"]
    elif change == "duplicate_profile":
        migration_resource["source_profiles"].append(deepcopy(migration_resource["source_profiles"][0]))
    elif change == "unsupported_version":
        migration_resource["schema_version"] = 4
    elif change == "foreign_schema":
        migration_resource["schema"] = "other.migration"
    elif change == "reverse_policy":
        migration_resource["reverse_policy"] = "infer_with_policy"
    elif change == "invalid_profile":
        migration_resource["target_profile"].pop("system_model")
    elif change == "old_layout":
        migration_resource["rules"] = []
    elif change == "mapped_count":
        migration_resource["coverage"]["mapped_identities"] = 0
    elif change == "source_count":
        migration_resource["coverage"]["source_identities"] = 1
    elif change == "negative_count":
        migration_resource["coverage"]["unresolved_identities"] = -1
    elif change == "method_counts":
        migration_resource["coverage"]["mapping_methods"] = {}
    elif change == "dispositions":
        migration_resource["coverage"]["dispositions"] = {}
    elif change == "nonfinite_metadata":
        migration_resource["custom_metadata"]["unexpected"] = float("inf")
    with pytest.raises(MigrationError):
        normalize_migration_resource(migration_resource)


def recipe_for(migration_resource):
    return {
        **migration_resource["replace"][0]["target"],
        "rationale": "Synthetic recipe",
        "evidence": ["Synthetic evidence"],
        "inputs": [
            {
                **migration_resource["replace"][0]["target"],
                "type": "technosphere",
                "amount": 0.5,
                "uncertainty type": 3,
                "loc": 0.5,
                "scale": 0.1,
            }
        ],
    }


def test_recipe_reference_and_uncertainty_survive_round_trip(migration_resource):
    recipe = recipe_for(migration_resource)
    migration_resource["recipes"]["helper"] = recipe
    migration_resource["replace"][0]["target"] = {"recipe_ref": "helper"}
    assert json.loads(dumps_migration_resource(migration_resource)) == migration_resource
    assert migration_resource["recipes"]["helper"]["inputs"][0]["scale"] == 0.1


@pytest.mark.parametrize(
    "change",
    [
        "cycle",
        "dependency",
        "output_unit",
        "ambiguous_target",
        "zero_inputs",
        "amount",
        "uncertainty",
        "bounds",
        "unclear_exchange",
        "supplier",
        "biosphere",
    ],
)
def test_invalid_recipes_are_rejected(migration_resource, change):
    recipe = recipe_for(migration_resource)
    migration_resource["recipes"]["helper"] = recipe
    migration_resource["replace"][0]["target"] = {"recipe_ref": "helper"}
    if change == "cycle":
        recipe["inputs"] = [{"recipe": "helper", "amount": 1}]
    elif change == "dependency":
        recipe["inputs"] = [{"recipe": "unknown", "amount": 1}]
    elif change == "output_unit":
        recipe["unit"] = "megajoule"
    elif change == "ambiguous_target":
        migration_resource["replace"][0]["target"].update(
            {k: recipe[k] for k in ("name", "reference product", "location", "unit")}
        )
    elif change == "zero_inputs":
        recipe["inputs"] = []
    elif change == "amount":
        recipe["inputs"][0]["amount"] = float("inf")
    elif change == "uncertainty":
        recipe["inputs"][0]["scale"] = -0.1
    elif change == "bounds":
        recipe["inputs"][0].update(minimum=2, maximum=1)
    elif change == "unclear_exchange":
        recipe["inputs"][0]["type"] = "unknown"
    elif change == "supplier":
        recipe["inputs"][0].pop("reference product")
    elif change == "biosphere":
        recipe["inputs"] = [{"name": "Flow", "type": "biosphere", "amount": 1, "unit": "kilogram"}]
    with pytest.raises(MigrationError):
        normalize_migration_resource(migration_resource)


def test_documented_proxy_needs_evidence_but_never_invents_confidence(migration_resource):
    rule = migration_resource["replace"][0]
    rule.pop("confidence")
    rule["classification"] = "proxy"
    rule["review_ref"] = intern_review(
        migration_resource, {"rationale": "A documented proxy", "evidence": ["A citation"]}
    )
    restored = normalize_migration_resource(migration_resource)
    assert "confidence" not in restored["replace"][0]
    assert rule_review(restored, restored["replace"][0])["evidence"] == ["A citation"]
    migration_resource["reviews"][rule["review_ref"]].pop("evidence")
    with pytest.raises(MigrationError, match="evidence"):
        normalize_migration_resource(migration_resource)


def test_disaggregation_and_deletion_follow_ecoinvent_operations(migration_resource):
    source = migration_resource["replace"].pop()["source"]
    migration_resource["disaggregate"] = [
        {
            "source": source,
            "targets": [
                {**migration_resource["replace"][0]["target"], "allocation": 0.25},
                {**migration_resource["replace"][0]["target"], "location": "RER", "allocation": 0.75},
            ],
        }
    ]
    migration_resource["delete"] = [{"source": {**source, "name": "Deleted"}}]
    migration_resource["coverage"] = {}
    assert normalize_migration_resource(migration_resource) == migration_resource
    migration_resource["disaggregate"][0]["targets"][0]["allocation"] = -0.25
    with pytest.raises(MigrationError):
        validate_migration_resource(migration_resource)


def test_exact_versions_and_unknown_metadata_survive_normalization(migration_resource):
    migration_resource["source_profiles"][0]["version"] = "3.10.1"
    migration_resource["target_profile"] = {"family": "ecoinvent", "version": "3.11.2", "system_model": "cutoff"}
    restored = normalize_migration_resource(migration_resource)
    assert restored == migration_resource
    assert restored["source_profiles"][0]["version"] == "3.10.1"
    assert restored["target_profile"]["version"] == "3.11.2"
    assert restored["replace"][0]["source"]["custom_field"] == [1, 2]


def test_offline_definition_and_all_upstream_resources_are_valid_without_rewriting():
    definition = json.loads(SCHEMA_PATH.read_text())
    Draft202012Validator.check_schema(definition)
    paths = list((DATA_DIR / "migrations/ecoinvent").rglob("*.json"))
    before = {path: hashlib.sha256(path.read_bytes()).hexdigest() for path in paths}
    for resources in (load_technosphere_resources("cutoff"), load_biosphere_resources()):
        for migration_resource in resources.values():
            validate_migration_resource(migration_resource)
            original = json.loads(Path(migration_resource["_path"]).read_text())
            for field in ("contributors", "licenses", "mapping", "version", "created", "homepage"):
                if field in original:
                    assert migration_resource[field] == original[field]
            for operation in ("replace", "disaggregate", "delete"):
                assert migration_resource[operation] == original.get(operation, [])
    assert before == {path: hashlib.sha256(path.read_bytes()).hexdigest() for path in paths}


def test_stable_rule_identifiers_ignore_unknown_source_metadata(migration_resource):
    from brightpath.migrations.schema import rule_identifier

    source = migration_resource["replace"][0]["source"]
    assert rule_identifier(migration_resource["name"], source) == rule_identifier(
        migration_resource["name"], {**source, "extra_note": "New metadata"}
    )


def test_unknown_underscore_metadata_is_serialized_but_internal_path_is_excluded(migration_resource):
    migration_resource["_custom"] = {"meaningful": [1, "é"]}
    migration_resource["_path"] = "/internal/resource.json"
    restored = json.loads(dumps_migration_resource(migration_resource))
    assert restored["_custom"] == migration_resource["_custom"]
    assert "_path" not in restored


@pytest.mark.parametrize("role,identifier", [("source", "ecoinvent-3.11-cutoff"), ("target", "uvek-2024-cutoff")])
def test_identifiers_cannot_contradict_profile_declarations(migration_resource, role, identifier):
    migration_resource[role + "_id"] = identifier
    with pytest.raises(MigrationError, match="profile"):
        normalize_migration_resource(migration_resource)


def test_legacy_alias_normalization_preserves_input_and_optional_scores(migration_resource):
    legacy = deepcopy(migration_resource)
    legacy.pop("schema")
    legacy["schema_version"] = 1
    legacy["source_profile"] = legacy.pop("source_profiles")[0]
    legacy["source_profile"].update(family="BAFU", version="2025")
    legacy["target_profile"] = {"family": "ecoinvent", "version": "3.12", "system_model": "cutoff"}
    legacy["replace"][0].pop("confidence")
    original = deepcopy(legacy)
    result = normalize_migration_resource(legacy)
    assert legacy == original
    assert result["source_profiles"][0]["family"] == "uvek"
    assert "confidence" not in result["replace"][0]


@pytest.mark.parametrize(
    "payload",
    [
        None,
        [],
        1,
        {"schema_version": 2},
        {"source_profile": None},
        {"source_profile": {"family": "ecoinvent", "versions": "3.12"}},
    ],
)
def test_malformed_legacy_resources_produce_domain_errors(payload):
    with pytest.raises(MigrationError):
        normalize_migration_resource(payload)
