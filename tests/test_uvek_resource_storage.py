import json
from copy import deepcopy
from pathlib import Path

import pytest

from brightpath import DATA_DIR
from brightpath.migrations.resources import load_uvek_technosphere_resource
from brightpath.migrations.schema import (
    dumps_migration_resource,
    intern_review,
    normalize_migration_resource,
    rule_review,
    validate_migration_resource,
)
from scripts.generate_uvek_migration_resources import _write_resource


def test_shared_reviews_and_inline_identities_round_trip_without_mutation(migration_resource):
    original = deepcopy(migration_resource)
    restored = normalize_migration_resource(migration_resource)
    serialized = dumps_migration_resource(restored)
    assert migration_resource == original == restored == json.loads(serialized)
    assert isinstance(restored["replace"][0]["target"], dict)
    assert len(restored["reviews"]) == 1
    assert restored["replace"][0]["review_ref"] == restored["replace"][1]["review_ref"]
    assert dumps_migration_resource(json.loads(serialized)) == serialized
    assert (
        intern_review(restored, rule_review(restored, restored["replace"][0])) == restored["replace"][0]["review_ref"]
    )
    assert len(restored["reviews"]) == 1


def test_copies_and_resolved_reviews_are_independently_owned(migration_resource):
    restored = normalize_migration_resource(migration_resource)
    restored["replace"][0]["target"]["name"] = "Changed"
    review = rule_review(restored, restored["replace"][0])
    review["limitations"].append("Changed")
    assert restored["replace"][1] == migration_resource["replace"][1]
    assert rule_review(restored, restored["replace"][0]) == rule_review(
        migration_resource, migration_resource["replace"][0]
    )
    assert migration_resource["replace"][0]["target"]["name"] == "Target"


def test_generator_uses_the_same_shared_writer(migration_resource, tmp_path):
    for filename in ("ecoinvent-to-uvek-2025.json", "uvek-2025-to-ecoinvent-3.12-cutoff.json"):
        path = tmp_path / filename
        _write_resource(path, migration_resource)
        assert path.read_text(encoding="utf-8") == dumps_migration_resource(migration_resource)
        assert json.loads(path.read_text(encoding="utf-8")) == migration_resource


def test_both_packaged_uvek_resources_use_one_schema():
    for filename, count in (("ecoinvent-to-uvek-2025.json", 31488), ("uvek-2025-to-ecoinvent-3.12-cutoff.json", 354)):
        path = DATA_DIR / "migrations/uvek" / filename
        result = json.loads(path.read_text(encoding="utf-8"))
        validate_migration_resource(result)
        assert result["schema"] == "brightpath.migration"
        assert result["schema_version"] == 3
        assert len(result["replace"]) == result["coverage"]["mapped_identities"] == count
        assert result["reverse_policy"] == "explicit_only"
        assert {
            "replace",
            "disaggregate",
            "delete",
            "reviews",
            "recipes",
            "source_profiles",
            "target_profile",
        } <= result.keys()
        assert not {"rules", "targets", "source_profile"} & result.keys()
        assert all(isinstance(rule["target"], dict) for rule in result["replace"])
        assert {rule["review_ref"] for rule in result["replace"] if "review_ref" in rule} == result["reviews"].keys()
    forward = load_uvek_technosphere_resource()
    assert len(forward["reviews"]) == 104
    assert (
        len(
            {
                tuple(rule["target"][field] for field in ("name", "reference product", "location", "unit"))
                for rule in forward["replace"]
            }
        )
        == 4043
    )


@pytest.mark.parametrize(
    "filename,expected",
    [
        ("ecoinvent-to-uvek-2025.json", "ea8ca00122a7ff5112cc55f099454a003e0ad6a41310dc4826bcedd8b2dcec84"),
        ("uvek-2025-to-ecoinvent-3.12-cutoff.json", "74ad437135442bad5fe82055ab8b4fda188a46a22eabb7a9c49a88cb7340112e"),
    ],
)
def test_all_mapping_values_and_runtime_evidence_match_the_baseline(filename, expected):
    # Reverse: the pre-upgrade v2 file. Forward: the saved pre-cleanup file,
    # projecting reviews to the retained rationale and limitations fields.
    import hashlib

    migration_resource = json.loads((DATA_DIR / "migrations/uvek" / filename).read_text(encoding="utf-8"))
    fields = ("source", "target", "conversion_factor", "classification", "confidence", "mapping_method")
    rows = []
    for rule in migration_resource["replace"]:
        row = {key: rule[key] for key in fields if key in rule}
        # Compare software-neutral method labels with their historical names.
        if row.get("mapping_method") in {"pairwise_review", "product_checked", "followup_review"}:
            row["mapping_method"] = "clic_" + row["mapping_method"]
        if "review_ref" in rule:
            row["review"] = rule_review(migration_resource, rule)
        if filename.startswith("uvek-"):
            row["id"] = rule["id"]
        rows.append(row)
    values = {"replace": rows, "recipes": migration_resource["recipes"]}
    encoded = json.dumps(values, sort_keys=True, ensure_ascii=False, separators=(",", ":"), allow_nan=False)
    assert hashlib.sha256(encoded.encode()).hexdigest() == expected


def test_packaged_uvek_resources_exclude_generation_and_audit_metadata():
    for path in (DATA_DIR / "migrations/uvek").glob("*.json"):
        resource = json.loads(path.read_text(encoding="utf-8"))
        assert not {"methodology", "source_catalogs", "evidence_sha256"} & resource.keys()
        for review in resource.get("reviews", {}).values():
            assert review.keys() <= {"rationale", "limitations", "evidence"}
        assert not any(rule.get("mapping_method", "").startswith("clic_") for rule in resource["replace"])
        assert not any(key.startswith("clic_") for key in resource["coverage"].get("mapping_methods", {}))
    forward = load_uvek_technosphere_resource()
    assert all(review.keys() == {"rationale", "limitations"} for review in forward["reviews"].values())


@pytest.mark.parametrize("default_encoding", ["utf-8", "cp1252"])
def test_authoring_json_io_preserves_unicode_with_any_default_encoding(tmp_path, monkeypatch, default_encoding):
    from scripts.generate_uvek_export import load, write

    original_open = Path.open

    def legacy_open(path, mode="r", buffering=-1, encoding=None, errors=None, newline=None):
        if "b" not in mode and encoding is None:
            encoding = default_encoding
        return original_open(path, mode, buffering, encoding, errors, newline)

    monkeypatch.setattr(Path, "open", legacy_open)
    path = tmp_path / "unicode-evidence.json"
    evidence = {"rationale": "Zürich → Genève", "confidence": 0.5}
    write(path, evidence)
    assert json.loads(path.read_bytes().decode("utf-8")) == evidence
    assert load(path) == evidence


def test_all_generated_helpers_match_the_pre_upgrade_output_baseline():
    import hashlib

    from brightpath.background.uvek_export import apply_uvek_export, load_uvek_export_resource
    from brightpath.core.policies import MigrationPolicy, PolicyAction

    migration_resource = load_uvek_export_resource()
    values = {}
    for rule in migration_resource["replace"]:
        if "recipe_ref" not in rule["target"]:
            continue
        data = [
            {
                "name": "Synthetic foreground",
                "reference product": "service",
                "location": "GLO",
                "unit": "unit",
                "exchanges": [
                    {
                        **rule["source"],
                        "type": "technosphere",
                        "amount": 2,
                        "uncertainty type": 3,
                        "loc": 2,
                        "scale": 0.1,
                    }
                ],
            }
        ]
        report, helpers = apply_uvek_export(data, MigrationPolicy(on_information_loss=PolicyAction.WARN))
        assert not report.has_errors
        values[rule["id"]] = {"data": data, "helpers": helpers}
    encoded = json.dumps(values, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    assert (
        hashlib.sha256(encoded.encode()).hexdigest()
        == "e913accb42429af3aca2bf2520a8d67df8dbec4679d85d3fef6f7f9a56463353"
    )
