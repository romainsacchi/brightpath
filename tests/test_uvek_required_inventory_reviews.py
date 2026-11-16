"""Late mapping-data reviews reject drift atomically without changing runtime code."""

import hashlib
import json
from copy import deepcopy

import pytest

from brightpath import TechnosphereProfile
from brightpath.background.rule_approvals import compatibility_rule_fingerprint
from brightpath.migrations.schema import intern_review
from scripts.review_uvek_required_inventories import INDEX_PATH, apply_required_inventory_reviews


def write_review(root, review, *, second_invalid=False):
    path = root / "brightpath/data/export/synthetic_required_review.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(review))
    entry = {"path": path.relative_to(root).as_posix(), "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}
    reviews = [entry, {**entry, "sha256": "0" * 64}] if second_invalid else [entry]
    (root / INDEX_PATH).write_text(json.dumps({"schema_version": 1, "reviews": reviews}))


@pytest.fixture
def review_case(migration_resource):
    resource = deepcopy(migration_resource)
    source = resource["source_profiles"][0]
    old = deepcopy(resource["replace"][0])
    card = {"rationale": "Independently reviewed synthetic supply", "evidence": ["Synthetic fixture"]}
    compiled = {
        **deepcopy(old),
        "target": {"recipe_ref": "reviewed"},
        "conversion_factor": 1,
        "review_ref": intern_review({}, card),
    }
    compiled["reference_flow_sign_review"] = {
        "status": "unresolved",
        "reason": "Exact inspected source only",
        "evidence": ["Synthetic fixture"],
        "profiles": [
            {
                "source_profile": source,
                "conversion_factor": 1,
                "target": {"recipe_ref": "reviewed"},
                "review": {
                    "status": "verified",
                    "source_sign": 1,
                    "target_sign": 1,
                    "reason": "One unit of product",
                    "evidence": ["Synthetic fixture"],
                },
            }
        ],
    }
    dependency = resource["replace"][1]
    fingerprint = compatibility_rule_fingerprint(
        resource,
        "replace",
        dependency,
        TechnosphereProfile(**source),
        TechnosphereProfile(**resource["target_profile"]),
    )
    review = {
        "schema_version": 1,
        "source_profile": source,
        "target_profile": resource["target_profile"],
        "records": [{"rule_id": old["id"], "original_rule": old, "compiled_rule": compiled}],
        "reviews": [card],
        "recipes": {
            "reviewed": {
                "name": "Synthetic supply",
                "reference product": "product",
                "location": "CH",
                "unit": "kilogram",
                "inputs": [
                    {
                        "type": "biosphere",
                        "name": "Carbon dioxide, fossil",
                        "categories": ["air"],
                        "unit": "kilogram",
                        "amount": 1,
                    }
                ],
                "rationale": "Synthetic test supply",
                "evidence": ["Synthetic fixture"],
            }
        },
        "source_rule_dependency_scopes": [{"rule_id": dependency["id"], "source": source, "fingerprint": fingerprint}],
        "recipe_dependency_sha256": {},
    }
    return resource, review


def test_valid_review_changes_only_the_declared_supply_and_is_idempotent(tmp_path, review_case):
    resource, review = review_case
    untouched = deepcopy(resource["replace"][1])
    write_review(tmp_path, review)
    apply_required_inventory_reviews(resource, tmp_path)
    assert resource["replace"][0] == review["records"][0]["compiled_rule"]
    assert resource["replace"][1] == untouched
    assert resource["custom_metadata"] == {"preserved": True}
    before = deepcopy(resource)
    apply_required_inventory_reviews(resource, tmp_path)
    assert resource == before


@pytest.mark.parametrize(
    "drift,match",
    [
        ("digest", "review changed"),
        ("parent", "parent changed"),
        ("component", "component mapping changed"),
        ("recipe", "recipe changed"),
        ("profile", "source system models"),
        ("guard", "exact reviewed source profiles"),
        ("second_review", "review changed"),
    ],
)
def test_review_or_dependency_drift_leaves_the_input_resource_unchanged(tmp_path, review_case, drift, match):
    resource, review = review_case
    if drift == "parent":
        resource["replace"][0]["confidence"] = 0.123
    elif drift == "component":
        resource["replace"][1]["conversion_factor"] = 3
    elif drift == "recipe":
        resource["recipes"]["reviewed"] = {**review["recipes"]["reviewed"], "inputs": []}
    elif drift == "profile":
        review["source_profile"] = {**review["source_profile"], "system_model": "consequential"}
    elif drift == "guard":
        review["records"][0]["compiled_rule"]["reference_flow_sign_review"]["status"] = "verified"
    write_review(tmp_path, review, second_invalid=drift == "second_review")
    if drift == "digest":
        path = tmp_path / INDEX_PATH
        index = json.loads(path.read_text())
        index["reviews"][0]["sha256"] = "0" * 64
        path.write_text(json.dumps(index))
    before = deepcopy(resource)
    with pytest.raises(ValueError, match=match):
        apply_required_inventory_reviews(resource, tmp_path)
    assert resource == before
