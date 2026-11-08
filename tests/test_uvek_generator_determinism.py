"""Hash-randomized candidate shortlists must not rewrite reviewed migration resources."""

import hashlib
import json
import os
import subprocess
import sys
from copy import deepcopy
from pathlib import Path

import pytest

from brightpath.migrations.schema import rule_identifier
from scripts.generate_uvek_migration_resources import apply_heuristic_candidate_baselines
from scripts.review_uvek_reference_signs import apply_sign_review, rule_binding

ROOT = Path(__file__).resolve().parents[1]


def fingerprints(row):
    candidate = row["candidate"]
    row["candidate_sha256"] = hashlib.sha256(
        json.dumps(candidate, sort_keys=True, ensure_ascii=False).encode()
    ).hexdigest()
    row["reference_binding_sha256"] = rule_binding(candidate)


@pytest.fixture
def baseline():
    source = {"name": "Synthetic waste candidate", "reference product": "waste", "location": "CH", "unit": "kilogram"}
    target = {**source, "name": "Historical candidate"}
    resource = {"name": "synthetic-generator-baseline", "replace": []}
    candidate = {
        "id": rule_identifier(resource["name"], source),
        "source": source,
        "target": target,
        "mapping_method": "heuristic_similarity",
        "confidence": 0.2,
    }
    resource["replace"] = [
        {**deepcopy(candidate), "target": {**target, "name": "Different heuristic candidate"}, "confidence": 0.3}
    ]
    row = {"candidate": candidate}
    fingerprints(row)
    payload = {
        "schema_version": 1,
        "status": "authoring_baseline_only",
        "resource": resource["name"],
        "candidates": [row],
    }
    targets = {tuple(target[f] for f in ("name", "reference product", "location", "unit"))}
    return resource, payload, targets


def test_frozen_authoring_candidate_has_no_runtime_approval_effect(baseline):
    resource, payload, targets = baseline
    original = deepcopy(payload)
    apply_heuristic_candidate_baselines(resource, payload, targets)
    assert resource["replace"] == [payload["candidates"][0]["candidate"]]
    assert set(resource["replace"][0]) == {"id", "source", "target", "mapping_method", "confidence"}
    assert payload == original


@pytest.mark.parametrize(
    "change",
    [
        "status",
        "resource",
        "hash",
        "binding",
        "id",
        "duplicate",
        "missing_source",
        "source",
        "target_catalog",
        "unit",
        "method",
        "current_reviewed",
        "review_metadata",
        "factor",
        "confidence",
        "nonfinite",
        "boolean",
    ],
)
def test_invalid_or_reviewed_baselines_fail_without_changing_any_candidate(baseline, change):
    resource, payload, targets = baseline
    row = payload["candidates"][0]
    candidate = row["candidate"]
    current = resource["replace"][0]
    if change == "status":
        payload["status"] = "scientifically_approved"
    elif change == "resource":
        payload["resource"] = "another-route"
    elif change == "hash":
        row["candidate_sha256"] = "0" * 64
    elif change == "binding":
        row["reference_binding_sha256"] = "0" * 64
    elif change == "id":
        candidate["id"] = "invalid"
    elif change == "duplicate":
        payload["candidates"].append(deepcopy(row))
    elif change == "missing_source":
        resource["replace"] = []
    elif change == "source":
        current["source"]["location"] = "RER"
    elif change == "target_catalog":
        targets.clear()
    elif change == "unit":
        candidate["target"]["unit"] = "unit"
        targets.add(tuple(candidate["target"][f] for f in ("name", "reference product", "location", "unit")))
    elif change == "method":
        candidate["mapping_method"] = "reviewed"
    elif change == "current_reviewed":
        current["mapping_method"] = "pairwise_review"
    elif change == "review_metadata":
        candidate["reference_flow_sign_review"] = {"status": "verified"}
    elif change == "factor":
        current["conversion_factor"] = -1
    elif change == "confidence":
        candidate["confidence"] = 2
    elif change == "nonfinite":
        candidate["confidence"] = float("nan")
    else:
        candidate["confidence"] = True
    if change not in {"hash", "binding"}:
        fingerprints(row)
    before = deepcopy(resource)
    with pytest.raises(ValueError):
        apply_heuristic_candidate_baselines(resource, payload, targets)
    assert resource == before


def test_even_a_consistently_rehashed_candidate_cannot_bypass_the_existing_sign_audit(baseline):
    resource, payload, targets = baseline
    candidate = payload["candidates"][0]["candidate"]
    original_binding = rule_binding(candidate)
    candidate["target"]["name"] = "A different catalog-valid suggestion"
    targets.add(tuple(candidate["target"][f] for f in ("name", "reference product", "location", "unit")))
    fingerprints(payload["candidates"][0])
    apply_heuristic_candidate_baselines(resource, payload, targets)
    ledger = {
        "resources": {
            resource["name"]: {candidate["id"]: {"binding": original_binding, "original_factor": 1, "factor": 1}}
        }
    }
    with pytest.raises(ValueError, match="Stale reference-flow sign target"):
        apply_sign_review(resource, ledger)


def test_technosphere_and_biosphere_shortlists_are_identical_across_python_hash_seeds():
    code = """
import json,hashlib
from scripts.generate_uvek_migration_resources import _TechnosphereMatcher,_BiosphereMatcher,TechnosphereIdentity,BiosphereIdentity
terms=['aluminium','arsenic','barium','cadmium','chromium','cobalt','copper','nickel']
tech=[TechnosphereIdentity(f'{term} emission {i}',f'{term} emission {i}','CH','kilogram') for term in terms for i in range(80)]
bio=[BiosphereIdentity(f'{term} emission {i}',('air',),'kilogram') for term in terms for i in range(80)]
name=' '.join(terms)
t=_TechnosphereMatcher(tech,{},{});b=_BiosphereMatcher(bio)
source_t=TechnosphereIdentity(name,name,'CH','kilogram');source_b=BiosphereIdentity(name,('air',),'kilogram')
result=[t._candidates(source_t,'kilogram'),b._candidates(source_b,'kilogram'),t.match(source_t)[0].as_rule_identity(),b.match(source_b)[0].as_rule_identity()]
print(json.dumps(result,sort_keys=True))
"""
    outputs = []
    for seed in ("0", "1", "42", "314159"):
        env = {**os.environ, "PYTHONHASHSEED": seed, "PYTHONPATH": str(ROOT)}
        result = subprocess.run(
            [sys.executable, "-c", code], cwd=ROOT, env=env, text=True, capture_output=True, check=True
        )
        outputs.append(json.loads(result.stdout.strip().splitlines()[-1]))
    assert outputs[1:] == outputs[:-1]


def test_packaged_baselines_preserve_exact_historical_inputs_and_do_not_replace_runtime_decisions():
    resource = json.loads((ROOT / "brightpath/data/migrations/uvek/ecoinvent-to-uvek-2025.json").read_text())
    ledger = json.loads((ROOT / "brightpath/data/export/uvek_reference_flow_sign_review.json").read_text())[
        "resources"
    ][resource["name"]]
    payload = json.loads((ROOT / "brightpath/data/export/uvek_heuristic_candidate_baselines.json").read_text())
    rules = {r["id"]: r for r in resource["replace"]}
    assert len(payload["candidates"]) == 9
    for row in payload["candidates"]:
        candidate = row["candidate"]
        current = rules[candidate["id"]]
        decision = ledger.get(candidate["id"], {})
        original = decision.get("original_rule", current)
        assert candidate == {k: original[k] for k in ("id", "source", "target", "mapping_method", "confidence")}
        assert row["reference_binding_sha256"] == rule_binding(candidate)
        assert set(candidate) == {"id", "source", "target", "mapping_method", "confidence"}
        if decision:
            assert decision["binding"] == row["reference_binding_sha256"]
    # This stage accepts only raw authoring records, never reviewed runtime records.
    before = deepcopy(resource)
    targets = {
        tuple(row["candidate"]["target"][f] for f in ("name", "reference product", "location", "unit"))
        for row in payload["candidates"]
    }
    with pytest.raises(ValueError):
        apply_heuristic_candidate_baselines(resource, payload, targets)
    assert resource == before
