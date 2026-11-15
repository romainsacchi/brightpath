"""The train supply review adds only its two independently verified profiles."""

import json
from copy import deepcopy

import pytest

from brightpath import DATA_DIR, TechnosphereProfile
from brightpath.background.reference_signs import select_profile_sign_rules
from scripts.review_uvek_hydrogen_train_direct_312 import apply_hydrogen_train_direct_312_review


def evidence():
    return json.loads((DATA_DIR / "export/uvek_hydrogen_train_direct_312_review.json").read_text())


def test_replay_preserves_old_profile_decisions_and_is_idempotent():
    rows = evidence()["profile_sign_extensions"]
    resource = {
        "source_profiles": [{"system_model": "cutoff"}],
        "replace": [deepcopy(r["original_rule"]) for r in rows],
    }
    original = deepcopy(resource)
    apply_hydrogen_train_direct_312_review(resource)
    once = deepcopy(resource)
    apply_hydrogen_train_direct_312_review(resource)
    assert resource == once
    for version in ("3.9", "3.10", "3.11"):
        profile = TechnosphereProfile("ecoinvent", version, "cutoff")
        assert select_profile_sign_rules(resource, profile) == select_profile_sign_rules(original, profile)
    selected = select_profile_sign_rules(resource, TechnosphereProfile("ecoinvent", "3.12", "cutoff"))
    for rule in selected["replace"]:
        assert rule["conversion_factor"] == 1
        assert rule["reference_flow_sign_review"]["status"] == "verified"
        assert rule["reference_flow_sign_review"]["source_sign"] == 1
        assert rule["reference_flow_sign_review"]["target_sign"] == 1
    with pytest.raises(ValueError, match="system model"):
        select_profile_sign_rules(resource, TechnosphereProfile("ecoinvent", "3.12", "consequential"))


def test_changed_parent_is_rejected_before_any_mutation():
    rows = evidence()["profile_sign_extensions"]
    resource = {"replace": [deepcopy(r["original_rule"]) for r in rows]}
    resource["replace"][1]["conversion_factor"] = 2
    before = deepcopy(resource)
    with pytest.raises(ValueError, match="parent changed"):
        apply_hydrogen_train_direct_312_review(resource)
    assert resource == before
