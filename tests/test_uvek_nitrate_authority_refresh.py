"""A reviewed witness-metadata extension must not alter nitrate modelling or weaken guards."""

import hashlib
import json
from copy import deepcopy

import pytest

from brightpath import DATA_DIR
from scripts import review_uvek_pilot_nitrate_market as nitrate

AUDIT_PATH = "export/uvek_nitrate_authority_refresh_review.json"
AUDIT = json.loads((DATA_DIR / AUDIT_PATH).read_text())


def digest(value):
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode()
    ).hexdigest()


def test_only_the_reviewed_pump_witness_extension_changed_the_legacy_authority():
    raw = (DATA_DIR / AUDIT["legacy_review_path"]).read_bytes()
    current = json.loads(raw)
    original = AUDIT["original_legacy_review"]
    assert hashlib.sha256(raw).hexdigest() == AUDIT["current_legacy_review_sha256"]
    assert any(
        hashlib.sha256((json.dumps(original, indent=2, ensure_ascii=flag) + "\n").encode()).hexdigest()
        == AUDIT["historical_legacy_review_sha256"]
        for flag in (False, True)
    )
    expected = deepcopy(original)
    index = AUDIT["permitted_extension_record_index"]
    for key in AUDIT["permitted_extension_fields"]:
        expected["records"][index][key] = deepcopy(current["records"][index][key])
    assert expected == current
    before = original["records"][index]["source_profiles"]
    after = current["records"][index]["source_profiles"]
    assert len(before) == 1 and len(after) == 7 and after[: len(before)] == before
    assert all(row["reference_amount"] == 1 for row in after)
    assert [
        dict(source_profile=r["source_profile"], reference_amount=r["reference_amount"]) for r in after[1:]
    ] == AUDIT["additional_pump_source_profile_witnesses"]


def test_all_nitrate_physical_payload_and_existing_authorities_are_retained():
    payload = json.loads((DATA_DIR / "export/uvek_pilot_nitrate_market_review.json").read_text())
    evidence = payload.pop("authorization_evidence")
    for annotation in AUDIT["existing_compiled_source_annotations_restored_to_authoring"]:
        edge = payload["auxiliary_recipes"][annotation["recipe_id"]]["inputs"][annotation["input_index"]]
        assert edge.pop("comment") == annotation["comment"]
        assert digest(edge) == annotation["unchanged_input_without_comment_sha256"]
    assert digest(payload) == AUDIT["unchanged_nitrate_payload_without_authorization_sha256"]
    expected = deepcopy(AUDIT["original_nitrate_authorization_evidence"])
    row = next(r for r in expected if r["path"] == AUDIT["legacy_review_path"])
    assert row["sha256"] == AUDIT["historical_legacy_review_sha256"]
    row["sha256"] = AUDIT["current_legacy_review_sha256"]
    expected.append({"path": AUDIT_PATH, "sha256": hashlib.sha256((DATA_DIR / AUDIT_PATH).read_bytes()).hexdigest()})
    assert evidence == expected
    assert len(payload["external_scoped_bindings"]) == AUDIT["nitrate_external_rule_bindings_count"] == 125
    assert not any(r["rule_id"] == AUDIT["pump_rule_id"] for r in payload["external_scoped_bindings"])
    assert AUDIT["nitrate_physical_rules_recipe_quantities_and_scopes_changed"] is False


class ReachedPhysicalValidator(Exception):
    """Stop a metadata-only unit check at the unchanged physical validators."""


def test_refreshed_metadata_reaches_the_existing_physical_validator(monkeypatch):
    def stop(*args, **kwargs):
        raise ReachedPhysicalValidator

    monkeypatch.setattr(nitrate, "validate_composition_bindings", stop)
    resource = {"unmodified": [1, 2, 3]}
    before = deepcopy(resource)
    with pytest.raises(ReachedPhysicalValidator):
        nitrate.apply_pilot_nitrate_market_review(resource)
    assert resource == before


@pytest.mark.parametrize("changed", ["nitrate", "legacy", "audit"])
def test_any_further_unreviewed_metadata_or_quantity_drift_still_refuses(changed, monkeypatch):
    from pathlib import Path

    targets = {
        "nitrate": nitrate.ROOT / nitrate.REVIEW_PATH,
        "legacy": DATA_DIR / AUDIT["legacy_review_path"],
        "audit": DATA_DIR / AUDIT_PATH,
    }
    target = targets[changed]
    original = Path.read_bytes

    def altered(path):
        raw = original(path)
        return raw + b"\n" if path == target else raw

    monkeypatch.setattr(Path, "read_bytes", altered)
    resource = {"unmodified": [1, 2, 3]}
    before = deepcopy(resource)
    with pytest.raises(ValueError, match="changed"):
        nitrate.apply_pilot_nitrate_market_review(resource)
    assert resource == before
