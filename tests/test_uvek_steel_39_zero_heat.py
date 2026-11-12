"""Zero allocated heat must never imply free negative-reference waste treatment."""

import json
from copy import deepcopy

import pytest
from test_uvek_reference_signs import consumer, contexts

from brightpath import DATA_DIR, BrightwayInventory, TechnosphereProfile
from brightpath.background.rule_approvals import compatibility_rule_fingerprint
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource
from scripts.review_uvek_steel_39_zero_heat import apply_steel_39_zero_heat_review

REVIEW = json.loads((DATA_DIR / "export/uvek_steel_39_zero_heat_review.json").read_text())
FIELDS = ("name", "reference product", "location", "unit")


def graph(amount, uncertainty=0):
    data = consumer(REVIEW["records"][0]["source_witness"]["identity"], amount, uncertainty)
    data[0]["exchanges"] = data[0]["exchanges"][:1] + [
        consumer(row["source_witness"]["identity"], amount, uncertainty)[0]["exchanges"][1] for row in REVIEW["records"]
    ]
    return data


def policy():
    resource = load_uvek_technosphere_resource()
    indexed = {r["id"]: r for r in resource["replace"]}
    scopes = tuple(
        compatibility_rule_fingerprint(
            resource,
            "replace",
            indexed[row["rule_id"]],
            TechnosphereProfile("ecoinvent", "3.9", "cutoff"),
            TechnosphereProfile("uvek", "2025", "cutoff"),
        )
        for row in REVIEW["records"]
    )
    return MigrationPolicy(approved_compatibility_rules=scopes, on_information_loss=PolicyAction.WARN)


@pytest.mark.parametrize("amount", [-2, 2])
@pytest.mark.parametrize("uncertainty", [2, 3])
def test_all_zero_heat_handovers_preserve_signed_uncertainty_without_helpers(amount, uncertainty):
    data = graph(amount, uncertainty)
    before = deepcopy(data)
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.9")).migrate_background(
        contexts("uvek", "2026").background, policy=policy()
    )
    assert data == before and len(result.data) == 1
    edges = result.data[0]["exchanges"][1:]
    assert len(edges) == 36
    target = REVIEW["native_2026_witness"]["identity"]
    for actual, expected in zip(edges, before[0]["exchanges"][1:], strict=True):
        assert {k: actual[k] for k in FIELDS} == target
        for field in ("amount", "uncertainty type", "loc", "scale", "minimum", "maximum", "negative", "formula"):
            if field in expected:
                assert actual[field] == expected[field]


@pytest.mark.parametrize("version, model", [("3.10", "cutoff"), ("3.9", "consequential")])
def test_other_context_does_not_inherit_zero_allocation(version, model):
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(graph(1), context=contexts("ecoinvent", version, model)).migrate_background(
            contexts("uvek", "2026").background, policy=policy()
        )


def test_zero_proofs_exclude_waste_references_and_nonzero_reuse_paths():
    assert REVIEW["direct_empty_sources"] == 12 and REVIEW["fully_zero_forwarders"] == 24
    assert REVIEW["new_helpers"] == 0
    assert REVIEW["native_2026_witness"]["reference_amounts"] == [1]
    assert REVIEW["native_2026_witness"]["nonproduction_count"] == 0
    sources = {tuple(row["source_witness"]["identity"][k] for k in FIELDS) for row in REVIEW["records"]}
    for row in REVIEW["records"]:
        assert row["source_witness"]["reference_amounts"] == [1]
        assert row["source_witness"]["identity"]["unit"] == "megajoule"
        assert len(row["compiled_rule"]["reference_flow_sign_review"]["profiles"]) == 1
        for control in row["allocation_control_witnesses"]:
            assert control["reference_amounts"] == [-1] and control["nonproduction_count"] > 0
            assert control["identity"]["unit"] == "kilogram"
            assert tuple(control["identity"][k] for k in FIELDS) not in sources
    assert len(REVIEW["excluded_nonzero_heat_forwarders"]) == 2
    for row in REVIEW["excluded_nonzero_heat_forwarders"]:
        assert tuple(row["source"][k] for k in FIELDS) not in sources
        assert row["nonempty_heat_inputs"] == 3 and row["zero_terminal_weight"] < 1


@pytest.mark.parametrize("drift", [None, 0, -1])
def test_review_replay_is_atomic_even_when_the_last_parent_drifts(drift):
    resource = deepcopy(load_uvek_technosphere_resource())
    if drift is not None:
        rid = REVIEW["records"][drift]["rule_id"]
        next(r for r in resource["replace"] if r["id"] == rid)["confidence"] = 0.1
    before = deepcopy(resource)
    if drift is None:
        apply_steel_39_zero_heat_review(resource)
    else:
        with pytest.raises(ValueError, match="parent changed"):
            apply_steel_39_zero_heat_review(resource)
    assert resource == before


@pytest.mark.parametrize("row", REVIEW["excluded_nonzero_heat_forwarders"], ids=lambda row: row["source"]["location"])
def test_nonzero_internal_reuse_paths_do_not_match_zero_heat_approvals(row):
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(
            consumer(row["source"], 1), context=contexts("ecoinvent", "3.9")
        ).migrate_background(contexts("uvek", "2026").background, policy=policy())


def test_heat_approvals_do_not_authorize_negative_kg_waste_treatment():
    controls = [w for row in REVIEW["records"] for w in row["allocation_control_witnesses"]]
    assert len(controls) == 12
    data = consumer(controls[0]["identity"], -1)
    data[0]["exchanges"] = data[0]["exchanges"][:1] + [consumer(w["identity"], -1)[0]["exchanges"][1] for w in controls]
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.9")).migrate_background(
            contexts("uvek", "2026").background, policy=policy()
        )
