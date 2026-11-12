"""Process heat keeps allocated EAF/rolling burdens rather than buying boiler heat."""

import json
import math
from copy import deepcopy

import pytest
from test_uvek_reference_signs import consumer, contexts

from brightpath import DATA_DIR, BrightwayInventory, TechnosphereProfile
from brightpath.background.rule_approvals import compatibility_rule_fingerprint
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource
from scripts.review_uvek_steel_39_process_heat import apply_steel_39_process_heat_review

REVIEW = json.loads((DATA_DIR / "export/uvek_steel_39_process_heat_review.json").read_text())
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
def test_allocated_heat_keeps_process_basis_signed_uncertainty_and_no_helpers(amount, uncertainty):
    data = graph(amount, uncertainty)
    before = deepcopy(data)
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.9")).migrate_background(
        contexts("uvek", "2026").background, policy=policy()
    )
    assert data == before and len(result.data) == 1
    for row, actual, original in zip(
        REVIEW["records"], result.data[0]["exchanges"][1:], before[0]["exchanges"][1:], strict=True
    ):
        factor = row["conversion_factor"]
        assert {k: actual[k] for k in FIELDS} == row["native_2026_witness"]["identity"]
        assert actual["amount"] == pytest.approx(amount * factor)
        assert actual["uncertainty type"] == uncertainty
        assert actual["formula"] == f"(dose) * ({factor})"
        if uncertainty == 2:
            assert actual["loc"] == pytest.approx(original["loc"] + math.log(factor))
            assert actual["negative"] == (amount < 0)
            assert actual["scale"] == original["scale"]
        else:
            for field in ("loc", "scale", "minimum", "maximum"):
                assert actual[field] == pytest.approx(original[field] * factor)


@pytest.mark.parametrize("version, model", [("3.10", "cutoff"), ("3.9", "consequential")])
def test_other_profiles_do_not_inherit_process_allocation(version, model):
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(graph(1), context=contexts("ecoinvent", version, model)).migrate_background(
            contexts("uvek", "2026").background, policy=policy()
        )


def test_each_factor_requires_complete_nominal_inventory_proportionality():
    assert len(REVIEW["records"]) == 3 and REVIEW["new_helpers"] == 0
    for row in REVIEW["records"]:
        proof = row["nominal_allocation_proof"]
        assert proof["nominal_vector_proportional"]
        for field in ("missing_source_keys", "missing_main_keys", "technosphere_mismatches", "biosphere_mismatches"):
            assert proof[field] == 0
        assert row["conversion_factor"] == proof["candidate_kg_equivalent_per_MJ"] > 0
        assert row["source_witness"]["identity"]["unit"] == "megajoule"
        assert row["native_2026_witness"]["identity"]["unit"] == "kilogram"
        for field in ("source_witness", "source_main_product_witness", "native_2026_witness"):
            assert row[field]["reference_amounts"] == [1] and row[field]["nonproduction_count"] > 0
        target = row["compiled_rule"]["target"]["name"]
        assert target in ("Hot rolling, steel", "Steel, electric, un- and low-alloyed, at plant")
        assert len(row["compiled_rule"]["reference_flow_sign_review"]["profiles"]) == 1


@pytest.mark.parametrize("drift", [None, 0, -1])
def test_review_replay_refuses_parent_drift_atomically(drift):
    resource = deepcopy(load_uvek_technosphere_resource())
    if drift is not None:
        rid = REVIEW["records"][drift]["rule_id"]
        next(r for r in resource["replace"] if r["id"] == rid)["confidence"] = 0.1
    before = deepcopy(resource)
    if drift is None:
        apply_steel_39_process_heat_review(resource)
    else:
        with pytest.raises(ValueError, match="parent changed"):
            apply_steel_39_process_heat_review(resource)
    assert resource == before
