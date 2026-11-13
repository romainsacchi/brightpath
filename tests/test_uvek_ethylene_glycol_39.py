"""Ethylene glycol keeps its named-product kg basis without duplicating embedded upstream burdens."""

import json
from copy import deepcopy

import pytest
from test_uvek_reference_signs import consumer, contexts

from brightpath import DATA_DIR, BrightwayInventory, TechnosphereProfile
from brightpath.background.rule_approvals import compatibility_rule_fingerprint
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource
from brightpath.migrations.scaling import scale_exchange

REVIEW = json.loads((DATA_DIR / "export/uvek_ethylene_glycol_39_review.json").read_text())
ROW = REVIEW["records"][0]


def policy(approved=True):
    r = load_uvek_technosphere_resource()
    rule = next(x for x in r["replace"] if x["id"] == ROW["rule_id"])
    fp = compatibility_rule_fingerprint(
        r,
        "replace",
        rule,
        TechnosphereProfile("ecoinvent", "3.9", "cutoff"),
        TechnosphereProfile("uvek", "2025", "cutoff"),
    )
    return MigrationPolicy(
        approved_compatibility_rules=(fp,) if approved else (), on_information_loss=PolicyAction.WARN
    )


@pytest.mark.parametrize("amount", [-2.5, 2.5])
@pytest.mark.parametrize("uncertainty", [2, 3])
@pytest.mark.parametrize("release", ["2025", "2026"])
def test_whole_chemical_keeps_signed_demand_uncertainty_and_separate_emissions(amount, uncertainty, release):
    data = consumer(ROW["original_rule"]["source"], amount, uncertainty)
    data[0]["exchanges"].append(
        {
            "name": "Carbon dioxide, fossil",
            "categories": ["air"],
            "unit": "kilogram",
            "type": "biosphere",
            "amount": 0.5,
        }
    )
    before = deepcopy(data)
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.9")).migrate_background(
        contexts("uvek", release).background, policy=policy()
    )
    assert data == before and len(result.data) == 1
    actual = result.data[0]["exchanges"][1]
    expected = deepcopy(before[0]["exchanges"][1])
    scale_exchange(expected, 1)
    for k in ("amount", "uncertainty type", "loc", "scale", "negative", "formula", "minimum", "maximum"):
        if k in expected:
            assert actual[k] == expected[k]
    assert all(actual[k] == value for k, value in ROW["original_rule"]["target"].items())
    assert result.data[0]["exchanges"][-1] == before[0]["exchanges"][-1]


def test_exact_39_service_requires_its_own_approval():
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(
            consumer(ROW["original_rule"]["source"], -1), context=contexts("ecoinvent", "3.9")
        ).migrate_background(contexts("uvek", "2026").background, policy=policy(False))


@pytest.mark.parametrize(
    "version,model", [("3.8", "cutoff"), ("3.9.1", "cutoff"), ("3.10", "cutoff"), ("3.9", "consequential")]
)
def test_39_scope_does_not_expand_to_other_profiles(version, model):
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(
            consumer(ROW["original_rule"]["source"], -1), context=contexts("ecoinvent", version, model)
        ).migrate_background(contexts("uvek", "2026").background, policy=policy())


def test_chemical_basis_and_embedded_energy_limit_are_explicit():
    resource = load_uvek_technosphere_resource()
    assert (
        next(r for r in resource["replace"] if r["id"] == ROW["rule_id"])
        == ROW["original_rule"]
        == ROW["compiled_rule"]
    )
    assert ROW["source_witness"]["reference_amounts"] == [1]
    assert all(w["reference_amounts"] == [1] for w in ROW["native_witnesses"].values())
    facts = ROW["boundary_checks"]
    assert facts["source_CAS"] == "107-21-1" and facts["source_formula"] == "C2H6O2"
    assert facts["source_pure_substance"]
    assert facts["native_exact_assay_independently_specified"] is False
    assert facts["native_named_product"] == "monoethylene glycol"
    assert facts["coproduct_yield_or_purity_multiplier_added"] is False
    assert facts["extra_source_feedstock_or_energy_added"] is False
    assert facts["native_explicit_electricity_or_heat_purchase_entries"] == 0
    assert facts["embedded_upstream_energy_can_be_updated_by_ordinary_supplier_substitution"] is False
    assert facts["native2026_biosphere_entries"] == 354
    assert REVIEW["new_helpers"] == 0 and REVIEW["new_source_omissions"] == []


def test_this_chemical_permission_does_not_admit_the_dimethyl_ether():
    ether = {
        "name": "ethylene glycol dimethyl ether production",
        "reference product": "ethylene glycol dimethyl ether",
        "location": "RoW",
        "unit": "kilogram",
    }
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(consumer(ether, 1), context=contexts("ecoinvent", "3.9")).migrate_background(
            contexts("uvek", "2026").background, policy=policy()
        )
