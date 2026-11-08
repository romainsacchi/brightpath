"""Industrial waste-gas treatment keeps actual energy credits and fuel boundaries."""

from copy import deepcopy

import pytest
from test_uvek_reference_signs import FIELDS, consumer
from uvek_review_helpers import historical_forward_resource

from brightpath import TechnosphereProfile
from brightpath.background.reference_signs import select_profile_sign_rules
from brightpath.core.policies import MigrationPolicy, PolicyAction
from scripts.review_uvek_industrial_gas import (
    APPROVAL,
    GAS_SOURCES,
    PRIMITIVES,
    gas_decision,
    validate_credit_correspondences,
)

VERSIONS = ["3.6", "3.7", "3.8", "3.9", "3.10", "3.11", "3.12"]
POLICY = MigrationPolicy(on_heuristic_mapping=PolicyAction.WARN, on_information_loss=PolicyAction.WARN)


def batch(amount, *, representative=False):
    data = None
    for source in GAS_SOURCES:
        if representative and source[2] != "DE":
            continue
        for kind, uncertainty in (("technosphere", 2), ("substitution", 3)):
            row = consumer(dict(zip(FIELDS, source, strict=True)), amount, uncertainty, kind)
            row[0]["exchanges"][1]["caller_metadata"] = {"keep": [1, 2]}
            if data is None:
                data = row
            else:
                data[0]["exchanges"].append(row[0]["exchanges"][1])
    data[0]["comment"] = "Synthetic industrial gas combustion conversion fixture."
    data[0]["exchanges"].append(
        {
            "name": "Carbon dioxide, fossil",
            "categories": ["air"],
            "type": "biosphere",
            "unit": "kilogram",
            "amount": 0.5,
        }
    )
    return data


def profile_for(source, version):
    return next(p for p in GAS_SOURCES[source]["profiles"] if p["label"] == f"ecoinvent-{version}-consequential")


def test_industrial_gas_review_has_103_sources_721_exact_decisions_and_stable_energy_correspondences():
    resource = historical_forward_resource()
    validate_credit_correspondences(resource)
    rules = [r for r in resource["replace"] if tuple(r["source"][f] for f in FIELDS) in GAS_SOURCES]
    assert len(rules) == APPROVAL["source_count"] == 103
    assert (
        sum(len(r["reference_flow_sign_review"]["profiles"]) for r in rules) == APPROVAL["source_profile_count"] == 721
    )
    for r in rules:
        assert r["mapping_method"] == "reviewed_industrial_gas_recipe"
        assert all(
            p["source_profile"]["system_model"] == "consequential"
            and p["review"]["status"] == "verified"
            and p["conversion_factor"] == -1
            for p in r["reference_flow_sign_review"]["profiles"]
        )
    for r in APPROVAL["credit_correspondences"]:
        assert r["conversion_factor"] == 1 and r["source"]["unit"] == r["target"]["unit"]
    selected = select_profile_sign_rules({"replace": rules}, TechnosphereProfile("ecoinvent", "3.12", "cutoff"))
    assert all(r["reference_flow_sign_review"]["status"] == "unresolved" for r in selected["replace"])


@pytest.mark.parametrize("problem", ["source_hash", "profile", "primitive_hash", "prior_target", "factor"])
def test_industrial_gas_changed_evidence_cannot_inherit_credit_or_combustion_approval(problem):
    source, approval = next(iter(GAS_SOURCES.items()))
    rule = {"source": approval["source"], "target": deepcopy(approval["previous_target"])}
    observations = [(p["label"], -1) for p in approval["profiles"]]
    inventories = [(p["label"], p["inventory_sha256"]) for p in approval["profiles"]]
    primitives = {t: [("uvek-2025-cutoff", row["inventory_sha256"])] for t, row in PRIMITIVES.items()}
    if problem == "source_hash":
        inventories[0] = (inventories[0][0], "0" * 64)
    elif problem == "profile":
        observations[0] = ("ecoinvent-3.9.1-consequential", -1)
    elif problem == "primitive_hash":
        primitives[next(iter(primitives))] = [("uvek-2025-cutoff", "0" * 64)]
    elif problem == "prior_target":
        rule["target"]["name"] = "Unreviewed combustion supplier"
    else:
        rule["conversion_factor"] = 2
    with pytest.raises(ValueError, match="repeat the review"):
        gas_decision(rule, observations, inventories, primitives, "synthetic", None)


def test_industrial_gas_energy_mapping_changes_require_new_review():
    resource = deepcopy(historical_forward_resource())
    key = APPROVAL["credit_correspondences"][0]["rule_id"]
    rule = next(r for r in resource["replace"] if r["id"] == key)
    rule["target"]["location"] = "Unreviewed"
    with pytest.raises(ValueError, match="energy-credit correspondence changed"):
        validate_credit_correspondences(resource)
