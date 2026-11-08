"""Precious-metal product markets preserve shares and credits at the refinery gate."""

from collections import defaultdict
from copy import deepcopy

import pytest
from test_uvek_reference_signs import FIELDS, consumer, contexts
from test_uvek_refrigerant_disposal import target_context

from brightpath import BrightwayInventory, TechnosphereProfile
from brightpath.background.reference_signs import select_profile_sign_rules
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource
from scripts import review_uvek_precious_anode_markets as review

POLICY = MigrationPolicy(on_heuristic_mapping=PolicyAction.WARN, on_information_loss=PolicyAction.WARN)
CASES = [(source, profile) for source, row in review.SOURCES.items() for profile in row["profiles"]]


def evidence():
    records = deepcopy(review.APPROVAL["inventories"])
    observations = defaultdict(lambda: defaultdict(set))
    review.extend_observations(records, observations)
    native = {
        review.identity(review.APPROVAL["target"]): {
            "native_flow_type": "PRODUCT_FLOW",
            "native_reference_is_input": False,
        }
    }
    return records, observations, native


@pytest.mark.parametrize("source,profile", CASES)
@pytest.mark.parametrize("release", ("2025", "2026", "native2026"))
@pytest.mark.parametrize("amount", (-2, 0, 2))
def test_every_market_preserves_product_credit_sign_uncertainty_and_original_refinery_shares(
    source, profile, release, amount
):
    version = profile["label"].split("-")[1]
    data = consumer(dict(zip(FIELDS, source, strict=True)), amount, 2)
    data[0]["exchanges"].append(
        consumer(dict(zip(FIELDS, source, strict=True)), amount, 3, "substitution")[0]["exchanges"][1]
    )
    for e in data[0]["exchanges"][1:]:
        e["caller_metadata"] = {"retained": [1, 2]}
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
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", version))
    result = inventory.migrate_background(target_context(release), policy=POLICY)
    assert len(result.data) == 2
    assert inventory.data == data == before
    assert result.data[0]["exchanges"][0] == before[0]["exchanges"][0]
    assert result.data[0]["exchanges"][-1]["amount"] == 0.5
    helper = result.data[1]
    for old, new in zip(before[0]["exchanges"][1:-1], result.data[0]["exchanges"][1:-1], strict=True):
        assert new["amount"] == old["amount"] == amount
        assert new["type"] == old["type"]
        assert new["formula"] == old["formula"]
        assert new["loc"] == old["loc"] and new["scale"] == old["scale"]
        for field in ("minimum", "maximum", "negative"):
            assert (field in new) == (field in old)
            assert new.get(field) == old.get(field)
        assert new["caller_metadata"] == old["caller_metadata"]
        assert new["input"][1] == helper["code"]
    assert helper["exchanges"][0]["type"] == "production" and helper["exchanges"][0]["amount"] == 1
    inputs = helper["exchanges"][1:]
    assert len(inputs) == len(profile["inputs"])
    for actual, expected in zip(inputs, profile["inputs"], strict=True):
        assert actual["type"] == "technosphere" and actual["unit"] == "kilogram"
        assert actual["amount"] == expected["amount"]
        assert actual.get("uncertainty type", 0) == 0
        assert "Precious metals" in actual["name"] and "refinery" in actual["name"]
        assert "preparation plant" not in actual["name"]
    assert sum(e["amount"] for e in inputs) == pytest.approx(1)
    assert not helper["brightpath_conversion"].get("zero_burden")
    issues = result.last_migration_report.issues
    assert any("same-function, same-unit screening proxy" in i.message for i in issues)
    assert not any(i.code.endswith("link_unresolved") for i in issues)
    assert not any(i.code == "migration.technosphere_omission" for i in issues)
    assert result.migrate_background(target_context(release)).data == result.data


@pytest.mark.parametrize(
    "mutation",
    (
        "missing",
        "duplicate",
        "market",
        "provider",
        "formula",
        "metadata",
        "source_sign",
        "native_role",
        "native_direction",
        "reverse",
    ),
)
def test_changed_complete_boundaries_cannot_inherit_market_approval(mutation, monkeypatch, tmp_path):
    records, observations, native = evidence()
    review.validate_inventories(records, observations, native)
    if mutation == "missing":
        records.pop()
    elif mutation == "duplicate":
        records[-1] = records[0]
    elif mutation in ("market", "provider"):
        row = next(r for r in records if r["source"]["name"].startswith("market") == (mutation == "market"))
        row["inventory_sha256"] = "0" * 64
    elif mutation == "formula":
        records[0]["formula_sha256"] = "0" * 64
    elif mutation == "metadata":
        records[0]["activity_comment_sha256"] = "0" * 64
    elif mutation == "source_sign":
        source, row = next(iter(review.SOURCES.items()))
        observations["ecoinvent"][source].add((row["profiles"][0]["label"], -1))
    elif mutation == "native_role":
        native[review.identity(review.APPROVAL["target"])]["native_flow_type"] = "WASTE_FLOW"
    elif mutation == "native_direction":
        native[review.identity(review.APPROVAL["target"])]["native_reference_is_input"] = True
    else:
        path = tmp_path / review.APPROVAL["independent_reverse"]["resource_path"]
        path.parent.mkdir(parents=True)
        path.write_text("changed")
        monkeypatch.setattr(review, "ROOT", tmp_path)
    with pytest.raises(ValueError, match="changed|inspected positive product"):
        review.validate_inventories(records, observations, native)


@pytest.mark.parametrize("mutation", ("supplier", "factor", "inventory", "profile"))
def test_projection_rejects_unreviewed_supplier_factor_or_exact_profile(mutation):
    source, row = next(iter(review.SOURCES.items()))
    records, observations, _ = evidence()
    rule = {"source": deepcopy(row["source"]), "target": deepcopy(row["previous_target"])}
    if mutation == "supplier":
        rule["target"]["name"] = "Incoming scrap is not a refinery product"
    elif mutation == "factor":
        rule["conversion_factor"] = 0.932
    elif mutation == "inventory":
        next(r for r in records if review.identity(r["source"]) == source)["inventory_sha256"] = "0" * 64
    else:
        observations["ecoinvent"][source].add(("ecoinvent-3.12.1-cutoff", 1))
    with pytest.raises(ValueError, match="repeat the scientific review"):
        review.forward_decision(rule, observations["ecoinvent"][source], records, "test binding", None)


@pytest.mark.parametrize("source", sorted(review.SOURCES))
def test_strict_consent_local_foreground_and_cross_model_boundaries(source):
    version = review.SOURCES[source]["profiles"][0]["label"].split("-")[1]
    data = consumer(dict(zip(FIELDS, source, strict=True)), -2, 3)
    before = deepcopy(data)
    for model, policy in (("cutoff", MigrationPolicy.strict()), ("consequential", MigrationPolicy.permissive())):
        inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", version, model))
        with pytest.raises(MigrationError):
            inventory.migrate_background(target_context("2025"), policy=policy)
        assert inventory.data == before
    local = {
        **dict(zip(FIELDS, source, strict=True)),
        "exchanges": [{**dict(zip(FIELDS, source, strict=True)), "type": "production", "amount": 1}],
    }
    data.append(local)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", version))
    assert inventory.migrate_background(target_context("2025"), policy=POLICY).data == data
    selected = select_profile_sign_rules(
        load_uvek_technosphere_resource(), TechnosphereProfile("ecoinvent", "3.9.2", "cutoff")
    )
    rule = next(r for r in selected["replace"] if review.identity(r["source"]) == source)
    assert rule["reference_flow_sign_review"]["status"] == "unresolved"


def test_directional_evidence_and_nonempty_refinery_boundary_remain_complete():
    records, observations, native = evidence()
    review.validate_inventories(records, observations, native)
    assert len(records) == 30 and len(CASES) == 11 and len(review.SOURCES) == 3
    assert all(r["reference_amount"] == 1 and r["total_exchanges"] > 1 for r in records)
    assert review.APPROVAL["boundary_evidence"]["all18_producer_descriptions_share620_of621_native_characters"]
