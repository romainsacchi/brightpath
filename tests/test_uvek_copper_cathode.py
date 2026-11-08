"""Bulk copper migration preserves signed caller state and never supplies a battery electrode."""

import hashlib
import json
from collections import defaultdict
from copy import deepcopy

import pytest
from test_uvek_reference_signs import FIELDS, consumer, contexts
from test_uvek_refrigerant_disposal import target_context

from brightpath import BrightwayInventory
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource
from scripts import review_uvek_copper_cathode as review

SOURCE = dict(zip(FIELDS, review.SOURCE, strict=True))
TARGET = dict(zip(FIELDS, review.TARGET, strict=True))
POLICY = MigrationPolicy(on_heuristic_mapping=PolicyAction.WARN, on_information_loss=PolicyAction.WARN)


@pytest.mark.parametrize("version", sorted(review.VERSIONS))
@pytest.mark.parametrize("release", ("2025", "2026", "native2026"))
@pytest.mark.parametrize("amount", (-2, 0, 2))
def test_whole_copper_preserves_mass_sign_uncertainty_formulas_metadata_and_separate_emissions(
    version, release, amount
):
    data = consumer(SOURCE, amount, 2)
    data[0]["comment"] = "Synthetic private consumer."
    data[0]["exchanges"].append(consumer(SOURCE, amount, 3, "substitution")[0]["exchanges"][1])
    for e in data[0]["exchanges"][1:]:
        e["caller_metadata"] = {"retain": [1, 2]}
    data[0]["exchanges"].append(
        {
            "name": "Carbon dioxide, fossil",
            "categories": ["air"],
            "unit": "kilogram",
            "amount": 0.5,
            "type": "biosphere",
        }
    )
    before = deepcopy(data)
    original = BrightwayInventory.from_data(data, context=contexts("ecoinvent", version))
    result = original.migrate_background(target_context(release), policy=POLICY)
    assert len(result.data) == 1
    for a, b in zip(before[0]["exchanges"][1:-1], result.data[0]["exchanges"][1:-1], strict=True):
        assert {f: b[f] for f in FIELDS} == TARGET
        assert all(b[k] == v for k, v in a.items() if k not in FIELDS)
    assert result.data[0]["exchanges"][0] == before[0]["exchanges"][0]
    assert result.data[0]["exchanges"][-1]["amount"] == 0.5
    assert result.data[0]["exchanges"][-1]["type"] == "biosphere"
    assert original.data == data == before
    issues = result.last_migration_report.issues
    assert any(i.code == "migration.technosphere_information_loss" for i in issues)
    assert not any(i.code == "migration.zero_burden_supplier" for i in issues)
    assert result.migrate_background(target_context(release)).data == result.data


@pytest.mark.parametrize("release", ("2025", "native2026"))
def test_strict_consent_boundary_and_consequential_refusal_leave_source_unchanged(release):
    data = consumer(SOURCE, -2, 3)
    before = deepcopy(data)
    for model in ("cutoff", "consequential"):
        inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.12", model))
        with pytest.raises(MigrationError):
            inventory.migrate_background(target_context(release), policy=MigrationPolicy.strict())
        assert inventory.data == before
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.12", "consequential"))
    with pytest.raises(MigrationError, match="system models"):
        inventory.migrate_background(target_context(release), policy=MigrationPolicy.permissive())
    assert inventory.data == before


def test_caller_owned_matching_foreground_copper_supplier_is_preserved():
    data = consumer(SOURCE, -2, 3)
    data.append({**SOURCE, "exchanges": [{**SOURCE, "type": "production", "amount": 1}]})
    original = deepcopy(data)
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.12")).migrate_background(
        target_context("2025"), policy=POLICY
    )
    assert result.data == data == original


@pytest.mark.parametrize("amount", (-2, 0, 2))
def test_separately_authored_reverse_copper_proxy_preserves_signed_mass_and_metadata(amount):
    data = consumer(TARGET, amount, 3)
    before = deepcopy(data)
    result = BrightwayInventory.from_data(data, context=contexts("uvek", "2025")).migrate_background(
        contexts("ecoinvent", "3.12").background, policy=POLICY
    )
    after = result.data[0]["exchanges"][1]
    assert {f: after[f] for f in FIELDS} == SOURCE
    assert all(after[k] == v for k, v in before[0]["exchanges"][1].items() if k not in FIELDS)
    assert data == before


def evidence():
    inventories, observations, native = defaultdict(list), {"ecoinvent": defaultdict(set), "uvek": defaultdict(set)}, {}
    for r in review.APPROVAL["inventories"]:
        s = tuple(r["source"][f] for f in FIELDS)
        inventories[s].append({**deepcopy(r), "source": list(s)})
        observations[r["label"].split("-")[0]][s].add((r["label"], 1))
    for r in review.APPROVAL["native_roles"]:
        native[tuple(r["source"][f] for f in FIELDS)] = deepcopy(r)
    return inventories, observations, native


@pytest.mark.parametrize(
    "mutation", ("inventory", "metadata", "missing", "duplicate", "source_sign", "native_role", "mass_unit")
)
def test_complete_inventory_and_role_drift_rejects_authoring(mutation):
    inventories, observations, native = evidence()
    review.validate_inventories(inventories, observations, native)
    if mutation == "inventory":
        inventories[review.SOURCE][0]["inventory_sha256"] = "0" * 64
    elif mutation == "metadata":
        inventories[review.TARGET][0]["activity_comment_sha256"] = "0" * 64
    elif mutation == "missing":
        inventories[review.SOURCE].pop()
    elif mutation == "duplicate":
        inventories[review.SOURCE].append(deepcopy(inventories[review.SOURCE][0]))
    elif mutation == "source_sign":
        observations["ecoinvent"][review.SOURCE] = {
            (label, -1) for label, _ in observations["ecoinvent"][review.SOURCE]
        }
    elif mutation == "native_role":
        native[review.TARGET]["native_reference_is_input"] = True
    else:
        native[review.TARGET]["unit_name"] = "g"
    with pytest.raises(ValueError, match="repeat the scientific review"):
        review.validate_inventories(inventories, observations, native)


@pytest.mark.parametrize("mutation", ("target", "factor"))
def test_projection_is_owned_and_prohibits_unreviewed_purity_or_yield_scaling(mutation):
    rule = {"source": deepcopy(SOURCE), "target": dict(zip(FIELDS, review.PREVIOUS, strict=True))}
    before = deepcopy(rule)
    assert review.approved_copper_rule(rule)["target"] == TARGET
    assert rule == before
    if mutation == "target":
        rule["target"]["name"] = "Copper concentrate"
    else:
        rule["conversion_factor"] = 0.99
    with pytest.raises(ValueError, match="repeat the scientific review"):
        review.approved_copper_rule(rule)


def test_reviewed_copper_supply_is_nonempty_with_a_unique_native_mass_product_and_unchanged_reverse():
    resource = load_uvek_technosphere_resource()
    (rule,) = [r for r in resource["replace"] if r["source"] == SOURCE]
    assert rule["target"] == TARGET and rule.get("conversion_factor", 1) == 1
    assert rule["reference_flow_sign_review"]["status"] == "verified"
    assert not any('"Cathode"' in json.dumps(r) for r in resource["recipes"].values())
    _, row = next((s, r) for s, r in review.EVIDENCE_SCOPE.items() if s == ("uvek", "2025", "cutoff"))
    assert len(row) == 15
    target_inventory = next(r for r in review.APPROVAL["inventories"] if r["source"] == TARGET)
    assert target_inventory["total_exchanges"] == 14
    reverse = review.APPROVAL["independent_reverse"]
    path = review.ROOT / reverse["resource_path"]
    assert hashlib.sha256(path.read_bytes()).hexdigest() == reverse["resource_sha256"]
    actual = json.loads(path.read_text())
    assert next(r for r in actual["replace"] if r["id"] == reverse["rule"]["id"]) == reverse["rule"]
    assert actual["reviews"][reverse["rule"]["review_ref"]] == reverse["review"]
