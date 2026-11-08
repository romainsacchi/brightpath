"""Concrete producer grade, caller ownership and explicit screening boundaries."""

from collections import defaultdict
from copy import deepcopy

import pytest
from test_uvek_reference_signs import FIELDS, consumer, contexts
from test_uvek_refrigerant_disposal import target_context

from brightpath import BrightwayInventory
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from scripts import review_uvek_concrete_grade as review

SOURCE = dict(zip(FIELDS, review.SOURCE, strict=True))
TARGET = dict(zip(FIELDS, review.TARGET, strict=True))
POLICY = MigrationPolicy(on_heuristic_mapping=PolicyAction.WARN, on_information_loss=PolicyAction.WARN)
VERSIONS = [label.split("-")[1] for label in review.APPROVAL["profiles"]]


@pytest.mark.parametrize("version", VERSIONS)
@pytest.mark.parametrize("release", ("2025", "2026", "native2026"))
@pytest.mark.parametrize("amount", (-2, 0, 2))
def test_concrete_grade_and_volume_preserve_caller_state(version, release, amount):
    data = consumer(SOURCE, amount, 2)
    data[0]["comment"] = "Synthetic concrete consumer."
    data[0]["exchanges"].append(consumer(SOURCE, amount, 3, "substitution")[0]["exchanges"][1])
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
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", version))
    migrated = inventory.migrate_background(target_context(release), policy=POLICY)
    assert len(migrated.data) == 1
    for original, result in zip(before[0]["exchanges"][1:-1], migrated.data[0]["exchanges"][1:-1], strict=True):
        assert {f: result[f] for f in FIELDS} == TARGET
        assert all(result[k] == v for k, v in original.items() if k not in FIELDS)
    assert migrated.data[0]["exchanges"][0] == before[0]["exchanges"][0]
    assert migrated.data[0]["exchanges"][-1]["amount"] == 0.5
    assert inventory.data == data == before
    assert any(i.code == "migration.technosphere_information_loss" for i in migrated.last_migration_report.issues)
    assert migrated.migrate_background(target_context(release)).data == migrated.data


def test_strict_proxy_and_cross_model_refusals_are_transactional():
    data = consumer(SOURCE, -2, 3)
    before = deepcopy(data)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.12"))
    with pytest.raises(MigrationError):
        inventory.migrate_background(target_context("2025"), policy=MigrationPolicy.strict())
    assert inventory.data == before
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.12", "consequential"))
    with pytest.raises(MigrationError, match="system models"):
        inventory.migrate_background(target_context("2025"), policy=MigrationPolicy.permissive())
    assert inventory.data == before


def test_foreground_provider_is_preserved():
    data = consumer(SOURCE, -2, 3)
    data.append({**SOURCE, "exchanges": [{**SOURCE, "type": "production", "amount": 1}]})
    before = deepcopy(data)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.12"))
    migrated = inventory.migrate_background(target_context("2025"), policy=POLICY)
    assert migrated.data == before
    assert inventory.data == before


def evidence_fixture():
    observations = defaultdict(lambda: defaultdict(set))
    observations["ecoinvent"][review.SOURCE] = {(p, 1) for p in review.APPROVAL["profiles"]}
    observations["uvek"][review.TARGET] = {("uvek-2025-cutoff", 1)}
    native = {review.TARGET: {"native_flow_type": "PRODUCT_FLOW", "native_reference_is_input": False}}
    return deepcopy(review.APPROVAL["inventories"]), observations, native


@pytest.mark.parametrize(
    "mutation", ("source_hash", "missing_profile", "duplicate", "negative_reference", "native_waste")
)
def test_changed_original_boundary_cannot_inherit_review(mutation):
    records, obs, native = evidence_fixture()
    review.validate_inventories(records, obs, native)
    if mutation == "source_hash":
        records[0]["inventory_sha256"] = "0" * 64
    if mutation == "missing_profile":
        records.pop()
    if mutation == "duplicate":
        records.append(deepcopy(records[0]))
    if mutation == "negative_reference":
        obs["ecoinvent"][review.SOURCE] = {(p, -1) for p in review.APPROVAL["profiles"]}
    if mutation == "native_waste":
        native[review.TARGET]["native_reference_is_input"] = True
    with pytest.raises(ValueError):
        review.validate_inventories(records, obs, native)


def test_grade_is_scoped_and_reverse_resource_is_unchanged():
    from brightpath import TechnosphereProfile
    from brightpath.background.reference_signs import select_profile_sign_rules
    from brightpath.migrations.resources import load_uvek_technosphere_resource

    resource = load_uvek_technosphere_resource()
    for version, status in [("3.12", "verified"), ("3.8", "unresolved")]:
        selected = select_profile_sign_rules(resource, TechnosphereProfile("ecoinvent", version, "cutoff"))
        rule = next(r for r in selected["replace"] if r["source"] == SOURCE)
        assert rule["reference_flow_sign_review"]["status"] == status
        assert rule["target"] == TARGET
    import hashlib

    path = review.ROOT / review.APPROVAL["independent_reverse"]["resource_path"]
    assert hashlib.sha256(path.read_bytes()).hexdigest() == review.APPROVAL["independent_reverse"]["resource_sha256"]
