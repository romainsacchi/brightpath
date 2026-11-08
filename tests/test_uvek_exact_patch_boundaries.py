"""Actual patch allocation boundaries keep caller data and restrict omissions to inspected profiles."""

import json
from collections import defaultdict
from copy import deepcopy

import pytest
from test_uvek_reference_signs import consumer, contexts
from test_uvek_refrigerant_disposal import target_context
from uvek_review_helpers import historical_forward_resource

from brightpath import BrightwayInventory, TechnosphereProfile
from brightpath.background.reference_signs import select_profile_sign_rules
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from scripts import review_uvek_exact_patch_boundaries as review

POLICY = MigrationPolicy(
    on_heuristic_mapping=PolicyAction.WARN, on_deletion=PolicyAction.ERROR, on_information_loss=PolicyAction.ERROR
)


def batch(version, amount):
    rows = [r for r in review.CASES.values() if r["label"] == f"ecoinvent-{version}-cutoff"]
    data = None
    for row in rows:
        for kind, uncertainty in (("technosphere", 2), ("substitution", 3)):
            part = consumer(row["source"], amount, uncertainty, kind)
            part[0]["exchanges"][1]["caller_metadata"] = {"retain": [1, 2]}
            if data is None:
                data = part
            else:
                data[0]["exchanges"].extend(part[0]["exchanges"][1:])
    data[0]["exchanges"].append(
        {
            "name": "Carbon dioxide, fossil",
            "categories": ["air"],
            "unit": "kilogram",
            "type": "biosphere",
            "amount": 0.5,
        }
    )
    return data


@pytest.mark.parametrize("version", ("3.9.1", "3.10.1"))
@pytest.mark.parametrize("release", ("2025", "2026", "native2026"))
@pytest.mark.parametrize("amount", (-2, 0, 2))
def test_exact_patch_profiles_preserve_stock_uncertainty_and_audit_every_omitted_handoff(version, release, amount):
    data = batch(version, amount)
    before = deepcopy(data)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", version))
    result = inventory.migrate_background(target_context(release), policy=POLICY)
    actual = result.data[0]["exchanges"]
    stocks = [e for e in before[0]["exchanges"][1:-1] if review.identity(e) in review.PATCH_STOCKS]
    assert len(result.data) == 3 and len(actual) == len(stocks) + 2
    assert actual[0] == before[0]["exchanges"][0]
    assert actual[-1]["name"] == "Carbon dioxide, fossil" and actual[-1]["amount"] == 0.5
    assert actual[-1]["unit"] == "kilogram"
    if release != "native2026":
        assert actual[-1] == before[0]["exchanges"][-1]
    helpers = {row["code"]: row for row in result.data[1:]}
    for old, new in zip(stocks, actual[1:-1], strict=True):
        for field in (
            "amount",
            "type",
            "formula",
            "uncertainty type",
            "loc",
            "scale",
            "minimum",
            "maximum",
            "negative",
            "caller_metadata",
        ):
            assert new.get(field) == old.get(field)
        helper = helpers[new["input"][1]]
        assert helper["brightpath_conversion"]["zero_burden"] is True
        assert helper["exchanges"] == [{**{f: helper[f] for f in review.FIELDS}, "type": "production", "amount": 1}]
    expected = {
        f"datasets[0].exchanges[{i}]": e
        for i, e in enumerate(before[0]["exchanges"])
        if e["type"] in {"technosphere", "substitution"} and review.identity(e) in review.HANDOFFS
    }
    warnings = [i for i in result.last_migration_report.issues if i.code == "migration.technosphere_omission"]
    losses = [i for i in result.last_migration_report.losses if i.code == "migration.technosphere_omission"]
    assert len(warnings) == len(losses) == len(expected)
    assert all(i.severity.value == "warning" for i in warnings)
    # Reports freeze nested lists internally; the public JSON form must retain
    # every original caller field and container value exactly.
    assert all(loss.to_dict()["details"]["source_exchange"] == expected[loss.path] for loss in losses)
    assert inventory.data == data == before
    assert result.migrate_background(target_context(release)).data == result.data


@pytest.mark.parametrize("version", ("3.9.1", "3.10.1"))
def test_exact_patch_foreground_ownership_and_unknown_version_refusal(version):
    data = batch(version, -2)
    for row in review.CASES.values():
        if row["label"] == f"ecoinvent-{version}-cutoff":
            data.append(
                {
                    **row["source"],
                    "exchanges": [{**row["source"], "type": "production", "amount": row["source_reference"]}],
                }
            )
    before = deepcopy(data)
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", version)).migrate_background(
        target_context("2025"), policy=POLICY
    )
    assert result.data == data == before
    resource = json.loads((review.ROOT / "brightpath/data/migrations/uvek/ecoinvent-to-uvek-2025.json").read_text())
    for unknown in ("3.9.2", "3.10.2"):
        selected = select_profile_sign_rules(resource, TechnosphereProfile("ecoinvent", unknown, "cutoff"))
        for source in review.SOURCES:
            rule = next(r for r in selected["replace"] if review.identity(r["source"]) == source)
            assert rule["reference_flow_sign_review"]["status"] == "unresolved"
    with pytest.raises(MigrationError, match="system models"):
        BrightwayInventory.from_data(
            batch(version, -2), context=contexts("ecoinvent", version, "consequential")
        ).migrate_background(target_context("2025"), policy=MigrationPolicy.permissive())


@pytest.mark.parametrize("change", ("none", "inventory", "description", "reference", "missing", "duplicate"))
def test_full_actual_patch_core_and_endpoint_evidence_cannot_drift(change):
    inventories = defaultdict(list)
    for row in review.APPROVAL["inventories"]:
        source = review.identity(row["source"])
        inventories[source].append({**deepcopy(row), "source": list(source)})
    source = next(iter(inventories))
    if change == "inventory":
        inventories[source][0]["inventory_sha256"] = "0" * 64
    elif change == "description":
        inventories[source][0]["activity_comment_sha256"] = "0" * 64
    elif change == "reference":
        inventories[source][0]["reference_amount"] *= -1
    elif change == "missing":
        inventories[source].pop()
    elif change == "duplicate":
        inventories[source].append(deepcopy(inventories[source][0]))
    if change == "none":
        review.validate_inventories(inventories)
    else:
        with pytest.raises(ValueError, match="repeat the scientific review"):
            review.validate_inventories(inventories)


def test_original_stock_helper_body_and_core_action_bindings_are_frozen():
    resource = historical_forward_resource()
    review.validate_bindings(resource)
    row = next(r for r in review.CASES.values() if r["kind"] == "stock")
    resource["recipes"][row["existing_stock_recipe"]]["zero_burden"] = False
    with pytest.raises(ValueError, match="repeat the scientific review"):
        review.validate_bindings(resource)
