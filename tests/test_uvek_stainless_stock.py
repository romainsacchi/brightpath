"""Hot-rolled stock substitutions retain material demand and caller quantities."""

from copy import deepcopy

import pytest
from test_uvek_reference_signs import FIELDS, consumer, contexts
from test_uvek_refrigerant_disposal import target_context

from brightpath import BrightwayInventory
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_release_resources, load_uvek_technosphere_resource
from scripts.review_uvek_stainless_stock import APPROVAL, SOURCES, TARGET, approved_stock_rule, validate_inventories

POLICY = MigrationPolicy(on_heuristic_mapping=PolicyAction.WARN, on_information_loss=PolicyAction.WARN)
VERSIONS = ("3.6", "3.7", "3.8", "3.9", "3.9.1", "3.10", "3.10.1", "3.11", "3.12")


def batch(amount):
    data = None
    for source in SOURCES:
        for kind, uncertainty in (("technosphere", 2), ("substitution", 3)):
            part = consumer(dict(zip(FIELDS, source, strict=True)), amount, uncertainty, kind)
            part[0]["exchanges"][1]["caller_metadata"] = {"keep": [1, 2]}
            if data is None:
                data = part
            else:
                data[0]["exchanges"].append(part[0]["exchanges"][1])
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


@pytest.mark.parametrize("version", VERSIONS)
@pytest.mark.parametrize("release", ("2025", "2026", "native2026"))
@pytest.mark.parametrize("amount", (-10, 0, 10))
def test_stock_keeps_kg_sign_uncertainty_formula_metadata_and_separate_emissions(version, release, amount):
    data = batch(amount)
    original = deepcopy(data)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", version))
    result = inventory.migrate_background(target_context(release), policy=POLICY)
    assert len(result.data) == 1
    assert result.data[0]["exchanges"][0] == original[0]["exchanges"][0]
    assert result.data[0]["exchanges"][-1]["amount"] == 0.5
    expected = TARGET
    if release != "2025":
        bridge = next(
            r for r in load_uvek_release_resources("technosphere") if r["source_profiles"][0]["version"] == "2025"
        )
        expected = next(r["target"] for r in bridge["replace"] if r["source"] == TARGET)
    for before, after in zip(original[0]["exchanges"][1:-1], result.data[0]["exchanges"][1:-1], strict=True):
        assert {f: after[f] for f in FIELDS} == expected
        assert all(
            after.get(f) == before.get(f)
            for f in (
                "amount",
                "type",
                "formula",
                "uncertainty type",
                "loc",
                "scale",
                "negative",
                "minimum",
                "maximum",
                "caller_metadata",
            )
        )
    warnings = [i for i in result.last_migration_report.issues if i.code == "migration.technosphere_information_loss"]
    assert len(warnings) == 4
    assert all(
        "full hot-rolled material" in i.message and "no separate rolling input" in i.message and "GLO/RoW" in i.message
        for i in warnings
    )
    assert inventory.data == data == original


def test_stock_proxy_requires_loss_acceptance_and_preserves_foreground_ownership():
    data = batch(-1)
    before = deepcopy(data)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.12"))
    with pytest.raises(MigrationError):
        inventory.migrate_background(
            target_context("2025"), policy=MigrationPolicy(on_heuristic_mapping=PolicyAction.WARN)
        )
    assert inventory.data == before
    for source in SOURCES:
        record = dict(zip(FIELDS, source, strict=True))
        data.append({**record, "exchanges": [{**record, "type": "production", "amount": 1}]})
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.12")).migrate_background(
        target_context("2025"), policy=POLICY
    )
    assert result.data == data


@pytest.mark.parametrize("release", ("2025", "2026", "native2026"))
def test_stock_review_cannot_enable_consequential_conversion(release):
    data = batch(1)
    original = deepcopy(data)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.12", "consequential"))
    with pytest.raises(MigrationError, match="system models"):
        inventory.migrate_background(target_context(release), policy=MigrationPolicy.permissive())
    assert inventory.data == original


@pytest.mark.parametrize("amount", (-2, 2))
def test_existing_reverse_material_rule_matches_the_independently_inspected_rer_producer(amount):
    rule = APPROVAL["independent_reverse_check"]
    data = consumer(rule["source"], amount, 3)
    result = BrightwayInventory.from_data(data, context=contexts("uvek", "2025")).migrate_background(
        contexts("ecoinvent", "3.12").background, policy=POLICY
    )
    assert len(result.data) == 1
    exchange = result.data[0]["exchanges"][1]
    assert {f: exchange[f] for f in FIELDS} == rule["target"]
    assert exchange["amount"] == amount


def observed_evidence():
    records, observations, native = {}, {"ecoinvent": {}, "uvek": {}}, {}
    for row in APPROVAL["inventories"]:
        source = tuple(row["source"][f] for f in FIELDS)
        records.setdefault(source, []).append({**row})
        family = row["label"].split("-")[0]
        observations[family].setdefault(source, set()).add((row["label"], 1))
        if family == "uvek":
            native[source] = {"native_flow_type": "PRODUCT_FLOW", "native_reference_is_input": False}
    return records, observations, native


@pytest.mark.parametrize(
    "mutation",
    (
        "inventory",
        "comment",
        "product_information",
        "missing",
        "duplicate",
        "source_sign",
        "target_sign",
        "native_role",
        "extra_profile",
    ),
)
def test_stock_inventory_boundary_or_native_role_drift_requires_independent_review(mutation):
    records, observations, native = observed_evidence()
    validate_inventories(records, observations, native)
    source = next(iter(SOURCES))
    target = tuple(TARGET[f] for f in FIELDS)
    if mutation in ("inventory", "comment", "product_information"):
        key = {
            "inventory": "inventory_sha256",
            "comment": "activity_comment_sha256",
            "product_information": "product_information_sha256",
        }[mutation]
        records[source][0][key] = "0" * 64
    elif mutation == "missing":
        records[source].pop()
    elif mutation == "duplicate":
        records[source].append(deepcopy(records[source][0]))
    elif mutation == "source_sign":
        observations["ecoinvent"][source] = {(label, -1) for label, _sign in observations["ecoinvent"][source]}
    elif mutation == "target_sign":
        observations["uvek"][target] = {("uvek-2025-cutoff", -1)}
    elif mutation == "extra_profile":
        observations["ecoinvent"][source].add(("ecoinvent-3.13-cutoff", 1))
    else:
        native[target]["native_reference_is_input"] = True
    with pytest.raises(ValueError, match="repeat the scientific review"):
        validate_inventories(records, observations, native)


@pytest.mark.parametrize("mutation", ("target", "factor"))
def test_stock_projection_preserves_owned_rules_and_refuses_unreviewed_changes(mutation):
    row = next(iter(SOURCES.values()))
    rule = {"source": deepcopy(row["source"]), "target": deepcopy(row["previous_target"])}
    saved = deepcopy(rule)
    result = approved_stock_rule(rule)
    assert result["target"] == TARGET and rule == saved
    if mutation == "target":
        rule["target"]["name"] = "Different material"
    else:
        rule["conversion_factor"] = -1
    with pytest.raises(ValueError, match="repeat the scientific review"):
        approved_stock_rule(rule)


def test_review_changes_no_unit_basis_adds_no_rolling_input_or_helper():
    assert len(SOURCES) == 2 and APPROVAL["source_profile_count"] == 14 and APPROVAL["inventory_count"] == 23
    resource = load_uvek_technosphere_resource()
    for rule in resource["replace"]:
        source = tuple(rule["source"][f] for f in FIELDS)
        if source in SOURCES:
            assert rule["target"] == TARGET and rule.get("conversion_factor", 1) == 1
    assert not any("stainless-stock" in key for key in resource["recipes"])
