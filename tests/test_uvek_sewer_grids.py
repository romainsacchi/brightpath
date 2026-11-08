"""Sewer construction keeps physical length and the inspected capacity class."""

from copy import deepcopy

import pytest
from test_uvek_reference_signs import FIELDS, consumer, contexts
from test_uvek_refrigerant_disposal import target_context

from brightpath import BrightwayInventory, SimaProInventory
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_release_resources, load_uvek_technosphere_resource
from scripts.review_uvek_sewer_grids import (
    APPROVAL,
    SOURCES,
    approved_grid_rule,
    validate_inventories,
)

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
    data[0]["comment"] = "Synthetic sewer-infrastructure capacity conversion fixture."
    return data


@pytest.mark.parametrize("version", VERSIONS)
@pytest.mark.parametrize("release", ("2025", "2026", "native2026"))
@pytest.mark.parametrize("amount", (-10, 0, 10))
def test_every_grid_keeps_length_sign_uncertainty_formula_and_capacity_class(version, release, amount):
    data = batch(amount)
    original = deepcopy(data)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", version))
    result = inventory.migrate_background(target_context(release), policy=POLICY)
    assert len(result.data) == 1
    assert result.data[0]["exchanges"][0] == original[0]["exchanges"][0]
    assert result.data[0]["exchanges"][-1]["amount"] == 0.5
    bridge = next(
        (r for r in load_uvek_release_resources("technosphere") if r["source_profiles"][0]["version"] == "2025"), None
    )
    for before, after in zip(original[0]["exchanges"][1:-1], result.data[0]["exchanges"][1:-1], strict=True):
        expected = SOURCES[tuple(before[f] for f in FIELDS)]["target"]
        if release != "2025":
            expected = next(r["target"] for r in bridge["replace"] if r["source"] == expected)
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
    assert len(warnings) == 30
    assert all(
        "capacity class" in i.message and "one kilometre" in i.message and "not wastewater treatment" in i.message
        for i in warnings
    )
    assert inventory.data == data == original


def test_grid_proxy_requires_loss_acceptance_and_preserves_owned_foreground():
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
def test_grid_correction_cannot_enable_consequential_conversion(release):
    data = batch(1)
    before = deepcopy(data)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.12", "consequential"))
    with pytest.raises(MigrationError, match="system models"):
        inventory.migrate_background(target_context(release), policy=MigrationPolicy.permissive())
    assert inventory.data == before


@pytest.mark.parametrize("amount", (-2, 2))
def test_existing_reverse_grid_correspondences_match_classes_independently(amount):
    reverse = APPROVAL["independent_reverse_checks"]
    data = None
    for row in reverse:
        part = consumer(row["source"], amount, 3)
        if data is None:
            data = part
        else:
            data[0]["exchanges"].append(part[0]["exchanges"][1])
    result = BrightwayInventory.from_data(data, context=contexts("uvek", "2025")).migrate_background(
        contexts("ecoinvent", "3.12").background, policy=POLICY
    )
    for after, row in zip(result.data[0]["exchanges"][1:], reverse, strict=True):
        assert {f: after[f] for f in FIELDS} == row["target"]
        assert after["amount"] == amount


def test_grid_scope_is_fifteen_km_identities_with_no_volumetric_factor_or_new_helper():
    assert len(SOURCES) == 15 and APPROVAL["source_profile_count"] == 105 and APPROVAL["inventory_count"] == 110
    assert {r["capacity_class"] for r in SOURCES.values()} == {1, 2, 3, 4, 5}
    assert all(r["source"]["unit"] == r["target"]["unit"] == "kilometer" for r in SOURCES.values())
    assert all(r["target"]["name"] == f'Sewer grid, class {r["capacity_class"]}' for r in SOURCES.values())
    resource = load_uvek_technosphere_resource()
    assert all(
        r["target"] == SOURCES[tuple(r["source"][f] for f in FIELDS)]["target"]
        for r in resource["replace"]
        if tuple(r["source"][f] for f in FIELDS) in SOURCES
    )
    assert not any("sewer-grid" in key for key in resource["recipes"])


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
        "source_hash",
        "comment_hash",
        "product_information",
        "missing_inventory",
        "duplicate_inventory",
        "source_sign",
        "target_sign",
        "native_role",
    ),
)
def test_grid_inventory_or_role_changes_require_a_new_independent_review(mutation):
    records, observations, native = observed_evidence()
    validate_inventories(records, observations, native)
    source = next(iter(SOURCES))
    target = tuple(SOURCES[source]["target"][f] for f in FIELDS)
    if mutation in ("source_hash", "comment_hash", "product_information"):
        field = {
            "source_hash": "inventory_sha256",
            "comment_hash": "activity_comment_sha256",
            "product_information": "product_information_sha256",
        }[mutation]
        records[source][0][field] = "0" * 64
    elif mutation == "missing_inventory":
        records[source].pop()
    elif mutation == "duplicate_inventory":
        records[source].append(deepcopy(records[source][0]))
    elif mutation == "source_sign":
        observations["ecoinvent"][source] = {(label, -1) for label, _ in observations["ecoinvent"][source]}
    elif mutation == "target_sign":
        observations["uvek"][target] = {("uvek-2025-cutoff", -1)}
    else:
        native[target]["native_reference_is_input"] = True
    with pytest.raises(ValueError, match="repeat the scientific review"):
        validate_inventories(records, observations, native)


@pytest.mark.parametrize("mutation", ("target", "factor"))
def test_grid_supplier_projection_rejects_unreviewed_target_or_length_scaling(mutation):
    row = next(iter(SOURCES.values()))
    rule = {"source": deepcopy(row["source"]), "target": deepcopy(row["previous_target"])}
    before = deepcopy(rule)
    result = approved_grid_rule(rule)
    assert result["target"] == row["target"] and rule == before
    if mutation == "target":
        rule["target"]["name"] = "Unreviewed construction supplier"
    else:
        rule["conversion_factor"] = 583
    with pytest.raises(ValueError, match="repeat the scientific review"):
        approved_grid_rule(rule)


@pytest.mark.parametrize("release", ("2025", "native2026"))
def test_capacity_correct_grid_links_survive_excel_and_simapro(tmp_path, release):
    data = batch(-2)
    for e in data[0]["exchanges"][1:-1]:
        e.pop("formula")
        e["type"] = "technosphere"
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.12")).migrate_background(
        target_context(release), policy=POLICY
    )
    excel = BrightwayInventory.from_excel(result.write_excel(tmp_path / "grid.xlsx"))
    categorized = deepcopy(result.data)
    categorized[0]["exchanges"][0]["simapro category"] = "Materials/Processing services"
    csv = (
        BrightwayInventory.from_data(categorized, context=result.context).to_simapro().write_csv(tmp_path / "grid.csv")
    )
    simapro = SimaProInventory.from_csv(
        csv, background_profile=result.background_profile, biosphere_profile=result.context.background.biosphere
    ).to_brightway()
    expected = sorted((tuple(e[f] for f in FIELDS), e["amount"]) for e in result.data[0]["exchanges"][1:-1])
    for restored in (excel, simapro):
        assert len(restored.data) == 1 and not restored.validate().has_errors
        actual = sorted(
            (tuple(e[f] for f in FIELDS), e["amount"])
            for e in restored.data[0]["exchanges"]
            if e["type"] == "technosphere"
        )
        assert actual == expected
