"""Complete source flaring, exact handoffs, physical flows and native coverage boundaries."""

import json
from collections import defaultdict
from copy import deepcopy

import pytest
from test_uvek_natural_gas_flares import assert_scaled
from test_uvek_reference_signs import FIELDS, consumer, contexts
from test_uvek_refrigerant_disposal import target_context

from brightpath import BrightwayInventory, TechnosphereProfile
from brightpath.background.reference_signs import select_profile_sign_rules
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource
from scripts import review_uvek_remaining_gas_flares as review

POLICY = MigrationPolicy(on_heuristic_mapping=PolicyAction.WARN, on_information_loss=PolicyAction.WARN)
PROFILES = [(row, profile) for row in review.APPROVAL["rules"] for profile in row["profiles"]]


@pytest.mark.parametrize("row,profile", PROFILES, ids=[p["label"] + "/" + r["source"]["name"] for r, p in PROFILES])
def test_all_exact_profiles_preserve_source_physics_and_caller_ownership(row, profile):
    version = profile["label"].split("-")[1]
    data = consumer(row["source"], -2, 2)
    data[0]["comment"] = "Synthetic gas-flare consumer."
    data[0]["exchanges"][1]["caller_metadata"] = {"retain": [1, 2]}
    data[0]["exchanges"].append(consumer(row["source"], -2, 3, "substitution")[0]["exchanges"][1])
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
    migrated = inventory.migrate_background(target_context("2025"), policy=POLICY)
    for previous, actual in zip(before[0]["exchanges"][1:3], migrated.data[0]["exchanges"][1:3], strict=True):
        assert_scaled(previous, actual, profile["conversion_factor"])
    assert migrated.data[0]["exchanges"][0] == before[0]["exchanges"][0]
    assert migrated.data[0]["exchanges"][-1]["amount"] == 0.5
    assert inventory.data == data == before
    if "retained_native_profile" in profile:
        assert len(migrated.data) == 1
        assert {f: migrated.data[0]["exchanges"][1][f] for f in FIELDS} == profile["target"]
        return
    helpers = migrated.data[1:]
    assert helpers and all(any(e["type"] != "production" for e in h["exchanges"]) for h in helpers)
    physical = [e for h in helpers for e in h["exchanges"] if e["type"] == "biosphere"]
    assert physical
    names = {e["name"] for e in physical}
    if "sour" in row["source"]["name"]:
        assert {"Sulfur dioxide", "Methane, fossil", "Nitrogen oxides"} <= names
    elif version in {"3.6", "3.7", "3.8"}:
        assert {"Methane, fossil", "Nitrogen oxides"} <= names
    else:
        assert "Methane, fossil" not in names and "Nitrogen oxides" not in names
    expected = profile["recipe"]
    if row["source"]["name"].startswith("market"):
        (expected,) = profile["dependency_recipes"].values()
    assert len(physical) == len(expected["inputs"])
    for e, original in zip(physical, expected["inputs"], strict=True):
        for field in ("name", "categories", "unit", "amount", "uncertainty type", "loc", "scale"):
            assert e[field] == original[field]
    assert not any("gas turbine" in h["name"] for h in helpers)
    assert not any("approved_omission" in e for e in physical)
    assert migrated.migrate_background(target_context("2025")).data == migrated.data


@pytest.mark.parametrize("version", ("3.6", "3.10.1", "3.12"))
@pytest.mark.parametrize("kind", ("sweet", "sour"))
def test_native_physical_gaps_refuse_or_retain_every_physical_link(version, kind):
    source = next(
        row["source"]
        for row in review.APPROVAL["rules"]
        if row["source"]["name"].startswith("treatment") and kind in row["source"]["name"]
    )
    data = consumer(source, -2, 3)
    before = deepcopy(data)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", version))
    retained = kind == "sweet" and version == "3.12"
    if retained:
        result = inventory.migrate_background(target_context("native2026"), policy=POLICY)
        assert len(result.data) == 1
        return
    with pytest.raises(MigrationError):
        inventory.migrate_background(target_context("native2026"), policy=POLICY)
    assert inventory.data == before
    result = inventory.migrate_background(target_context("native2026"), policy=MigrationPolicy.permissive())
    physical = [e for h in result.data[1:] for e in h["exchanges"] if e["type"] == "biosphere"]
    row = review.SOURCES[tuple(source[f] for f in FIELDS)]
    p = next(p for p in row["profiles"] if p["label"] == f"ecoinvent-{version}-cutoff")
    assert len(physical) == len(p["recipe"]["inputs"])
    assert any(e["name"] == "Mercury II" and e["amount"] > 0 for e in physical)
    assert inventory.data == before
    assert not any(i.code == "migration.biosphere_omission" for i in result.last_migration_report.issues)


@pytest.mark.parametrize("amount", (-2, 0, 2))
def test_credits_zero_and_positive_quantities_are_not_absolute_valued(amount):
    source = next(row["source"] for row in review.APPROVAL["rules"] if "sour" in row["source"]["name"])
    data = consumer(source, amount, 3)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.12"))
    result = inventory.migrate_background(target_context("2025"), policy=POLICY)
    assert result.data[0]["exchanges"][1]["amount"] == -amount
    assert result.data[0]["exchanges"][1]["minimum"] == -amount - 1
    assert result.data[0]["exchanges"][1]["maximum"] == -amount + 1
    assert inventory.data == data


def test_foreground_and_system_model_boundaries_remain_explicit():
    source = next(iter(review.APPROVAL["rules"]))["source"]
    data = consumer(source, -2, 3)
    data.append({**source, "exchanges": [{**source, "type": "production", "amount": -1}]})
    before = deepcopy(data)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.12"))
    result = inventory.migrate_background(target_context("2025"), policy=POLICY)
    assert result.data == before == inventory.data
    inventory = BrightwayInventory.from_data(data[:1], context=contexts("ecoinvent", "3.12", "consequential"))
    with pytest.raises(MigrationError, match="system models"):
        inventory.migrate_background(target_context("2025"), policy=MigrationPolicy.permissive())
    resource = load_uvek_technosphere_resource()
    selected = select_profile_sign_rules(resource, TechnosphereProfile("ecoinvent", "3.12.1", "cutoff"))
    assert all(
        r["reference_flow_sign_review"]["status"] == "unresolved"
        for r in selected["replace"]
        if tuple(r["source"][f] for f in FIELDS) in review.SOURCES
    )


@pytest.mark.parametrize("change", ("inventory", "formula", "sign", "missing", "native", "canonical", "reverse"))
def test_inventory_or_role_drift_cannot_inherit_source_review(change, monkeypatch, tmp_path):
    records = deepcopy(review.APPROVAL["inventories"])
    obs = defaultdict(lambda: defaultdict(set))
    review.extend_observations(records, obs)
    witness = next(r for r in records if r["label"] == "uvek-2025-cutoff")
    native = {
        review.identity(witness["source"]): {"native_flow_type": "PRODUCT_FLOW", "native_reference_is_input": False}
    }
    review.validate_inventories(records, obs, native)
    if change == "inventory":
        records[0]["inventory_sha256"] = "0" * 64
    if change == "formula":
        records[0]["formula_sha256"] = "0" * 64
    if change == "sign":
        obs["ecoinvent"][next(iter(review.SOURCES))] = {("ecoinvent-3.12-cutoff", 1)}
    if change == "missing":
        records.pop()
    if change == "native":
        native[review.identity(witness["source"])]["native_reference_is_input"] = True
    if change in {"canonical", "reverse"}:
        for path in [
            *review.APPROVAL["canonical_identity_dependency_sha256"],
            review.APPROVAL["independent_reverse"]["resource_path"],
        ]:
            target = tmp_path / path
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes((review.ROOT / path).read_bytes())
        path = (
            next(iter(review.APPROVAL["canonical_identity_dependency_sha256"]))
            if change == "canonical"
            else review.APPROVAL["independent_reverse"]["resource_path"]
        )
        (tmp_path / path).write_bytes(b"changed")
        monkeypatch.setattr(review, "ROOT", tmp_path)
    with pytest.raises(ValueError):
        review.validate_inventories(records, obs, native)


@pytest.mark.parametrize("change", ("native_target", "profile_factor", "physical_coefficient"))
def test_hybrid_native_defaults_and_source_recipes_are_bound_against_stale_replay(change):
    from scripts.review_uvek_reference_signs import REVIEW_PATH, reviewed_replacements

    resource = deepcopy(load_uvek_technosphere_resource())
    decisions = json.loads((review.ROOT / REVIEW_PATH).read_text())["resources"][resource["name"]]
    rule = next(r for r in resource["replace"] if r["source"]["name"] == "market for waste natural gas, sweet")
    if change == "native_target":
        rule["target"]["name"] = "Unreviewed turbine"
    if change == "profile_factor":
        rule["reference_flow_sign_review"]["profiles"][0]["conversion_factor"] = 99
    if change == "physical_coefficient":
        decision = decisions[rule["id"]]
        rid = next(k for k, v in decision["recipes"].items() if any(e.get("type") == "biosphere" for e in v["inputs"]))
        resource["recipes"][rid]["inputs"][0]["amount"] *= 2
    with pytest.raises(ValueError, match="Stale approved"):
        reviewed_replacements(resource, decisions)


@pytest.mark.parametrize("version,kind", [("3.8", "sweet"), ("3.12", "sour")])
def test_source_helpers_survive_compatibility_release_and_excel_export(version, kind, tmp_path):
    source = next(
        r["source"]
        for r in review.APPROVAL["rules"]
        if r["source"]["name"].startswith("market") and kind in r["source"]["name"]
    )
    data = consumer(source, -2, 2)
    data[0]["comment"] = "Synthetic source gas-flare export regression."
    data[0]["exchanges"][1].pop("formula")
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", version))
    migrated = inventory.migrate_background(target_context("2026"), policy=POLICY)
    restored = BrightwayInventory.from_excel(migrated.write_excel(tmp_path / "gas.xlsx"))
    assert len(restored.data) == len(migrated.data) > 1
    physical = [e for h in restored.data[1:] for e in h["exchanges"] if e["type"] == "biosphere"]
    expected = [e for h in migrated.data[1:] for e in h["exchanges"] if e["type"] == "biosphere"]
    assert len(physical) == len(expected)
    assert sorted((e["name"], e["unit"], e["amount"]) for e in physical) == sorted(
        (e["name"], e["unit"], e["amount"]) for e in expected
    )
