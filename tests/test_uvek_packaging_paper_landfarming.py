"""Wet packaging-paper sludge retains its emissions and a separately bounded tanker service."""

from copy import deepcopy

import pytest
from test_uvek_reference_signs import FIELDS, consumer, contexts
from test_uvek_refrigerant_disposal import target_context

from brightpath import BackgroundContext, BiosphereProfile, BrightwayInventory, TechnosphereProfile
from brightpath.background.reference_signs import select_profile_sign_rules
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource
from scripts.review_uvek_packaging_paper_landfarming import (
    APPROVAL,
    SOURCES,
    VERSIONS,
    validate_bindings,
    validate_inventories,
)

SOURCE = next(iter(SOURCES.values()))["source"]
POLICY = MigrationPolicy(on_information_loss=PolicyAction.WARN, on_heuristic_mapping=PolicyAction.WARN)


def batch(amount):
    data = consumer(SOURCE, amount, 2)
    data[0]["exchanges"].append(consumer(SOURCE, amount, 3, "substitution")[0]["exchanges"][1])
    for e in data[0]["exchanges"][1:]:
        e["caller_metadata"] = {"retain": [1, 2]}
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


@pytest.mark.parametrize("version", sorted(VERSIONS))
@pytest.mark.parametrize("release", ("2025", "2026"))
@pytest.mark.parametrize("amount", (-2, 0, 2))
def test_wet_sludge_reference_direction_preserves_all_releases_and_original_spreading_volume(version, release, amount):
    data = batch(amount)
    original = deepcopy(data)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", version))
    result = inventory.migrate_background(target_context(release), policy=POLICY)
    assert len(result.data) == 2
    assert result.data[0]["exchanges"][0] == original[0]["exchanges"][0]
    assert result.data[0]["exchanges"][-1] == original[0]["exchanges"][-1]
    (helper,) = result.data[1:]
    (production,) = [e for e in helper["exchanges"] if e["type"] == "production"]
    assert production["amount"] == 1 and helper["unit"] == "kilogram"
    for before, after in zip(original[0]["exchanges"][1:-1], result.data[0]["exchanges"][1:-1], strict=True):
        assert after["amount"] == -before["amount"] and after["type"] == before["type"]
        assert after["formula"] == "(dose) * (-1.0)" and after["scale"] == before["scale"]
        assert after["caller_metadata"] == before["caller_metadata"]
        assert after["amount"] / production["amount"] == before["amount"] / -1
        if before["uncertainty type"] == 2:
            assert after["loc"] == before["loc"] and after["negative"] is not before["negative"]
        else:
            assert after["loc"] == -before["loc"]
            assert (after["minimum"], after["maximum"]) == (-before["maximum"], -before["minimum"])
    (service,) = [e for e in helper["exchanges"] if e["type"] == "technosphere"]
    assert service["name"] == "Slurry spreading, by vacuum tanker" and service["unit"] == "cubic meter"
    assert service["amount"] == 0.00097087
    flows = [e for e in helper["exchanges"] if e["type"] == "biosphere"]
    assert len(flows) == 39 and all(e["amount"] > 0 for e in flows)
    by_name = {e["name"]: e for e in flows if e["name"] != "Water"}
    assert by_name["Carbon dioxide, fossil"]["amount"] == 0.001196426
    assert by_name["Carbon dioxide, non-fossil"]["amount"] == 0.03066096
    assert by_name["Nitrate"]["amount"] == 0.0017818
    assert by_name["Chromium III"]["amount"] == 1.9428e-7
    assert by_name["Chromium III"]["categories"] == ["soil", "agricultural"]
    assert not any(e["name"] == "Chromium VI" for e in flows)
    water = {(tuple(e["categories"]), e["unit"]): e["amount"] for e in flows if e["name"] == "Water"}
    assert water == {
        (("air", "non-urban air or from high stacks"), "cubic meter"): 0.00035718,
        (("water", "ground-"), "cubic meter"): 0.00061282,
    }
    assert not any(e["unit"] == "ton kilometer" or "fertilizer" in e["name"] for e in helper["exchanges"])
    assert result.last_migration_report.losses
    assert inventory.data == data == original


@pytest.mark.parametrize("version", sorted(VERSIONS))
@pytest.mark.parametrize("release", ("2025", "2026"))
def test_existing_independent_reverse_spreading_supplier_keeps_complete_canonical_helper_graph(version, release):
    canonical = BrightwayInventory.from_data(batch(-1), context=contexts("ecoinvent", version)).migrate_background(
        target_context(release), policy=POLICY
    )
    original = deepcopy(canonical.data)
    result = canonical.migrate_background(
        BackgroundContext(TechnosphereProfile("ecoinvent", "3.12", "cutoff"), BiosphereProfile("ecoinvent", "3.10")),
        policy=MigrationPolicy(
            on_information_loss=PolicyAction.WARN,
            on_heuristic_mapping=PolicyAction.WARN,
            on_inferred_reverse=PolicyAction.WARN,
        ),
    )
    assert len(result.data) == 2 and result.data[0] == original[0]
    assert [e for e in result.data[1]["exchanges"] if e["type"] == "biosphere"] == [
        e for e in original[1]["exchanges"] if e["type"] == "biosphere"
    ]
    (service,) = [e for e in result.data[1]["exchanges"] if e["type"] == "technosphere"]
    assert service["name"] == "liquid manure spreading, by vacuum tanker" and service["location"] == "CH"
    assert service["amount"] == 0.00097087 and service["unit"] == "cubic meter"
    assert canonical.data == original


@pytest.mark.parametrize("version", sorted(VERSIONS))
def test_unreviewed_native_soil_and_groundwater_flows_are_not_relabelled_or_omitted(version):
    data = batch(-1)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", version))
    with pytest.raises(MigrationError) as caught:
        inventory.migrate_background(target_context("native2026"), policy=POLICY)
    assert any("biosphere" in i.code and "unresolved" in i.code for i in caught.value.report.issues)
    assert inventory.data == data


def test_strict_loss_refusal_foreground_ownership_and_uninspected_profiles_remain_explicit():
    data = batch(-1)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.12"))
    with pytest.raises(MigrationError):
        inventory.migrate_background(
            target_context("2025"), policy=MigrationPolicy(on_heuristic_mapping=PolicyAction.WARN)
        )
    assert inventory.data == data
    data.append({**SOURCE, "exchanges": [{**SOURCE, "type": "production", "amount": -1}]})
    assert (
        BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.12"))
        .migrate_background(target_context("2025"), policy=POLICY)
        .data
        == data
    )
    for version in ("3.9", "3.10.2", "3.13"):
        selected = select_profile_sign_rules(
            load_uvek_technosphere_resource(), TechnosphereProfile("ecoinvent", version, "cutoff")
        )
        rule = next(r for r in selected["replace"] if r["source"] == SOURCE)
        assert rule["reference_flow_sign_review"]["status"] == "unresolved"
    inventory = BrightwayInventory.from_data(batch(-1), context=contexts("ecoinvent", "3.12", "consequential"))
    with pytest.raises(MigrationError, match="system models"):
        inventory.migrate_background(target_context("2025"), policy=MigrationPolicy.permissive())
    assert inventory.data == batch(-1)


@pytest.mark.parametrize("mutation", ("inventory", "description", "missing", "duplicate", "patch", "native_role"))
def test_source_reference_description_inventory_or_provider_role_drift_requires_review(mutation):
    records, observations = {}, {"ecoinvent": {}, "uvek": {}}
    for row in APPROVAL["inventories"]:
        source = tuple(row["source"][f] for f in FIELDS)
        records.setdefault(source, []).append({**deepcopy(row), "source": list(source)})
        observations[row["label"].split("-")[0]].setdefault(source, set()).add((row["label"], row["reference_amount"]))
    native = {tuple(row["source"][f] for f in FIELDS): deepcopy(row) for row in APPROVAL["native_roles"]}
    validate_inventories(records, observations, native)
    source = next(iter(SOURCES))
    if mutation == "inventory":
        records[source][0]["inventory_sha256"] = "0" * 64
    elif mutation == "description":
        records[source][0]["product_information_sha256"] = "0" * 64
    elif mutation == "missing":
        records[source].pop()
    elif mutation == "duplicate":
        records[source].append(deepcopy(records[source][0]))
    elif mutation == "patch":
        observations["ecoinvent"][source].add(("ecoinvent-3.10.2-cutoff", -1))
    else:
        native[next(iter(native))]["native_reference_is_input"] = True
    with pytest.raises(ValueError, match="repeat the scientific review"):
        validate_inventories(records, observations, native)


def test_source_emissions_spreading_volume_and_wet_basis_cannot_drift(monkeypatch):
    validate_bindings(load_uvek_technosphere_resource())
    profile = APPROVAL["rules"][0]["profiles"][0]
    changed = deepcopy(profile["inputs"])
    changed[0]["amount"] *= 0.97
    monkeypatch.setitem(profile, "inputs", changed)
    with pytest.raises(ValueError, match="quantities, emissions or profile scope changed"):
        validate_bindings(load_uvek_technosphere_resource())
