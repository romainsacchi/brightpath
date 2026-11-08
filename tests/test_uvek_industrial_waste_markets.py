"""Industrial markets retain full treatment children, wet mass and complete road service."""

from copy import deepcopy

import pytest
from test_uvek_reference_signs import FIELDS, consumer, contexts
from test_uvek_refrigerant_disposal import target_context

from brightpath import BackgroundContext, BiosphereProfile, BrightwayInventory, TechnosphereProfile
from brightpath.background.reference_signs import select_profile_sign_rules
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource
from scripts.review_uvek_industrial_waste_markets import (
    APPROVAL,
    ROAD,
    SOURCES,
    validate_bindings,
    validate_inventories,
)

POLICY = MigrationPolicy(on_heuristic_mapping=PolicyAction.WARN, on_information_loss=PolicyAction.WARN)
VERSIONS = ("3.9", "3.9.1", "3.10", "3.10.1", "3.11", "3.12")


def batch(amount, residues_only=False):
    data = None
    for source in SOURCES:
        if residues_only and not source[0].startswith("market for residues"):
            continue
        for kind, uncertainty in (("technosphere", 2), ("substitution", 3)):
            part = consumer(dict(zip(FIELDS, source, strict=True)), amount, uncertainty, kind)
            part[0]["exchanges"][1]["caller_metadata"] = {"retain": [1, 2]}
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
@pytest.mark.parametrize("release", ("2025", "2026"))
@pytest.mark.parametrize("amount", (-2, 0, 2))
def test_signed_market_demand_keeps_real_treatment_and_original_road_quantities(version, release, amount):
    data = batch(amount)
    original = deepcopy(data)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", version))
    result = inventory.migrate_background(target_context(release), policy=POLICY)
    assert len(result.data) == 29
    helpers = {d["code"]: d for d in result.data[1:]}
    by_identity = {tuple(d[f] for f in FIELDS): d for d in result.data[1:]}
    assert result.data[0]["exchanges"][0] == original[0]["exchanges"][0]
    assert result.data[0]["exchanges"][-1] == original[0]["exchanges"][-1]
    for before, after in zip(original[0]["exchanges"][1:-1], result.data[0]["exchanges"][1:-1], strict=True):
        assert after["amount"] == -before["amount"] and after["type"] == before["type"]
        assert after["formula"] == "(dose) * (-1.0)" and after["scale"] == before["scale"]
        assert after["caller_metadata"] == before["caller_metadata"]
        if before["uncertainty type"] == 2:
            assert after["loc"] == before["loc"] and after["negative"] is not before["negative"]
        else:
            assert after["loc"] == -before["loc"]
            assert (after["minimum"], after["maximum"]) == (-before["maximum"], -before["minimum"])
        parent = helpers[after["input"][1]]
        assert not any(e["type"] == "biosphere" for e in parent["exchanges"])
        (treatment,) = [e for e in parent["exchanges"] if e["type"] == "technosphere" and e["unit"] == "kilogram"]
        assert treatment["amount"] == 1
        child = by_identity[tuple(treatment[f] for f in FIELDS)]
        expected = (
            "residual-landfill-" if before["name"].startswith("market for residues") else "industrial-landfarming-"
        )
        assert child["brightpath_conversion"]["recipe_id"].startswith(expected)
        assert not child["brightpath_conversion"].get("zero_burden")
        assert any(e["type"] == "biosphere" and e["amount"] > 0 for e in child["exchanges"])
        freight = [e for e in parent["exchanges"] if e["type"] == "technosphere" and e["unit"] == "ton kilometer"]
        assert freight and all(e["name"] == ROAD["name"] and e["amount"] > 0 for e in freight)
        assert sum(e["amount"] for e in freight) == pytest.approx(0.077)
        assert len(freight) == (3 if before["location"] == "RoW" else 1)
        if version == "3.12" and before["location"] == "RoW":
            assert sorted(e["amount"] for e in freight) == [
                0.0013796252463669672,
                0.008811943375615302,
                0.06680843137801773,
            ]
        if expected == "industrial-landfarming-":
            (service,) = [e for e in child["exchanges"] if e["type"] == "technosphere"]
            assert service["name"] == "Slurry spreading, by vacuum tanker" and service["amount"] == 0.00097087
        assert len([e for e in parent["exchanges"] if e["type"] == "production"]) == 1
    assert not any(
        "aircraft" in e["name"] or "operation, lorry" in e["name"].lower()
        for d in result.data[1:]
        for e in d["exchanges"]
    )
    assert result.last_migration_report.losses and inventory.data == data == original


@pytest.mark.parametrize("version", VERSIONS)
def test_eight_residue_market_graphs_support_native_biosphere_with_only_approved_child_indicator_policy(version):
    canonical = BrightwayInventory.from_data(
        batch(-1, residues_only=True), context=contexts("ecoinvent", version)
    ).migrate_background(target_context("2025"), policy=POLICY)
    original = deepcopy(canonical.data)
    native = canonical.migrate_background(target_context("native2026"), policy=POLICY)
    assert len(native.data) == len(original) == 17
    assert canonical.data == original
    indicators = {
        "Hazardous waste disposed",
        "Non-hazardous waste disposed",
        "Organic carbon, placed in landfill",
        "Waste mass, total, placed in landfill",
    }
    by_code = {d["code"]: d for d in native.data[1:]}

    def physical(dataset):
        return sorted(
            e["amount"] for e in dataset["exchanges"] if e["type"] == "biosphere" and e["name"] not in indicators
        )

    for before in original[1:]:
        after = by_code[before["code"]]
        assert physical(before) == physical(after)
    omitted = [loss for loss in native.last_migration_report.losses if loss.code == "migration.recipe_input_omission"]
    assert all(loss.details["source_exchange"]["name"] in indicators for loss in omitted)


@pytest.mark.parametrize("version", VERSIONS)
def test_six_sludge_children_keep_native_physical_gaps_explicit_and_roll_back_the_entire_market_batch(version):
    data = batch(-1)
    original = deepcopy(data)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", version))
    with pytest.raises(MigrationError) as caught:
        inventory.migrate_background(target_context("native2026"), policy=POLICY)
    assert any("biosphere" in i.code and "unresolved" in i.code for i in caught.value.report.issues)
    assert inventory.data == data == original


@pytest.mark.parametrize("version", VERSIONS)
def test_complete_canonical_market_graph_uses_independent_reverse_treatment_and_road_suppliers(version):
    canonical = BrightwayInventory.from_data(batch(-1), context=contexts("ecoinvent", version)).migrate_background(
        target_context("2025"), policy=POLICY
    )
    original = deepcopy(canonical.data)
    result = canonical.migrate_background(
        BackgroundContext(TechnosphereProfile("ecoinvent", "3.12", "cutoff"), BiosphereProfile("ecoinvent", "3.10")),
        policy=MigrationPolicy(
            on_heuristic_mapping=PolicyAction.WARN,
            on_information_loss=PolicyAction.WARN,
            on_inferred_reverse=PolicyAction.WARN,
        ),
    )
    assert len(result.data) == len(original) == 29 and result.data[0] == original[0]
    assert canonical.data == original
    assert all(d["code"] == before["code"] for d, before in zip(result.data[1:], original[1:], strict=True))


def test_strict_loss_refusal_foreground_ownership_uninspected_versions_and_system_models():
    data = batch(-1)
    original = deepcopy(data)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.12"))
    with pytest.raises(MigrationError):
        inventory.migrate_background(
            target_context("2025"), policy=MigrationPolicy(on_heuristic_mapping=PolicyAction.WARN)
        )
    assert inventory.data == original
    for row in SOURCES.values():
        source = row["source"]
        data.append({**source, "exchanges": [{**source, "type": "production", "amount": -1}]})
    assert (
        BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.12"))
        .migrate_background(target_context("2025"), policy=POLICY)
        .data
        == data
    )
    for version in ("3.8", "3.10.2", "3.13"):
        selected = select_profile_sign_rules(
            load_uvek_technosphere_resource(), TechnosphereProfile("ecoinvent", version, "cutoff")
        )
        assert all(
            r["reference_flow_sign_review"]["status"] == "unresolved"
            for r in selected["replace"]
            if tuple(r["source"][f] for f in FIELDS) in SOURCES
        )
    inventory = BrightwayInventory.from_data(batch(-1), context=contexts("ecoinvent", "3.12", "consequential"))
    with pytest.raises(MigrationError, match="system models"):
        inventory.migrate_background(target_context("2025"), policy=MigrationPolicy.permissive())
    assert inventory.data == batch(-1)


@pytest.mark.parametrize("mutation", ("inventory", "description", "missing", "duplicate", "patch", "native_role"))
def test_all_original_road_treatment_and_native_supplier_evidence_is_required(mutation):
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


@pytest.mark.parametrize("mutation", ("child_recipe", "road_quantity", "road_function", "full_service", "child_trace"))
def test_child_and_complete_road_function_boundaries_cannot_drift(monkeypatch, mutation):
    resource = deepcopy(load_uvek_technosphere_resource())
    validate_bindings(resource)
    if mutation == "child_recipe":
        rid = next(iter(APPROVAL["recipe_dependencies"]))
        resource["recipes"][rid]["inputs"][0]["amount"] *= 2
    elif mutation == "road_quantity":
        p = APPROVAL["rules"][0]["profiles"][0]
        inputs = deepcopy(p["inputs"])
        next(e for e in inputs if e.get("unit") == "ton kilometer")["amount"] /= 1000
        monkeypatch.setitem(p, "inputs", inputs)
    elif mutation == "road_function":
        monkeypatch.setitem(APPROVAL["freight_choices"][0], "target", {**ROAD, "name": "Operation, lorry"})
    elif mutation == "full_service":
        monkeypatch.setitem(APPROVAL["full_road_service_proofs"][0], "vehicle_maintenance_and_road_verified", False)
    else:
        monkeypatch.setitem(APPROVAL["child_checks"][0], "source_amount", 1)
    with pytest.raises(ValueError):
        validate_bindings(resource)
