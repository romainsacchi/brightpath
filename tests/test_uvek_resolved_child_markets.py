"""Complete market demand retains reviewed landfill burdens and actual freight modes."""

from copy import deepcopy

import pytest
from test_uvek_reference_signs import FIELDS, consumer, contexts
from test_uvek_refrigerant_disposal import target_context

from brightpath import BackgroundContext, BiosphereProfile, BrightwayInventory, InventoryContext, TechnosphereProfile
from brightpath.background.reference_signs import select_profile_sign_rules
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_release_resources, load_uvek_technosphere_resource
from scripts.review_uvek_resolved_child_markets import (
    APPROVAL,
    BARGE,
    DIESEL,
    ELECTRIC,
    ROAD,
    SOURCES,
    validate_bindings,
    validate_inventories,
)

POLICY = MigrationPolicy(on_heuristic_mapping=PolicyAction.WARN, on_information_loss=PolicyAction.WARN)
VERSIONS = ("3.7", "3.8", "3.9", "3.9.1", "3.10", "3.10.1", "3.11", "3.12")


def batch(version, amount):
    data = None
    for source, row in SOURCES.items():
        if not any(p["label"] == f"ecoinvent-{version}-cutoff" for p in row["profiles"]):
            continue
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
@pytest.mark.parametrize("amount", (-2, 0, 2))
def test_markets_reuse_real_treatment_children_and_keep_freight_modes_quantities_and_signed_callers(
    version, release, amount
):
    data = batch(version, amount)
    original = deepcopy(data)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", version))
    result = inventory.migrate_background(target_context(release), policy=POLICY)
    helpers = {d["code"]: d for d in result.data[1:]}
    by_identity = {tuple(d[f] for f in FIELDS): d for d in result.data[1:]}
    assert len(helpers) == len(result.data) - 1
    assert result.data[0]["exchanges"][0] == original[0]["exchanges"][0]
    assert result.data[0]["exchanges"][-1]["amount"] == 0.5
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
        children = [e for e in parent["exchanges"] if e["type"] == "technosphere" and e["unit"] == "kilogram"]
        assert len(children) == 1 and children[0]["amount"] == 1
        child = by_identity[tuple(children[0][f] for f in FIELDS)]
        assert child["brightpath_conversion"]["recipe_id"].startswith("residual-landfill-")
        assert not child["brightpath_conversion"].get("zero_burden")
        assert any(e["type"] == "biosphere" and e["amount"] > 0 for e in child["exchanges"])
        freight = [e for e in parent["exchanges"] if e["type"] == "technosphere" and e["unit"] == "ton kilometer"]
        assert freight and all(e["amount"] > 0 for e in freight)
        if before["location"] == "IN":
            assert sorted(e["amount"] for e in freight) == [0.02, 0.112, 0.193]
            assert any(e["name"] == BARGE["name"] and e["amount"] == 0.02 for e in freight)
            assert any(e["name"] == ROAD["name"] and e["amount"] == 0.193 for e in freight)
            rail = next(
                by_identity[tuple(e[f] for f in FIELDS)] for e in freight if tuple(e[f] for f in FIELDS) in by_identity
            )
            traction = {e["name"]: e["amount"] for e in rail["exchanges"] if e["type"] == "technosphere"}
            assert traction == {DIESEL["name"]: 0.355708918522341, ELECTRIC["name"]: 0.644291081477659}
        if before["location"] == "CH":
            assert sorted(e["amount"] for e in freight) == [0.00356, 0.02577]
        if before["location"] == "GLO" or before["reference product"].endswith(", dust"):
            assert [e["amount"] for e in freight] == [0.077]
    assert not any(
        e["name"].startswith("xxx ") or e["name"] == "Transport, coal freight, rail"
        for d in result.data[1:]
        for e in d["exchanges"]
    )
    assert inventory.data == data == original


@pytest.mark.parametrize("version", VERSIONS)
def test_nested_market_and_rail_helpers_keep_the_scoped_native_land_binding_on_later_migration(version):
    canonical = BrightwayInventory.from_data(
        batch(version, -1), context=contexts("ecoinvent", version)
    ).migrate_background(target_context("2025"), policy=POLICY)
    original = deepcopy(canonical.data)
    native = canonical.migrate_background(target_context("native2026"), policy=POLICY)
    assert len(native.data) == len(original)
    assert canonical.data == original
    land = [
        e
        for d in native.data[1:]
        for e in d["exchanges"]
        if e["name"] == "Transformation, from dump site, residual material landfill"
    ]
    assert bool(land) == (version in {"3.10", "3.10.1", "3.11", "3.12"})
    assert all(e["amount"] == 5.7143e-5 for e in land)
    indicators = {
        "Hazardous waste disposed",
        "Non-hazardous waste disposed",
        "Organic carbon, placed in landfill",
        "Waste mass, total, placed in landfill",
    }
    by_code = {d["code"]: d for d in native.data[1:]}
    for before in original[1:]:
        after = by_code[before["code"]]
        physical_before = sorted(
            e["amount"] for e in before["exchanges"] if e["type"] == "biosphere" and e["name"] not in indicators
        )
        physical_after = sorted(
            e["amount"] for e in after["exchanges"] if e["type"] == "biosphere" and e["name"] not in indicators
        )
        assert physical_before == physical_after
    omissions = [loss for loss in native.last_migration_report.losses if loss.code == "migration.recipe_input_omission"]
    assert all(loss.details["source_exchange"]["name"] in indicators for loss in omissions)
    assert bool(omissions) == (version in {"3.10", "3.10.1", "3.11", "3.12"})


@pytest.mark.parametrize("version", VERSIONS)
def test_complete_canonical_market_graph_has_independent_reverse_suppliers(version):
    canonical = BrightwayInventory.from_data(
        batch(version, -1), context=contexts("ecoinvent", version)
    ).migrate_background(target_context("2025"), policy=POLICY)
    original = deepcopy(canonical.data)
    result = canonical.migrate_background(
        BackgroundContext(TechnosphereProfile("ecoinvent", "3.12", "cutoff"), BiosphereProfile("ecoinvent", "3.10")),
        policy=MigrationPolicy(
            on_heuristic_mapping=PolicyAction.WARN,
            on_information_loss=PolicyAction.WARN,
            on_inferred_reverse=PolicyAction.WARN,
        ),
    )
    assert len(result.data) == len(original) and result.data[0] == original[0]
    assert canonical.data == original
    assert all(d["code"] == before["code"] for d, before in zip(result.data[1:], original[1:], strict=True))


@pytest.mark.parametrize("release", ("2025", "2026", "native2026"))
def test_eight_older_clinker_dependent_profiles_cannot_inherit_newer_approval_or_omit_unresolved_inputs(release):
    count = 0
    for source, row in SOURCES.items():
        for profile in row["pending_profiles"]:
            count += 1
            version = profile["label"].split("-")[1]
            data = batch(version, -1)
            data[0]["exchanges"].insert(1, consumer(dict(zip(FIELDS, source, strict=True)), -1)[0]["exchanges"][1])
            original = deepcopy(data)
            inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", version))
            with pytest.raises(MigrationError) as caught:
                inventory.migrate_background(target_context(release), policy=MigrationPolicy.permissive())
            assert any(i.code == "migration.reference_flow_sign_unresolved" for i in caught.value.report.issues)
            assert inventory.data == data == original
    assert count == 8


@pytest.mark.parametrize("version", ("3.6", *VERSIONS))
@pytest.mark.parametrize("release", ("2025", "2026", "native2026"))
@pytest.mark.parametrize("amount", (-2, 0, 2))
def test_four_independently_inspected_reverse_freight_suppliers_preserve_ton_km_and_uncertainty(
    version, release, amount
):
    suppliers = [r["source"] for r in APPROVAL["reverse_bindings"]]
    if release != "2025":
        bridge = next(
            r for r in load_uvek_release_resources("technosphere") if r["source_profiles"][0]["version"] == "2025"
        )
        suppliers = [next(r["target"] for r in bridge["replace"] if r["source"] == source) for source in suppliers]
    data = consumer(suppliers[0], amount, 3)
    data[0]["exchanges"][1:] = [consumer(s, amount, 3)[0]["exchanges"][1] for s in suppliers]
    inventory = BrightwayInventory.from_data(
        data, context=InventoryContext(contexts("uvek", "2025").format, target_context(release))
    )
    result = inventory.migrate_background(
        BackgroundContext(TechnosphereProfile("ecoinvent", version, "cutoff"), BiosphereProfile("ecoinvent", version)),
        policy=MigrationPolicy(
            on_heuristic_mapping=PolicyAction.WARN,
            on_information_loss=PolicyAction.WARN,
            on_inferred_reverse=PolicyAction.WARN,
        ),
    )
    assert len(result.data) == 1
    for before, after in zip(data[0]["exchanges"][1:], result.data[0]["exchanges"][1:], strict=True):
        for field in ("amount", "unit", "formula", "loc", "scale", "minimum", "maximum"):
            assert after[field] == before[field]
    assert inventory.data == data


def test_strict_loss_refusal_and_foreground_supplier_ownership_are_transactional():
    data = batch("3.12", -1)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.12"))
    with pytest.raises(MigrationError):
        inventory.migrate_background(
            target_context("2025"), policy=MigrationPolicy(on_heuristic_mapping=PolicyAction.WARN)
        )
    assert inventory.data == data
    for source, row in SOURCES.items():
        if any(p["label"] == "ecoinvent-3.12-cutoff" for p in row["profiles"]):
            supplier = dict(zip(FIELDS, source, strict=True))
            data.append({**supplier, "exchanges": [{**supplier, "type": "production", "amount": -1}]})
    assert (
        BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.12"))
        .migrate_background(target_context("2025"), policy=POLICY)
        .data
        == data
    )


def test_native_indicator_omission_is_confined_to_reviewed_helpers_and_strict_later_migration_rolls_back():
    canonical = BrightwayInventory.from_data(
        batch("3.12", -1), context=contexts("ecoinvent", "3.12")
    ).migrate_background(target_context("2025"), policy=POLICY)
    original = deepcopy(canonical.data)
    with pytest.raises(MigrationError) as strict:
        canonical.migrate_background(
            target_context("native2026"), policy=MigrationPolicy(on_heuristic_mapping=PolicyAction.WARN)
        )
    assert any(
        i.code == "migration.recipe_input_omission" and i.severity.value == "error" for i in strict.value.report.issues
    )
    assert canonical.data == original
    data = canonical.data
    data[0]["exchanges"].append(
        {
            "name": "Organic carbon, placed in landfill",
            "categories": ["inventory indicator", "waste"],
            "unit": "kilogram",
            "type": "biosphere",
            "amount": 0.25,
        }
    )
    inventory = BrightwayInventory.from_data(data, context=canonical.context)
    with pytest.raises(MigrationError) as unrelated:
        inventory.migrate_background(target_context("native2026"), policy=POLICY)
    assert any(i.code == "background.biosphere_link_unresolved" for i in unrelated.value.report.issues)
    assert inventory.data == data


def test_uninspected_profiles_and_consequential_models_do_not_inherit_cutoff_market_helpers():
    selected = select_profile_sign_rules(
        load_uvek_technosphere_resource(), TechnosphereProfile("ecoinvent", "3.13", "cutoff")
    )
    assert all(
        r["reference_flow_sign_review"]["status"] == "unresolved"
        for r in selected["replace"]
        if tuple(r["source"][f] for f in FIELDS) in SOURCES
    )
    data = batch("3.12", -1)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.12", "consequential"))
    with pytest.raises(MigrationError, match="system models"):
        inventory.migrate_background(target_context("native2026"), policy=MigrationPolicy.permissive())
    assert inventory.data == data


@pytest.mark.parametrize("mutation", ("inventory", "description", "missing", "duplicate", "patch", "native_role"))
def test_inventory_boundary_patch_or_native_role_drift_requires_new_review(mutation):
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


def test_child_recipe_and_freight_choices_cannot_inherit_drifted_evidence(monkeypatch):
    resource = deepcopy(load_uvek_technosphere_resource())
    validate_bindings(resource)
    rid = next(iter(APPROVAL["recipe_dependencies"]))
    resource["recipes"][rid]["inputs"][0]["amount"] *= 2
    with pytest.raises(ValueError, match="recipe dependency changed"):
        validate_bindings(resource)
    resource = deepcopy(load_uvek_technosphere_resource())
    monkeypatch.setitem(APPROVAL["freight_choices"][0], "target", {**ROAD, "name": "Wrong service"})
    with pytest.raises(ValueError, match="freight evidence"):
        validate_bindings(resource)
