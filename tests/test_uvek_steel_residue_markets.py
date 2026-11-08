"""Steel-waste composition omits only approved handoffs and retains real landfill burdens."""

from copy import deepcopy

import pytest
from test_uvek_reference_signs import FIELDS, consumer, contexts
from test_uvek_refrigerant_disposal import target_context

from brightpath import BackgroundContext, BiosphereProfile, BrightwayInventory, TechnosphereProfile
from brightpath.background.reference_signs import select_profile_sign_rules
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource
from scripts.review_uvek_complete_patch_boundaries import CASES as COMPLETION_CASES
from scripts.review_uvek_exact_patch_boundaries import CASES as PATCH_CASES
from scripts.review_uvek_steel_residue_markets import APPROVAL, SOURCES, validate_bindings, validate_inventories

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
@pytest.mark.parametrize("release", ("2025", "2026"))
@pytest.mark.parametrize("amount", (-2, 0, 2))
def test_signed_demand_preserves_uncertainty_ownership_emissions_and_nonempty_treatment(version, release, amount):
    data = batch(version, amount)
    original = deepcopy(data)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", version))
    result = inventory.migrate_background(target_context(release), policy=POLICY)
    helpers = {d["code"]: d for d in result.data[1:]}
    assert len(helpers) == len(result.data) - 1
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
        helper = helpers[after["input"][1]]
        assert helper["exchanges"] and not helper["brightpath_conversion"].get("zero_burden")
        assert [e["amount"] for e in helper["exchanges"] if e["type"] == "production"] == [1]
        if before["name"].startswith("market"):
            assert not any(e["type"] == "biosphere" for e in helper["exchanges"])
            assert any(
                e["type"] == "technosphere" and e["unit"] == "kilogram" and e["amount"] > 0 for e in helper["exchanges"]
            )
        else:
            assert any(
                e["type"] == "biosphere" and e["categories"] == ["water", "ground-, long-term"]
                for e in helper["exchanges"]
            )
    assert not any(
        "for recovery" in e["name"] or "deinking" in e["name"] or "coal freight" in e["name"] or "aircraft" in e["name"]
        for d in result.data[1:]
        for e in d["exchanges"]
    )
    assert result.last_migration_report.losses
    assert inventory.data == data == original


def test_modern_markets_keep_unrenormalized_waste_fractions_separate_freight_and_explicit_signed_omission_warnings():
    result = BrightwayInventory.from_data(batch("3.12", -1), context=contexts("ecoinvent", "3.12")).migrate_background(
        target_context("2025"), policy=POLICY
    )
    helpers = {d["code"]: d for d in result.data[1:]}
    expected = {
        ("market for basic oxygen furnace dust", "IN"): (1, None),
        ("market for blast furnace sludge", "IN"): (1, None),
        ("market for basic oxygen furnace dust", "RER"): (0.15581098339719, -0.84418901660281),
        ("market for basic oxygen furnace dust", "RoW"): (0.15581098339719, -0.84418901660281),
        ("market for blast furnace sludge", "CH"): (1, None),
        ("market for blast furnace sludge", "Europe without Switzerland"): (0.8900718609803939, -0.1099281390196062),
        ("market for blast furnace sludge", "RoW"): (0.04418490695759889, -0.9558150930424011),
    }
    for before, after in zip(batch("3.12", -1)[0]["exchanges"][1:-1], result.data[0]["exchanges"][1:-1], strict=True):
        if not before["name"].startswith("market"):
            continue
        fraction, omitted = expected[before["name"], before["location"]]
        helper = helpers[after["input"][1]]
        assert [
            e["amount"] for e in helper["exchanges"] if e["type"] == "technosphere" and e["unit"] == "kilogram"
        ] == [fraction]
        if omitted is not None:
            assert str(omitted) in helper["comment"] and "omit signed" in helper["comment"]
        freight = [
            e["amount"] for e in helper["exchanges"] if e["type"] == "technosphere" and e["unit"] == "ton kilometer"
        ]
        assert all(amount > 0 for amount in freight)
        if before["location"] == "RoW":
            assert sorted(freight) == [0.0013796252463669672, 0.008811943375615302, 0.06680843137801773]
        if before["location"] == "CH":
            assert sorted(freight) == [0.00356, 0.02577]
        if before["location"] == "IN":
            assert sorted(freight) == [0.02, 0.112, 0.193]
    # The source landfill model, rather than a different waste's native emissions, remains explicit.
    for after in result.data[0]["exchanges"][1:-1]:
        helper = helpers[after["input"][1]]
        if helper["brightpath_conversion"]["recipe_id"].startswith("steel-residue-landfill-"):
            cement = sum(
                e["amount"]
                for e in helper["exchanges"]
                if e["type"] == "technosphere" and e["name"] == "Cement, unspecified, at plant"
            )
            assert cement == pytest.approx(0.4)
            assert any(
                e["type"] == "biosphere" and e["name"] == "Transformation, from dump site" and e["amount"] == 5.7143e-5
                for e in helper["exchanges"]
            )


@pytest.mark.parametrize("version", VERSIONS)
def test_canonical_graph_has_independently_reviewed_reverse_suppliers(version):
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
    assert result.data[0] == original[0] and len(result.data) == len(original)
    assert canonical.data == original


@pytest.mark.parametrize("release", ("2025", "2026", "native2026"))
def test_clinker_and_steam_profiles_remain_transactionally_blocked_even_under_permissive_policy(release):
    count = 0
    for source, row in SOURCES.items():
        for profile in row["pending_profiles"]:
            count += 1
            version = profile["label"].split("-")[1]
            data = consumer(dict(zip(FIELDS, source, strict=True)), -1)
            original = deepcopy(data)
            inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", version))
            with pytest.raises(MigrationError) as caught:
                inventory.migrate_background(target_context(release), policy=MigrationPolicy.permissive())
            assert any(i.code == "migration.reference_flow_sign_unresolved" for i in caught.value.report.issues)
            assert inventory.data == data == original
    assert count == 20


def test_native_land_refinement_does_not_leak_from_36_previously_approved_contexts_to_new_steel_treatments():
    canonical = BrightwayInventory.from_data(
        batch("3.12", -1), context=contexts("ecoinvent", "3.12")
    ).migrate_background(target_context("2025"), policy=POLICY)
    original = deepcopy(canonical.data)
    with pytest.raises(MigrationError) as caught:
        canonical.migrate_background(target_context("native2026"), policy=POLICY)
    assert any(i.code == "background.biosphere_link_unresolved" for i in caught.value.report.issues)
    assert canonical.data == original


def test_strict_loss_rejection_and_exact_foreground_supplier_ownership_preserve_source_data():
    data = batch("3.12", -1)
    original = deepcopy(data)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.12"))
    with pytest.raises(MigrationError):
        inventory.migrate_background(
            target_context("2025"), policy=MigrationPolicy(on_heuristic_mapping=PolicyAction.WARN)
        )
    assert inventory.data == original
    for source in SOURCES:
        supplier = dict(zip(FIELDS, source, strict=True))
        data.append({**supplier, "exchanges": [{**supplier, "type": "production", "amount": -1}]})
    assert (
        BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.12"))
        .migrate_background(target_context("2025"), policy=POLICY)
        .data
        == data
    )


def test_uninspected_patch_and_consequential_models_cannot_inherit_cutoff_approval():
    selected = select_profile_sign_rules(
        load_uvek_technosphere_resource(), TechnosphereProfile("ecoinvent", "3.10.2", "cutoff")
    )
    assert all(
        r["reference_flow_sign_review"]["status"] == "unresolved"
        for r in selected["replace"]
        if tuple(r["source"][f] for f in FIELDS) in SOURCES
    )
    data = batch("3.12", -1)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.12", "consequential"))
    with pytest.raises(MigrationError, match="system models"):
        inventory.migrate_background(target_context("2025"), policy=MigrationPolicy.permissive())
    assert inventory.data == data


@pytest.mark.parametrize("mutation", ("inventory", "description", "missing", "duplicate", "patch", "native_role"))
def test_actual_inventory_and_exact_profile_guards_require_new_review_on_drift(mutation):
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


def test_omission_quantity_and_freight_scope_cannot_inherit_modified_review(monkeypatch):
    resource = deepcopy(load_uvek_technosphere_resource())
    validate_bindings(resource)
    profile = next(p for row in APPROVAL["rules"] for p in row["profiles"] if p["omissions"])
    monkeypatch.setitem(profile["omissions"][0], "amount", 0)
    with pytest.raises(ValueError, match="quantities or omission boundary changed"):
        validate_bindings(resource)


def test_nested_regional_rail_shares_preserve_actual_diesel_and_electric_ton_km():
    source = next(s for s in SOURCES if s[0] == "market for basic oxygen furnace dust" and s[2] == "RER")
    data = consumer(dict(zip(FIELDS, source, strict=True)), -1)
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.12")).migrate_background(
        target_context("2025"), policy=POLICY
    )
    helpers = {tuple(d[f] for f in FIELDS): d for d in result.data[1:]}
    parent = next(d for d in result.data[1:] if d["code"] == result.data[0]["exchanges"][1]["input"][1])
    (rail_input,) = [
        e
        for e in parent["exchanges"]
        if e["type"] == "technosphere" and tuple(e[f] for f in FIELDS) in helpers and e["unit"] == "ton kilometer"
    ]
    rail = helpers[tuple(rail_input[f] for f in FIELDS)]
    traction = [e for e in rail["exchanges"] if e["type"] == "technosphere"]
    assert len(traction) == 4
    assert rail_input["amount"] == 0.00735211149004253
    diesel = sum(e["amount"] for e in traction if "diesel" in e["name"])
    electric = sum(e["amount"] for e in traction if "electricity" in e["name"])
    assert rail_input["amount"] * diesel == pytest.approx(0.002852687312888655)
    assert rail_input["amount"] * electric == pytest.approx(0.004499424177153874)
    assert diesel + electric == pytest.approx(1)


def test_four_parent_patch_handoffs_have_separate_exact_profile_omission_approvals():
    count = 0
    for row in APPROVAL["rules"]:
        for profile in row["profiles"]:
            for omission in profile["omissions"]:
                if omission["authorization_label"] == profile["label"]:
                    continue
                count += 1
                data = consumer(dict(zip(FIELDS, omission["source"], strict=True)), -1)
                inventory = BrightwayInventory.from_data(
                    data, context=contexts("ecoinvent", profile["label"].split("-")[1])
                )
                # These four were subsequently inspected outside their parent helpers.
                # Require their own exact-profile approval, not inherited parent scope.
                key = (profile["label"], tuple(omission["source"]))
                approval = {**PATCH_CASES, **COMPLETION_CASES}[key]
                assert approval["kind"] == "handoff"
                assert approval["inventory_sha256"] and approval["endpoint_inventory_sha256"]
                result = inventory.migrate_background(target_context("2025"), policy=MigrationPolicy.permissive())
                assert result.data[0]["exchanges"] == data[0]["exchanges"][:1]
                report = result.last_migration_report
                warnings = [i for i in report.issues if i.code == "migration.technosphere_omission"]
                losses = [i for i in report.losses if i.code == "migration.technosphere_omission"]
                assert len(warnings) == len(losses) == 1
                assert warnings[0].severity.value == "warning"
                assert losses[0].details["source_exchange"] == data[0]["exchanges"][1]
                assert not any(i.code == "migration.reference_flow_sign_unresolved" for i in report.issues)
                assert inventory.data == data
    assert count == 4


@pytest.mark.parametrize("version", ("3.7", "3.8", "3.9", "3.9.1"))
def test_older_steel_helpers_can_use_native_biosphere_without_extending_modern_land_refinement(version):
    canonical = BrightwayInventory.from_data(
        batch(version, -1), context=contexts("ecoinvent", version)
    ).migrate_background(target_context("2025"), policy=POLICY)
    original = deepcopy(canonical.data)
    native = canonical.migrate_background(target_context("native2026"), policy=POLICY)
    assert len(native.data) == len(original)
    assert canonical.data == original
    by_code = {d["code"]: d for d in native.data[1:]}

    def physical(dataset):
        return sorted(
            e["amount"]
            for e in dataset["exchanges"]
            if e["type"] == "biosphere"
            and e.get("categories", [None])[0] not in ("inventory indicator", "inventory indicators")
        )

    for before in original[1:]:
        assert physical(before) == physical(by_code[before["code"]])
    assert not any(i.code.endswith("link_unresolved") for i in native.last_migration_report.issues)
