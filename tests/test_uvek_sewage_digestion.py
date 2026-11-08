"""Treatment volume, allocated gas and native operating burdens remain distinct."""

from copy import deepcopy

import pytest
from test_uvek_reference_signs import FIELDS, consumer, contexts
from test_uvek_refrigerant_disposal import target_context

from brightpath import BackgroundContext, BiosphereProfile, BrightwayInventory, InventoryContext, TechnosphereProfile
from brightpath.background.reference_signs import select_profile_sign_rules
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_release_resources, load_uvek_technosphere_resource
from scripts.review_uvek_sewage_digestion import (
    APPROVAL,
    MARKETS,
    SOURCES,
    market_decision,
    validate_bindings,
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
    return data


@pytest.mark.parametrize("version", VERSIONS)
@pytest.mark.parametrize("release", ("2025", "2026", "native2026"))
@pytest.mark.parametrize("amount", (-10, 0, 10))
def test_each_exact_version_preserves_allocated_functions_signs_emissions_and_owned_input(version, release, amount):
    data = batch(amount)
    saved = deepcopy(data)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", version))
    result = inventory.migrate_background(target_context(release), policy=POLICY)
    assert len(result.data) == 5
    helpers = {d["code"]: d for d in result.data[1:]}
    assert result.data[0]["exchanges"][0] == saved[0]["exchanges"][0]
    assert result.data[0]["exchanges"][-1]["amount"] == 0.5
    native = release == "native2026"
    for before, after in zip(saved[0]["exchanges"][1:-1], result.data[0]["exchanges"][1:-1], strict=True):
        treatment = before["reference product"] == "sewage sludge"
        factor = -1 if treatment else 1
        assert after["amount"] == factor * before["amount"] and after["type"] == before["type"]
        assert after["caller_metadata"] == before["caller_metadata"]
        assert after["formula"] == ("(dose) * (-1.0)" if treatment else "dose")
        assert after["scale"] == before["scale"]
        if before["uncertainty type"] == 2:
            assert after["loc"] == before["loc"]
            assert after["negative"] == (not before["negative"] if treatment else before["negative"])
        else:
            assert after["loc"] == factor * before["loc"]
            assert (after["minimum"], after["maximum"]) == (
                (-before["maximum"], -before["minimum"]) if treatment else (before["minimum"], before["maximum"])
            )
        helper = helpers[after["input"][1]]
        tech = [e for e in helper["exchanges"] if e["type"] == "technosphere"]
        bio = [e for e in helper["exchanges"] if e["type"] == "biosphere"]
        assert not any(e["name"] == "Biogas, from sewage sludge, at storage" for e in tech)
        if not treatment:
            assert not tech and all(e["amount"] == 0 for e in bio)
            assert len(bio) == (3 if version == "3.6" else 2 if version in ("3.7", "3.8") else 0)
            if not bio:
                assert helper["brightpath_conversion"]["zero_burden"] is True
            continue
        assert tech
        (methane,) = [e for e in bio if e["name"] == ("Methane, biogenic" if native else "Methane, non-fossil")]
        expected_methane = (
            0.05640909090909093 if before["location"] == "CH" and version != "3.6" else 0.0558416818181818
        )
        assert methane["amount"] == pytest.approx(expected_methane)
        assert methane["categories"] == (["emissions to air", "unspecified"] if native else ["air"])
        (co2,) = [
            e for e in bio if e["name"] == ("Carbon dioxide, biogenic" if native else "Carbon dioxide, non-fossil")
        ]
        assert co2["amount"] == pytest.approx(1.65666863636364)
        uptake = [e for e in bio if e["name"] == "Carbon dioxide, in air"]
        assert len(uptake) == (1 if version == "3.6" else 0)
        if uptake:
            assert uptake[0]["amount"] == pytest.approx(33.2946363636364)
        indicators = [e for e in bio if e["name"] == "Non-hazardous waste disposed"]
        assert len(indicators) == (1 if version == "3.12" and not native else 0)
        if indicators:
            assert indicators[0]["amount"] == pytest.approx(16590.9090909091 if before["location"] == "CH" else 1000)
        if before["location"] == "CH":
            expected_inputs = (
                [6.06065909090909e-7, 0.00472724772727273, 4.18173863636364, 66.7634772727273]
                if version == "3.6"
                else [6.055681818181821e-7, 0.085775, 3.2684090909090924, 58.83136363636366]
            )
            assert sorted(e["amount"] for e in tech) == pytest.approx(sorted(expected_inputs))
    indicator_losses = [
        loss for loss in result.last_migration_report.losses if loss.code == "migration.recipe_input_omission"
    ]
    assert len(indicator_losses) == (2 if version == "3.12" and native else 0)
    assert all(loss.details["source_exchange"]["name"] == "Non-hazardous waste disposed" for loss in indicator_losses)
    assert inventory.data == data == saved


def test_source_waste_indicators_keep_their_exact_amounts_when_target_supports_them():
    source = contexts("ecoinvent", "3.12")
    result = BrightwayInventory.from_data(batch(-2), context=source).migrate_background(
        BackgroundContext(TechnosphereProfile("uvek", "2025", "cutoff"), BiosphereProfile("ecoinvent", "3.12")),
        policy=POLICY,
    )
    indicators = [
        e["amount"] for d in result.data[1:] for e in d["exchanges"] if e.get("name") == "Non-hazardous waste disposed"
    ]
    assert sorted(indicators) == pytest.approx([1000, 16590.9090909091])
    assert not any(loss.code == "migration.recipe_input_omission" for loss in result.last_migration_report.losses)


@pytest.mark.parametrize("release", ("2025", "2026", "native2026"))
@pytest.mark.parametrize("amount", (-2, 0, 2))
@pytest.mark.parametrize("version", VERSIONS)
def test_reverse_reconstructs_native_operating_inputs_and_its_own_methane_per_gas_m3(release, amount, version):
    supplier = APPROVAL["reverse"]["source"]
    data = consumer(supplier, amount, 3)
    original = deepcopy(data)
    # The release bridge resolves the same physical gas supplier for 2026.
    source = target_context(release)
    if release != "2025":
        from brightpath.migrations.resources import load_uvek_release_resources

        bridge = next(
            r for r in load_uvek_release_resources("technosphere") if r["source_profiles"][0]["version"] == "2025"
        )
        supplier = next(r["target"] for r in bridge["replace"] if r["source"] == supplier)
        data[0]["exchanges"][1].update(supplier)
        original = deepcopy(data)
    inventory = BrightwayInventory.from_data(
        data, context=InventoryContext(contexts("uvek", source.technosphere.version).format, source)
    )
    target = BackgroundContext(
        TechnosphereProfile("ecoinvent", version, "cutoff"), BiosphereProfile("ecoinvent", version)
    )
    policy = MigrationPolicy(
        on_heuristic_mapping=PolicyAction.WARN,
        on_information_loss=PolicyAction.WARN,
        on_inferred_reverse=PolicyAction.WARN,
    )
    result = inventory.migrate_background(target, policy=policy)
    assert result.background_profile.version == version
    assert len(result.data) == 2
    assert result.data[0]["exchanges"][1]["amount"] == amount
    assert result.data[0]["exchanges"][1]["formula"] == "dose"
    helper = result.data[1]
    tech = [e for e in helper["exchanges"] if e["type"] == "technosphere"]
    bio = [e for e in helper["exchanges"] if e["type"] == "biosphere"]
    assert len(tech) == 4 and len(bio) == 2
    assert sorted(e["amount"] for e in tech) == pytest.approx(sorted([3.65e-8, 0.00517, 0.197, 3.55]))
    assert not any(e.get("reference product") in ("biogas", "sewage sludge") for e in tech)
    assert next(e["amount"] for e in bio if e["name"] == "Methane, non-fossil") == 0.014
    assert next(e["amount"] for e in bio if e["name"] == "Carbon dioxide, non-fossil") == 0.0999
    assert inventory.data == data == original


def observed_evidence():
    records, observations, native = {}, {"ecoinvent": {}, "uvek": {}}, {}
    for row in APPROVAL["inventories"]:
        key = tuple(row["source"][f] for f in FIELDS)
        record = {**row, "source": list(key)}
        records.setdefault(key, []).append(record)
        family = row["label"].split("-")[0]
        observations[family].setdefault(key, set()).add((row["label"], row["reference_amount"]))
        if family == "uvek":
            native[key] = {"native_flow_type": "PRODUCT_FLOW", "native_reference_is_input": False}
    return records, observations, native


@pytest.mark.parametrize(
    "mutation",
    ("inventory", "description", "activity", "active_gas", "patch_reference", "missing", "duplicate", "native_role"),
)
def test_allocation_inventory_patch_or_native_role_changes_require_new_review(mutation):
    records, observations, native = observed_evidence()
    validate_inventories(records, observations, native)
    gas = next(key for key in SOURCES if key[1] == "biogas")
    row = records[gas][0]
    if mutation == "inventory":
        row["inventory_sha256"] = "0" * 64
    elif mutation == "description":
        row["product_information_sha256"] = "0" * 64
    elif mutation == "activity":
        row["activity"] = "different allocation activity"
    elif mutation == "active_gas":
        row["active_nonproduction_count"] = 1
    elif mutation == "patch_reference":
        next(r for r in records[gas] if r["label"] == "ecoinvent-3.9.1-cutoff")["reference_amount"] = -1
    elif mutation == "missing":
        records[gas].pop()
    elif mutation == "duplicate":
        records[gas].append(deepcopy(row))
    else:
        native[next(iter(native))]["native_reference_is_input"] = True
    with pytest.raises(ValueError, match="repeat the scientific review"):
        validate_inventories(records, observations, native)


def test_changed_component_mapping_cannot_inherit_the_complete_helper_approval():
    resource = deepcopy(load_uvek_technosphere_resource())
    validate_bindings(resource)
    component = next(r for r in resource["replace"] if r["source"]["name"] == "market for chemical, inorganic")
    component["target"]["name"] = "Another chemical"
    with pytest.raises(ValueError, match="component binding changed"):
        validate_bindings(resource)


@pytest.mark.parametrize("release", ("2025", "2026", "native2026"))
def test_digestion_recipes_cannot_enable_consequential_conversion(release):
    data = batch(-1)
    original = deepcopy(data)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.12", "consequential"))
    with pytest.raises(MigrationError, match="system models"):
        inventory.migrate_background(target_context(release), policy=MigrationPolicy.permissive())
    assert inventory.data == original


def test_uninspected_profiles_cannot_inherit_patch_or_base_allocations():
    resource = select_profile_sign_rules(
        load_uvek_technosphere_resource(), TechnosphereProfile("ecoinvent", "3.13", "cutoff")
    )
    chosen = [r for r in resource["replace"] if tuple(r["source"][f] for f in FIELDS) in SOURCES]
    assert len(chosen) == 4 and all(r["reference_flow_sign_review"]["status"] == "unresolved" for r in chosen)


def test_proxy_refusal_is_transactional_and_owned_suppliers_keep_their_reference_coordinates():
    data = batch(-1)
    original = deepcopy(data)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.12"))
    with pytest.raises(MigrationError):
        inventory.migrate_background(
            target_context("2025"), policy=MigrationPolicy(on_heuristic_mapping=PolicyAction.WARN)
        )
    assert inventory.data == original
    for source in SOURCES:
        record = dict(zip(FIELDS, source, strict=True))
        data.append(
            {
                **record,
                "exchanges": [{**record, "type": "production", "amount": -1 if source[1] == "sewage sludge" else 1}],
            }
        )
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.12")).migrate_background(
        target_context("2025"), policy=POLICY
    )
    assert result.data == data


def market_batch(amount):
    """Demand each market and its direct child together to expose duplicate helpers."""
    data = None
    for source in MARKETS:
        for kind, uncertainty in (("technosphere", 2), ("substitution", 3)):
            part = consumer(dict(zip(FIELDS, source, strict=True)), amount, uncertainty, kind)
            part[0]["exchanges"][1]["caller_metadata"] = {"keep": [1, 2]}
            if data is None:
                data = part
            else:
                data[0]["exchanges"].append(part[0]["exchanges"][1])
        child = ("treatment of sewage sludge by anaerobic digestion", "sewage sludge", source[2], "cubic meter")
        data[0]["exchanges"].append(consumer(dict(zip(FIELDS, child, strict=True)), amount, 3)[0]["exchanges"][1])
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
def test_markets_reuse_the_exact_full_digestion_child_without_gas_volume_or_duplicate_burdens(version, release, amount):
    data = market_batch(amount)
    saved = deepcopy(data)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", version))
    result = inventory.migrate_background(target_context(release), policy=POLICY)
    direct = BrightwayInventory.from_data(batch(amount), context=contexts("ecoinvent", version)).migrate_background(
        target_context(release), policy=POLICY
    )
    assert len(result.data) == 3
    helpers = {d["code"]: d for d in result.data[1:]}
    assert result.data[0]["exchanges"][0] == saved[0]["exchanges"][0]
    assert result.data[0]["exchanges"][-1]["amount"] == 0.5
    assert {d["brightpath_conversion"]["recipe_id"] for d in helpers.values()} <= {
        d["brightpath_conversion"]["recipe_id"] for d in direct.data[1:]
    }
    for before, after in zip(saved[0]["exchanges"][1:-1], result.data[0]["exchanges"][1:-1], strict=True):
        assert after["amount"] == -before["amount"] and after["type"] == before["type"]
        assert after["formula"] == "(dose) * (-1.0)" and after["scale"] == before["scale"]
        if "caller_metadata" in before:
            assert after["caller_metadata"] == before["caller_metadata"]
        if before["uncertainty type"] == 2:
            assert after["loc"] == before["loc"] and after["negative"] is not before["negative"]
        else:
            assert after["loc"] == -before["loc"]
            assert (after["minimum"], after["maximum"]) == (-before["maximum"], -before["minimum"])
        helper = helpers[after["input"][1]]
        peer = next(
            d
            for d in direct.data[1:]
            if d["brightpath_conversion"]["recipe_id"] == helper["brightpath_conversion"]["recipe_id"]
        )
        assert helper["exchanges"] == peer["exchanges"]
        assert any(e["type"] == "biosphere" and e["amount"] != 0 for e in helper["exchanges"])
        assert not any(e["name"] == "Biogas, from sewage sludge, at storage" for e in helper["exchanges"])
    for offset in (1, 4):
        assert len({tuple(e[f] for f in FIELDS) for e in result.data[0]["exchanges"][offset : offset + 3]}) == 1
    assert inventory.data == data == saved
    assert result.migrate_background(target_context(release), policy=POLICY).data == result.data


@pytest.mark.parametrize("release", ("2025", "2026", "native2026"))
@pytest.mark.parametrize("version", VERSIONS)
def test_reused_market_helper_graph_has_independent_reverse_components(release, version):
    data = market_batch(-1)
    forward = BrightwayInventory.from_data(data, context=contexts("ecoinvent", version)).migrate_background(
        target_context(release), policy=POLICY
    )
    saved = deepcopy(forward.data)
    reverse = forward.migrate_background(
        contexts("ecoinvent", version).background,
        policy=MigrationPolicy(
            on_heuristic_mapping=PolicyAction.WARN,
            on_information_loss=PolicyAction.WARN,
            on_inferred_reverse=PolicyAction.WARN,
        ),
    )
    assert len(reverse.data) == len(saved) == 3
    assert reverse.data[0]["exchanges"][:-1] == saved[0]["exchanges"][:-1]
    assert reverse.data[0]["exchanges"][-1] == data[0]["exchanges"][-1]
    for helper in reverse.data[1:]:
        assert any(e["type"] == "biosphere" and e["name"] == "Methane, non-fossil" for e in helper["exchanges"])
        assert not any(e["name"] == "Biogas, from sewage sludge, at storage" for e in helper["exchanges"])
    assert forward.data == saved


def test_aliases_preserve_foreground_ownership_and_require_loss_acceptance():
    data = market_batch(-1)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.12"))
    original = deepcopy(data)
    with pytest.raises(MigrationError):
        inventory.migrate_background(
            target_context("2025"), policy=MigrationPolicy(on_heuristic_mapping=PolicyAction.WARN)
        )
    assert inventory.data == original
    for source in [*MARKETS, *(s for s in SOURCES if s[1] == "sewage sludge")]:
        row = dict(zip(FIELDS, source, strict=True))
        data.append({**row, "exchanges": [{**row, "type": "production", "amount": -1}]})
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.12")).migrate_background(
        target_context("2025"), policy=POLICY
    )
    assert result.data == data


@pytest.mark.parametrize("version", ("3.13", "3.12.1"))
def test_market_alias_does_not_enable_uninspected_profiles(version):
    selected = select_profile_sign_rules(
        load_uvek_technosphere_resource(), TechnosphereProfile("ecoinvent", version, "cutoff")
    )
    aliases = [r for r in selected["replace"] if tuple(r["source"][f] for f in FIELDS) in MARKETS]
    assert len(aliases) == 2 and all(r["reference_flow_sign_review"]["status"] == "unresolved" for r in aliases)


def test_market_alias_does_not_enable_consequential_profiles():
    inventory = BrightwayInventory.from_data(market_batch(-1), context=contexts("ecoinvent", "3.12", "consequential"))
    original = deepcopy(inventory.data)
    with pytest.raises(MigrationError, match="system models"):
        inventory.migrate_background(target_context("2025"), policy=MigrationPolicy.permissive())
    assert inventory.data == original


@pytest.mark.parametrize("change", ("hash", "missing", "sign", "prior_target"))
def test_alias_source_drift_requires_repeat_scientific_review(change):
    _source, row = next(iter(MARKETS.items()))
    rule = {"source": row["source"], "target": deepcopy(row["previous_target"])}
    inventories = [
        {"label": p["label"], "inventory_sha256": p["inventory_sha256"], "reference_amount": -1}
        for p in row["profiles"]
    ]
    observations = {(p["label"], -1) for p in row["profiles"]}
    market_decision(rule, observations, inventories, "synthetic", None)
    if change == "hash":
        inventories[0]["inventory_sha256"] = "0" * 64
    elif change == "missing":
        inventories.pop()
    elif change == "sign":
        observations = {(p["label"], 1) for p in row["profiles"]}
    else:
        rule["target"]["name"] = "Another gas or sludge reference"
    with pytest.raises(ValueError, match="repeat the scientific review"):
        market_decision(rule, observations, inventories, "synthetic", None)


def test_alias_child_inventory_cannot_change_without_new_review():
    resource = deepcopy(load_uvek_technosphere_resource())
    validate_bindings(resource)
    recipe_id = next(iter(APPROVAL["market_recipe_dependencies"]))
    resource["recipes"][recipe_id]["inputs"][0]["amount"] *= 2
    with pytest.raises(ValueError, match="recipe dependency changed"):
        validate_bindings(resource)


@pytest.mark.parametrize("release", ("2025", "2026", "native2026"))
@pytest.mark.parametrize("version", VERSIONS)
@pytest.mark.parametrize("amount", (-2, 0, 2))
def test_independent_plant_and_regional_delivered_electricity_reverse_preserve_units_and_uncertainty(
    release, version, amount
):
    data = None
    targets = []
    bridge = (
        None
        if release == "2025"
        else next(
            r for r in load_uvek_release_resources("technosphere") if r["source_profiles"][0]["version"] == "2025"
        )
    )
    for row in APPROVAL["reverse_components"]:
        source = (
            row["source"]
            if bridge is None
            else next(r["target"] for r in bridge["replace"] if r["source"] == row["source"])
        )
        for kind, uncertainty in (("technosphere", 2), ("substitution", 3)):
            part = consumer(source, amount, uncertainty, kind)
            part[0]["exchanges"][1]["caller_metadata"] = {"keep": [1, 2]}
            if data is None:
                data = part
            else:
                data[0]["exchanges"].append(part[0]["exchanges"][1])
            targets.append(row["target"])
    saved = deepcopy(data)
    context = target_context(release)
    inventory = BrightwayInventory.from_data(
        data, context=InventoryContext(contexts("uvek", context.technosphere.version).format, context)
    )
    result = inventory.migrate_background(
        contexts("ecoinvent", version).background,
        policy=MigrationPolicy(
            on_heuristic_mapping=PolicyAction.WARN,
            on_information_loss=PolicyAction.WARN,
            on_inferred_reverse=PolicyAction.WARN,
        ),
    )
    assert len(result.data) == 1 and len(result.data[0]["exchanges"]) == 17
    for before, after, target in zip(saved[0]["exchanges"][1:], result.data[0]["exchanges"][1:], targets, strict=True):
        assert {f: after[f] for f in FIELDS} == target
        for field in (
            "amount",
            "type",
            "uncertainty type",
            "formula",
            "loc",
            "scale",
            "negative",
            "minimum",
            "maximum",
            "caller_metadata",
        ):
            assert after.get(field) == before.get(field)
    assert inventory.data == data == saved


def test_component_reverse_scope_cannot_change_under_legacy_projection():
    from brightpath.background.uvek_export import load_uvek_export_resource
    from scripts.review_uvek_sewage_digestion import COMPONENT_METHOD, legacy_reverse_projection

    resource = deepcopy(load_uvek_export_resource())
    legacy_reverse_projection(resource)
    rule = next(r for r in resource["replace"] if r.get("mapping_method") == COMPONENT_METHOD)
    rule["conversion_factor"] = 1.1
    with pytest.raises(ValueError, match="repeat the independent review"):
        legacy_reverse_projection(resource)
