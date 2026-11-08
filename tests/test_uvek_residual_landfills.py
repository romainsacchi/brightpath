"""Landfill demands retain releases, stabilization and exact reference coordinates."""

from copy import deepcopy

import pytest
from test_uvek_reference_signs import FIELDS, consumer, contexts
from test_uvek_refrigerant_disposal import target_context

from brightpath import BackgroundContext, BiosphereProfile, BrightwayInventory, InventoryContext, TechnosphereProfile
from brightpath.background.reference_signs import select_profile_sign_rules
from brightpath.background.uvek_import import apply_reviewed_recipe_biosphere_bindings
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_release_resources, load_uvek_technosphere_resource
from scripts.review_uvek_residual_landfills import APPROVAL, SOURCES, validate_bindings, validate_inventories

POLICY = MigrationPolicy(on_heuristic_mapping=PolicyAction.WARN, on_information_loss=PolicyAction.WARN)
VERSIONS = ("3.7", "3.8", "3.9", "3.9.1", "3.10", "3.10.1", "3.11", "3.12")


def batch(version, amount):
    exchanges = []
    for source, row in SOURCES.items():
        if not any(p["label"] == f"ecoinvent-{version}-cutoff" for p in row["profiles"]):
            continue
        for kind, uncertainty in (("technosphere", 2), ("substitution", 3)):
            exchange = consumer(dict(zip(FIELDS, source, strict=True)), amount, uncertainty, kind)[0]["exchanges"][1]
            exchange["caller_metadata"] = {"keep": [1, 2]}
            exchanges.append(exchange)
    data = consumer(dict(zip(FIELDS, next(iter(SOURCES)), strict=True)), amount)
    data[0]["exchanges"][1:] = exchanges
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
def test_complete_landfill_profiles_preserve_signs_releases_and_caller_ownership(version, release, amount):
    data = batch(version, amount)
    saved = deepcopy(data)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", version))
    result = inventory.migrate_background(target_context(release), policy=POLICY)
    helpers = {d["code"]: d for d in result.data[1:]}
    assert len(helpers) == len(saved[0]["exchanges"][1:-1]) // 2
    assert result.data[0]["exchanges"][0] == saved[0]["exchanges"][0]
    assert result.data[0]["exchanges"][-1]["amount"] == 0.5
    label = f"ecoinvent-{version}-cutoff"
    land_count = 0
    for before, after in zip(saved[0]["exchanges"][1:-1], result.data[0]["exchanges"][1:-1], strict=True):
        source = tuple(before[f] for f in FIELDS)
        profile = next(p for p in SOURCES[source]["profiles"] if p["label"] == label)
        factor = profile["source_sign"]
        assert after["amount"] == factor * before["amount"] and after["type"] == before["type"]
        assert after["caller_metadata"] == before["caller_metadata"]
        assert after["formula"] == ("(dose) * (-1.0)" if factor == -1 else "dose")
        assert after["scale"] == before["scale"]
        if before["uncertainty type"] == 2:
            assert after["loc"] == before["loc"]
            assert after["negative"] == (not before["negative"] if factor == -1 else before["negative"])
        else:
            assert after["loc"] == factor * before["loc"]
            assert (after["minimum"], after["maximum"]) == (
                (-before["maximum"], -before["minimum"]) if factor == -1 else (before["minimum"], before["maximum"])
            )
        helper = helpers[after["input"][1]]
        assert not helper["brightpath_conversion"].get("zero_burden")
        assert any(e["type"] == "technosphere" for e in helper["exchanges"])
        indicators = {
            "Hazardous waste disposed",
            "Non-hazardous waste disposed",
            "Organic carbon, placed in landfill",
            "Waste mass, total, placed in landfill",
        }
        original_releases = sorted(
            e["amount"]
            for e in profile["inputs"]
            if e["type"] == "biosphere" and not e.get("approved_omission_if_unavailable")
        )
        actual_releases = sorted(
            e["amount"] for e in helper["exchanges"] if e["type"] == "biosphere" and e["name"] not in indicators
        )
        assert actual_releases == original_releases
        land = [e for e in helper["exchanges"] if e["name"].startswith("Transformation, from dump site")]
        if any("biosphere_targets" in e for e in profile["inputs"]):
            (exchange,) = land
            assert exchange["amount"] == 5.7143e-5
            assert exchange["unit"] == "square meter"
            assert exchange["name"] == (
                "Transformation, from dump site, residual material landfill"
                if release == "native2026"
                else "Transformation, from dump site"
            )
            land_count += 1
    losses = [
        loss for loss in result.last_migration_report.losses if loss.code == "migration.recipe_biosphere_approximation"
    ]
    land_losses = [
        loss for loss in losses if loss.details["source_exchange"]["name"] == "Transformation, from dump site"
    ]
    assert len(land_losses) == (land_count // 2 if release == "native2026" else 0)
    assert inventory.data == data == saved


def modern_landfill():
    return next(
        row["source"]
        for row in APPROVAL["rules"]
        if any(
            p["label"] == "ecoinvent-3.12-cutoff" and any("biosphere_targets" in e for e in p["inputs"])
            for p in row["profiles"]
        )
    )


@pytest.mark.parametrize("version", ("3.10", "3.10.1", "3.11", "3.12"))
@pytest.mark.parametrize("intermediate_release", ("2025", "2026"))
def test_canonical_helper_retains_scoped_land_approval_for_later_native_migration(version, intermediate_release):
    data = consumer(modern_landfill(), -2, 3)
    canonical = BrightwayInventory.from_data(data, context=contexts("ecoinvent", version)).migrate_background(
        target_context(intermediate_release), policy=POLICY
    )
    original = deepcopy(canonical.data)
    result = canonical.migrate_background(target_context("native2026"), policy=POLICY)
    land = [
        e for d in result.data[1:] for e in d["exchanges"] if e["name"].startswith("Transformation, from dump site")
    ]
    assert len(land) == 1 and land[0]["name"] == "Transformation, from dump site, residual material landfill"
    assert land[0]["amount"] == 5.7143e-5
    assert any(i.code == "migration.recipe_biosphere_approximation" for i in result.last_migration_report.issues)
    assert canonical.data == original


@pytest.mark.parametrize("change", ("code", "name", "recipe", "resource", "profile", "strict"))
def test_later_native_refinement_rejects_stale_helper_identity_and_strict_loss(change):
    canonical = BrightwayInventory.from_data(
        consumer(modern_landfill(), -1), context=contexts("ecoinvent", "3.12")
    ).migrate_background(target_context("2025"), policy=POLICY)
    data = canonical.data
    helper = data[1]
    metadata = helper["brightpath_conversion"]
    if change == "name":
        data[0]["exchanges"][1]["name"] += "-edited"
    if change in {"name", "code"}:
        helper[change] += "-edited"
    elif change == "recipe":
        metadata["recipe_id"] = "unreviewed"
    elif change == "resource":
        metadata["resource"] = "unreviewed"
    elif change == "profile":
        metadata["biosphere_binding_profile"] = {"family": "ecoinvent", "version": "3.12"}
    inventory = BrightwayInventory.from_data(data, context=canonical.context)
    policy = POLICY if change != "strict" else MigrationPolicy(on_heuristic_mapping=PolicyAction.WARN)
    with pytest.raises(MigrationError) as caught:
        inventory.migrate_background(target_context("native2026"), policy=policy)
    expected = "migration.recipe_biosphere_review_stale" if change != "strict" else "migration.recipe_input_omission"
    assert any(i.code == expected and i.severity.value == "error" for i in caught.value.report.issues)
    assert inventory.data == data


def test_later_refinement_preserves_exchange_sign_uncertainty_formula_and_metadata():
    canonical = BrightwayInventory.from_data(
        consumer(modern_landfill(), -1), context=contexts("ecoinvent", "3.12")
    ).migrate_background(target_context("2025"), policy=POLICY)
    data = canonical.data
    exchange = next(e for e in data[1]["exchanges"] if e["name"] == "Transformation, from dump site")
    exchange.update(
        amount=-5.7143e-5,
        **{
            "uncertainty type": 3,
            "loc": -5.7143e-5,
            "scale": 0.01,
            "formula": "land_area",
            "custom_metadata": {"keep": [1]},
        },
    )
    saved = deepcopy(exchange)
    result = BrightwayInventory.from_data(data, context=canonical.context).migrate_background(
        target_context("native2026"), policy=POLICY
    )
    mapped = next(
        e
        for e in result.data[1]["exchanges"]
        if e["name"] == "Transformation, from dump site, residual material landfill"
    )
    assert {k: v for k, v in mapped.items() if k not in {"name", "categories"}} == {
        k: v for k, v in saved.items() if k not in {"name", "categories", "input", "database"}
    }
    omissions = [loss for loss in result.last_migration_report.losses if loss.code == "migration.recipe_input_omission"]
    assert omissions and {loss.details["source_exchange"]["name"] for loss in omissions} <= {
        "Hazardous waste disposed",
        "Organic carbon, placed in landfill",
        "Waste mass, total, placed in landfill",
    }
    assert canonical.data != data


def test_unrelated_foreground_dump_site_flow_never_inherits_landfill_refinement():
    data = consumer(modern_landfill(), -1)
    data[0]["exchanges"][1] = {
        "name": "Transformation, from dump site",
        "categories": ["natural resource", "land"],
        "unit": "square meter",
        "type": "biosphere",
        "amount": -0.25,
    }
    inventory = BrightwayInventory.from_data(data, context=contexts("uvek", "2025"))
    with pytest.raises(MigrationError):
        inventory.migrate_background(target_context("native2026"), policy=POLICY)
    assert inventory.data == data


@pytest.mark.parametrize("metadata", (None, 1, "external provenance", [], {"origin": "external"}))
def test_unrelated_caller_metadata_does_not_activate_a_helper_binding(metadata):
    owners = [{"brightpath_conversion": metadata}]
    data = [{"exchanges": [{"type": "biosphere", "name": "Transformation, from dump site", "amount": 1}]}]
    original = deepcopy(data)
    report = apply_reviewed_recipe_biosphere_bindings(
        data, owners, {"family": "uvek", "version": "2026"}, set(), POLICY
    )
    assert not report.issues and not report.changes and not report.losses
    assert data == original


def test_two_active_positive_reference_blast_furnace_profiles_keep_stabilization_and_releases():
    version = "3.9"
    source = next(s for s in SOURCES if s[1] == "blast furnace slag" and s[2] == "US")
    selected = select_profile_sign_rules(
        load_uvek_technosphere_resource(), TechnosphereProfile("ecoinvent", version, "cutoff")
    )
    rule = next(r for r in selected["replace"] if tuple(r["source"][f] for f in FIELDS) == source)
    assert rule["conversion_factor"] == 1
    result = BrightwayInventory.from_data(
        consumer(rule["source"], -2), context=contexts("ecoinvent", version)
    ).migrate_background(target_context("2025"), policy=POLICY)
    assert result.data[0]["exchanges"][1]["amount"] == -2
    helper = result.data[1]
    amounts = {e["name"]: e["amount"] for e in helper["exchanges"] if e["type"] == "technosphere"}
    assert amounts["Cement, unspecified, at plant"] == 0.4
    assert amounts["Process-specific burdens, residual material landfill"] == 1
    assert amounts["Residual material landfill facility"] == pytest.approx(2.08333333333333e-9)
    assert amounts["Disposal, cement, hydrated, 0% water, to residual material landfill"] == 1
    bio = [e for e in helper["exchanges"] if e["type"] == "biosphere"]
    assert len(bio) == 54 and all(e["amount"] > 0 for e in bio)


@pytest.mark.parametrize("release", ("2025", "2026", "native2026"))
def test_strict_proxy_refusal_and_consequential_scope_are_transactional(release):
    data = batch("3.12", -1)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.12"))
    with pytest.raises(MigrationError):
        inventory.migrate_background(
            target_context(release), policy=MigrationPolicy(on_heuristic_mapping=PolicyAction.WARN)
        )
    assert inventory.data == data
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.12", "consequential"))
    with pytest.raises(MigrationError, match="system models"):
        inventory.migrate_background(target_context(release), policy=MigrationPolicy.permissive())
    assert inventory.data == data


@pytest.mark.parametrize("mutation", ("inventory", "description", "missing", "duplicate", "native_role", "reference"))
def test_source_or_native_evidence_drift_requires_review(mutation):
    records, observations, native = {}, {"ecoinvent": {}, "uvek": {}}, {}
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
    elif mutation == "reference":
        observations["ecoinvent"][source].add(("ecoinvent-3.13-cutoff", -1))
    else:
        native[next(iter(native))]["native_reference_is_input"] = None
    with pytest.raises(ValueError, match="repeat the scientific review"):
        validate_inventories(records, observations, native)


def test_used_component_bindings_are_frozen():
    resource = deepcopy(load_uvek_technosphere_resource())
    validate_bindings(resource)
    source = tuple(APPROVAL["rules"][0]["profiles"][0]["bindings"][0]["source"])
    rule = next(r for r in resource["replace"] if tuple(r["source"][f] for f in FIELDS) == source)
    rule["target"]["name"] = "Different supplier"
    with pytest.raises(ValueError, match="binding changed"):
        validate_bindings(resource)


def test_modern_construction_uses_complete_materials_and_keeps_extrusion_and_qualified_heat_separate():
    source = next(s for s in SOURCES if s[1] == "basic oxygen furnace secondary metallurgy slag" and s[2] == "GLO")
    data = consumer(dict(zip(FIELDS, source, strict=True)), -1)
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.12")).migrate_background(
        target_context("2025"), policy=POLICY
    )
    exchanges = [e for e in result.data[1]["exchanges"] if e["type"] == "technosphere"]

    def amounts(name):
        return [e["amount"] for e in exchanges if e["name"] == name]

    assert sorted(amounts("Bitumen, at refinery")) == pytest.approx(
        sorted(
            [
                0.00019512751885108115,
                1.2829473984924292e-6,
                0.0008586347348773363,
                9.71937411574715e-6,
                6.80821809873394e-6,
                0.00015188736910178033,
                3.3559007295353476e-6,
                5.88839368272935e-5,
            ]
        )
    )
    assert sum(amounts("Concrete, normal, at plant")) == pytest.approx(4.6937e-7)
    assert sum(amounts("Polyvinylchloride, at regional storage")) == pytest.approx(8.1981e-7)
    assert amounts("Heat, heavy fuel oil, at industrial furnace 1MW") == [0.00092016]
    assert any(e["amount"] == 8.1818e-5 and "Extrusion" in e["name"] for e in exchanges)
    assert not any(
        e["name"] in {"Pitch despergents, in paper production, at plant", "Poor concrete", "Emulsion polymerisation"}
        for e in exchanges
    )
    warnings = [
        i.message for i in result.last_migration_report.issues if i.code == "migration.technosphere_information_loss"
    ]
    assert any("full PVC" in message and "heat fuel" in message for message in warnings)


@pytest.mark.parametrize("version", ("3.6", *VERSIONS))
@pytest.mark.parametrize("release", ("2025", "2026", "native2026"))
@pytest.mark.parametrize("amount", (-2, 0, 2))
def test_independent_reverse_landfill_components_retain_their_own_reference_coordinates(version, release, amount):
    names = {
        "Process-specific burdens, residual material landfill": 1,
        "Residual material landfill facility": 1,
        "Cement, unspecified, at plant": 1,
        "Disposal, cement, hydrated, 0% water, to residual material landfill": -1,
    }
    suppliers = [r["source"] for r in APPROVAL["native_roles"] if r["source"]["name"] in names]
    assert len(suppliers) == 4
    if release != "2025":
        bridge = next(
            r for r in load_uvek_release_resources("technosphere") if r["source_profiles"][0]["version"] == "2025"
        )
        suppliers = [next(r["target"] for r in bridge["replace"] if r["source"] == s) for s in suppliers]
    data = consumer(suppliers[0], amount, 3)
    data[0]["exchanges"][1:] = [consumer(s, amount, 3)[0]["exchanges"][1] for s in suppliers]
    inventory = BrightwayInventory.from_data(
        data, context=InventoryContext(contexts("uvek", "2025").format, target_context(release))
    )
    target = BackgroundContext(
        TechnosphereProfile("ecoinvent", version, "cutoff"), BiosphereProfile("ecoinvent", version)
    )
    result = inventory.migrate_background(
        target,
        policy=MigrationPolicy(
            on_heuristic_mapping=PolicyAction.WARN,
            on_information_loss=PolicyAction.WARN,
            on_inferred_reverse=PolicyAction.WARN,
        ),
    )
    assert len(result.data) == 1
    for before, after in zip(data[0]["exchanges"][1:], result.data[0]["exchanges"][1:], strict=True):
        factor = -1 if "cement, hydrated" in before["name"] else 1
        assert after["amount"] == factor * amount and after["loc"] == factor * amount
        assert after["formula"] == ("(dose) * (-1.0)" if factor == -1 else "dose")
        assert after["scale"] == before["scale"]
    assert inventory.data == data


@pytest.mark.parametrize("version", VERSIONS)
def test_canonical_full_helper_graph_has_independent_reverse_supplier_links(version):
    forward = BrightwayInventory.from_data(
        batch(version, -2), context=contexts("ecoinvent", version)
    ).migrate_background(target_context("2025"), policy=POLICY)
    original = deepcopy(forward.data)
    target = BackgroundContext(
        TechnosphereProfile("ecoinvent", "3.12", "cutoff"), BiosphereProfile("ecoinvent", "3.10")
    )
    result = forward.migrate_background(
        target,
        policy=MigrationPolicy(
            on_heuristic_mapping=PolicyAction.WARN,
            on_information_loss=PolicyAction.WARN,
            on_inferred_reverse=PolicyAction.WARN,
        ),
    )
    assert len(result.data) == len(original)
    assert result.data[0] == original[0]
    for before, after in zip(original[1:], result.data[1:], strict=True):
        assert before["code"] == after["code"] and before["name"] == after["name"]
        assert [e for e in before["exchanges"] if e["type"] == "biosphere"] == [
            e for e in after["exchanges"] if e["type"] == "biosphere"
        ]
    assert forward.data == original


def test_native_conversion_can_combine_landfill_and_already_native_refrigerant_helpers():
    from scripts.review_uvek_venting import VENTING_SOURCES

    data = consumer(modern_landfill(), -1)
    for source, row in VENTING_SOURCES.items():
        if any(p["label"] == "ecoinvent-3.12-cutoff" for p in row["profiles"]):
            data[0]["exchanges"].append(consumer(dict(zip(FIELDS, source, strict=True)), -1)[0]["exchanges"][1])
    original = deepcopy(data)
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.12")).migrate_background(
        target_context("native2026"), policy=POLICY
    )
    assert len(result.data) > 2
    assert any(
        e["name"] == "Transformation, from dump site, residual material landfill"
        for d in result.data[1:]
        for e in d["exchanges"]
    )
    assert any(i.code == "migration.recipe_biosphere_approximation" for i in result.last_migration_report.issues)
    assert data == original


@pytest.mark.parametrize("version", ("3.6", *VERSIONS))
@pytest.mark.parametrize("release", ("2025", "2026", "native2026"))
@pytest.mark.parametrize("amount", (-2, 0, 2))
def test_seven_independently_reviewed_reverse_materials_keep_signed_kg_or_volume(version, release, amount):
    suppliers = [row["source"] for row in APPROVAL["reverse_components"]]
    if release != "2025":
        bridge = next(
            r for r in load_uvek_release_resources("technosphere") if r["source_profiles"][0]["version"] == "2025"
        )
        suppliers = [next(r["target"] for r in bridge["replace"] if r["source"] == s) for s in suppliers]
    data = consumer(suppliers[0], amount)
    data[0]["exchanges"][1:] = [
        consumer(s, amount, uncertainty, kind)[0]["exchanges"][1]
        for s in suppliers
        for uncertainty, kind in ((2, "technosphere"), (3, "substitution"))
    ]
    original = deepcopy(data)
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
    for before, after in zip(original[0]["exchanges"][1:], result.data[0]["exchanges"][1:], strict=True):
        assert after["amount"] == amount and after["unit"] == before["unit"]
        for key in ("formula", "scale", "loc", "negative", "minimum", "maximum", "type", "uncertainty type"):
            assert after.get(key) == before.get(key)
    warnings = [i for i in result.last_migration_report.issues if i.code == "migration.documented_proxy"]
    assert len(warnings) == 14
    assert inventory.data == data == original
