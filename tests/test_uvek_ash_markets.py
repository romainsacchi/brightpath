"""Ash markets retain treatment, freight and traction shares in both signed directions."""

from collections import defaultdict
from copy import deepcopy

import pytest
from test_uvek_bottom_ash import POLICY, REVERSE_POLICY
from test_uvek_reference_signs import FIELDS, consumer, contexts
from test_uvek_refrigerant_disposal import target_context

from brightpath import BrightwayInventory, InventoryContext, TechnosphereProfile
from brightpath.background.reference_signs import select_profile_sign_rules
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_release_resources, load_uvek_technosphere_resource
from scripts import review_uvek_ash_markets as review

VERSIONS = ("3.9", "3.9.1", "3.10", "3.10.1", "3.11", "3.12")


def batch(version, amount):
    data = None
    for source, row in review.SOURCES.items():
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


def release_identity(identity, release):
    if release == "2025":
        return identity
    bridge = next(
        r for r in load_uvek_release_resources("technosphere") if r["source_profiles"][0]["version"] == "2025"
    )
    return next(r["target"] for r in bridge["replace"] if r["source"] == identity)


@pytest.mark.parametrize("version", VERSIONS)
@pytest.mark.parametrize("release", ("2025", "2026", "native2026"))
@pytest.mark.parametrize("amount", (-10, 0, 10))
def test_all_market_profiles_preserve_waste_child_original_freight_and_complete_rail_mix(version, release, amount):
    data = batch(version, amount)
    saved = deepcopy(data)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", version))
    result = inventory.migrate_background(target_context(release), policy=POLICY)
    helpers = {d["code"]: d for d in result.data[1:]}
    by_identity = {tuple(d[f] for f in FIELDS): d for d in result.data[1:]}
    assert result.data[0]["exchanges"][0] == saved[0]["exchanges"][0]
    assert result.data[0]["exchanges"][-1]["amount"] == 0.5
    road = release_identity(review.ROAD, release)
    diesel, electric = (release_identity(t, release) for t in (review.DIESEL, review.ELECTRIC))
    for before, after in zip(saved[0]["exchanges"][1:-1], result.data[0]["exchanges"][1:-1], strict=True):
        assert after["amount"] == -before["amount"] and after["type"] == before["type"]
        assert after["formula"] == "(dose) * (-1.0)" and after["caller_metadata"] == before["caller_metadata"]
        assert after["scale"] == before["scale"]
        if before["uncertainty type"] == 2:
            assert after["loc"] == before["loc"] and after["negative"] is not before["negative"]
        else:
            assert after["loc"] == -before["loc"]
            assert (after["minimum"], after["maximum"]) == (-before["maximum"], -before["minimum"])
        parent = helpers[after["input"][1]]
        assert not any(e["type"] == "biosphere" for e in parent["exchanges"])
        tech = [e for e in parent["exchanges"] if e["type"] == "technosphere"]
        children = [e for e in tech if e["unit"] == "kilogram"]
        assert len(children) == 1 and children[0]["amount"] == 1
        child = by_identity[tuple(children[0][f] for f in FIELDS)]
        assert any(e["type"] == "biosphere" and e["amount"] != 0 for e in child["exchanges"])
        assert any(
            e["type"] == "technosphere" and e["unit"] == "unit" and e["amount"] == 1.7778e-9 for e in child["exchanges"]
        )
        trucks = [e for e in tech if all(e.get(f) == road[f] for f in FIELDS)]
        rail = [e for e in tech if e["unit"] == "ton kilometer" and e not in trucks]
        if rail:
            assert len(rail) == len(trucks) == 1
            assert trucks[0]["amount"] == pytest.approx(0.02577) and rail[0]["amount"] == pytest.approx(0.00356)
            mixture = by_identity[tuple(rail[0][f] for f in FIELDS)]
            traction = [e for e in mixture["exchanges"] if e["type"] == "technosphere"]
            assert len(traction) == 2
            assert {tuple(e[f] for f in FIELDS) for e in traction} == {
                tuple(t[f] for f in FIELDS) for t in (diesel, electric)
            }
            assert sorted(e["amount"] for e in traction) == pytest.approx([0.0412324596234946, 0.958767540376505])
        else:
            assert len(trucks) in (1, 3)
            assert sum(e["amount"] for e in trucks) == pytest.approx(0.077)
    assert inventory.data == data == saved
    assert result.migrate_background(target_context(release), policy=POLICY).data == result.data


@pytest.mark.parametrize("release", ("2025", "2026"))
def test_canonical_ash_market_graph_reverses_with_independently_reviewed_complete_freight(release):
    forward = BrightwayInventory.from_data(batch("3.12", -1), context=contexts("ecoinvent", "3.12")).migrate_background(
        target_context(release), policy=POLICY
    )
    saved = deepcopy(forward.data)
    reverse = forward.migrate_background(contexts("ecoinvent", "3.12").background, policy=REVERSE_POLICY)
    assert len(reverse.data) == len(saved)
    assert reverse.data[0]["exchanges"] == saved[0]["exchanges"]
    freight = [
        e
        for d in reverse.data[1:]
        for e in d["exchanges"]
        if e["type"] == "technosphere"
        and e["unit"] == "ton kilometer"
        and e.get("input", (None,))[0] != "brightpath-conversion"
    ]
    assert any(
        e["name"] == "transport, freight, train, electric" and e["amount"] == pytest.approx(0.958767540376505)
        for e in freight
    )
    assert any(
        e["name"] == "transport, freight, train, diesel, with particle filter"
        and e["amount"] == pytest.approx(0.0412324596234946)
        for e in freight
    )
    assert forward.data == saved


@pytest.mark.parametrize("release", ("2025", "2026", "native2026"))
@pytest.mark.parametrize("amount", (-2, 0, 2))
@pytest.mark.parametrize("version", ("3.6", "3.7", "3.8", "3.9", "3.9.1", "3.10", "3.10.1", "3.11", "3.12"))
def test_independent_complete_electric_freight_reverse_preserves_positive_product_coordinates(release, amount, version):
    source = release_identity(review.ELECTRIC, release)
    data = consumer(source, amount, 3)
    saved = deepcopy(data)
    context = target_context(release)
    inventory = BrightwayInventory.from_data(
        data, context=InventoryContext(contexts("uvek", context.technosphere.version).format, context)
    )
    result = inventory.migrate_background(contexts("ecoinvent", version).background, policy=REVERSE_POLICY)
    assert len(result.data) == 1
    exchange = result.data[0]["exchanges"][1]
    expected = (
        review.REVERSE_TARGET
        if version in ("3.11", "3.12")
        else dict(
            zip(
                FIELDS,
                ("transport, freight train, electricity", "transport, freight train", "CH", "ton kilometer"),
                strict=True,
            )
        )
    )
    assert {f: exchange[f] for f in FIELDS} == expected and exchange["amount"] == amount
    assert exchange["formula"] == "dose" and exchange["loc"] == saved[0]["exchanges"][1]["loc"]
    assert any("electric rail" in loss.message.lower() for loss in result.last_migration_report.losses)
    assert inventory.data == data == saved


def test_native_ash_graph_does_not_infer_missing_generic_water_reverse():
    forward = BrightwayInventory.from_data(batch("3.12", -1), context=contexts("ecoinvent", "3.12")).migrate_background(
        target_context("native2026"), policy=POLICY
    )
    saved = deepcopy(forward.data)
    with pytest.raises(MigrationError):
        forward.migrate_background(contexts("ecoinvent", "3.12").background, policy=REVERSE_POLICY)
    assert forward.data == saved


def test_strict_loss_policy_rolls_back_and_owned_suppliers_are_preserved():
    data = batch("3.12", -1)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.12"))
    saved = deepcopy(data)
    with pytest.raises(MigrationError):
        inventory.migrate_background(
            target_context("2025"), policy=MigrationPolicy(on_heuristic_mapping=PolicyAction.WARN)
        )
    assert inventory.data == saved
    for source in review.SOURCES:
        row = dict(zip(FIELDS, source, strict=True))
        data.append({**row, "exchanges": [{**row, "type": "production", "amount": 1}]})
    assert (
        BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.12"))
        .migrate_background(target_context("2025"), policy=POLICY)
        .data
        == data
    )


@pytest.mark.parametrize("version", ("3.8", "3.13", "3.12.1"))
def test_no_uninspected_patch_or_version_fallback(version):
    resource = select_profile_sign_rules(
        load_uvek_technosphere_resource(), TechnosphereProfile("ecoinvent", version, "cutoff")
    )
    rows = [r for r in resource["replace"] if review.identity(r["source"]) in review.SOURCES]
    assert len(rows) == 42 and all(r["reference_flow_sign_review"]["status"] == "unresolved" for r in rows)


def test_consequential_market_cannot_use_cutoff_helper():
    inventory = BrightwayInventory.from_data(batch("3.12", -1), context=contexts("ecoinvent", "3.12", "consequential"))
    saved = deepcopy(inventory.data)
    with pytest.raises(MigrationError, match="system models"):
        inventory.migrate_background(target_context("2025"), policy=MigrationPolicy.permissive())
    assert inventory.data == saved


@pytest.mark.parametrize("change", ("inventory", "metadata", "missing", "duplicate", "sign", "native_role"))
def test_evidence_drift_requires_scientific_review(change):
    inventories, observations = defaultdict(list), {"ecoinvent": defaultdict(set), "uvek": defaultdict(set)}
    native = {}
    for row in review.APPROVAL["inventories"]:
        source = review.identity(row["source"])
        inventories[source].append({**row, "source": list(source)})
        observations[row["label"].split("-")[0]][source].add((row["label"], row["reference_amount"]))
        if row["label"] == "uvek-2025-cutoff":
            native[source] = {"native_flow_type": "PRODUCT_FLOW", "native_reference_is_input": False}
    review.validate_inventories(inventories, observations, native)
    source = next(iter(review.SOURCES))
    if change in ("inventory", "metadata"):
        inventories[source][0]["inventory_sha256" if change == "inventory" else "activity_comment_sha256"] = "0" * 64
    elif change == "missing":
        inventories[source].pop()
    elif change == "duplicate":
        inventories[source].append(deepcopy(inventories[source][0]))
    elif change == "sign":
        observations["ecoinvent"][source] = {(label, 1) for label, _ in observations["ecoinvent"][source]}
    else:
        native[next(iter(native))]["native_flow_type"] = "WASTE_FLOW"
    with pytest.raises(ValueError, match="repeat the scientific review"):
        review.validate_inventories(inventories, observations, native)


def test_child_recipe_and_scoped_rail_choice_drift_cannot_inherit_approval(monkeypatch):
    resource = deepcopy(load_uvek_technosphere_resource())
    review.validate_bindings(resource)
    dependency = next(iter(review.APPROVAL["recipe_dependencies"]))
    resource["recipes"][dependency]["inputs"][0]["amount"] *= 2
    with pytest.raises(ValueError, match="recipe dependency changed"):
        review.validate_bindings(resource)
    approval = deepcopy(review.APPROVAL)
    next(iter(approval["local_recipes"].values()))["inputs"][0]["amount"] = 0.5
    monkeypatch.setitem(review.APPROVAL, "local_recipes", approval["local_recipes"])
    with pytest.raises(ValueError, match="rail-service mix changed"):
        review.validate_bindings(load_uvek_technosphere_resource())
