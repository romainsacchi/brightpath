"""Complete uncontrolled-disposal inventories retain their original reference coordinates."""

from collections import Counter
from copy import deepcopy

import pytest
from test_uvek_reference_signs import FIELDS, consumer, contexts
from test_uvek_refrigerant_disposal import target_context

from brightpath import (
    BackgroundContext,
    BiosphereProfile,
    BrightwayInventory,
    FormatProfile,
    InventoryContext,
    TechnosphereProfile,
)
from brightpath.background import PackageCatalogProvider
from brightpath.background.reference_signs import select_profile_sign_rules
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_release_resources, load_uvek_technosphere_resource
from scripts import review_uvek_uncontrolled_disposal as review

POLICY = MigrationPolicy(on_information_loss=PolicyAction.WARN, on_heuristic_mapping=PolicyAction.WARN)
VERSIONS = tuple(sorted(review.VERSIONS))


def profiles(version):
    return {
        source: p
        for source, row in review.SOURCES.items()
        for p in row["profiles"]
        if p["label"] == f"ecoinvent-{version}-cutoff"
    }


def batch(version, amount, sources=None):
    data = None
    for source in profiles(version):
        if sources is not None and source not in sources:
            continue
        for kind, distribution in (("technosphere", 2), ("substitution", 3)):
            part = consumer(dict(zip(FIELDS, source, strict=True)), amount, distribution, kind)
            part[0]["exchanges"][1]["caller_metadata"] = {"retain": [1, 2]}
            if data is None:
                data = part
            else:
                data[0]["exchanges"].append(part[0]["exchanges"][1])
    assert data
    data[0]["exchanges"].append(
        {
            "type": "biosphere",
            "name": "Carbon dioxide, fossil",
            "categories": ["air"],
            "unit": "kilogram",
            "amount": 0.5,
        }
    )
    return data


def signature(e):
    if e["type"] == "biosphere":
        return e["type"], e["name"], tuple(e["categories"]), e["unit"], e["amount"]
    return e["type"], *(e[f] for f in FIELDS), e["amount"]


def expected_inputs(profile, release):
    result = deepcopy(profile["inputs"])
    if release == "2026":
        bridge = next(
            r
            for r in load_uvek_release_resources("technosphere")
            if r["target_profile"] == {"family": "uvek", "version": "2026", "system_model": "cutoff"}
        )
        rules = {tuple(r["source"][f] for f in FIELDS): r for r in bridge["replace"]}
        available = PackageCatalogProvider().load_technosphere(TechnosphereProfile("uvek", "2026", "cutoff")).identities
        for e in result:
            if e["type"] != "technosphere":
                continue
            key = tuple(e[f] for f in FIELDS)
            if key in rules:
                assert rules[key].get("conversion_factor", 1) == 1
                e.update(rules[key]["target"])
            else:
                assert key in available
    return Counter(signature(e) for e in result)


@pytest.mark.parametrize("version", VERSIONS)
@pytest.mark.parametrize("release", ("2025", "2026"))
@pytest.mark.parametrize("amount", (-2, 0, 2))
def test_full_source_vectors_and_actual_reference_signs_preserve_caller_fields(version, release, amount):
    data = batch(version, amount)
    original = deepcopy(data)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", version))
    result = inventory.migrate_background(target_context(release), policy=POLICY)
    helpers = {d["code"]: d for d in result.data[1:]}
    assert len(helpers) == len(profiles(version))
    assert result.data[0]["exchanges"][0] == original[0]["exchanges"][0]
    assert result.data[0]["exchanges"][-1] == original[0]["exchanges"][-1]
    for before, after in zip(original[0]["exchanges"][1:-1], result.data[0]["exchanges"][1:-1], strict=True):
        profile = profiles(version)[tuple(before[f] for f in FIELDS)]
        factor = profile["source_sign"]
        assert after["amount"] == before["amount"] * factor and after["type"] == before["type"]
        assert after["formula"] == ("(dose) * (-1.0)" if factor == -1 else "dose")
        assert after["scale"] == before["scale"] and after["caller_metadata"] == before["caller_metadata"]
        if before["uncertainty type"] == 2:
            assert after["loc"] == before["loc"] and after["negative"] == (
                before["negative"] if factor == 1 else not before["negative"]
            )
        else:
            assert after["loc"] == before["loc"] * factor
            assert (after["minimum"], after["maximum"]) == (
                (before["minimum"], before["maximum"]) if factor == 1 else (-before["maximum"], -before["minimum"])
            )
        helper = helpers[after["input"][1]]
        (production,) = [e for e in helper["exchanges"] if e["type"] == "production"]
        assert production["amount"] == 1 and helper["unit"] == "kilogram"
        assert after["amount"] / production["amount"] == before["amount"] / profile["source_sign"]
        actual = [e for e in helper["exchanges"] if e["type"] != "production"]
        assert Counter(signature(e) for e in actual) == expected_inputs(profile, release)
        assert helper["brightpath_conversion"]["recipe_id"].startswith("uncontrolled-disposal-")
        assert not helper["brightpath_conversion"].get("zero_burden")
    assert result.last_migration_report.losses and inventory.data == data == original


@pytest.mark.parametrize("version", VERSIONS)
@pytest.mark.parametrize("release", ("2025", "2026"))
def test_independent_reverse_operating_services_preserve_the_complete_foreground_graph(version, release):
    forward = BrightwayInventory.from_data(
        batch(version, -1), context=contexts("ecoinvent", version)
    ).migrate_background(target_context(release), policy=POLICY)
    original = deepcopy(forward.data)
    result = forward.migrate_background(
        BackgroundContext(TechnosphereProfile("ecoinvent", "3.12", "cutoff"), BiosphereProfile("ecoinvent", "3.10")),
        policy=MigrationPolicy(
            on_information_loss=PolicyAction.WARN,
            on_heuristic_mapping=PolicyAction.WARN,
            on_inferred_reverse=PolicyAction.WARN,
        ),
    )
    assert result.data[0] == original[0] and len(result.data) == len(original)
    by_source = {tuple(b["source"][f] for f in FIELDS): b["target"] for b in review.APPROVAL["reverse_bindings"]}
    for before, after in zip(original[1:], result.data[1:], strict=True):
        assert [e for e in after["exchanges"] if e["type"] == "biosphere"] == [
            e for e in before["exchanges"] if e["type"] == "biosphere"
        ]
        for old, new in zip(
            [e for e in before["exchanges"] if e["type"] == "technosphere"],
            [e for e in after["exchanges"] if e["type"] == "technosphere"],
            strict=True,
        ):
            # Both release bridges retain the whole MJ or m3 operating function.
            target = next(t for s, t in by_source.items() if s[3] == old["unit"])
            assert tuple(new[f] for f in FIELDS) == tuple(target[f] for f in FIELDS)
            assert new["amount"] == old["amount"]
    assert forward.data == original


def native_gap(p):
    missing = {
        (r["name"], tuple(r["categories"]), r["unit"]) for r in review.APPROVAL["native_unmapped_physical_definitions"]
    }
    return any(
        e["type"] == "biosphere" and (e["name"], tuple(e["categories"]), e["unit"]) in missing for e in p["inputs"]
    )


NATIVE_COMPLETE = [(source, p) for source, row in review.SOURCES.items() for p in row["profiles"] if not native_gap(p)]
NATIVE_BLOCKED = next((source, p) for source, row in review.SOURCES.items() for p in row["profiles"] if native_gap(p))


@pytest.mark.parametrize("source,p", NATIVE_COMPLETE)
def test_complete_native_profiles_omit_only_explicitly_approved_accounting_indicators(source, p):
    version = p["label"].split("-")[1]
    data = batch(version, -1, {source})
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", version)).migrate_background(
        target_context("native2026"), policy=POLICY
    )
    catalog = PackageCatalogProvider().load_biosphere(BiosphereProfile("uvek", "2026")).identities
    assert all(
        (e["name"], tuple(e["categories"]), e["unit"]) in catalog
        for d in result.data
        for e in d["exchanges"]
        if e["type"] == "biosphere"
    )
    omissions = [i for i in result.last_migration_report.issues if i.code == "migration.recipe_input_omission"]
    assert omissions and all(i.details["source_exchange"]["categories"][0] == "inventory indicator" for i in omissions)


def test_native_physical_gaps_block_default_coverage_and_permissive_mode_keeps_unlinked_flows_explicit():
    source, p = NATIVE_BLOCKED
    version = p["label"].split("-")[1]
    data = batch(version, -1, {source})
    original = deepcopy(data)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", version))
    with pytest.raises(MigrationError) as caught:
        inventory.migrate_background(target_context("native2026"), policy=POLICY)
    assert any(i.code == "background.biosphere_link_unresolved" for i in caught.value.report.issues)
    permissive = inventory.migrate_background(target_context("native2026"), policy=MigrationPolicy.permissive())
    available = PackageCatalogProvider().load_biosphere(BiosphereProfile("uvek", "2026")).identities
    unresolved = [
        e
        for d in permissive.data[1:]
        for e in d["exchanges"]
        if e["type"] == "biosphere" and (e["name"], tuple(e["categories"]), e["unit"]) not in available
    ]
    expected = Counter(signature(e) for e in p["inputs"] if e["type"] == "biosphere")
    assert unresolved and Counter(signature(e) for e in unresolved) <= expected
    assert any(i.code == "background.biosphere_link_unresolved" for i in permissive.last_migration_report.issues)
    assert inventory.data == data == original


def test_original_39_positive_references_are_preserved_without_inheriting_patch_approval():
    positive = {s: p for s, p in profiles("3.9").items() if p["source_sign"] == 1}
    assert len(positive) == 11 and not (positive.keys() & profiles("3.9.1").keys())
    selected = select_profile_sign_rules(
        load_uvek_technosphere_resource(), TechnosphereProfile("ecoinvent", "3.12.1", "cutoff")
    )
    by_source = {tuple(r["source"][f] for f in FIELDS): r for r in selected["replace"]}
    assert all(by_source[s]["reference_flow_sign_review"]["status"] == "unresolved" for s in review.SOURCES)


def test_older_separate_foreground_emissions_use_the_fixed_canonical_bridge_alongside_new_helpers():
    source = next(iter(profiles("3.6")))
    data = batch("3.6", -1, {source})
    data[0]["exchanges"].append(
        {
            "name": "Nickel",
            "categories": ["air", "non-urban air or from high stacks"],
            "unit": "kilogram",
            "type": "biosphere",
            "amount": -0.25,
            "formula": "dose",
        }
    )
    context = InventoryContext(
        FormatProfile("brightway_excel"),
        BackgroundContext(TechnosphereProfile("ecoinvent", "3.6", "cutoff"), BiosphereProfile("ecoinvent", "3.6")),
    )
    inventory = BrightwayInventory.from_data(data, context=context)
    result = inventory.migrate_background(target_context("2025"), policy=POLICY)
    after = result.data[0]["exchanges"][-1]
    assert (after["name"], after["categories"], after["amount"], after["formula"]) == (
        "Nickel II",
        ["air", "non-urban air or from high stacks"],
        -0.25,
        "dose",
    )
    assert after["flow_id"] == "e8fc62ba-678e-4706-97d2-b79d83e227d5" and inventory.data == data


@pytest.mark.parametrize(
    "mutation", ("inventory", "description", "missing", "duplicate", "patch", "sign", "native_role")
)
def test_full_inventory_or_reference_drift_requires_new_review(mutation):
    records = {}
    observations = {"ecoinvent": {}, "uvek": {}}
    for r in review.APPROVAL["inventories"]:
        source = tuple(r["source"][f] for f in FIELDS)
        records.setdefault(source, []).append({**deepcopy(r), "source": list(source)})
        observations[r["label"].split("-")[0]].setdefault(source, set()).add((r["label"], r["reference_amount"]))
    native = {
        tuple(r["source"][f] for f in FIELDS): deepcopy(r)
        for r in [*review.APPROVAL["native_roles"], *review.APPROVAL.get("companion_native_roles", [])]
    }
    review.validate_inventories(records, observations, native)
    source = next(iter(review.SOURCES))
    if mutation == "inventory":
        records[source][0]["inventory_sha256"] = "0" * 64
    elif mutation == "description":
        records[source][0]["activity_comment_sha256"] = "0" * 64
    elif mutation == "missing":
        records[source].pop()
    elif mutation == "duplicate":
        records[source].append(deepcopy(records[source][0]))
    elif mutation == "patch":
        observations["ecoinvent"][source].add(("ecoinvent-3.12.1-cutoff", -1))
    elif mutation == "sign":
        label, amount = next(iter(observations["ecoinvent"][source]))
        observations["ecoinvent"][source].remove((label, amount))
        observations["ecoinvent"][source].add((label, -amount))
    else:
        native[next(iter(native))]["native_reference_is_input"] = True
    with pytest.raises(ValueError):
        review.validate_inventories(records, observations, native)


def test_coefficients_contexts_and_optional_indicator_scopes_are_bound_to_scientific_review(monkeypatch):
    resource = load_uvek_technosphere_resource()
    review.validate_bindings(resource)
    e = review.APPROVAL["rules"][0]["profiles"][0]["inputs"][0]
    monkeypatch.setitem(e, "amount", e["amount"] + 1)
    with pytest.raises(ValueError, match="approval changed"):
        review.validate_bindings(resource)


@pytest.mark.parametrize("version", VERSIONS)
@pytest.mark.parametrize("release", ("2025", "2026"))
def test_reverse_operating_services_reach_every_inspected_cutoff_release(version, release):
    inputs = []
    for binding in review.APPROVAL["reverse_bindings"]:
        source = dict(binding["source"])
        if release == "2026":
            # The same explicit release correspondence used by the complete helpers.
            bridge = next(
                r
                for r in load_uvek_release_resources("technosphere")
                if r["target_profile"] == {"family": "uvek", "version": "2026", "system_model": "cutoff"}
            )
            matched = next((r for r in bridge["replace"] if r["source"] == source), None)
            if matched:
                source = matched["target"]
        inputs.append({**source, "type": "technosphere", "amount": 2, "formula": "dose"})
    output = {
        "name": "Independent reverse operating services",
        "reference product": "service",
        "unit": "unit",
        "location": "GLO",
    }
    data = [{**output, "exchanges": [{**output, "type": "production", "amount": 1}, *inputs]}]
    inventory = BrightwayInventory.from_data(
        data, context=InventoryContext(FormatProfile("brightway_excel"), target_context(release))
    )
    target = BackgroundContext(
        TechnosphereProfile("ecoinvent", version, "cutoff"), BiosphereProfile("ecoinvent", "3.10")
    )
    result = inventory.migrate_background(
        target,
        policy=MigrationPolicy(
            on_heuristic_mapping=PolicyAction.WARN,
            on_information_loss=PolicyAction.WARN,
            on_inferred_reverse=PolicyAction.WARN,
        ),
    )
    for after, binding in zip(result.data[0]["exchanges"][1:], review.APPROVAL["reverse_bindings"], strict=True):
        assert tuple(after[f] for f in FIELDS) == tuple(binding["target"][f] for f in FIELDS)
        assert after["amount"] == 2 and after["formula"] == "dose"
    assert inventory.data == data


def test_foreground_ownership_idempotence_and_explicit_loss_acceptance():
    data = batch("3.12", -1)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.12"))
    with pytest.raises(MigrationError):
        inventory.migrate_background(
            target_context("2025"), policy=MigrationPolicy(on_heuristic_mapping=PolicyAction.WARN)
        )
    first = inventory.migrate_background(target_context("2025"), policy=POLICY)
    second = inventory.migrate_background(target_context("2025"), policy=POLICY)
    assert first.data == second.data and inventory.data == data
    for source, p in profiles("3.12").items():
        fields = dict(zip(FIELDS, source, strict=True))
        data.append({**fields, "exchanges": [{**fields, "type": "production", "amount": p["source_sign"]}]})
    owned = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.12")).migrate_background(
        target_context("2025"), policy=POLICY
    )
    assert owned.data == data


@pytest.mark.parametrize("positive_reference", (False, True))
def test_excel_export_keeps_complete_helpers_and_reference_direction(positive_reference, tmp_path):
    version = "3.9" if positive_reference else "3.12"
    source, p = next((s, p) for s, p in profiles(version).items() if (p["source_sign"] == 1) == positive_reference)
    data = consumer(dict(zip(FIELDS, source, strict=True)), -1, 3)
    data[0]["comment"] = "Complete uncontrolled-disposal export regression."
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", version)).migrate_background(
        target_context("2025"), policy=POLICY
    )
    path = tmp_path / "uncontrolled-disposal.xlsx"
    result.write_excel(path)
    restored = BrightwayInventory.from_excel(path)
    assert len(restored.data) == len(result.data) == 2
    for before, after in zip(result.data, restored.data, strict=True):
        assert len(after["exchanges"]) == len(before["exchanges"])
        assert [e["amount"] for e in after["exchanges"]] == pytest.approx([e["amount"] for e in before["exchanges"]])
    assert result.data[0]["exchanges"][1]["amount"] == (-1 if positive_reference else 1)
