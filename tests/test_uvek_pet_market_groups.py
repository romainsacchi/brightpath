"""PET groups retain original regional weights and complete reviewed treatment/freight graphs."""

from collections import Counter, defaultdict
from copy import deepcopy

import pytest
from test_uvek_reference_signs import FIELDS, consumer, contexts
from test_uvek_refrigerant_disposal import target_context

from brightpath import BackgroundContext, BiosphereProfile, BrightwayInventory, TechnosphereProfile
from brightpath.background.catalogs import PackageCatalogProvider
from brightpath.background.reference_signs import select_profile_sign_rules
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource
from scripts import review_uvek_pet_market_groups as review

POLICY = MigrationPolicy(on_information_loss=PolicyAction.WARN, on_heuristic_mapping=PolicyAction.WARN)


def profiles(version):
    return {
        source: p
        for source, row in review.SOURCES.items()
        for p in row["profiles"]
        if p["label"] == f"ecoinvent-{version}-cutoff"
    }


def batch(version, amount):
    data = None
    for source in profiles(version):
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


def flow_signature(e):
    return e["name"], tuple(e["categories"]), e["unit"], e["amount"]


@pytest.mark.parametrize("version", sorted(review.VERSIONS))
@pytest.mark.parametrize("release", ("2025", "2026"))
@pytest.mark.parametrize("amount", (-2, 0, 2))
def test_original_group_weights_complete_children_and_caller_signed_state_are_preserved(version, release, amount):
    data = batch(version, amount)
    original = deepcopy(data)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", version))
    result = inventory.migrate_background(target_context(release), policy=POLICY)
    by_code = {d["code"]: d for d in result.data[1:]}
    by_recipe = {d["brightpath_conversion"]["recipe_id"]: d for d in result.data[1:]}
    assert len(by_code) == len(by_recipe) == len(result.data) - 1
    assert result.data[0]["exchanges"][0] == original[0]["exchanges"][0]
    assert result.data[0]["exchanges"][-1] == original[0]["exchanges"][-1]
    for before, after in zip(original[0]["exchanges"][1:-1], result.data[0]["exchanges"][1:-1], strict=True):
        source = tuple(before[f] for f in FIELDS)
        assert after["amount"] == -before["amount"] and after["type"] == before["type"]
        assert after["formula"] == "(dose) * (-1.0)" and after["scale"] == before["scale"]
        assert after["caller_metadata"] == before["caller_metadata"]
        if before["uncertainty type"] == 2:
            assert after["loc"] == before["loc"] and after["negative"] is not before["negative"]
        else:
            assert after["loc"] == -before["loc"]
            assert (after["minimum"], after["maximum"]) == (-before["maximum"], -before["minimum"])
        parent = by_code[after["input"][1]]
        (production,) = [e for e in parent["exchanges"] if e["type"] == "production"]
        assert production["amount"] == 1 and parent["unit"] == "kilogram"
        assert after["amount"] / production["amount"] == before["amount"] / -1
        actual = [e for e in parent["exchanges"] if e["type"] != "production"]
        assert len(actual) == (35 if source[2] == "Europe without Switzerland" else 2)
        for expected, exchange in zip(profiles(version)[source]["inputs"], actual, strict=True):
            assert exchange["type"] == "technosphere" and exchange["unit"] == "kilogram"
            assert exchange["amount"] == expected["amount"] > 0
            supplier = by_recipe[expected["recipe"]]
            if "input" in exchange:
                assert by_code[exchange["input"][1]] is supplier
            else:
                assert (
                    sum(tuple(d[f] for f in FIELDS) == tuple(exchange[f] for f in FIELDS) for d in result.data[1:]) == 1
                )
            assert tuple(exchange[f] for f in FIELDS) == tuple(supplier[f] for f in FIELDS)
    groups = [d for d in result.data[1:] if d["brightpath_conversion"]["recipe_id"].startswith("pet-market-group-")]
    assert len(groups) == 2 and {d["location"] for d in groups} == {"RER", "Europe without Switzerland"}
    assert all(e["type"] in {"production", "technosphere"} for d in groups for e in d["exchanges"])
    assert any(e["unit"] == "ton kilometer" for d in result.data[1:] for e in d["exchanges"])
    assert any(e["type"] == "biosphere" for d in result.data[1:] for e in d["exchanges"])
    assert any(d["brightpath_conversion"]["recipe_id"].startswith("swiss-pet-fluwa-market-") for d in result.data[1:])
    assert not any("Fleece production" in e["name"] for d in result.data for e in d["exchanges"])
    assert result.last_migration_report.losses and inventory.data == data == original


@pytest.mark.parametrize(
    "version,swiss_share,german_share",
    (("3.6", 0.0168876858357569, 0.170244083714134), ("3.12", 0.3552138086192435, 0.17024408371413374)),
)
def test_nested_swiss_and_german_shares_remain_original_without_normalization(version, swiss_share, german_share):
    result = BrightwayInventory.from_data(
        batch(version, -1), context=contexts("ecoinvent", version)
    ).migrate_background(target_context("2025"), policy=POLICY)
    groups = {
        d["location"]: d
        for d in result.data[1:]
        if d["brightpath_conversion"]["recipe_id"].startswith("pet-market-group-")
    }
    rer = {e["location"]: e for e in groups["RER"]["exchanges"] if e["type"] == "technosphere"}
    assert rer["CH"]["amount"] == swiss_share
    regional = [e for e in groups["Europe without Switzerland"]["exchanges"] if e["type"] == "technosphere"]
    assert len(regional) == 35 and next(e["amount"] for e in regional if e["location"] == "DE") == german_share
    assert all(e["unit"] == "kilogram" for e in rer.values())
    assert "FLUWA" in groups["RER"]["comment"]


@pytest.mark.parametrize("version", sorted(review.VERSIONS))
def test_reverse_requires_existing_independent_pet_treatment_coverage_and_preserves_owned_graph_on_refusal(version):
    canonical = BrightwayInventory.from_data(
        batch(version, -1), context=contexts("ecoinvent", version)
    ).migrate_background(target_context("2025"), policy=POLICY)
    original = deepcopy(canonical.data)
    with pytest.raises(MigrationError) as caught:
        canonical.migrate_background(
            BackgroundContext(
                TechnosphereProfile("ecoinvent", "3.12", "cutoff"), BiosphereProfile("ecoinvent", "3.10")
            ),
            policy=MigrationPolicy.permissive(),
        )
    assert any("No reviewed directional correspondence" in i.message for i in caught.value.report.issues)
    assert canonical.data == original


def test_native_physical_gaps_inherited_from_children_are_explicit_and_never_dropped():
    data = batch("3.12", -1)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.12"))
    with pytest.raises(MigrationError) as caught:
        inventory.migrate_background(target_context("native2026"), policy=POLICY)
    assert any(i.code == "background.biosphere_link_unresolved" for i in caught.value.report.issues)
    canonical = inventory.migrate_background(target_context("2025"), policy=POLICY)
    result = inventory.migrate_background(target_context("native2026"), policy=MigrationPolicy.permissive())
    available = PackageCatalogProvider().load_biosphere(BiosphereProfile("uvek", "2026")).identities
    unresolved = [
        e
        for d in result.data[1:]
        for e in d["exchanges"]
        if e["type"] == "biosphere" and (e["name"], tuple(e["categories"]), e["unit"]) not in available
    ]
    original = Counter(
        flow_signature(e) for d in canonical.data[1:] for e in d["exchanges"] if e["type"] == "biosphere"
    )
    assert unresolved and Counter(map(flow_signature, unresolved)) <= original
    assert any(i.code == "background.biosphere_link_unresolved" for i in result.last_migration_report.issues)
    assert inventory.data == data


def test_strict_loss_refusal_exact_profiles_system_model_and_foreground_ownership():
    data = batch("3.12", -1)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.12"))
    with pytest.raises(MigrationError):
        inventory.migrate_background(
            target_context("2025"), policy=MigrationPolicy(on_heuristic_mapping=PolicyAction.WARN)
        )
    assert inventory.data == data
    for source in profiles("3.12"):
        row = dict(zip(FIELDS, source, strict=True))
        data.append({**row, "exchanges": [{**row, "type": "production", "amount": -1}]})
    assert (
        BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.12"))
        .migrate_background(target_context("2025"), policy=POLICY)
        .data
        == data
    )
    for version in ("3.6.1", "3.10.2", "3.13"):
        selected = select_profile_sign_rules(
            load_uvek_technosphere_resource(), TechnosphereProfile("ecoinvent", version, "cutoff")
        )
        for row in review.SOURCES.values():
            rule = next(r for r in selected["replace"] if r["source"] == row["source"])
            assert rule["reference_flow_sign_review"]["status"] == "unresolved"
    inventory = BrightwayInventory.from_data(batch("3.12", -1), context=contexts("ecoinvent", "3.12", "consequential"))
    with pytest.raises(MigrationError, match="system models"):
        inventory.migrate_background(target_context("2025"), policy=MigrationPolicy.permissive())


def evidence():
    records = defaultdict(list)
    observations = {"ecoinvent": defaultdict(set), "uvek": defaultdict(set)}
    for row in review.APPROVAL["inventories"]:
        source = tuple(row["source"][f] for f in FIELDS)
        records[source].append({**deepcopy(row), "source": list(source)})
        observations["ecoinvent"][source].add((row["label"], row["reference_amount"]))
    return records, observations


@pytest.mark.parametrize(
    "change",
    ("inventory_sha256", "activity_comment_sha256", "product_information_sha256", "missing", "duplicate", "patch"),
)
def test_parent_or_child_full_inventory_description_reference_or_patch_drift_requires_review(change):
    data, observations = evidence()
    review.validate_inventories(data, observations, {})
    source = next(iter(review.SOURCES))
    if change in ("inventory_sha256", "activity_comment_sha256", "product_information_sha256"):
        data[source][0][change] = "0" * 64
    elif change == "missing":
        data[source].pop()
    elif change == "duplicate":
        data[source].append(deepcopy(data[source][0]))
    else:
        observations["ecoinvent"][source].add(("ecoinvent-3.10.2-cutoff", -1))
    with pytest.raises(ValueError, match="repeat the scientific review"):
        review.validate_inventories(data, observations, {})


@pytest.mark.parametrize("change", ("share", "added_freight", "local_group", "source_sign"))
def test_group_quantities_operations_nested_graph_and_sign_cannot_drift(monkeypatch, change):
    review.validate_bindings(load_uvek_technosphere_resource())
    approval = deepcopy(review.APPROVAL)
    if change == "share":
        approval["rules"][0]["profiles"][0]["inputs"][0]["amount"] *= 0.5
    elif change == "added_freight":
        approval["rules"][0]["profiles"][0]["inputs"].append({"name": "extra road freight", "amount": 1})
    elif change == "local_group":
        next(iter(approval["local_recipes"].values()))["inputs"].pop()
    else:
        approval["child_checks"][0]["conversion_factor"] = 1
    monkeypatch.setattr(review, "APPROVAL", approval)
    with pytest.raises(ValueError, match="quantities, children or approval scope changed"):
        review.validate_bindings(load_uvek_technosphere_resource())


def test_transitive_child_recipe_dependency_changes_require_new_scientific_review():
    resource = deepcopy(load_uvek_technosphere_resource())
    rid = next(iter(review.APPROVAL["recipe_dependencies"]))
    resource["recipes"][rid]["inputs"][0]["amount"] *= 2
    with pytest.raises(ValueError, match="recipe dependency changed"):
        review.validate_bindings(resource)


@pytest.mark.parametrize("version", ("3.9.1", "3.12"))
def test_excel_round_trip_exports_entire_nested_helper_graph_and_preserves_caller_uncertainty(tmp_path, version):
    source = next(s for s in profiles(version) if s[2] == "RER")
    data = consumer(dict(zip(FIELDS, source, strict=True)), -2, 3)
    data[0]["comment"] = "Original nested PET market-group export fixture"
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", version)).migrate_background(
        target_context("2025"), policy=POLICY
    )
    path = tmp_path / "pet-groups.xlsx"
    result.write_excel(path)
    restored = BrightwayInventory.from_excel(path, context=result.context)
    assert len(restored.data) == len(result.data)
    actual = restored.data[0]["exchanges"][1]
    expected = result.data[0]["exchanges"][1]
    assert tuple(actual[f] for f in FIELDS) == tuple(expected[f] for f in FIELDS)
    assert actual["amount"] == expected["amount"] == 2 and actual["uncertainty type"] == 3
    assert (actual["loc"], actual["scale"]) == (expected["loc"], expected["scale"])
    by_recipe = {d["brightpath_conversion"]["recipe_id"]: d for d in result.data[1:]}
    assert len(restored.data) == len(by_recipe) + 1
    assert not restored.validate().has_errors
