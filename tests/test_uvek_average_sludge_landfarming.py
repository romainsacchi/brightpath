"""Average wet-sludge treatment preserves physical releases, delivery and caller state."""

import json
from collections import Counter, defaultdict
from copy import deepcopy

import pytest
from test_uvek_reference_signs import consumer, contexts
from test_uvek_refrigerant_disposal import target_context

from brightpath import BackgroundContext, BiosphereProfile, BrightwayInventory, TechnosphereProfile
from brightpath.background.catalogs import PackageCatalogProvider
from brightpath.background.reference_signs import select_profile_sign_rules
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from scripts import review_uvek_average_sludge_landfarming as review
from scripts.review_uvek_market_compositions import composition_recipe

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
    for row in review.SOURCES.values():
        for kind, uncertainty in (("technosphere", 2), ("substitution", 3)):
            part = consumer(row["source"], amount, uncertainty, kind)
            part[0]["exchanges"][1]["caller_metadata"] = {"keep": [1, 2]}
            if data is None:
                data = part
            else:
                data[0]["exchanges"].extend(part[0]["exchanges"][1:])
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


def signature(exchange):
    return exchange["name"], tuple(exchange["categories"]), exchange["unit"], exchange["amount"]


@pytest.mark.parametrize("version", sorted(review.VERSIONS))
@pytest.mark.parametrize("release", ("2025", "2026"))
@pytest.mark.parametrize("amount", (-2, 0, 2))
def test_all_exact_treatments_and_markets_preserve_mass_emissions_freight_and_caller_state(version, release, amount):
    data = batch(version, amount)
    before = deepcopy(data)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", version))
    result = inventory.migrate_background(target_context(release), policy=POLICY)
    helpers = {d["code"]: d for d in result.data[1:]}
    by_recipe = {d["brightpath_conversion"]["recipe_id"]: d for d in helpers.values()}
    assert len(helpers) == 9
    assert result.data[0]["exchanges"][0] == before[0]["exchanges"][0]
    assert result.data[0]["exchanges"][-1] == before[0]["exchanges"][-1]
    for old, new in zip(before[0]["exchanges"][1:-1], result.data[0]["exchanges"][1:-1], strict=True):
        source = review.identity(old)
        profile = profiles(version)[source]
        rid, _ = composition_recipe(source, profile, rationale=review.LIMITATIONS, review_path=review.REVIEW_PATH)
        assert new["amount"] == -old["amount"] and new["type"] == old["type"]
        assert new["formula"] == "(dose) * (-1.0)" and new["scale"] == old["scale"]
        assert new["caller_metadata"] == old["caller_metadata"]
        if old["uncertainty type"] == 2:
            assert new["loc"] == old["loc"] and new["negative"] is not old["negative"]
        else:
            assert new["loc"] == -old["loc"]
            assert (new["minimum"], new["maximum"]) == (-old["maximum"], -old["minimum"])
        helper = helpers[new["input"][1]]
        assert helper is by_recipe[rid] and helper["unit"] == "kilogram"
        assert helper["exchanges"][0]["type"] == "production" and helper["exchanges"][0]["amount"] == 1
        actual_bio = [e for e in helper["exchanges"] if e["type"] == "biosphere"]
        expected_bio = [e for e in profile["inputs"] if e.get("type") == "biosphere"]
        assert Counter(map(signature, actual_bio)) == Counter(map(signature, expected_bio))
        if review.SOURCES[source]["kind"] == "treatment":
            (service,) = [e for e in helper["exchanges"] if e["type"] == "technosphere"]
            assert service["name"] == "Slurry spreading, by vacuum tanker"
            assert service["unit"] == "cubic meter" and service["amount"] == 0.00097087
            assert len(actual_bio) == (45 if version in {"3.9", "3.9.1"} else 44)
            assert sum(e["amount"] for e in actual_bio if e["name"] == "Water") == pytest.approx(0.00097)
            assert any(e["name"] == "Carbon" for e in actual_bio) is (version in {"3.9", "3.9.1"})
        else:
            actual = [e for e in helper["exchanges"] if e["type"] == "technosphere"]
            assert len(actual) == len(profile["inputs"])
            for expected, exchange in zip(profile["inputs"], actual, strict=True):
                assert exchange["amount"] == expected["amount"]
                if "recipe" in expected:
                    child = by_recipe[expected["recipe"]]
                    assert review.identity(exchange) == review.identity(child)
                    if "input" in exchange:
                        assert helpers[exchange["input"][1]] is child
                else:
                    assert review.identity(exchange) == review.identity(expected)
                    assert exchange["unit"] == "ton kilometer"
                    assert exchange["name"] == "Transport, freight, lorry, fleet average"
    assert result.last_migration_report.losses
    assert inventory.data == data == before
    assert result.migrate_background(target_context(release)).data == result.data


@pytest.mark.parametrize("version", sorted(review.VERSIONS))
def test_native_physical_gaps_refuse_strict_and_remain_explicitly_unlinked_when_permissive(version):
    data = batch(version, -1)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", version))
    with pytest.raises(MigrationError) as caught:
        inventory.migrate_background(target_context("native2026"), policy=POLICY)
    assert any(i.code == "background.biosphere_link_unresolved" for i in caught.value.report.issues)
    result = inventory.migrate_background(target_context("native2026"), policy=MigrationPolicy.permissive())
    available = PackageCatalogProvider().load_biosphere(BiosphereProfile("uvek", "2026")).identities
    unresolved = [
        e
        for d in result.data[1:]
        for e in d["exchanges"]
        if e["type"] == "biosphere" and (e["name"], tuple(e["categories"]), e["unit"]) not in available
    ]
    expected = Counter(
        signature(e) for p in profiles(version).values() for e in p["inputs"] if e.get("type") == "biosphere"
    )
    assert unresolved and Counter(map(signature, unresolved)) <= expected

    # The existing native water mapping converts air releases from m3 to kg.
    # Compare physical quantities in that common water basis, including retained
    # unlinked groundwater; every other physical unit and amount stays exact.
    def physical_quantity(exchange):
        if exchange["name"] == "Water" and exchange["unit"] == "cubic meter":
            return "kilogram", exchange["amount"] * 1000
        return exchange["unit"], exchange["amount"]

    expected_physical = Counter(
        physical_quantity(e)
        for p in profiles(version).values()
        for e in p["inputs"]
        if e.get("type") == "biosphere" and not e.get("approved_omission_if_unavailable")
    )
    actual_physical = Counter(
        physical_quantity(e)
        for d in result.data[1:]
        for e in d["exchanges"]
        if e["type"] == "biosphere" and e["name"] != "Non-hazardous waste disposed"
    )
    assert actual_physical == expected_physical
    assert not any(e["name"] == "Non-hazardous waste disposed" for e in unresolved)
    assert any(i.code == "background.biosphere_link_unresolved" for i in result.last_migration_report.issues)
    assert inventory.data == data


def test_consent_foreground_ownership_unknown_profiles_and_consequential_refusal():
    data = batch("3.12", -2)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.12"))
    with pytest.raises(MigrationError):
        inventory.migrate_background(target_context("2025"), policy=MigrationPolicy.strict())
    assert inventory.data == data
    for row in review.SOURCES.values():
        data.append({**row["source"], "exchanges": [{**row["source"], "type": "production", "amount": -1}]})
    assert (
        BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.12"))
        .migrate_background(target_context("2025"), policy=POLICY)
        .data
        == data
    )
    resource = json.loads((review.ROOT / "brightpath/data/migrations/uvek/ecoinvent-to-uvek-2025.json").read_text())
    for version in ("3.8", "3.10.2", "3.13"):
        selected = select_profile_sign_rules(resource, TechnosphereProfile("ecoinvent", version, "cutoff"))
        for row in review.SOURCES.values():
            rule = next(r for r in selected["replace"] if r["source"] == row["source"])
            assert rule["reference_flow_sign_review"]["status"] == "unresolved"
    with pytest.raises(MigrationError, match="system models"):
        BrightwayInventory.from_data(
            batch("3.12", -1), context=contexts("ecoinvent", "3.12", "consequential")
        ).migrate_background(target_context("2025"), policy=MigrationPolicy.permissive())


def evidence():
    inventories = defaultdict(list)
    observations = {"ecoinvent": defaultdict(set), "uvek": defaultdict(set)}
    for row in review.APPROVAL["inventories"]:
        source = review.identity(row["source"])
        inventories[source].append({**deepcopy(row), "source": list(source)})
        observations[row["label"].split("-")[0]][source].add((row["label"], row["reference_amount"]))
    native = {review.identity(row["source"]): deepcopy(row) for row in review.APPROVAL["native_roles"]}
    return inventories, observations, native


@pytest.mark.parametrize("change", ("none", "inventory", "metadata", "missing", "duplicate", "sign", "native_role"))
def test_exact_full_source_and_native_inventory_evidence_cannot_change(change):
    inventories, observations, native = evidence()
    source = next(iter(review.SOURCES))
    if change == "inventory":
        inventories[source][0]["inventory_sha256"] = "0" * 64
    elif change == "metadata":
        inventories[source][0]["activity_comment_sha256"] = "0" * 64
    elif change == "missing":
        inventories[source].pop()
    elif change == "duplicate":
        inventories[source].append(deepcopy(inventories[source][0]))
    elif change == "sign":
        observations["ecoinvent"][source] = {(label, 1) for label, _ in observations["ecoinvent"][source]}
    elif change == "native_role":
        role = native[next(iter(native))]
        role["native_reference_is_input"] = not role["native_reference_is_input"]
    if change == "none":
        review.validate_inventories(inventories, observations, native)
    else:
        with pytest.raises(ValueError, match="repeat the scientific review"):
            review.validate_inventories(inventories, observations, native)


def test_source_release_scoped_soil_carbon_and_stable_uuid_label_refinements():
    assert len(review.APPROVAL["same_uuid_canonical_label_refinements"]) == 6
    assert {r["source_name"] for r in review.APPROVAL["same_uuid_canonical_label_refinements"]} == {
        "Calcium",
        "Sodium",
        "Strontium",
    }
    assert len(review.APPROVAL["canonical_biosphere_definitions"]) == 45
    resource = json.loads((review.ROOT / "brightpath/data/migrations/uvek/ecoinvent-to-uvek-2025.json").read_text())
    review.validate_bindings(resource)
    dependency = next(iter(review.APPROVAL["recipe_dependencies"]))
    resource["recipes"][dependency]["inputs"][0]["amount"] += 1
    with pytest.raises(ValueError, match="repeat the scientific review"):
        review.validate_bindings(resource)


@pytest.mark.parametrize("release", ("2025", "2026"))
@pytest.mark.parametrize("amount", (-2, 0, 2))
def test_independent_reverse_mechanical_service_preserves_signed_caller_exchanges(release, amount):
    entry = review.APPROVAL["reverse_binding"]
    data = consumer(entry["source"], amount, 2)
    data[0]["exchanges"].append(consumer(entry["source"], amount, 3, "substitution")[0]["exchanges"][1])
    original = deepcopy(data)
    inventory = BrightwayInventory.from_data(data, context=contexts("uvek", release))
    result = inventory.migrate_background(
        BackgroundContext(TechnosphereProfile("ecoinvent", "3.12", "cutoff"), BiosphereProfile("ecoinvent", "3.10")),
        policy=MigrationPolicy(
            on_information_loss=PolicyAction.WARN,
            on_heuristic_mapping=PolicyAction.WARN,
            on_inferred_reverse=PolicyAction.WARN,
        ),
    )
    assert len(result.data) == 1
    for before, after in zip(original[0]["exchanges"][1:], result.data[0]["exchanges"][1:], strict=True):
        assert review.identity(after) == review.identity(entry["target"])
        for field in ("amount", "type", "scale", "loc", "minimum", "maximum", "negative"):
            if field in before:
                assert after[field] == before[field]
    assert inventory.data == data == original


def test_former_incineration_supplier_does_not_acquire_an_inferred_landfarming_inverse():
    resource = json.loads(
        (review.ROOT / "brightpath/data/migrations/uvek/uvek-2025-to-ecoinvent-3.12-cutoff.json").read_text()
    )
    former = ("Disposal, raw sewage sludge, to municipal incineration",) * 2 + ("CH", "kilogram")
    assert not any(review.identity(r["source"]) == former for r in resource["replace"])
