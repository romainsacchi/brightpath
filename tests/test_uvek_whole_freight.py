"""Freeze inspected freight class/unit boundaries before propagating their exact parent DAG."""

import json
from collections import defaultdict
from copy import deepcopy

import pytest
from test_uvek_reference_signs import consumer, contexts
from test_uvek_refrigerant_disposal import target_context

from brightpath import BrightwayInventory
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from scripts import review_uvek_whole_freight as review
from scripts.review_uvek_market_compositions import composition_recipe

POLICY = MigrationPolicy(on_heuristic_mapping=PolicyAction.WARN, on_information_loss=PolicyAction.WARN)
CASES = [(source, p["label"].split("-")[1]) for source, row in review.SOURCES.items() for p in row["profiles"]]


@pytest.mark.parametrize("source,version", CASES)
@pytest.mark.parametrize("release", ("2025", "2026", "native2026"))
@pytest.mark.parametrize("amount", (-2, 0, 2))
def test_actual_whole_freight_migration_preserves_signed_tkm_and_all_caller_state(source, version, release, amount):
    row = review.SOURCES[source]
    data = consumer(row["source"], amount, 2)
    data[0]["comment"] = "Synthetic whole-freight regression."
    data[0]["exchanges"].append(consumer(row["source"], amount, 3, "substitution")[0]["exchanges"][1])
    for exchange in data[0]["exchanges"][1:]:
        exchange["caller_metadata"] = {"retain": [1, 2]}
    data[0]["exchanges"].append(
        {
            "name": "Carbon dioxide, fossil",
            "categories": ["air"],
            "unit": "kilogram",
            "type": "biosphere",
            "amount": 0.5,
        }
    )
    before = deepcopy(data)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", version))
    result = inventory.migrate_background(target_context(release), policy=POLICY)
    assert len(result.data) == 1
    for a, b in zip(before[0]["exchanges"][1:-1], result.data[0]["exchanges"][1:-1], strict=True):
        assert {f: b[f] for f in review.FIELDS} == row["target"]
        assert all(b[k] == v for k, v in a.items() if k not in review.FIELDS)
    assert result.data[0]["exchanges"][0] == before[0]["exchanges"][0]
    assert result.data[0]["exchanges"][-1]["amount"] == 0.5
    assert any(i.code == "migration.technosphere_information_loss" for i in result.last_migration_report.issues)
    assert not any(i.code == "migration.zero_burden_supplier" for i in result.last_migration_report.issues)
    assert inventory.data == data == before
    assert result.migrate_background(target_context(release)).data == result.data


@pytest.mark.parametrize("release", ("2025", "native2026"))
def test_strict_consent_boundary_foreground_ownership_and_consequential_refusal(release):
    source, row = next(iter(review.SOURCES.items()))
    data = consumer(row["source"], -2, 3)
    before = deepcopy(data)
    for model, policy in (("cutoff", MigrationPolicy.strict()), ("consequential", MigrationPolicy.permissive())):
        inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.10", model))
        with pytest.raises(MigrationError):
            inventory.migrate_background(target_context(release), policy=policy)
        assert inventory.data == before
    data.append({**row["source"], "exchanges": [{**row["source"], "type": "production", "amount": 1}]})
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.10"))
    assert inventory.migrate_background(target_context(release), policy=POLICY).data == data


@pytest.mark.parametrize("version", ("3.6", "3.7", "3.8", "3.9", "3.10", "3.11", "3.12"))
@pytest.mark.parametrize("release", ("2025", "native2026"))
@pytest.mark.parametrize("amount", (-2, 0, 2))
def test_every_propagated_parent_retains_its_reference_sign_and_complete_recipe_graph(version, release, amount):
    rows = [r for r in review.APPROVAL["parent_rule_bindings"] if r["label"] == f"ecoinvent-{version}-cutoff"]
    assert rows
    data = None
    for row in rows:
        piece = consumer(row["source"], amount, 3)
        if data is None:
            data = piece
        else:
            data[0]["exchanges"].append(piece[0]["exchanges"][1])
    data[0]["comment"] = "Synthetic full freight parent graph."
    before = deepcopy(data)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", version))
    native_physical_gaps = release == "native2026" and version in ("3.10", "3.11", "3.12")
    if native_physical_gaps:
        # Later source-faithful disposal children introduced documented physical gaps.
        # Native strict coverage must refuse; explicit permissive retains those links.
        with pytest.raises(MigrationError) as caught:
            inventory.migrate_background(target_context(release), policy=POLICY)
        assert any(i.code == "background.biosphere_link_unresolved" for i in caught.value.report.issues)
        assert inventory.data == before
        result = inventory.migrate_background(target_context(release), policy=MigrationPolicy.permissive())
    else:
        result = inventory.migrate_background(target_context(release), policy=POLICY)
    by_code = {r.get("code"): r for r in result.data[1:]}
    resource = json.loads((review.ROOT / "brightpath/data/migrations/uvek/ecoinvent-to-uvek-2025.json").read_text())
    from brightpath import TechnosphereProfile
    from brightpath.background.reference_signs import select_profile_sign_rules

    unresolved = [i for i in result.last_migration_report.issues if i.code.endswith("link_unresolved")]
    if native_physical_gaps:
        from scripts.review_uvek_uncontrolled_disposal import APPROVAL as DISPOSAL_APPROVAL
        from scripts.review_uvek_uncontrolled_disposal import REVIEW_PATH

        def key(row):
            return row["name"], tuple(row["categories"]), row["unit"]

        missing = {key(row) for row in DISPOSAL_APPROVAL["native_unmapped_physical_definitions"]}
        expected = set()
        approved_datasets = set()
        for di, dataset in enumerate(result.data):
            recipe_id = dataset.get("brightpath_conversion", {}).get("recipe_id")
            if not recipe_id:
                continue
            recipe = resource["recipes"][recipe_id]
            if not any(REVIEW_PATH in evidence for evidence in recipe.get("evidence", [])):
                continue
            retained = [e for e in dataset["exchanges"] if e["type"] == "biosphere" and key(e) in missing]
            original = [e for e in recipe["inputs"] if "categories" in e and key(e) in missing]
            assert len(retained) == len(original)
            assert sorted((key(e), e["amount"]) for e in retained) == sorted((key(e), e["amount"]) for e in original)
            for exchange in retained:
                expected.add(key(exchange))
                approved_datasets.add(di)
        assert len(expected) == 13
        assert unresolved and all(i.code == "background.biosphere_link_unresolved" for i in unresolved)
        assert {key(i.details["identity"]) for i in unresolved} == expected
        for issue in unresolved:
            if issue.path.startswith("datasets["):
                assert int(issue.path.split("[", 1)[1].split("]", 1)[0]) in approved_datasets
            else:
                assert issue.path.startswith("generated_helpers[")
    else:
        assert not unresolved
    selected = select_profile_sign_rules(resource, TechnosphereProfile("ecoinvent", version, "cutoff"))
    rules = {review.identity(r["source"]): r for r in selected["replace"]}
    for original, exchange, row in zip(before[0]["exchanges"][1:], result.data[0]["exchanges"][1:], rows, strict=True):
        rule = rules[review.identity(row["source"])]
        factor = rule["conversion_factor"]
        assert factor in (-1, 1) and exchange["amount"] == amount * factor
        helper = by_code[exchange["input"][1]]
        assert helper["brightpath_conversion"]["recipe_id"] == rule["target"]["recipe_ref"]
        assert helper["exchanges"][0]["type"] == "production" and helper["exchanges"][0]["amount"] == 1
        assert exchange["loc"] == original["loc"] * factor and exchange["scale"] == original["scale"]
        assert exchange["minimum"] == (original["minimum"] if factor == 1 else -original["maximum"])
        assert exchange["maximum"] == (original["maximum"] if factor == 1 else -original["minimum"])
        assert exchange["formula"] == ("dose" if factor == 1 else "(dose) * (-1.0)")
    assert inventory.data == data == before


def evidence():
    inventories = defaultdict(list)
    observations = {"ecoinvent": defaultdict(set), "uvek": defaultdict(set)}
    for row in review.APPROVAL["inventories"]:
        source = review.identity(row["source"])
        inventories[source].append({**deepcopy(row), "source": list(source)})
        observations[row["label"].split("-")[0]][source].add((row["label"], row["reference_amount"]))
    native = {review.identity(r["source"]): deepcopy(r) for r in review.APPROVAL["native_roles"]}
    return inventories, observations, native


@pytest.mark.parametrize("source", sorted(review.SOURCES))
def test_projection_corrects_only_the_reviewed_vehicle_class_and_whole_service_with_factor_one(source):
    row = review.SOURCES[source]
    rule = {"source": deepcopy(row["source"]), "target": deepcopy(row["previous_target"])}
    before = deepcopy(rule)
    result = review.approved_freight_rule(rule)
    assert result["target"] == row["target"] and result.get("conversion_factor", 1) == 1
    assert rule == before
    name, location = review.TARGETS[row["kind"]]
    assert review.identity(result["target"]) == (name, name, location, "ton kilometer")


@pytest.mark.parametrize("source", sorted(review.SOURCES))
@pytest.mark.parametrize("mutation", ("target", "factor"))
def test_projection_refuses_unreviewed_vehicle_or_payload_scaling(source, mutation):
    row = review.SOURCES[source]
    rule = {"source": deepcopy(row["source"]), "target": deepcopy(row["previous_target"])}
    if mutation == "target":
        rule["target"]["name"] = "Unreviewed freight class"
    else:
        rule["conversion_factor"] = 1 / 0.263
    with pytest.raises(ValueError, match="repeat the scientific review"):
        review.approved_freight_rule(rule)


@pytest.mark.parametrize("source", sorted(review.SOURCES))
def test_decision_is_bounded_to_actual_cutoff_profiles_without_a_global_or_consequential_fallback(source):
    row = review.SOURCES[source]
    observations = {(p["label"], 1) for p in row["profiles"]}
    decision = review.forward_decision(
        {"source": row["source"], "target": row["target"]},
        observations,
        {1},
        {"native_flow_type": "PRODUCT_FLOW", "native_reference_is_input": False},
        "private fixture binding",
        {},
        None,
    )
    assert decision["factor"] == decision["original_factor"] == 1
    assert decision["review"]["status"] == "unresolved"
    profiles = decision["review"]["profiles"]
    assert {"-".join(p["source_profile"][f] for f in ("family", "version", "system_model")) for p in profiles} == {
        p["label"] for p in row["profiles"]
    }
    assert all(p["source_profile"]["system_model"] == "cutoff" for p in profiles)
    assert all(p["conversion_factor"] == 1 and p["review"]["status"] == "verified" for p in profiles)
    with pytest.raises(ValueError, match="repeat the scientific review"):
        review.forward_decision(
            {"source": row["source"], "target": row["target"]},
            observations | {("ecoinvent-3.12.1-cutoff", 1)},
            {1},
            {"native_flow_type": "PRODUCT_FLOW", "native_reference_is_input": False},
            "private fixture binding",
            {},
            None,
        )


@pytest.mark.parametrize("mutation", ("inventory", "metadata", "missing", "duplicate", "source_sign", "target_role"))
def test_actual_complete_fleet_vehicle_and_parent_fingerprints_are_required(mutation):
    inventories, observations, native = evidence()
    review.validate_inventories(inventories, observations, native)
    source, row = next(iter(review.SOURCES.items()))
    if mutation == "inventory":
        inventories[source][0]["inventory_sha256"] = "0" * 64
    elif mutation == "metadata":
        inventories[source][0]["product_information_sha256"] = "0" * 64
    elif mutation == "missing":
        inventories[source].pop()
    elif mutation == "duplicate":
        inventories[source].append(deepcopy(inventories[source][0]))
    elif mutation == "source_sign":
        observations["ecoinvent"][source] = {(label, -1) for label, _ in observations["ecoinvent"][source]}
    else:
        native[review.identity(row["target"])]["native_reference_is_input"] = True
    with pytest.raises(ValueError, match="repeat the scientific review"):
        review.validate_inventories(inventories, observations, native)


def test_partially_propagated_shared_freight_cannot_be_published(monkeypatch):
    monkeypatch.setitem(review.APPROVAL, "publication_status", "staged")
    with pytest.raises(ValueError, match="propagation is incomplete"):
        review.validate_bindings({"replace": []})


def test_a_changed_review_cannot_exchange_light_and_heavy_freight_classes(monkeypatch):
    source = next(s for s, row in review.SOURCES.items() if row["kind"] == "light_road")
    row = review.SOURCES[source]
    name, location = review.TARGETS["heavy_road"]
    monkeypatch.setitem(row, "kind", "heavy_road")
    monkeypatch.setitem(row, "target", dict(zip(review.FIELDS, (name, name, location, "ton kilometer"), strict=True)))
    with pytest.raises(ValueError, match="mode/class changed"):
        review.approved_freight_rule({"source": row["source"], "target": row["previous_target"]})


def test_explicit_scoped_namespace_forks_a_recipe_id_without_changing_any_signed_input_or_legacy_default():
    source = ("Private composition", "Private service", "GLO", "kilogram")
    profile = {"inputs": [{"name": "Private supplier", "amount": -2, "type": "technosphere"}]}
    before = deepcopy(profile)
    original_id, original = composition_recipe(source, profile)
    scoped_id, scoped = composition_recipe(source, {**profile, "recipe_namespace": "whole-freight-market-composition"})
    assert original_id.startswith("market-composition-")
    assert scoped_id.startswith("whole-freight-market-composition-")
    assert scoped_id != original_id
    assert scoped == original and profile == before
    assert composition_recipe(source, profile) == (original_id, original)


@pytest.mark.parametrize("kind", ("market", "processing", "slag"))
def test_current_profile_recipe_ids_and_bodies_reproduce_packaged_output(kind):
    from scripts.review_uvek_cutoff_slag_products import APPROVAL as slag
    from scripts.review_uvek_cutoff_slag_products import FORWARD_LIMITATIONS as slag_reason
    from scripts.review_uvek_cutoff_slag_products import REVIEW_PATH as slag_path
    from scripts.review_uvek_market_compositions import APPROVAL as markets
    from scripts.review_uvek_market_compositions import LIMITATIONS as market_reason
    from scripts.review_uvek_market_compositions import REVIEW_PATH as market_path
    from scripts.review_uvek_processing_compositions import APPROVAL as processing
    from scripts.review_uvek_processing_compositions import LIMITATIONS as processing_reason
    from scripts.review_uvek_processing_compositions import REVIEW_PATH as processing_path

    approval, reason, path, namespace, method = {
        "market": (markets, market_reason, market_path, "market-composition", "reviewed_market_composition"),
        "processing": (
            processing,
            processing_reason,
            processing_path,
            "processing-composition",
            "reviewed_processing_composition",
        ),
        "slag": (slag, slag_reason, slag_path, "cutoff-slag-product", "reviewed_cutoff_slag_product"),
    }[kind]
    resource = json.loads((review.ROOT / "brightpath/data/migrations/uvek/ecoinvent-to-uvek-2025.json").read_text())
    rules = {review.identity(r["source"]): r for r in resource["replace"]}
    checked = 0
    for row in approval["rules"]:
        source = review.identity(row["source"])
        rule = rules.get(source, {})
        if rule.get("mapping_method") != method:
            continue
        profiles = {
            "-".join(p["source_profile"][f] for f in ("family", "version", "system_model")): p
            for p in rule["reference_flow_sign_review"]["profiles"]
            if p["review"]["status"] == "verified" and p["source_profile"]["system_model"] == "cutoff"
        }
        for profile in row["profiles"]:
            entry = profiles.get(profile["label"])
            if entry is None:
                continue
            recipe_id, body = composition_recipe(
                source, profile, rationale=reason, review_path=path, namespace=namespace
            )
            from scripts.review_uvek_digester_stock import APPROVAL as DIGESTER_APPROVAL
            from scripts.review_uvek_digester_stock import SOURCES as DIGESTER_SOURCES

            if recipe_id in DIGESTER_APPROVAL["retired_helper_bodies"]:
                # Keep the original recipe body pinned as audit evidence while
                # checking the independently reviewed allocated-stock replacement.
                assert kind == "market"
                assert body == DIGESTER_APPROVAL["retired_helper_bodies"][recipe_id]
                replacement = DIGESTER_SOURCES[source]
                assert replacement["kind"] == "market"
                (approved,) = [p for p in replacement["profiles"] if p["label"] == profile["label"]]
                recipe_id, body = approved["recipe_id"], approved["recipe"]
            assert entry.get("target", rule["target"]) == {"recipe_ref": recipe_id}
            assert resource["recipes"][recipe_id] == body
            checked += 1
    assert checked > 0


@pytest.mark.parametrize("namespace", (None, "", " ", 1, True))
def test_review_namespace_must_be_explicit_and_nonempty(namespace):
    with pytest.raises(ValueError, match="nonempty string"):
        composition_recipe(
            ("Private", "Private", "GLO", "kilogram"),
            {"inputs": [{"amount": 1}], "recipe_namespace": namespace},
        )
