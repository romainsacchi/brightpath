"""Wet landfill-leachate sludge keeps its physical releases and independent spreading service."""

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
from scripts import review_uvek_slf_landfarming as review

POLICY = MigrationPolicy(on_information_loss=PolicyAction.WARN, on_heuristic_mapping=PolicyAction.WARN)


def profiles(version):
    return {
        source: profile
        for source, row in review.SOURCES.items()
        for profile in row["profiles"]
        if profile["label"] == f"ecoinvent-{version}-cutoff"
    }


def signature(exchange):
    return exchange["name"], tuple(exchange["categories"]), exchange["unit"], exchange["amount"]


def batch(version, amount):
    data = None
    for source in profiles(version):
        part = consumer(dict(zip(FIELDS, source, strict=True)), amount, 2)
        part[0]["exchanges"].append(
            consumer(dict(zip(FIELDS, source, strict=True)), amount, 3, "substitution")[0]["exchanges"][1]
        )
        if data is None:
            data = part
        else:
            data[0]["exchanges"].extend(part[0]["exchanges"][1:])
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
    return data


@pytest.mark.parametrize("version", sorted(review.VERSIONS))
@pytest.mark.parametrize("release", ("2025", "2026"))
@pytest.mark.parametrize("amount", (-2, 0, 2))
def test_complete_wet_sludge_helpers_preserve_emissions_volume_and_signed_caller_state(version, release, amount):
    data = batch(version, amount)
    original = deepcopy(data)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", version))
    result = inventory.migrate_background(target_context(release), policy=POLICY)
    assert len(result.data) == 1 + len(profiles(version))
    assert result.data[0]["exchanges"][0] == original[0]["exchanges"][0]
    assert result.data[0]["exchanges"][-1] == original[0]["exchanges"][-1]
    helpers = {d["code"]: d for d in result.data[1:]}
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
        helper = helpers[after["input"][1]]
        (production,) = [e for e in helper["exchanges"] if e["type"] == "production"]
        assert production["amount"] == 1 and helper["unit"] == "kilogram"
        assert after["amount"] / production["amount"] == before["amount"] / -1
        (service,) = [e for e in helper["exchanges"] if e["type"] == "technosphere"]
        assert service["name"] == "Slurry spreading, by vacuum tanker" and service["unit"] == "cubic meter"
        assert service["amount"] == 0.00097087
        flows = [e for e in helper["exchanges"] if e["type"] == "biosphere"]
        expected = [e for e in profiles(version)[source]["inputs"] if e["type"] == "biosphere"]
        assert Counter(map(signature, flows)) == Counter(map(signature, expected))
        assert sum(e["amount"] for e in flows if e["name"] == "Water") == pytest.approx(0.00097)
        assert not any(e["name"] == "Chromium VI" for e in flows)
        assert all(e["amount"] > 0 for e in flows)
        assert not any("recipe" in e or e["unit"] == "ton kilometer" for e in helper["exchanges"])
    assert result.last_migration_report.losses
    assert inventory.data == data == original


@pytest.mark.parametrize("version", sorted(review.VERSIONS))
def test_independently_reviewed_reverse_spreading_keeps_helper_graph_and_physical_emissions(version):
    canonical = BrightwayInventory.from_data(
        batch(version, -1), context=contexts("ecoinvent", version)
    ).migrate_background(target_context("2025"), policy=POLICY)
    original = deepcopy(canonical.data)
    result = canonical.migrate_background(
        BackgroundContext(TechnosphereProfile("ecoinvent", "3.12", "cutoff"), BiosphereProfile("ecoinvent", "3.10")),
        policy=MigrationPolicy(
            on_information_loss=PolicyAction.WARN,
            on_heuristic_mapping=PolicyAction.WARN,
            on_inferred_reverse=PolicyAction.WARN,
        ),
    )
    assert len(result.data) == len(original) and result.data[0] == original[0]
    for after, before in zip(result.data[1:], original[1:], strict=True):
        assert [e for e in after["exchanges"] if e["type"] == "biosphere"] == [
            e for e in before["exchanges"] if e["type"] == "biosphere"
        ]
        (service,) = [e for e in after["exchanges"] if e["type"] == "technosphere"]
        assert service["name"] == "liquid manure spreading, by vacuum tanker" and service["location"] == "CH"
        assert service["amount"] == 0.00097087
    assert canonical.data == original


@pytest.mark.parametrize("version", sorted(review.VERSIONS))
def test_native_physical_gaps_reject_default_and_remain_unlinked_in_explicit_permissive_mode(version):
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
        signature(e) for p in profiles(version).values() for e in p["inputs"] if e["type"] == "biosphere"
    )
    assert unresolved and Counter(map(signature, unresolved)) <= expected
    assert not any(e["name"] == "Non-hazardous waste disposed" for e in unresolved)
    assert any(i.code == "background.biosphere_link_unresolved" for i in result.last_migration_report.issues)
    assert inventory.data == data


def test_strict_loss_refusal_foreground_ownership_and_exact_profiles():
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
    for version in ("3.9.1", "3.10.2", "3.13"):
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
    data = defaultdict(list)
    observations = {"ecoinvent": defaultdict(set), "uvek": defaultdict(set)}
    for row in review.APPROVAL["inventories"]:
        source = tuple(row["source"][f] for f in FIELDS)
        data[source].append({**deepcopy(row), "source": list(source)})
        observations[row["label"].split("-")[0]][source].add((row["label"], row["reference_amount"]))
    native = {tuple(row["source"][f] for f in FIELDS): deepcopy(row) for row in review.APPROVAL["native_roles"]}
    return data, observations, native


@pytest.mark.parametrize(
    "change",
    (
        "inventory_sha256",
        "activity_comment_sha256",
        "product_information_sha256",
        "missing",
        "duplicate",
        "patch",
        "native_role",
    ),
)
def test_changed_source_inventory_description_profile_or_native_role_requires_review(change):
    data, observations, native = evidence()
    review.validate_inventories(data, observations, native)
    source = next(iter(review.SOURCES))
    if change in ("inventory_sha256", "activity_comment_sha256", "product_information_sha256"):
        data[source][0][change] = "0" * 64
    elif change == "missing":
        data[source].pop()
    elif change == "duplicate":
        data[source].append(deepcopy(data[source][0]))
    elif change == "patch":
        observations["ecoinvent"][source].add(("ecoinvent-3.10.2-cutoff", -1))
    else:
        native[next(iter(native))]["native_reference_is_input"] = True
    with pytest.raises(ValueError, match="repeat the scientific review"):
        review.validate_inventories(data, observations, native)


@pytest.mark.parametrize("change", ("wet_basis", "emission", "prior_factor"))
def test_signed_source_coordinate_and_complete_physical_inventory_cannot_drift(monkeypatch, change):
    review.validate_bindings(load_uvek_technosphere_resource())
    row = review.APPROVAL["rules"][0]
    if change == "prior_factor":
        monkeypatch.setitem(row["previous_flat_sign_decision"], "original_factor", -1)
    else:
        inputs = deepcopy(row["profiles"][0]["inputs"])
        inputs[0 if change == "wet_basis" else 1]["amount"] *= 0.97
        monkeypatch.setitem(row["profiles"][0], "inputs", inputs)
    with pytest.raises(ValueError, match="quantities, emissions or profile scope changed"):
        review.validate_bindings(load_uvek_technosphere_resource())


def test_prior_flat_negative_factor_is_restored_only_for_the_exact_verified_sign_binding():
    row = next(iter(review.SOURCES.values()))
    prior = row["previous_flat_sign_decision"]
    rule = {
        "source": row["source"],
        "target": row["previous_target"],
        "conversion_factor": -1,
        "reference_flow_sign_review": deepcopy(prior["review"]),
    }
    original = deepcopy(rule)
    obs = {(p["label"], p["source_sign"]) for p in row["profiles"]}
    records = [{"label": p["label"], "inventory_sha256": p["inventory_sha256"]} for p in row["profiles"]]
    decision = review.forward_decision(rule, obs, records, prior["binding"], {})
    assert decision["original_factor"] == 1 and decision["factor"] == -1
    assert decision["original_rule"].get("conversion_factor", 1) == 1
    assert rule == original
    rule["reference_flow_sign_review"]["target_sign"] = -1
    with pytest.raises(ValueError, match="prior reference sign changed"):
        review.forward_decision(rule, obs, records, prior["binding"], {})


@pytest.mark.parametrize("version", ("3.10.1", "3.12"))
def test_excel_round_trip_keeps_all_helpers_physical_releases_and_caller_uncertainty(tmp_path, version):
    source = next(iter(profiles(version)))
    data = consumer(dict(zip(FIELDS, source, strict=True)), -2, 3)
    data[0]["comment"] = "Complete wet landfill-leachate sludge helper export"
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", version)).migrate_background(
        target_context("2025"), policy=POLICY
    )
    path = tmp_path / "wet-sludge.xlsx"
    result.write_excel(path)
    restored = BrightwayInventory.from_excel(path, context=result.context)
    assert len(restored.data) == len(result.data) == 2
    actual = restored.data[0]["exchanges"][1]
    expected = result.data[0]["exchanges"][1]
    assert tuple(actual[f] for f in FIELDS) == tuple(expected[f] for f in FIELDS)
    assert actual["amount"] == expected["amount"] == 2 and actual["uncertainty type"] == 3
    assert (actual["loc"], actual["scale"]) == (expected["loc"], expected["scale"])
    assert Counter(signature(e) for e in restored.data[1]["exchanges"] if e["type"] == "biosphere") == Counter(
        signature(e) for e in result.data[1]["exchanges"] if e["type"] == "biosphere"
    )
