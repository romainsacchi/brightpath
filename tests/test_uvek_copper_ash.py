"""Copper-specific ash disposal must retain releases, infrastructure and collection."""

import json
from collections import defaultdict
from copy import deepcopy

import pytest
from test_uvek_reference_signs import consumer, contexts
from test_uvek_refrigerant_disposal import target_context

from brightpath import BrightwayInventory
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.migrations.resources import load_uvek_technosphere_resource
from scripts import review_uvek_copper_ash as review

F = review.FIELDS
CASES = [(row, profile) for row in review.SOURCES.values() for profile in row["profiles"]]
POLICY = MigrationPolicy(on_heuristic_mapping=PolicyAction.WARN, on_information_loss=PolicyAction.WARN)


def key(row):
    return tuple(row[f] for f in F)


@pytest.mark.parametrize("row,profile", CASES)
@pytest.mark.parametrize("release", ("2025", "2026", "native2026"))
@pytest.mark.parametrize("amount", (-2, 0, 2))
def test_signed_copper_ash_preserves_waste_specific_releases_and_collection(row, profile, release, amount):
    version = profile["label"].split("-")[1]
    data = consumer(row["source"], amount, 2)
    data[0]["exchanges"][1]["caller_metadata"] = {"preserve": [1, 2]}
    before = deepcopy(data)
    source = BrightwayInventory.from_data(data, context=contexts("ecoinvent", version))
    result = source.migrate_background(target_context(release), policy=POLICY)
    assert source.data == before == data
    after = result.data[0]["exchanges"][1]
    original = before[0]["exchanges"][1]
    assert after["amount"] == -amount
    assert after["formula"] == "(dose) * (-1.0)"
    assert after["scale"] == original["scale"] and after["loc"] == original["loc"]
    assert after["negative"] is not original["negative"]
    assert after["caller_metadata"] == original["caller_metadata"]
    helpers = {d["code"]: d for d in result.data[1:]}
    root = helpers[after["input"][1]]
    coefficient_fields = (
        "amount",
        "uncertainty type",
        "loc",
        "scale",
        "shape",
        "minimum",
        "maximum",
        "negative",
        "pedigree",
        "scale without pedigree",
        "formula",
    )
    actual_inputs = [e for e in root["exchanges"] if e["type"] != "production"]
    expected_inputs = [
        e
        for e in profile["inputs"]
        if not (release == "native2026" and e.get("categories", [])[:1] == ["inventory indicator"])
    ]

    def coefficients(exchange):
        return json.dumps({f: exchange[f] for f in coefficient_fields if f in exchange}, sort_keys=True)

    assert sorted(map(coefficients, actual_inputs)) == sorted(map(coefficients, expected_inputs))
    if row["source"]["name"].startswith("market"):
        inputs = [e for e in root["exchanges"] if e["type"] != "production"]
        assert len(inputs) == 3
        assert sorted(e["amount"] for e in inputs) == [0.00356, 0.02577, 1]
        ash = next(e for e in inputs if e["unit"] == "kilogram")
        (leaf,) = [d for d in helpers.values() if key(d) == key(ash)]
    else:
        leaf = root
    tech = [e for e in leaf["exchanges"] if e["type"] == "technosphere"]
    assert len(tech) == 2
    assert sorted(e["amount"] for e in tech) == [1.7778e-9, 1]
    bio = [e for e in leaf["exchanges"] if e["type"] == "biosphere"]
    indicators = [e for e in bio if e["categories"][0] == "inventory indicator"]
    physical = [e for e in bio if e not in indicators]
    assert len(indicators) == (0 if release == "native2026" else 2)
    assert len(physical) == 2
    assert sorted(e["amount"] for e in physical) == [2.684e-5, 0.77305]
    assert {e["name"] for e in physical} == ({"Copper"} if release == "native2026" else {"Copper ion"})
    (ground,) = [e for e in physical if "long-term" in "/".join(e["categories"])]
    assert ground["amount"] == 0.77305
    assert not any(
        "horse" in e["name"].lower() or "concrete" in e["name"].lower()
        for d in helpers.values()
        for e in d["exchanges"]
    )


@pytest.mark.parametrize("change", ("inventory", "metadata", "reference", "profile", "native-role"))
def test_stale_copper_ash_evidence_cannot_inherit_approval(change):
    records = deepcopy(review.APPROVAL["inventories"])
    observations = defaultdict(lambda: defaultdict(set))
    for row, profile in CASES:
        observations["ecoinvent"][key(row["source"])].add((profile["label"], -1))
    native = {key(r["source"]): deepcopy(r) for r in review.APPROVAL["native_roles"]}
    review.validate_inventories(records, observations, native)
    if change == "inventory":
        records[0]["inventory_sha256"] = "0" * 64
    elif change == "metadata":
        records[0]["metadata_sha256"] = "0" * 64
    elif change == "reference":
        records[0]["reference_amount"] = 1
    elif change == "profile":
        source = key(CASES[0][0]["source"])
        observations["ecoinvent"][source].add(("ecoinvent-3.10.1-cutoff", -1))
    else:
        next(iter(native.values()))["native_reference_is_input"] = "changed"
    with pytest.raises(ValueError, match="Copper-ash"):
        review.validate_inventories(records, observations, native)


@pytest.mark.parametrize("kind", ("operation", "facility", "road", "rail"))
def test_changed_component_or_collection_rule_cannot_inherit_approval(kind):
    resource = deepcopy(load_uvek_technosphere_resource())
    review.validate_bindings(resource)
    names = {
        "operation": "process-specific burdens, slag landfill",
        "facility": "slag landfill construction",
        "road": "market for transport, freight, lorry, unspecified",
        "rail": "market for transport, freight, train, fleet average",
    }
    source = next(
        tuple(binding["source"])
        for row, profile in CASES
        for binding in profile["bindings"]
        if "source" in binding and binding["source"][0] == names[kind]
    )
    (child,) = [r for r in resource["replace"] if key(r["source"]) == source]
    # The stored fingerprint covers exact selected profile decisions as well as the target.
    child["confidence"] = 0.125
    with pytest.raises(ValueError, match="binding changed"):
        review.validate_bindings(resource)


def test_exact_scope_and_authoring_decisions_replay():
    resource = load_uvek_technosphere_resource()
    ledger = json.loads((review.ROOT / "brightpath/data/export/uvek_reference_flow_sign_review.json").read_text())
    decisions = ledger["resources"][resource["name"]]
    assert len(review.SOURCES) == 2 and len(CASES) == 4
    assert {p["label"] for _, p in CASES} == {"ecoinvent-3.11-cutoff", "ecoinvent-3.12-cutoff"}
    for row in review.SOURCES.values():
        previous = decisions[row["id"]]
        replayed = review.forward_decision(
            previous["original_rule"],
            {(p["label"], -1) for p in row["profiles"]},
            previous["binding"],
            previous["original_review"],
        )
        assert replayed == previous
        assert replayed["factor"] == -1 and replayed["original_factor"] == 1
        assert all("PENDING" not in json.dumps(body) for body in replayed["recipes"].values())
    reverse = review.APPROVAL["independent_reverse"]
    import hashlib

    assert (
        hashlib.sha256((review.ROOT / reverse["resource_path"]).read_bytes()).hexdigest() == reverse["resource_sha256"]
    )


@pytest.mark.parametrize("native", (False, True))
def test_complete_copper_ash_hierarchy_survives_brightway_roundtrip(tmp_path, native):

    row = next(r for r in review.SOURCES.values() if r["source"]["name"].startswith("market"))
    data = consumer(row["source"], -2, 3)
    data[0]["parameters"] = [{"name": "dose", "amount": -2}]
    data[0]["comment"] = "Synthetic copper-ash disposal fixture."
    converted = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.12")).migrate_background(
        target_context("native2026" if native else "2025"), policy=POLICY
    )
    restored = BrightwayInventory.from_excel(converted.write_excel(tmp_path / "copper-ash.xlsx"))
    assert not restored.validate().has_errors
    by_identity = {key(d): d for d in restored.data}
    for original in converted.data[1:]:
        final = by_identity[key(original)]
        assert [e["amount"] for e in final["exchanges"]] == [e["amount"] for e in original["exchanges"]]
        if any(e["type"] == "biosphere" and e["name"].startswith("Copper") for e in original["exchanges"]):
            assert sorted(
                (e["name"], tuple(e["categories"]), e["unit"], e["amount"])
                for e in final["exchanges"]
                if e["type"] == "biosphere"
            ) == sorted(
                (e["name"], tuple(e["categories"]), e["unit"], e["amount"])
                for e in original["exchanges"]
                if e["type"] == "biosphere"
            )


def test_simapro_refuses_unrepresented_indicator_without_data_loss(tmp_path):
    native = False
    from brightpath.exceptions import InventoryValidationError

    row = next(r for r in review.SOURCES.values() if r["source"]["name"].startswith("market"))
    data = consumer(row["source"], -2, 3)
    data[0]["comment"] = "Synthetic complete copper-ash fixture for SimaPro validation."
    data[0]["parameters"] = [{"name": "dose", "amount": -2}]
    converted = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.12")).migrate_background(
        target_context("native2026" if native else "2025"), policy=POLICY
    )
    categorized = deepcopy(converted.data)
    for dataset in categorized:
        dataset["exchanges"][0]["simapro category"] = "Materials/Services"
    inventory = BrightwayInventory.from_data(categorized, context=converted.context).to_simapro()
    original = deepcopy(inventory.data)
    expected = "groundwater, long-term" if native else "inventory indicator"
    destination = tmp_path / "unsupported-copper-ash.csv"
    with pytest.raises(InventoryValidationError, match=expected):
        inventory.write_csv(destination)
    assert inventory.data == original
    assert not destination.exists()
    assert any(
        e["type"] == "biosphere" and e["name"].startswith("Copper") for d in inventory.data for e in d["exchanges"]
    )


def test_native_copper_ash_hierarchy_survives_simapro_roundtrip(tmp_path):
    from brightpath import SimaProInventory

    row = next(r for r in review.SOURCES.values() if r["source"]["name"].startswith("market"))
    data = consumer(row["source"], -2, 3)
    data[0]["parameters"] = [{"name": "dose", "amount": -2}]
    data[0]["comment"] = "Synthetic native copper-ash roundtrip."
    converted = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.12")).migrate_background(
        target_context("native2026"), policy=POLICY
    )
    categorized = deepcopy(converted.data)
    for dataset in categorized:
        dataset["exchanges"][0]["simapro category"] = "Materials/Services"
    # Generated helpers supply positive treatment/transport services. Products
    # preserve that coordinate; native Waste treatment rows use a negative reference.
    original = deepcopy(categorized)
    source = BrightwayInventory.from_data(categorized, context=converted.context).to_simapro()
    path = source.write_csv(tmp_path / "native-copper-ash.csv")
    restored = SimaProInventory.from_csv(
        path,
        background_profile=converted.background_profile,
        biosphere_profile=converted.context.background.biosphere,
    ).to_brightway()
    assert not restored.validate().has_errors
    by_identity = {key(d): d for d in restored.data}
    for before in converted.data[1:]:
        after = by_identity[key(before)]
        assert [e["amount"] for e in after["exchanges"]] == [e["amount"] for e in before["exchanges"]]
        assert sorted(
            (e["name"], tuple(e["categories"]), e["unit"], e["amount"])
            for e in after["exchanges"]
            if e["type"] == "biosphere"
        ) == sorted(
            (e["name"], tuple(e["categories"]), e["unit"], e["amount"])
            for e in before["exchanges"]
            if e["type"] == "biosphere"
        )
    assert categorized == original
