"""Keep Swiss concrete on its inspected inert-landfill route with preserved caller coordinates."""

import json
from collections import Counter
from copy import deepcopy
from pathlib import Path

import pytest
from test_uvek_reference_signs import consumer, contexts
from test_uvek_refrigerant_disposal import target_context

from brightpath import BrightwayInventory
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.migrations.schema import intern_review, rule_review, validate_migration_resource
from scripts import review_uvek_waste_concrete_inert as review
from scripts.review_uvek_venting import compiled_venting_rule

R = Path(__file__).resolve().parents[1]
F = ("name", "reference product", "location", "unit")


def key(row):
    return tuple(row.get(field) for field in F)


DRAFT = {
    "parent_rows": [
        {"source": row["source"], "profile": profile} for row in review.SOURCES.values() for profile in row["profiles"]
    ],
    "helper_bodies": 12,
}
BASE = json.loads((R / "brightpath/data/migrations/uvek/ecoinvent-to-uvek-2025.json").read_text())
POLICY = MigrationPolicy(on_heuristic_mapping=PolicyAction.WARN, on_information_loss=PolicyAction.WARN)


@pytest.fixture(scope="module")
def corrected():
    if all(
        r["mapping_method"] == "reviewed_waste_concrete_inert_reconstruction"
        for r in BASE["replace"]
        if key(r["source"]) in review.SOURCES
    ):
        validate_migration_resource(BASE)
        # Per-profile assertions cover this cohort; unrelated reviewed helpers may be appended.
        return deepcopy(BASE)
    resource = deepcopy(BASE)
    changed = []
    for rule in resource["replace"]:
        rows = [r for r in DRAFT["parent_rows"] if key(r["source"]) == key(rule["source"])]
        if not rows:
            continue
        previous = json.loads((R / "brightpath/data/export/uvek_reference_flow_sign_review.json").read_text())[
            "resources"
        ][BASE["name"]][rule["id"]]
        decision = review.forward_decision(
            rule,
            {(r["profile"]["label"], -1) for r in rows},
            previous["binding"],
            deepcopy(rule_review(resource, rule)),
        )
        changed.append(rule["id"])
        rule.clear()
        rule.update(compiled_venting_rule(decision))
        resource["recipes"].update(deepcopy(decision["recipes"]))
        intern_review(resource, decision["recipe_review"])
    assert len(changed) == 2
    resource["coverage"]["mapping_methods"] = dict(
        sorted(Counter(r["mapping_method"] for r in resource["replace"]).items())
    )
    resource["coverage"]["mean_confidence"] = round(
        sum(r["confidence"] for r in resource["replace"]) / len(resource["replace"]), 6
    )
    dispositions = Counter(
        "recipe" if "recipe_ref" in r["target"] else r.get("classification", "unclassified")
        for r in resource["replace"]
    )
    dispositions["omitted"] = len(resource["delete"])
    if resource["coverage"].get("unresolved_identities"):
        dispositions["unresolved"] = resource["coverage"]["unresolved_identities"]
    resource["coverage"]["dispositions"] = dict(dispositions)
    validate_migration_resource(resource)
    assert all(resource["recipes"][rid] == body for rid, body in BASE["recipes"].items())
    assert len(resource["recipes"]) == len(BASE["recipes"]) + DRAFT["helper_bodies"]
    before = {r["id"]: r for r in BASE["replace"]}
    assert all(r == before[r["id"]] for r in resource["replace"] if r["id"] not in changed)
    return resource


@pytest.fixture
def injected(monkeypatch, corrected):
    import brightpath.background.execution as execution
    import brightpath.background.migration as migration
    import brightpath.background.uvek_import as uvek_import
    import brightpath.migrations.resources as resources

    for module in (execution, migration, uvek_import, resources):
        monkeypatch.setattr(module, "load_uvek_technosphere_resource", lambda: deepcopy(corrected))


@pytest.mark.parametrize("row", DRAFT["parent_rows"])
@pytest.mark.parametrize("release", ("2025", "native2026"))
@pytest.mark.parametrize("amount", (-2, 0, 2))
def test_wet_concrete_follows_source_inert_route_with_exact_collection_and_signed_quantities(
    injected, row, release, amount
):
    version = row["profile"]["label"].split("-")[1]
    data = consumer(row["source"], amount, 2)
    data[0]["exchanges"][1]["caller_metadata"] = {"preserve": [1, 2]}
    before = deepcopy(data)
    source = BrightwayInventory.from_data(data, context=contexts("ecoinvent", version))
    result = source.migrate_background(target_context(release), policy=POLICY)
    assert source.data == data == before
    exchange = result.data[0]["exchanges"][1]
    original = before[0]["exchanges"][1]
    assert exchange["amount"] == -amount and exchange["formula"] == "(dose) * (-1.0)"
    assert exchange["scale"] == original["scale"] and exchange["loc"] == original["loc"]
    assert (
        exchange["negative"] is not original["negative"] and exchange["caller_metadata"] == original["caller_metadata"]
    )
    helpers = {d["code"]: d for d in result.data[1:]}
    root = helpers[exchange["input"][1]]
    if row["source"]["name"].startswith("market"):
        inputs = [x for x in root["exchanges"] if x["type"] != "production"]
        assert len(inputs) == 3 and sorted(x["amount"] for x in inputs) == [0.00356, 0.02577, 1]
        (leaf_link,) = [x for x in inputs if x["unit"] == "kilogram"]
        (leaf,) = [d for d in helpers.values() if key(d) == key(leaf_link)]
    else:
        leaf = root
    tech = [x for x in leaf["exchanges"] if x["type"] == "technosphere"]
    assert len(tech) == 2 and sorted(x["amount"] for x in tech) == [1.48e-9, 1]
    assert {x["name"] for x in tech} == {
        "xx Inert material landfill facility",
        "Process-specific burdens, inert material landfill",
    }
    bio = [x for x in leaf["exchanges"] if x["type"] == "biosphere"]
    assert len(bio) == (1 if version == "3.12" and release == "2025" else 0)
    assert all(x["name"] == "Non-hazardous waste disposed" and x["amount"] == 1 for x in bio)
    assert not any(
        "crusher" in x["name"].lower()
        or "demolition" in x["name"].lower()
        or x["name"] == "Disposal, concrete, as building waste"
        for d in helpers.values()
        for x in d["exchanges"]
    )


@pytest.mark.parametrize("change", ("inventory", "metadata", "reference", "profile"))
def test_stale_full_boundary_witnesses_refuse(change):
    from collections import defaultdict

    records = deepcopy(review.APPROVAL["inventories"])
    observations = defaultdict(lambda: defaultdict(set))
    review.validate_inventories(records, observations, {})
    if change == "inventory":
        records[0]["inventory_sha256"] = "0" * 64
    elif change == "metadata":
        records[0]["metadata_sha256"] = "0" * 64
    elif change == "reference":
        records[0]["reference_amount"] = 1
    else:
        source = key(next(iter(review.SOURCES.values()))["source"])
        observations["ecoinvent"][source].add(("ecoinvent-3.13-cutoff", -1))
    with pytest.raises(ValueError, match="Waste-concrete"):
        review.validate_inventories(records, observations, {})


def test_complete_component_and_collection_bindings_are_frozen():
    review.validate_bindings(BASE)
    resource = deepcopy(BASE)
    source = next(
        tuple(b["source"])
        for row in review.SOURCES.values()
        for p in row["profiles"]
        for b in p["bindings"]
        if "source" in b
    )
    (child,) = [r for r in resource["replace"] if key(r["source"]) == source]
    child["confidence"] = 0.123
    with pytest.raises(ValueError, match="binding changed"):
        review.validate_bindings(resource)


def test_whole_function_review_has_no_inferred_lifetime_or_reverse():
    assert len(review.APPROVAL["inventories"]) == 38
    assert {r["reference_amount"] for r in review.APPROVAL["inventories"]} == {-1, 1}
    assert (
        review.APPROVAL["component_review"]["native_use_and_aftercare_durations"]
        == "not explicitly documented; no equal-lifetime claim"
    )
    assert review.APPROVAL["component_review"]["new_user_approval_inferred"] is False
    assert "no density, capacity, moisture or lifetime scaling" in review.LIMITATIONS.lower()


def test_exact_authoring_decisions_replay_without_fallback():
    ledger = json.loads((R / "brightpath/data/export/uvek_reference_flow_sign_review.json").read_text())
    decisions = ledger["resources"][BASE["name"]]
    for row in review.SOURCES.values():
        previous = decisions[row["id"]]
        replayed = review.forward_decision(
            previous["original_rule"],
            {(p["label"], -1) for p in row["profiles"]},
            previous["binding"],
            previous["original_review"],
        )
        assert replayed == previous
        assert len(replayed["review"]["profiles"]) == 9
        assert replayed["factor"] == -1 and replayed["original_factor"] == 1
        assert "PENDING" not in json.dumps(replayed)


@pytest.mark.parametrize("row", list(review.SOURCES.values()))
def test_native_hierarchy_roundtrips_and_reports_indicator_omission(injected, row, tmp_path):
    from brightpath import SimaProInventory

    data = consumer(row["source"], -2, 3)
    data[0]["parameters"] = [{"name": "dose", "amount": -2}]
    data[0]["comment"] = "Synthetic native waste-concrete roundtrip."
    converted = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.12")).migrate_background(
        target_context("native2026"), policy=POLICY
    )
    issues = converted.last_migration_report.issues
    assert len([i for i in issues if i.code == "migration.recipe_input_omission"]) == 1
    assert any(i.code == "migration.technosphere_information_loss" for i in issues)
    categorized = deepcopy(converted.data)
    for dataset in categorized:
        dataset["exchanges"][0]["simapro category"] = "Materials/Services"
    source = BrightwayInventory.from_data(categorized, context=converted.context).to_simapro()
    before = deepcopy(source.data)
    path = source.write_csv(tmp_path / "native-waste-concrete.csv")
    assert source.data == before
    restored = SimaProInventory.from_csv(
        path,
        background_profile=converted.background_profile,
        biosphere_profile=converted.context.background.biosphere,
    ).to_brightway()
    assert not restored.validate().has_errors
    by_identity = {key(d): d for d in restored.data}
    for helper in converted.data[1:]:
        assert [e["amount"] for e in by_identity[key(helper)]["exchanges"]] == [
            e["amount"] for e in helper["exchanges"]
        ]
