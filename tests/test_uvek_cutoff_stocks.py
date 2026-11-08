"""Known cut-off stocks retain allocation boundaries, including inside exact-profile assemblies."""

import json
from copy import deepcopy
from pathlib import Path

import pytest
from test_uvek_reference_signs import FIELDS, consumer, contexts
from test_uvek_refrigerant_disposal import target_context

from brightpath import BrightwayInventory, TechnosphereProfile
from brightpath.background.reference_signs import select_profile_sign_rules
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource
from brightpath.migrations.schema import validate_migration_resource
from scripts.review_uvek_cutoff_stocks import APPROVAL, STOCKS, stock_decision, zero_supplier_binding
from scripts.review_uvek_market_compositions import COMPOSITIONS
from scripts.review_uvek_processing_compositions import PROCESSING

ROOT = Path(__file__).resolve().parents[1]
LABELS = [
    label
    for label in sorted({p["label"] for row in STOCKS.values() for p in row["profiles"]})
    if label.endswith("-cutoff")
]
ASSEMBLIES = {**COMPOSITIONS, **PROCESSING}
POLICY = MigrationPolicy.permissive()


def batch(label, amount):
    data = None
    for source, row in STOCKS.items():
        if label not in {p["label"] for p in row["profiles"]}:
            continue
        for kind, uncertainty in (("technosphere", 2), ("substitution", 3)):
            part = consumer(dict(zip(FIELDS, source, strict=True)), amount, uncertainty, kind)
            part[0]["exchanges"][1]["caller_metadata"] = {"stock": [1, 2]}
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


@pytest.mark.parametrize("label", LABELS)
@pytest.mark.parametrize("kind", ["2025", "native2026"])
@pytest.mark.parametrize("amount", [-10, 0, 10])
def test_all_inspected_stocks_keep_signed_materials_uncertainty_formulas_and_emissions(label, kind, amount):
    _, version, model = label.split("-")
    data = batch(label, amount)
    before = deepcopy(data)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", version, model))
    result = inventory.migrate_background(
        target_context(kind), policy=MigrationPolicy(on_heuristic_mapping=PolicyAction.WARN)
    )
    helpers = {h["code"]: h for h in result.data[1:]}
    assert len(helpers) == sum(label in {p["label"] for p in r["profiles"]} for r in STOCKS.values())
    for old, new in zip(before[0]["exchanges"][1:-1], result.data[0]["exchanges"][1:-1], strict=True):
        assert new["amount"] == old["amount"] and new["type"] == old["type"]
        assert new["formula"] == old["formula"] == "dose"
        for key in ("uncertainty type", "loc", "scale", "minimum", "maximum", "negative", "caller_metadata"):
            assert new.get(key) == old.get(key)
        stock = helpers[new["input"][1]]
        assert stock["brightpath_conversion"]["zero_burden"] is True
        assert stock["exchanges"] == [{**{f: stock[f] for f in FIELDS}, "type": "production", "amount": 1}]
    assert result.data[0]["exchanges"][0] == before[0]["exchanges"][0]
    assert result.data[0]["exchanges"][-1]["name"] == "Carbon dioxide, fossil"
    assert result.data[0]["exchanges"][-1]["amount"] == 0.5
    warnings = [i for i in result.last_migration_report.issues if i.code == "migration.zero_burden_supplier"]
    assert len(warnings) == len(before[0]["exchanges"]) - 2
    assert all(i.severity.value == "warning" for i in warnings)
    assert not any(
        loss.code == "migration.technosphere_information_loss" for loss in result.last_migration_report.losses
    )
    assert inventory.data == data == before
    assert result.migrate_background(target_context(kind)).data == result.data


def test_stock_scope_is_the_complete_inspected_positive_cutoff_allocation_set():
    assert len(STOCKS) == APPROVAL["source_count"] == 34
    assert sum(len(r["profiles"]) for r in STOCKS.values()) == APPROVAL["source_profile_count"] == 168
    from scripts.review_uvek_complete_patch_boundaries import CASES as COMPLETION_CASES
    from scripts.review_uvek_exact_patch_boundaries import CASES as PATCH_CASES

    resource = load_uvek_technosphere_resource()
    rules = {tuple(r["source"][f] for f in FIELDS): r for r in resource["replace"]}
    for source, row in STOCKS.items():
        assert "Recycled Content cut-off" in source[0]
        rule = rules[source]
        assert rule["mapping_method"] == "reviewed_cutoff_stock_recipe"
        for p in row["profiles"]:
            assert p["label"].endswith("-cutoff")
            assert p["total_exchanges"] == 1 and p["only_self_production"] is True and p["production_amount"] == 1
        assert {
            tuple(p["source_profile"][f] for f in ("family", "version", "system_model"))
            for p in rule["reference_flow_sign_review"]["profiles"]
        } == {tuple(p["label"].split("-")) for p in row["profiles"]} | {
            tuple(p["label"].split("-"))
            for p in [*PATCH_CASES.values(), *COMPLETION_CASES.values()]
            if p["kind"] == "stock" and tuple(p["source"][f] for f in FIELDS) == source
        }
    for version, model in [("3.12", "consequential"), ("3.12.1", "cutoff")]:
        if model == "consequential":
            with pytest.raises(ValueError, match="system models"):
                select_profile_sign_rules(resource, TechnosphereProfile("ecoinvent", version, model))
            continue
        selected = select_profile_sign_rules(resource, TechnosphereProfile("ecoinvent", version, model))
        assert all(
            r["reference_flow_sign_review"]["status"] == "unresolved"
            for r in selected["replace"]
            if tuple(r["source"][f] for f in FIELDS) in STOCKS
        )


@pytest.mark.parametrize("change", ["fingerprint", "amount", "self", "nonempty", "model", "observation", "target"])
def test_stock_approval_rejects_uninspected_or_changed_allocation_boundaries(change):
    source, row = next(iter(STOCKS.items()))
    original = {"source": row["source"], "target": deepcopy(row["previous_target"])}
    inventories = deepcopy(row["profiles"])
    observations = [(p["label"], 1) for p in inventories]
    if change == "fingerprint":
        inventories[0]["inventory_sha256"] = "0" * 64
    elif change == "amount":
        inventories[0]["production_amount"] = -1
    elif change == "self":
        inventories[0]["only_self_production"] = False
    elif change == "nonempty":
        inventories[0]["total_exchanges"] = 2
    elif change == "model":
        inventories[0]["label"] = inventories[0]["label"].replace("cutoff", "consequential")
    elif change == "observation":
        observations[0] = (observations[0][0], -1)
    else:
        original["target"]["name"] = "Unapproved disposal"
    with pytest.raises(ValueError, match="repeat the scientific review"):
        stock_decision(original, observations, inventories, "synthetic", None)


@pytest.fixture
def nested_stock_resource():
    # Reduce a real reviewed stock to a small resource, retaining every schema and allocation guard.
    resource = deepcopy(load_uvek_technosphere_resource())
    stock = next(r for r in resource["replace"] if r.get("mapping_method") == "reviewed_cutoff_stock_recipe")
    resource.update(replace=[stock], delete=[], disaggregate=[], coverage={})
    stock_id = stock["target"]["recipe_ref"]
    resource["recipes"] = {stock_id: resource["recipes"][stock_id]}
    parent = deepcopy(stock)
    parent.update(
        id="synthetic-parent", source={**stock["source"], "name": "Synthetic assembly"}, target={"recipe_ref": "parent"}
    )
    profile = deepcopy(stock["reference_flow_sign_review"]["profiles"][0])
    profile["target"] = {"recipe_ref": "parent"}
    parent["reference_flow_sign_review"]["profiles"] = [profile]
    resource["replace"].append(parent)
    resource["recipes"]["parent"] = {
        **{f: parent["source"][f] for f in FIELDS},
        "inputs": [
            {
                "recipe": stock_id,
                "amount": -0.75,
                "zero_burden_supplier": zero_supplier_binding(stock, profile["source_profile"], stock_id),
            }
        ],
        "rationale": "Synthetic assembly with inspected allocation supplier and exact signed quantity.",
        "evidence": ["synthetic"],
    }
    return resource


def test_nested_stock_is_valid_only_with_an_explicit_direct_source_approval(nested_stock_resource):
    validate_migration_resource(nested_stock_resource)


@pytest.mark.parametrize(
    "change",
    [
        "missing",
        "rule",
        "digest",
        "profile",
        "recipe",
        "literal",
        "root_model",
        "root_version",
        "default",
        "unprofiled",
        "indirect",
    ],
)
def test_nested_stock_never_inherits_an_unknown_profile_or_an_unbound_zero(nested_stock_resource, change):
    r = nested_stock_resource
    parent = r["replace"][-1]
    e = r["recipes"]["parent"]["inputs"][0]
    if change == "missing":
        e.pop("zero_burden_supplier")
    elif change == "rule":
        e["zero_burden_supplier"]["rule_id"] = "unapproved"
    elif change == "digest":
        e["zero_burden_supplier"]["profile_review_sha256"] = "0" * 64
    elif change == "profile":
        e["zero_burden_supplier"]["source_profile"]["system_model"] = "consequential"
    elif change == "recipe":
        r["recipes"]["nonzero"] = {
            **deepcopy(r["recipes"]["parent"]),
            "inputs": [
                {
                    "type": "biosphere",
                    "name": "Carbon dioxide, fossil",
                    "categories": ["air"],
                    "unit": "kilogram",
                    "amount": 1,
                }
            ],
        }
        e["recipe"] = "nonzero"
    elif change == "literal":
        e.update(type="biosphere", name="Carbon dioxide, fossil", categories=["air"], unit="kilogram")
        e.pop("recipe")
    elif change in ("root_model", "root_version", "indirect"):
        if change == "indirect":
            r["recipes"]["outer"] = {**deepcopy(r["recipes"]["parent"]), "inputs": [{"recipe": "parent", "amount": 1}]}
            parent["target"] = {"recipe_ref": "outer"}
            parent["reference_flow_sign_review"]["profiles"][0]["target"] = {"recipe_ref": "outer"}
        p = parent["reference_flow_sign_review"]["profiles"][0]["source_profile"]
        p["system_model" if change != "root_version" else "version"] = (
            "consequential" if change != "root_version" else "3.12"
        )
        # A few stocks begin in 3.12; choose another declared version in that case.
        if p == e["zero_burden_supplier"]["source_profile"]:
            p["version"] = "3.6"
    elif change == "default":
        parent["reference_flow_sign_review"].update(status="verified", source_sign=1, target_sign=1)
    else:
        parent.pop("reference_flow_sign_review")
    with pytest.raises(MigrationError):
        validate_migration_resource(r)


def test_pending_assembly_counterparts_remain_visible_in_queue_and_cannot_execute():
    from scripts.review_uvek_additional_cutoff_waste_markets import SOURCES as ADDITIONAL_MARKETS
    from scripts.review_uvek_industrial_waste_markets import SOURCES as INDUSTRIAL_MARKETS
    from scripts.review_uvek_steel_residue_markets import SOURCES as STEEL_MARKETS

    # Later independent reviews add exact patch profiles to these assemblies.
    # Compare with the current scientific evidence, including every held profile.
    current_approvals = ASSEMBLIES | STEEL_MARKETS | INDUSTRIAL_MARKETS | ADDITIONAL_MARKETS
    resource = load_uvek_technosphere_resource()
    rules = {tuple(r["source"][f] for f in FIELDS): r for r in resource["replace"]}
    queue = json.loads((ROOT / "docs/uvek_forward_review_queue.json").read_text())
    pending_ids = {r["id"] for r in queue["rules"] if r["review_state"] == "partially_approved"}
    count = 0
    for source, original_approval in ASSEMBLIES.items():
        if not any(p["label"].endswith("-cutoff") for p in original_approval.get("pending_profiles", [])):
            continue
        approval = current_approvals[source]
        pending = [p for p in approval.get("pending_profiles", []) if p["label"].endswith("-cutoff")]
        if not pending:
            continue
        count += 1
        rule = rules[source]
        assert rule["id"] in pending_ids
        entries = rule["reference_flow_sign_review"]["profiles"]
        expected_states = {p["label"]: "verified" for p in approval["profiles"] if p["label"].endswith("-cutoff")} | {
            p["label"]: "unresolved" for p in pending
        }
        assert {
            "-".join(e["source_profile"][f] for f in ("family", "version", "system_model")): e["review"]["status"]
            for e in entries
        } == expected_states
        for p in pending:
            profile = TechnosphereProfile(*p["label"].split("-"))
            entry = next(
                e
                for e in entries
                if e["source_profile"]
                == dict(zip(("family", "version", "system_model"), p["label"].split("-"), strict=True))
            )
            assert entry["review"]["status"] == "unresolved"
            assert "target" not in entry and "conversion_factor" not in entry
            selected = select_profile_sign_rules({"replace": [rule]}, profile)["replace"][0]
            assert selected["reference_flow_sign_review"]["status"] == "unresolved"
    assert count == 10


@pytest.mark.parametrize("model,version", [("consequential", "3.12"), ("cutoff", "3.12.1")])
def test_uninspected_stock_profiles_fail_transactionally_and_foreground_ownership_is_preserved(model, version):
    data = batch("ecoinvent-3.12-cutoff", -1)
    original = deepcopy(data)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", version, model))
    with pytest.raises(MigrationError):
        inventory.migrate_background(target_context("2025"), policy=POLICY)
    assert inventory.data == data == original
    for source, approval in STOCKS.items():
        if "ecoinvent-3.12-cutoff" in {p["label"] for p in approval["profiles"]}:
            supplier = dict(zip(FIELDS, source, strict=True))
            data.append({**supplier, "exchanges": [{**supplier, "type": "production", "amount": 1}]})
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.12")).migrate_background(
        target_context("2025"), policy=POLICY
    )
    assert result.data == data


@pytest.mark.parametrize("kind", ["2025", "native2026"])
def test_nested_helpers_keep_stock_warnings_and_export_without_internal_review_annotations(kind, tmp_path):
    source, approval = next(
        (s, a)
        for s, a in ASSEMBLIES.items()
        if any("zero_burden_supplier" in e for p in a["profiles"] for e in p["inputs"])
    )
    p = next(p for p in approval["profiles"] if any("zero_burden_supplier" in e for e in p["inputs"]))
    _, version, model = p["label"].split("-")
    data = consumer(approval["source"], -3, 3)
    data[0]["comment"] = "Nested cut-off stock export regression."
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", version, model))
    result = inventory.migrate_background(target_context(kind), policy=POLICY)
    stocks = [h for h in result.data[1:] if h["brightpath_conversion"].get("zero_burden")]
    assert stocks and all(len(h["exchanges"]) == 1 for h in stocks)
    assert any(i.code == "migration.zero_burden_supplier" for i in result.last_migration_report.issues)
    assert all("zero_burden_supplier" not in e for h in result.data for e in h["exchanges"])
    path = tmp_path / f"stocks-{kind}.xlsx"
    result.write_excel(path)
    assert path.is_file() and path.stat().st_size > 0
    restored = BrightwayInventory.from_excel(path)
    assert len(restored.data) == len(result.data)
    assert sorted(len(d["exchanges"]) for d in restored.data) == sorted(len(d["exchanges"]) for d in result.data)
    before = deepcopy(result.data)
    # Returned foreground suppliers stay foreground in the independently checked reverse direction.
    if kind == "2025":
        with pytest.raises(MigrationError) as caught:
            result.migrate_background(contexts("ecoinvent", "3.12").background, policy=POLICY)
        assert any(i.code == "migration.uvek_supplier_unresolved" for i in caught.value.report.issues)
        assert result.data == before


@pytest.mark.parametrize(
    "label",
    sorted(
        {
            p["label"]
            for a in ASSEMBLIES.values()
            for p in a.get("pending_profiles", [])
            if p["label"].endswith("-cutoff")
        }
    ),
)
def test_pending_assembly_profiles_fail_transactionally_even_with_permissive_policy(label):
    data = None
    for _source, approval in ASSEMBLIES.items():
        if label not in {p["label"] for p in approval.get("pending_profiles", [])}:
            continue
        part = consumer(approval["source"], -2, 3)
        if data is None:
            data = part
        else:
            data[0]["exchanges"].append(part[0]["exchanges"][1])
    before = deepcopy(data)
    _, version, model = label.split("-")
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", version, model))
    with pytest.raises(MigrationError) as caught:
        inventory.migrate_background(target_context("2025"), policy=POLICY)
    assert any(i.code == "migration.reference_flow_sign_unresolved" for i in caught.value.report.issues)
    assert inventory.data == data == before


@pytest.mark.parametrize("amount", [-3, 3])
def test_negative_cutoff_stock_input_never_creates_a_false_disposal_credit(amount):
    resource = load_uvek_technosphere_resource()
    source, approval, p = next(
        (s, a, p)
        for s, a in ASSEMBLIES.items()
        for p in a["profiles"]
        if any(e.get("zero_burden_supplier") and e["amount"] < 0 for e in p["inputs"])
        and all("recipe" not in e or resource["recipes"][e["recipe"]].get("zero_burden") for e in p["inputs"])
        and any("recipe" not in e and e["amount"] != 0 for e in p["inputs"])
    )
    _, version, model = p["label"].split("-")
    result = BrightwayInventory.from_data(
        consumer(approval["source"], amount), context=contexts("ecoinvent", version, model)
    ).migrate_background(target_context("2025"), policy=POLICY)
    helpers = {h["code"]: h for h in result.data[1:]}

    by_identity = {tuple(h[f] for f in FIELDS): h for h in helpers.values()}

    def score(helper):
        # Synthetic factors test signs and the allocation boundary, not actual LCIA values.
        return sum(
            e["amount"]
            * (score(by_identity[tuple(e[f] for f in FIELDS)]) if tuple(e[f] for f in FIELDS) in by_identity else 2)
            for e in helper["exchanges"]
            if e["type"] != "production"
        )

    parent = helpers[result.data[0]["exchanges"][1]["input"][1]]
    expected = amount * p["source_sign"] * sum(2 * e["amount"] for e in p["inputs"] if "recipe" not in e)
    assert result.data[0]["exchanges"][1]["amount"] * score(parent) == pytest.approx(expected)
    assert any(e["amount"] < 0 and tuple(e[f] for f in FIELDS) in by_identity for e in parent["exchanges"])
    assert all(score(h) == 0 for h in helpers.values() if h["brightpath_conversion"].get("zero_burden"))


@pytest.mark.parametrize("change", ["missing_pending", "pending_hash", "pending_sign"])
def test_partial_assembly_author_requires_every_observed_counterpart_inventory(change):
    from scripts.review_uvek_market_compositions import composition_decision

    source, approval = next((s, a) for s, a in COMPOSITIONS.items() if a.get("pending_profiles"))
    all_profiles = [*approval["profiles"], *approval["pending_profiles"]]
    observations = [(p["label"], p["source_sign"]) for p in all_profiles]
    inventories = [(p["label"], p["inventory_sha256"]) for p in all_profiles]
    pending_index = len(approval["profiles"])
    if change == "missing_pending":
        observations.pop(pending_index)
        inventories.pop(pending_index)
    elif change == "pending_hash":
        inventories[pending_index] = (inventories[pending_index][0], "0" * 64)
    else:
        observations[pending_index] = (observations[pending_index][0], -observations[pending_index][1])
    with pytest.raises(ValueError, match="repeat the scientific review"):
        composition_decision(
            {"source": approval["source"], "target": approval["previous_target"]},
            observations,
            inventories,
            "synthetic",
            None,
        )


@pytest.mark.parametrize("kind", ["2025", "native2026"])
def test_reviewed_stocks_remain_owned_foreground_in_the_independent_reverse_direction(kind):
    result = BrightwayInventory.from_data(
        batch("ecoinvent-3.12-cutoff", -3), context=contexts("ecoinvent", "3.12")
    ).migrate_background(target_context(kind), policy=POLICY)
    before = deepcopy(result.data)
    back = result.migrate_background(contexts("ecoinvent", "3.12").background, policy=POLICY)
    assert back.data[1:] == result.data[1:]
    assert all(h["brightpath_conversion"]["zero_burden"] for h in back.data[1:])
    assert result.data == before
