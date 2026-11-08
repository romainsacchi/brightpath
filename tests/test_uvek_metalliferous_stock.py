"""Production-only sludge stock cannot become primary lithium production or a credit."""

import pickle
from collections import defaultdict
from copy import deepcopy

import pytest
from test_uvek_reference_signs import FIELDS, consumer, contexts
from test_uvek_refrigerant_disposal import target_context

from brightpath import BrightwayInventory, TechnosphereProfile
from brightpath.background.reference_signs import select_profile_sign_rules
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource
from scripts import review_uvek_metalliferous_stock as review

POLICY = MigrationPolicy(on_heuristic_mapping=PolicyAction.WARN)
VERSIONS = [p["label"].split("-")[1] for p in review.APPROVAL["profiles"]]


def evidence():
    records = deepcopy(review.APPROVAL["inventories"])
    observations = defaultdict(lambda: defaultdict(set))
    review.extend_observations(records, observations)
    return records, observations


@pytest.mark.parametrize("version", VERSIONS)
@pytest.mark.parametrize("release", ("2025", "2026", "native2026"))
@pytest.mark.parametrize("amount", (-2, 0, 2))
def test_exact_stock_preserves_signed_caller_state_and_separate_emissions(version, release, amount):
    source = dict(zip(FIELDS, review.SOURCE, strict=True))
    data = consumer(source, amount, 2)
    data[0]["exchanges"].append(consumer(source, amount, 3, "substitution")[0]["exchanges"][1])
    for e in data[0]["exchanges"][1:]:
        e["caller_metadata"] = {"preserve": [1, 2]}
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
    assert len(result.data) == 2
    helper = result.data[1]
    assert helper["brightpath_conversion"]["zero_burden"] is True
    assert helper["exchanges"] == [{**{f: helper[f] for f in FIELDS}, "type": "production", "amount": 1}]
    for old, new in zip(before[0]["exchanges"][1:-1], result.data[0]["exchanges"][1:-1], strict=True):
        assert new["input"][1] == helper["code"]
        assert new["amount"] == old["amount"] == amount
        assert new["type"] == old["type"]
        for field in (
            "formula",
            "uncertainty type",
            "loc",
            "scale",
            "minimum",
            "maximum",
            "negative",
            "caller_metadata",
        ):
            assert new.get(field) == old.get(field)
    assert result.data[0]["exchanges"][0] == before[0]["exchanges"][0]
    assert result.data[0]["exchanges"][-1]["amount"] == 0.5
    warnings = [i for i in result.last_migration_report.issues if i.code == "migration.zero_burden_supplier"]
    assert len(warnings) == 2 and all(i.severity.value == "warning" for i in warnings)
    assert all("production-only" in i.message for i in warnings)
    assert not any(i.code.endswith("link_unresolved") for i in result.last_migration_report.issues)
    assert not any(i.code == "migration.technosphere_omission" for i in result.last_migration_report.losses)
    assert inventory.data == data == before
    assert result.migrate_background(target_context(release)).data == result.data


@pytest.mark.parametrize(
    "mutation", ("missing", "duplicate", "inventory", "description", "amount", "extra_profile", "reverse")
)
def test_changed_inventory_or_exact_scope_cannot_inherit_empty_stock_approval(mutation, monkeypatch, tmp_path):
    records, observations = evidence()
    review.validate_inventories(records, observations)
    if mutation == "missing":
        records.pop()
    elif mutation == "duplicate":
        records[-1] = records[0]
    elif mutation == "inventory":
        records[0]["inventory_sha256"] = "0" * 64
    elif mutation == "description":
        records[0]["activity_comment_sha256"] = "0" * 64
    elif mutation == "amount":
        records[0]["production_amount"] = -1
    elif mutation == "extra_profile":
        observations["ecoinvent"][review.SOURCE].add(("ecoinvent-3.12.1-cutoff", 1))
    else:
        path = tmp_path / review.APPROVAL["independent_reverse"]["resource_path"]
        path.parent.mkdir(parents=True)
        path.write_text("changed")
        monkeypatch.setattr(review, "ROOT", tmp_path)
    with pytest.raises(ValueError, match="changed"):
        review.validate_inventories(records, observations)


@pytest.mark.parametrize("mutation", ("negative", "nonempty", "not_self"))
def test_collector_never_zeros_negative_empty_treatment_or_nonempty_supply(mutation):
    link = ("test", "stock")
    row = {"type": "production", "amount": 1, "input": link, "output": link}
    rows = [row]
    if mutation == "negative":
        row["amount"] = -1
    elif mutation == "nonempty":
        rows.append({"type": "biosphere", "amount": 1})
    else:
        row["input"] = ("test", "other")
    with pytest.raises(ValueError, match="allocation boundary changed"):
        review.inventory_record(
            "ecoinvent-3.12-cutoff", dict(zip(FIELDS, review.SOURCE, strict=True)), [pickle.dumps(x) for x in rows]
        )


@pytest.mark.parametrize("mutation", ("target", "factor", "profile", "stock_name"))
def test_projection_requires_original_supplier_unit_factor_and_explicit_stock(mutation):
    records, observations = evidence()
    rule = {"source": deepcopy(review.APPROVAL["source"]), "target": deepcopy(review.APPROVAL["previous_target"])}
    if mutation == "target":
        rule["target"]["name"] = "A different production process"
    elif mutation == "factor":
        rule["conversion_factor"] = -1
    elif mutation == "profile":
        observations["ecoinvent"][review.SOURCE].add(("ecoinvent-3.12.1-cutoff", 1))
    else:
        rule["source"]["name"] = "Uncharacterized sludge"
    with pytest.raises(ValueError, match="repeat the scientific review"):
        review.forward_decision(rule, observations["ecoinvent"][review.SOURCE], records, "test binding", None)


def test_foreground_ownership_unknown_patch_and_cross_model_refusal():
    source = dict(zip(FIELDS, review.SOURCE, strict=True))
    data = consumer(source, -2, 3)
    for version, model in (("3.12", "consequential"), ("3.9.2", "cutoff")):
        inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", version, model))
        with pytest.raises(MigrationError):
            inventory.migrate_background(target_context("2025"), policy=MigrationPolicy.permissive())
        assert inventory.data == data
    selected = select_profile_sign_rules(
        load_uvek_technosphere_resource(), TechnosphereProfile("ecoinvent", "3.12.1", "cutoff")
    )
    rule = next(r for r in selected["replace"] if review.identity(r["source"]) == review.SOURCE)
    assert rule["reference_flow_sign_review"]["status"] == "unresolved"
    local = {**source, "exchanges": [{**source, "type": "production", "amount": 1}]}
    data.append(local)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.12"))
    assert inventory.migrate_background(target_context("2025"), policy=POLICY).data == data


@pytest.mark.parametrize("version", VERSIONS)
@pytest.mark.parametrize("release", ("2025", "native2026"))
@pytest.mark.parametrize("amount", (-2, 0, 2))
def test_source_traced_market_stock_substitution_keeps_all_four_original_freight_services(version, release, amount):
    source = {
        "name": "market for metalliferous hydroxide sludge",
        "reference product": "metalliferous hydroxide sludge",
        "location": "GLO",
        "unit": "kilogram",
    }
    data = consumer(source, amount, 3)
    original = deepcopy(data)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", version))
    result = inventory.migrate_background(
        target_context(release),
        policy=MigrationPolicy(on_heuristic_mapping=PolicyAction.WARN, on_information_loss=PolicyAction.WARN),
    )
    assert len(result.data) == 3
    exchange = result.data[0]["exchanges"][1]
    assert exchange["amount"] == amount
    for field in ("formula", "uncertainty type", "loc", "scale", "minimum", "maximum"):
        assert exchange[field] == original[0]["exchanges"][1][field]
    parent = next(h for h in result.data[1:] if h["code"] == exchange["input"][1])
    (stock,) = [h for h in result.data[1:] if h["brightpath_conversion"].get("zero_burden")]
    assert len(stock["exchanges"]) == 1
    assert len(parent["exchanges"]) == 6
    freight = [e for e in parent["exchanges"] if e["unit"] == "ton kilometer"]
    # Independently inspected complete source vectors have these four tkm amounts.
    assert sorted(e["amount"] for e in freight) == [0.0199, 0.112, 0.1932, 0.2115]
    (material,) = [e for e in parent["exchanges"] if e["type"] == "technosphere" and e["unit"] == "kilogram"]
    assert all(material[f] == stock[f] for f in FIELDS) and material["amount"] == 1
    assert not any("Lithium hydroxide" in e["name"] for h in result.data for e in h["exchanges"])
    assert any(i.code == "migration.zero_burden_supplier" for i in result.last_migration_report.issues)
    assert not any(i.code.endswith("link_unresolved") for i in result.last_migration_report.issues)
    assert inventory.data == data == original


@pytest.mark.parametrize("mutation", ("rule", "parent_recipe", "stock_recipe", "prior_binding", "unrelated_parent"))
def test_later_stock_review_cannot_bypass_other_freight_or_recipe_guards(mutation):
    from scripts.review_uvek_cutoff_stocks import stock_recipe
    from scripts.review_uvek_whole_freight import APPROVAL as FREIGHT_APPROVAL

    resource = load_uvek_technosphere_resource()
    entry = next(p for p in review.APPROVAL["parent_rule_bindings"] if p["label"] == "ecoinvent-3.12-cutoff")
    previous = deepcopy(
        next(
            p
            for p in FREIGHT_APPROVAL["parent_rule_bindings"]
            if p["label"] == entry["label"] and p["source"] == entry["source"]
        )
    )
    selected = select_profile_sign_rules(resource, TechnosphereProfile("ecoinvent", "3.12", "cutoff"))
    rule = deepcopy(next(r for r in selected["replace"] if r["source"] == entry["source"]))
    assert review.accepts_revised_parent_binding(resource, previous, rule)
    changed = {**resource, "recipes": dict(resource["recipes"])}
    if mutation == "rule":
        rule["conversion_factor"] = 2
    elif mutation == "parent_recipe":
        recipe = deepcopy(changed["recipes"][entry["recipe_id"]])
        recipe["inputs"][0]["amount"] *= 2
        changed["recipes"][entry["recipe_id"]] = recipe
    elif mutation == "stock_recipe":
        rid, _ = stock_recipe(review.SOURCE, review_path=review.REVIEW_PATH)
        recipe = deepcopy(changed["recipes"][rid])
        recipe["inputs"] = [
            {
                "type": "biosphere",
                "name": "Carbon dioxide, fossil",
                "categories": ["air"],
                "unit": "kilogram",
                "amount": 1,
            }
        ]
        changed["recipes"][rid] = recipe
    elif mutation == "prior_binding":
        previous["binding_sha256"] = "0" * 64
    else:
        previous["source"]["name"] = "Unrelated genuine lithium hydroxide production"
    if mutation in ("prior_binding", "unrelated_parent"):
        assert not review.accepts_revised_parent_binding(changed, previous, rule)
    else:
        with pytest.raises(ValueError, match="changed"):
            review.accepts_revised_parent_binding(changed, previous, rule)
