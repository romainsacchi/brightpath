"""Packed stone wool retains complete manufacture, each original delivery coefficient and explicit reverse boundaries."""

from copy import deepcopy

import pytest
from test_uvek_reference_signs import consumer, contexts
from test_uvek_refrigerant_disposal import target_context

from brightpath import (
    BackgroundContext,
    BiosphereProfile,
    BrightwayInventory,
    FormatProfile,
    InventoryContext,
    TechnosphereProfile,
)
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from scripts import review_uvek_packed_wool as wool
from scripts import review_uvek_whole_freight as freight

POLICY = MigrationPolicy(on_heuristic_mapping=PolicyAction.WARN, on_information_loss=PolicyAction.WARN)
SOURCE = wool.APPROVAL["rules"][0]["source"]
VERSIONS = [p["label"].split("-")[1] for p in wool.APPROVAL["rules"][0]["profiles"]]


@pytest.mark.parametrize("version", VERSIONS)
@pytest.mark.parametrize("release", ("2025", "2026", "native2026"))
@pytest.mark.parametrize("amount", (-2, 0, 2))
def test_complete_packed_product_and_each_delivery_coefficient_survive_actual_migration(version, release, amount):
    data = consumer(SOURCE, amount, 2)
    data[0]["comment"] = "Synthetic complete packed-wool consumer."
    data[0]["exchanges"].append(consumer(SOURCE, amount, 3, "substitution")[0]["exchanges"][1])
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
    assert helper["brightpath_conversion"]["recipe_id"].startswith("packed-wool-market-")
    assert helper["exchanges"][0]["type"] == "production" and helper["exchanges"][0]["amount"] == 1
    actual = helper["exchanges"][1:]
    expected = next(
        p["inputs"] for p in wool.APPROVAL["rules"][0]["profiles"] if p["label"] == f"ecoinvent-{version}-cutoff"
    )
    assert len(actual) == len(expected) == 6
    for a, b in zip(actual, expected, strict=True):
        assert {f: a[f] for f in wool.FIELDS} == {f: b[f] for f in wool.FIELDS}
        assert a["amount"] == b["amount"] and a["type"] == b["type"] == "technosphere"
    wool_inputs = [e for e in actual if e["unit"] == "kilogram"]
    assert len(wool_inputs) == 2 and all(wool.identity(e) == wool.TARGET for e in wool_inputs)
    assert sum(e["amount"] for e in wool_inputs) == pytest.approx(1)
    assert len([e for e in actual if e["unit"] == "ton kilometer"]) == 4
    assert not any(e["name"] == "Glass wool mat, at plant" for e in actual)
    for a, b in zip(before[0]["exchanges"][1:-1], result.data[0]["exchanges"][1:-1], strict=True):
        assert b["amount"] == amount and b["input"][1] == helper["code"]
        assert all(b.get(k) == v for k, v in a.items() if k not in wool.FIELDS)
    assert result.data[0]["exchanges"][-1]["amount"] == 0.5
    assert not any(i.code == "migration.zero_burden_supplier" for i in result.last_migration_report.issues)
    assert inventory.data == data == before
    assert result.migrate_background(target_context(release)).data == result.data


@pytest.mark.parametrize("release", ("2025", "native2026"))
def test_consent_foreground_ownership_and_consequential_refusal_remain_explicit(release):
    data = consumer(SOURCE, -2, 3)
    original = deepcopy(data)
    for model, policy in (("cutoff", MigrationPolicy.strict()), ("consequential", MigrationPolicy.permissive())):
        inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.12", model))
        with pytest.raises(MigrationError):
            inventory.migrate_background(target_context(release), policy=policy)
        assert inventory.data == original
    data.append({**SOURCE, "exchanges": [{**SOURCE, "type": "production", "amount": 1}]})
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.12"))
    assert inventory.migrate_background(target_context(release), policy=POLICY).data == data


@pytest.mark.parametrize("kind", ("light", "wool"))
@pytest.mark.parametrize("release", ("2025", "2026", "native2026"))
@pytest.mark.parametrize("amount", (-2, 0, 2))
def test_independently_reviewed_reverse_pair_preserves_reference_basis_and_caller_state(kind, release, amount):
    rule = freight.light_reverse_addition()[0] if kind == "light" else wool.reverse_addition()[0]
    data = consumer(rule["source"], amount, 2)
    data[0]["exchanges"].append(consumer(rule["source"], amount, 3, "substitution")[0]["exchanges"][1])
    before = deepcopy(data)
    source = InventoryContext(FormatProfile("brightway_excel"), target_context(release))
    inventory = BrightwayInventory.from_data(data, context=source)
    target = BackgroundContext(
        TechnosphereProfile("ecoinvent", "3.12", "cutoff"), BiosphereProfile("ecoinvent", "3.12")
    )
    result = inventory.migrate_background(target, policy=POLICY)
    assert len(result.data) == 1
    for a, b in zip(before[0]["exchanges"][1:], result.data[0]["exchanges"][1:], strict=True):
        assert {f: b[f] for f in wool.FIELDS} == rule["target"]
        assert all(b[k] == v for k, v in a.items() if k not in wool.FIELDS)
    assert inventory.data == data == before
    assert any(i.code == "migration.documented_proxy" for i in result.last_migration_report.issues)


@pytest.mark.parametrize("kind", ("light", "wool"))
@pytest.mark.parametrize("mutation", ("target", "factor", "sign", "review"))
def test_independent_reverse_projection_rejects_a_changed_endpoint_unit_coordinate_or_evidence(kind, mutation):
    module = freight if kind == "light" else wool
    rule, reviews = freight.light_reverse_addition() if kind == "light" else wool.reverse_addition()
    resource = {
        "replace": [deepcopy(rule)],
        "reviews": deepcopy(reviews),
        "coverage": {"mapped_identities": 1, "unresolved_identities": 0, "dispositions": {"proxy": 1, "unresolved": 0}},
    }
    assert module.legacy_reverse_projection(resource)["replace"] == []
    if mutation == "target":
        resource["replace"][0]["target"]["name"] = "Unreviewed supplier"
    elif mutation == "factor":
        resource["replace"][0]["conversion_factor"] = -1
    elif mutation == "sign":
        resource["replace"][0]["reference_flow_sign_review"]["source_sign"] = -1
    else:
        resource["reviews"][rule["review_ref"]]["rationale"] = "Unreviewed boundary"
    with pytest.raises(ValueError, match="repeat the scientific review"):
        module.legacy_reverse_projection(resource)
