"""Positive cement products retain original delivery instead of losing market freight."""

from copy import deepcopy

import pytest
from test_uvek_reference_signs import consumer, contexts
from test_uvek_refrigerant_disposal import target_context

from brightpath import BrightwayInventory
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource
from scripts import review_uvek_swiss_cement_markets as review

POLICY = MigrationPolicy(on_heuristic_mapping=PolicyAction.WARN, on_information_loss=PolicyAction.WARN)
CASES = [(row, profile) for row in review.SOURCES.values() for profile in row["profiles"]]


@pytest.mark.parametrize("row,profile", CASES)
@pytest.mark.parametrize("release", ("2025", "2026", "native2026"))
@pytest.mark.parametrize("amount", (-2, 0, 2))
def test_cement_class_and_original_delivery_survive_signed_conversion(row, profile, release, amount):
    version = profile["label"].split("-")[1]
    data = consumer(row["source"], amount, 2)
    data[0]["exchanges"][1]["caller_metadata"] = {"preserve": [1, 2]}
    before = deepcopy(data)
    source = BrightwayInventory.from_data(data, context=contexts("ecoinvent", version))
    result = source.migrate_background(target_context(release), policy=POLICY)
    changed = result.data[0]["exchanges"][1]
    assert changed["amount"] == amount
    assert all(changed.get(k) == value for k, value in before[0]["exchanges"][1].items() if k not in review.FIELDS)
    assert source.data == data == before
    helpers = {d["code"]: d for d in result.data[1:]}
    market = helpers[changed["input"][1]]
    assert market["exchanges"][0]["amount"] == 1
    inputs = [e for e in market["exchanges"] if e["type"] != "production"]
    assert len(inputs) == 3
    assert sorted(e["amount"] for e in inputs) == [0.0037, 0.0259, 1]
    assert sum(e["unit"] == "ton kilometer" for e in inputs) == 2
    cement = next(e for e in inputs if e["unit"] == "kilogram")
    assert review.identity(cement) == review.identity(row["previous_target"])
    for actual, original in zip(inputs, profile["inputs"], strict=True):
        for field in (
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
        ):
            assert actual.get(field) == original.get(field)
    assert not any(d.get("brightpath_conversion", {}).get("zero_burden") for d in helpers.values())


def test_source_product_scope_does_not_approve_same_named_allocated_ash_or_older_profiles():
    resource = load_uvek_technosphere_resource()
    review.validate_bindings(resource)
    assert len(review.SOURCES) == 2 and len(CASES) == 12
    assert {p["label"].split("-")[1] for _, p in CASES} == {"3.9", "3.9.1", "3.10", "3.10.1", "3.11", "3.12"}
    assert all(row["source"]["reference product"].startswith("cement, CEM II/") for row in review.SOURCES.values())
    assert all(row["source"]["name"].startswith("market for cement,") for row in review.SOURCES.values())
    assert all(not profile.get("parameters") for _, profile in CASES)
    assert all(not e.get("formula") for _, profile in CASES for e in profile["inputs"])


@pytest.mark.parametrize("change", ("factor", "unit", "grade", "missing"))
def test_changed_delivery_or_production_binding_cannot_inherit_review(change):
    resource = deepcopy(load_uvek_technosphere_resource())
    _, profile = CASES[0]
    from brightpath import TechnosphereProfile
    from brightpath.background.reference_signs import select_profile_sign_rules

    source = tuple(profile["bindings"][0]["source"])
    # Select the exact head being fingerprinted, then alter its canonical rule.
    selected = select_profile_sign_rules(resource, TechnosphereProfile(*profile["label"].split("-")))
    chosen = next(r for r in selected["replace"] if review.identity(r["source"]) == source)
    original = next(r for r in resource["replace"] if r["id"] == chosen["id"])
    if change == "grade":
        original["target"]["name"] += " changed"
    elif change == "factor":
        original["conversion_factor"] = 2
    elif change == "unit":
        original["target"]["unit"] = "ton"
    else:
        resource["replace"].remove(original)
    with pytest.raises(ValueError, match="production or freight binding changed"):
        review.validate_bindings(resource)


@pytest.mark.parametrize("native", (False, True))
@pytest.mark.parametrize("format_name", ("brightway", "simapro"))
def test_cement_market_roundtrip_retains_factory_supply_and_separate_delivery(tmp_path, native, format_name):
    from brightpath import SimaProInventory

    row = next(iter(review.SOURCES.values()))
    data = consumer(row["source"], -2, 3)
    data[0]["parameters"] = [{"name": "dose", "amount": -2}]
    data[0]["comment"] = "Synthetic cement delivery fixture."
    converted = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.12")).migrate_background(
        target_context("native2026" if native else "2025"), policy=POLICY
    )
    if format_name == "brightway":
        restored = BrightwayInventory.from_excel(converted.write_excel(tmp_path / "cement.xlsx"))
    else:
        categorized = deepcopy(converted.data)
        for dataset in categorized:
            dataset["exchanges"][0]["simapro category"] = "Materials/Construction"
        path = (
            BrightwayInventory.from_data(categorized, context=converted.context)
            .to_simapro()
            .write_csv(tmp_path / "cement.csv")
        )
        restored = SimaProInventory.from_csv(
            path,
            background_profile=converted.background_profile,
            biosphere_profile=converted.context.background.biosphere,
        ).to_brightway()
    assert not restored.validate().has_errors
    by_identity = {review.identity(d): d for d in restored.data}
    for original in converted.data[1:]:
        final = by_identity[review.identity(original)]
        assert [e["amount"] for e in final["exchanges"]] == [e["amount"] for e in original["exchanges"]]


@pytest.mark.parametrize("row", list(review.SOURCES.values()))
@pytest.mark.parametrize("release", ("2025", "2026", "native2026"))
def test_native_factory_supply_does_not_inherit_an_inferred_delivered_market_inverse(row, release):
    from brightpath import BackgroundContext, BiosphereProfile, FormatProfile, InventoryContext, TechnosphereProfile

    data = consumer(row["previous_target"], 2, 2)
    before = deepcopy(data)
    source = BrightwayInventory.from_data(
        data, context=InventoryContext(FormatProfile("brightway_excel"), target_context(release))
    )
    target = BackgroundContext(
        TechnosphereProfile("ecoinvent", "3.12", "cutoff"), BiosphereProfile("ecoinvent", "3.12")
    )
    with pytest.raises(MigrationError, match="No reviewed directional correspondence"):
        source.migrate_background(target, policy=POLICY)
    assert source.data == data == before
