"""Textile markets retain unsanitary disposal, regional shares and complete freight."""

from collections import Counter
from copy import deepcopy

import pytest
from test_uvek_reference_signs import consumer, contexts
from test_uvek_refrigerant_disposal import target_context
from test_uvek_uncontrolled_disposal import expected_inputs, signature

from brightpath import BiosphereProfile, BrightwayInventory, TechnosphereProfile
from brightpath.background import PackageCatalogProvider
from brightpath.background.reference_signs import select_profile_sign_rules
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource
from scripts import review_uvek_uncontrolled_disposal as disposal
from scripts.review_uvek_market_compositions import COMPOSITIONS

FIELDS = ("name", "reference product", "location", "unit")
LANDFILL = (
    "treatment of waste yarn and waste textile, unsanitary landfill",
    "waste yarn and waste textile",
    "RoW",
    "kilogram",
)
VERSIONS = sorted(disposal.VERSIONS)
POLICY = MigrationPolicy(on_information_loss=PolicyAction.WARN, on_heuristic_mapping=PolicyAction.WARN)


def market_source(version):
    location = "GLO" if version in {"3.6", "3.7", "3.8", "3.9", "3.9.1"} else "RoW"
    return ("market for waste yarn and waste textile", "waste yarn and waste textile", location, "kilogram")


def market_profile(version):
    return next(
        p for p in COMPOSITIONS[market_source(version)]["profiles"] if p["label"] == f"ecoinvent-{version}-cutoff"
    )


def landfill_profile(version):
    return next(p for p in disposal.SOURCES[LANDFILL]["profiles"] if p["label"] == f"ecoinvent-{version}-cutoff")


def batch(version, amount):
    data = consumer(dict(zip(FIELDS, market_source(version), strict=True)), amount, 2)
    data[0]["exchanges"].append(
        consumer(dict(zip(FIELDS, market_source(version), strict=True)), amount, 3, "substitution")[0]["exchanges"][1]
    )
    for e in data[0]["exchanges"][1:]:
        e["caller_metadata"] = {"retain": [1, 2]}
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


@pytest.mark.parametrize("version", VERSIONS)
@pytest.mark.parametrize("release", ("2025", "2026"))
@pytest.mark.parametrize("amount", (-2, 0, 2))
def test_market_signs_uncertainty_ownership_and_full_disposal_inputs(version, release, amount):
    data = batch(version, amount)
    original = deepcopy(data)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", version))
    result = inventory.migrate_background(target_context(release), policy=POLICY)
    assert result.data[0]["exchanges"][0] == original[0]["exchanges"][0]
    assert result.data[0]["exchanges"][-1] == original[0]["exchanges"][-1]
    for before, after in zip(original[0]["exchanges"][1:-1], result.data[0]["exchanges"][1:-1], strict=True):
        assert after["amount"] == -amount and after["type"] == before["type"]
        assert after["formula"] == "(dose) * (-1.0)"
        assert after["caller_metadata"] == before["caller_metadata"] and after["scale"] == before["scale"]
        if before["uncertainty type"] == 2:
            assert after["loc"] == before["loc"] and after["negative"] is not before["negative"]
        else:
            assert after["loc"] == -before["loc"]
            assert (after["minimum"], after["maximum"]) == (-before["maximum"], -before["minimum"])
    helpers = {r["code"]: r for r in result.data[1:]}
    market = helpers[result.data[0]["exchanges"][1]["input"][1]]
    landfill = next(
        r
        for r in helpers.values()
        if r["location"] == "RoW" and r["brightpath_conversion"]["recipe_id"].startswith("uncontrolled-disposal-")
    )
    actual = Counter(signature(e) for e in landfill["exchanges"] if e["type"] != "production")
    assert actual == expected_inputs(landfill_profile(version), release)
    for helper in helpers.values():
        (production,) = [e for e in helper["exchanges"] if e["type"] == "production"]
        assert production["amount"] == 1 and not helper["brightpath_conversion"].get("zero_burden")
    children = [e for e in market["exchanges"] if e["type"] == "technosphere"]
    # Nested helper links use full inventory identities; caller links additionally carry codes.
    by_identity = {tuple(r[f] for f in FIELDS): r for r in helpers.values()}
    treatment = {
        by_identity[tuple(e[f] for f in FIELDS)]["location"]: e["amount"]
        for e in children
        if tuple(e[f] for f in FIELDS) in by_identity
    }
    freight = {e["name"]: e["amount"] for e in children if e.get("unit") == "ton kilometer"}
    if market_source(version)[2] == "GLO":
        assert treatment == {"IN": 0.391087708785563, "RoW": 0.608912291214437}
        assert sum(treatment.values()) == 1
        assert freight == {
            "Transport, transoceanic container ship": 0.2115,
            "Transport, freight, rail": 0.112,
            "Transport, barge": 0.0199,
            "Transport, freight, lorry, fleet average": 0.1932,
        }
    else:
        assert treatment == {"RoW": 1}
        assert freight.keys() == {"Transport, freight, lorry, fleet average"}
        assert next(iter(freight.values())) == pytest.approx(0.077, rel=1e-14, abs=0)
    forbidden = {"Disposal, municipal solid waste, 22.9% water, to sanitary landfill", "xx Textile, jute, at plant"}
    assert not any(e["name"] in forbidden for r in result.data for e in r["exchanges"] if e["type"] == "technosphere")
    assert result.last_migration_report.losses
    assert inventory.data == data == original
    assert result.migrate_background(target_context(release)).data == result.data


@pytest.mark.parametrize("version", VERSIONS)
def test_native_physical_gaps_refuse_transactionally_and_permissive_mode_retains_releases(version):
    data = batch(version, -1)
    original = deepcopy(data)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", version))
    with pytest.raises(MigrationError) as caught:
        inventory.migrate_background(target_context("native2026"), policy=POLICY)
    assert any(i.code == "background.biosphere_link_unresolved" for i in caught.value.report.issues)
    result = inventory.migrate_background(target_context("native2026"), policy=MigrationPolicy.permissive())
    landfill = next(
        r
        for r in result.data[1:]
        if r["location"] == "RoW" and r["brightpath_conversion"]["recipe_id"].startswith("uncontrolled-disposal-")
    )
    physical = [
        e
        for e in landfill_profile(version)["inputs"]
        if e["type"] == "biosphere" and not e.get("approved_omission_if_unavailable")
    ]
    actual = [e for e in landfill["exchanges"] if e["type"] == "biosphere"]
    assert len(actual) >= len(physical)
    available = PackageCatalogProvider().load_biosphere(BiosphereProfile("uvek", "2026")).identities
    unresolved = [e for e in actual if (e["name"], tuple(e["categories"]), e["unit"]) not in available]
    assert unresolved and Counter(signature(e) for e in unresolved) <= Counter(signature(e) for e in physical)
    omissions = [i for i in result.last_migration_report.issues if i.code == "migration.recipe_input_omission"]
    assert all(i.details["source_exchange"]["categories"][0] == "inventory indicator" for i in omissions)
    assert inventory.data == data == original


@pytest.mark.parametrize("version", VERSIONS)
def test_market_foreground_supplier_keeps_its_own_production_and_inventory(version):
    data = batch(version, -2)
    source = dict(zip(FIELDS, market_source(version), strict=True))
    data.append({**source, "exchanges": [{**source, "type": "production", "amount": -1}]})
    original = deepcopy(data)
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", version)).migrate_background(
        target_context("2025"), policy=POLICY
    )
    assert result.data == data == original


@pytest.mark.parametrize("version", ("3.9.2", "3.10.2"))
def test_uninspected_patch_cannot_inherit_textile_approval(version):
    view = select_profile_sign_rules(
        load_uvek_technosphere_resource(), TechnosphereProfile("ecoinvent", version, "cutoff")
    )
    sources = {LANDFILL, market_source("3.9.1"), market_source("3.10.1")}
    rules = {tuple(r["source"][f] for f in FIELDS): r for r in view["replace"]}
    assert all(rules[s]["reference_flow_sign_review"]["status"] == "unresolved" for s in sources)


@pytest.mark.parametrize("version,expected_datasets", (("3.9.1", 4), ("3.10.1", 3)))
def test_exact_patch_excel_export_retains_every_nested_helper(version, expected_datasets, tmp_path):
    data = consumer(dict(zip(FIELDS, market_source(version), strict=True)), -1, 3)
    data[0]["comment"] = "Exact-patch textile market and complete nested-helper export regression."
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", version)).migrate_background(
        target_context("2025"), policy=POLICY
    )
    path = tmp_path / "textile-market.xlsx"
    result.write_excel(path)
    restored = BrightwayInventory.from_excel(path)
    assert len(restored.data) == len(result.data) == expected_datasets
    for before, after in zip(result.data, restored.data, strict=True):
        assert len(after["exchanges"]) == len(before["exchanges"])
        assert [e["amount"] for e in after["exchanges"]] == pytest.approx([e["amount"] for e in before["exchanges"]])
    assert restored.data[0]["exchanges"][1]["amount"] == 1
