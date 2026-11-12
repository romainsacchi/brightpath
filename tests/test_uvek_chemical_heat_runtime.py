"""Exercise retained-water chemical heat through the actual canonical and native migration routes."""

import json
from copy import deepcopy

import pytest
from test_uvek_reference_signs import consumer, contexts
from test_uvek_refrigerant_disposal import target_context

from brightpath import (
    DATA_DIR,
    BackgroundContext,
    BiosphereProfile,
    BrightwayInventory,
    FormatProfile,
    InventoryContext,
    TechnosphereProfile,
)
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from scripts import review_uvek_chemical_heat as heat

POLICY = MigrationPolicy(on_heuristic_mapping=PolicyAction.WARN, on_information_loss=PolicyAction.WARN)


def batch(amount):
    data = None
    for row in heat.SOURCES.values():
        for kind, uncertainty in (("technosphere", 2), ("substitution", 3)):
            example = consumer(row["source"], amount, uncertainty, kind)
            example[0]["exchanges"][1]["caller_metadata"] = {"preserve": [1, 2]}
            if data is None:
                data = example
            else:
                data[0]["exchanges"].append(example[0]["exchanges"][1])
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


@pytest.mark.parametrize("version", sorted(heat.VERSIONS))
@pytest.mark.parametrize("release", ("2025", "2026", "native2026"))
@pytest.mark.parametrize("amount", (-2, 0, 2))
def test_signed_heat_uncertainty_and_every_water_flow_survive_real_migration(version, release, amount):
    data = batch(amount)
    original = deepcopy(data)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", version))
    target = target_context(release)
    result = inventory.migrate_background(target, policy=POLICY)
    assert len(result.data) == 5
    helpers = {d["code"]: d for d in result.data[1:]}
    assert len(helpers) == 4
    for before, after in zip(original[0]["exchanges"][1:-1], result.data[0]["exchanges"][1:-1], strict=True):
        assert after["amount"] == amount
        assert all(after.get(k) == v for k, v in before.items() if k not in heat.FIELDS)
        helper = helpers[after["input"][1]]
        source = heat.identity(before)
        profile = next(p for p in heat.SOURCES[source]["profiles"] if p["label"] == f"ecoinvent-{version}-cutoff")
        rid, recipe = heat.heat_recipe(source, profile)
        # Later scoped reviews retain point quantities and restore water metadata.
        if source[0] == "market for heat, from steam, in chemical industry" and source[2] == "RoW":
            if version == "3.8":
                review = json.loads((DATA_DIR / "export/uvek_fuels_38_chemical_heat_review.json").read_text())
                rid = review["new_recipe_id"]
                recipe = review["recipes"][rid]
            elif version == "3.9":
                review = json.loads((DATA_DIR / "export/uvek_pcc_last_four_review.json").read_text())
                row = next(r for r in review["records"] if r["rule_id"] == "bp-05200779ee64a17f")
                rid = row["recipe_id"]
                recipe = review["recipes"][rid]
        assert helper["brightpath_conversion"]["recipe_id"] == rid
        assert helper["unit"] == "megajoule"
        assert helper["exchanges"][0]["type"] == "production" and helper["exchanges"][0]["amount"] == 1
        tech = [e for e in helper["exchanges"][1:] if e["type"] == "technosphere"]
        bio = [e for e in helper["exchanges"][1:] if e["type"] == "biosphere"]
        expected_tech = [e for e in recipe["inputs"] if e["type"] == "technosphere"]
        assert len(tech) == len(expected_tech)
        for actual, expected in zip(tech, expected_tech, strict=True):
            assert heat.identity(actual) == heat.identity(expected)
            assert actual["amount"] == expected["amount"]
        assert heat.identity(tech[0]) == heat.TARGET and tech[0]["amount"] == 1
        unit, factor = ("kilogram", 1000) if release == "native2026" else ("cubic meter", 1)
        assert len(bio) == 2 and all(e["unit"] == unit for e in bio)
        by_compartment = {}
        for e in bio:
            receiving = " ".join(e["categories"])
            compartment = "air" if "air" in receiving else "water" if "water" in receiving else None
            assert compartment is not None and compartment not in by_compartment
            # Native UVEK water is mass-based; its existing reviewed route uses
            # 1000 kg/m3. Compare the physical volume after that reported change.
            by_compartment[compartment] = e["amount"] / factor
        assert by_compartment == {
            "air": pytest.approx(9.27272727272727e-6),
            "water": pytest.approx(5.25454545454545e-5),
        }
        assert sum(e["amount"] for e in tech[1:]) == pytest.approx(sum(by_compartment.values()) * 1000)
        assert not any(e.get("name") == "Steam, for chemical processes, at plant" for e in helper["exchanges"])
    assert result.data[0]["exchanges"][-1]["amount"] == 0.5
    assert any(i.code == "migration.technosphere_information_loss" for i in result.last_migration_report.issues)
    assert not any(
        i.code in {"migration.zero_burden_supplier", "migration.biosphere_omitted"}
        for i in result.last_migration_report.issues
    )
    assert inventory.data == data == original
    assert result.migrate_background(target).data == result.data


@pytest.mark.parametrize("release", ("2025", "2026", "native2026"))
def test_proxy_consent_foreground_ownership_and_consequential_refusal_still_apply(release):
    data = batch(-2)
    original = deepcopy(data)
    target = target_context(release)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.12"))
    with pytest.raises(MigrationError):
        inventory.migrate_background(target, policy=MigrationPolicy.strict())
    assert inventory.data == original
    consequential = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.12", "consequential"))
    with pytest.raises(MigrationError):
        consequential.migrate_background(target, policy=MigrationPolicy.permissive())
    assert consequential.data == original
    own = deepcopy(data)
    for row in heat.SOURCES.values():
        own.append({**row["source"], "exchanges": [{**row["source"], "type": "production", "amount": 1}]})
    inventory = BrightwayInventory.from_data(own, context=contexts("ecoinvent", "3.12"))
    result = inventory.migrate_background(target, policy=POLICY)
    assert len(result.data) == len(own)
    assert result.data[0]["exchanges"][1:-1] == own[0]["exchanges"][1:-1]


@pytest.mark.parametrize("release", ("2025", "2026", "native2026"))
def test_reverse_heat_requires_its_own_water_boundary_review_instead_of_inverting_the_forward_helper(release):
    source = dict(zip(heat.FIELDS, heat.TARGET, strict=True))
    data = consumer(source, -2, 2)
    before = deepcopy(data)
    inventory = BrightwayInventory.from_data(
        data, context=InventoryContext(FormatProfile("brightway_excel"), target_context(release))
    )
    target = BackgroundContext(
        TechnosphereProfile("ecoinvent", "3.12", "cutoff"), BiosphereProfile("ecoinvent", "3.12")
    )
    with pytest.raises(MigrationError, match="No reviewed directional correspondence"):
        inventory.migrate_background(target, policy=POLICY)
    assert inventory.data == data == before
