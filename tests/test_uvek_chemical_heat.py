"""Chemical heat retains energy units, every make-up-water share and both physical water releases."""

import json
from collections import defaultdict
from copy import deepcopy

import pytest

from brightpath import TechnosphereProfile
from brightpath.background.reference_signs import select_profile_sign_rules
from scripts import review_uvek_chemical_heat as heat
from scripts.review_uvek_reference_signs import compiled_venting_rule, rule_binding

PROFILES = [(s, p) for s, row in heat.SOURCES.items() for p in row["profiles"]]


@pytest.fixture(scope="module")
def original_resource():
    resource = json.loads((heat.ROOT / "brightpath/data/migrations/uvek/ecoinvent-to-uvek-2025.json").read_text())
    ledger = json.loads((heat.ROOT / "brightpath/data/export/uvek_reference_flow_sign_review.json").read_text())
    prior = ledger["resources"][resource["name"]]
    for index, rule in enumerate(resource["replace"]):
        if heat.identity(rule["source"]) in heat.SOURCES and "recipe_ref" in rule["target"]:
            decision = prior[rule["id"]]
            assert compiled_venting_rule(decision) == rule
            resource["replace"][index] = deepcopy(decision["original_rule"])
    return resource


def inventory_evidence():
    inventories = defaultdict(list)
    observations = defaultdict(lambda: defaultdict(set))
    for record in heat.APPROVAL["inventories"]:
        source = heat.identity(record["source"])
        row = {**record, "source": list(source)}
        inventories[source].append(row)
        observations[record["label"].split("-")[0]][source].add((record["label"], record["reference_amount"]))
    native = {
        heat.identity(r["source"]): {k: v for k, v in r.items() if k != "source"} for r in heat.APPROVAL["native_roles"]
    }
    return inventories, observations, native


def decisions(resource):
    rows = {}
    for rule in resource["replace"]:
        source = heat.identity(rule["source"])
        if source not in heat.SOURCES:
            continue
        row = heat.SOURCES[source]
        observations = {(p["label"], 1) for p in row["profiles"]}
        observations.add(("ecoinvent-3.12-consequential", 1))
        entries = [{"label": p["label"], "inventory_sha256": p["inventory_sha256"]} for p in row["profiles"]]
        rows[source] = heat.forward_decision(rule, observations, entries, rule_binding(rule), None)
    assert len(rows) == 4
    return rows


@pytest.fixture(scope="module")
def compiled_resource(original_resource):
    resource = deepcopy(original_resource)
    rows = decisions(resource)
    for index, rule in enumerate(resource["replace"]):
        decision = rows.get(heat.identity(rule["source"]))
        if decision is None:
            continue
        resource["replace"][index] = compiled_venting_rule(decision)
        resource["recipes"].update(deepcopy(decision["recipes"]))
    return resource


@pytest.mark.parametrize("source,profile", PROFILES)
def test_exact_profile_preserves_heat_unit_water_balance_and_separate_provider_shares(source, profile):
    before = deepcopy(profile)
    recipe_id, recipe = heat.heat_recipe(source, profile)
    assert recipe_id.startswith("chemical-heat-water-")
    assert recipe["unit"] == "megajoule"
    tech = [e for e in recipe["inputs"] if e["type"] == "technosphere"]
    biosphere = [e for e in recipe["inputs"] if e["type"] == "biosphere"]
    assert heat.identity(tech[0]) == heat.TARGET and tech[0]["amount"] == 1
    assert len(tech[1:]) == (1 if source[2] == "RER" else 7)
    assert all(e["unit"] == "kilogram" and e["name"].startswith("Tap water,") for e in tech[1:])
    water_input = sum(e["amount"] for e in tech[1:])
    assert water_input == pytest.approx(0.0618181818181818, rel=0, abs=2e-15)
    assert {tuple(e["categories"]): e["amount"] for e in biosphere} == {
        ("air",): pytest.approx(9.27272727272727e-6),
        ("water",): pytest.approx(5.25454545454545e-5),
    }
    assert sum(e["amount"] for e in biosphere) * 1000 == pytest.approx(water_input, rel=0, abs=2e-15)
    assert not any(e.get("name") == "Steam, for chemical processes, at plant" for e in recipe["inputs"])
    assert not any(e.get("approved_omission_if_unavailable") for e in biosphere)
    recipe["inputs"][1]["amount"] = 0
    assert profile == before


@pytest.mark.parametrize("source,profile", PROFILES)
def test_compiled_profile_selects_only_per_MJ_helper_and_explicit_water_warning(compiled_resource, source, profile):
    chosen = select_profile_sign_rules(compiled_resource, TechnosphereProfile(*profile["label"].split("-")))
    rule = next(r for r in chosen["replace"] if heat.identity(r["source"]) == source)
    assert rule["conversion_factor"] == 1
    assert rule["reference_flow_sign_review"]["status"] == "verified"
    recipe_id, expected = heat.heat_recipe(source, profile)
    assert rule["target"] == {"recipe_ref": recipe_id}
    assert compiled_resource["recipes"][recipe_id] == expected
    assert "retain" in rule["technosphere_information_loss"] and "water" in rule["technosphere_information_loss"]
    assert "0.36 kg/MJ" in rule["technosphere_information_loss"]


def test_all_native_water_suppliers_physical_contexts_and_complete_source_fingerprints_are_frozen(original_resource):
    inventories, observations, native = inventory_evidence()
    heat.validate_inventories(inventories, observations, native)
    heat.validate_bindings(original_resource)
    assert len(heat.APPROVAL["canonical_biosphere_definitions"]) == 2
    assert not hasattr(heat, "reverse_addition")
    assert heat.APPROVAL["reverse"]["status"] == "unsupported_heat_native_identity"


@pytest.mark.parametrize("kind", ("hash", "description", "reference", "missing", "duplicate"))
def test_changed_actual_inventory_or_reference_cannot_reuse_heat_review(kind):
    inventories, observations, native = inventory_evidence()
    source = next(iter(heat.SOURCES))
    row = inventories[source][0]
    if kind == "hash":
        row["inventory_sha256"] = "0" * 64
    elif kind == "description":
        row["activity_comment_sha256"] = "0" * 64
    elif kind == "reference":
        label, _ = next(iter(observations["ecoinvent"][source]))
        observations["ecoinvent"][source].remove((label, 1))
        observations["ecoinvent"][source].add((label, -1))
    elif kind == "missing":
        inventories[source].pop()
    else:
        inventories[source].append(deepcopy(row))
    with pytest.raises(ValueError, match="repeat the scientific review"):
        heat.validate_inventories(inventories, observations, native)


@pytest.mark.parametrize("mutation", ("unit", "role", "input", "sign"))
def test_native_mass_steam_or_waste_role_cannot_replace_heat_energy(mutation):
    inventories, observations, native = inventory_evidence()
    if mutation == "unit":
        native[heat.TARGET]["flow_property_name"] = "Mass"
        native[heat.TARGET]["unit_name"] = "kg"
    elif mutation == "role":
        native[heat.TARGET]["native_flow_type"] = "WASTE_FLOW"
    elif mutation == "input":
        native[heat.TARGET]["native_reference_is_input"] = True
    else:
        observations["uvek"][heat.TARGET] = {("uvek-2025-cutoff", -1)}
    with pytest.raises(ValueError, match="repeat the scientific review"):
        heat.validate_inventories(inventories, observations, native)


@pytest.mark.parametrize("mutation", ("water", "release", "compartment", "fuel", "steam", "consequential"))
def test_a_new_water_omission_fuel_or_profile_choice_requires_another_review(monkeypatch, mutation):
    approval = deepcopy(heat.APPROVAL)
    p = approval["rules"][0]["profiles"][0]
    if mutation == "water":
        p["inputs"][1]["amount"] = 0
    elif mutation == "release":
        p["inputs"].pop()
    elif mutation == "compartment":
        p["inputs"][-1]["categories"] = ["water", "river"]
    elif mutation == "fuel":
        p["inputs"][0]["name"] = "Natural gas, burned in industrial furnace 1MWth"
    elif mutation == "steam":
        p["inputs"][0]["unit"] = "kilogram"
    else:
        p["label"] = "ecoinvent-3.12-consequential"
    monkeypatch.setattr(heat, "APPROVAL", approval)
    with pytest.raises(ValueError, match="repeat the scientific review"):
        heat.validate_review()


@pytest.mark.parametrize("source", sorted(heat.SOURCES))
@pytest.mark.parametrize("mutation", ("factor", "target", "hash", "profile"))
def test_decision_rejects_a_changed_supplier_original_factor_or_uninspected_profile(
    original_resource, source, mutation
):
    rule = deepcopy(next(r for r in original_resource["replace"] if heat.identity(r["source"]) == source))
    row = heat.SOURCES[source]
    observations = {(p["label"], 1) for p in row["profiles"]}
    entries = [{"label": p["label"], "inventory_sha256": p["inventory_sha256"]} for p in row["profiles"]]
    if mutation == "factor":
        rule["conversion_factor"] = -7
    elif mutation == "target":
        rule["target"] = dict(zip(heat.FIELDS, heat.TARGET, strict=True))
        rule["target"]["location"] = "Unreviewed region"
    elif mutation == "hash":
        entries[0]["inventory_sha256"] = "0" * 64
    else:
        observations.add(("ecoinvent-3.13-cutoff", 1))
    with pytest.raises(ValueError, match="repeat the scientific review"):
        heat.forward_decision(rule, observations, entries, rule_binding(rule), None)


@pytest.mark.parametrize(
    "unit,name",
    (
        ("kilogram", "steam production, in chemical industry"),
        ("megajoule", "steam production, from aniline"),
        ("ton kilometer", "transport, freight train, steam"),
    ),
)
def test_unrelated_mass_steam_coproducts_and_transport_do_not_inherit_this_authorization(unit, name):
    source = (name, "steam, in chemical industry", "RER", unit)
    profile = deepcopy(PROFILES[0][1])
    with pytest.raises(ValueError, match="repeat the scientific review"):
        heat.heat_recipe(source, profile)
