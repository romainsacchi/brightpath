"""Exact patch services retain physical quantities and executable local formulas."""

from copy import deepcopy

import pytest
from test_uvek_reference_signs import FIELDS, consumer, contexts
from test_uvek_refrigerant_disposal import target_context

from brightpath import BrightwayInventory, TechnosphereProfile
from brightpath.background.reference_signs import select_profile_sign_rules
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource
from scripts import review_uvek_refrigerant_patch_formulas as review
from scripts.review_uvek_venting import compiled_venting_rule

POLICY = MigrationPolicy(on_heuristic_mapping=PolicyAction.WARN, on_information_loss=PolicyAction.WARN)
CASES = [(source, p) for source, row in review.SOURCES.items() for p in row["profiles"]]
QFIELDS = (
    "amount",
    "uncertainty type",
    "loc",
    "scale",
    "shape",
    "negative",
    "minimum",
    "maximum",
    "formula",
    "variable name",
)


def helpers_by_id(result):
    return {d["brightpath_conversion"]["recipe_id"]: d for d in result.data if "brightpath_conversion" in d}


def evaluate(expression, variables):
    return eval(expression, {"__builtins__": {}, "ABS": abs, "abs": abs}, variables)


def parameter_values(parameters):
    values = {p["name"]: p["amount"] for p in parameters}
    for p in parameters:
        if p.get("formula"):
            assert evaluate(p["formula"], values) == pytest.approx(p["amount"])
            values[p["name"]] = evaluate(p["formula"], values)
    return values


@pytest.mark.parametrize("source,profile", CASES)
@pytest.mark.parametrize("release", ("2025", "2026"))
@pytest.mark.parametrize("amount", (-2, 0, 2))
def test_every_declared_profile_preserves_quantities_and_formula_responses(source, profile, release, amount):
    data = consumer(dict(zip(FIELDS, source, strict=True)), amount, 2)
    data[0]["parameters"] = [{"name": "used_refrigerant", "amount": 999}, {"name": "dose", "amount": amount}]
    data[0]["exchanges"].append(
        consumer(dict(zip(FIELDS, source, strict=True)), amount, 3, "substitution")[0]["exchanges"][1]
    )
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
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", profile["label"].split("-")[1]))
    result = inventory.migrate_background(target_context(release), policy=POLICY)
    helpers = helpers_by_id(result)
    root = helpers[profile["recipe_id"]]
    assert root["unit"] == source[3] and root["location"] == source[2]
    assert root.get("parameters", []) == profile["recipe"].get("parameters", [])
    assert root["exchanges"][0]["amount"] == 1 and root["exchanges"][0]["type"] == "production"
    variables = parameter_values(root.get("parameters", []))
    assert len(root["exchanges"]) == len(profile["recipe"]["inputs"]) + 1
    for old, final in zip(profile["recipe"]["inputs"], root["exchanges"][1:], strict=True):
        assert all(old.get(f) == final.get(f) for f in QFIELDS)
        if old.get("formula"):
            assert evaluate(final["formula"], variables) == pytest.approx(final["amount"])
        if "recipe" in old:
            assert all(final[f] == helpers[old["recipe"]][f] for f in FIELDS)
        elif old["type"] == "biosphere":
            assert all(final[f] == old[f] for f in ("name", "categories", "unit"))
        else:
            assert all(final[f] == old[f] for f in FIELDS)
    for old, new in zip(before[0]["exchanges"][1:-1], result.data[0]["exchanges"][1:-1], strict=True):
        assert new["amount"] == -amount and new["type"] == old["type"]
        assert new["formula"] == "(dose) * (-1.0)"
        assert new["scale"] == old["scale"]
        if old["uncertainty type"] == 2:
            assert new["loc"] == old["loc"] and new["negative"] is not old["negative"]
        else:
            assert new["loc"] == -old["loc"]
            assert (new["minimum"], new["maximum"]) == (-old["maximum"], -old["minimum"])
        assert new["input"][1] == root["code"]
    assert result.data[0]["parameters"] == before[0]["parameters"]
    assert result.data[0]["exchanges"][0] == before[0]["exchanges"][0]
    assert result.data[0]["exchanges"][-1]["amount"] == 0.5
    assert not any(i.code.endswith("link_unresolved") for i in result.last_migration_report.issues)
    assert inventory.data == data == before
    assert result.migrate_background(target_context(release)).data == result.data


@pytest.mark.parametrize("source,profile", CASES)
def test_native_preserves_every_new_physical_flow_and_local_parameter(source, profile):
    data = consumer(dict(zip(FIELDS, source, strict=True)), -2, 3)
    before = deepcopy(data)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", profile["label"].split("-")[1]))
    result = inventory.migrate_background(target_context("native2026"), policy=MigrationPolicy.permissive())
    root = helpers_by_id(result)[profile["recipe_id"]]
    assert root.get("parameters", []) == profile["recipe"].get("parameters", [])
    physical = [e for e in root["exchanges"] if e["type"] == "biosphere"]
    expected = [e for e in profile["recipe"]["inputs"] if e.get("type") == "biosphere"]
    assert len(physical) == len(expected)
    for a, b in zip(expected, physical, strict=True):
        assert a["unit"] == b["unit"] == "kilogram"
        assert all(a.get(f) == b.get(f) for f in QFIELDS)
        bindings = [x for x in a.get("biosphere_targets", []) if x["profile"] == {"family": "uvek", "version": "2026"}]
        if bindings:
            assert all(b[f] == bindings[0]["target"][f] for f in ("name", "categories", "unit"))
    unresolved = [i for i in result.last_migration_report.issues if i.code.endswith("link_unresolved")]
    if unresolved:
        with pytest.raises(MigrationError) as caught:
            inventory.migrate_background(target_context("native2026"), policy=POLICY)
        assert any(i.code.endswith("link_unresolved") for i in caught.value.report.issues)
    else:
        assert inventory.migrate_background(target_context("native2026"), policy=POLICY).data == result.data
    assert inventory.data == before


@pytest.mark.parametrize("row", [r for r in review.SOURCES.values() if r["mode"] == "augment"])
def test_all_previous_profile_decisions_and_compiled_bindings_remain_exact(row):
    import json

    ledger = json.loads((review.ROOT / "brightpath/data/export/uvek_reference_flow_sign_review.json").read_text())
    resource = load_uvek_technosphere_resource()
    current = ledger["resources"][resource["name"]][row["rule_id"]]
    base = review.base_decision(row, current)
    assert all(current["recipes"][rid] == body for rid, body in base["recipes"].items())
    assert current["review"]["profiles"][: len(base["review"]["profiles"])] == base["review"]["profiles"]
    old = compiled_venting_rule(base)
    rule = next(r for r in resource["replace"] if r["id"] == row["rule_id"])
    for p in base["review"]["profiles"]:
        if p["source_profile"]["system_model"] != "cutoff":
            continue
        context = TechnosphereProfile(**p["source_profile"])
        (expected,) = select_profile_sign_rules({"replace": [old]}, context)["replace"]
        (actual,) = select_profile_sign_rules({"replace": [rule]}, context)["replace"]
        assert actual == expected


@pytest.mark.parametrize("source,profile", [(s, p) for s, p in CASES if s[0].endswith(", reclamation")])
def test_reclamation_keeps_motor_power_response_without_changing_emissions(source, profile):
    recipe = profile["recipe"]
    values = parameter_values(recipe["parameters"])
    (e,) = [e for e in recipe["inputs"] if "motor_power" in e.get("formula", "")]
    assert evaluate(e["formula"], {**values, "motor_power": values["motor_power"] * 2}) == pytest.approx(
        e["amount"] * 2
    )
    (bio,) = [e for e in recipe["inputs"] if e.get("type") == "biosphere"]
    assert evaluate(bio["formula"], {**values, "motor_power": values["motor_power"] * 2}) == pytest.approx(
        bio["amount"]
    )


@pytest.mark.parametrize(
    "field",
    ("inventory_sha256", "formula_sha256", "parameters_sha256", "activity_comment_sha256", "reference_amount", "label"),
)
def test_changed_full_inventory_or_parameter_boundary_requires_review(field):
    records = deepcopy(review.APPROVAL["inventories"])
    review.validate_inventories(records)
    records[0][field] = "changed"
    with pytest.raises(ValueError, match="changed"):
        review.validate_inventories(records)


@pytest.mark.parametrize("mutation", ("factor", "target", "confidence"))
def test_changed_declared_local_component_cannot_reuse_approval(mutation):
    import json

    resource = deepcopy(load_uvek_technosphere_resource())
    prior = json.loads((review.ROOT / "brightpath/data/export/uvek_reference_flow_sign_review.json").read_text())[
        "resources"
    ][resource["name"]]
    review.validate_bindings(resource, prior)
    binding = next(
        b
        for row in review.SOURCES.values()
        for p in row["profiles"]
        for b in p["component_bindings"]
        if b["kind"] == "local_profile"
    )
    rule = next(r for r in resource["replace"] if review.identity(r["source"]) == review.identity(binding["source"]))
    (p,) = [
        p
        for p in rule["reference_flow_sign_review"]["profiles"]
        if p["source_profile"]["version"] == binding["label"].split("-")[1]
        and p["source_profile"]["system_model"] == "cutoff"
    ]
    if mutation == "factor":
        p["conversion_factor"] = 999
    elif mutation == "target":
        p["target"] = {"name": "Unapproved", "reference product": "Unapproved", "location": "GLO", "unit": "kilogram"}
    else:
        rule["confidence"] = 0.123
    with pytest.raises(ValueError, match="changed"):
        review.validate_bindings(resource, prior)


@pytest.mark.parametrize("source", sorted(review.SOURCES))
def test_foreground_ownership_and_unsupported_contexts_remain_explicit(source):
    data = consumer(dict(zip(FIELDS, source, strict=True)), -2, 3)
    before = deepcopy(data)
    for version, model in (("3.12", "consequential"), ("3.12.1", "cutoff")):
        inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", version, model))
        with pytest.raises(MigrationError):
            inventory.migrate_background(target_context("2025"), policy=MigrationPolicy.permissive())
        assert inventory.data == data == before
    supplier = dict(zip(FIELDS, source, strict=True))
    data.append(
        {
            **supplier,
            "parameters": [{"name": "owned", "amount": 42}],
            "exchanges": [{**supplier, "amount": -1, "type": "production"}],
        }
    )
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.12")).migrate_background(
        target_context("2025"), policy=POLICY
    )
    assert result.data == data


@pytest.mark.parametrize("source,profile", [(s, p) for s, p in CASES if s[0].endswith(", reclamation")])
@pytest.mark.parametrize("native", (False, True))
@pytest.mark.parametrize("format_name", ("brightway", "simapro"))
def test_parameterized_reclamation_exports_keep_local_response(tmp_path, source, profile, native, format_name):
    from brightpath import SimaProInventory

    data = consumer(dict(zip(FIELDS, source, strict=True)), -1, 3)
    data[0]["parameters"] = [{"name": "dose", "amount": -1}]
    data[0]["comment"] = "Synthetic parameterized helper export fixture."
    converted = BrightwayInventory.from_data(
        data, context=contexts("ecoinvent", profile["label"].split("-")[1])
    ).migrate_background(target_context("native2026" if native else "2025"), policy=POLICY)
    if format_name == "brightway":
        path = converted.write_excel(tmp_path / "parameterized.xlsx")
        restored = BrightwayInventory.from_excel(path)
    else:
        categorized = deepcopy(converted.data)
        for d in categorized:
            d["exchanges"][0]["simapro category"] = "Materials/Refrigerants"
        path = (
            BrightwayInventory.from_data(categorized, context=converted.context)
            .to_simapro()
            .write_csv(tmp_path / "parameterized.csv")
        )
        restored = SimaProInventory.from_csv(
            path,
            background_profile=converted.background_profile,
            biosphere_profile=converted.context.background.biosphere,
        ).to_brightway()
    assert not restored.validate().has_errors
    original_root = helpers_by_id(converted)[profile["recipe_id"]]
    (root,) = [d for d in restored.data if all(d[f] == original_root[f] for f in FIELDS)]
    before = {p["name"]: p for p in profile["recipe"]["parameters"]}
    after = {p["name"]: p for p in root["parameters"]}
    assert after.keys() == before.keys()
    for name, parameter in before.items():
        assert after[name]["amount"] == pytest.approx(parameter["amount"])
        assert after[name].get("formula") == parameter.get("formula")
    values = parameter_values(root["parameters"])
    (e,) = [e for e in root["exchanges"] if "motor_power" in e.get("formula", "")]
    assert evaluate(e["formula"], {**values, "motor_power": values["motor_power"] * 2}) == pytest.approx(
        e["amount"] * 2
    )


@pytest.mark.parametrize("source,profile", CASES)
def test_authored_formulas_evaluate_with_the_brightway_parameter_engine(source, profile):
    from bw2parameters import ParameterSet

    recipe = profile["recipe"]
    definitions = {p["name"]: deepcopy(p) for p in recipe.get("parameters", [])}
    expressions = [deepcopy(e) for e in recipe["inputs"] if e.get("formula")]
    before = [e["amount"] for e in expressions]
    ParameterSet(definitions)(expressions)
    assert [e["amount"] for e in expressions] == pytest.approx(before)
