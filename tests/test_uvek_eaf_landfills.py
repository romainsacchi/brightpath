"""Legacy steel-waste landfill proxies preserve waste demand and disclose wet-mass limits."""

from collections import defaultdict
from copy import deepcopy

import pytest
from test_uvek_reference_signs import FIELDS, consumer, contexts
from test_uvek_refrigerant_disposal import target_context

from brightpath import BrightwayInventory, SimaProInventory, TechnosphereProfile
from brightpath.background.reference_signs import select_profile_sign_rules
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_release_resources, load_uvek_technosphere_resource
from scripts.review_uvek_eaf_landfills import LANDFILLS, TARGETS, landfill_profiles

POLICY = MigrationPolicy(on_heuristic_mapping=PolicyAction.WARN, on_information_loss=PolicyAction.WARN)


def batch(amount):
    data = None
    for source in LANDFILLS:
        for kind in ("technosphere", "substitution"):
            for uncertainty in (2, 3, 4, 5):
                row = consumer(dict(zip(FIELDS, source, strict=True)), amount, uncertainty, kind)
                row[0]["exchanges"][1]["caller_metadata"] = {"preserve": ["wet mass", 1]}
                if data is None:
                    data = row
                else:
                    data[0]["exchanges"].append(row[0]["exchanges"][1])
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


@pytest.mark.parametrize("model", ["cutoff"])
@pytest.mark.parametrize("amount", [-10, 0, 10])
@pytest.mark.parametrize("release", ["2025", "native2026"])
def test_eaf_landfills_preserve_signs_uncertainty_formulas_and_foreground_emissions(model, amount, release):
    data = batch(amount)
    before = deepcopy(data)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.6", model))
    result = inventory.migrate_background(target_context(release), policy=POLICY)
    assert len(result.data) == 1 and result.data[0]["exchanges"][0] == before[0]["exchanges"][0]
    emission = result.data[0]["exchanges"][-1]
    assert emission["name"] == "Carbon dioxide, fossil" and emission["amount"] == 0.5
    assert emission["categories"] == (["emissions to air", "unspecified"] if release == "native2026" else ["air"])
    release_resource = (
        next(r for r in load_uvek_release_resources("technosphere") if r["source_profiles"][0]["version"] == "2025")
        if release != "2025"
        else None
    )
    report = result.last_migration_report
    warnings = {i.path: i for i in report.issues if i.code == "migration.technosphere_information_loss"}
    losses = {loss.path: loss for loss in report.losses if loss.code == "migration.technosphere_information_loss"}
    assert len(warnings) == len(losses) == 24
    for i, (old, new) in enumerate(
        zip(before[0]["exchanges"][1:-1], result.data[0]["exchanges"][1:-1], strict=True), start=1
    ):
        approval = LANDFILLS[tuple(old[f] for f in FIELDS)]
        target = dict(zip(FIELDS, approval["target"], strict=True))
        if release_resource:
            target = next(r["target"] for r in release_resource["replace"] if r["source"] == target)
        assert {f: new[f] for f in FIELDS} == target
        assert new["amount"] == -amount and new["type"] == old["type"]
        assert new["formula"] == "(dose) * (-1.0)" and new["caller_metadata"] == old["caller_metadata"]
        if old["uncertainty type"] == 2:
            assert new["loc"] == old["loc"] and new["negative"] is (amount >= 0)
        else:
            assert new["loc"] == -amount and (new["minimum"], new["maximum"]) == (-amount - 1, -amount + 1)
        if "scale" in old:
            assert new["scale"] == old["scale"]
        path = f"datasets[0].exchanges[{i}]"
        assert approval["limitations"] in warnings[path].message
        details = losses[path].to_dict()["details"]
        assert details["source"] == {f: old[f] for f in FIELDS}
        assert details["target"] == dict(zip(FIELDS, approval["target"], strict=True))
    assert data == inventory.data == before
    assert result.migrate_background(target_context(release)).data == result.data


def test_eaf_landfill_proxy_warnings_distinguish_material_boundaries():
    for source, approval in LANDFILLS.items():
        text = approval["limitations"]
        if "dust," in source[1]:
            assert "105410 ppm (10.541%)" in text and "15.4% water" in text
        else:
            assert "H2O unavailable" in text and "0% water" in text
        if "dust, alloyed" in source[1]:
            assert "cement stabilization" in text and "adds road and rail freight" in text
        assert "short- and long-term water releases" in text and "not established or quantified" in text


@pytest.mark.parametrize("version", ["3.6.1", "3.7", "3.12"])
def test_eaf_landfill_approval_is_not_extended_to_uninspected_versions(version):
    resource = select_profile_sign_rules(
        load_uvek_technosphere_resource(), TechnosphereProfile("ecoinvent", version, "cutoff")
    )
    rules = [r for r in resource["replace"] if tuple(r["source"][f] for f in FIELDS) in LANDFILLS]
    assert len(rules) == 3 and all(r["reference_flow_sign_review"]["status"] == "unresolved" for r in rules)


def test_eaf_landfill_loss_policy_foreground_ownership_and_rollback():
    data = batch(-1)
    before = deepcopy(data)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.6"))
    with pytest.raises(MigrationError) as caught:
        inventory.migrate_background(
            target_context("2025"),
            policy=MigrationPolicy(on_heuristic_mapping=PolicyAction.WARN, on_information_loss=PolicyAction.ERROR),
        )
    assert any(i.code == "migration.technosphere_information_loss" for i in caught.value.report.issues)
    assert inventory.data == data == before
    for source in LANDFILLS:
        supplier = dict(zip(FIELDS, source, strict=True))
        data.append({**supplier, "exchanges": [{**supplier, "amount": -1, "type": "production"}]})
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.6")).migrate_background(
        target_context("2025"), policy=POLICY
    )
    assert result.data == data
    assert not any(i.code == "migration.technosphere_information_loss" for i in result.last_migration_report.issues)


@pytest.mark.parametrize("target", list(TARGETS))
def test_eaf_landfill_reverse_remains_independently_unresolved(target):
    data = consumer(dict(zip(FIELDS, target, strict=True)), 1)
    inventory = BrightwayInventory.from_data(data, context=contexts("uvek", "2025"))
    with pytest.raises(MigrationError):
        inventory.migrate_background(contexts("ecoinvent", "3.12").background, policy=POLICY)
    assert inventory.data == data


@pytest.mark.parametrize("mutation", ["source", "target", "missing_profile", "sign"])
def test_eaf_landfill_authoring_rejects_changed_inventory_or_profile(mutation):
    inventories = defaultdict(list)
    observations = {}
    for source, approval in LANDFILLS.items():
        observations[source] = [(p["label"], -1) for p in approval["profiles"]]
        inventories[source] = [(p["label"], p["inventory_sha256"]) for p in approval["profiles"]]
    for target, row in TARGETS.items():
        inventories[target] = [("uvek-2025-cutoff", row["inventory_sha256"])]
    source = next(iter(LANDFILLS))
    assert len(landfill_profiles(source, observations[source], inventories, [])) == 2
    if mutation in ("source", "target"):
        key = source if mutation == "source" else LANDFILLS[source]["target"]
        label, _ = inventories[key][0]
        inventories[key][0] = (label, "0" * 64)
    elif mutation == "missing_profile":
        observations[source].pop()
    else:
        label, _ = observations[source][0]
        observations[source][0] = (label, 1)
    with pytest.raises(ValueError, match="Legacy EAF landfill inventory or profile changed"):
        landfill_profiles(source, observations[source], inventories, [])


@pytest.mark.parametrize("model", ["cutoff"])
def test_eaf_landfill_suppliers_round_trip_through_brightway_and_simapro(tmp_path, model):
    data = batch(-1)
    data[0]["comment"] = "Synthetic steel-waste landfill export fixture."
    for e in data[0]["exchanges"][1:-1]:
        e.pop("formula")
        e["type"] = "technosphere"
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.6", model)).migrate_background(
        target_context("2025"), policy=POLICY
    )
    restored = BrightwayInventory.from_excel(result.write_excel(tmp_path / "eaf.xlsx"))
    categorized = result.data
    categorized[0]["exchanges"][0]["simapro category"] = "Materials/Processing services"
    csv = BrightwayInventory.from_data(categorized, context=result.context).to_simapro().write_csv(tmp_path / "eaf.csv")
    loaded = SimaProInventory.from_csv(
        csv, background_profile=result.background_profile, biosphere_profile=result.context.background.biosphere
    ).to_brightway()
    expected = [(tuple(e[f] for f in FIELDS), e["amount"]) for e in result.data[0]["exchanges"][1:-1]]
    for inventory in (restored, loaded):
        assert len(inventory.data) == 1 and not inventory.validate().has_errors
        inputs = [e for e in inventory.data[0]["exchanges"] if e["type"] == "technosphere"]
        assert [(tuple(e[f] for f in FIELDS), e["amount"]) for e in inputs] == expected
