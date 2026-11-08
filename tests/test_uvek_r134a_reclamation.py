"""Reclamation allocation, actual processing and legitimate recovery credits remain distinct."""

from copy import deepcopy

import pytest
from test_uvek_reference_signs import FIELDS, consumer, contexts

from brightpath import BackgroundContext, BiosphereProfile, BrightwayInventory, TechnosphereProfile
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_release_resources, load_uvek_technosphere_resource
from brightpath.migrations.schema import validate_migration_resource
from scripts.review_uvek_r134a_reclamation import (
    ELECTRICITY_TARGET,
    RECLAMATION_SOURCES,
    REFRIGERANT_TARGET,
    reclamation_decision,
)
from scripts.review_uvek_refrigerant_disposal import DISPOSAL_TARGET
from scripts.review_uvek_venting import NATIVE_EMISSIONS

LABELS = [
    label
    for label in sorted({p["label"] for a in RECLAMATION_SOURCES.values() for p in a["profiles"]})
    if label.endswith("-cutoff")
]
POLICY = MigrationPolicy(on_heuristic_mapping=PolicyAction.WARN, on_information_loss=PolicyAction.WARN)


def batch(label, amount, *, products_only=False):
    data = None
    for source, approval in RECLAMATION_SOURCES.items():
        if label not in {p["label"] for p in approval["profiles"]} or (products_only and approval["kind"] == "service"):
            continue
        for kind, uncertainty in (("technosphere", 2), ("substitution", 3)):
            row = consumer(dict(zip(FIELDS, source, strict=True)), amount, uncertainty, kind)
            row[0]["exchanges"][1]["caller_metadata"] = {"retain": [1, 2]}
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


def target_context(kind):
    return BackgroundContext(
        TechnosphereProfile("uvek", "2025" if kind == "2025" else "2026", "cutoff"),
        BiosphereProfile("uvek", "2026") if kind == "native2026" else BiosphereProfile("ecoinvent", "3.10"),
    )


def supplier(target, release):
    if release == "2025":
        return target
    resource = next(
        r for r in load_uvek_release_resources("technosphere") if r["source_profiles"][0]["version"] == "2025"
    )
    return next(r["target"] for r in resource["replace"] if r["source"] == target)


@pytest.mark.parametrize("label", LABELS)
@pytest.mark.parametrize("target_kind", ["2025", "2026", "native2026"])
@pytest.mark.parametrize("amount", [-10, 0, 10])
def test_reclamation_preserves_allocation_processing_materials_signed_credits_and_full_provenance(
    label, target_kind, amount
):
    _, version, model = label.split("-")
    data = batch(label, amount)
    original = deepcopy(data)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", version, model))
    target = target_context(target_kind)
    result = inventory.migrate_background(target, policy=POLICY)
    assert len(result.data) == (3 if model == "cutoff" else 2)
    assert result.data[0]["exchanges"][0] == original[0]["exchanges"][0]
    assert result.data[0]["exchanges"][-1]["name"] == "Carbon dioxide, fossil"
    assert result.data[0]["exchanges"][-1]["amount"] == 0.5
    helpers = {h["code"]: h for h in result.data[1:]}
    changes = [c for c in result.last_migration_report.changes if c.code == "migration.foreground_recipe"]
    assert len(changes) == len(original[0]["exchanges"]) - 2
    for before, after, change in zip(
        original[0]["exchanges"][1:-1], result.data[0]["exchanges"][1:-1], changes, strict=True
    ):
        approval = RECLAMATION_SOURCES[tuple(before[f] for f in FIELDS)]
        helper = helpers[after["input"][1]]
        factor = -1 if approval["kind"] == "service" else 1
        assert after["amount"] == amount * factor
        assert after["type"] == before["type"] and after["caller_metadata"] == before["caller_metadata"]
        assert after["formula"] == ("(dose) * (-1.0)" if factor == -1 else "dose")
        assert after["scale"] == 0.2
        if before["uncertainty type"] == 2:
            assert after["loc"] == before["loc"]
            assert after["negative"] is ((amount >= 0) if factor == -1 else (amount < 0))
        else:
            assert after["loc"] == amount * factor
            assert after["minimum"] == amount * factor - 1 and after["maximum"] == amount * factor + 1
        assert change.to_dict()["before"] == before
        assert change.to_dict()["details"]["source_exchange"] == before
        assert helper["exchanges"][0]["type"] == "production" and helper["exchanges"][0]["amount"] == 1
        if approval["kind"] == "recovered_product":
            assert len(helper["exchanges"]) == 1
            assert helper["brightpath_conversion"]["zero_burden"] is True
            continue
        bio = [e for e in helper["exchanges"] if e["type"] == "biosphere"]
        assert len(bio) == 1 and bio[0]["amount"] == 0.001
        assert bio[0]["name"] == (
            NATIVE_EMISSIONS["r134a"] if target_kind == "native2026" else "1,1,1,2-Tetrafluoroethane"
        )
        tech = {tuple(e[f] for f in FIELDS): e["amount"] for e in helper["exchanges"] if e["type"] == "technosphere"}
        expected = {
            tuple(supplier(t, target.technosphere.version)[f] for f in FIELDS): qty
            for t, qty in [(ELECTRICITY_TARGET, 0.0466666666666667), (DISPOSAL_TARGET, 0.1)]
        }
        if model == "consequential":
            expected[tuple(supplier(REFRIGERANT_TARGET, target.technosphere.version)[f] for f in FIELDS)] = -0.899
        assert tech == expected
        assert len(helper["exchanges"]) == (5 if model == "consequential" else 4)
    assert len([i for i in result.last_migration_report.issues if i.code == "migration.zero_burden_supplier"]) == (
        2 if model == "cutoff" else 0
    )
    assert (
        len([i for i in result.last_migration_report.issues if i.code == "migration.technosphere_information_loss"])
        == 2
    )
    assert not any(i.code.endswith("link_unresolved") for i in result.last_migration_report.issues)
    assert inventory.data == data == original
    assert result.migrate_background(target).data == result.data


def test_empty_cutoff_products_keep_materials_and_warn_without_proxy_loss_or_a_loss_acceptance_requirement():
    data = batch("ecoinvent-3.12-cutoff", 3, products_only=True)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.12"))
    result = inventory.migrate_background(
        target_context("2025"), policy=MigrationPolicy(on_heuristic_mapping=PolicyAction.WARN)
    )
    assert len(result.data) == 2 and len(result.data[0]["exchanges"]) == len(data[0]["exchanges"])
    assert all(e["amount"] == 3 for e in result.data[0]["exchanges"][1:-1])
    assert len(result.data[1]["exchanges"]) == 1
    assert all(loss.code == "migration.heuristic_mapping" for loss in result.last_migration_report.losses)
    warnings = [i for i in result.last_migration_report.issues if i.code == "migration.zero_burden_supplier"]
    assert len(warnings) == 2 and all(i.severity.value == "warning" for i in warnings)


@pytest.mark.parametrize(
    "change",
    [
        "unapproved_empty",
        "missing_reason",
        "nonempty_zero",
        "consequential_zero",
        "negative_reference",
        "missing_profiles",
        "missing_evidence",
        "dependency",
        "orphan",
        "scaled_mass",
    ],
)
def test_empty_recipes_require_the_exact_reviewed_positive_cutoff_allocation_boundary(change):
    resource = deepcopy(load_uvek_technosphere_resource())
    rule = next(r for r in resource["replace"] if r.get("mapping_method") == "reviewed_r134a_cutoff_product_recipe")
    recipe_id = rule["target"]["recipe_ref"]
    recipe = resource["recipes"][recipe_id]
    entry = rule["reference_flow_sign_review"]["profiles"][0]
    if change == "unapproved_empty":
        recipe.pop("zero_burden")
    elif change == "missing_reason":
        recipe.pop("zero_burden_reason")
    elif change == "nonempty_zero":
        recipe["inputs"] = [
            {
                "type": "biosphere",
                "name": "Carbon dioxide, fossil",
                "categories": ["air"],
                "unit": "kilogram",
                "amount": 1,
            }
        ]
    elif change == "consequential_zero":
        entry["source_profile"]["system_model"] = "consequential"
    elif change == "negative_reference":
        entry["review"]["source_sign"] = -1
        entry["conversion_factor"] = -1
    elif change == "missing_profiles":
        rule["reference_flow_sign_review"] = {
            "status": "verified",
            "source_sign": 1,
            "target_sign": 1,
            "reason": "Unscoped",
            "evidence": ["synthetic"],
        }
    elif change == "missing_evidence":
        resource["reviews"][rule["review_ref"]].pop("evidence")
    elif change == "dependency":
        resource["recipes"]["unapproved_dependency"] = {
            **deepcopy(recipe),
            "inputs": [{"recipe": recipe_id, "amount": 1}],
        }
        resource["recipes"]["unapproved_dependency"].pop("zero_burden")
        resource["recipes"]["unapproved_dependency"].pop("zero_burden_reason")
    elif change == "orphan":
        resource["recipes"]["unapproved_zero"] = deepcopy(recipe)
    else:
        entry["conversion_factor"] = 2
    with pytest.raises(MigrationError):
        validate_migration_resource(resource)


def test_missing_profiles_and_unrelated_errors_roll_back_processing_and_empty_suppliers(unreviewed_uvek_correspondence):
    for version in ("3.9.1", "3.12"):
        data = batch("ecoinvent-3.9-cutoff" if version == "3.9.1" else "ecoinvent-3.12-cutoff", -1)
        if version == "3.12":
            data[0]["exchanges"].append({**unreviewed_uvek_correspondence, "amount": -1, "type": "technosphere"})
        before = deepcopy(data)
        inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", version))
        with pytest.raises(MigrationError) as caught:
            inventory.migrate_background(target_context("2025"), policy=MigrationPolicy.permissive())
        assert any(i.code == "migration.reference_flow_sign_unresolved" for i in caught.value.report.issues)
        assert inventory.data == data == before


def test_foreground_ownership_preserves_nonempty_local_suppliers_with_matching_names():
    data = batch("ecoinvent-3.12-cutoff", -1)
    for source, approval in RECLAMATION_SOURCES.items():
        if not any(p["label"] == "ecoinvent-3.12-cutoff" for p in approval["profiles"]):
            continue
        identity = dict(zip(FIELDS, source, strict=True))
        data.append(
            {
                **identity,
                "exchanges": [
                    {**identity, "amount": approval["source_reference_sign"], "type": "production"},
                    {
                        "name": "Carbon dioxide, fossil",
                        "categories": ["air"],
                        "unit": "kilogram",
                        "amount": 0.25,
                        "type": "biosphere",
                    },
                ],
            }
        )
    original = deepcopy(data)
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.12")).migrate_background(
        target_context("2025"), policy=POLICY
    )
    assert result.data == data == original
    assert not any(i.code == "migration.zero_burden_supplier" for i in result.last_migration_report.issues)


def test_changed_source_or_credited_market_evidence_requires_reinspection():
    source, approval = next((s, a) for s, a in RECLAMATION_SOURCES.items() if a["kind"] == "service")
    rule = {"source": dict(zip(FIELDS, source, strict=True)), "target": approval["previous_target"]}
    observations = [(p["label"], -1) for p in approval["profiles"]]
    inventories = [{"source": list(source), **p} for p in approval["profiles"]]
    assert reclamation_decision(rule, observations, inventories, "test", None)["factor"] == -1
    for key in ("inventory_sha256", "credit_market_inventory_sha256"):
        mutated = deepcopy(inventories)
        entry = next(p for p in mutated if key in p)
        entry[key] = "changed"
        with pytest.raises(ValueError, match="credited market changed"):
            reclamation_decision(rule, observations, mutated, "test", None)


@pytest.mark.parametrize("model", ["cutoff"])
@pytest.mark.parametrize("native", [False, True])
def test_service_and_production_only_helpers_round_trip_through_brightway_and_simapro(tmp_path, model, native):
    from brightpath import SimaProInventory

    data = batch(f"ecoinvent-3.12-{model}", -1)
    data[0]["comment"] = "Synthetic reclamation allocation export fixture."
    for exchange in data[0]["exchanges"][1:-1]:
        exchange.pop("formula")
        exchange["type"] = "technosphere"
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.12", model)).migrate_background(
        target_context("native2026" if native else "2025"), policy=POLICY
    )
    path = result.write_excel(tmp_path / "reclamation.xlsx")
    restored = BrightwayInventory.from_excel(path)
    assert len(restored.data) == len(result.data) and not restored.validate().has_errors
    categorized = deepcopy(result.data)
    for dataset in categorized:
        dataset["exchanges"][0]["simapro category"] = "Materials/Refrigerants"
    csv = (
        BrightwayInventory.from_data(categorized, context=result.context)
        .to_simapro()
        .write_csv(tmp_path / "reclamation.csv")
    )
    loaded = SimaProInventory.from_csv(
        csv, background_profile=result.background_profile, biosphere_profile=result.context.background.biosphere
    ).to_brightway()
    assert len(loaded.data) == len(result.data) and not loaded.validate().has_errors
    if model == "cutoff":
        assert len([h for h in loaded.data[1:] if len(h["exchanges"]) == 1]) == 1
    else:
        assert any(
            e["type"] == "technosphere" and e["amount"] == -0.899 for h in loaded.data[1:] for e in h["exchanges"]
        )
