"""Approved unsupported exchanges are omitted with warnings and a complete audit."""

import json
import math
from copy import deepcopy

import pytest
from uvek_review_helpers import historical_forward_resource

from brightpath import (
    DATA_DIR,
    BackgroundContext,
    BiosphereProfile,
    BrightwayInventory,
    FormatProfile,
    InventoryContext,
    TechnosphereProfile,
)
from brightpath.background.execution import _apply_technosphere_deletions
from brightpath.background.uvek_export import load_uvek_export_resource
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.models import MigrationStepReport
from brightpath.migrations.resources import load_uvek_technosphere_resource
from brightpath.migrations.schema import validate_migration_resource
from scripts.review_uvek_reference_signs import (
    APPROVED_COMPOSTING_OMISSIONS,
    APPROVED_TECHNOSPHERE_OMISSIONS,
    APPROVED_WWT_RESIDUE_OMISSIONS,
    BIOWASTE_PREVIOUS_TARGET,
    BIOWASTE_SOURCE,
    REVIEW_PATH,
    apply_sign_review,
)

FIELDS = ("name", "reference product", "location", "unit")
SOURCES = list(APPROVED_TECHNOSPHERE_OMISSIONS)


def context(family, version, model="cutoff"):
    return InventoryContext(
        FormatProfile("brightway_excel"),
        BackgroundContext(TechnosphereProfile(family, version, model), BiosphereProfile("ecoinvent", "3.10")),
    )


def consumer(source, amount=-10, kind="technosphere"):
    output = {"name": "Omission consumer", "reference product": "output", "location": "CH", "unit": "unit"}
    row = {
        **dict(zip(FIELDS, source, strict=True)),
        "amount": amount,
        "type": kind,
        "uncertainty type": 2,
        "loc": math.log(abs(amount)) if amount else 0,
        "negative": amount < 0,
        "scale": 0.2,
        "formula": "untreated_volume",
        "audit_note": "original data",
    }
    return [{**output, "exchanges": [{**output, "type": "production", "amount": 1}, row]}]


def warning_policy(**overrides):
    return MigrationPolicy(on_heuristic_mapping=PolicyAction.WARN, **overrides)


@pytest.mark.parametrize(
    "source,version",
    [
        (source, version)
        for source, approval in APPROVED_TECHNOSPHERE_OMISSIONS.items()
        # The large WWT cohort has equivalent all-profile, sign, uncertainty and
        # provenance coverage in batched test_uvek_wwt_residue_omissions cases.
        if source not in APPROVED_WWT_RESIDUE_OMISSIONS
        for version in approval["source_versions"]
    ],
)
@pytest.mark.parametrize("model", ["cutoff"])
@pytest.mark.parametrize("amount", [-10, 10])
@pytest.mark.parametrize("kind", ["technosphere", "substitution"])
def test_approved_omission_continues_and_preserves_the_source_exchange_in_report(source, version, model, amount, kind):
    data = consumer(source, amount, kind)
    before = deepcopy(data)
    inventory = BrightwayInventory.from_data(data, context=context("ecoinvent", version, model))
    result = inventory.migrate_background(context("uvek", "2025").background, policy=warning_policy())
    assert result.data[0]["exchanges"] == [before[0]["exchanges"][0]]
    report = result.last_migration_report
    warnings = [i for i in report.issues if i.code == "migration.technosphere_omission"]
    assert len(warnings) == 1 and warnings[0].severity.value == "warning"
    assert warnings[0].path == "datasets[0].exchanges[1]"
    assert "not represented in the target dataset" in warnings[0].message
    losses = [loss for loss in report.losses if loss.code == "migration.technosphere_omission"]
    assert len(losses) == 1
    assert losses[0].to_dict()["details"]["source_exchange"] == before[0]["exchanges"][1]
    assert losses[0].details["approved_omission"] is True
    assert any(
        step["counts"]["technosphere_deletions"] == 1
        for stage in report.stages
        for step in stage.metrics.get("steps", [])
    )
    serialized_losses = [
        loss
        for stage in json.loads(report.to_json())["stages"]
        for loss in stage["losses"]
        if loss["code"] == "migration.technosphere_omission"
    ]
    assert serialized_losses[0]["details"]["source_exchange"]["amount"] == amount
    assert inventory.data == data == before


@pytest.mark.parametrize("on_deletion", list(PolicyAction))
@pytest.mark.parametrize("on_information_loss", list(PolicyAction))
def test_reviewed_omissions_warn_independently_of_generic_loss_policies(on_deletion, on_information_loss):
    data = consumer(SOURCES[0])
    inventory = BrightwayInventory.from_data(data, context=context("ecoinvent", "3.11"))
    # SOURCES[0] is direct disposal in GLO; all three ordinary policies are retained.
    result = inventory.migrate_background(
        context("uvek", "2025").background,
        policy=warning_policy(on_deletion=on_deletion, on_information_loss=on_information_loss),
    )
    assert len(result.data[0]["exchanges"]) == 1
    assert any(
        i.code == "migration.technosphere_omission" and i.severity.value == "warning"
        for i in result.last_migration_report.issues
    )


@pytest.mark.parametrize("source", SOURCES)
def test_omission_survives_uvek_2026_bridge_and_is_idempotent(source):
    version = APPROVED_TECHNOSPHERE_OMISSIONS[source]["source_versions"][-1]
    inventory = BrightwayInventory.from_data(consumer(source), context=context("ecoinvent", version))
    result = inventory.migrate_background(context("uvek", "2026").background, policy=warning_policy())
    assert len(result.data[0]["exchanges"]) == 1
    again = result.migrate_background(context("uvek", "2026").background, policy=warning_policy())
    assert again.data == result.data
    assert not any(i.code == "migration.technosphere_omission" for i in again.last_migration_report.issues)


def test_duplicates_zero_quantities_and_unrelated_emissions_are_handled_individually():
    data = consumer(SOURCES[0], 0)
    data[0]["exchanges"].append({**data[0]["exchanges"][1], "amount": -5, "formula": "other_volume"})
    emission = {
        "name": "Carbon dioxide, fossil",
        "categories": ["air"],
        "unit": "kilogram",
        "amount": 0.42,
        "type": "biosphere",
    }
    data[0]["exchanges"].append(emission)
    before = deepcopy(data)
    inventory = BrightwayInventory.from_data(data, context=context("ecoinvent", "3.11"))
    result = inventory.migrate_background(context("uvek", "2025").background, policy=warning_policy())
    assert result.data[0]["exchanges"] == [before[0]["exchanges"][0], emission]
    losses = [x for x in result.last_migration_report.losses if x.code == "migration.technosphere_omission"]
    assert [x.path for x in losses] == ["datasets[0].exchanges[1]", "datasets[0].exchanges[2]"]
    assert [x.details["source_exchange"]["amount"] for x in losses] == [0, -5]
    assert inventory.data == data == before


def test_foreground_links_and_production_are_not_omitted():
    supplier = dict(zip(FIELDS, SOURCES[0], strict=True))
    data = consumer(SOURCES[0])
    data.append({**supplier, "exchanges": [{**supplier, "amount": 1, "type": "production"}]})
    before = deepcopy(data)
    result = BrightwayInventory.from_data(data, context=context("ecoinvent", "3.11")).migrate_background(
        context("uvek", "2025").background, policy=warning_policy()
    )
    assert result.data == data == before
    assert not any(i.code == "migration.technosphere_omission" for i in result.last_migration_report.issues)


@pytest.mark.parametrize("kind", ["technosphere", "substitution"])
def test_omissions_accept_existing_cubic_meter_alias_and_audit_original_unit(kind):
    data = consumer(SOURCES[0], kind=kind)
    data[0]["exchanges"][1]["unit"] = "m3"
    before = deepcopy(data)
    inventory = BrightwayInventory.from_data(data, context=context("ecoinvent", "3.11"))
    # Exact source catalog validation is independent of migration-rule unit aliases.
    result = inventory.migrate_background(
        context("uvek", "2025").background, policy=warning_policy(validate_source=False)
    )
    assert len(result.data[0]["exchanges"]) == 1
    loss = next(x for x in result.last_migration_report.losses if x.code == "migration.technosphere_omission")
    assert loss.details["source_exchange"]["unit"] == "m3"
    assert inventory.data == data == before


@pytest.mark.parametrize("field", FIELDS)
def test_omission_does_not_match_another_identity_or_quantity_basis(field):
    data = consumer(SOURCES[0])
    data[0]["exchanges"][1][field] = "liter" if field == "unit" else "Unreviewed"
    before = deepcopy(data)
    report = MigrationStepReport("source", "target", "forward", "synthetic")
    losses = _apply_technosphere_deletions(data, load_uvek_technosphere_resource(), warning_policy(), report, 0)
    assert data == before and not losses and not report.issues and report.technosphere_deletions == 0


def test_unrelated_failure_rolls_back_omission(monkeypatch):
    data = consumer(SOURCES[0])
    data[0]["exchanges"].append(
        {
            "name": "treatment of waste polyethylene terephthalate, dust, municipal incineration",
            "reference product": "waste polyethylene terephthalate, dust",
            "location": "RER",
            "unit": "kilogram",
            "type": "technosphere",
            "amount": -1,
        }
    )
    # Keep this rollback fixture independent of subsequent real supplier approvals.
    resource = deepcopy(load_uvek_technosphere_resource())
    rule = next(
        rule
        for rule in resource["replace"]
        if rule["source"]["name"] == data[0]["exchanges"][-1]["name"] and rule["source"]["unit"] == "kilogram"
    )
    rule["reference_flow_sign_review"] = {
        "status": "unresolved",
        "reason": "Synthetic unresolved supplier review for transactional rollback.",
        "evidence": ["Synthetic test evidence; not a change to the approved real correspondence."],
    }
    monkeypatch.setattr("brightpath.background.migration.load_uvek_technosphere_resource", lambda: resource)
    monkeypatch.setattr("brightpath.background.execution.load_uvek_technosphere_resource", lambda: resource)
    source = next(source for source in SOURCES if source[2] == "BD")
    data[0]["exchanges"][1].update(dict(zip(FIELDS, source, strict=True)))
    before = deepcopy(data)
    inventory = BrightwayInventory.from_data(data, context=context("ecoinvent", "3.12"))
    with pytest.raises(MigrationError) as caught:
        inventory.migrate_background(context("uvek", "2025").background, policy=MigrationPolicy.permissive())
    assert "migration.reference_flow_sign_unresolved" in {i.code for i in caught.value.report.issues}
    assert any(stage.metrics.get("rolled_back") for stage in caught.value.report.stages)
    assert inventory.data == data == before


@pytest.mark.parametrize("action", list(PolicyAction))
def test_unreviewed_deletions_keep_the_generic_deletion_policy(action):
    data = consumer(SOURCES[0])
    before = deepcopy(data)
    resource = {"delete": [{"source": dict(zip(FIELDS, SOURCES[0], strict=True))}], "reviews": {}}
    report = MigrationStepReport("source", "target", "forward", "synthetic")
    losses = _apply_technosphere_deletions(data, resource, MigrationPolicy(on_deletion=action), report, 0)
    assert (
        len(losses) == 1
        and report.issues[0].severity == {"error": "error", "warn": "warning", "allow": "info"}[action.value]
    )
    assert data == before if action is PolicyAction.ERROR else len(data[0]["exchanges"]) == 1


@pytest.mark.parametrize("problem", ["missing_review", "false", "replace", "overlap"])
def test_approved_omission_schema_rejects_unaudited_or_conflicting_rules(problem):
    resource = deepcopy(load_uvek_technosphere_resource())
    rule = resource["delete"][0]
    if problem == "missing_review":
        del rule["review_ref"]
    elif problem == "false":
        rule["approved_omission"] = False
    elif problem == "replace":
        resource["replace"][0]["approved_omission"] = True
    else:
        resource["replace"].append(
            {
                "id": "conflicting",
                "source": deepcopy(rule["source"]),
                "target": deepcopy(resource["replace"][0]["target"]),
            }
        )
    with pytest.raises(MigrationError):
        validate_migration_resource(resource)


def test_historical_omission_replay_is_idempotent_and_rejects_changed_sources():
    resource = deepcopy(historical_forward_resource())
    ledger = json.loads((DATA_DIR.parent.parent / REVIEW_PATH).read_text(encoding="utf-8"))
    before = deepcopy(resource)
    apply_sign_review(resource, ledger)
    assert resource == before
    resource["delete"][0]["source"]["location"] = "Unreviewed"
    with pytest.raises(ValueError, match="Stale"):
        apply_sign_review(resource, ledger)


@pytest.mark.parametrize("source", APPROVED_COMPOSTING_OMISSIONS)
@pytest.mark.parametrize("version", ["2025", "2026"])
@pytest.mark.parametrize("amount", [-10, 0, 10])
def test_composting_omission_discloses_excluded_burdens_and_empty_placeholder(source, version, amount):
    data = consumer(source, amount)
    before = deepcopy(data)
    inventory = BrightwayInventory.from_data(data, context=context("ecoinvent", "3.12"))
    result = inventory.migrate_background(context("uvek", version).background, policy=warning_policy())
    assert result.data[0]["exchanges"] == before[0]["exchanges"][:1]
    warning = next(i for i in result.last_migration_report.issues if i.code == "migration.technosphere_omission")
    assert warning.severity.value == "warning"
    for term in ("empty cut-off placeholder", "energy", "infrastructure", "methane", "nitrous oxide", "ammonia"):
        assert term in warning.message
    loss = next(x for x in result.last_migration_report.losses if x.code == "migration.technosphere_omission")
    assert loss.details["source_exchange"] == before[0]["exchanges"][1]
    assert inventory.data == data == before


def test_composting_omissions_preserve_historical_targets_and_exclude_other_biowaste_functions():
    resource = load_uvek_technosphere_resource()
    ledger = json.loads((DATA_DIR.parent.parent / REVIEW_PATH).read_text(encoding="utf-8"))
    for rule in resource["delete"]:
        source = tuple(rule["source"][field] for field in FIELDS)
        if source not in APPROVED_COMPOSTING_OMISSIONS:
            continue
        decision = ledger["resources"][resource["name"]][rule["id"]]
        assert tuple(decision["original_rule"]["target"][field] for field in FIELDS) == BIOWASTE_SOURCE
        assert decision["uvek_native_flow_type"] == "PRODUCT_FLOW"
        assert len(decision["source_observations"]) == 14
        assert all(sign == -1 for _, sign in decision["source_observations"])
        assert any("bafu-509472" in entry for entry in decision["omission_review"]["evidence"])
    other_biowaste = []
    for rule in [*resource["replace"], *resource["delete"]]:
        if tuple(rule["source"][field] for field in FIELDS) in APPROVED_COMPOSTING_OMISSIONS:
            continue
        later = ledger["resources"][resource["name"]].get(rule["id"], {})
        original = later.get("original_rule", rule)
        target = original.get("target", {})
        if tuple(target.get(field) for field in FIELDS) != BIOWASTE_SOURCE:
            continue
        other_biowaste.append(rule)
        # The composting approval cannot authorize another function. A later
        # independent review may supersede that function's historical proxy.
        if later.get("operation") == "recipe":
            assert rule["target"] == later["target"] and later["recipes"]
        elif later.get("operation") == "delete":
            assert rule["approved_omission"] and later["omission_review"]["evidence"]
        else:
            assert rule["reference_flow_sign_review"]["status"] == "unresolved"
    assert len(other_biowaste) == 15


@pytest.mark.parametrize("version", ["2025", "2026"])
@pytest.mark.parametrize("amount", [-10, 0, 10])
@pytest.mark.parametrize("kind", ["technosphere", "substitution"])
def test_reverse_biowaste_omission_warns_about_collection_and_uptake_and_preserves_source(version, amount, kind):
    data = consumer(BIOWASTE_SOURCE, amount, kind)
    data[0]["exchanges"][1]["formula"] = "collected_mass"
    before = deepcopy(data)
    inventory = BrightwayInventory.from_data(data, context=context("uvek", version))
    result = inventory.migrate_background(context("ecoinvent", "3.12").background)
    assert result.data[0]["exchanges"] == [before[0]["exchanges"][0]]
    report = result.last_migration_report
    warnings = [i for i in report.issues if i.code == "migration.technosphere_omission"]
    losses = [x for x in report.losses if x.code == "migration.technosphere_omission"]
    assert len(warnings) == len(losses) == 1
    assert warnings[0].severity.value == "warning"
    assert "collection transport" in warnings[0].message and "CO2 uptake" in warnings[0].message
    snapshot = losses[0].to_dict()["details"]["source_exchange"]
    original = before[0]["exchanges"][1]
    assert {key: snapshot[key] for key in original} == original
    # The 2026 release bridge can add the canonical legacy product alias.
    assert set(snapshot) - set(original) <= {"product"}
    assert any(stage.metrics.get("omitted_external_exchanges") == 1 for stage in report.stages)
    assert report.metadata["committed"] is True
    assert inventory.data == data == before
    again = result.migrate_background(context("ecoinvent", "3.12").background)
    assert again.data == result.data and not again.last_migration_report.losses


@pytest.mark.parametrize("on_deletion", list(PolicyAction))
@pytest.mark.parametrize("on_information_loss", list(PolicyAction))
def test_reverse_approved_omission_warns_under_all_generic_loss_policies(on_deletion, on_information_loss):
    inventory = BrightwayInventory.from_data(consumer(BIOWASTE_SOURCE, 10), context=context("uvek", "2025"))
    result = inventory.migrate_background(
        context("ecoinvent", "3.12").background,
        policy=MigrationPolicy(on_deletion=on_deletion, on_information_loss=on_information_loss),
    )
    assert len(result.data[0]["exchanges"]) == 1
    assert any(
        i.code == "migration.technosphere_omission" and i.severity.value == "warning"
        for i in result.last_migration_report.issues
    )


def test_reverse_omission_preserves_foreground_uptake_and_other_inputs():
    from brightpath.background import PackageCatalogProvider

    biosphere = PackageCatalogProvider().load_biosphere(BiosphereProfile("ecoinvent", "3.10")).identities
    name, categories, unit = next(row for row in biosphere if row[0] == "Carbon dioxide, in air")
    data = consumer(BIOWASTE_SOURCE, 10)
    uptake = {"name": name, "categories": list(categories), "unit": unit, "amount": 0.42, "type": "biosphere"}
    data[0]["exchanges"].append(uptake)
    before = deepcopy(data)
    inventory = BrightwayInventory.from_data(data, context=context("uvek", "2025"))
    result = inventory.migrate_background(context("ecoinvent", "3.12").background)
    assert result.data[0]["exchanges"] == [before[0]["exchanges"][0], uptake]
    assert inventory.data == data == before


def test_reverse_omission_preserves_foreground_biowaste_supplier():
    supplier = dict(zip(FIELDS, BIOWASTE_SOURCE, strict=True))
    data = consumer(BIOWASTE_SOURCE, 10)
    data.append({**supplier, "exchanges": [{**supplier, "amount": 1, "type": "production"}]})
    before = deepcopy(data)
    result = BrightwayInventory.from_data(data, context=context("uvek", "2025")).migrate_background(
        context("ecoinvent", "3.12").background
    )
    assert result.data == data == before
    assert not any(i.code == "migration.technosphere_omission" for i in result.last_migration_report.issues)


def test_reverse_unrelated_unresolved_supplier_rolls_back_omission():
    from brightpath.background import PackageCatalogProvider

    resource = load_uvek_export_resource()
    covered = {tuple(rule["source"][f] for f in FIELDS) for op in ("replace", "delete") for rule in resource[op]}
    catalog = PackageCatalogProvider().load_technosphere(context("uvek", "2025").background.technosphere).identities
    unknown = min(catalog - covered)
    data = consumer(BIOWASTE_SOURCE, 10)
    data[0]["exchanges"].append({**dict(zip(FIELDS, unknown, strict=True)), "amount": 1, "type": "technosphere"})
    before = deepcopy(data)
    inventory = BrightwayInventory.from_data(data, context=context("uvek", "2025"))
    with pytest.raises(MigrationError) as caught:
        inventory.migrate_background(context("ecoinvent", "3.12").background, policy=MigrationPolicy.permissive())
    assert "migration.uvek_supplier_unresolved" in {i.code for i in caught.value.report.issues}
    assert caught.value.report.metadata["committed"] is False and not caught.value.report.changes
    assert any(stage.metrics.get("rolled_back") for stage in caught.value.report.stages)
    assert inventory.data == data == before


def test_reverse_omission_does_not_invert_into_forward_biowaste_omission():
    data = consumer(BIOWASTE_PREVIOUS_TARGET, -10)
    inventory = BrightwayInventory.from_data(data, context=context("ecoinvent", "3.12"))
    with pytest.raises(MigrationError) as caught:
        inventory.migrate_background(context("uvek", "2025").background, policy=MigrationPolicy.permissive())
    assert "migration.reference_flow_sign_unresolved" in {i.code for i in caught.value.report.issues}
    assert inventory.data == data


@pytest.mark.parametrize("version", ["3.11", "3.10.1"])
def test_reverse_omission_is_carried_into_older_ecoinvent_routes(version):
    data = consumer(BIOWASTE_SOURCE, 10)
    inventory = BrightwayInventory.from_data(data, context=context("uvek", "2025"))
    result = inventory.migrate_background(context("ecoinvent", version).background, policy=MigrationPolicy.permissive())
    assert len(result.data[0]["exchanges"]) == 1
    assert any(i.code == "migration.technosphere_omission" for i in result.last_migration_report.issues)
    assert inventory.data == data


def test_reverse_omission_audit_replays_and_records_collection_evidence():
    resource = deepcopy(load_uvek_export_resource())
    ledger = json.loads((DATA_DIR.parent.parent / REVIEW_PATH).read_text(encoding="utf-8"))
    before = deepcopy(resource)
    apply_sign_review(resource, ledger)
    assert resource == before
    rule = resource["delete"][0]
    decision = ledger["resources"][resource["name"]][rule["id"]]
    assert decision["operation"] == "delete"
    assert tuple(decision["original_rule"]["target"][f] for f in FIELDS) == BIOWASTE_PREVIOUS_TARGET
    assert decision["uvek_native_flow_type"] == "PRODUCT_FLOW"
    assert decision["source_observations"] == [["ecoinvent-3.12-cutoff", -1]]
    assert "CO2 uptake" in decision["omission_review"]["rationale"]
