"""Native CHP, diesel operation and heavy freight retain reviewed reference bases."""

import json
import math
from copy import deepcopy

import pytest
from test_uvek_reference_signs import consumer, contexts
from uvek_review_helpers import compiled_authoring_stage_resource

from brightpath import DATA_DIR, BrightwayInventory, TechnosphereProfile
from brightpath.background.rule_approvals import compatibility_rule_fingerprint
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource
from brightpath.migrations.scaling import scale_exchange

REVIEW = json.loads((DATA_DIR / "export/uvek_metals_38_operating_services_review.json").read_text(encoding="utf-8"))


def policy(version="3.8"):
    resource = load_uvek_technosphere_resource()
    return MigrationPolicy(
        approved_compatibility_rules=tuple(
            compatibility_rule_fingerprint(
                resource,
                "replace",
                row["compiled_rule"],
                TechnosphereProfile("ecoinvent", version, "cutoff"),
                TechnosphereProfile(**REVIEW["target_rule_profile"]),
            )
            for row in REVIEW["records"]
        ),
        on_information_loss=PolicyAction.WARN,
    )


@pytest.mark.parametrize("amount", [-2.5, 2.5])
@pytest.mark.parametrize("uncertainty", [2, 3])
@pytest.mark.parametrize("release", ["2025", "2026"])
def test_complete_native_operations_scale_only_reviewed_reference_basis(amount, uncertainty, release):
    data = []
    for i, row in enumerate(REVIEW["records"]):
        activity = consumer(row["original_rule"]["source"], amount, uncertainty)[0]
        activity["name"] += str(i)
        activity["exchanges"][0]["name"] = activity["name"]
        activity["exchanges"].append(
            {
                "type": "biosphere",
                "name": "Carbon dioxide, fossil",
                "categories": ["air"],
                "unit": "kilogram",
                "amount": 0.5,
            }
        )
        data.append(activity)
    before = deepcopy(data)
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.8")).migrate_background(
        contexts("uvek", release).background, policy=policy()
    )
    assert data == before and len(result.data) == len(REVIEW["records"]) == 5
    for output, original, row in zip(result.data, before, REVIEW["records"], strict=True):
        actual = output["exchanges"][1]
        assert all(actual[k] == v for k, v in row["compiled_rule"]["target"].items())
        expected = deepcopy(original["exchanges"][1])
        scale_exchange(expected, row["conversion_factor"])
        for key in ("amount", "formula", "uncertainty type", "loc", "scale", "negative", "minimum", "maximum"):
            if key in expected:
                assert actual[key] == expected[key]
        assert output["exchanges"][-1] == original["exchanges"][-1]


def test_machine_hour_conversion_preserves_audited_diesel_draw_and_freight_is_tonne_km():
    rows = {r["kind"]: r for r in REVIEW["records"]}
    machine = rows["diesel_machine_fuel_proxy"]
    facts = machine["boundary_checks"]
    assert math.isclose(
        machine["conversion_factor"] * facts["native_diesel_kg_per_MJ"], facts["source_diesel_kg_per_hour"]
    )
    assert machine["original_rule"]["source"]["unit"] == "hour"
    assert machine["compiled_rule"]["target"]["unit"] == "megajoule"
    assert not facts["equal_mechanical_work_claimed"]
    for kind in ("allocated_gas_CHP_heat", "allocated_gas_CHP_electricity"):
        assert rows[kind]["boundary_checks"]["same_named_1MWe_lean_burn_technology"]
        assert rows[kind]["boundary_checks"]["same_exergy_allocation"]
        assert "1MWe lean burn, allocation exergy" in rows[kind]["compiled_rule"]["target"]["name"]
    assert rows["heavy_lorry_freight"]["compiled_rule"]["target"]["unit"] == "ton kilometer"
    assert REVIEW["new_helpers"] == REVIEW["new_physical_omissions"] == 0
    assert all(r["source_witness"]["source_database"] == "ecoinvent-3.8-cutoff" for r in REVIEW["records"])


def test_new_38_approvals_do_not_authorize_310():
    row = REVIEW["records"][0]
    data = consumer(row["original_rule"]["source"], -1)
    before = deepcopy(data)
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.10")).migrate_background(
            contexts("uvek", "2026").background, policy=policy()
        )
    assert data == before


def test_old_39_generator_supplier_and_MJ_basis_are_retained():
    row = next(r for r in REVIEW["records"] if r["kind"] == "burned_diesel_generator_service")
    data = consumer(row["original_rule"]["source"], -2)
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.9")).migrate_background(
        contexts("uvek", "2026").background, policy=policy("3.9")
    )
    edge = result.data[0]["exchanges"][1]
    assert all(edge[k] == v for k, v in row["original_rule"]["target"].items())
    assert edge["amount"] == -2 and edge["unit"] == "megajoule"
    assert row["boundary_checks"]["source_diesel_kg_per_MJ"] == 0.0234


def test_replay_is_atomic_idempotent_and_rejects_unreviewed_parent():
    from scripts.review_uvek_metals_38_operating_services import apply_metals_38_operating_services_review

    resource = compiled_authoring_stage_resource(
        "review_uvek_metals_38_operating_services", "apply_metals_38_operating_services_review"
    )
    before = deepcopy(resource)
    apply_metals_38_operating_services_review(resource)
    assert resource == before
    for row in REVIEW["records"]:
        rule = next(r for r in resource["replace"] if r["id"] == row["rule_id"])
        rule.clear()
        rule.update(deepcopy(row["original_rule"]))
    apply_metals_38_operating_services_review(resource)
    assert resource == before
    last = next(r for r in resource["replace"] if r["id"] == REVIEW["records"][-1]["rule_id"])
    last["confidence"] = 0.1
    changed = deepcopy(resource)
    with pytest.raises(ValueError):
        apply_metals_38_operating_services_review(resource)
    assert resource == changed
