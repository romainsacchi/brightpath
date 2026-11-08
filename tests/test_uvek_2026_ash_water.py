"""Approved water proxies preserve mass and disclose each lost chemical/compartment detail."""

import json
import math
from copy import deepcopy

import pytest

from brightpath import InventoryContext
from brightpath.background import PackageCatalogProvider
from brightpath.background.execution import execute_background_migration
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.migrations.resources import load_uvek_release_resources
from scripts.review_uvek_2026_ash_water import (
    METHOD,
    REPORT,
    apply_ash_water_review,
    before_ash_water_review,
    validate_report,
)
from scripts.review_uvek_2026_biosphere import identity
from scripts.review_uvek_2026_particulates import before_particulate_review
from tests.test_uvek_2026_biosphere_review import CATALOGS, context, document

REVIEW = json.loads(REPORT.read_text())
POLICY = MigrationPolicy(
    on_heuristic_mapping=PolicyAction.WARN,
    on_information_loss=PolicyAction.WARN,
    on_inferred_reverse=PolicyAction.WARN,
)


def test_exact_56_directional_additions_preserve_all_previous_rules_and_independent_reverse():
    current = before_particulate_review(list(load_uvek_release_resources("biosphere")))
    before = before_ash_water_review(current)
    saved = deepcopy(before)
    assert apply_ash_water_review(before, REPORT, CATALOGS) == current
    assert before == saved
    for old, new in zip(before, current, strict=True):
        if new["source_profiles"][0]["family"] == "uvek":
            assert new == old
            continue
        assert len(new["replace"]) == 1354 and len(new["unresolved"]) == 3008
        assert [r for r in new["replace"] if r.get("mapping_method") != METHOD] == old["replace"]
        assert all(new[k] == old[k] for k in ("delete", "disaggregate", "recipes"))
        assert len(new["reviews"]) == len(old["reviews"]) + 1


@pytest.mark.parametrize("version", ("3.10", "3.12"))
@pytest.mark.parametrize("uncertainty", (0, 2, 3, 4))
@pytest.mark.parametrize("amount", (-2, 0, 2))
def test_all_proxies_preserve_signed_kg_uncertainty_formulas_metadata_and_report_original_cas(
    version, uncertainty, amount
):
    original = document(REVIEW["decisions"])
    data = original.data
    for e in data[0]["exchanges"][1:]:
        e.update(amount=amount, formula="dose", minimum=-3, maximum=3, scale=0.3, **{"uncertainty type": uncertainty})
        e["loc"] = math.log(2) if uncertainty == 2 else amount
        if uncertainty == 2:
            e["negative"] = amount < 0
    original = original.replace(
        data=data, context=InventoryContext(original.context.format, context(("ecoinvent", version)))
    )
    saved = deepcopy(original.data)
    result = execute_background_migration(original, context(("uvek", "2026")), PackageCatalogProvider(), POLICY)
    assert result.succeeded, result.report.to_json()
    assert len(result.value.data) == 1 and len(result.value.data[0]["exchanges"]) == 57
    for decision, before, after in zip(
        REVIEW["decisions"], saved[0]["exchanges"][1:], result.value.data[0]["exchanges"][1:], strict=True
    ):
        assert identity(after) == identity(decision["target"])
        for key in (
            "amount",
            "formula",
            "minimum",
            "maximum",
            "loc",
            "scale",
            "uncertainty type",
            "negative",
            "custom",
        ):
            assert after.get(key) == before.get(key)
    losses = [loss for loss in result.report.losses if loss.code == "migration.biosphere_information_loss"]
    assert len(losses) == 56
    by_source = {identity(loss.details["source"]): loss for loss in losses}
    for row in REVIEW["decisions"]:
        loss = by_source[identity(row["source"])]
        assert row["source_uuid"] in loss.message and (row["source_cas"] or "unspecified") in loss.message
        assert "No emissions are omitted" in loss.message
        assert loss.details["policy_action"] == "warn"
    assert original.data == saved
    repeated = execute_background_migration(result.value, context(("uvek", "2026")), PackageCatalogProvider())
    assert repeated.succeeded and repeated.value.data == result.value.data


def test_loss_rejection_rolls_back_without_dropping_any_emission():
    original = document(REVIEW["decisions"])
    original = original.replace(context=InventoryContext(original.context.format, context(("ecoinvent", "3.10"))))
    saved = deepcopy(original.data)
    result = execute_background_migration(original, context(("uvek", "2026")), PackageCatalogProvider())
    assert not result.succeeded and result.value is original
    assert result.value.data == saved
    assert any(loss.code == "migration.biosphere_information_loss" for loss in result.report.losses)


@pytest.mark.parametrize("change", ("chemical", "compartment", "factor", "cas", "uuid", "missing", "duplicate"))
def test_chemical_mass_cas_compartment_or_exact_scope_changes_need_new_scientific_review(change):
    report = deepcopy(REVIEW)
    row = report["decisions"][0]
    if change == "chemical":
        row["target"]["name"] = "Chromium VI"
    elif change == "compartment":
        row["target"]["categories"] = ["emissions to water", "river"]
    elif change == "factor":
        row["conversion_factor"] = 0.1
    elif change == "cas":
        row["source_cas"] = "000000-00-0"
    elif change == "uuid":
        row["source_uuid"] = "unreviewed"
    elif change == "missing":
        report["decisions"].pop()
    else:
        report["decisions"].append(deepcopy(row))
    with pytest.raises(ValueError, match="repeat the scientific review"):
        validate_report(report, CATALOGS)
