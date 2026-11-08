"""Exact particulate correspondences preserve size, location and signed caller quantities."""

import json
import math
from copy import deepcopy

import pytest

from brightpath import InventoryContext
from brightpath.background import PackageCatalogProvider
from brightpath.background.execution import execute_background_migration
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.migrations.resources import load_uvek_release_resources
from scripts.review_uvek_2026_biosphere import identity
from scripts.review_uvek_2026_chemical_aliases import before_chemical_alias_review
from scripts.review_uvek_2026_particulates import (
    METHOD,
    REPORT,
    apply_particulate_review,
    before_particulate_review,
    validate_report,
)
from tests.test_uvek_2026_biosphere_review import CATALOGS, context, document

REVIEW = json.loads(REPORT.read_text())
VERSIONS = ("3.6", "3.7", "3.8", "3.9", "3.9.1", "3.10", "3.10.1", "3.11", "3.12")
POLICY = MigrationPolicy(on_inferred_reverse=PolicyAction.WARN)


def ecoinvent_flows(version):
    catalog = json.loads((CATALOGS / f"ecoinvent__{version}__cutoff.json").read_text())["biosphere"]
    by_uuid = {row["flow_id"]: row for row in catalog}
    return [
        {key: by_uuid[row["source_uuid"]][key] for key in ("name", "categories", "unit")} for row in REVIEW["decisions"]
    ]


def test_thirteen_exact_additions_preserve_all_previous_rules_and_publisher_uuid_reverse_bindings():
    current = before_chemical_alias_review(list(load_uvek_release_resources("biosphere")))
    before = before_particulate_review(current)
    saved = deepcopy(before)
    assert apply_particulate_review(before, REPORT, CATALOGS) == current
    assert before == saved
    for old, new in zip(before, current, strict=True):
        if new["source_profiles"][0]["family"] == "uvek":
            assert old == new
            continue
        assert len(new["replace"]) == 1367 and len(new["unresolved"]) == 2995
        assert [r for r in new["replace"] if r.get("mapping_method") != METHOD] == old["replace"]
        assert all(new[k] == old[k] for k in ("delete", "disaggregate", "recipes"))
        assert len(new["reviews"]) == len(old["reviews"]) + 1


@pytest.mark.parametrize("reverse", (False, True))
@pytest.mark.parametrize("version", VERSIONS)
@pytest.mark.parametrize("uncertainty", (0, 2, 3, 4))
@pytest.mark.parametrize("amount", (-2, 0, 2))
def test_all_size_bins_and_air_contexts_preserve_signed_mass_uncertainty_formulas_and_owned_data(
    reverse, version, uncertainty, amount
):
    version_flows = ecoinvent_flows(version)
    rules = [
        {"source": row["target"] if reverse else flow}
        for row, flow in zip(REVIEW["decisions"], version_flows, strict=True)
    ]
    original = document(rules)
    data = original.data
    for e in data[0]["exchanges"][1:]:
        e.update(amount=amount, formula="dose", minimum=-3, maximum=3, scale=0.3, **{"uncertainty type": uncertainty})
        e["loc"] = math.log(2) if uncertainty == 2 else amount
        if uncertainty == 2:
            e["negative"] = amount < 0
    source = ("uvek", "2026") if reverse else ("ecoinvent", version)
    target = ("ecoinvent", version) if reverse else ("uvek", "2026")
    original = original.replace(data=data, context=InventoryContext(original.context.format, context(source)))
    saved = deepcopy(original.data)
    result = execute_background_migration(original, context(target), PackageCatalogProvider(), POLICY)
    assert result.succeeded, result.report.to_json()
    assert len(result.value.data) == 1 and len(result.value.data[0]["exchanges"]) == 14
    assert result.value.data[0]["exchanges"][0] == saved[0]["exchanges"][0]
    for row, version_flow, before, after in zip(
        REVIEW["decisions"],
        version_flows,
        saved[0]["exchanges"][1:],
        result.value.data[0]["exchanges"][1:],
        strict=True,
    ):
        expected = version_flow if reverse else row["target"]
        assert identity(after) == identity(expected)
        for field in (
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
            assert after.get(field) == before.get(field)
    assert all(loss.code == "migration.inferred_reverse" for loss in result.report.losses)
    assert original.data == saved
    repeated = execute_background_migration(result.value, context(target), PackageCatalogProvider())
    assert repeated.succeeded and repeated.value.data == result.value.data


def test_exact_direct_route_does_not_need_a_proxy_or_loss_policy_and_foreground_bins_are_not_combined():
    original = document(REVIEW["decisions"])
    original = original.replace(context=InventoryContext(original.context.format, context(("ecoinvent", "3.10"))))
    saved = deepcopy(original.data)
    result = execute_background_migration(original, context(("uvek", "2026")), PackageCatalogProvider())
    assert result.succeeded and not result.report.losses
    assert len(result.value.data[0]["exchanges"]) == 14
    assert original.data == saved


@pytest.mark.parametrize("change", ("size", "context", "mass", "factor", "uuid", "definition", "missing", "duplicate"))
def test_size_compartment_mass_and_definition_drift_cannot_inherit_label_approval(change):
    report = deepcopy(REVIEW)
    row = report["decisions"][0]
    if change == "size":
        row["target"]["name"] = "Particulates, < 10 um"
    elif change == "context":
        row["target"]["categories"] = ["emissions to air", "low. pop."]
    elif change == "mass":
        row["target"]["unit"] = "gram"
    elif change == "factor":
        row["conversion_factor"] = 1000
    elif change == "uuid":
        row["source_uuid"] = "unreviewed"
    elif change == "definition":
        row["native_definition"]["flow_definition"]["name"] = "Particulates"
    elif change == "missing":
        report["decisions"].pop()
    else:
        report["decisions"].append(deepcopy(row))
    with pytest.raises(ValueError, match="repeat the independent review"):
        validate_report(report, CATALOGS)


def test_projection_and_independent_inverse_drift_are_rejected():
    current = before_chemical_alias_review(deepcopy(list(load_uvek_release_resources("biosphere"))))
    incoming = next(r for r in current if r["source_profiles"][0]["family"] == "ecoinvent")
    next(r for r in incoming["replace"] if r.get("mapping_method") == METHOD)["conversion_factor"] = 2
    with pytest.raises(ValueError, match="projection changed"):
        before_particulate_review(current)
    before = before_particulate_review(list(load_uvek_release_resources("biosphere")))
    outgoing = next(r for r in before if r["source_profiles"][0]["family"] == "uvek")
    next(r for r in outgoing["replace"] if identity(r["source"]) == identity(REVIEW["decisions"][0]["target"]))[
        "conversion_factor"
    ] = 2
    with pytest.raises(ValueError, match="directional baseline changed"):
        apply_particulate_review(before, REPORT, CATALOGS)
