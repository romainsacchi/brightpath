"""Publisher radioactivity identities require explicit, correctly directed SI scaling."""

import hashlib
import json
import math
from copy import deepcopy

import pytest

from brightpath import BackgroundContext, BiosphereProfile, TechnosphereProfile
from brightpath.background import PackageCatalogProvider
from brightpath.background.execution import execute_background_migration
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.migrations.schema import dumps_migration_resource
from scripts.review_uvek_2026_biosphere import identity
from scripts.review_uvek_2026_radioactivity import (
    METHOD,
    REPORT,
    apply_radioactivity_review,
    validate_decision,
)
from tests.test_uvek_2026_biosphere_followup import CATALOGS
from tests.test_uvek_2026_biosphere_followup import forward as parent_forward
from tests.test_uvek_2026_biosphere_followup import resources as parent_resources
from tests.test_uvek_2026_biosphere_review import context, document
from tests.uvek_biosphere_history import before_land_review


def forward():
    return next(row for row in before_land_review() if row["source_profiles"][0]["family"] == "uvek")


def additions():
    return [row for row in forward()["replace"] if row["mapping_method"] == METHOD]


def test_review_is_complete_preserves_previous_1772_rules_and_reproduces_resource():
    review = json.loads(REPORT.read_text())
    current, parent = forward(), parent_forward()
    assert review["summary"]["accepted_identities"] == len(additions()) == 158
    assert len(review["decisions"]) == 907
    assert review["summary"]["accepted_exchange_uses"] == 3151
    assert review["summary"]["information_loss_rules"] == 56
    assert len(current["replace"]) == current["coverage"]["mapped_identities"] == 1930
    assert len(current["unresolved"]) == current["coverage"]["unresolved_identities"] == 749
    assert current["coverage"]["directional_additions"] == 640
    assert current["coverage"]["information_loss_rules"] == 146
    assert (
        hashlib.sha256(dumps_migration_resource(parent).encode()).hexdigest()
        == review["sources"]["baseline_forward_sha256"]
    )
    old_rules = {row["id"]: row for row in parent["replace"]}
    assert {row["id"]: row for row in current["replace"] if row["id"] in old_rules} == old_rules
    assert apply_radioactivity_review(parent_resources(), REPORT, CATALOGS) == before_land_review()
    assert all(rule["conversion_factor"] == 0.001 for rule in additions())


@pytest.mark.parametrize("version", ["3.10", "3.12"])
def test_all_158_radioactivity_links_scale_amounts_uncertainty_and_expressions_once(version):
    rules = additions()
    original = document(rules)
    data = original.data
    data[0]["parameters"] = [{"name": "dose", "amount": 12}]
    for exchange in data[0]["exchanges"][1:]:
        exchange["formula"] = "dose"
    original = original.replace(data=data)
    before = deepcopy(original.data)
    target = BackgroundContext(
        TechnosphereProfile("ecoinvent", version, "cutoff"), BiosphereProfile("ecoinvent", version)
    )
    policy = MigrationPolicy(on_information_loss=PolicyAction.WARN, on_inferred_reverse=PolicyAction.WARN)
    result = execute_background_migration(original, target, PackageCatalogProvider(), policy)
    assert result.succeeded, result.report.to_json()
    flows = result.value.data[0]["exchanges"][1:]
    assert len(flows) == 158
    for flow in flows:
        assert flow["unit"] == "kilo Becquerel"
        assert (flow["amount"], flow["loc"], flow["scale"]) == pytest.approx((0.012, 0.012, 0.0003))
        assert flow["formula"] == "(dose) * (0.001)"
        assert flow["custom"] == [1]
    if version == "3.10":
        assert [identity(flow) for flow in flows] == [identity(rule["target"]) for rule in rules]
    assert original.data == before
    repeated = execute_background_migration(result.value, target, PackageCatalogProvider(), policy)
    assert repeated.succeeded and repeated.value.data == result.value.data


def test_strict_accepts_exact_contexts_and_rolls_back_surface_water_coarsening():
    safe = [row for row in additions() if not row.get("biosphere_information_loss")]
    assert len(safe) == 102
    accepted = execute_background_migration(document(safe), context(("ecoinvent", "3.10")), PackageCatalogProvider())
    assert accepted.succeeded, accepted.report.to_json()
    original = document(additions())
    before = deepcopy(original.data)
    refused = execute_background_migration(original, context(("ecoinvent", "3.10")), PackageCatalogProvider())
    assert not refused.succeeded
    assert original.data == before
    assert "biosphere_information_loss" in refused.report.to_json()


@pytest.mark.parametrize("uncertainty", [0, 2, 3, 4])
@pytest.mark.parametrize("amount", [-12, 0, 12])
def test_si_scaling_preserves_distribution_parameters(uncertainty, amount):
    rule = next(row for row in additions() if not row.get("biosphere_information_loss"))
    original = document([rule])
    data = original.data
    flow = data[0]["exchanges"][1]
    flow.update(amount=amount, **{"uncertainty type": uncertainty}, minimum=-15, maximum=15)
    if uncertainty == 2:
        flow.update(loc=math.log(12), scale=0.3, negative=amount < 0)
    else:
        flow.update(loc=amount, scale=0.3)
    original = original.replace(data=data)
    result = execute_background_migration(original, context(("ecoinvent", "3.10")), PackageCatalogProvider())
    assert result.succeeded, result.report.to_json()
    actual = result.value.data[0]["exchanges"][1]
    assert actual["amount"] == pytest.approx(amount / 1000)
    assert (actual["minimum"], actual["maximum"]) == pytest.approx((-0.015, 0.015))
    if uncertainty == 2:
        assert actual["loc"] == pytest.approx(math.log(0.012))
        assert actual["scale"] == 0.3
        assert actual["negative"] == (amount < 0)
    else:
        assert actual["loc"] == pytest.approx(amount / 1000)
        assert actual["scale"] == pytest.approx(0.0003 if uncertainty == 3 else 0.3)


@pytest.mark.parametrize(
    "mutation",
    [
        "missing_factor",
        "inverse_factor",
        "unit",
        "mass_unit",
        "uuid",
        "isotope",
        "context",
        "legacy_context",
        "cas",
        "cas_conflict",
        "ambiguous_uuid",
        "missing_loss",
    ],
)
def test_malformed_radioactivity_correspondences_are_rejected(mutation):
    review = json.loads(REPORT.read_text())
    row = deepcopy(next(row for row in review["decisions"] if row.get("biosphere_information_loss")))
    if mutation == "missing_factor":
        row.pop("conversion_factor")
    elif mutation == "inverse_factor":
        row["conversion_factor"] = 1000
    elif mutation in {"unit", "mass_unit"}:
        row["source"]["unit"] = "kilo Becquerel" if mutation == "unit" else "kilogram"
    elif mutation == "uuid":
        row["target"]["uuid"] = "wrong"
    elif mutation == "isotope":
        row["source"]["name"] += "m"
    elif mutation == "context":
        row["source"]["categories"] = ["emissions to water", "groundwater, long-term"]
    elif mutation == "legacy_context":
        for publisher in row["publisher_rows"]:
            publisher["SubCategory v2.2"] = "ocean"
    elif mutation == "cas":
        row["source_cas"] = ["7732-18-5"]
    elif mutation == "cas_conflict":
        row["source_cas"] = ["7732-18-5", "71-36-3"]
    elif mutation == "ambiguous_uuid":
        extra = deepcopy(row["publisher_rows"][0])
        extra["UUID Elementary Flow v3"] = "another"
        row["publisher_rows"].append(extra)
    else:
        row.pop("biosphere_information_loss")
    catalog = json.loads((CATALOGS / "ecoinvent__3.10__cutoff.json").read_text())["biosphere"]
    with pytest.raises(ValueError):
        validate_decision(row, {identity(target): target for target in catalog}, {})


def test_no_new_reverse_allocations_or_unsubstantiated_substances_are_inferred():
    current, parent = before_land_review(), parent_resources()

    def reverse(resources):
        return next(row for row in resources if row["source_profiles"][0]["family"] == "ecoinvent")

    assert reverse(current) == reverse(parent)
    assert len(reverse(current)["replace"]) == 1290
    report = json.loads(REPORT.read_text())
    blocked = [row for row in report["decisions"] if "target" not in row]
    assert all(
        row["source"]["unit"] != "Becquerel" or row["source"]["name"] == "Radioactive species, unspecified"
        for row in blocked
    )
    assert any(row["source"]["name"] == "Zinc" and row["reason"] == "source_cas_conflict" for row in blocked)
