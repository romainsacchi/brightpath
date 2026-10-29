"""Authored biosphere additions preserve quantities and refuse unsupported reverse choices."""

import hashlib
import json
from collections import defaultdict
from copy import deepcopy
from pathlib import Path

import pytest

from brightpath import BackgroundContext, BiosphereProfile, FormatProfile, InventoryContext, TechnosphereProfile
from brightpath.background import PackageCatalogProvider
from brightpath.background.execution import execute_background_migration
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.migrations.schema import dumps_migration_resource, validate_migration_resource
from brightpath.models import InventoryDocument
from scripts.review_uvek_2026_biosphere import (
    WORKBOOK_SHA256,
    apply_review,
    identity,
    validate_decision,
)

ROOT = Path(__file__).resolve().parents[1]
REVIEW_PATH = ROOT / "docs/uvek_2026_biosphere_review.json"
CATALOGS = ROOT / "brightpath/data/export/reference_catalogs"


def resources():
    """Keep the original 427-rule batch as an independent historical regression."""
    from tests.test_uvek_2026_biosphere_followup import resources as followup_resources

    result = followup_resources()
    resource = next(row for row in result if row["source_profiles"][0]["family"] == "uvek")
    if "biosphere_followup_review" in resource["provenance"]:
        from scripts.review_uvek_2026_biosphere_followup import METHODS

        followup = json.loads((ROOT / "docs/uvek_2026_biosphere_followup.json").read_text())
        resource["replace"] = [row for row in resource["replace"] if row["mapping_method"] not in METHODS]
        used = {row["review_ref"] for row in resource["replace"]}
        resource["reviews"] = {key: value for key, value in resource["reviews"].items() if key in used}
        resource["provenance"].pop("biosphere_followup_review")
        resource["unresolved"] = [row["source"] for row in followup["decisions"]]
        resource["coverage"] = followup["sources"]["baseline_coverage"]
    return result


def forward():
    return next(row for row in resources() if row["source_profiles"][0]["family"] == "uvek")


def additions():
    return [row for row in forward()["replace"] if row.get("report_correspondence")]


def context(biosphere):
    return BackgroundContext(TechnosphereProfile("uvek", "2026", "cutoff"), BiosphereProfile(*biosphere))


def document(rules):
    foreground = {
        "name": "Synthetic review foreground",
        "reference product": "product",
        "location": "CH",
        "unit": "kilogram",
    }
    return InventoryDocument(
        data=[
            {
                **foreground,
                "comment": "Synthetic reference test; no background inventory.",
                "exchanges": [
                    {**foreground, "type": "production", "amount": 1},
                    *[
                        {
                            **row["source"],
                            "type": "biosphere",
                            "amount": 12,
                            "uncertainty type": 3,
                            "loc": 12,
                            "scale": 0.3,
                            "custom": [1],
                        }
                        for row in rules
                    ],
                ],
            }
        ],
        context=InventoryContext(FormatProfile("brightway_excel"), context(("uvek", "2026"))),
    )


def test_review_is_complete_source_bound_and_matches_exact_catalog_uuids():
    report = json.loads(REVIEW_PATH.read_text())
    current = forward()
    review_source = current["provenance"]["biosphere_review"]
    assert review_source["sha256"] == hashlib.sha256(REVIEW_PATH.read_bytes()).hexdigest()
    assert report["sources"]["publisher_workbook"]["sha256"] == WORKBOOK_SHA256
    assert report["summary"]["accepted_identities"] == len(additions()) == 427
    assert report["summary"]["remaining_unresolved"] == len(current["unresolved"]) == 962
    assert current["coverage"]["mapped_identities"] == 1717
    assert len({identity(row["source"]) for row in report["decisions"]}) == 1389
    targets = {
        identity(row): row for row in json.loads((CATALOGS / "ecoinvent__3.10__cutoff.json").read_text())["biosphere"]
    }
    for rule in additions():
        assert targets[identity(rule["target"])]["flow_id"] == rule["target"]["uuid"]
    assert sum("biosphere_information_loss" in row for row in additions()) == 81
    assert all("target" not in row for row in report["decisions"] if row["reason"] == "source_cas_conflict")


def test_all_reviewed_links_execute_with_reported_loss_and_preserve_caller_input():
    rules = additions()
    original = document(rules)
    before = deepcopy(original.data)
    result = execute_background_migration(
        original,
        context(("ecoinvent", "3.10")),
        PackageCatalogProvider(),
        MigrationPolicy(on_information_loss=PolicyAction.WARN),
    )
    assert result.succeeded, result.report.to_json()
    flows = result.value.data[0]["exchanges"][1:]
    assert [identity(flow) for flow in flows] == [identity(row["target"]) for row in rules]
    for flow, rule in zip(flows, rules, strict=True):
        factor = rule.get("conversion_factor", 1)
        assert (flow["amount"], flow["loc"], flow["scale"]) == pytest.approx(
            (12 * factor, 12 * factor, 0.3 * abs(factor))
        )
        assert flow["custom"] == [1]
    loss_codes = [loss.code for stage in result.report.stages for loss in stage.losses]
    assert loss_codes.count("migration.biosphere_information_loss") == 81
    assert original.data == before
    repeated = execute_background_migration(result.value, context(("ecoinvent", "3.10")), PackageCatalogProvider())
    assert repeated.succeeded and repeated.value.data == result.value.data


def test_strict_policy_accepts_lossless_additions_and_rolls_back_coarsening():
    safe = [row for row in additions() if not row.get("biosphere_information_loss")]
    result = execute_background_migration(document(safe), context(("ecoinvent", "3.10")), PackageCatalogProvider())
    assert result.succeeded, result.report.to_json()
    coarse = next(row for row in additions() if row.get("biosphere_information_loss"))
    original = document([coarse])
    before = deepcopy(original.data)
    blocked = execute_background_migration(original, context(("ecoinvent", "3.10")), PackageCatalogProvider())
    assert not blocked.succeeded
    assert blocked.value.data == before and blocked.value.context == original.context
    assert any(issue.code == "migration.biosphere_information_loss" for issue in blocked.report.issues)
    assert original.data == before


def test_forward_additions_do_not_invent_a_reverse_allocation():
    reverse = next(row for row in resources() if row["source_profiles"][0]["family"] == "ecoinvent")
    assert len(reverse["replace"]) == 1290
    rule = next(
        row for row in additions() if row["source"]["name"] == "Carbonate" and row["source"]["categories"][-1] == "lake"
    )
    original = document([rule])
    exported = execute_background_migration(
        original,
        context(("ecoinvent", "3.10")),
        PackageCatalogProvider(),
        MigrationPolicy(on_information_loss=PolicyAction.WARN),
    )
    assert exported.succeeded
    back = execute_background_migration(exported.value, original.context.background, PackageCatalogProvider())
    assert not back.succeeded


def test_review_replays_without_changing_baseline_rules_or_reverse_resources():
    report = json.loads(REVIEW_PATH.read_text())
    baseline = deepcopy(list(resources()))
    item = next(row for row in baseline if row["source_profiles"][0]["family"] == "uvek")
    item["replace"] = [row for row in item["replace"] if not row.get("report_correspondence")]
    used = {row["review_ref"] for row in item["replace"]}
    item["reviews"] = {key: value for key, value in item["reviews"].items() if key in used}
    item["provenance"].pop("biosphere_review")
    item["licenses"] = [row for row in item["licenses"] if row["name"] != "Source publisher terms"]
    item["unresolved"] = sorted([row["source"] for row in report["decisions"]], key=identity)
    item["coverage"] = {
        "mapped_identities": 1290,
        "mapping_methods": {"native_compartment_and_bw2io_name": 1290},
        "source_identities": 2679,
        "target_identities": 4362,
        "unit_changes_without_factor": 0,
        "unresolved_identities": 1389,
    }
    assert (
        hashlib.sha256(dumps_migration_resource(item).encode()).hexdigest()
        == report["sources"]["baseline_forward_sha256"]
    )
    rebuilt = apply_review(baseline, REVIEW_PATH, CATALOGS)
    assert rebuilt == list(resources())
    assert len(item["replace"]) == 1290


@pytest.mark.parametrize("mutation", ["cas", "uuid", "unit", "source", "water_factor", "loss_finding"])
def test_authoring_rejects_changed_identity_cas_factor_or_loss_evidence(mutation):
    report = json.loads(REVIEW_PATH.read_text())
    method = "reviewed_water_mass_volume" if mutation == "water_factor" else "publisher_elementary_uuid_correspondence"
    decision = deepcopy(
        next(row for row in report["decisions"] if row["reason"] == method and row.get("biosphere_information_loss"))
    )
    catalog = json.loads((CATALOGS / "ecoinvent__3.10__cutoff.json").read_text())
    targets = {identity(row): row for row in catalog["biosphere"]}
    uuids = defaultdict(list)
    for row in catalog["biosphere"]:
        uuids[row["flow_id"]].append(row)
    if mutation == "cas":
        decision["source_cas"] = ["definitely-wrong"]
        decision["publisher_row"]["CAS"] = "different"
    elif mutation == "uuid":
        decision["target"]["uuid"] = "wrong"
    elif mutation == "unit":
        decision["source"]["unit"] = "gram"
    elif mutation == "source":
        decision["publisher_row"]["Elementary Name v2.2"] = "Another flow"
    elif mutation == "water_factor":
        decision["conversion_factor"] = 1
    else:
        decision.pop("biosphere_information_loss")
    with pytest.raises(ValueError):
        validate_decision(decision, targets, uuids)


@pytest.mark.parametrize("invalid", [True, False, ""])
def test_schema_rejects_unstructured_information_loss(invalid):
    from brightpath.exceptions import MigrationError

    resource = deepcopy(forward())
    resource["replace"][0]["biosphere_information_loss"] = invalid
    with pytest.raises(MigrationError):
        validate_migration_resource(resource)
