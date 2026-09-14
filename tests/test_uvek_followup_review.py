import copy
import json
from pathlib import Path

import openpyxl
import pytest

from brightpath import DATA_DIR
from scripts.generate_uvek_migration_resources import apply_reviewed_overrides
from scripts.review_uvek_followup import (
    CURATION_FILES,
    BranchLineage,
    followup,
    group_id,
    identity_risks,
    reviewed_decision,
    scope_hash,
    write_reports,
)
from scripts.review_uvek_mapping import identity


def comparison_row(outcome="review_pair", product="stone wool", old="Stone meal", proposed="Rock wool"):
    source = {"name": product + " production", "reference product": product, "location": "RER", "unit": "kilogram"}
    target = {"name": proposed, "reference product": proposed, "location": "RER", "unit": "kilogram"}
    previous = target | {"name": old, "reference product": old}
    return source | {
        "outcome": outcome,
        "previous_target": json.dumps(previous),
        "candidate_target": json.dumps(target),
        "previous_confidence": "0.2",
        "candidate_confidence": "1.0",
        "candidate_method": "review",
        "candidate_reasoning": "Reviewed by hand",
    }


def lineage_for(row, filename=CURATION_FILES[0], **changes):
    records = {name: [] for name in CURATION_FILES}
    records[filename] = [
        {
            "source": {field: row[field] for field in ("name", "reference product", "location", "unit")},
            "target": json.loads(row["candidate_target"]),
        }
        | changes
    ]
    return BranchLineage(records)


def inputs(row):
    current = {
        "source": {field: row[field] for field in ("name", "reference product", "location", "unit")},
        "target": json.loads(row["previous_target"]),
        "confidence": float(row["previous_confidence"]),
        "mapping_method": "heuristic_similarity",
    }
    ledger = {
        "source_revision": "revision",
        "groups": [
            {
                "id": group_id(row),
                "scope_sha256": scope_hash([row]),
                "entries": 1,
                "outcome": "accept_followup_identity",
                "reason": "Same product, different previous material.",
                "confidence_cap": 0.8,
            }
        ],
    }
    return {"replace": [current]}, {"source_revision": "revision", "decisions": []}, ledger


def test_inherited_review_is_not_independent_validation_or_full_confidence():
    row = comparison_row()
    lineage = lineage_for(row, fit="rough proxy", comment="reuse: learned choice")
    inherited = row | {"location": "RoW"}
    trace = lineage.trace(inherited)
    assert trace["lineage_kind"] == "script_reuse"
    assert trace["lineage_scope"] == "inherited_location"
    assert "inherited_confidence_exceeds_origin" in trace["lineage_flags"]
    assert trace["lineage_confidence"] == 0.4
    decision = reviewed_decision(
        inherited,
        {"confidence_cap": 0.8, "reason": "Product improvement", "outcome": "accept_followup_identity"},
        trace,
    )
    assert decision["confidence"] == 0.4
    assert any("inherited" in limitation for limitation in decision["limitations"])


def test_lineage_uses_branch_precedence_last_exact_and_first_inherited():
    row = comparison_row()
    source = {field: row[field] for field in ("name", "reference product", "location", "unit")}
    chosen = json.loads(row["candidate_target"])
    records = {name: [] for name in CURATION_FILES}
    records[CURATION_FILES[0]] = [
        {"source": source | {"location": "CH"}, "target": chosen, "comment": "first"},
        {"source": source, "target": chosen, "comment": "second"},
        {"source": source, "target": chosen, "comment": "last exact"},
    ]
    records[CURATION_FILES[1]] = [{"source": source, "target": chosen, "comment": "lower priority"}]
    lineage = BranchLineage(records)
    assert lineage.trace(row)["lineage_comment"] == "last exact"
    assert lineage.trace(row | {"location": "RoW"})["lineage_comment"] == "first"


def test_lineage_flags_unit_inheritance_invalid_fit_and_conflicts():
    row = comparison_row()
    lineage = lineage_for(row, fit="exact/good proxy")
    assert "fit_label_not_recognized" in lineage.trace(row)["lineage_flags"]
    assert lineage.trace(row | {"unit": "gram"})["lineage_scope"] == "inherited_unit"
    wrong = row | {"candidate_target": row["previous_target"]}
    assert lineage.trace(wrong)["lineage_kind"] == "unresolved_lineage"
    assert lineage.trace(row | {"candidate_method": "heuristic"})["lineage_kind"] == "algorithmic"


def test_followup_is_non_mutating_and_preserves_same_target_scores():
    row = comparison_row()
    resource, overrides, ledger = inputs(row)
    same = comparison_row(outcome="same_target", product="brick", old="Brick", proposed="Brick")
    same_resource, _, _ = inputs(same)
    resource["replace"].extend(same_resource["replace"])
    original = copy.deepcopy((resource, overrides, ledger))
    result, audit, agreement, summary = followup(
        resource, overrides, [row, same], ledger, lineage_for(row), {identity(json.loads(row["candidate_target"]))}
    )
    assert (resource, overrides, ledger) == original
    assert len(result["decisions"]) == 1
    assert audit[0]["final_confidence"] == 0.8
    assert agreement[0]["final_confidence"] == 0.2
    assert summary["same_target_score_changes"] == 0
    assert summary["low_confidence_after"] == 1


@pytest.mark.parametrize(
    "problem",
    ["revision", "scope", "count", "duplicate", "stale", "unit", "catalog", "curated", "prior", "confidence", "bundle"],
)
def test_followup_rejects_invalid_input_without_mutation(problem):
    row = comparison_row()
    if problem == "bundle":
        row = comparison_row(product="tree seedling", proposed="Tree seedling, 1000 units")
    resource, overrides, ledger = inputs(row)
    comparison = [row]
    targets = {identity(json.loads(row["candidate_target"]))}
    if problem == "revision":
        ledger["source_revision"] = "other"
    elif problem == "scope":
        ledger["groups"][0]["scope_sha256"] = "stale"
    elif problem == "count":
        ledger["groups"][0]["entries"] = 2
    elif problem == "duplicate":
        comparison.append(copy.deepcopy(row))
    elif problem == "stale":
        resource["replace"][0]["confidence"] = 0.3
    elif problem == "unit":
        target = json.loads(row["candidate_target"]) | {"unit": "gram"}
        row["candidate_target"] = json.dumps(target)
        targets = {identity(target)}
        ledger["groups"][0]["scope_sha256"] = scope_hash([row])
    elif problem == "catalog":
        targets.clear()
    elif problem == "curated":
        resource["replace"][0]["mapping_method"] = "curated_legacy"
    elif problem == "prior":
        overrides["decisions"] = [{"source": resource["replace"][0]["source"]}]
    elif problem == "confidence":
        ledger["groups"][0]["confidence_cap"] = float("nan")
    original = copy.deepcopy(resource)
    with pytest.raises(ValueError):
        followup(resource, overrides, comparison, ledger, lineage_for(row), targets)
    assert resource == original


def test_same_target_screening_does_not_equate_agreement_with_compatibility():
    row = comparison_row(product="electricity, high voltage", proposed="xxx Electricity, allocation heat")
    assert "electricity_allocation_to_heat_unverified" in identity_risks(row)
    row = comparison_row(product="tree seedling", proposed="Tree seedling, 1000 units")
    assert "bundled_reference_amount_needs_conversion" in identity_risks(row)
    row = comparison_row(product="operation, computer, desktop, off mode", proposed="Use, smartphone")
    assert "desktop_to_smartphone_mismatch" in identity_risks(row)
    assert "off_standby_operation_not_validated" in identity_risks(row)


@pytest.mark.parametrize(
    "source_unit,target_unit", [("person kilometer", "person-kilometer"), ("person-kilometer", "person kilometer")]
)
def test_reviewed_passenger_distance_alias_has_explicit_factor_one(source_unit, target_unit):
    row = comparison_row(product="transport, passenger train", old="Transport, aircraft", proposed="Transport, train")
    row["unit"] = source_unit
    row["candidate_target"] = json.dumps(json.loads(row["candidate_target"]) | {"unit": target_unit})
    resource, overrides, ledger = inputs(row)
    resource["coverage"] = {}
    resource["methodology"] = {}
    targets = {identity(json.loads(row["candidate_target"]))}
    reviewed, _, _, _ = followup(resource, overrides, [row], ledger, lineage_for(row), targets)
    apply_reviewed_overrides(resource, reviewed, targets)
    assert resource["replace"][0]["conversion_factor"] == 1.0
    original = copy.deepcopy(resource)
    apply_reviewed_overrides(resource, reviewed, targets)
    assert resource == original


def test_report_writes_literal_excel_strings_and_filters(tmp_path):
    row = comparison_row() | {"candidate_reasoning": "=1+1"}
    write_reports(tmp_path, [row], [row], {"review_version": 3})
    workbook = openpyxl.load_workbook(tmp_path / "uvek_followup_review.xlsx")
    sheet = workbook["Alternatives"]
    cell = sheet.cell(2, list(row).index("candidate_reasoning") + 1)
    assert cell.value == "=1+1"
    assert cell.data_type == "s"
    assert sheet.freeze_panes == "A2"
    assert sheet.auto_filter.ref
    workbook.close()


def test_packaged_followup_ledger_and_resource_agree():
    root = Path(__file__).resolve().parents[1]
    ledger = json.loads((root / "docs/uvek_followup_decisions.json").read_text())
    overrides = json.loads((DATA_DIR / "export/uvek_reviewed_overrides.json").read_text())
    resource = json.loads((DATA_DIR / "migrations/uvek/ecoinvent-to-uvek-2025.json").read_text())
    selected = [decision for decision in overrides["decisions"] if decision["mapping_method"] == "clic_followup_review"]
    expected = sum(group["entries"] for group in ledger["groups"] if group["outcome"].startswith("accept_"))
    assert len(selected) == expected == 164
    assert len(resource["replace"]) == 31488
    indexed = {identity(row["source"]): row for row in resource["replace"]}
    for decision in selected:
        assert indexed[identity(decision["source"])]["target"] == decision["target"]
        assert indexed[identity(decision["source"])]["confidence"] == decision["confidence"]
    outcomes = {group["id"]: group["outcome"] for group in ledger["groups"]}
    assert outcomes["d0c5a104ee3c"] == "needs_quantity_basis"
    assert outcomes["077d136cfc22"] == "retain_explicit_specification"
    assert outcomes["7292f0240eb6"] == "retain_product_identity"
