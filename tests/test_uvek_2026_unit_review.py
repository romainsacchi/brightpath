"""Unit review recommendations must follow source quantities and stay read-only."""

import importlib.util
import json
from copy import deepcopy
from pathlib import Path

import pytest
from openpyxl import Workbook

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("review_uvek_2026_units", ROOT / "scripts/review_uvek_2026_units.py")
REVIEW = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(REVIEW)
GENERATOR_SPEC = importlib.util.spec_from_file_location(
    "generate_uvek_2026_resources", ROOT / "scripts/generate_uvek_2026_resources.py"
)
GENERATOR = importlib.util.module_from_spec(GENERATOR_SPEC)
GENERATOR_SPEC.loader.exec_module(GENERATOR)


def historical_baseline():
    """Reconstruct the evidence snapshot without changing packaged resources."""
    from collections import Counter

    from brightpath.migrations.resources import load_uvek_release_resources

    resources = deepcopy(list(load_uvek_release_resources("technosphere")))
    for resource in resources:
        original_review = next(
            key
            for key, review in resource["reviews"].items()
            if review["rationale"].startswith("Reference identity correspondence only;")
        )
        for rule in resource["replace"]:
            if rule["mapping_method"] == "reviewed_unit_label_correspondence":
                rule.pop("conversion_factor")
                rule.pop("unit_review_rule_id")
                rule.update(mapping_method="unit_change_requires_factor", review_ref=original_review)
        resource["reviews"] = {original_review: resource["reviews"][original_review]}
        resource["provenance"].pop("unit_review")
        resource["coverage"]["mapping_methods"] = dict(Counter(rule["mapping_method"] for rule in resource["replace"]))
        resource["coverage"]["unit_changes_without_factor"] = 756
    return resources


def source_case(name="Machine operation", unit="year", native_unit="hr", inputs=True):
    filename = "process_11111111-1111-4111-8111-111111111111.xml"
    source = {"name": name, "reference product": name, "location": "CH", "unit": unit}
    target = {**source, "unit": REVIEW.normalize_unit(native_unit)}
    old = {
        **source,
        "code": "legacy-12",
        "filename": filename,
        "exchanges": [
            {"name": name, "location": "CH", "unit": unit, "type": "production", "flow": 12, "amount": 1},
            *(
                [
                    {
                        "name": "Electricity",
                        "location": "CH",
                        "unit": "kilowatt hour",
                        "type": "technosphere",
                        "flow": 15,
                        "amount": 0.019,
                    }
                ]
                if inputs
                else []
            ),
        ],
    }
    demand = (
        '<exchange name="Electricity" number="15" unit="kWh" meanValue="0.019"><inputGroup>5</inputGroup></exchange>'
        if inputs
        else ""
    )
    xml = f"""<ecoSpold><dataset number="12"><metaInformation><processInformation>
    <referenceFunction name="{name}" unit="{native_unit}" includedProcesses="per hour"/><geography location="CH"/>
    </processInformation></metaInformation><flowData>
    <exchange name="{name}" number="12" location="CH" unit="{native_unit}" meanValue="1"><outputGroup>0</outputGroup></exchange>
    {demand}</flowData></dataset></ecoSpold>""".encode()
    return {"id": "synthetic", "source": source, "target": target}, old, xml, "ecoSpold files/" + filename


def test_hour_label_recommendation_preserves_one_reference_unit_and_input_sources():
    rule, old, raw, member = source_case()
    before = deepcopy(old)
    result = REVIEW.review_record(rule, old, raw, member, {"15": "CH"})
    assert result["status"] == "supported_label_correspondence"
    assert result["recommended_forward_factor"] == result["recommended_reverse_factor"] == 1
    assert result["supplier_locations_resolved_by_number"] == 1
    assert result["old_technosphere_vector_sha256"] == result["native_technosphere_vector_sha256"]
    assert all(result["checks"].values())
    assert old == before


@pytest.mark.parametrize("change", ["reference_amount", "input_amount", "supplier_number", "supplier_location"])
def test_changed_source_quantities_or_providers_do_not_receive_a_factor(change):
    rule, old, raw, member = source_case()
    locations = {"15": "CH"}
    if change == "reference_amount":
        raw = raw.replace(b'meanValue="1"', b'meanValue="8760"')
    elif change == "input_amount":
        raw = raw.replace(b'meanValue="0.019"', b'meanValue="166.44"')
    elif change == "supplier_number":
        raw = raw.replace(b'number="15"', b'number="16"')
    else:
        locations["15"] = "DE"
    result = REVIEW.review_record(rule, old, raw, member, locations)
    assert result["status"] == "manual_review_required"
    assert result["recommended_forward_factor"] is None
    assert result["recommended_reverse_factor"] is None


def test_hour_record_with_no_operating_inputs_remains_unapproved():
    rule, old, raw, member = source_case(name="Production, washing machine, V-ZUG", inputs=False)
    result = REVIEW.review_record(rule, old, raw, member, {})
    assert all(result["checks"].values())
    assert result["status"] == "manual_review_required"
    assert result["recommended_forward_factor"] is None


def test_gas_label_recommendation_is_scoped_to_a_matched_record():
    rule, old, raw, member = source_case(name="Natural gas", unit="cubic meter", native_unit="Nm3")
    result = REVIEW.review_record(rule, old, raw, member, {"15": "CH"})
    assert result["group"] == "gas_volume"
    assert result["recommended_forward_factor"] == 1


def test_workbook_review_reads_unmodified_labels_and_rejects_missing_identities(tmp_path):
    rule, old, _raw, _member = source_case()
    path = tmp_path / "source.xlsx"
    workbook = Workbook()
    sheet = workbook.active
    for row in [
        ["Database", "source"],
        ["Activity", old["name"]],
        *[[field, old[field]] for field in ("code", "filename", "location", "reference product", "unit")],
        ["Exchanges"],
        ["name", "amount", "unit", "type", "flow", "location"],
        *[
            [exchange.get(field) for field in ("name", "amount", "unit", "type", "flow", "location")]
            for exchange in old["exchanges"]
        ],
    ]:
        sheet.append(row)
    workbook.save(path)
    before = path.read_bytes()
    result = REVIEW.read_workbook(path, {REVIEW.identity(rule["source"])})
    assert next(iter(result.values()))["unit"] == "year"
    assert path.read_bytes() == before
    with pytest.raises(ValueError, match="missing 1"):
        REVIEW.read_workbook(path, {("Missing", "Missing", "CH", "year")})


def test_historical_review_is_bound_to_applied_rules_in_both_directions():
    from brightpath.migrations.resources import load_uvek_release_resources
    from brightpath.migrations.schema import rule_review

    report = json.loads((ROOT / "docs/uvek_2026_unit_review.json").read_text())
    resource = json.loads(
        (ROOT / "brightpath/data/migrations/uvek/uvek-2025-cutoff-to-uvek-2026-cutoff.json").read_text()
    )
    investigated = {
        rule["id"]
        for rule in resource["replace"]
        if rule["mapping_method"] in {"unit_change_requires_factor", "reviewed_unit_label_correspondence"}
    }
    assert {row["rule_id"] for row in report["processes"]} == investigated
    assert (
        report["sources"]["migration_resource"]["sha256"]
        == resource["provenance"]["unit_review"]["baseline_forward_sha256"]
    )
    assert report["summary"]["reviewed"] == report["summary"]["all_checks_passed"] == 756
    assert report["summary"]["statuses"] == {"supported_label_correspondence": 755, "manual_review_required": 1}
    assert not report["runtime_rules_changed"]
    blocked = [row for row in report["processes"] if row["recommended_forward_factor"] is None]
    assert [(row["source"]["name"], row["source"]["location"]) for row in blocked] == [
        ("Production, washing machine, V-ZUG", "CH")
    ]
    accepted = {row["rule_id"]: row for row in report["processes"] if row["recommended_forward_factor"] is not None}
    report_digest = REVIEW.digest((ROOT / "docs/uvek_2026_unit_review.json").read_bytes())
    for directional in load_uvek_release_resources("technosphere"):
        applied = [
            rule for rule in directional["replace"] if rule["mapping_method"] == "reviewed_unit_label_correspondence"
        ]
        assert len(applied) == 755
        assert directional["coverage"]["unit_changes_without_factor"] == 1
        assert directional["provenance"]["unit_review"]["sha256"] == report_digest
        forward = directional["source_profiles"][0]["version"] == "2025"
        for rule in applied:
            evidence = accepted[rule["unit_review_rule_id"]]
            assert rule["conversion_factor"] == 1
            assert rule["source"] == evidence["source" if forward else "target"]
            assert rule["target"] == evidence["target" if forward else "source"]
            assert report_digest in " ".join(rule_review(directional, rule)["evidence"])


def test_generation_replays_review_without_mutating_its_baseline():
    baseline = historical_baseline()
    before = deepcopy(baseline)
    result = GENERATOR.apply_unit_review(baseline, ROOT / "docs/uvek_2026_unit_review.json")
    assert baseline == before
    assert len(result) == 2
    for resource in result:
        assert resource["coverage"]["mapping_methods"]["reviewed_unit_label_correspondence"] == 755
        assert resource["coverage"]["unit_changes_without_factor"] == 1


@pytest.mark.parametrize(
    "change",
    [
        "baseline_digest",
        "archive_digest",
        "duplicate",
        "identity",
        "checks",
        "missing_check",
        "factor",
        "reference_quantity",
        "reverse_identity",
    ],
)
def test_generation_rejects_stale_or_unverified_review_data(tmp_path, change):
    baseline = historical_baseline()
    report = json.loads((ROOT / "docs/uvek_2026_unit_review.json").read_text())
    row = next(row for row in report["processes"] if row["status"] == "supported_label_correspondence")
    if change == "baseline_digest":
        report["sources"]["migration_resource"]["sha256"] = "0" * 64
    elif change == "archive_digest":
        report["sources"]["native_archive"]["sha256"] = "0" * 64
    elif change == "duplicate":
        report["processes"][0] = deepcopy(report["processes"][1])
    elif change == "identity":
        row["target"]["location"] = "unknown"
    elif change == "checks":
        row["checks"]["reference_output_amount_unchanged"] = False
    elif change == "missing_check":
        row["checks"].pop("reference_output_amount_unchanged")
    elif change == "factor":
        row["recommended_forward_factor"] = 8760
    elif change == "reference_quantity":
        row["old_reference_amount"] = 8760
    else:
        reverse = next(rule for rule in baseline[1]["replace"] if rule["source"] == row["target"])
        reverse["target"]["location"] = "unknown"
    path = tmp_path / "review.json"
    path.write_text(json.dumps(report), encoding="utf-8")
    with pytest.raises(ValueError):
        GENERATOR.apply_unit_review(baseline, path)
