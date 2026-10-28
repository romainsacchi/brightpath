"""Publisher-backed additions must preserve function, units, geography and prior reviews."""

import json
from copy import deepcopy
from pathlib import Path

import pytest
from openpyxl import Workbook

from brightpath import (
    BackgroundContext,
    BiosphereProfile,
    BrightwayInventory,
    FormatProfile,
    InventoryContext,
    TechnosphereProfile,
)
from brightpath.background.uvek_export import identity, load_uvek_export_resource
from brightpath.migrations.resources import load_uvek_release_resources
from scripts.generate_uvek_export import (
    CHANGE_COLUMNS,
    CORRESPONDENCE_COLUMNS,
    CORRESPONDENCE_SHEET,
    LINEAGE_METHOD,
    LINEAGE_PATH,
    build,
    build_lineage_evidence,
    extract_publisher_lineage,
    lineage_decisions,
    main,
)

ROOT = Path(__file__).resolve().parents[1]
PRODUCT_ID = "11111111-1111-4111-8111-111111111111"


def catalog_fixture():
    source = {
        "name": "Polymer, at plant",
        "reference_product": "Polymer, at plant",
        "location": "RER",
        "unit": "kilogram",
    }
    target = {
        "name": "polymer production",
        "reference_product": "polymer",
        "location": "RER",
        "unit": "kilogram",
        "flow_id": PRODUCT_ID,
    }
    row = {column: None for column in CORRESPONDENCE_COLUMNS}
    row.update(
        {
            "SINGLE-OUTPUT DATASET-ID v2.2": 42,
            "PRODUCT NAME v2.2": "polymer, at plant",
            "ACTIVITY NAME v3": target["name"],
            "PRODUCT NAME v3": target["reference_product"],
            "UUID PRODUCT NAME v3": PRODUCT_ID,
            "REFERENCE PRODUCT": "x",
            "Unit": "kg",
            "Location": "RER",
        }
    )
    return {"technosphere": [source]}, {"technosphere": [target]}, row


def workbook(path, rows, columns=CORRESPONDENCE_COLUMNS):
    book = Workbook()
    sheet = book.active
    sheet.title = CORRESPONDENCE_SHEET
    sheet.append(list(columns))
    for row in rows:
        sheet.append([row.get(column) for column in columns])
    book.save(path)
    book.close()
    return path


def test_publisher_review_is_read_only_and_retains_exact_reference_metadata(tmp_path):
    source, target, row = catalog_fixture()
    before = deepcopy((source, target))
    path = workbook(tmp_path / "publisher.xlsx", [row])
    original = path.read_bytes()
    decisions, counts = extract_publisher_lineage(path, source, target, set())
    assert counts == {"publisher_correspondence": 1}
    assert decisions[0]["source"]["name"] == "Polymer, at plant"
    assert decisions[0]["target"]["reference product"] == "polymer"
    assert decisions[0]["publisher_row"]["UUID PRODUCT NAME v3"] == PRODUCT_ID
    assert decisions[0]["worksheet_row"] == 2
    assert (source, target) == before
    assert path.read_bytes() == original


@pytest.mark.parametrize("column", CHANGE_COLUMNS)
def test_every_publisher_change_flag_requires_review(tmp_path, column):
    source, target, row = catalog_fixture()
    row[column] = "changed"
    decisions, counts = extract_publisher_lineage(workbook(tmp_path / "publisher.xlsx", [row]), source, target, set())
    assert decisions == []
    assert counts == {"publisher_change_flag_requires_review": 1}


@pytest.mark.parametrize(
    "change",
    [
        "multioutput",
        "negative_reference",
        "missing_id",
        "product_uuid",
        "unit",
        "geography",
        "source_product",
        "waste_role",
    ],
)
def test_reference_bases_and_current_targets_must_be_unambiguous(tmp_path, change):
    source, target, row = catalog_fixture()
    if change == "multioutput":
        row["MULTI-OUTPUT DATASET-ID v2.2"] = 5
    elif change == "negative_reference":
        row["REFERENCE PRODUCT"] = "-"
    elif change == "missing_id":
        row["SINGLE-OUTPUT DATASET-ID v2.2"] = None
    elif change == "product_uuid":
        target["technosphere"][0]["flow_id"] = "different"
    elif change == "unit":
        target["technosphere"][0]["unit"] = "cubic meter"
    elif change == "geography":
        target["technosphere"][0]["location"] = "GLO"
    elif change == "source_product":
        source["technosphere"][0]["reference_product"] = "different product"
    else:
        row["ACTIVITY NAME v3"] = target["technosphere"][0]["name"] = "waste polymer treatment"
    decisions, counts = extract_publisher_lineage(workbook(tmp_path / "publisher.xlsx", [row]), source, target, set())
    assert decisions == []
    assert sum(counts.values()) == 1
    assert "publisher_correspondence" not in counts


@pytest.mark.parametrize("duplicate", ["publisher", "current_target"])
def test_duplicate_publisher_or_target_identities_are_not_resolved_by_row_order(tmp_path, duplicate):
    source, target, row = catalog_fixture()
    rows = [row, deepcopy(row)] if duplicate == "publisher" else [row]
    if duplicate == "current_target":
        target["technosphere"].append({**target["technosphere"][0], "flow_id": "different"})
    decisions, _ = extract_publisher_lineage(workbook(tmp_path / "publisher.xlsx", rows), source, target, set())
    assert decisions == []


def test_previously_reviewed_sources_are_protected(tmp_path):
    source, target, row = catalog_fixture()
    protected = {("Polymer, at plant", "Polymer, at plant", "RER", "kilogram")}
    decisions, counts = extract_publisher_lineage(
        workbook(tmp_path / "publisher.xlsx", [row]), source, target, protected
    )
    assert decisions == []
    assert counts == {"existing_review_preserved": 1}


@pytest.mark.parametrize("columns", [CORRESPONDENCE_COLUMNS[:-1], (*CORRESPONDENCE_COLUMNS, "Unit")])
def test_incomplete_or_duplicate_headers_do_not_hide_change_flags(tmp_path, columns):
    source, target, row = catalog_fixture()
    with pytest.raises(ValueError, match="columns"):
        extract_publisher_lineage(workbook(tmp_path / "publisher.xlsx", [row], columns), source, target, set())


def test_unreviewed_workbook_revisions_are_rejected(tmp_path):
    _, _, row = catalog_fixture()
    with pytest.raises(ValueError, match="checksum changed"):
        build_lineage_evidence(ROOT, workbook(tmp_path / "different.xlsx", [row]))


def test_missing_review_evidence_cannot_silently_drop_the_new_rules(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr("sys.argv", ["generate_uvek_export.py", "--root", str(tmp_path)])
    with pytest.raises(SystemExit) as error:
        main()
    assert error.value.code == 2
    assert "Publisher-lineage evidence is missing" in capsys.readouterr().err
    assert not list(tmp_path.iterdir())


@pytest.fixture(scope="module")
def evidence():
    return json.loads((ROOT / LINEAGE_PATH).read_text(encoding="utf-8"))


def test_evidence_replays_the_packaged_rules_without_changing_earlier_decisions(evidence):
    decisions = lineage_decisions(ROOT, evidence)
    assert len(decisions) == 1070
    before = deepcopy(evidence)
    resource, review = build(
        ROOT, json.loads((ROOT / "docs/uvek_export_evidence.json").read_text(encoding="utf-8")), evidence
    )
    packaged = json.loads(
        (ROOT / "brightpath/data/migrations/uvek/uvek-2025-to-ecoinvent-3.12-cutoff.json").read_text(encoding="utf-8")
    )
    assert resource == packaged
    assert len([rule for rule in resource["replace"] if rule.get("mapping_method") == LINEAGE_METHOD]) == 1070
    assert review["coverage"] == resource["coverage"]
    assert evidence == before


@pytest.mark.parametrize(
    "change", ["catalog_digest", "legacy_digest", "workbook_digest", "target", "uuid", "flag", "duplicate", "row"]
)
def test_stale_or_inconsistent_evidence_is_rejected(evidence, change):
    changed = deepcopy(evidence)
    entry = changed["correspondences"][0]
    if change == "catalog_digest":
        changed["catalog_sha256"]["uvek__2025__cutoff.json"] = "0" * 64
    elif change == "legacy_digest":
        changed["legacy_export_sha256"] = "0" * 64
    elif change == "workbook_digest":
        changed["source"]["sha256"] = "0" * 64
    elif change == "target":
        entry["target"]["name"] = "unreviewed target"
    elif change == "uuid":
        entry["publisher_row"]["UUID PRODUCT NAME v3"] = "different product"
    elif change == "flag":
        entry["publisher_row"]["Unit changed to:"] = "g"
    elif change == "duplicate":
        changed["correspondences"].append(deepcopy(entry))
    else:
        entry["worksheet_row"] = 0
    with pytest.raises(ValueError):
        lineage_decisions(ROOT, changed)


@pytest.mark.parametrize("version", ["2025", "2026"])
def test_every_added_rule_executes_from_both_uvek_releases_under_strict_policy(version):
    rules = [rule for rule in load_uvek_export_resource()["replace"] if rule.get("mapping_method") == LINEAGE_METHOD]
    native = next(
        r for r in load_uvek_release_resources("technosphere") if r["source_profiles"][0]["version"] == "2026"
    )
    by_legacy = {
        identity(rule["target"]): rule["source"]
        for rule in native["replace"]
        if "conversion_factor" in rule or rule["source"]["unit"] == rule["target"]["unit"]
    }
    foreground = {"name": "Lineage foreground", "reference product": "test output", "unit": "unit", "location": "CH"}
    data = [
        {
            **foreground,
            "unknown metadata": {"keep": True},
            "exchanges": [{**foreground, "type": "production", "amount": 1}],
        }
    ]
    for rule in rules:
        source = rule["source"] if version == "2025" else by_legacy[identity(rule["source"])]
        data[0]["exchanges"].append(
            {**source, "type": "technosphere", "amount": 2, "uncertainty type": 3, "loc": 2, "scale": 0.1}
        )
    biosphere = BiosphereProfile("uvek", "2026") if version == "2026" else BiosphereProfile("ecoinvent", "3.10")
    source = InventoryContext(
        FormatProfile("brightway_excel"), BackgroundContext(TechnosphereProfile("uvek", version, "cutoff"), biosphere)
    )
    inventory = BrightwayInventory.from_data(data, context=source)
    original = deepcopy(inventory.data)
    target = BackgroundContext(TechnosphereProfile("ecoinvent", "3.12", "cutoff"), biosphere)
    migrated = inventory.migrate_background(target)
    assert inventory.data == original == data
    assert migrated.data[0]["unknown metadata"] == {"keep": True}
    assert len(migrated.data[0]["exchanges"]) == 1071
    for exchange, rule in zip(migrated.data[0]["exchanges"][1:], rules, strict=True):
        assert identity(exchange) == identity(rule["target"])
        assert (exchange["amount"], exchange["loc"], exchange["scale"]) == (2, 2, 0.1)
    assert migrated.migrate_background(target).data == migrated.data
