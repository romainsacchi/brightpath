from __future__ import annotations

import hashlib
import importlib.util
import json
from pathlib import Path

from brightpath import DATA_DIR

SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "generate_openlca_reference_catalog.py"
SCRIPT_SPEC = importlib.util.spec_from_file_location("generate_openlca_reference_catalog", SCRIPT_PATH)
SCRIPT_MODULE = importlib.util.module_from_spec(SCRIPT_SPEC)
assert SCRIPT_SPEC.loader is not None
SCRIPT_SPEC.loader.exec_module(SCRIPT_MODULE)


class FakeNode(dict):
    def __init__(self, database: str, code: str, **values):
        super().__init__(code=code, **values)
        self.key = (database, code)


def test_packaged_openlca_reference_manifest_matches_resource_bytes_and_counts():
    directory = DATA_DIR / "export" / "openlca_references"
    manifest = json.loads((directory / "RESOURCE_MANIFEST.json").read_text(encoding="utf-8"))
    resource = manifest["resources"][0]
    path = directory / resource["file"]
    raw = path.read_bytes()
    payload = json.loads(raw)

    assert resource["status"] == "legal_review_required"
    assert resource["size"] == len(raw)
    assert resource["sha256"] == hashlib.sha256(raw).hexdigest()
    assert resource["technosphere_references"] == len(payload["technosphere"]) == 11_747
    assert resource["biosphere_references"] == len(payload["biosphere"]) == 3_954
    assert payload["coverage"]["missing_biosphere_references"] == 408


def test_reference_generator_joins_brightway_identities_to_openlca_entity_refs(monkeypatch):
    activity = FakeNode(
        "uvek",
        "uvek-1",
        name="supplier",
        **{
            "reference product": "product",
            "location": "CH",
            "unit": "kilogram",
            "filename": "process_00000000-0000-3000-8000-000000000001.xml",
        },
    )
    flow = FakeNode(
        "biosphere",
        "00000000-0000-4000-8000-000000000002",
        name="emission",
        categories=("air",),
        unit="kilogram",
    )
    monkeypatch.setattr(
        SCRIPT_MODULE.bd,
        "Database",
        lambda name: [activity] if name == "uvek" else [flow],
    )
    raw_references = {
        "processes": [
            {
                "process_ref_id": "00000000-0000-3000-8000-000000000001",
                "process_name": "supplier",
                "location_ref_id": "00000000-0000-3000-8000-000000000003",
                "location": "CH",
                "flow_ref_id": "00000000-0000-3000-8000-000000000004",
                "flow_name": "product {CH}",
                "flow_property_ref_id": "00000000-0000-4000-8000-000000000005",
                "flow_property_name": "Mass",
                "unit_ref_id": "00000000-0000-4000-8000-000000000006",
                "unit": "kg",
            }
        ],
        "flows": [
            {
                "flow_ref_id": flow["code"],
                "flow_name": "emission",
                "flow_property_ref_id": "00000000-0000-4000-8000-000000000005",
                "flow_property_name": "Mass",
                "unit_ref_id": "00000000-0000-4000-8000-000000000006",
                "unit": "kg",
            }
        ],
    }

    payload = SCRIPT_MODULE.generate_catalog(
        raw_references=raw_references,
        technosphere_database="uvek",
        biosphere_database="biosphere",
        technosphere_profile={"family": "uvek", "version": "2025", "system_model": "cutoff"},
        biosphere_profile={"family": "ecoinvent", "version": "3.10"},
        source="test",
    )

    assert payload["technosphere"][0]["process_id"] == "00000000-0000-3000-8000-000000000001"
    assert payload["technosphere"][0]["flow_id"] == "00000000-0000-3000-8000-000000000004"
    assert payload["biosphere"][0]["flow_id"] == flow["code"]
    assert payload["coverage"] == {
        "technosphere_references": 1,
        "biosphere_references": 1,
        "missing_biosphere_references": 0,
    }
