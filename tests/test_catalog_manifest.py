import importlib.util
import json
from pathlib import Path

from brightpath import DATA_DIR

SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "generate_catalog_manifest.py"
SCRIPT_SPEC = importlib.util.spec_from_file_location("generate_catalog_manifest", SCRIPT_PATH)
SCRIPT_MODULE = importlib.util.module_from_spec(SCRIPT_SPEC)
assert SCRIPT_SPEC.loader is not None
SCRIPT_SPEC.loader.exec_module(SCRIPT_MODULE)


def test_packaged_catalog_manifest_matches_every_catalog_byte_for_byte():
    directory = DATA_DIR / "export" / "reference_catalogs"
    expected = SCRIPT_MODULE.build_manifest(directory)
    packaged = json.loads((directory / "RESOURCE_MANIFEST.json").read_text(encoding="utf-8"))

    assert packaged == expected
    assert packaged["status"] == "legal_review_required"
    assert len(packaged["resources"]) == 15
    assert all(resource["sha256"] for resource in packaged["resources"])
