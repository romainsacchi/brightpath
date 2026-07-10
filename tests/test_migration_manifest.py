import importlib.util
import json
from pathlib import Path

from brightpath import DATA_DIR

SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "generate_migration_manifest.py"
SCRIPT_SPEC = importlib.util.spec_from_file_location("generate_migration_manifest", SCRIPT_PATH)
SCRIPT_MODULE = importlib.util.module_from_spec(SCRIPT_SPEC)
assert SCRIPT_SPEC.loader is not None
SCRIPT_SPEC.loader.exec_module(SCRIPT_MODULE)


def test_packaged_migration_manifest_matches_every_resource():
    directory = DATA_DIR / "migrations"
    expected = SCRIPT_MODULE.build_manifest(directory)
    packaged = json.loads((directory / "RESOURCE_MANIFEST.json").read_text(encoding="utf-8"))

    assert packaged == expected
    assert len(packaged["resources"]) == 14
    placeholder = next(item for item in packaged["resources"] if item["status"] == "placeholder")
    assert placeholder["path"] == "uvek/ecoinvent-to-uvek-2025-placeholder.json"
