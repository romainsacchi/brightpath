import importlib.util
import json
from pathlib import Path

SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "enrich_reference_catalog_uuids.py"
SCRIPT_SPEC = importlib.util.spec_from_file_location("enrich_reference_catalog_uuids", SCRIPT_PATH)
SCRIPT_MODULE = importlib.util.module_from_spec(SCRIPT_SPEC)
assert SCRIPT_SPEC.loader is not None
SCRIPT_SPEC.loader.exec_module(SCRIPT_MODULE)

NS = "http://www.EcoInvent.org/EcoSpold02"
ACTIVITY_ID = "11111111-1111-4111-8111-111111111111"
NAME_ID = "22222222-2222-4222-8222-222222222222"
GEOGRAPHY_ID = "33333333-3333-4333-8333-333333333333"
PRODUCT_ID = "44444444-4444-4444-8444-444444444444"
BIOSPHERE_ID = "55555555-5555-4555-8555-555555555555"


def _write(path: Path, body: str) -> None:
    path.write_text(f'<?xml version="1.0"?><root xmlns="{NS}">{body}</root>', encoding="utf-8")


def test_enricher_uses_brightway_location_normalization_and_exact_release_uuids(tmp_path):
    release = tmp_path / "release"
    master = release / "MasterData"
    datasets = release / "datasets"
    master.mkdir(parents=True)
    datasets.mkdir()
    _write(master / "ActivityNames.xml", f'<activityName id="{NAME_ID}"><name>market</name></activityName>')
    _write(
        master / "Geographies.xml",
        f'<geography id="{GEOGRAPHY_ID}"><shortname>HICC</shortname></geography>',
    )
    _write(
        master / "IntermediateExchanges.xml",
        f'<intermediateExchange id="{PRODUCT_ID}"><name>product</name><unitName>kg</unitName></intermediateExchange>',
    )
    _write(
        master / "ActivityIndex.xml",
        f'<activityIndexEntry id="{ACTIVITY_ID}" activityNameId="{NAME_ID}" geographyId="{GEOGRAPHY_ID}"/>',
    )
    _write(
        master / "ElementaryExchanges.xml",
        f'<elementaryExchange id="{BIOSPHERE_ID}"><name>Carbon dioxide, fossil</name><unitName>kg</unitName>'
        "<compartment><compartment>air</compartment><subcompartment>unspecified</subcompartment></compartment>"
        "</elementaryExchange>",
    )
    (datasets / f"{ACTIVITY_ID}_{PRODUCT_ID}.spold").write_text("", encoding="utf-8")
    catalog = tmp_path / "ecoinvent__3.6__cutoff.json"
    catalog.write_text(
        json.dumps(
            {
                "profile": {"family": "ecoinvent", "version": "3.6", "system_model": "cutoff"},
                "technosphere": [
                    {
                        "name": "market",
                        "reference_product": "product",
                        "location": "US-HICC",
                        "unit": "kilogram",
                    }
                ],
                "biosphere": [
                    {
                        "name": "Carbon dioxide, fossil",
                        "categories": ["air"],
                        "unit": "kilogram",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    assert SCRIPT_MODULE.enrich_catalog(catalog, release) == (1, 1)

    payload = json.loads(catalog.read_text(encoding="utf-8"))
    assert payload["schema_version"] == 2
    assert payload["technosphere"][0]["activity_id"] == ACTIVITY_ID
    assert payload["technosphere"][0]["flow_id"] == PRODUCT_ID
    assert payload["biosphere"][0]["flow_id"] == BIOSPHERE_ID
