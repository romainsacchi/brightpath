"""Verify a synthetic ILCD round trip with an installed openLCA 2.6 runtime.

Example::

    python scripts/verify_ilcd_openlca.py --java /path/to/java \
        --libraries /path/to/openlca/libs --output /tmp/ilcd-pilot

The check uses an in-memory database, never a user's openLCA database. The
application runtime is an optional verification tool, not a BrightPath dependency.
"""

from __future__ import annotations

import argparse
import json
import subprocess
from pathlib import Path

_BRIDGE = """
import sys
from java.io import File
from org.openlca.core.database import Derby, ProcessDao
from org.openlca.ilcd.io import ZipStore, FileStore
from org.openlca.io.ilcd.input import Import
from org.openlca.io.ilcd.output import Export
db = Derby.createInMemory()
store = ZipStore(File(sys.argv[1]))
try:
    importer = Import.of(store, db)
    importer.run()
    processes = ProcessDao(db).getAll()
    assert len(processes) == 2
    producer = next(p for p in processes if p.name == 'ILCD pilot producer')
    consumer = next(p for p in processes if p.name == 'ILCD pilot consumer')
    exchange = next(e for e in consumer.exchanges if e.isInput)
    assert exchange.defaultProviderId == producer.id
    assert exchange.amount == -2.0
    assert producer.quantitativeReference.amount == 100.0
    assert producer.quantitativeReference.unit.name == 'kilogram'
    assert consumer.quantitativeReference.uncertainty.parameter1 == 1.0
    assert abs(consumer.quantitativeReference.uncertainty.parameter2 - 0.1) < 1e-12
    output = FileStore(File(sys.argv[2]))
    try:
        exporter = Export(db, output)
        for process in processes:
            exporter.write(process)
    finally:
        output.close()
    print('BRIGHTPATH_ILCD_PILOT_OK')
finally:
    store.close()
    db.close()
"""


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--java", required=True, type=Path)
    parser.add_argument("--libraries", required=True, type=Path)
    parser.add_argument(
        "--output", required=True, type=Path, help="New directory for synthetic artifacts and validation evidence."
    )
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)

    from brightpath.core import (
        BackgroundContext,
        BiosphereProfile,
        FormatProfile,
        InventoryContext,
        TechnosphereProfile,
    )
    from brightpath.formats.ilcd import load_ilcd, write_ilcd
    from brightpath.models import InventoryDocument

    background = BackgroundContext(
        TechnosphereProfile("ecoinvent", "3.10.1", "cutoff"), BiosphereProfile("ecoinvent", "3.10.1")
    )
    records = []
    for suffix, quantity in (("producer", 100), ("consumer", 1)):
        record = {
            "name": "ILCD pilot " + suffix,
            "reference product": "Pilot " + suffix + " product",
            "location": "CH",
            "unit": "kilogram",
            "code": "ilcd-pilot-" + suffix,
        }
        record["exchanges"] = [
            {
                **{k: record[k] for k in ("name", "reference product", "location", "unit")},
                "type": "production",
                "amount": quantity,
            }
        ]
        records.append(record)
    records[1]["exchanges"][0].update({"uncertainty type": 3, "loc": 1.0, "scale": 0.1})
    records[1]["exchanges"].append(
        {
            **{k: records[0][k] for k in ("name", "reference product", "location", "unit")},
            "type": "technosphere",
            "amount": -2,
        }
    )
    context = InventoryContext(FormatProfile("brightway_excel"), background)
    document = InventoryDocument(data=records, context=context)
    metadata = {
        "defaults": {
            "dataset_version": "01.00.000",
            "process_type": "Unit process, single operation",
            "reference_year": 2020,
            "valid_until": 2025,
        }
    }
    package = write_ilcd(document, args.output / "brightpath.zip", metadata_profile=metadata, dialect="openlca")
    bridge = args.output / "openlca_bridge.py"
    bridge.write_text(_BRIDGE, encoding="utf-8")
    native_output = args.output / "native-export"
    command = [
        str(args.java),
        f"-Dpython.cachedir={args.output / 'jython-cache'}",
        f"-Dderby.stream.error.file={args.output / 'derby.log'}",
        "-cp",
        str(args.libraries / "*"),
        "org.python.util.jython",
        str(bridge),
        str(package),
        str(native_output),
    ]
    result = subprocess.run(command, capture_output=True, text=True, check=False, timeout=180)
    (args.output / "openlca.log").write_text(result.stdout + result.stderr, encoding="utf-8")
    if result.returncode or "BRIGHTPATH_ILCD_PILOT_OK" not in result.stdout:
        raise RuntimeError(f"openLCA pilot failed; inspect {args.output / 'openlca.log'}")
    parsed = load_ilcd(native_output, context=InventoryContext(FormatProfile("ilcd"), background))
    by_name = {record["name"]: record for record in parsed.data}
    assert by_name["ILCD pilot producer"]["exchanges"][0]["amount"] == 100
    assert by_name["ILCD pilot consumer"]["exchanges"][1]["amount"] == -2
    assert by_name["ILCD pilot consumer"]["exchanges"][1]["name"] == "ILCD pilot producer"
    assert abs(by_name["ILCD pilot consumer"]["exchanges"][0]["scale"] - 0.1) < 1e-12
    write_ilcd(parsed, args.output / "reimported.zip", dialect="openlca")
    report = {
        "dialect": "openlca",
        "normal_standard_deviation": 0.1,
        "processes": 2,
        "reference_production_kg": 100,
        "input_kg": -2,
        "native_import": "passed",
        "selected_supplier": "preserved",
        "native_export_reimport": "passed",
        "findings": sorted({i["code"] for record in parsed.data for i in record["ilcd"]["issues"]}),
    }
    (args.output / "report.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
