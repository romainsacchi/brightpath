"""Verify automatic ILCD references against a synthetic pre-existing openLCA background.

Uses an explicitly supplied openLCA runtime and its public units template. All
process amounts are synthetic; no installed user database is opened.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
from pathlib import Path

_BRIDGE = """
import json, sys
from java.io import File
from org.openlca.core.database import Derby, FlowDao, FlowPropertyDao, ProcessDao
from org.openlca.core.model import Flow, Process
from org.openlca.core.model import Version
from org.openlca.ilcd.io import ZipStore, FileStore
from org.openlca.io.ilcd.input import Import
from org.openlca.io.ilcd.output import Export
config = json.load(open(sys.argv[1]))
folder = File(config['database'])
Derby.unzip(folder, File(config['units_template']))
db = Derby(folder)
store = ZipStore(File(config['package']))
try:
    properties = FlowPropertyDao(db)
    energy = properties.getForRefId(config['property_id'])
    assert energy is not None
    flow = Flow.product('Synthetic background product', energy)
    flow.refId = config['flow_id']
    flow.version = Version.valueOf(1, 0, 0)
    flow = FlowDao(db).insert(flow)
    provider = Process.of('Synthetic pre-existing background', flow)
    provider.refId = config['process_id']
    provider = ProcessDao(db).insert(provider)
    mass = properties.getForRefId(config['mass_property_id'])
    emission = Flow.elementary('Synthetic pre-existing emission', mass)
    emission.refId = config['emission_id']
    emission.version = Version.valueOf(1, 0, 0)
    emission = FlowDao(db).insert(emission)
    importer = Import.of(store, db)
    importer.run()
    processes = ProcessDao(db).getAll()
    assert len(processes) == 2, len(processes)
    consumer = next(p for p in processes if p.name == 'Automatic ILCD reference pilot')
    exchange = next(e for e in consumer.exchanges if e.isInput)
    assert exchange.flow.id == flow.id
    assert exchange.defaultProviderId == provider.id
    assert abs(exchange.amount - 7.2) < 1e-12, exchange.amount
    assert exchange.unit.name == 'MJ', exchange.unit.name
    assert abs(exchange.uncertainty.parameter1 - 7.2) < 1e-12
    assert abs(exchange.uncertainty.parameter2 - 0.72) < 1e-12
    biosphere = next(e for e in consumer.exchanges if e.flow.id == emission.id)
    assert biosphere.amount == 0.5
    assert len(FlowDao(db).getAll()) == 3
    output = FileStore(File(config['native_export']))
    try:
        Export(db, output).write(consumer)
    finally:
        output.close()
    print('BRIGHTPATH_ILCD_BACKGROUND_OK')
finally:
    store.close()
    db.close()
"""


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--java", type=Path, required=True)
    parser.add_argument("--libraries", type=Path, required=True)
    parser.add_argument("--units-template", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)

    from brightpath.background.catalogs import catalog_provider_from_environment
    from brightpath.core import (
        BackgroundContext,
        BiosphereProfile,
        FormatProfile,
        InventoryContext,
        TechnosphereProfile,
    )
    from brightpath.formats.ilcd import load_ilcd, write_ilcd
    from brightpath.formats.ilcd_background import automatic_references, openlca_key, quantity_definition
    from brightpath.formats.ilcd_references import ILCDReferenceCatalog
    from brightpath.models import InventoryDocument

    background = BackgroundContext(
        TechnosphereProfile("ecoinvent", "3.10.1", "cutoff"), BiosphereProfile("ecoinvent", "3.10.1")
    )
    catalogs = catalog_provider_from_environment()
    tech = catalogs.load_technosphere(background.technosphere)
    key = next(key for key in sorted(tech.references) if key[-1] == "kilowatt hour")
    bio = catalogs.load_biosphere(background.biosphere)
    bio_key = next(key for key in sorted(bio.references) if key[1][0] == "air" and key[-1] == "kilogram")
    record = {
        "name": "Automatic ILCD reference pilot",
        "reference product": "Synthetic product",
        "location": "CH",
        "unit": "kilogram",
        "code": "ilcd-background-pilot",
        "parameters": [{"name": "dose", "amount": 1}],
        "exchanges": [
            {
                "type": "production",
                "name": "Automatic ILCD reference pilot",
                "reference product": "Synthetic product",
                "location": "CH",
                "unit": "kilogram",
                "amount": 1,
            },
            {
                **dict(zip(("name", "reference product", "location", "unit"), key, strict=True)),
                "type": "technosphere",
                "amount": 2,
                "formula": "dose * 2",
                "uncertainty type": 3,
                "loc": 2,
                "scale": 0.2,
            },
            {"type": "biosphere", "name": bio_key[0], "categories": bio_key[1], "unit": bio_key[2], "amount": 0.5},
        ],
    }
    document = InventoryDocument(data=[record], context=InventoryContext(FormatProfile("brightway_excel"), background))
    metadata = {
        "defaults": {
            "dataset_version": "01.00.000",
            "process_type": "Unit process, single operation",
            "reference_year": 2020,
            "valid_until": 2025,
        }
    }
    package = write_ilcd(document, args.output / "brightpath.zip", metadata_profile=metadata, dialect="openlca")
    activity_id, product_id = tech.references[key]
    config = {
        "database": str(args.output / "native-database"),
        "units_template": str(args.units_template),
        "package": str(package),
        "native_export": str(args.output / "native-export"),
        "process_id": openlca_key(activity_id, product_id),
        "flow_id": product_id,
        "property_id": quantity_definition("kilowatt hour")["property_id"],
        "mass_property_id": quantity_definition("kilogram")["property_id"],
        "emission_id": bio.references[bio_key],
    }
    config_path = args.output / "config.json"
    config_path.write_text(json.dumps(config, indent=2))
    bridge = args.output / "native_check.py"
    bridge.write_text(_BRIDGE)
    # Pin these versions ahead of any older jars shipped in the application.
    classpath = os.pathsep.join(
        str(args.libraries / name) for name in ("olca-io-2.6.2.jar", "olca-core-2.6.2.jar", "*")
    )
    result = subprocess.run(
        [
            str(args.java),
            f"-Dpython.cachedir={args.output / 'jython-cache'}",
            f"-Dderby.stream.error.file={args.output / 'derby.log'}",
            "-cp",
            classpath,
            "org.python.util.jython",
            str(bridge),
            str(config_path),
        ],
        capture_output=True,
        text=True,
        check=False,
        timeout=180,
    )
    (args.output / "openlca.log").write_text(result.stdout + result.stderr)
    if result.returncode or "BRIGHTPATH_ILCD_BACKGROUND_OK" not in result.stdout:
        raise RuntimeError(f"Native openLCA reference verification failed; see {args.output / 'openlca.log'}")
    references = automatic_references(document, ILCDReferenceCatalog(), dialect="openlca")
    restored = load_ilcd(
        args.output / "native-export",
        context=InventoryContext(FormatProfile("ilcd"), background),
        reference_catalog=references,
    )
    exchange = next(e for e in restored.data[0]["exchanges"] if e["type"] == "technosphere")
    assert abs(exchange["amount"] - 2) < 1e-12
    assert exchange["unit"] == "kilowatt hour"
    assert abs(exchange["scale"] - 0.2) < 1e-12
    # Re-export from the preserved native reference closure, including its metadata.
    write_ilcd(restored, args.output / "reimported.zip")
    report = {
        "native_import": "passed",
        "preexisting_supplier": "reused",
        "product_flow": "reused",
        "biosphere_flow": "reused",
        "kwh_input": 2,
        "native_mj_input": 7.2,
        "normal_sd_kwh": 0.2,
        "normal_sd_mj": 0.72,
        "native_export_reimport": "passed",
    }
    (args.output / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
