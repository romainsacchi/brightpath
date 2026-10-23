"""Add authoritative ecoSpold2 UUIDs to a packaged reference catalog.

The source must be an extracted, exact ecoinvent system-model release with
``datasets`` and ``MasterData`` directories. No UUID is inferred across
versions or system models. The catalog is only replaced when every existing
identity has one unambiguous UUID reference.
"""

from __future__ import annotations

import argparse
import json
import os
import tempfile
from pathlib import Path
from uuid import UUID
from xml.etree import ElementTree

from bw2io import normalize_units
from bw2io.strategies.locations import GEO_UPDATE

NAMESPACE = "http://www.EcoInvent.org/EcoSpold02"
NS = {"es": NAMESPACE}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("catalog", type=Path, help="Combined BrightPath reference-catalog JSON")
    parser.add_argument("release", type=Path, help="Extracted exact ecoinvent ecoSpold2 release")
    parser.add_argument("--check", action="store_true", help="Validate coverage without modifying the catalog")
    parser.add_argument(
        "--biosphere-only",
        action="store_true",
        help="Only enrich the biosphere rows (for catalogs that deliberately use another technosphere)",
    )
    return parser.parse_args()


def _children(path: Path, element_name: str):
    tag = f"{{{NAMESPACE}}}{element_name}"
    for _event, element in ElementTree.iterparse(path, events=("end",)):
        if element.tag == tag:
            yield element
            element.clear()


def _text(element: ElementTree.Element, child_name: str) -> str:
    child = element.find(f"es:{child_name}", NS)
    # ecoinvent identities can contain significant non-breaking spaces; match
    # the importer behavior and preserve leaf text byte-for-byte.
    return str(child.text or "") if child is not None else ""


def _uuid(value: str, *, source: Path) -> str:
    try:
        return str(UUID(value))
    except (AttributeError, TypeError, ValueError) as error:
        raise ValueError(f"Invalid UUID {value!r} in {source}.") from error


def _id_text_map(path: Path, element_name: str, child_name: str) -> dict[str, str]:
    result = {}
    for element in _children(path, element_name):
        identifier = _uuid(str(element.get("id") or ""), source=path)
        value = _text(element, child_name)
        if not value:
            raise ValueError(f"Missing {child_name} for {identifier} in {path}.")
        previous = result.setdefault(identifier, value)
        if previous != value:
            raise ValueError(f"Conflicting {child_name} values for {identifier} in {path}.")
    return result


def technosphere_references(release: Path) -> dict[tuple[str, str, str, str], tuple[str, str]]:
    master = release / "MasterData"
    names = _id_text_map(master / "ActivityNames.xml", "activityName", "name")
    geographies = _id_text_map(master / "Geographies.xml", "geography", "shortname")
    products = {}
    for element in _children(master / "IntermediateExchanges.xml", "intermediateExchange"):
        identifier = _uuid(str(element.get("id") or ""), source=master / "IntermediateExchanges.xml")
        value = (_text(element, "name"), normalize_units(_text(element, "unitName")))
        previous = products.setdefault(identifier, value)
        if previous != value:
            raise ValueError(f"Conflicting intermediate-exchange values for {identifier}.")

    activity_index = {}
    index_path = master / "ActivityIndex.xml"
    for element in _children(index_path, "activityIndexEntry"):
        activity_id = _uuid(str(element.get("id") or ""), source=index_path)
        activity_index[activity_id] = (
            _uuid(str(element.get("activityNameId") or ""), source=index_path),
            _uuid(str(element.get("geographyId") or ""), source=index_path),
        )

    result = {}
    datasets = release / "datasets"
    files = sorted(datasets.glob("*.spold"))
    if not files:
        raise ValueError(f"No .spold datasets found in {datasets}.")
    for path in files:
        try:
            raw_activity_id, raw_flow_id = path.stem.split("_")
        except ValueError as error:
            raise ValueError(f"Unexpected ecoSpold2 dataset filename: {path.name}.") from error
        activity_id = _uuid(raw_activity_id, source=path)
        flow_id = _uuid(raw_flow_id, source=path)
        try:
            name_id, geography_id = activity_index[activity_id]
            product, unit = products[flow_id]
            location = geographies[geography_id]
            identity = (names[name_id], product, GEO_UPDATE.get(location, location), unit)
        except KeyError as error:
            raise ValueError(f"Incomplete master data for {path.name}: missing {error.args[0]!r}.") from error
        reference = (activity_id, flow_id)
        previous = result.setdefault(identity, reference)
        if previous != reference:
            raise ValueError(f"Ambiguous technosphere identity {identity!r}: {previous!r} and {reference!r}.")
    return result


def biosphere_references(release: Path) -> dict[tuple[str, tuple[str, ...], str], str]:
    path = release if release.is_file() else release / "MasterData" / "ElementaryExchanges.xml"
    result = {}
    for element in _children(path, "elementaryExchange"):
        compartment = element.find("es:compartment", NS)
        categories = ()
        if compartment is not None:
            categories = tuple(
                value
                for value in (
                    _text(compartment, "compartment"),
                    _text(compartment, "subcompartment"),
                )
                if value and value not in {"unspecified", "(unspecified)"}
            )
        identity = (
            _text(element, "name"),
            categories,
            normalize_units(_text(element, "unitName")),
        )
        flow_id = _uuid(str(element.get("id") or ""), source=path)
        previous = result.setdefault(identity, flow_id)
        if previous != flow_id:
            raise ValueError(f"Ambiguous biosphere identity {identity!r}: {previous!r} and {flow_id!r}.")
    return result


def enrich_catalog(
    catalog_path: Path,
    release: Path,
    *,
    check: bool = False,
    biosphere_only: bool = False,
) -> tuple[int, int]:
    payload = json.loads(catalog_path.read_text(encoding="utf-8"))
    technosphere = {} if biosphere_only else technosphere_references(release)
    biosphere = biosphere_references(release)

    missing_technosphere = []
    if not biosphere_only:
        for row in payload.get("technosphere", ()):
            identity = (row["name"], row["reference_product"], row["location"], row["unit"])
            reference = technosphere.get(identity)
            if reference is None:
                missing_technosphere.append(identity)
            else:
                row["activity_id"], row["flow_id"] = reference

    missing_biosphere = []
    for row in payload.get("biosphere", ()):
        identity = (row["name"], tuple(row["categories"]), row["unit"])
        flow_id = biosphere.get(identity)
        if flow_id is None:
            missing_biosphere.append(identity)
        else:
            row["flow_id"] = flow_id

    if missing_technosphere or missing_biosphere:
        examples = [*missing_technosphere[:2], *missing_biosphere[:2]]
        raise ValueError(
            f"Exact UUID coverage failed for {catalog_path.name}: "
            f"{len(missing_technosphere)} technosphere and {len(missing_biosphere)} biosphere "
            f"identities are missing; examples: {examples!r}."
        )

    payload["schema_version"] = 2
    if not check:
        encoded = (json.dumps(payload, indent=2, sort_keys=True) + "\n").encode("utf-8")
        catalog_path.parent.mkdir(parents=True, exist_ok=True)
        descriptor, temporary_name = tempfile.mkstemp(prefix=f".{catalog_path.name}.", dir=catalog_path.parent)
        try:
            with os.fdopen(descriptor, "wb") as handle:
                handle.write(encoded)
            Path(temporary_name).replace(catalog_path)
        except BaseException:
            Path(temporary_name).unlink(missing_ok=True)
            raise
    return len(payload.get("technosphere", ())), len(payload.get("biosphere", ()))


def main() -> None:
    args = parse_args()
    counts = enrich_catalog(
        args.catalog,
        args.release,
        check=args.check,
        biosphere_only=args.biosphere_only,
    )
    action = "Verified" if args.check else "Enriched"
    print(f"{action} {args.catalog}: {counts[0]} technosphere and {counts[1]} biosphere UUID references.")


if __name__ == "__main__":
    main()
