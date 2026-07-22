"""Enrich ecoinvent biosphere migration sources from exact Brightway databases."""

from __future__ import annotations

import argparse
import json
import re
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Mapping

import bw2data as bd

SOURCE_ID_PATTERN = re.compile(r"^ecoinvent-(?P<version>\d+(?:\.\d+){1,2})-biosphere$")
RULE_VERBS = ("replace", "delete", "disaggregate")


@dataclass(frozen=True)
class BiosphereDatabase:
    """One exact ecoinvent biosphere database selected for enrichment."""

    version: str
    project: str
    database: str


def load_resources(directory: Path) -> list[tuple[Path, dict]]:
    """Load every standard ecoinvent biosphere migration resource."""

    resources = []
    for path in sorted(directory.glob("*.json")):
        resources.append((path, json.loads(path.read_text(encoding="utf-8"))))
    return resources


def source_version(payload: Mapping, path: Path) -> str:
    """Return the exact source version declared by a migration resource."""

    match = SOURCE_ID_PATTERN.fullmatch(str(payload.get("source_id") or ""))
    if not match:
        raise ValueError(f"Migration resource {path} has an invalid ecoinvent biosphere source_id.")
    return match.group("version")


def required_sources(resources: Iterable[tuple[Path, dict]]) -> dict[str, dict[str, str]]:
    """Collect the UUID and name pairs required for each exact source version."""

    required: dict[str, dict[str, str]] = defaultdict(dict)
    for path, payload in resources:
        version = source_version(payload, path)
        for verb in RULE_VERBS:
            for index, rule in enumerate(payload.get(verb, ())):
                source = rule.get("source")
                if not isinstance(source, dict):
                    raise ValueError(f"Migration resource {path} {verb} rule {index} has no source object.")
                uuid = str(source.get("uuid") or "")
                name = str(source.get("name") or "")
                if not uuid or not name:
                    raise ValueError(f"Migration resource {path} {verb} rule {index} needs both source UUID and name.")
                previous = required[version].setdefault(uuid, name)
                if previous != name:
                    raise ValueError(
                        f"Source UUID {uuid} has conflicting names {previous!r} and {name!r} for ecoinvent {version}."
                    )
    return dict(required)


def flow_index(database_name: str) -> dict[str, dict]:
    """Index a current-project Brightway biosphere database by flow code."""

    index = {}
    for flow in bd.Database(database_name):
        code = str(flow.get("code") or "")
        if not code:
            continue
        if code in index:
            raise ValueError(f"Biosphere database {database_name!r} repeats flow code {code!r}.")
        index[code] = dict(flow)
    return index


def index_covers(index: Mapping[str, Mapping], required: Mapping[str, str]) -> bool:
    """Return whether *index* contains every exact UUID and name pair."""

    return all(uuid in index and str(index[uuid].get("name") or "") == name for uuid, name in required.items())


def database_names(version: str) -> tuple[str, ...]:
    """Return accepted exact-version biosphere database names."""

    return (f"ecoinvent-{version}-biosphere", f"biosphere-{version}")


def find_database(version: str, required: Mapping[str, str], project_prefix: str) -> BiosphereDatabase | None:
    """Find an exact existing database which resolves every required source."""

    preferred = f"{project_prefix}-{version}"
    project_names = sorted(project.name for project in bd.projects)
    if preferred in project_names:
        project_names.remove(preferred)
        project_names.insert(0, preferred)

    for project_name in project_names:
        bd.projects.set_current(project_name)
        for database_name in database_names(version):
            if database_name not in bd.databases:
                continue
            index = flow_index(database_name)
            if index_covers(index, required):
                return BiosphereDatabase(version, project_name, database_name)
    return None


def import_database(version: str, required: Mapping[str, str], project_prefix: str) -> BiosphereDatabase:
    """Import one exact biosphere-only database into an isolated project."""

    from bw2io.importers.ecospold2_biosphere import Ecospold2BiosphereImporter
    from ecoinvent_interface import EcoinventRelease, ReleaseType, Settings

    project_name = f"{project_prefix}-{version}"
    database_name = f"ecoinvent-{version}-biosphere"
    bd.projects.set_current(project_name)
    if database_name not in bd.databases:
        release = EcoinventRelease(Settings())
        available_models = release.list_system_models(version)
        if not available_models:
            raise ValueError(f"No ecoSpold system model is available for ecoinvent {version}.")
        system_model = (
            "cutoff" if "cutoff" in available_models else "apos" if "apos" in available_models else available_models[0]
        )
        release_path = release.get_release(
            version=version,
            system_model=system_model,
            release_type=ReleaseType.ecospold,
        )
        importer = Ecospold2BiosphereImporter(
            name=database_name,
            filepath=release_path / "MasterData" / "ElementaryExchanges.xml",
        )
        importer.apply_strategies()
        if not importer.all_linked:
            raise ValueError(f"Biosphere database {database_name!r} contains unlinked flows.")
        importer.write_database(overwrite=False)

    index = flow_index(database_name)
    if not index_covers(index, required):
        missing = sorted(
            uuid for uuid, name in required.items() if uuid not in index or str(index[uuid].get("name") or "") != name
        )
        raise ValueError(
            f"Imported biosphere database {database_name!r} does not resolve {len(missing)} required source(s): "
            + ", ".join(missing[:10])
        )
    return BiosphereDatabase(version, project_name, database_name)


def select_databases(
    required: Mapping[str, Mapping[str, str]],
    *,
    project_prefix: str,
    import_missing: bool,
) -> dict[str, BiosphereDatabase]:
    """Select or create exact source databases for every required version."""

    selected = {}
    for version, identities in sorted(required.items(), key=lambda item: _version_key(item[0])):
        database = find_database(version, identities, project_prefix)
        if database is None and import_missing:
            database = import_database(version, identities, project_prefix)
        if database is None:
            raise ValueError(
                f"No exact Brightway biosphere database resolves all ecoinvent {version} migration sources; "
                "rerun with --import-missing."
            )
        selected[version] = database
    return selected


def enriched_source(source: Mapping, flow: Mapping) -> dict:
    """Return one source with a complete Brightway identity tuple."""

    uuid = str(source.get("uuid") or "")
    expected_name = str(source.get("name") or "")
    actual_name = str(flow.get("name") or "")
    categories = tuple(str(value) for value in flow.get("categories", ()))
    unit = str(flow.get("unit") or "")
    if expected_name != actual_name:
        raise ValueError(f"Source {uuid} is named {expected_name!r}, but its Brightway flow is {actual_name!r}.")
    if not categories or not unit:
        raise ValueError(f"Brightway flow {uuid} lacks categories or unit.")

    enriched = {
        "name": actual_name,
        "categories": list(categories),
        "unit": unit,
    }
    enriched.update((key, value) for key, value in source.items() if key not in {"name", "categories", "unit"})
    return enriched


def enrich_payload(payload: dict, index: Mapping[str, Mapping]) -> int:
    """Enrich every source object in one resource and return its rule count."""

    count = 0
    identities = set()
    for verb in RULE_VERBS:
        for rule_index, rule in enumerate(payload.get(verb, ())):
            source = rule["source"]
            uuid = str(source.get("uuid") or "")
            if uuid not in index:
                raise ValueError(f"No Brightway biosphere flow found for source UUID {uuid!r}.")
            enriched = enriched_source(source, index[uuid])
            identity = (enriched["name"], tuple(enriched["categories"]), enriched["unit"])
            if identity in identities:
                raise ValueError(
                    f"Migration resource {payload.get('name')!r} repeats source identity {identity!r} "
                    f"at {verb} rule {rule_index}."
                )
            identities.add(identity)
            rule["source"] = enriched
            count += 1
    return count


def write_resources(
    resources: Iterable[tuple[Path, dict]],
    selected: Mapping[str, BiosphereDatabase],
    *,
    write: bool,
) -> int:
    """Enrich resources from selected databases and optionally write them."""

    total = 0
    indexes = {}
    for version, database in selected.items():
        bd.projects.set_current(database.project)
        indexes[version] = flow_index(database.database)

    for path, payload in resources:
        version = source_version(payload, path)
        count = enrich_payload(payload, indexes[version])
        total += count
        if write:
            path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
        print(f"{'Wrote' if write else 'Checked'} {path}: {count} source rules from ecoinvent {version}")
    return total


def _version_key(version: str) -> tuple[int, ...]:
    return tuple(int(part) for part in version.split("."))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "directory",
        nargs="?",
        type=Path,
        default=Path("brightpath/data/migrations/ecoinvent/biosphere"),
    )
    parser.add_argument(
        "--project-prefix",
        default="brightpath-biosphere-enrichment",
        help="Prefix for isolated projects created for missing exact releases.",
    )
    parser.add_argument(
        "--import-missing",
        action="store_true",
        help="Import an exact biosphere-only database when no existing project resolves every source.",
    )
    parser.add_argument("--write", action="store_true", help="Rewrite resources; otherwise perform a dry run.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    resources = load_resources(args.directory)
    required = required_sources(resources)
    selected = select_databases(
        required,
        project_prefix=args.project_prefix,
        import_missing=args.import_missing,
    )
    for version, database in sorted(selected.items(), key=lambda item: _version_key(item[0])):
        print(f"ecoinvent {version}: project={database.project!r}, database={database.database!r}")
    total = write_resources(resources, selected, write=args.write)
    print(f"Processed {total} source rules across {len(resources)} resources.")


if __name__ == "__main__":
    main()
