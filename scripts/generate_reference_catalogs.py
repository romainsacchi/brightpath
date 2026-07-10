from __future__ import annotations

import argparse
import os
import re
from pathlib import Path

import bw2data as bd
import bw2io
from bw2io import import_ecoinvent_release
from bw2io.importers.excel import ExcelImporter
from ecoinvent_interface import EcoinventRelease, Settings

from brightpath.catalogs import (
    collect_technosphere_catalog_entries,
    write_background_catalog,
)
from brightpath.models import BackgroundProfile

DEFAULT_ECOINVENT_VERSIONS = ["3.5", "3.6", "3.7", "3.8", "3.9", "3.10", "3.11", "3.12"]
DEFAULT_SYSTEM_MODELS = ["cutoff", "consequential"]
DEFAULT_UVEK_BIOSPHERE_PROFILE = BackgroundProfile(
    family="ecoinvent",
    version="3.10",
    system_model="cutoff",
)


def clean_credential_value(value: str) -> str:
    # RTF exports can leave line-control delimiters attached to plain-text tokens.
    return value.strip().rstrip("\\}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate local BrightPath background reference catalogs from Brightway databases."
    )
    parser.add_argument(
        "--versions",
        nargs="+",
        default=DEFAULT_ECOINVENT_VERSIONS,
        help="ecoinvent versions to generate",
    )
    parser.add_argument(
        "--system-models",
        nargs="+",
        default=DEFAULT_SYSTEM_MODELS,
        help="system models to generate",
    )
    parser.add_argument(
        "--project-prefix",
        default="brightpath-reference",
        help="Brightway project prefix for imported reference databases",
    )
    parser.add_argument(
        "--credentials-file",
        help="Path to a text or RTF file containing 'username:' and 'password:' lines",
    )
    parser.add_argument("--username", help="ecoinvent username (overrides ECOINVENT_USERNAME)")
    parser.add_argument("--password", help="ecoinvent password (overrides ECOINVENT_PASSWORD)")
    parser.add_argument(
        "--output-dir",
        help="Override output directory for generated catalogs",
    )
    parser.add_argument(
        "--skip-import",
        action="store_true",
        help="Fail instead of importing if the expected Brightway database is missing",
    )
    parser.add_argument(
        "--skip-ecoinvent",
        action="store_true",
        help="Skip ecoinvent catalog generation and only generate any explicitly requested non-ecoinvent catalogs.",
    )
    parser.add_argument(
        "--uvek-workbook",
        help="Path to a Brightway-compatible Excel workbook containing the UVEK/BAFU background database.",
    )
    parser.add_argument(
        "--uvek-version",
        default="2025",
        help="Version label to use for the generated UVEK catalog.",
    )
    return parser.parse_args()


def load_credentials(args: argparse.Namespace) -> tuple[str, str]:
    username = clean_credential_value(args.username or os.getenv("ECOINVENT_USERNAME", ""))
    password = clean_credential_value(args.password or os.getenv("ECOINVENT_PASSWORD", ""))
    if username and password:
        return username, password

    credentials_file = args.credentials_file or os.getenv("ECOINVENT_CREDENTIALS_FILE", "")
    if not credentials_file:
        raise ValueError(
            "Provide ECOINVENT_USERNAME/ECOINVENT_PASSWORD, " "--username/--password, or --credentials-file."
        )

    raw_text = Path(credentials_file).read_text(encoding="utf-8", errors="ignore")
    username_match = re.search(r"username:\s*([^\s]+)", raw_text, re.IGNORECASE)
    password_match = re.search(r"password:\s*([^\s]+)", raw_text, re.IGNORECASE)
    if not username_match or not password_match:
        raise ValueError("Could not parse username/password from credentials file.")
    return (
        clean_credential_value(username_match.group(1)),
        clean_credential_value(password_match.group(1)),
    )


def ensure_ecoinvent_database(
    *,
    version: str,
    system_model: str,
    project_prefix: str,
    username: str | None,
    password: str | None,
    skip_import: bool,
) -> tuple[str, str]:
    preferred_project_name = f"{project_prefix}-{version}-{system_model}"
    fallback_project_name = f"ecoinvent-{version}-{system_model}"
    biosphere_name = f"ecoinvent-{version}-biosphere"
    database_name = f"ecoinvent-{version}-{system_model}"

    for project_name in (preferred_project_name, fallback_project_name):
        if project_name in {project.name for project in bd.projects}:
            bd.projects.set_current(project_name)
            if database_name in bd.databases and biosphere_name in bd.databases:
                return database_name, biosphere_name

    bd.projects.set_current(preferred_project_name)

    if skip_import:
        raise ValueError(
            f"Missing Brightway databases for {version} {system_model} in projects "
            f"{preferred_project_name!r} or {fallback_project_name!r}."
        )
    if not username or not password:
        raise ValueError(f"Missing ecoinvent credentials to import {version} {system_model}.")

    import_ecoinvent_release(
        version=version,
        system_model=system_model,
        username=username,
        password=password,
        lci=True,
        lcia=False,
        biosphere_name=biosphere_name,
        biosphere_write_mode="patch",
    )
    return database_name, biosphere_name


def available_release_matrix(username: str, password: str) -> dict[str, set[str]]:
    release = EcoinventRelease(Settings(username=username, password=password))
    return {version: set(release.list_system_models(version)) for version in release.list_versions()}


def export_catalog(
    *,
    profile: BackgroundProfile,
    database_name: str,
    biosphere_name: str,
    output_dir: Path | None = None,
) -> Path:
    technosphere = sorted(
        {
            (
                str(activity.get("name") or ""),
                str(activity.get("reference product") or ""),
                str(activity.get("location") or ""),
                str(activity.get("unit") or ""),
            )
            for activity in bd.Database(database_name)
        }
    )
    biosphere = sorted(
        {
            (
                str(flow.get("name") or ""),
                tuple(str(item) for item in flow.get("categories", ())),
                str(flow.get("unit") or ""),
            )
            for flow in bd.Database(biosphere_name)
        }
    )
    return write_background_catalog(
        profile,
        technosphere=technosphere,
        biosphere=biosphere,
        output_dir=output_dir,
    )


def load_brightway_excel_inventory(path: str | Path) -> list[dict]:
    workbook = Path(path)
    if "biosphere-2-3-categories" not in bw2io.migrations:
        bw2io.create_core_migrations()
    importer = ExcelImporter(workbook)
    importer.apply_strategies()
    return importer.data


def export_uvek_catalog(
    *,
    workbook_path: str | Path,
    version: str,
    project_prefix: str,
    username: str | None,
    password: str | None,
    skip_import: bool,
    output_dir: Path | None = None,
) -> Path:
    inventory_data = load_brightway_excel_inventory(workbook_path)
    _, biosphere_name = ensure_ecoinvent_database(
        version=DEFAULT_UVEK_BIOSPHERE_PROFILE.version,
        system_model=DEFAULT_UVEK_BIOSPHERE_PROFILE.system_model,
        project_prefix=project_prefix,
        username=username,
        password=password,
        skip_import=skip_import,
    )
    biosphere = sorted(
        {
            (
                str(flow.get("name") or ""),
                tuple(str(item) for item in flow.get("categories", ())),
                str(flow.get("unit") or ""),
            )
            for flow in bd.Database(biosphere_name)
        }
    )
    return write_background_catalog(
        BackgroundProfile(
            family="uvek",
            version=version,
            system_model="cutoff",
        ),
        technosphere=collect_technosphere_catalog_entries(inventory_data),
        biosphere=biosphere,
        output_dir=output_dir,
    )


def main() -> None:
    args = parse_args()
    output_dir = Path(args.output_dir).expanduser() if args.output_dir else None
    username: str | None = None
    password: str | None = None

    if not args.skip_ecoinvent:
        username, password = load_credentials(args)
        release_matrix = available_release_matrix(username, password)

        for version in args.versions:
            available_models = release_matrix.get(version)
            if not available_models:
                print(f"Skipping {version}: not available to this ecoinvent account.")
                continue
            for system_model in args.system_models:
                if system_model not in available_models:
                    print(f"Skipping {version} {system_model}: unsupported release combination.")
                    continue
                profile = BackgroundProfile(
                    family="ecoinvent",
                    version=version,
                    system_model=system_model,
                ).normalized()
                database_name, biosphere_name = ensure_ecoinvent_database(
                    version=version,
                    system_model=system_model,
                    project_prefix=args.project_prefix,
                    username=username,
                    password=password,
                    skip_import=args.skip_import,
                )
                path = export_catalog(
                    profile=profile,
                    database_name=database_name,
                    biosphere_name=biosphere_name,
                    output_dir=output_dir,
                )
                print(f"Wrote {path}")

    if args.uvek_workbook:
        if username is None or password is None:
            username, password = (
                load_credentials(args) if args.credentials_file or (args.username and args.password) else ("", "")
            )
        path = export_uvek_catalog(
            workbook_path=args.uvek_workbook,
            version=args.uvek_version,
            project_prefix=args.project_prefix,
            username=username or None,
            password=password or None,
            skip_import=args.skip_import,
            output_dir=output_dir,
        )
        print(
            "Wrote "
            f"{path} using technosphere entries from {args.uvek_workbook} "
            f"and biosphere entries from {DEFAULT_UVEK_BIOSPHERE_PROFILE.family} "
            f"{DEFAULT_UVEK_BIOSPHERE_PROFILE.version} {DEFAULT_UVEK_BIOSPHERE_PROFILE.system_model}."
        )


if __name__ == "__main__":
    main()
