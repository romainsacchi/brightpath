from __future__ import annotations

import hashlib
import json
import re
from functools import lru_cache
from pathlib import Path

from brightpath import DATA_DIR
from brightpath.core.context import resolve_migration_series
from brightpath.exceptions import MigrationError

_PROFILE_ID = re.compile(r"^ecoinvent-(?P<version>\d+(?:\.\d+){1,2})-(?P<kind>cutoff|consequential|biosphere)$")
_UVEK_TECHNOSPHERE_RESOURCE = "ecoinvent-to-uvek-2025.json"
_UVEK_BIOSPHERE_RESOURCE = "ecoinvent-to-ecoinvent-3.10-biosphere.json"


@lru_cache(maxsize=4)
def load_technosphere_resources(system_model: str) -> dict[tuple[str, str], dict]:
    directory = DATA_DIR / "migrations" / "ecoinvent" / system_model
    return _load_resources(directory, expected_kind=system_model)


@lru_cache(maxsize=1)
def load_biosphere_resources() -> dict[tuple[str, str], dict]:
    directory = DATA_DIR / "migrations" / "ecoinvent" / "biosphere"
    return _load_resources(directory, expected_kind="biosphere")


@lru_cache(maxsize=1)
def load_uvek_technosphere_resource() -> dict:
    """Load the active heuristic ecoinvent-to-UVEK technosphere resource."""

    return _load_uvek_resource(_UVEK_TECHNOSPHERE_RESOURCE, expected_axis="technosphere")


@lru_cache(maxsize=1)
def load_uvek_biosphere_resource() -> dict:
    """Load the ecoinvent 3.x to 3.10 biosphere resource used for UVEK."""

    return _load_uvek_resource(_UVEK_BIOSPHERE_RESOURCE, expected_axis="biosphere")


def available_ecoinvent_versions(system_model: str = "cutoff") -> tuple[str, ...]:
    """Return versions connected by packaged migration resources."""

    resources = load_technosphere_resources(system_model)
    return tuple(sorted({version for pair in resources for version in pair}, key=_version_key))


def _load_resources(directory: Path, *, expected_kind: str) -> dict[tuple[str, str], dict]:
    if not directory.is_dir():
        return {}

    resources = {}
    manifest = _load_resource_manifest()
    for path in sorted(directory.glob("*.json")):
        try:
            raw = path.read_bytes()
            payload = json.loads(raw.decode("utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise MigrationError(f"Could not load migration resource {path}.") from exc
        _verify_manifest_entry(path, raw, payload, manifest)

        source_version, source_kind = _parse_profile_id(payload.get("source_id"), path)
        target_version, target_kind = _parse_profile_id(payload.get("target_id"), path)
        if source_kind != expected_kind or target_kind != expected_kind:
            raise MigrationError(
                f"Migration resource {path} has unexpected profile kinds " f"{source_kind!r} and {target_kind!r}."
            )
        _validate_rule_lists(payload, path, expected_axis=expected_kind)
        payload["_path"] = str(path)
        resources[(source_version, target_version)] = payload
    return resources


def _load_uvek_resource(filename: str, *, expected_axis: str) -> dict:
    path = DATA_DIR / "migrations" / "uvek" / filename
    try:
        raw = path.read_bytes()
        payload = json.loads(raw.decode("utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise MigrationError(f"Could not load migration resource {path}.") from exc
    if not isinstance(payload, dict):
        raise MigrationError(f"Migration resource {path} must contain an object.")
    _verify_manifest_entry(path, raw, payload, _load_resource_manifest())
    if payload.get("status") != "active":
        raise MigrationError(f"Migration resource {path} is not active.")
    if payload.get("axis") != expected_axis:
        raise MigrationError(f"Migration resource {path} does not describe the {expected_axis} axis.")
    if payload.get("quality") != "heuristic":
        raise MigrationError(f"Migration resource {path} must declare its heuristic quality.")
    _validate_profile(payload.get("source_profile"), path, role="source")
    _validate_profile(payload.get("target_profile"), path, role="target")
    _validate_rule_lists(payload, path, expected_axis=expected_axis)
    payload["_path"] = str(path)
    return payload


def _validate_profile(value, path: Path, *, role: str) -> None:
    if not isinstance(value, dict) or not str(value.get("family") or ""):
        raise MigrationError(f"Migration resource {path} has an invalid {role} profile.")
    versions = value.get("versions")
    version = value.get("version")
    if versions is None and not str(version or ""):
        raise MigrationError(f"Migration resource {path} has no {role} profile version.")
    if versions is not None and (
        not isinstance(versions, list) or not versions or not all(str(item or "") for item in versions)
    ):
        raise MigrationError(f"Migration resource {path} has invalid {role} profile versions.")


@lru_cache(maxsize=1)
def _load_resource_manifest() -> dict[str, dict]:
    path = DATA_DIR / "migrations" / "RESOURCE_MANIFEST.json"
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise MigrationError(f"Could not load migration resource manifest {path}.") from exc
    if not isinstance(payload, dict) or payload.get("schema_version") != 1:
        raise MigrationError(f"Migration resource manifest {path} has an unsupported schema.")
    resources = payload.get("resources")
    if not isinstance(resources, list):
        raise MigrationError(f"Migration resource manifest {path} must define a resource list.")
    result = {}
    for index, resource in enumerate(resources):
        if not isinstance(resource, dict) or not str(resource.get("path") or ""):
            raise MigrationError(f"Migration resource manifest {path} has an invalid entry at index {index}.")
        relative = str(resource["path"])
        if relative in result:
            raise MigrationError(f"Migration resource manifest {path} repeats {relative!r}.")
        result[relative] = resource
    return result


def _verify_manifest_entry(path: Path, raw: bytes, payload: dict, manifest: dict[str, dict]) -> None:
    relative = path.relative_to(DATA_DIR / "migrations").as_posix()
    expected = manifest.get(relative)
    if expected is None:
        raise MigrationError(f"Migration resource {path} is absent from RESOURCE_MANIFEST.json.")
    if expected.get("size") != len(raw) or expected.get("sha256") != hashlib.sha256(raw).hexdigest():
        raise MigrationError(f"Migration resource {path} does not match its manifest digest or size.")
    if expected.get("name") != str(payload.get("name") or ""):
        raise MigrationError(f"Migration resource {path} name does not match its manifest.")
    if expected.get("source_id") != str(payload.get("source_id") or ""):
        raise MigrationError(f"Migration resource {path} source ID does not match its manifest.")
    if expected.get("target_id") != str(payload.get("target_id") or ""):
        raise MigrationError(f"Migration resource {path} target ID does not match its manifest.")


def _parse_profile_id(value, path: Path) -> tuple[str, str]:
    match = _PROFILE_ID.fullmatch(str(value or ""))
    if not match:
        raise MigrationError(f"Malformed source_id or target_id in migration resource {path}.")
    version = resolve_migration_series("ecoinvent", match.group("version")).migration_series
    return version, match.group("kind")


def _validate_rule_lists(payload: dict, path: Path, *, expected_axis: str) -> None:
    biosphere_identities: dict[tuple[str, tuple[str, ...], str], tuple[str, int]] = {}
    for field in ("replace", "disaggregate", "delete"):
        rules = payload.get(field, [])
        if not isinstance(rules, list):
            raise MigrationError(f"Migration resource {path} field {field!r} must be a list.")
        for index, rule in enumerate(rules):
            if not isinstance(rule, dict) or not isinstance(rule.get("source"), dict):
                raise MigrationError(f"Migration resource {path} has an invalid {field} rule at index {index}.")
            if expected_axis == "biosphere":
                identity = _validate_biosphere_source(rule["source"], path, field=field, index=index)
                previous = biosphere_identities.setdefault(identity, (field, index))
                if previous != (field, index):
                    raise MigrationError(
                        f"Migration resource {path} repeats biosphere source identity {identity!r} "
                        f"at {previous[0]} rule {previous[1]} and {field} rule {index}."
                    )
            if field == "replace" and not isinstance(rule.get("target"), dict):
                raise MigrationError(f"Migration resource {path} has an invalid replacement target at index {index}.")
            if field == "disaggregate" and not isinstance(rule.get("targets"), list):
                raise MigrationError(f"Migration resource {path} has invalid disaggregation targets at index {index}.")
            if field == "disaggregate" and not all(isinstance(target, dict) for target in rule.get("targets", [])):
                raise MigrationError(
                    f"Migration resource {path} has a non-dictionary disaggregation target " f"at index {index}."
                )


def _validate_biosphere_source(
    source: dict,
    path: Path,
    *,
    field: str,
    index: int,
) -> tuple[str, tuple[str, ...], str]:
    """Validate and return the UUID-independent identity of a biosphere rule source."""

    name = source.get("name")
    categories = source.get("categories")
    unit = source.get("unit")
    if not isinstance(name, str) or not name:
        raise MigrationError(f"Migration resource {path} {field} rule {index} has no biosphere source name.")
    if (
        not isinstance(categories, list)
        or not categories
        or not all(isinstance(category, str) and category for category in categories)
    ):
        raise MigrationError(f"Migration resource {path} {field} rule {index} has invalid biosphere categories.")
    if not isinstance(unit, str) or not unit:
        raise MigrationError(f"Migration resource {path} {field} rule {index} has no biosphere source unit.")
    return name, tuple(categories), unit


def _version_key(version: str) -> tuple[int, ...]:
    return tuple(int(part) for part in version.split("."))
