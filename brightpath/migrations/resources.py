from __future__ import annotations

import json
import re
from functools import lru_cache
from pathlib import Path

from brightpath import DATA_DIR
from brightpath.exceptions import MigrationError
from brightpath.models import BackgroundProfile

_PROFILE_ID = re.compile(r"^ecoinvent-(?P<version>\d+(?:\.\d+){1,2})-(?P<kind>cutoff|consequential|biosphere)$")


@lru_cache(maxsize=4)
def load_technosphere_resources(system_model: str) -> dict[tuple[str, str], dict]:
    directory = DATA_DIR / "migrations" / "ecoinvent" / system_model
    return _load_resources(directory, expected_kind=system_model)


@lru_cache(maxsize=1)
def load_biosphere_resources() -> dict[tuple[str, str], dict]:
    directory = DATA_DIR / "migrations" / "ecoinvent" / "biosphere"
    return _load_resources(directory, expected_kind="biosphere")


def available_ecoinvent_versions(system_model: str = "cutoff") -> tuple[str, ...]:
    """Return versions connected by packaged migration resources."""

    resources = load_technosphere_resources(system_model)
    return tuple(sorted({version for pair in resources for version in pair}, key=_version_key))


def _load_resources(directory: Path, *, expected_kind: str) -> dict[tuple[str, str], dict]:
    if not directory.is_dir():
        return {}

    resources = {}
    for path in sorted(directory.glob("*.json")):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise MigrationError(f"Could not load migration resource {path}.") from exc

        source_version, source_kind = _parse_profile_id(payload.get("source_id"), path)
        target_version, target_kind = _parse_profile_id(payload.get("target_id"), path)
        if source_kind != expected_kind or target_kind != expected_kind:
            raise MigrationError(
                f"Migration resource {path} has unexpected profile kinds " f"{source_kind!r} and {target_kind!r}."
            )
        _validate_rule_lists(payload, path)
        payload["_path"] = str(path)
        resources[(source_version, target_version)] = payload
    return resources


def _parse_profile_id(value, path: Path) -> tuple[str, str]:
    match = _PROFILE_ID.fullmatch(str(value or ""))
    if not match:
        raise MigrationError(f"Malformed source_id or target_id in migration resource {path}.")
    version = (
        BackgroundProfile(
            family="ecoinvent",
            version=match.group("version"),
            system_model="cutoff",
        )
        .normalized()
        .version
    )
    return version, match.group("kind")


def _validate_rule_lists(payload: dict, path: Path) -> None:
    for field in ("replace", "disaggregate", "delete"):
        rules = payload.get(field, [])
        if not isinstance(rules, list):
            raise MigrationError(f"Migration resource {path} field {field!r} must be a list.")
        for index, rule in enumerate(rules):
            if not isinstance(rule, dict) or not isinstance(rule.get("source"), dict):
                raise MigrationError(f"Migration resource {path} has an invalid {field} rule at index {index}.")
            if field == "replace" and not isinstance(rule.get("target"), dict):
                raise MigrationError(f"Migration resource {path} has an invalid replacement target at index {index}.")
            if field == "disaggregate" and not isinstance(rule.get("targets"), list):
                raise MigrationError(f"Migration resource {path} has invalid disaggregation targets at index {index}.")
            if field == "disaggregate" and not all(isinstance(target, dict) for target in rule.get("targets", [])):
                raise MigrationError(
                    f"Migration resource {path} has a non-dictionary disaggregation target " f"at index {index}."
                )


def _version_key(version: str) -> tuple[int, ...]:
    return tuple(int(part) for part in version.split("."))
