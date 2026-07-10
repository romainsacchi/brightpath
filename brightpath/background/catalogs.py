"""Injectable providers for exact technosphere and biosphere catalogs."""

from __future__ import annotations

import hashlib
import json
import os
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Mapping

from brightpath.core.context import BiosphereProfile, TechnosphereProfile

TechnosphereIdentity = tuple[str, str, str, str]
BiosphereIdentity = tuple[str, tuple[str, ...], str]


class CatalogNotFoundError(FileNotFoundError):
    """Raised when a provider has no catalog for an exact profile."""


class CatalogIntegrityError(ValueError):
    """Raised when catalog metadata or contents do not match their profile."""


@dataclass(frozen=True)
class TechnosphereCatalog:
    """Exact technosphere identities and auditable resource metadata."""

    profile: TechnosphereProfile
    identities: frozenset[TechnosphereIdentity]
    digest: str = ""
    schema_version: int = 1
    source: str = ""

    def __post_init__(self) -> None:
        if not isinstance(self.profile, TechnosphereProfile):
            raise TypeError("profile must be a TechnosphereProfile.")
        object.__setattr__(self, "identities", frozenset(self.identities))
        _validate_digest(self.digest)
        _validate_schema_version(self.schema_version)


@dataclass(frozen=True)
class BiosphereCatalog:
    """Exact biosphere identities and auditable resource metadata."""

    profile: BiosphereProfile
    identities: frozenset[BiosphereIdentity]
    digest: str = ""
    schema_version: int = 1
    source: str = ""

    def __post_init__(self) -> None:
        if not isinstance(self.profile, BiosphereProfile):
            raise TypeError("profile must be a BiosphereProfile.")
        object.__setattr__(self, "identities", frozenset(self.identities))
        _validate_digest(self.digest)
        _validate_schema_version(self.schema_version)


class CatalogProvider(ABC):
    """Abstract source of exact background catalogs.

    Providers are deliberately injected into validation and migration services.
    Environment variables are interpreted only by
    :func:`catalog_provider_from_environment` at the application boundary.
    """

    @abstractmethod
    def load_technosphere(self, profile: TechnosphereProfile) -> TechnosphereCatalog:
        """Load the exact technosphere catalog for *profile*."""

    @abstractmethod
    def load_biosphere(self, profile: BiosphereProfile) -> BiosphereCatalog:
        """Load the exact biosphere catalog for *profile*."""

    @abstractmethod
    def technosphere_profiles(self) -> tuple[TechnosphereProfile, ...]:
        """Return exact technosphere profiles available from this provider."""

    @abstractmethod
    def biosphere_profiles(self) -> tuple[BiosphereProfile, ...]:
        """Return exact biosphere profiles available from this provider."""


class InMemoryCatalogProvider(CatalogProvider):
    """Catalog provider for tests, applications, and non-filesystem stores."""

    def __init__(
        self,
        *,
        technosphere: Iterable[TechnosphereCatalog] = (),
        biosphere: Iterable[BiosphereCatalog] = (),
    ) -> None:
        self._technosphere = _unique_catalogs(technosphere, "technosphere")
        self._biosphere = _unique_catalogs(biosphere, "biosphere")

    def load_technosphere(self, profile: TechnosphereProfile) -> TechnosphereCatalog:
        try:
            return self._technosphere[profile]
        except KeyError as error:
            raise CatalogNotFoundError(f"No technosphere catalog is available for {profile.label()}.") from error

    def load_biosphere(self, profile: BiosphereProfile) -> BiosphereCatalog:
        try:
            return self._biosphere[profile]
        except KeyError as error:
            raise CatalogNotFoundError(f"No biosphere catalog is available for {profile.label()}.") from error

    def technosphere_profiles(self) -> tuple[TechnosphereProfile, ...]:
        return tuple(sorted(self._technosphere, key=_technosphere_profile_key))

    def biosphere_profiles(self) -> tuple[BiosphereProfile, ...]:
        return tuple(sorted(self._biosphere, key=_biosphere_profile_key))


class DirectoryCatalogProvider(CatalogProvider):
    """Load the current combined JSON catalog files from one directory.

    Existing BrightPath resources contain both identity sets in files named
    ``family__version__system-model.json``. This provider exposes those files
    through independent typed catalog methods and validates the embedded
    profile before returning either axis.
    """

    def __init__(self, directory: str | Path) -> None:
        self.directory = Path(directory).expanduser()
        self._technosphere_cache: dict[TechnosphereProfile, TechnosphereCatalog] = {}
        self._biosphere_cache: dict[BiosphereProfile, BiosphereCatalog] = {}
        self._manifest = _load_catalog_manifest(self.directory)

    def load_technosphere(self, profile: TechnosphereProfile) -> TechnosphereCatalog:
        if profile in self._technosphere_cache:
            return self._technosphere_cache[profile]
        path = self.directory / _combined_filename(profile)
        payload, digest = _load_payload(path)
        _verify_manifest_resource(path, payload, digest, self._manifest)
        embedded = _technosphere_profile(payload, path)
        if embedded != profile:
            raise CatalogIntegrityError(
                f"Catalog {path} embeds {embedded.label()}, not requested profile {profile.label()}."
            )
        identities = frozenset(_parse_technosphere_rows(payload.get("technosphere"), path))
        catalog = TechnosphereCatalog(
            profile=profile,
            identities=identities,
            digest=digest,
            schema_version=_schema_version(payload, path),
            source=str(path.resolve()),
        )
        self._technosphere_cache[profile] = catalog
        return catalog

    def load_biosphere(self, profile: BiosphereProfile) -> BiosphereCatalog:
        if profile in self._biosphere_cache:
            return self._biosphere_cache[profile]
        candidates = sorted(self.directory.glob(f"{profile.family}__{profile.version}__*.json"))
        if not candidates:
            raise CatalogNotFoundError(f"No biosphere catalog is available for {profile.label()}.")

        loaded = []
        for path in candidates:
            payload, digest = _load_payload(path)
            _verify_manifest_resource(path, payload, digest, self._manifest)
            embedded = _technosphere_profile(payload, path)
            if (embedded.family, embedded.version) != (profile.family, profile.version):
                raise CatalogIntegrityError(
                    f"Catalog {path} does not match requested biosphere profile {profile.label()}."
                )
            identities = frozenset(_parse_biosphere_rows(payload.get("biosphere"), path))
            loaded.append((path, payload, digest, identities))

        schema_versions = {_schema_version(payload, path) for path, payload, _digest, _identities in loaded}
        if len(schema_versions) != 1:
            sources = ", ".join(path.name for path, _payload, _digest, _identities in loaded)
            raise CatalogIntegrityError(
                f"Biosphere catalog shards for {profile.label()} use inconsistent schema versions: {sources}."
            )
        identities = frozenset(identity for _path, _payload, _digest, rows in loaded for identity in rows)
        catalog = BiosphereCatalog(
            profile=profile,
            identities=identities,
            digest=_composite_biosphere_digest(profile, loaded, identities),
            schema_version=next(iter(schema_versions)),
            source=";".join(str(path.resolve()) for path, _payload, _digest, _identities in loaded),
        )
        self._biosphere_cache[profile] = catalog
        return catalog

    def technosphere_profiles(self) -> tuple[TechnosphereProfile, ...]:
        profiles = []
        for path in sorted(self.directory.glob("*__*__*.json")):
            try:
                family, version, system_model = path.stem.split("__")
                profiles.append(TechnosphereProfile(family, version, system_model))
            except (TypeError, ValueError):
                continue
        return tuple(sorted(set(profiles), key=_technosphere_profile_key))

    def biosphere_profiles(self) -> tuple[BiosphereProfile, ...]:
        profiles = {BiosphereProfile(profile.family, profile.version) for profile in self.technosphere_profiles()}
        return tuple(sorted(profiles, key=_biosphere_profile_key))


class PackageCatalogProvider(DirectoryCatalogProvider):
    """Load the reference catalogs distributed with BrightPath."""

    def __init__(self) -> None:
        from brightpath import DATA_DIR

        super().__init__(DATA_DIR / "export" / "reference_catalogs")


class CompositeCatalogProvider(CatalogProvider):
    """Try providers in order, allowing application catalogs to override package data."""

    def __init__(self, providers: Iterable[CatalogProvider]) -> None:
        self.providers = tuple(providers)
        if not self.providers:
            raise ValueError("CompositeCatalogProvider requires at least one provider.")
        if any(not isinstance(provider, CatalogProvider) for provider in self.providers):
            raise TypeError("providers must contain only CatalogProvider instances.")

    def load_technosphere(self, profile: TechnosphereProfile) -> TechnosphereCatalog:
        return self._first("load_technosphere", profile)

    def load_biosphere(self, profile: BiosphereProfile) -> BiosphereCatalog:
        return self._first("load_biosphere", profile)

    def technosphere_profiles(self) -> tuple[TechnosphereProfile, ...]:
        profiles = {profile for provider in self.providers for profile in provider.technosphere_profiles()}
        return tuple(sorted(profiles, key=_technosphere_profile_key))

    def biosphere_profiles(self) -> tuple[BiosphereProfile, ...]:
        profiles = {profile for provider in self.providers for profile in provider.biosphere_profiles()}
        return tuple(sorted(profiles, key=_biosphere_profile_key))

    def _first(self, method: str, profile):
        errors = []
        for provider in self.providers:
            try:
                return getattr(provider, method)(profile)
            except CatalogNotFoundError as error:
                errors.append(str(error))
        raise CatalogNotFoundError(" ".join(errors))


def catalog_provider_from_environment() -> CatalogProvider:
    """Create the application default provider.

    ``BRIGHTPATH_REFERENCE_DIR`` is an application convenience, not hidden
    state in the provider contract. When set, custom catalogs take precedence
    and packaged catalogs remain available as a fallback.
    """

    configured = (os.getenv("BRIGHTPATH_REFERENCE_DIR") or "").strip()
    packaged = PackageCatalogProvider()
    if not configured:
        return packaged
    return CompositeCatalogProvider((DirectoryCatalogProvider(configured), packaged))


def _unique_catalogs(catalogs: Iterable, kind: str) -> Mapping:
    result = {}
    for catalog in catalogs:
        if catalog.profile in result:
            raise ValueError(f"Duplicate {kind} catalog for {catalog.profile.label()}.")
        result[catalog.profile] = catalog
    return result


def _combined_filename(profile: TechnosphereProfile) -> str:
    return f"{profile.family}__{profile.version}__{profile.system_model}.json"


def _load_payload(path: Path) -> tuple[dict, str]:
    if not path.is_file():
        raise CatalogNotFoundError(f"Catalog file is missing: {path}")
    raw = path.read_bytes()
    try:
        payload = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise CatalogIntegrityError(f"Catalog {path} is not valid UTF-8 JSON.") from error
    if not isinstance(payload, dict):
        raise CatalogIntegrityError(f"Catalog {path} must contain a JSON object.")
    return payload, hashlib.sha256(raw).hexdigest()


def _load_catalog_manifest(directory: Path) -> dict[str, dict]:
    path = directory / "RESOURCE_MANIFEST.json"
    if not path.is_file():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise CatalogIntegrityError(f"Catalog manifest {path} is not valid UTF-8 JSON.") from error
    if not isinstance(payload, dict) or payload.get("schema_version") != 1:
        raise CatalogIntegrityError(f"Catalog manifest {path} has an unsupported schema.")
    resources = payload.get("resources")
    if not isinstance(resources, list):
        raise CatalogIntegrityError(f"Catalog manifest {path} field 'resources' must be a list.")
    result = {}
    for index, resource in enumerate(resources):
        if not isinstance(resource, dict) or not str(resource.get("file") or ""):
            raise CatalogIntegrityError(f"Catalog manifest {path} resource {index} is invalid.")
        filename = str(resource["file"])
        if filename in result:
            raise CatalogIntegrityError(f"Catalog manifest {path} repeats {filename!r}.")
        result[filename] = resource
    return result


def _verify_manifest_resource(path: Path, payload: dict, digest: str, manifest: dict[str, dict]) -> None:
    if not manifest:
        return
    resource = manifest.get(path.name)
    if resource is None:
        raise CatalogIntegrityError(f"Catalog {path} has no entry in RESOURCE_MANIFEST.json.")
    if resource.get("sha256") != digest or resource.get("size") != path.stat().st_size:
        raise CatalogIntegrityError(f"Catalog {path} does not match its manifest digest or size.")
    if resource.get("schema_version", 1) != payload.get("schema_version", 1):
        raise CatalogIntegrityError(f"Catalog {path} schema version does not match its manifest.")
    if resource.get("profile") != payload.get("profile"):
        raise CatalogIntegrityError(f"Catalog {path} profile does not match its manifest.")
    if resource.get("technosphere_identities") != len(payload.get("technosphere", ())):
        raise CatalogIntegrityError(f"Catalog {path} technosphere count does not match its manifest.")
    if resource.get("biosphere_identities") != len(payload.get("biosphere", ())):
        raise CatalogIntegrityError(f"Catalog {path} biosphere count does not match its manifest.")


def _composite_biosphere_digest(
    profile: BiosphereProfile,
    loaded: list[tuple[Path, dict, str, frozenset[BiosphereIdentity]]],
    identities: frozenset[BiosphereIdentity],
) -> str:
    payload = {
        "profile": {"family": profile.family, "version": profile.version},
        "sources": [{"file": path.name, "sha256": digest} for path, _payload, digest, _identities in loaded],
        "identities": [
            {"name": name, "categories": list(categories), "unit": unit}
            for name, categories, unit in sorted(identities)
        ],
    }
    encoded = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _technosphere_profile(payload: dict, path: Path) -> TechnosphereProfile:
    raw = payload.get("profile")
    if not isinstance(raw, dict):
        raise CatalogIntegrityError(f"Catalog {path} is missing an object-valued profile.")
    try:
        return TechnosphereProfile(
            family=raw["family"],
            version=raw["version"],
            system_model=raw["system_model"],
        )
    except (KeyError, TypeError, ValueError) as error:
        raise CatalogIntegrityError(f"Catalog {path} has an invalid profile.") from error


def _parse_technosphere_rows(rows, path: Path) -> Iterable[TechnosphereIdentity]:
    if not isinstance(rows, list):
        raise CatalogIntegrityError(f"Catalog {path} field 'technosphere' must be a list.")
    for index, row in enumerate(rows):
        if not isinstance(row, dict):
            raise CatalogIntegrityError(f"Catalog {path} technosphere row {index} must be an object.")
        try:
            identity = (
                str(row["name"]),
                str(row["reference_product"]),
                str(row["location"]),
                str(row["unit"]),
            )
        except KeyError as error:
            raise CatalogIntegrityError(f"Catalog {path} technosphere row {index} is incomplete.") from error
        if not all(identity):
            raise CatalogIntegrityError(f"Catalog {path} technosphere row {index} contains an empty identity field.")
        yield identity


def _parse_biosphere_rows(rows, path: Path) -> Iterable[BiosphereIdentity]:
    if not isinstance(rows, list):
        raise CatalogIntegrityError(f"Catalog {path} field 'biosphere' must be a list.")
    for index, row in enumerate(rows):
        if not isinstance(row, dict):
            raise CatalogIntegrityError(f"Catalog {path} biosphere row {index} must be an object.")
        try:
            categories = row["categories"]
            if not isinstance(categories, list) or not categories:
                raise TypeError
            identity = (
                str(row["name"]),
                tuple(str(item) for item in categories),
                str(row["unit"]),
            )
        except (KeyError, TypeError) as error:
            raise CatalogIntegrityError(f"Catalog {path} biosphere row {index} is incomplete.") from error
        if not identity[0] or not all(identity[1]) or not identity[2]:
            raise CatalogIntegrityError(f"Catalog {path} biosphere row {index} contains an empty identity field.")
        yield identity


def _schema_version(payload: dict, path: Path) -> int:
    value = payload.get("schema_version", 1)
    _validate_schema_version(value, path=path)
    return value


def _validate_schema_version(value: int, *, path: Path | None = None) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        location = f"Catalog {path}" if path else "Catalog"
        raise CatalogIntegrityError(f"{location} has an invalid schema version.")


def _validate_digest(value: str) -> None:
    if value and (len(value) != 64 or any(character not in "0123456789abcdef" for character in value.lower())):
        raise CatalogIntegrityError("Catalog digest must be a hexadecimal SHA-256 value.")


def _version_key(version: str) -> tuple:
    return tuple(int(part) if part.isdigit() else part for part in version.replace("-", ".").split("."))


def _technosphere_profile_key(profile: TechnosphereProfile) -> tuple:
    return profile.family, _version_key(profile.version), profile.system_model


def _biosphere_profile_key(profile: BiosphereProfile) -> tuple:
    return profile.family, _version_key(profile.version)
