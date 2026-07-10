"""Versioned, lossless transitional canonical inventory schema.

The schema provides typed views over the fields BrightPath currently needs for
validation and migration while retaining an opaque copy of every legacy
dictionary.  This allows adapters to move to the new core incrementally without
discarding format-specific or not-yet-interpreted data.
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
from copy import deepcopy
from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Tuple

from .context import InventoryContext

CANONICAL_SCHEMA_VERSION = "1.0"


class ExtensionMap(Mapping):
    """Read-only, copy-on-read mapping used for extension payloads.

    Values may contain arbitrary nested Python structures.  The constructor
    deep-copies its input, and every value access returns another deep copy, so
    neither the caller's source nor the stored payload can be mutated through
    this interface.
    """

    __slots__ = ("__data",)

    def __init__(self, data: Optional[Mapping] = None) -> None:
        if data is None:
            copied = {}
        elif isinstance(data, ExtensionMap):
            copied = data.to_dict()
        elif isinstance(data, Mapping):
            copied = deepcopy(dict(data))
        else:
            raise TypeError("Extension data must be a mapping or None.")
        object.__setattr__(self, "_ExtensionMap__data", copied)

    def __setattr__(self, name: str, value: Any) -> None:
        raise AttributeError("ExtensionMap is immutable.")

    def __getitem__(self, key: Any) -> Any:
        return deepcopy(self.__data[key])

    def __iter__(self) -> Iterator:
        return iter(self.__data)

    def __len__(self) -> int:
        return len(self.__data)

    def __repr__(self) -> str:
        return "ExtensionMap({!r})".format(self.__data)

    def __eq__(self, other: object) -> bool:
        if isinstance(other, ExtensionMap):
            return self.__data == other.__data
        if isinstance(other, Mapping):
            return self.__data == dict(other)
        return False

    def __deepcopy__(self, memo: Dict[int, Any]) -> "ExtensionMap":
        # The public interface is immutable and copy-on-read.
        return self

    def to_dict(self) -> dict:
        """Return a deep, mutable copy of the stored mapping."""

        return deepcopy(self.__data)


def _extension_map(value: Optional[Mapping]) -> ExtensionMap:
    return value if isinstance(value, ExtensionMap) else ExtensionMap(value)


def _string(mapping: Mapping, key: str, fallback: str = "") -> str:
    value = mapping.get(key, fallback)
    return str(value) if value is not None else ""


def _reference_product(mapping: Mapping) -> str:
    if "reference product" in mapping:
        return _string(mapping, "reference product")
    return _string(mapping, "product")


def _validate_namespace(source_namespace: str) -> str:
    namespace = str(source_namespace or "").strip().lower()
    if not namespace:
        raise ValueError("source_namespace must not be empty.")
    return namespace


def _unknown_extensions(mapping: Mapping, known_keys: set, source_namespace: str) -> ExtensionMap:
    unknown = {key: deepcopy(value) for key, value in mapping.items() if key not in known_keys}
    if not unknown:
        return ExtensionMap()
    return ExtensionMap({_validate_namespace(source_namespace): unknown})


def _require_mapping(value: object, path: str) -> Mapping:
    if not isinstance(value, Mapping):
        raise TypeError("{} must be a mapping.".format(path))
    return value


def _mapping_sequence(value: object, path: str) -> Tuple[Mapping, ...]:
    if value is None:
        return ()
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        raise TypeError("{} must be a sequence of mappings.".format(path))
    return tuple(_require_mapping(item, "{}[{}]".format(path, index)) for index, item in enumerate(value))


@dataclass(frozen=True)
class DatasetIdentity:
    """Typed identity of one foreground dataset."""

    name: str = ""
    reference_product: str = ""
    location: str = ""
    unit: str = ""
    code: str = ""

    @classmethod
    def from_legacy_dict(cls, data: Mapping) -> "DatasetIdentity":
        """Read identity fields without modifying the legacy dictionary."""

        data = _require_mapping(data, "dataset")
        return cls(
            name=_string(data, "name"),
            reference_product=_reference_product(data),
            location=_string(data, "location"),
            unit=_string(data, "unit"),
            code=_string(data, "code"),
        )


@dataclass(frozen=True)
class ExchangeIdentity:
    """Typed identity of one production, technosphere, or biosphere exchange."""

    name: str = ""
    reference_product: str = ""
    location: str = ""
    unit: str = ""
    code: str = ""
    categories: Tuple[str, ...] = ()

    @classmethod
    def from_legacy_dict(cls, data: Mapping) -> "ExchangeIdentity":
        """Read identity fields without modifying the legacy dictionary."""

        data = _require_mapping(data, "exchange")
        raw_categories = data.get("categories", ())
        if raw_categories is None:
            categories = ()
        elif isinstance(raw_categories, str):
            categories = (raw_categories,)
        elif isinstance(raw_categories, Sequence):
            categories = tuple(str(value) for value in raw_categories)
        else:
            categories = (str(raw_categories),)
        return cls(
            name=_string(data, "name"),
            reference_product=_reference_product(data),
            location=_string(data, "location"),
            unit=_string(data, "unit"),
            code=_string(data, "code"),
            categories=categories,
        )


_UNCERTAINTY_KEYS = {
    "uncertainty type",
    "loc",
    "scale",
    "shape",
    "minimum",
    "maximum",
}


@dataclass(frozen=True)
class Uncertainty:
    """Typed view of common Brightway uncertainty fields."""

    uncertainty_type: Any = None
    loc: Any = None
    scale: Any = None
    shape: Any = None
    minimum: Any = None
    maximum: Any = None

    @classmethod
    def from_legacy_dict(cls, data: Mapping) -> "Uncertainty":
        """Read common uncertainty values from an exchange dictionary."""

        data = _require_mapping(data, "exchange")
        return cls(
            uncertainty_type=deepcopy(data.get("uncertainty type")),
            loc=deepcopy(data.get("loc")),
            scale=deepcopy(data.get("scale")),
            shape=deepcopy(data.get("shape")),
            minimum=deepcopy(data.get("minimum")),
            maximum=deepcopy(data.get("maximum")),
        )


_PARAMETER_KEYS = {"name", "amount", "formula", "group"}


@dataclass(frozen=True)
class CanonicalParameter:
    """Canonical parameter with lossless source extensions."""

    name: str = ""
    amount: Any = None
    formula: str = ""
    group: str = ""
    extensions: ExtensionMap = field(default_factory=ExtensionMap)
    _legacy_payload: ExtensionMap = field(default_factory=ExtensionMap, repr=False)
    _from_legacy: bool = field(default=False, repr=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "name", str(self.name or ""))
        object.__setattr__(self, "amount", deepcopy(self.amount))
        object.__setattr__(self, "formula", str(self.formula or ""))
        object.__setattr__(self, "group", str(self.group or ""))
        object.__setattr__(self, "extensions", _extension_map(self.extensions))
        object.__setattr__(self, "_legacy_payload", _extension_map(self._legacy_payload))

    @classmethod
    def from_legacy_dict(cls, data: Mapping, source_namespace: str = "legacy") -> "CanonicalParameter":
        """Create a typed parameter while retaining its complete source mapping."""

        data = _require_mapping(data, "parameter")
        return cls(
            name=_string(data, "name"),
            amount=deepcopy(data.get("amount")),
            formula=_string(data, "formula"),
            group=_string(data, "group"),
            extensions=_unknown_extensions(data, _PARAMETER_KEYS, source_namespace),
            _legacy_payload=ExtensionMap(data),
            _from_legacy=True,
        )

    def to_legacy_dict(self, extension_namespace: Optional[str] = None) -> dict:
        """Return the exact source dictionary, or render a new typed parameter."""

        if self._from_legacy:
            return self._legacy_payload.to_dict()
        result = {"name": self.name}
        if self.amount is not None:
            result["amount"] = deepcopy(self.amount)
        if self.formula:
            result["formula"] = self.formula
        if self.group:
            result["group"] = self.group
        _merge_extension_namespace(result, self.extensions, extension_namespace)
        return result


_EXCHANGE_KEYS = {
    "name",
    "reference product",
    "product",
    "location",
    "unit",
    "code",
    "categories",
    "type",
    "amount",
    "formula",
    *_UNCERTAINTY_KEYS,
}


@dataclass(frozen=True)
class CanonicalExchange:
    """Typed exchange with an opaque, lossless legacy payload."""

    identity: ExchangeIdentity = field(default_factory=ExchangeIdentity)
    exchange_type: str = ""
    amount: Any = None
    formula: str = ""
    uncertainty: Uncertainty = field(default_factory=Uncertainty)
    extensions: ExtensionMap = field(default_factory=ExtensionMap)
    _legacy_payload: ExtensionMap = field(default_factory=ExtensionMap, repr=False)
    _from_legacy: bool = field(default=False, repr=False)

    def __post_init__(self) -> None:
        if not isinstance(self.identity, ExchangeIdentity):
            raise TypeError("identity must be an ExchangeIdentity.")
        if not isinstance(self.uncertainty, Uncertainty):
            raise TypeError("uncertainty must be an Uncertainty.")
        object.__setattr__(self, "exchange_type", str(self.exchange_type or ""))
        object.__setattr__(self, "amount", deepcopy(self.amount))
        object.__setattr__(self, "formula", str(self.formula or ""))
        object.__setattr__(self, "extensions", _extension_map(self.extensions))
        object.__setattr__(self, "_legacy_payload", _extension_map(self._legacy_payload))

    @classmethod
    def from_legacy_dict(cls, data: Mapping, source_namespace: str = "legacy") -> "CanonicalExchange":
        """Create a typed exchange without changing or filtering source data."""

        data = _require_mapping(data, "exchange")
        return cls(
            identity=ExchangeIdentity.from_legacy_dict(data),
            exchange_type=_string(data, "type"),
            amount=deepcopy(data.get("amount")),
            formula=_string(data, "formula"),
            uncertainty=Uncertainty.from_legacy_dict(data),
            extensions=_unknown_extensions(data, _EXCHANGE_KEYS, source_namespace),
            _legacy_payload=ExtensionMap(data),
            _from_legacy=True,
        )

    def to_legacy_dict(self, extension_namespace: Optional[str] = None) -> dict:
        """Return the exact source dictionary, or render a new typed exchange."""

        if self._from_legacy:
            return self._legacy_payload.to_dict()
        result = _exchange_identity_dict(self.identity)
        if self.exchange_type:
            result["type"] = self.exchange_type
        if self.amount is not None:
            result["amount"] = deepcopy(self.amount)
        if self.formula:
            result["formula"] = self.formula
        uncertainty_values = {
            "uncertainty type": self.uncertainty.uncertainty_type,
            "loc": self.uncertainty.loc,
            "scale": self.uncertainty.scale,
            "shape": self.uncertainty.shape,
            "minimum": self.uncertainty.minimum,
            "maximum": self.uncertainty.maximum,
        }
        result.update({key: deepcopy(value) for key, value in uncertainty_values.items() if value is not None})
        _merge_extension_namespace(result, self.extensions, extension_namespace)
        return result


_DATASET_KEYS = {
    "name",
    "reference product",
    "product",
    "location",
    "unit",
    "code",
    "exchanges",
    "parameters",
}


@dataclass(frozen=True)
class CanonicalDataset:
    """Typed dataset that preserves all unknown and software-specific fields."""

    identity: DatasetIdentity = field(default_factory=DatasetIdentity)
    exchanges: Tuple[CanonicalExchange, ...] = ()
    parameters: Tuple[CanonicalParameter, ...] = ()
    extensions: ExtensionMap = field(default_factory=ExtensionMap)
    _legacy_payload: ExtensionMap = field(default_factory=ExtensionMap, repr=False)
    _from_legacy: bool = field(default=False, repr=False)

    def __post_init__(self) -> None:
        if not isinstance(self.identity, DatasetIdentity):
            raise TypeError("identity must be a DatasetIdentity.")
        exchanges = tuple(self.exchanges)
        parameters = tuple(self.parameters)
        if any(not isinstance(exchange, CanonicalExchange) for exchange in exchanges):
            raise TypeError("exchanges must contain only CanonicalExchange values.")
        if any(not isinstance(parameter, CanonicalParameter) for parameter in parameters):
            raise TypeError("parameters must contain only CanonicalParameter values.")
        object.__setattr__(self, "exchanges", exchanges)
        object.__setattr__(self, "parameters", parameters)
        object.__setattr__(self, "extensions", _extension_map(self.extensions))
        object.__setattr__(self, "_legacy_payload", _extension_map(self._legacy_payload))

    @classmethod
    def from_legacy_dict(cls, data: Mapping, source_namespace: str = "legacy") -> "CanonicalDataset":
        """Create a typed dataset while retaining its complete source mapping."""

        data = _require_mapping(data, "dataset")
        exchange_data = _mapping_sequence(data.get("exchanges", ()), "dataset.exchanges")
        parameter_data = _mapping_sequence(data.get("parameters", ()), "dataset.parameters")
        return cls(
            identity=DatasetIdentity.from_legacy_dict(data),
            exchanges=tuple(
                CanonicalExchange.from_legacy_dict(exchange, source_namespace) for exchange in exchange_data
            ),
            parameters=tuple(
                CanonicalParameter.from_legacy_dict(parameter, source_namespace) for parameter in parameter_data
            ),
            extensions=_unknown_extensions(data, _DATASET_KEYS, source_namespace),
            _legacy_payload=ExtensionMap(data),
            _from_legacy=True,
        )

    def to_legacy_dict(self, extension_namespace: Optional[str] = None) -> dict:
        """Return the exact source dictionary, or render a new typed dataset."""

        if self._from_legacy:
            return self._legacy_payload.to_dict()
        result = _dataset_identity_dict(self.identity)
        result["exchanges"] = [exchange.to_legacy_dict(extension_namespace) for exchange in self.exchanges]
        if self.parameters:
            result["parameters"] = [parameter.to_legacy_dict(extension_namespace) for parameter in self.parameters]
        _merge_extension_namespace(result, self.extensions, extension_namespace)
        return result


@dataclass(frozen=True)
class CanonicalInventory:
    """Versioned software-neutral inventory with copy-on-read boundaries.

    :param context: Exact software format, technosphere, and biosphere context.
    :param datasets: Immutable sequence of canonical datasets.
    :param schema_version: Canonical schema version.  Legacy bridges create the
        current :data:`CANONICAL_SCHEMA_VERSION`.
    :param extensions: Explicit top-level extension namespaces.  Namespace
        values remain opaque to the core schema.
    """

    context: InventoryContext
    datasets: Tuple[CanonicalDataset, ...]
    schema_version: str = CANONICAL_SCHEMA_VERSION
    database_name: str = ""
    metadata: ExtensionMap = field(default_factory=ExtensionMap)
    database_parameters: Tuple[CanonicalParameter, ...] = ()
    project_parameters: Tuple[CanonicalParameter, ...] = ()
    extensions: ExtensionMap = field(default_factory=ExtensionMap)
    _database_parameters_present: bool = field(default=False, repr=False)
    _project_parameters_present: bool = field(default=False, repr=False)

    def __post_init__(self) -> None:
        if not isinstance(self.context, InventoryContext):
            raise TypeError("context must be an InventoryContext.")
        datasets = tuple(self.datasets)
        database_parameters = tuple(self.database_parameters)
        project_parameters = tuple(self.project_parameters)
        if any(not isinstance(dataset, CanonicalDataset) for dataset in datasets):
            raise TypeError("datasets must contain only CanonicalDataset values.")
        if any(not isinstance(parameter, CanonicalParameter) for parameter in database_parameters):
            raise TypeError("database_parameters must contain only CanonicalParameter values.")
        if any(not isinstance(parameter, CanonicalParameter) for parameter in project_parameters):
            raise TypeError("project_parameters must contain only CanonicalParameter values.")
        schema_version = str(self.schema_version or "").strip()
        if not schema_version:
            raise ValueError("schema_version must not be empty.")
        object.__setattr__(self, "datasets", datasets)
        object.__setattr__(self, "schema_version", schema_version)
        object.__setattr__(self, "database_name", str(self.database_name or ""))
        object.__setattr__(self, "metadata", _extension_map(self.metadata))
        object.__setattr__(self, "database_parameters", database_parameters)
        object.__setattr__(self, "project_parameters", project_parameters)
        object.__setattr__(self, "extensions", _extension_map(self.extensions))

    @classmethod
    def from_legacy_dicts(
        cls,
        data: Sequence,
        *,
        context: InventoryContext,
        database_name: str = "",
        metadata: Optional[Mapping] = None,
        database_parameters: Optional[Sequence] = None,
        project_parameters: Optional[Sequence] = None,
        extensions: Optional[Mapping] = None,
        source_namespace: str = "legacy",
    ) -> "CanonicalInventory":
        """Bridge legacy dictionary data into the canonical schema losslessly.

        All source mappings and nested values are copied.  Unknown dataset,
        exchange, and parameter fields are additionally exposed under the
        selected software extension namespace.
        """

        namespace = _validate_namespace(source_namespace)
        dataset_data = _mapping_sequence(data, "data")
        database_parameter_data = _mapping_sequence(database_parameters, "database_parameters")
        project_parameter_data = _mapping_sequence(project_parameters, "project_parameters")
        if metadata is not None:
            _require_mapping(metadata, "metadata")
        if extensions is not None:
            _require_mapping(extensions, "extensions")
        return cls(
            context=context,
            datasets=tuple(CanonicalDataset.from_legacy_dict(dataset, namespace) for dataset in dataset_data),
            database_name=database_name,
            metadata=ExtensionMap(metadata),
            database_parameters=tuple(
                CanonicalParameter.from_legacy_dict(parameter, namespace) for parameter in database_parameter_data
            ),
            project_parameters=tuple(
                CanonicalParameter.from_legacy_dict(parameter, namespace) for parameter in project_parameter_data
            ),
            extensions=ExtensionMap(extensions),
            _database_parameters_present=database_parameters is not None,
            _project_parameters_present=project_parameters is not None,
        )

    @classmethod
    def from_legacy(cls, data: Sequence, **kwargs: Any) -> "CanonicalInventory":
        """Alias for :meth:`from_legacy_dicts`."""

        return cls.from_legacy_dicts(data, **kwargs)

    def to_legacy_dicts(self) -> list:
        """Return fresh mutable copies of all legacy dataset dictionaries."""

        return [dataset.to_legacy_dict() for dataset in self.datasets]

    def to_legacy(self) -> list:
        """Alias for :meth:`to_legacy_dicts`."""

        return self.to_legacy_dicts()

    def to_legacy_components(self) -> dict:
        """Return all dictionary-backed components for transitional adapters."""

        return {
            "data": self.to_legacy_dicts(),
            "database_name": self.database_name,
            "metadata": self.metadata.to_dict(),
            "database_parameters": (
                [parameter.to_legacy_dict() for parameter in self.database_parameters]
                if self._database_parameters_present
                else None
            ),
            "project_parameters": (
                [parameter.to_legacy_dict() for parameter in self.project_parameters]
                if self._project_parameters_present
                else None
            ),
            "extensions": self.extensions.to_dict(),
        }


def _dataset_identity_dict(identity: DatasetIdentity) -> dict:
    result = {}
    if identity.name:
        result["name"] = identity.name
    if identity.reference_product:
        result["reference product"] = identity.reference_product
    if identity.location:
        result["location"] = identity.location
    if identity.unit:
        result["unit"] = identity.unit
    if identity.code:
        result["code"] = identity.code
    return result


def _exchange_identity_dict(identity: ExchangeIdentity) -> dict:
    result = _dataset_identity_dict(
        DatasetIdentity(
            name=identity.name,
            reference_product=identity.reference_product,
            location=identity.location,
            unit=identity.unit,
            code=identity.code,
        )
    )
    if identity.categories:
        result["categories"] = tuple(identity.categories)
    return result


def _merge_extension_namespace(result: dict, extensions: ExtensionMap, namespace: Optional[str]) -> None:
    if namespace is None:
        return
    normalized_namespace = _validate_namespace(namespace)
    extension_values = extensions.get(normalized_namespace, {})
    if not isinstance(extension_values, Mapping):
        raise TypeError("Extension namespace {!r} must contain a mapping.".format(normalized_namespace))
    overlap = set(result).intersection(extension_values)
    if overlap:
        raise ValueError("Extension namespace would overwrite canonical fields: {}.".format(sorted(overlap)))
    result.update(deepcopy(dict(extension_values)))
