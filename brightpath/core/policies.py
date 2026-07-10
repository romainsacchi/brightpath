"""Explicit policies for lossy or ambiguous BrightPath operations."""

from __future__ import annotations

import json
from dataclasses import dataclass, fields
from enum import Enum
from typing import Any, Dict, Mapping, Type, TypeVar


class PolicyAction(str, Enum):
    """Action to take when a policy-controlled condition is encountered."""

    ERROR = "error"
    WARN = "warn"
    ALLOW = "allow"


P = TypeVar("P", bound="_SerializablePolicy")


class _SerializablePolicy:
    """Shared deterministic dictionary conversion for policy value objects."""

    def to_dict(self) -> Dict[str, Any]:
        """Return a JSON-compatible policy snapshot."""

        snapshot = {}
        for item in fields(self):
            value = getattr(self, item.name)
            snapshot[item.name] = value.value if isinstance(value, Enum) else value
        return snapshot

    def to_json(self) -> str:
        """Serialize this policy snapshot to deterministic JSON text."""

        return json.dumps(self.to_dict(), ensure_ascii=False, separators=(",", ":"), sort_keys=True)

    @classmethod
    def from_dict(cls: Type[P], data: Mapping[str, Any]) -> P:
        """Construct a policy from :meth:`to_dict` output."""

        names = {item.name for item in fields(cls)}
        unknown = sorted(set(data) - names)
        if unknown:
            raise ValueError(f"Unknown {cls.__name__} fields: {', '.join(unknown)}.")
        values = {
            item.name: PolicyAction(data[item.name]) if item.name.startswith("on_") else data[item.name]
            for item in fields(cls)
            if item.name in data
        }
        return cls(**values)

    @classmethod
    def from_json(cls: Type[P], payload: str) -> P:
        """Deserialize a policy produced by :meth:`to_json`."""

        data = json.loads(payload)
        if not isinstance(data, dict):
            raise TypeError(f"A {cls.__name__} JSON document must contain an object.")
        return cls.from_dict(data)


def _coerce_actions(policy: Any) -> None:
    for item in fields(policy):
        value = getattr(policy, item.name)
        if item.name.startswith("on_"):
            try:
                object.__setattr__(policy, item.name, PolicyAction(value))
            except (TypeError, ValueError) as error:
                allowed = ", ".join(action.value for action in PolicyAction)
                raise ValueError(f"{item.name} must be one of: {allowed}.") from error


def _validate_flags(policy: Any) -> None:
    for item in fields(policy):
        if item.name.startswith("validate_") and not isinstance(getattr(policy, item.name), bool):
            raise TypeError(f"{item.name} must be a boolean.")


@dataclass(frozen=True)
class ConversionPolicy(_SerializablePolicy):
    """Decisions applied while preflighting and converting file formats."""

    on_unsupported_feature: PolicyAction = PolicyAction.ERROR
    on_information_loss: PolicyAction = PolicyAction.ERROR
    on_ambiguous_mapping: PolicyAction = PolicyAction.ERROR
    on_invalid_target: PolicyAction = PolicyAction.ERROR
    validate_target: bool = True

    def __post_init__(self) -> None:
        _coerce_actions(self)
        _validate_flags(self)

    @classmethod
    def strict(cls) -> "ConversionPolicy":
        """Return a policy that reports every unsafe condition as an error."""

        return cls()

    @classmethod
    def permissive(cls) -> "ConversionPolicy":
        """Return a policy that warns, but does not fail, on unsafe conditions."""

        return cls(
            on_unsupported_feature=PolicyAction.WARN,
            on_information_loss=PolicyAction.WARN,
            on_ambiguous_mapping=PolicyAction.WARN,
            on_invalid_target=PolicyAction.WARN,
        )


@dataclass(frozen=True)
class MigrationPolicy(_SerializablePolicy):
    """Decisions applied while validating and migrating background links."""

    on_invalid_source: PolicyAction = PolicyAction.ERROR
    on_unresolved_link: PolicyAction = PolicyAction.ERROR
    on_ambiguous_rule: PolicyAction = PolicyAction.ERROR
    on_information_loss: PolicyAction = PolicyAction.ERROR
    on_deletion: PolicyAction = PolicyAction.ERROR
    on_inferred_reverse: PolicyAction = PolicyAction.ERROR
    on_unit_change_without_factor: PolicyAction = PolicyAction.ERROR
    on_invalid_target: PolicyAction = PolicyAction.ERROR
    validate_source: bool = True
    validate_target: bool = True
    minimum_coverage: float = 1.0

    def __post_init__(self) -> None:
        _coerce_actions(self)
        _validate_flags(self)
        if isinstance(self.minimum_coverage, bool) or not isinstance(self.minimum_coverage, (int, float)):
            raise TypeError("minimum_coverage must be a number between 0 and 1.")
        if not 0 <= self.minimum_coverage <= 1:
            raise ValueError("minimum_coverage must be between 0 and 1.")
        object.__setattr__(self, "minimum_coverage", float(self.minimum_coverage))

    @classmethod
    def strict(cls) -> "MigrationPolicy":
        """Return a policy requiring complete, unambiguous, lossless migration."""

        return cls()

    @classmethod
    def permissive(cls) -> "MigrationPolicy":
        """Return a warning policy that accepts incomplete or lossy migration."""

        return cls(
            on_invalid_source=PolicyAction.WARN,
            on_unresolved_link=PolicyAction.WARN,
            on_ambiguous_rule=PolicyAction.WARN,
            on_information_loss=PolicyAction.WARN,
            on_deletion=PolicyAction.WARN,
            on_inferred_reverse=PolicyAction.WARN,
            on_unit_change_without_factor=PolicyAction.WARN,
            on_invalid_target=PolicyAction.WARN,
            minimum_coverage=0.0,
        )
