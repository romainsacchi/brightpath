from __future__ import annotations

import csv
import re
from functools import lru_cache

import yaml

from brightpath import DATA_DIR
from brightpath.catalogs import load_background_catalog
from brightpath.models import BackgroundProfile
from brightpath.units import normalize_unit

_LOCATION_CORRECTIONS = {
    "WECC, US only": "US-WECC",
    "ASCC, US only": "US-ASCC",
    "HICC, US only": "US-HICC",
    "MRO, US only": "US-MRO",
    "NPCC, US only": "US-NPCC",
    "PR, US only": "US-PR",
    "RFC, US only": "US-RFC",
    "SERC, US only": "US-SERC",
    "TRE, US only": "US-TRE",
    "HICC": "US-HICC",
    "SERC": "US-SERC",
    "RFC": "US-RFC",
    "ASCC": "US-ASCC",
    "TRE": "US-TRE",
    "FRCC": "US-FRCC",
    "SPP": "US-SPP",
    "Europe, without Russia and Turkey": "Europe, without Russia and T\u00fcrkiye",
}
_GENERIC_PROCESS_NAMES = {
    "production",
    "production mix",
    "processing",
    "market for",
    "market group for",
    "production, at grid",
    "at market",
    "cut-off, U",
    "consequential, U",
    "generic production",
    "market",
    "processing, mass based",
    "transport",
    "treatment of",
    "purification",
}


def format_simapro_technosphere_name(
    *,
    name: str,
    reference_product: str,
    location: str,
    unit: str,
    profile: BackgroundProfile,
) -> str:
    normalized = profile.normalized()
    if normalized.family == "ecoinvent":
        return _format_ecoinvent_name(
            name=name,
            reference_product=reference_product,
            location=location,
            system_model=normalized.system_model,
        )
    if normalized.family == "uvek":
        mapping, _ = _uvek_simapro_mappings()
        key = (name, location, _canonical_unit(unit), reference_product)
        return mapping.get(key, f"{name}/{location} U")
    raise ValueError(f"Unsupported background family for SimaPro names: {normalized.family!r}.")


def parse_simapro_technosphere_name(
    text: str,
    *,
    unit: str = "",
    profile: BackgroundProfile,
) -> tuple[str, str, str]:
    normalized = profile.normalized()
    if normalized.family == "ecoinvent":
        return _parse_ecoinvent_name(text)
    if normalized.family == "uvek":
        return _parse_uvek_name(text, unit=unit, profile=normalized)
    raise ValueError(f"Unsupported background family for SimaPro names: {normalized.family!r}.")


def _format_ecoinvent_name(
    *,
    name: str,
    reference_product: str,
    location: str,
    system_model: str,
) -> str:
    if not name or not reference_product:
        raise ValueError("Technosphere exchanges require non-empty names and reference products.")

    product = reference_product[0].upper() + reference_product[1:]
    process = name[0].upper() + name[1:]
    formatted = f"{product} {{{location}}}| {process}"

    for market_name in ("market for", "market group for"):
        if market_name not in name.lower():
            continue
        formatted = f"{product} {{{location}}}"
        lower_product = product[0].lower() + product[1:]
        if location == "GLO" and lower_product in _ecoinvent_market_exceptions():
            formatted += f"| {market_name}"
        else:
            formatted += f"| {market_name} {lower_product}"
        break

    suffixes = {
        "cutoff": "Cut-off, U",
        "consequential": "Consequential, U",
    }
    try:
        suffix = suffixes[system_model]
    except KeyError as exc:
        raise ValueError(f"Unsupported ecoinvent system model for SimaPro: {system_model!r}.") from exc
    return f"{formatted} | {suffix}"


def _parse_ecoinvent_name(text: str) -> tuple[str, str, str]:
    if not isinstance(text, str) or not text.strip():
        raise ValueError("Cannot parse an empty SimaPro technosphere exchange name.")

    parts = [part.strip() for part in text.split("|")]
    if len(parts) > 4:
        raise ValueError(f"Cannot parse SimaPro technosphere exchange name with too many fields: {text!r}.")

    reference_product, name = "", ""
    if len(parts) == 1:
        reference_product = name = parts[0]
    elif len(parts) >= 2:
        reference_product, name = parts[:2]

    _validate_braces(reference_product, text)
    _validate_braces(name, text)
    reference_product = _lower_initial(reference_product)

    if "{" in reference_product:
        reference_product, location_block = reference_product.split("{", 1)
        location, remainder = location_block.split("}", 1)
        if not location.strip():
            raise ValueError(f"Cannot parse SimaPro exchange name with empty location: {text!r}.")
        if remainder.strip():
            reference_product = f"{reference_product} {remainder.strip()}".replace("  ", " ")
    else:
        location = "GLO"

    if location in {"French Guiana", "French Guinana"}:
        location = "FG"

    for suffix in ("Cut-off, U", "cut-off, U", "Consequential, U", "consequential, U"):
        name = name.replace(suffix, "")
    if name[-3:] in {", U", ", S"}:
        name = name[:-3]
    if "{" in name:
        name = name.split("{", 1)[0]
    name = _lower_initial(name.strip()) or reference_product
    reference_product = reference_product.strip().rstrip(",.")

    if name in _GENERIC_PROCESS_NAMES:
        if name in {"market for", "market group for", "treatment of"}:
            name = f"{name} {reference_product}"
        elif name == "market":
            name = f"market for {reference_product}"
        elif name == "production":
            if ", " in reference_product:
                first, *remainder = reference_product.split(", ")
                name = f"{first} production, {', '.join(remainder)}"
            else:
                name = f"{reference_product} production"
        elif name == "processing":
            name = reference_product
        else:
            name = f"{reference_product} {name}"

    if name.startswith("treatment of,"):
        name = f"treatment of {reference_product}, {name.split(', ')[-1]}"
    if name == "construction":
        if ", " in reference_product:
            first, *remainder = reference_product.split(", ")
            name = f"{first} construction, {', '.join(remainder)}"
        else:
            name = f"{reference_product} construction"
    if name.startswith("production, from") or name.startswith("production from"):
        name = f"{reference_product} {name}"

    location = _LOCATION_CORRECTIONS.get(location.replace("}", "").strip(), location)
    if not name.strip() or not reference_product.strip():
        raise ValueError(f"Could not parse non-empty name and reference product from {text!r}.")
    return name, reference_product, location


def _parse_uvek_name(
    text: str,
    *,
    unit: str,
    profile: BackgroundProfile,
) -> tuple[str, str, str]:
    if not isinstance(text, str) or not text.strip():
        raise ValueError("Cannot parse an empty SimaPro technosphere exchange name.")

    _, reverse = _uvek_simapro_mappings()
    mapped = reverse.get(text.strip(), ())
    if len(mapped) == 1:
        name, location, _mapped_unit, reference_product = mapped[0]
        return name, reference_product, location
    if len(mapped) > 1:
        unit_matches = [row for row in mapped if _units_equal(row[2], unit)]
        if len(unit_matches) == 1:
            name, location, _mapped_unit, reference_product = unit_matches[0]
            return name, reference_product, location
        unique = {(row[0], row[3], row[1]) for row in (unit_matches or mapped)}
        if len(unique) == 1:
            return next(iter(unique))
        raise ValueError(f"Ambiguous UVEK SimaPro name: {text!r}.")

    match = re.fullmatch(r"(?P<name>.+)/(?P<location>[^/]+) U", text.strip())
    if not match:
        raise ValueError(f"Cannot parse UVEK SimaPro technosphere name: {text!r}.")
    name = match.group("name").strip()
    location = match.group("location").strip()

    try:
        catalog = load_background_catalog(profile)
    except FileNotFoundError:
        return name, name, location
    candidates = {
        reference_product
        for candidate_name, reference_product, candidate_location, candidate_unit in catalog.technosphere
        if candidate_name == name
        and candidate_location == location
        and (not unit or _units_equal(candidate_unit, unit))
    }
    if len(candidates) == 1:
        return name, next(iter(candidates)), location
    if len(candidates) > 1:
        raise ValueError(f"Ambiguous UVEK reference product for SimaPro name: {text!r}.")
    return name, name, location


@lru_cache(maxsize=1)
def _uvek_simapro_mappings():
    path = DATA_DIR / "export" / "ecoinvent_to_uvek_mapping.csv"
    forward: dict[tuple[str, str, str, str], str] = {}
    reverse: dict[str, set[tuple[str, str, str, str]]] = {}
    with path.open(encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            key = (
                row["UVEK name"],
                row["UVEK location"],
                _canonical_unit(row["UVEK unit"]),
                row["UVEK ref prod"],
            )
            simapro_name = row["UVEK simapro name"]
            forward[key] = simapro_name
            reverse.setdefault(simapro_name, set()).add(key)
    return forward, {name: tuple(sorted(values)) for name, values in reverse.items()}


@lru_cache(maxsize=1)
def _ecoinvent_market_exceptions() -> frozenset[str]:
    path = DATA_DIR / "export" / "simapro_ei_exceptions.yaml"
    with path.open(encoding="utf-8") as handle:
        payload = yaml.safe_load(handle) or {}
    return frozenset(str(value).strip().lower() for value in payload.get("market", ()))


def _validate_braces(value: str, original: str) -> None:
    if value.count("{") != value.count("}"):
        raise ValueError(f"Cannot parse malformed SimaPro location braces in {original!r}.")
    if value.count("{") > 1 or value.count("}") > 1:
        raise ValueError(f"Cannot parse multiple SimaPro location blocks in {original!r}.")


def _canonical_unit(value: str) -> str:
    try:
        return str(normalize_unit(value))
    except (KeyError, TypeError, ValueError):
        return str(value or "").strip()


def _units_equal(left: str, right: str) -> bool:
    return _canonical_unit(left) == _canonical_unit(right)


def _lower_initial(value: str) -> str:
    if re.match(r"^[A-Z]+(\s|$|[-,])", value):
        return value
    return value[0].lower() + value[1:] if value else value
