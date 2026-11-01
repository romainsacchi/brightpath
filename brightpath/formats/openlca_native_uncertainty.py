"""Explicit openLCA 2.6.2 MC compatibility graph; generic JSON-LD is unchanged.

The native 2.6.2 lognormal sampler interprets its first parameter arithmetically,
despite the JSON-LD geomMean field. Encode an unbounded lognormal instead as
EXP(Normal(mu, sigma)). A Uniform gate has nominal 0 and support [1, 2], so an
IFF retains the deterministic amount/formula without changing the sampled law.
Helpers are genuine native parameters, never comment-based restoration data.
"""

from __future__ import annotations

import ast
import math
import re
from copy import deepcopy
from dataclasses import dataclass, replace
from hashlib import sha256

from brightpath.exceptions import SerializationError
from brightpath.models import InventoryDocument

COMPATIBILITY = "openlca-2.6.2"
_PREFIX = "bp_olca262_"
_UNCERTAINTY = frozenset({"uncertainty type", "loc", "scale", "shape", "minimum", "maximum", "min", "max", "negative"})
_NAME = r"bp_olca262_[a-f0-9]{12}_(?:gate|latent_\d+)(?:_\d+)?"
_GRAPH = re.compile(rf"IFF\((?P<gate>{_NAME})=0;\((?P<nominal>.*)\);(?P<sign>-?)EXP\((?P<latent>{_NAME})\)\)", re.S)
_TOKEN = re.compile(r"[A-Za-z_][A-Za-z_0-9]*")


class NativeUncertaintyError(SerializationError):
    """The explicitly requested native graph cannot preserve this contract."""


@dataclass(frozen=True)
class LognormalSpec:
    """Ephemeral render instructions; these are never serialized as provenance."""

    process_index: int
    process_name: str
    process_id: str | None
    exchange_index: int
    amount: float
    loc: float
    scale: float
    negative: bool
    original_formula: str | None
    gate: str
    latent: str
    amount_factor: float = 1.0


def _number(value, label):
    if isinstance(value, bool):
        raise NativeUncertaintyError(f"{label} must be a finite number, not Boolean.")
    try:
        result = float(value)
    except (TypeError, ValueError, OverflowError) as error:
        raise NativeUncertaintyError(f"{label} must be finite.") from error
    if not math.isfinite(result):
        raise NativeUncertaintyError(f"{label} must be finite.")
    return result


def _active(row):
    if row.get("uncertainty type") != 2:
        return False
    scale = _number(row.get("scale"), "Lognormal scale")
    if scale < 0:
        raise NativeUncertaintyError("Lognormal scale must be nonnegative.")
    return scale > 0


def _log_parameters(loc, scale):
    """Retain the generic codec's finite geometric-value guards, without rounding."""
    loc = _number(loc, "Lognormal loc")
    scale = _number(scale, "Lognormal scale")
    try:
        centre, spread = math.exp(loc), math.exp(scale)
    except OverflowError as error:
        raise NativeUncertaintyError("Lognormal geometric values overflow the existing JSON-LD contract.") from error
    if centre == 0 or not math.isfinite(centre) or not math.isfinite(spread):
        raise NativeUncertaintyError("Lognormal geometric values must remain finite and nonzero.")
    return loc, scale


def _arithmetic(formula, parameters):
    """Verify a small deterministic arithmetic contract without eval or sampling."""
    visiting = set()
    resolved = {}
    work_remaining = 4096
    walk_depth = 0

    def walk(node):
        nonlocal work_remaining, walk_depth
        work_remaining -= 1
        if work_remaining < 0 or walk_depth >= 128:
            raise NativeUncertaintyError("Nominal formula exceeds the total arithmetic work/depth contract.")
        walk_depth += 1
        try:
            return evaluate(node)
        finally:
            walk_depth -= 1

    def evaluate(node):
        if isinstance(node, ast.Expression):
            return walk(node.body)
        if isinstance(node, ast.Constant) and type(node.value) in (int, float):
            return _number(node.value, "Arithmetic constant")
        if isinstance(node, ast.Name):
            name = node.id.casefold()
            if name in resolved:
                return resolved[name]
            if name in visiting or name not in parameters:
                raise NativeUncertaintyError("Nominal formula has a missing or cyclic parameter.")
            parameter = parameters[name]
            if parameter.get("uncertainty type") not in (None, "", 0, 1):
                raise NativeUncertaintyError("Nominal formula depends on an uncertain parameter.")
            visiting.add(name)
            try:
                value = (
                    parse(parameter["formula"]) if parameter.get("formula") else _number(parameter.get("amount"), name)
                )
            finally:
                visiting.remove(name)
            resolved[name] = value
            return value
        if isinstance(node, ast.UnaryOp) and isinstance(node.op, (ast.UAdd, ast.USub)):
            return walk(node.operand) * (-1 if isinstance(node.op, ast.USub) else 1)
        if isinstance(node, ast.BinOp):
            left, right = walk(node.left), walk(node.right)
            if isinstance(node.op, ast.Add):
                return left + right
            if isinstance(node.op, ast.Sub):
                return left - right
            if isinstance(node.op, ast.Mult):
                return left * right
            if isinstance(node.op, ast.Div):
                return left / right
            if isinstance(node.op, ast.Pow):
                raise NativeUncertaintyError(
                    "Native openLCA 2.6.2 power syntax/precedence exceeds the verified nominal formula contract; "
                    "source formulas are not rewritten."
                )
        raise NativeUncertaintyError("Nominal formula exceeds the verified deterministic arithmetic contract.")

    def parse(text):
        text = str(text).strip()
        if len(text) > 4096:
            raise NativeUncertaintyError("Nominal formula exceeds the bounded arithmetic contract.")
        tree = ast.parse(text.replace("^", "**"), mode="eval")
        if sum(1 for _ in ast.walk(tree)) > 256:
            raise NativeUncertaintyError("Nominal formula exceeds the bounded arithmetic contract.")
        return walk(tree)

    try:
        return _number(parse(formula), "Nominal formula result")
    except NativeUncertaintyError:
        raise
    except (SyntaxError, RecursionError, ZeroDivisionError, OverflowError, TypeError, ValueError) as error:
        raise NativeUncertaintyError("Nominal formula cannot be verified safely.") from error


def _environment(global_scopes, dataset):
    result = {}
    global_names = [str(p.get("name", "")).casefold() for rows in global_scopes for p in rows]
    if len(global_names) != len(set(global_names)):
        raise NativeUncertaintyError("Database/project parameters collide in the native global scope.")
    for rows in (*global_scopes, dataset.get("parameters", [])):
        names = set()
        for row in rows:
            name = str(row.get("name", "")).casefold()
            if name in names:
                raise NativeUncertaintyError(f"Duplicate parameter name {name!r} in one scope.")
            names.add(name)
            result[name] = row
    return result


def _fresh(stem, occupied):
    name, index = stem, 0
    while name.casefold() in occupied:
        index += 1
        name = f"{stem}_{index}"
    occupied.add(name.casefold())
    return name


def prepare(document: InventoryDocument, *, compatibility: str):
    """Return a copied source without selected uncertainty fields plus render specs.

    Validate the original source through the normal pipeline first. Canonical
    helpers are deliberately not introduced here. Active lognormal parameters
    are refused: the audited 500 contexts contain none, and this exchange-only
    proposal does not claim a parameter dependency-graph transformation.
    """
    if compatibility != COMPATIBILITY:
        raise NativeUncertaintyError(f"Native uncertainty compatibility must be explicitly {COMPATIBILITY!r}.")
    data, specs = document.data, []
    from .openlca_references import load_openlca_reference_catalog

    reference_catalog = load_openlca_reference_catalog(document.context)
    global_scopes = (document.database_parameters or [], document.project_parameters or [])
    all_parameters = [*global_scopes[0], *global_scopes[1]]
    all_parameters.extend(p for dataset in data for p in dataset.get("parameters", []))
    if any(_active(p) for p in all_parameters):
        raise NativeUncertaintyError(
            "Active lognormal input parameters require a separately verified dependency contract."
        )
    occupied_global = {str(p.get("name", "")).casefold() for p in all_parameters}
    for pi, dataset in enumerate(data):
        occupied = occupied_global | {
            t.casefold() for e in dataset.get("exchanges", []) for t in _TOKEN.findall(str(e.get("formula") or ""))
        }
        token = sha256(f"{pi}:{dataset.get('code')}:{dataset.get('name')}:{sorted(occupied)}".encode()).hexdigest()[:12]
        gate = _fresh(f"{_PREFIX}{token}_gate", occupied)
        parameters = _environment(global_scopes, dataset)
        for ei, exchange in enumerate(dataset.get("exchanges", [])):
            if not _active(exchange):
                continue
            label = f"Process {dataset.get('code') or dataset.get('name')!r}, exchange {ei}"
            try:
                if any(exchange.get(k) not in (None, "") for k in ("minimum", "maximum", "min", "max")):
                    raise NativeUncertaintyError(
                        "Lognormal bound fields cannot be preserved by the unbounded native graph."
                    )
                if exchange.get("shape") not in (None, "", 0):
                    raise NativeUncertaintyError("An extra lognormal shape parameter is unsupported.")
                amount = _number(exchange.get("amount"), "Nominal amount")
                loc, scale = _log_parameters(exchange.get("loc"), exchange.get("scale"))
                negative = exchange.get("negative", amount < 0)
                if type(negative) not in (bool, int, float) or negative not in (0, 1):
                    raise NativeUncertaintyError("Lognormal negative must be Boolean or numeric 0/1 when explicit.")
                negative = bool(negative)
                formula = exchange.get("formula") or None
                if formula is not None and not math.isclose(
                    _arithmetic(formula, parameters), amount, rel_tol=1e-12, abs_tol=0
                ):
                    raise NativeUncertaintyError("Native nominal formula does not evaluate to the stored amount.")
                hint = (dataset.get("openlca process") or {}).get("@id") or dataset.get("code")
                specs.append(
                    LognormalSpec(
                        pi,
                        dataset.get("name", ""),
                        str(hint) if hint else None,
                        ei,
                        amount,
                        loc,
                        scale,
                        negative,
                        formula,
                        gate,
                        _fresh(f"{_PREFIX}{token}_latent_{ei}", occupied),
                        (
                            getattr(
                                reference_catalog.biosphere.get(
                                    (
                                        exchange.get("name"),
                                        tuple(exchange.get("categories") or ()),
                                        exchange.get("unit"),
                                    )
                                ),
                                "amount_factor",
                                1.0,
                            )
                            if reference_catalog is not None and exchange.get("type") == "biosphere"
                            else 1.0
                        ),
                    )
                )
                for key in _UNCERTAINTY:
                    exchange.pop(key, None)
            except NativeUncertaintyError as error:
                raise NativeUncertaintyError(f"{label}: {error}") from error
    return document.replace(data=data), tuple(specs)


def apply(rendered, specs):
    """Copy rendered schema entities and apply only genuine native MC graphs."""
    import olca_schema as schema

    result = deepcopy(rendered)
    processes = list(result.processes.values())
    added_gates = set()
    for spec in specs:
        if spec.process_index >= len(processes):
            raise NativeUncertaintyError("Render process order no longer matches the prepared source.")
        process = processes[spec.process_index]
        if process.name != spec.process_name or (spec.process_id and process.id != spec.process_id):
            raise NativeUncertaintyError("Render process identity no longer matches the prepared source.")
        if spec.exchange_index >= len(process.exchanges or []):
            raise NativeUncertaintyError("Render exchange order no longer matches the prepared source.")
        exchange = process.exchanges[spec.exchange_index]
        if spec.amount_factor != 1:
            from .openlca_quantity_projection import FACTOR, NATIVE_UUID, PROPERTY_UUID, UNIT_UUID, scale_detached

            if (
                spec.amount_factor != FACTOR
                or exchange.flow.id != NATIVE_UUID
                or exchange.flow_property.id != PROPERTY_UUID
                or exchange.unit.id != UNIT_UUID
            ):
                raise NativeUncertaintyError(
                    "Native quantity projection conflicts with the actual rendered references."
                )
            values = {
                "amount": spec.amount,
                "loc": spec.loc,
                "scale": spec.scale,
                "negative": spec.negative,
                "uncertainty type": 2,
            }
            if spec.original_formula is not None:
                values["formula"] = spec.original_formula
            values = scale_detached(values, spec.amount_factor)
            spec = replace(spec, amount=values["amount"], loc=values["loc"], original_formula=values.get("formula"))
        if (
            exchange.amount != spec.amount
            or (exchange.amount_formula or None) != spec.original_formula
            or exchange.uncertainty is not None
        ):
            raise NativeUncertaintyError("Render exchange no longer matches the stripped source.")
        names = {str(p.name).casefold() for p in process.parameters or []}
        if spec.latent.casefold() in names or (
            spec.gate.casefold() in names and (process.id, spec.gate) not in added_gates
        ):
            raise NativeUncertaintyError("A native helper name collides with a rendered parameter.")
        process.parameters = list(process.parameters or [])
        if (process.id, spec.gate) not in added_gates:
            process.parameters.append(
                schema.Parameter(
                    name=spec.gate,
                    value=0.0,
                    is_input_parameter=True,
                    parameter_scope=schema.ParameterScope.PROCESS_SCOPE,
                    uncertainty=schema.Uncertainty(
                        distribution_type=schema.UncertaintyType.UNIFORM_DISTRIBUTION, minimum=1.0, maximum=2.0
                    ),
                )
            )
            added_gates.add((process.id, spec.gate))
        process.parameters.append(
            schema.Parameter(
                name=spec.latent,
                value=spec.loc,
                is_input_parameter=True,
                parameter_scope=schema.ParameterScope.PROCESS_SCOPE,
                uncertainty=schema.Uncertainty(
                    distribution_type=schema.UncertaintyType.NORMAL_DISTRIBUTION, mean=spec.loc, sd=spec.scale
                ),
            )
        )
        nominal = f"({spec.original_formula})" if spec.original_formula is not None else repr(spec.amount)
        # JSON-LD carries openLCA expression syntax directly. CSV's importer
        # converts comma separators, whereas JSON-LD requires semicolons here.
        exchange.amount_formula = f"IFF({spec.gate}=0;({nominal});{'-' if spec.negative else ''}EXP({spec.latent}))"
    return result


def _helper(parameter, kind):
    raw = parameter.get("openlca parameter") or {}
    if parameter.get("formula") or parameter.get("uncertainty type") != kind:
        raise NativeUncertaintyError("Native helper distribution or formula was altered.")
    if not raw or raw.get("isInputParameter") is not True or raw.get("parameterScope") != "PROCESS_SCOPE":
        raise NativeUncertaintyError("Native helper must remain a process input parameter.")
    uncertainty = raw.get("uncertainty") or {}
    native_kind = "UNIFORM_DISTRIBUTION" if kind == 4 else "NORMAL_DISTRIBUTION"
    if (
        raw.get("formula")
        or uncertainty.get("distributionType") != native_kind
        or raw.get("value") != parameter.get("amount")
    ):
        raise NativeUncertaintyError("Native helper differs from its canonical projection.")
    if kind == 4:
        if parameter.get("amount") != 0 or parameter.get("minimum") != 1 or parameter.get("maximum") != 2:
            raise NativeUncertaintyError("Native gate nominal/support was altered.")
        if uncertainty.get("minimum") != 1 or uncertainty.get("maximum") != 2:
            raise NativeUncertaintyError("Native gate bounds differ from its projection.")
    elif parameter.get("minimum") is not None or parameter.get("maximum") is not None:
        raise NativeUncertaintyError("Native normal latent has unsupported bounds.")
    elif (
        uncertainty.get("mean") != parameter.get("loc")
        or uncertainty.get("sd") != parameter.get("scale")
        or uncertainty.get("minimum") is not None
        or uncertainty.get("maximum") is not None
    ):
        raise NativeUncertaintyError("Native normal uncertainty differs from its projection.")


def decode(document: InventoryDocument):
    """Collapse only a verified native graph; derive actual uncertainty from it.

    Unknown ordinary formulas are untouched. A claimed compatibility graph with
    an altered gate, shared latent, extra dependency or unsupported grammar is
    refused. Changing a valid latent's mean/SD changes decoded loc/scale; no
    original uncertainty is recovered from names, hashes, comments or metadata.
    """
    data = document.data
    global_scopes = (document.database_parameters or [], document.project_parameters or [])
    for dataset in data:
        rows = dataset.get("parameters", [])
        formulas = [str(e.get("formula") or "") for e in dataset.get("exchanges", [])]
        formulas.extend(str(p.get("formula") or "") for p in rows)
        if not any(re.search(_NAME, formula) for formula in formulas):
            continue
        parameters = {str(p.get("name", "")): p for p in rows}
        if len(parameters) != len(rows):
            raise NativeUncertaintyError("Duplicate native parameter names.")
        claims = [
            (ei, e, _GRAPH.fullmatch(str(e.get("formula") or ""))) for ei, e in enumerate(dataset.get("exchanges", []))
        ]
        relevant = [(i, e, m) for i, e, m in claims if m]
        if not relevant:
            if any(re.search(_NAME, f) for f in formulas):
                raise NativeUncertaintyError("Unrecognized native uncertainty graph; no reconstruction performed.")
            continue
        used, gates = set(), set()
        environment = _environment(global_scopes, dataset)
        for _ei, exchange, match in relevant:
            gate, latent = match["gate"], match["latent"]
            if not gate.endswith("_gate") and not re.search(r"_gate_\d+$", gate):
                raise NativeUncertaintyError("Malformed native gate name.")
            if "_latent_" not in latent or gate not in parameters or latent not in parameters:
                raise NativeUncertaintyError("Native graph has missing helpers.")
            if latent in used or sum(_TOKEN.findall(f).count(latent) for f in formulas) != 1:
                raise NativeUncertaintyError("Native latent is shared or has extra dependencies.")
            _helper(parameters[gate], 4)
            _helper(parameters[latent], 3)
            if exchange.get("uncertainty type") not in (None, "", 0, 1):
                raise NativeUncertaintyError("Native graph has a second exchange uncertainty.")
            raw_exchange = exchange.get("openlca exchange") or {}
            if (
                raw_exchange.get("amountFormula") != exchange.get("formula")
                or raw_exchange.get("amount") != exchange.get("amount")
                or raw_exchange.get("uncertainty")
            ):
                raise NativeUncertaintyError(
                    "Native exchange graph differs from its canonical projection/stored amount."
                )
            nominal = match["nominal"]
            original_formula = nominal[1:-1] if nominal.startswith("(") and nominal.endswith(")") else None
            amount = _number(exchange.get("amount"), "Native exchange amount")
            actual = _arithmetic(original_formula if original_formula is not None else nominal, environment)
            if not math.isclose(actual, amount, rel_tol=1e-12, abs_tol=0):
                raise NativeUncertaintyError("Native nominal formula disagrees with its stored amount.")
            normal = parameters[latent]
            loc, scale = _log_parameters(normal.get("loc"), normal.get("scale"))
            if scale <= 0 or normal.get("amount") != loc:
                raise NativeUncertaintyError("Native latent nominal/centre contract was altered.")
            for key in _UNCERTAINTY:
                exchange.pop(key, None)
            exchange.update({"uncertainty type": 2, "loc": loc, "scale": scale, "negative": bool(match["sign"])})
            if original_formula is None:
                exchange.pop("formula", None)
            else:
                exchange["formula"] = original_formula
            # These are transport templates, not an independent source of values.
            raw = exchange.get("openlca exchange")
            if isinstance(raw, dict):
                raw.pop("amountFormula", None)
                raw.pop("uncertainty", None)
                if original_formula is not None:
                    raw["amountFormula"] = original_formula
            used.add(latent)
            gates.add(gate)
        for gate in gates:
            if sum(_TOKEN.findall(f).count(gate) for f in formulas) != sum(m["gate"] == gate for _, _, m in relevant):
                raise NativeUncertaintyError("Native gate has an extra formula dependency.")
        helpers = used | gates
        remaining = [p for p in rows if p.get("name") not in helpers]
        if remaining:
            dataset["parameters"] = remaining
        else:
            dataset.pop("parameters", None)
        raw_process = dataset.get("openlca process")
        if isinstance(raw_process, dict):
            raw_process["parameters"] = [p for p in raw_process.get("parameters", []) if p.get("name") not in helpers]
    return document.replace(data=data)
