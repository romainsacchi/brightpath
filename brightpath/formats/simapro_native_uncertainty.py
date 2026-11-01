"""Opt-in SimaPro CSV uncertainty graphs; ordinary writer defaults are unchanged.

Independent nominals and lognormal centres use process input parameters and
``iff(abs(gate)>0, signed_exp_latent, nominal)``. The gate is Normal(0, 1),
with nominal zero; independent Normal(mu, sigma) latents carry the log law.
Positive triangular inputs use native Triangle parameters with their actual mode
and bounds. The continuous sampled law is exact almost surely. A finite RNG zero
is a disclosed residual; no equality-to-zero tolerance is introduced.

Literal CSV import/LCI/MC is verified in openLCA 2.6.2, including Triangle input
edges. This is not a claim of proprietary SimaPro desktop execution. Readers must
call :func:`decode` after ordinary sign, unit and identifier normalization.
Original values are reconstructed from actual native graphs, never comments.
"""

from __future__ import annotations

import ast
import math
import re
from copy import deepcopy
from dataclasses import dataclass

from brightpath.exceptions import SimaProSerializationError
from brightpath.models import InventoryDocument


class NativeUncertaintyError(SimaProSerializationError):
    """The requested native uncertainty graph cannot preserve its contract."""


# Preserve the reviewed prototype's exception name for explicit integrations.
GateError = NativeUncertaintyError


_UNCERTAINTY = {"uncertainty type", "loc", "scale", "negative"}
_HELPER = re.compile(r"bpsp[gl]\d+", re.I)
_NAME = re.compile(r"[A-Za-z_][A-Za-z_0-9]*")


def _number(value):
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise GateError("Expected a finite numeric value")
    try:
        result = float(value)
    except (ValueError, OverflowError) as error:
        raise GateError("Expected a finite numeric value") from error
    if not math.isfinite(result):
        raise GateError("Expected a finite numeric value")
    return result


def _names(expression):
    return {s.casefold() for s in _NAME.findall(str(expression))}


def _source_number(row, key, path):
    try:
        return _number(row[key])
    except KeyError as error:
        raise GateError(f"{path}: missing uncertainty field {key!r}") from error
    except NativeUncertaintyError as error:
        raise GateError(f"{path}.{key}: {error}") from error


def _finite_exp(loc):
    """Reject a native EXP centre that would already overflow or become zero."""
    try:
        centre = math.exp(loc)
    except OverflowError as error:
        raise GateError("Native EXP centre overflows") from error
    if centre == 0 or not math.isfinite(centre):
        raise GateError("Native EXP centre must remain finite and nonzero")


@dataclass(frozen=True)
class NativeUncertaintySpec:
    """Ephemeral process/exchange indexing; never serialized as provenance."""

    process_index: int
    exchange_index: int
    gate: str
    latent: str


def prepare(document: InventoryDocument) -> tuple[InventoryDocument, tuple[NativeUncertaintySpec, ...]]:
    """Validate the original document and return a detached native graph copy.

    Only formula-free independent-centre input lognormals and positive,
    nondegenerate input triangles are transformed. No mismatch tolerance is
    applied. Source formulas on selected uncertain exchanges, bound fields on
    lognormals and incompatible signs are refused, never recentered or clamped.
    Validation does not query a background catalog or a database.
    """
    from brightpath.validation.brightway import validate_brightway_inventory

    if not isinstance(document, InventoryDocument):
        raise TypeError("prepare requires an InventoryDocument")
    validation = validate_brightway_inventory(document, check_background_links=False)
    if validation.has_errors:
        raise GateError(
            "Original source validation failed: "
            + "; ".join(i.message for i in validation.issues if i.severity == "error")
        )
    data = document.data
    reserved = set()
    for p in [*(document.database_parameters or []), *(document.project_parameters or [])]:
        reserved.add(str(p.get("name", "")).casefold())
        reserved.update(_names(p.get("formula", "")))
    for a in data:
        for p in a.get("parameters") or []:
            reserved.add(str(p.get("name", "")).casefold())
            reserved.update(_names(p.get("formula", "")))
        for e in a.get("exchanges", []):
            reserved.update(_names(e.get("formula", "")))
    counter = 0

    def fresh(kind):
        nonlocal counter
        while True:
            value = f"bpsp{kind}{counter}"
            counter += 1
            if value.casefold() not in reserved:
                reserved.add(value.casefold())
                return value

    specs = []
    existing_graphs = False
    for pi, a in enumerate(data):
        gate = None
        for ei, e in enumerate(a.get("exchanges", [])):
            kind = e.get("uncertainty type")
            if e.get("type") not in {"biosphere", "technosphere"} or kind not in {2, 5}:
                if _claims_graph(e.get("formula", "")):
                    old_gate, latent, *_ = _graph(e["formula"])
                    specs.append(NativeUncertaintySpec(pi, ei, old_gate, latent))
                    existing_graphs = True
                continue
            path = f"activity[{pi}].exchanges[{ei}]"
            amount, mu = _source_number(e, "amount", path), _source_number(e, "loc", path)
            if kind == 2:
                sigma = _source_number(e, "scale", path)
                if sigma < 0:
                    raise GateError("Negative log standard deviation")
                negative = e.get("negative", amount < 0)
                if negative not in (False, True, 0, 1) or bool(negative) != (amount < 0):
                    raise GateError(f"activity[{pi}].exchanges[{ei}]: source nominal/sign mismatch")
                if sigma > 0 and amount == 0:
                    raise GateError(f"activity[{pi}].exchanges[{ei}]: zero-nominal lognormal is outside this graph")
                # No centre mismatch, including ~1e-10, is waived by a tolerance.
                if sigma == 0 or math.log(abs(amount)) == mu:
                    continue
                if any(k in e for k in ("minimum", "maximum", "min", "max", "shape")):
                    raise GateError("Bounded or shaped distribution is outside this compatibility graph")
                _finite_exp(mu)
            else:
                if amount == mu:
                    continue  # Preserve the ordinary codec's existing centered cases.
                lo, hi = _source_number(e, "minimum", path), _source_number(e, "maximum", path)
                if not (0 <= lo <= mu <= hi and lo < hi and amount > 0):
                    raise GateError("This graph only supports nonnegative nondegenerate triangles")
                if any(k in e for k in ("scale", "shape", "min", "max")):
                    raise GateError("Extra triangular uncertainty fields are outside this compatibility graph")
            if e.get("formula"):
                raise GateError("Existing source formula is outside this compatibility graph")
            negative = e.get("negative", amount < 0)
            if negative not in (False, True, 0, 1) or bool(negative) != (amount < 0):
                raise GateError("Source nominal/sign mismatch")
            if kind == 2 and not math.isfinite(2 * sigma):
                raise GateError("Native doubled standard deviation is not finite")
            if gate is None:
                gate = fresh("g")
                a["parameters"] = list(a.get("parameters") or [])
                a["parameters"].append(
                    {"name": gate, "amount": 0.0, "uncertainty type": 3, "loc": 0.0, "scale": 1.0, "hidden": True}
                )
            latent = fresh("l")
            parameter = {
                "name": latent,
                "amount": mu,
                "uncertainty type": 3 if kind == 2 else 5,
                "loc": mu,
                "hidden": True,
            }
            parameter.update({"scale": sigma} if kind == 2 else {"minimum": lo, "maximum": hi})
            a["parameters"].append(parameter)
            for key in _UNCERTAINTY | {"minimum", "maximum"}:
                e.pop(key, None)
            sampled = f"{'-' if negative else ''}exp({latent})" if kind == 2 else latent
            e["formula"] = f"iff(abs({gate})>0,{sampled},{amount:.17g})"
            specs.append(NativeUncertaintySpec(pi, ei, gate, latent))
    if existing_graphs:
        decode(data)
    return document.replace(data=data), tuple(specs)


def render(prepared: InventoryDocument, specs: tuple[NativeUncertaintySpec, ...], *, category_mode="preserve"):
    """Render the prepared document with the ordinary renderer and exact helpers.

    Returns native rows suitable for the existing Latin-1 CSV writer. The
    existing ``_Formula`` cell markers, issues/guards and category behavior are
    retained. Only helper parameter numeric fields/comments are replaced.
    """
    from brightpath.formats.simapro_csv import render_simapro_rows

    decode(prepared.data)  # Validate actual graph values before emitting them.
    expected = []
    data = prepared.data
    for pi, activity in enumerate(data):
        for ei, exchange in enumerate(activity.get("exchanges", [])):
            if _claims_graph(exchange.get("formula", "")):
                gate, latent, *_ = _graph(exchange["formula"])
                expected.append(NativeUncertaintySpec(pi, ei, gate, latent))
    if len(set(specs)) != len(specs) or set(specs) != set(expected):
        raise GateError("Render specs differ from the prepared native graphs")
    result = render_simapro_rows(prepared, category_mode=category_mode)
    if result.has_errors:
        raise GateError("; ".join(i.message for i in result.issues if i.severity == "error"))
    rows = deepcopy(result.rows)
    helper_keys = {(s.process_index, name) for s in specs for name in (s.gate, s.latent)}
    parameters = {
        (pi, p["name"]): p
        for pi, activity in enumerate(data)
        for p in activity.get("parameters") or []
        if (pi, p["name"]) in helper_keys
    }
    if parameters.keys() != helper_keys:
        raise GateError("Render specs differ from the prepared native parameters")
    process_index = -1
    section = None
    for index, row in enumerate(rows):
        if row == ["Process"] and (index == 0 or not rows[index - 1]):
            process_index += 1
        if not row:
            section = None
        elif len(row) == 1:
            section = row[0]
        elif section == "Input parameters" and (process_index, row[0]) in parameters and len(row) == 8:
            p = parameters[(process_index, row[0])]
            if p["uncertainty type"] == 3:
                row[1:6] = [f"{p['amount']:.17g}", "Normal", f"{2 * p['scale']:.17g}", "0", "0"]
            else:
                row[1:6] = [f"{p['amount']:.17g}", "Triangle", "0", f"{p['minimum']:.17g}", f"{p['maximum']:.17g}"]
            row[7] = ""  # No original-field metadata is needed or trusted.
    return rows


def _constant(node):
    sign = 1
    for _ in range(32):
        if not (isinstance(node, ast.UnaryOp) and isinstance(node.op, (ast.USub, ast.UAdd))):
            break
        sign *= -1 if isinstance(node.op, ast.USub) else 1
        node = node.operand
    else:
        raise GateError("Native literal depth limit")
    if isinstance(node, ast.Constant):
        return sign * _number(node.value)
    raise GateError("Expected a literal native nominal or unit factor")


def _graph(expression):
    if len(expression) > 4096:
        raise GateError("Native graph expression too long")
    try:
        node = ast.parse(expression.replace(";", ","), mode="eval").body
    except (SyntaxError, RecursionError) as error:
        raise GateError("Invalid native graph") from error
    factor = 1.0
    # Normalize only literal signed multiplication/division and unary signs;
    # this is bounded structural interpretation, never eval or general simplify.
    for _ in range(32):
        if isinstance(node, ast.UnaryOp) and isinstance(node.op, (ast.USub, ast.UAdd)):
            factor *= -1 if isinstance(node.op, ast.USub) else 1
            node = node.operand
        elif isinstance(node, ast.BinOp) and isinstance(node.op, (ast.Mult, ast.Div)):
            division = isinstance(node.op, ast.Div)
            try:
                value = _constant(node.right)
            except GateError as error:
                if division:
                    raise GateError("A graph cannot be used as a native denominator") from error
                value = _constant(node.left)
                node = node.right
            else:
                node = node.left
            if value == 0:
                raise GateError("Zero native multiplier/divisor")
            factor = factor / value if division else factor * value
        else:
            break
    else:
        raise GateError("Native sign/unit wrapper depth limit")
    if factor == 0 or not math.isfinite(factor):
        raise GateError("Invalid finite nonzero native multiplier")
    if isinstance(node, ast.IfExp):
        condition, sample, nominal_node = node.test, node.body, node.orelse
    elif (
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id.casefold() == "iff"
        and len(node.args) == 3
        and not node.keywords
    ):
        condition, sample, nominal_node = node.args
    else:
        raise GateError("Expected a complete direct-IFF graph")
    if (
        isinstance(condition, ast.Compare)
        and len(condition.ops) == 1
        and isinstance(condition.ops[0], ast.Gt)
        and len(condition.comparators) == 1
        and _constant(condition.comparators[0]) == 0
        and isinstance(condition.left, ast.Call)
        and isinstance(condition.left.func, ast.Name)
        and condition.left.func.id.casefold() == "abs"
        and len(condition.left.args) == 1
        and not condition.left.keywords
        and isinstance(condition.left.args[0], ast.Name)
    ):
        gate = condition.left.args[0].id
    else:
        raise GateError("Unsupported native conditional dialect")
    if isinstance(sample, ast.Name):
        return gate, sample.id, False, _constant(nominal_node), factor, 5
    negative = isinstance(sample, ast.UnaryOp) and isinstance(sample.op, ast.USub)
    if negative:
        sample = sample.operand
    if not (
        isinstance(sample, ast.Call)
        and isinstance(sample.func, ast.Name)
        and sample.func.id.casefold() == "exp"
        and len(sample.args) == 1
        and not sample.keywords
        and isinstance(sample.args[0], ast.Name)
    ):
        raise GateError("Expected a signed EXP of one independent latent")
    return gate, sample.args[0].id, negative, _constant(nominal_node), factor, 2


def decode(data: list[dict] | InventoryDocument):
    """Copy and reconstruct canonical uncertainty only from genuine native graphs.

    Input retains the actual native parameters and amount formulas after the
    ordinary SimaPro reader's sign, unit and identifier normalization.
    Units are the normalized input units. A native outer factor is applied
    exactly once to the nominal and added as ln(abs(factor)) to the log centre;
    a negative factor also reflects the sampling sign or triangular bounds.
    Ordinary source expressions, even with helper-like names, are untouched.
    Unknown graph shapes remain intact; no scientific fields are fabricated.
    Recognized graphs with invalid bindings or leaked/reused helpers are refused.

    Accepts a normalized list or an InventoryDocument and returns the same kind.
    """
    document = data if isinstance(data, InventoryDocument) else None
    result = document.data if document is not None else deepcopy(data)
    if not isinstance(result, list):
        raise TypeError("decode requires normalized inventory data or an InventoryDocument")
    for a in result:
        parameters = a.get("parameters") or []
        claims = [e for e in a.get("exchanges", []) if _claims_graph(e.get("formula", ""))]
        if not claims:
            continue
        index = {}
        for p in parameters:
            key = p["name"].casefold()
            if key in index:
                raise GateError("Duplicate native parameter name")
            index[key] = p
        used = set()
        latents = set()
        for e in claims:
            if e.get("type") not in {"biosphere", "technosphere"} or e.get("uncertainty type", 0) not in {0, 1}:
                raise GateError("Unexpected directly uncertain or production graph")
            gate, latent, negative, nominal, factor, kind = _graph(e["formula"])
            g, p = index.get(gate.casefold()), index.get(latent.casefold())
            if (
                g is None
                or p is None
                or not _HELPER.fullmatch(gate)
                or not _HELPER.fullmatch(latent)
                or not gate.casefold().startswith("bpspg")
                or not latent.casefold().startswith("bpspl")
            ):
                raise GateError("Missing native helper parameter")
            for parameter in (g, p):
                expected = 3 if parameter is g or kind == 2 else 5
                if parameter.get("uncertainty type") != expected or parameter.get("formula"):
                    raise GateError("Expected actual native input parameter distribution")
                if expected == 3 and any(k in parameter for k in ("minimum", "maximum", "min", "max", "shape")):
                    raise GateError("Unexpected helper bounds")
                _number(parameter["amount"])
                if "loc" in parameter and parameter["loc"] != parameter["amount"]:
                    raise GateError("Native helper default and distribution centre disagree")
                if expected == 3:
                    _number(parameter["scale"])
            if g["amount"] != 0 or g["scale"] != 1:
                raise GateError("Changed native Normal gate")
            if (kind == 2 and p["scale"] <= 0) or nominal == 0 or negative != (nominal < 0):
                raise GateError("Invalid latent spread or nominal sign")
            if kind == 5 and not (
                0 <= _number(p["minimum"]) <= p["amount"] <= _number(p["maximum"]) and p["minimum"] < p["maximum"]
            ):
                raise GateError("Invalid native triangular mode or bounds")
            if latent.casefold() in latents:
                raise GateError("Reused latent would correlate independent exchanges")
            # Exclusivity of latent use is part of the graph, not a naming assertion.
            references = [v for v in a.get("exchanges", []) if latent.casefold() in _names(v.get("formula", ""))]
            references += [v for v in parameters if latent.casefold() in _names(v.get("formula", ""))]
            if len(references) != 1:
                raise GateError("Native latent has another consumer")
            e.pop("formula")
            e.update({"amount": nominal * factor, "uncertainty type": kind})
            if kind == 2:
                _finite_exp(p["amount"])
                _finite_exp(p["amount"] + math.log(abs(factor)))
                e.update(
                    {
                        "loc": p["amount"] + math.log(abs(factor)),
                        "scale": p["scale"],
                        "negative": negative != (factor < 0),
                    }
                )
            else:
                bounds = sorted((p["minimum"] * factor, p["maximum"] * factor))
                e.update({"loc": p["amount"] * factor, "minimum": bounds[0], "maximum": bounds[1]})
            for field in ("amount", "loc", "scale", "minimum", "maximum"):
                if field in e:
                    _number(e[field])
            if e["amount"] == 0:
                raise GateError("Native unit multiplication underflows the nonzero nominal")
            latents.add(latent.casefold())
            used.update((gate.casefold(), latent.casefold()))
        for e in a.get("exchanges", []):
            if used.intersection(_names(e.get("formula", ""))):
                raise GateError("Native helper leaks into another expression")
        for p in parameters:
            if used.intersection(_names(p.get("formula", ""))):
                raise GateError("Native helper leaks into a calculated parameter")
        a["parameters"] = [p for p in parameters if p["name"].casefold() not in used]
        if not a["parameters"]:
            a.pop("parameters")
    return document.replace(data=result) if document is not None else result


def _claims_graph(expression):
    """Prefixes alone never claim ordinary source parameter expressions."""
    if not isinstance(expression, str):
        return False
    identifiers = _names(expression)
    if not (
        any(n.startswith("bpspg") and _HELPER.fullmatch(n) for n in identifiers)
        and any(n.startswith("bpspl") and _HELPER.fullmatch(n) for n in identifiers)
    ):
        return False
    if not (re.search(r"\biff\s*\(", expression, re.I) or re.search(r"\bif\b", expression)):
        return False
    # Only a complete known shape can claim source expressions. Prefixes and
    # an IFF token alone are insufficient. A separate pass below rejects uses
    # of actually consumed helpers in any unknown expression.
    try:
        _graph(expression)
    except NativeUncertaintyError:
        return False
    return True
