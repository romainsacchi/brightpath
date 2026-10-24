"""Bounded arithmetic for portable ILCD process parameters (never Python eval)."""

from __future__ import annotations

import ast
import math
import operator
import re

from .ilcd_package import ILCDError, fail

_OPERATORS = {
    ast.Add: operator.add,
    ast.Sub: operator.sub,
    ast.Mult: operator.mul,
    ast.Div: operator.truediv,
    ast.Mod: operator.mod,
    ast.Pow: operator.pow,
}


def expression(formula: str) -> tuple[ast.Expression, set[str]]:
    """Validate portable scalar arithmetic and return its named dependencies."""
    if not isinstance(formula, str) or len(formula) > 4096:
        fail("formula_unsupported", "A formula must contain at most 4096 characters.")
    try:
        tree = ast.parse(formula, mode="eval")
    except (SyntaxError, RecursionError):
        fail("formula_unsupported", "Invalid portable arithmetic formula.")
    nodes = list(ast.walk(tree))
    if len(nodes) > 256:
        fail("formula_unsupported", "Formula exceeds the arithmetic complexity limit.")
    allowed = (
        ast.Expression,
        ast.BinOp,
        ast.UnaryOp,
        ast.Constant,
        ast.Name,
        ast.Load,
        ast.UAdd,
        ast.USub,
        *_OPERATORS.keys(),
    )
    for node in nodes:
        if not isinstance(node, allowed):
            fail("formula_unsupported", "Only numeric constants, names, and scalar arithmetic are supported.")
        if isinstance(node, ast.Constant):
            try:
                valid = type(node.value) in (float, int) and math.isfinite(float(node.value))
            except (OverflowError, ValueError):
                valid = False
            if not valid:
                fail("formula_unsupported", "Formula constants must be finite numbers.")
    return tree, {node.id for node in nodes if isinstance(node, ast.Name)}


def evaluate(formula: str, values: dict[str, float]) -> float:
    tree, names = expression(formula)
    if names - values.keys():
        fail("formula_undefined", f"Undefined parameters: {', '.join(sorted(names - values.keys()))}")

    def walk(node):
        if isinstance(node, ast.Constant):
            result = float(node.value)
        elif isinstance(node, ast.Name):
            result = values[node.id]
        elif isinstance(node, ast.UnaryOp):
            result = walk(node.operand) * (-1 if isinstance(node.op, ast.USub) else 1)
        elif isinstance(node, ast.BinOp):
            left, right = walk(node.left), walk(node.right)
            if isinstance(node.op, ast.Pow) and abs(right) > 1000:
                fail("formula_unsupported", "Formula exponent exceeds the arithmetic limit.")
            result = _OPERATORS[type(node.op)](left, right)
        else:
            fail("formula_unsupported", "Unsupported arithmetic expression.")
        if not isinstance(result, (float, int)) or not math.isfinite(result):
            fail("formula_result_invalid", "Formula result must be finite and real.")
        return result

    try:
        return float(walk(tree.body))
    except (ArithmeticError, ValueError, RecursionError) as error:
        fail("formula_result_invalid", str(error))


def parameter_values(parameters: list[dict]) -> dict[str, float]:
    """Resolve acyclic process-scoped parameters and verify cached amounts."""
    if len(parameters) > 1000:
        fail("parameter_invalid", "ILCD processes support at most 1000 parameters.")
    names = [p.get("name", "") for p in parameters]
    if len(set(names)) != len(names) or any(not re.fullmatch(r"[A-Za-z_][A-Za-z_0-9]*", name) for name in names):
        fail("parameter_invalid", "Process parameter names must be unique portable identifiers.")
    pending, values = {p["name"]: p for p in parameters}, {}
    for parameter in parameters:
        if parameter.get("formula") and expression(parameter["formula"])[1] - set(names):
            fail("formula_undefined", f"Undefined variables in {parameter['name']}.")
    while pending:
        ready = [
            name for name, p in pending.items() if not p.get("formula") or expression(p["formula"])[1] <= values.keys()
        ]
        if not ready:
            fail("formula_cyclic", "Process parameters contain a dependency cycle.")
        for name in ready:
            parameter = pending.pop(name)
            try:
                value = (
                    evaluate(parameter["formula"], values) if parameter.get("formula") else float(parameter["amount"])
                )
                if not math.isfinite(value) or isinstance(parameter.get("amount"), bool):
                    raise ValueError()
                if (
                    parameter.get("formula")
                    and parameter.get("amount") is not None
                    and not math.isclose(value, float(parameter["amount"]), rel_tol=1e-9, abs_tol=1e-12)
                ):
                    fail("formula_result_conflict", f"Cached amount disagrees with formula for {name}.")
            except ILCDError:
                raise
            except (KeyError, ValueError, TypeError, OverflowError):
                fail("parameter_invalid", f"Parameter {name} needs a finite value.")
            values[name] = value
    return values
