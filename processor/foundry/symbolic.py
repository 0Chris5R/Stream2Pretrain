"""Safe symbolic parsing helpers shared by task routing and verifier runtime."""

from __future__ import annotations

import re
from typing import Any


def symbolic_expression_is_checkable(value: str) -> bool:
    """Return whether ``value`` can participate in deterministic equivalence."""
    try:
        _residual(value)
    except (ImportError, TypeError, ValueError):
        return False
    return True


def symbolically_equivalent(left: str, right: str) -> bool:
    """Compare expressions or equalities without executing generated code."""
    try:
        import sympy

        left_residual, left_is_equation = _residual(left)
        right_residual, right_is_equation = _residual(right)
        if left_is_equation != right_is_equation:
            return False
        if sympy.simplify(left_residual - right_residual) == 0:
            return True
        # ``a=b`` and ``b=a`` describe the same equality.
        return bool(left_is_equation and sympy.simplify(left_residual + right_residual) == 0)
    except (ImportError, TypeError, ValueError):
        return _normalize_expression(left) == _normalize_expression(right)


def _residual(value: str) -> tuple[Any, bool]:
    normalized = value.strip().strip("$")
    if not normalized or len(normalized) > 2_000:
        raise ValueError("symbolic answer is empty or exceeds the safe length bound")
    if re.search(r"\b(if|else|otherwise|where|when)\b", normalized, flags=re.IGNORECASE):
        raise ValueError("symbolic answer contains prose instead of a canonical expression")
    try:
        import sympy
        from sympy.core.relational import Equality
        from sympy.parsing.latex import parse_latex

        parsed = parse_latex(normalized, backend="lark")
    except Exception as exc:
        raise ValueError("symbolic answer is not parseable LaTeX") from exc
    if isinstance(parsed, Equality):
        return parsed.lhs - parsed.rhs, True
    if not isinstance(parsed, sympy.Expr) or parsed.has(sympy.Tuple):
        raise ValueError("symbolic answer did not parse to one expression")
    return parsed, False


def _normalize_expression(value: str) -> str:
    return re.sub(r"\s+|\\left|\\right|\$", "", value).replace("^", "**")


__all__ = ["symbolic_expression_is_checkable", "symbolically_equivalent"]
