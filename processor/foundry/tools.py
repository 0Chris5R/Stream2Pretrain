"""Frozen deterministic paper-local tools used by RL environments."""

from __future__ import annotations

import ast
import math
import operator
import re
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any


class ToolError(ValueError):
    pass


_BINARY: dict[type[ast.operator], Callable[[float, float], float]] = {
    ast.Add: operator.add,
    ast.Sub: operator.sub,
    ast.Mult: operator.mul,
    ast.Div: operator.truediv,
    ast.FloorDiv: operator.floordiv,
    ast.Mod: operator.mod,
    ast.Pow: operator.pow,
}
_UNARY: dict[type[ast.unaryop], Callable[[float], float]] = {
    ast.UAdd: operator.pos,
    ast.USub: operator.neg,
}
_FUNCTIONS: dict[str, Callable[..., float]] = {
    "abs": abs,
    "sqrt": math.sqrt,
    "log": math.log,
    "exp": math.exp,
    "sin": math.sin,
    "cos": math.cos,
    "tan": math.tan,
    "min": min,
    "max": max,
}
_CONSTANTS = {"pi": math.pi, "e": math.e}


@dataclass(slots=True)
class PaperRuntime:
    spans: dict[str, str]
    equations: dict[str, dict[str, Any]] = field(default_factory=dict)
    tables: dict[str, dict[str, Any]] = field(default_factory=dict)
    calls: int = 0

    def _consume(self) -> None:
        self.calls += 1

    def search(self, query: str, *, limit: int = 8) -> list[dict[str, Any]]:
        self._consume()
        terms = set(_tokens(query))
        if not terms:
            return []
        scored: list[tuple[float, str, str]] = []
        for span_id, text in self.spans.items():
            words = _tokens(text)
            word_set = set(words)
            overlap = len(terms & word_set)
            if not overlap:
                continue
            density = overlap / max(1, len(terms))
            length_penalty = 1.0 / (1.0 + math.log1p(len(words)))
            scored.append((density + length_penalty, span_id, text))
        scored.sort(key=lambda item: (-item[0], item[1]))
        return [
            {"span_id": span_id, "score": round(score, 8), "snippet": text[:500]}
            for score, span_id, text in scored[: max(1, min(limit, 20))]
        ]

    def open(self, object_id: str) -> dict[str, Any]:
        self._consume()
        if object_id in self.spans:
            return {"type": "span", "id": object_id, "text": self.spans[object_id]}
        if object_id in self.equations:
            return {"type": "equation", "id": object_id, **self.equations[object_id]}
        if object_id in self.tables:
            return {"type": "table", "id": object_id, **self.tables[object_id]}
        raise ToolError(f"unknown public object {object_id}")

    def find(self, needle: str, *, object_id: str | None = None) -> list[dict[str, Any]]:
        self._consume()
        if not needle:
            return []
        corpus = (
            {object_id: self.spans[object_id]}
            if object_id and object_id in self.spans
            else self.spans
            if object_id is None
            else {}
        )
        lowered = needle.casefold()
        results: list[dict[str, Any]] = []
        for span_id, text in corpus.items():
            start = 0
            folded = text.casefold()
            while (index := folded.find(lowered, start)) >= 0:
                results.append(
                    {
                        "span_id": span_id,
                        "start": index,
                        "end": index + len(needle),
                        "context": text[max(0, index - 100) : index + len(needle) + 100],
                    }
                )
                start = index + max(1, len(needle))
        return results[:100]

    def calculator(self, expression: str) -> float:
        self._consume()
        try:
            tree = ast.parse(expression, mode="eval")
        except SyntaxError as exc:
            # Provider-authored tool requests are untrusted input. A malformed
            # calculator expression is a normal tool observation, not a worker
            # failure that may pin the same paper at the head of the queue.
            raise ToolError("invalid calculator expression") from exc
        value = _calculate(tree.body)
        if not math.isfinite(value):
            raise ToolError("calculator result is not finite")
        return value

    def symbolic(self, operation: str, expression: str, *, other: str | None = None) -> str | bool:
        self._consume()
        import sympy

        left = _symbolic_parse(expression)
        if operation == "simplify":
            return str(sympy.simplify(left))
        if operation == "expand":
            return str(sympy.expand(left))
        if operation == "factor":
            return str(sympy.factor(left))
        if operation == "equivalent":
            if other is None:
                raise ToolError("equivalent requires other")
            right = _symbolic_parse(other)
            return bool(sympy.simplify(left - right) == 0)
        raise ToolError(f"unsupported symbolic operation {operation}")

    def reset(self) -> None:
        self.calls = 0


def _calculate(node: ast.AST) -> float:
    if isinstance(node, ast.Constant) and isinstance(node.value, (int, float)):
        return float(node.value)
    if isinstance(node, ast.Name) and node.id in _CONSTANTS:
        return float(_CONSTANTS[node.id])
    if isinstance(node, ast.BinOp) and type(node.op) in _BINARY:
        left = _calculate(node.left)
        right = _calculate(node.right)
        if isinstance(node.op, ast.Pow) and abs(right) > 100:
            raise ToolError("exponent outside safe bound")
        return float(_BINARY[type(node.op)](left, right))
    if isinstance(node, ast.UnaryOp) and type(node.op) in _UNARY:
        return float(_UNARY[type(node.op)](_calculate(node.operand)))
    if (
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id in _FUNCTIONS
    ):
        if node.keywords:
            raise ToolError("keyword arguments are not allowed")
        args = [_calculate(arg) for arg in node.args]
        return float(_FUNCTIONS[node.func.id](*args))
    raise ToolError(f"unsupported calculator syntax: {type(node).__name__}")


def _symbolic_parse(expression: str) -> Any:
    import sympy

    if len(expression) > 2_000:
        raise ToolError("symbolic expression exceeds the safe length bound")
    try:
        root = ast.parse(expression.replace("^", "**"), mode="eval").body
    except SyntaxError as exc:
        raise ToolError("invalid symbolic expression") from exc

    functions = {
        "sqrt": sympy.sqrt,
        "log": sympy.log,
        "exp": sympy.exp,
        "sin": sympy.sin,
        "cos": sympy.cos,
        "tan": sympy.tan,
    }

    def convert(node: ast.AST) -> Any:
        if isinstance(node, ast.Constant) and isinstance(node.value, (int, float)):
            return (
                sympy.Float(node.value)
                if isinstance(node.value, float)
                else sympy.Integer(node.value)
            )
        if isinstance(node, ast.Name) and re.fullmatch(r"[A-Za-z][A-Za-z0-9_]{0,63}", node.id):
            return sympy.Symbol(node.id)
        if isinstance(node, ast.BinOp) and type(node.op) in _BINARY:
            left = convert(node.left)
            right = convert(node.right)
            if (
                isinstance(node.op, ast.Pow)
                and getattr(right, "is_number", False)
                and abs(float(right)) > 100
            ):
                raise ToolError("symbolic exponent outside safe bound")
            operations: dict[type[ast.operator], Callable[[], Any]] = {
                ast.Add: lambda: left + right,
                ast.Sub: lambda: left - right,
                ast.Mult: lambda: left * right,
                ast.Div: lambda: left / right,
                ast.Pow: lambda: left**right,
            }
            operation = operations.get(type(node.op))
            if operation is None:
                raise ToolError("unsupported symbolic binary operation")
            return operation()
        if isinstance(node, ast.UnaryOp) and type(node.op) in _UNARY:
            value = convert(node.operand)
            return value if isinstance(node.op, ast.UAdd) else -value
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id in functions
            and not node.keywords
        ):
            return functions[node.func.id](*(convert(value) for value in node.args))
        raise ToolError(f"unsupported symbolic syntax: {type(node).__name__}")

    return convert(root)


def _tokens(value: str) -> list[str]:
    return re.findall(r"[a-z0-9]+", value.casefold())


__all__ = ["PaperRuntime", "ToolError"]
