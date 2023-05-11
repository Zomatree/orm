from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .column import Column

__all__ = ("WhereQuery",)


class WhereQuery:
    def __init__(self, column: Column[Any, Any], value: Any, op: str):
        self.column = column
        self.value = value
        self.op = op
