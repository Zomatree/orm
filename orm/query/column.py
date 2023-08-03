from __future__ import annotations

from typing import Any, Generic, Literal

from orm.column import Column

from ..column import Column
from ..utils import T


class ColumnQueryBuilder(Generic[T]):
    def __init__(self, col: Column[Any, Any]):
        self.col = col

    def build(self) -> tuple[str, list[Any]]:
        raise NotImplementedError

class MaxColumn(ColumnQueryBuilder[T]):
    def __init__(self, col: Column[Any, T]):
        super().__init__(col)

    def build(self) -> tuple[str, list[Any]]:
        return f"max({self.col._to_full_name()})", []

class CountColumn(ColumnQueryBuilder[int]):
    def __init__(self, col: Column[Any, Any] | Literal["*"]):
        self.col_name = col._to_full_name() if not isinstance(col, str) else col

    def build(self) -> tuple[str, list[Any]]:
        return f"count({self.col_name})", []
