from __future__ import annotations

from typing import Any

from typing_extensions import Self

from ..column import Column
from ..utils import T_T, T
from ..where_query import WhereQuery
from .base import QueryBuilder


class UpdateQueryBuilder(QueryBuilder[T_T]):
    def __init__(self, table: type[T_T]) -> None:
        super().__init__(table)
        self._set: list[tuple[Column[Any, Any], Any]] = []
        self._wheres: list[WhereQuery] = []

    def set(self, column: Column[Any, T], value: T) -> Self:
        self._set.append((column, value))
        return self

    def where(self, query: WhereQuery) -> Self:
        self._wheres.append(query)
        return self

    def build(self) -> tuple[str, list[str]]:
        values: list[Any] = []

        sets: list[str] = []

        for column, value in self._set:
            value = f"${len(values) + 1}"
            values.append(value)

            sets.append(f"{column._to_full_name()} = {value}")

        wheres: list[str] = []

        for where in self._wheres:
            if isinstance(where.value, Column):
                value = f"`{where.value.table._metadata.name}`.`{where.value.name}`"
            else:
                value = f"${len(values) + 1}"
                values.append(where.value)

            wheres.append(f"{where.column._to_full_name()} {where.op} {value}")

        if wheres:
            where_clause = f"where {' and '.join(wheres)}"
        else:
            where_clause = ""

        return (
            f"update {self.table._metadata.name} set {','.join(sets)} {where_clause} returning *",
            values,
        )
