from __future__ import annotations

from typing import Any, Literal, overload

from typing_extensions import Self

from ..column import Column
from ..utils import T_T, T
from ..where_query import WhereQuery
from .base import QueryBuilder


class UpdateQueryBuilder(QueryBuilder[T_T]):
    def __init__(self, table: type[T_T]) -> None:
        super().__init__(table)
        self._set: list[tuple[Column[Any, Any], Any]] = []
        self._wheres: list[tuple[str, WhereQuery]] = []

    def set(self, column: Column[Any, T], value: T) -> Self:
        self._set.append((column, value))
        return self

    @overload
    def where(self, arg: Literal["and", "AND", "or", "OR"], query: WhereQuery) -> Self:
        ...

    @overload
    def where(self, arg: WhereQuery) -> Self:
        ...

    def where(
        self,
        arg: Literal["and", "AND", "or", "OR"] | WhereQuery,
        query: WhereQuery | None = None,
    ) -> Self:
        if query:
            assert arg in ["and", "AND", "or", "OR"]

            where = (arg, query)
        else:
            assert isinstance(arg, WhereQuery)

            where = ("and", arg)

        self._wheres.append(where)
        return self

    def or_where(self, query: WhereQuery) -> Self:
        return self.where("or", query)

    def and_where(self, query: WhereQuery) -> Self:
        return self.where("and", query)

    def build(self) -> tuple[str, list[str]]:
        query_parts: list[str] = []
        values: list[Any] = []

        query_parts.append(f"update \"{self.table._metadata.name}\"")

        sets: list[str] = []

        for column, value in self._set:
            value = f"${len(values) + 1}"
            values.append(value)

            sets.append(f"{column._to_full_name()} = {value}")

        query_parts.append(f"set {','.join(sets)}")

        wheres: list[str] = []

        for i, (joiner, where) in enumerate(self._wheres, 1):
            if isinstance(where.value, Column):
                value = f"\"{where.value.table._metadata.name}\".\"{where.value.name}\""
            else:
                value = f"${len(values) + 1}"
                values.append(where.value)

            if i == 1:
                wheres.append(f"{where.column._to_full_name()} {where.op} {value}")
            else:
                wheres.append(f"{joiner} {where.column._to_full_name()} {where.op} {value}")

        if wheres:
            query_parts.append(f"where {' '.join(wheres)}")

        query_parts.append("returning *")

        return (
            " ".join(query_parts),
            values,
        )
