from __future__ import annotations
from typing import Literal, Never, Self, overload

from ..utils import T_T
from .base import QueryBuilder
from ..where_query import WhereQuery

class DeleteQueryBuilder(QueryBuilder[T_T]):
    def __init__(self, table: type[T_T]):
        self.table = table
        self._wheres: list[tuple[str, WhereQuery]] = []

    @overload
    def where(self, arg: Literal["and", "AND", "or", "OR"], query: WhereQuery) -> Self:
        ...

    @overload
    def where(self, arg: WhereQuery, query: Never = Never) -> Self:
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
        values: list[str] = []

        query_parts: list[str] = ["delete from", self.table._metadata.name]

        if self._wheres:
            where_clause: list[str] = []

            for i, (joiner, where) in enumerate(self._wheres, 1):
                if i == 1:
                    where_clause.append(f"{where.column._to_full_name()} {where.op} ${i}")
                else:
                    where_clause.append(f"{joiner} {where.column._to_full_name()} {where.op} ${i}")

            query_parts.append(f"where {' '.join(where_clause)}")

        query_parts.append

        return (
            " ".join(query_parts),
            values
        )
