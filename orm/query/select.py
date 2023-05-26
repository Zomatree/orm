from __future__ import annotations

from typing import TYPE_CHECKING, Any, Generic, Literal, cast, overload

from typing_extensions import Self

from ..column import Column
from ..utils import T_OT, T_T, T_Ts
from ..where_query import WhereQuery
from ..utils import T_T, T_OT, T_Ts

if TYPE_CHECKING:
    from ..table import Table
    from ..utils import Connection

from .base import QueryBuilder


class SelectQueryBuilder(QueryBuilder[T_T]):
    def __init__(self, table: type[T_T]) -> None:
        super().__init__(table)
        self._wheres: list[tuple[str, WhereQuery]] = []
        self._order: tuple[Column[Any, Any], str] | None = None
        self._groups: list[Column[Any, Any]] = []
        self._limit: int | None = None

    def order_by_asc(self, column: Column[Any, Any]) -> Self:
        self._order = (column, "asc")
        return self

    def order_by_desc(self, column: Column[Any, Any]) -> Self:
        self._order = (column, "desc")
        return self

    def group_by(self, column: Column[Any, Any]) -> Self:
        self._groups.append(column)
        return self

    def limit(self, count: int) -> Self:
        self._limit = count
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

    def build(self) -> tuple[str, list[Any]]:
        columns = ", ".join([f"`{column.name}`" for column in self.table._metadata.columns])
        query_parts = [f"select {columns} from `{self.table._metadata.name}`"]

        if self._wheres:
            where_clause: list[str] = []

            for i, (joiner, where) in enumerate(self._wheres, 1):
                if i == 1:
                    where_clause.append(f"{where.column._to_full_name()} {where.op} ${i}")
                else:
                    where_clause.append(f"{joiner} {where.column._to_full_name()} {where.op} ${i}")

            query_parts.append(f"where {' '.join(where_clause)}")

        for column in self._groups:
            query_parts.append(f"group by `{column._to_full_name()}`")

        if order := self._order:
            column, ty = order

            if isinstance(column, tuple):
                query_parts.append(f"order by `{column._to_full_name()}` {ty}")

        if (limit := self._limit) is not None:
            query_parts.append(f"limit {limit}")

        return " ".join(query_parts), [where.value for _, where in self._wheres]

    def join(self, query: SelectQueryBuilder[T_OT]) -> JoinSelectQueryBuilder[T_T, T_OT]:
        return JoinSelectQueryBuilder(self, query)


class JoinSelectQueryBuilder(SelectQueryBuilder[T_T], Generic[T_T, *T_Ts]):
    def __init__(self, select_query: SelectQueryBuilder[T_T], join: SelectQueryBuilder[Any]):
        super().__init__(select_query.table)
        self._wheres = select_query._wheres
        self.joins: list[SelectQueryBuilder[Table]] = [join]

    def join(self, query: SelectQueryBuilder[T_OT]) -> JoinSelectQueryBuilder[T_T, *T_Ts, T_OT]:
        self.joins.append(query)
        return cast(JoinSelectQueryBuilder[T_T, *T_Ts, T_OT], self)

    def build(self) -> tuple[str, list[str]]:
        query_parts: list[str] = []
        columns: list[str] = []
        values: list[Any] = []

        for table in [self.table] + [join.table for join in self.joins]:
            for column in table._metadata.columns:
                columns.append(f"{table._metadata.name}.{column.name} as {table._metadata.name}_{column.name}")

        query_parts.append(f"select {','.join(columns)} from {self.table._metadata.name}")

        for join in self.joins:
            wheres: list[str] = []

            for i, (joiner, where) in enumerate(join._wheres, 1):
                if isinstance(where.value, Column):
                    value = f"`{where.value.table._metadata.name}`.`{where.value.name}`"
                else:
                    value = f"${len(values) + 1}"
                    values.append(where.value)

                if i == 1:
                    wheres.append(f"{where.column._to_full_name()} {where.op} {value}")
                else:
                    wheres.append(f"{joiner} {where.column._to_full_name()} {where.op} {value}")

            query_parts.append(f"inner join `{join.table._metadata.name}` on {' and '.join(wheres)}")

        wheres = []

        for i, (joiner, where) in enumerate(self._wheres, 1):
            if isinstance(where.value, Column):
                value = f"`{where.value.table._metadata.name}`.`{where.value.name}`"
            else:
                value = f"${len(values) + 1}"
                values.append(where.value)

            if i == 1:
                wheres.append(f"{where.column._to_full_name()} {where.op} {value}")
            else:
                wheres.append(f"{joiner} {where.column._to_full_name()} {where.op} {value}")

        query_parts.append(f"where {' '.join(wheres)}" if wheres else "")

        for column in self._groups:
            query_parts.append(f"group by {column._to_full_name()}")

        if order := self._order:
            column, ty = order

            if isinstance(column, tuple):
                query_parts.append(f"order by {column._to_full_name()} {ty}")

        if (limit := self._limit) is not None:
            query_parts.append(f"limit {limit}")

        query = " ".join(query_parts)

        return query, values

    async def fetchone(self, conn: Connection) -> tuple[T_T, *T_Ts] | None:
        query, parameters = self.build()

        row = await conn.fetchrow(query, *parameters)

        if row:
            collections: dict[str, dict[str, Any]] = {}

            for column, value in row.items():
                table_name, *rest = column.split("_")

                collections.setdefault(table_name, {})["_".join(rest)] = value

            return cast(
                tuple[T_T, *T_Ts],
                tuple([table(**collections[table._metadata.name.lower()]) for table in [self.table] + [join.table for join in self.joins]]),
            )

    async def fetch(self, conn: Connection) -> list[tuple[T_T, *T_Ts]]:
        query, parameters = self.build()

        rows = await conn.fetch(query, *parameters)
        output: list[tuple[Table, ...]] = []

        for row in rows:
            collections: dict[str, dict[str, Any]] = {}

            for column, value in row.items():
                table_name, *rest = column.split("_")

                collections.setdefault(table_name, {})["_".join(rest)] = value

            output.append(tuple(table(**collections[table._metadata.name]) for table in [self.table] + [join.table for join in self.joins]))

        return cast(list[tuple[T_T, *T_Ts]], output)
