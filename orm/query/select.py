from __future__ import annotations

from typing import TYPE_CHECKING, Any, Generic, Literal, cast, overload, Self

from ..column import Column
from ..utils import T, T_OT, T_T, Extras
from ..where_query import WhereQuery
from ..utils import T_T, T_OT, Extras

from .base import QueryBuilder
from .column import ColumnQueryBuilder

if TYPE_CHECKING:
    from ..table import Table
    from ..utils import Connection
    from asyncpg import Record

class SelectQueryBuilder(QueryBuilder[T_T], Generic[T_T]):
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

    def join(self, query: SelectQueryBuilder[T_OT]) -> TupleSelectQueryBuilder[T_T, *T_OT]:
        return TupleSelectQueryBuilder(self, query)

    def column(self, query: ColumnQueryBuilder[T]) -> TupleSelectQueryBuilder[T_T, T]:
        return TupleSelectQueryBuilder(self, query)

class TupleSelectQueryBuilder(SelectQueryBuilder[T_T], Generic[T_T, *Extras]):
    def __init__(self, select_query: SelectQueryBuilder[T_T], extra: SelectQueryBuilder[Any] | ColumnQueryBuilder[Any]):
        super().__init__(select_query.table)
        self._wheres = select_query._wheres
        self._extras: list[SelectQueryBuilder[Table] | ColumnQueryBuilder[Any]] = [extra]

    def join(self, query: SelectQueryBuilder[T_OT]) -> TupleSelectQueryBuilder[T_T, *Extras, T_OT]:
        self._extras.append(query)
        return cast(TupleSelectQueryBuilder[T_T, *Extras, T_OT], self)

    def column(self, query: ColumnQueryBuilder[T]) -> TupleSelectQueryBuilder[T_T, *Extras, T]:
        self._extras.append(query)
        return cast(TupleSelectQueryBuilder[T_T, *Extras, T], self)

    def build(self) -> tuple[str, list[Any]]:
        query_parts: list[str] = []
        columns: list[str] = []
        values: list[Any] = []

        for column in self.table._metadata.columns:
            columns.append(f"{self.table._metadata.name}.{column.name} as table_{self.table._metadata.name}_{column.name}")

        for i, extra in enumerate(self._extras):
            if isinstance(extra, SelectQueryBuilder):
                for column in extra.table._metadata.columns:
                    columns.append(f"{extra.table._metadata.name}.{column.name} as table_{extra.table._metadata.name}_{column.name}")
            else:
                extra_query, extra_values = extra.build()

                columns.append(f"{extra_query} as extra_{i}")
                values.extend(extra_values)

        query_parts.append(f"select {','.join(columns)} from {self.table._metadata.name}")

        for extra in self._extras:
            if isinstance(extra, SelectQueryBuilder):
                wheres: list[str] = []

                for i, (joiner, where) in enumerate(extra._wheres, 1):
                    if isinstance(where.value, Column):
                        value = f"`{where.value.table._metadata.name}`.`{where.value.name}`"
                    else:
                        value = f"${len(values) + 1}"
                        values.append(where.value)

                    if i == 1:
                        wheres.append(f"{where.column._to_full_name()} {where.op} {value}")
                    else:
                        wheres.append(f"{joiner} {where.column._to_full_name()} {where.op} {value}")

                query_parts.append(f"inner join `{extra.table._metadata.name}` on {' and '.join(wheres)}")

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

    def _build_record(self, record: Record) -> tuple[T_T, *Extras]:
        collections: list[dict[str, Any] | Any] = []
        last = None

        for column, value in record.items():
            column_type, rest = column.split("_", 1)

            if column_type == "table":
                table_name, column_name = rest.split("_", 1)

                if last == table_name:
                    collections[-1][column_name] = value
                else:
                    last = table_name
                    collections.append({column_name: value})
            else:
                collections.append(value)

        return cast(tuple[T_T, *Extras], tuple(collections))

    async def fetchone(self, conn: Connection) -> tuple[T_T, *Extras] | None:
        query, parameters = self.build()
        record = await conn.fetchrow(query, *parameters)

        if record:
            return self._build_record(record)

    async def fetch(self, conn: Connection) -> list[tuple[T_T, *Extras]]:
        query, parameters = self.build()
        records = await conn.fetch(query, *parameters)

        return [self._build_record(record) for record in records]
