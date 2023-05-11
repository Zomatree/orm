from __future__ import annotations

from typing import TYPE_CHECKING, Any, Generic, Literal, cast, overload
from typing_extensions import Self

from orm.utils import Connection

from .utils import T_T, T_OT, T, T_Ts

if TYPE_CHECKING:
    from .column import Column
    from .table import Table
    from .utils import Connection


class WhereQuery:
    def __init__(self, column: Column[Any, Any], value: Any, op: str):
        self.column = column
        self.value = value
        self.op = op


class QueryBuilder(Generic[T_T]):
    def __init__(self, table: type[T_T]) -> None:
        self.table = table

    def build(self) -> tuple[str, list[Any]]:
        raise NotImplementedError

    async def execute(self, conn: Connection) -> int:
        query, parameters = self.build()

        res = await conn.execute(query, *parameters)

        return int(res.split(" ")[1])

    async def fetch(self, conn: Connection) -> list[T_T]:
        query, parameters = self.build()

        records = await conn.fetch(query, *parameters)

        return [self.table(**record) for record in records]

    async def fetchone(self, conn: Connection) -> T_T | None:
        query, parameters = self.build()

        record = await conn.fetchrow(query, *parameters)

        if record:
            return self.table(**record)


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
        columns = ", ".join([column.name for column in self.table._metadata.columns])
        query_parts = [f"select {columns} from {self.table._metadata.name}"]

        if self._wheres:
            where_clause: list[str] = []

            for i, (joiner, where) in enumerate(self._wheres, 1):
                if i == 1:
                    where_clause.append(
                        f"{where.column._to_full_name()} {where.op} ${i}"
                    )
                else:
                    where_clause.append(
                        f"{joiner} {where.column._to_full_name()} {where.op} ${i}"
                    )

            query_parts.append(f"where {' '.join(where_clause)}")

        for column in self._groups:
            query_parts.append(f"group by {column._to_full_name()}")

        if order := self._order:
            column, ty = order

            if isinstance(column, tuple):
                query_parts.append(f"order by {column._to_full_name()} {ty}")

        if (limit := self._limit) is not None:
            query_parts.append(f"limit {limit}")

        return " ".join(query_parts), [where.value for _, where in self._wheres]

    def join(
        self, query: SelectQueryBuilder[T_OT]
    ) -> JoinSelectQueryBuilder[T_T, T_OT]:
        return JoinSelectQueryBuilder(self, query)


class JoinSelectQueryBuilder(SelectQueryBuilder[T_T], Generic[T_T, *T_Ts]):
    def __init__(
        self, select_query: SelectQueryBuilder[T_T], join: SelectQueryBuilder[Any]
    ):
        self._wheres = select_query._wheres
        self.table = select_query.table
        self.joins: list[SelectQueryBuilder[Table]] = [join]

    def join(
        self, query: SelectQueryBuilder[T_OT]
    ) -> JoinSelectQueryBuilder[T_T, *T_Ts, T_OT]:
        self.joins.append(query)
        return cast(JoinSelectQueryBuilder[T_T, *T_Ts, T_OT], self)

    def build(self) -> tuple[str, list[str]]:
        query_parts: list[str] = []
        columns: list[str] = []
        values: list[Any] = []


        for table in [self.table] + [join.table for join in self.joins]:
            for column in table._metadata.columns:
                columns.append(
                    f"{table._metadata.name}.{column.name} as {table._metadata.name}_{column.name}"
                )

        query_parts.append(f"select {','.join(columns)} from {self.table._metadata.name}")

        for join in self.joins:
            wheres: list[str] = []

            for i, (joiner, where) in enumerate(join._wheres, 1):
                if isinstance(where.value, Column):
                    value = f"{where.value.table._metadata.name}.{where.value.name}"
                else:
                    value = f"${len(values) + 1}"
                    values.append(where.value)

                if i == 1:
                    wheres.append(f"{where.column._to_full_name()} {where.op} {value}")
                else:
                    wheres.append(
                        f"{joiner} {where.column._to_full_name()} {where.op} {value}"
                    )

            query_parts.append(
                f"inner join {join.table._metadata.name} on {' and '.join(wheres)}"
            )

        wheres = []

        for i, (joiner, where) in enumerate(self._wheres, 1):
            if isinstance(where.value, Column):
                value = f"{where.value.table._metadata.name}.{where.value.name}"
            else:
                value = f"${len(values) + 1}"
                values.append(where.value)

            if i == 1:
                wheres.append(f"{where.column._to_full_name()} {where.op} {value}")
            else:
                wheres.append(
                    f"{joiner} {where.column._to_full_name()} {where.op} {value}"
                )

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
                [
                    table(**collections[table._metadata.name])
                    for table in [self.table] + [join.table for join in self.joins]
                ],
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

            output.append(
                tuple(
                    table(**collections[table._metadata.name])
                    for table in [self.table] + [join.table for join in self.joins]
                )
            )

        return cast(list[tuple[T_T, *T_Ts]], output)


class InsertQueryBuilder(QueryBuilder[T_T]):
    def __init__(self, table: Table):
        self.table = table

    def build(self) -> tuple[str, list[Any]]:
        columns = ", ".join([column.name for column in self.table._metadata.columns])
        values = ", ".join(f"${i}" for i in range(len(self.table._metadata.columns)))

        return (
            f"insert into {self.table._metadata.name} ({columns}) values ({values})",
            [getattr(self, column.name) for column in self.table._metadata.columns],
        )

    async def fetchone(self, conn: Connection) -> T_T:
        row = await super().fetchone(conn)
        assert row is not None
        return row

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
                value = f"{where.value.table._metadata.name}.{where.value.name}"
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


class CreateTableQueryBuilder(QueryBuilder[T_T]):
    def build(self) -> tuple[str, list[str]]:
        column_defs: list[str] = []

        for column in self.table._metadata.columns:
            col_type: list[str] = []

            if column.primary:
                col_type.append("primary key")

            if other_column := column.foreign:
                col_type.append(f"foreign key references {other_column.table._metadata.name}({other_column.name})")

            column_def = f"{column.name} {column.db_datatype} {'' if column.optional else 'not null'} {col_type}"
            column_defs.append(column_def)

        query = f"create table {self.table._metadata.name} ({','.join(column_defs)})"
        return query, []
