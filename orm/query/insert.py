from __future__ import annotations

from typing import TYPE_CHECKING, Any

from ..utils import T_T, Missing
from .base import QueryBuilder

if TYPE_CHECKING:
    from ..table import Table
    from ..utils import Connection


class InsertQueryBuilder(QueryBuilder[T_T]):
    def __init__(self, table: Table):
        self.table_instance = table
        self.table = type(table)

    def build(self) -> tuple[str, list[Any]]:
        columns: list[str] = []
        values: list[Any] = []

        for column in self.table._metadata.columns:
            columns.append(column._to_full_name())

            if (value := getattr(self.table_instance, column.name, Missing)) is not Missing:
                values.append(value)

            elif default := column.default:
                values.append(default())

            else:
                raise Exception(f"Missing required column {self.table.__name__}.{column.name}")

        column_placeholders = [f"${i + 1}" for i in range(len(columns))]

        return (
            f"insert into \"{self.table._metadata.name}\" ({','.join(columns)}) values ({','.join(column_placeholders)})",
            values,
        )

    async def fetchone(self, conn: Connection) -> T_T:
        row = await super().fetchone(conn)
        assert row is not None

        return row
