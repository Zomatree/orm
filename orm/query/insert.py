from __future__ import annotations

from typing import TYPE_CHECKING, Any

from ..utils import T_T
from .base import QueryBuilder

if TYPE_CHECKING:
    from ..table import Table
    from ..utils import Connection


class InsertQueryBuilder(QueryBuilder[T_T]):
    def __init__(self, table: Table):
        self.table = table

    def build(self) -> tuple[str, list[Any]]:
        columns = ", ".join([column.name for column in self.table._metadata.columns])
        values = ", ".join(f"${i + 1}" for i in range(len(self.table._metadata.columns)))

        return (
            f"insert into {self.table._metadata.name} ({columns}) values ({values}) returning *",
            [self.table._metadata.values[column.name] for column in self.table._metadata.columns],
        )

    async def fetchone(self, conn: Connection) -> T_T:
        row = await super().fetchone(conn)
        assert row is not None
        return row
