from __future__ import annotations

from typing import TYPE_CHECKING, Any, Generic

from ..utils import T_T

if TYPE_CHECKING:
    from ..utils import Connection


class QueryBuilder(Generic[T_T]):
    def __init__(self, table: type[T_T]) -> None:
        self.table = table

    def build(self) -> tuple[str, list[Any]]:
        raise NotImplementedError

    async def execute(self, conn: Connection) -> int:
        query, parameters = self.build()

        res = await conn.execute(query, *parameters)

        try:
            return int(res.split(" ")[-1])
        except ValueError:  # didnt give an amount - default to zero
            return 0

    async def fetch(self, conn: Connection) -> list[T_T]:
        query, parameters = self.build()

        records = await conn.fetch(query, *parameters)

        return [self.table(**record) for record in records]

    async def fetchone(self, conn: Connection) -> T_T | None:
        query, parameters = self.build()

        record = await conn.fetchrow(query, *parameters)

        if record:
            if isinstance(self.table, type):
                return self.table(**record)
            else:
                return self.table.__class__(**record)
