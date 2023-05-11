from __future__ import annotations

from ..utils import T_T
from .base import QueryBuilder


class CreateTableQueryBuilder(QueryBuilder[T_T]):
    def build(self) -> tuple[str, list[str]]:
        column_defs: list[str] = []

        for column in self.table._metadata.columns:
            col_type: list[str] = []

            if column.primary:
                col_type.append("primary key")

            if other_column := column.foreign:
                col_type.append(f"references {other_column.table._metadata.name}({other_column.name})")

            column_def = f"{column.name} {column.db_datatype} {'' if column.optional else 'not null'} {' '.join(col_type)}"
            column_defs.append(column_def)

        query = f"create table {self.table._metadata.name} ({','.join(column_defs)})"
        return query, []
