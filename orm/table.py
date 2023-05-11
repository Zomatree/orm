from __future__ import annotations

from typing import Annotated, Any, ClassVar, Self, get_args, get_origin, get_type_hints

from typing_extensions import Self

from .column import Column, ColumnBuilder
from .query import CreateTableQueryBuilder, InsertQueryBuilder, SelectQueryBuilder, UpdateQueryBuilder
from .utils import Missing, eval_annotation
from .where_query import WhereQuery

__all__ = ("TableMetadata", "Table")


class TableMetadata:
    def __init__(self, name: str, columns: list[Column[Any, Any]]) -> None:
        self.name = name
        self.columns = columns
        self.values: dict[str, Any] = {}


class Table:
    _metadata: ClassVar[TableMetadata]

    def __init_subclass__(cls, *, table_name: str | None = None) -> None:
        columns: list[Column[Any, Any]] = []

        for key, ann in get_type_hints(cls, include_extras=True).items():
            if key.startswith("_"):
                continue

            ann = eval_annotation(ann)
            origin: Any = get_origin(ann)

            if origin is Annotated:
                column_builder: ColumnBuilder[Any]
                ty, column_builder = get_args(ann)
                db_ty = get_args(ty)[0]
                column_builder._type = eval_annotation(ty)
            else:
                column_ty: type[Column[Any, Any]] = ann
                (db_ty, ty) = get_args(column_ty)
                column_builder = ColumnBuilder[Any]().type(ty)

            column_builder = column_builder.name(key).table(cls)
            column_builder._db_type = get_args(db_ty)[0]

            if (value := getattr(cls, key, Missing)) is not Missing:
                column_builder = column_builder.default(value)

            column = column_builder.build()
            columns.append(column)
            setattr(cls, key, column)

        cls._metadata = TableMetadata(table_name or cls.__name__, columns)

    def __init__(self, **kwargs: Any):
        self._metadata.values = kwargs

    def __repr__(self) -> str:
        attrs = " ".join(f"{k}={v!r}" for k, v in self._metadata.values.items())

        return f"<{self.__class__.__name__} {attrs}>"

    @classmethod
    def select(cls) -> SelectQueryBuilder[Self]:
        return SelectQueryBuilder(cls)

    @classmethod
    def where(cls, where: WhereQuery) -> SelectQueryBuilder[Self]:
        return cls.select().where(where)

    @classmethod
    def create(cls) -> CreateTableQueryBuilder[Self]:
        return CreateTableQueryBuilder(cls)

    @classmethod
    def update(cls) -> UpdateQueryBuilder[Self]:
        return UpdateQueryBuilder(cls)

    def insert(self) -> InsertQueryBuilder[Self]:
        return InsertQueryBuilder(self)
