from __future__ import annotations

from types import NoneType
from typing import TYPE_CHECKING, Any, Generic, TypeVar, get_args, overload

from typing_extensions import Self

from .where_query import WhereQuery
from typing import Callable, Generic, TypeVar, get_args, overload, TYPE_CHECKING, Any
from typing_extensions import Self

from .utils import T

if TYPE_CHECKING:
    from .table import Table

__all__ = ("Column", "ColumnBuilder", "primary", "foreign", "default")

T_P = TypeVar("T_P")


class Column(Generic[T_P, T]):
    def __init__(self, table: type[Table], name: str, datatype: T, db_datatype: str, default: Callable[[], T] | None, optional: bool, primary: bool, foreign: Column[Any, T] | None):
        self.table = table
        self.name = name
        self.datatype = datatype
        self.db_datatype = db_datatype
        self.default = default
        self.optional = optional
        self.primary = primary
        self.foreign = foreign

    def _to_full_name(self) -> str:
        return f"\"{self.table._metadata.name}\".\"{self.name}\""

    @overload
    def __get__(self, instance: None, _: type[Table]) -> Self:
        ...

    @overload
    def __get__(self, instance: Table, _: type[Table]) -> T:
        ...

    def __get__(self, instance: Table | None, _: type[Table]) -> T | Self:
        if instance is None:
            return self

        try:
            return instance._metadata.values[self.name]
        except:
            raise AttributeError

    def __eq__(self, value: T | Self) -> WhereQuery:
        return WhereQuery(self, value, "=")

    def __lt__(self, value: T | Self) -> WhereQuery:
        return WhereQuery(self, value, "<")

    def __le__(self, value: T | Self) -> WhereQuery:
        return WhereQuery(self, value, "<=")

    def __ne__(self, value: T | Self) -> WhereQuery:
        return WhereQuery(self, value, "!=")

class ColumnBuilder(Generic[T]):
    def __init__(self) -> None:
        self._name: str | None = None
        self._type: T | None = None
        self._db_type: str | None = None
        self._default: Callable[[], T] | None = None
        self._primary: bool = False
        self._foreign: Column[Any, Any] | None = None
        self._table: type[Table] | None = None

    def name(self, name: str) -> Self:
        self._name = name
        return self

    def type(self, type: T) -> ColumnBuilder[T]:
        self._type = type
        return self

    def default(self, default: Callable[[], T]) -> Self:
        self._default = default
        return self

    def primary(self) -> Self:
        self._primary = True
        return self

    def foreign(self, column: Column[Any, T]) -> Self:
        self._foreign = column
        return self

    def table(self, table: type[Table]) -> Self:
        self._table = table
        return self

    def build(self) -> Column[Any, T]:
        if not self._name:
            raise Exception("No name")

        if self._type is None or self._db_type is None:
            raise Exception("No type")

        if not self._table:
            raise Exception("No table")

        if self._foreign and self._type != self._foreign.datatype:
            raise Exception(f"{self._table.__name__}.{self._name} does not match the foreign key type of {self._foreign.datatype.__name__}")

        return Column[Any, T](self._table, self._name, self._type, self._db_type, self._default, NoneType in get_args(self._type), self._primary, self._foreign)


def primary() -> ColumnBuilder[Any]:
    return ColumnBuilder[Any]().primary()


def foreign(column: Column[Any, T]) -> ColumnBuilder[T]:
    return ColumnBuilder[T]().foreign(column)


def default(default: Callable[[], T]) -> ColumnBuilder[T]:
    return ColumnBuilder[T]().default(default)
