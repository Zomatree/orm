from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal, TypeVar, TypeVarTuple
import asyncpg

if TYPE_CHECKING:
    Connection = asyncpg.Connection[asyncpg.Record]
    from .table import Table

T = TypeVar("T")
T_T = TypeVar("T_T", bound="Table", covariant=True)
T_OT = TypeVar("T_OT", bound="Table", covariant=True)
T_Ts = TypeVarTuple("T_Ts")


def eval_annotation(
    annot: Any,
    locals: dict[str, Any] | None = None,
    globals: dict[str, Any] | None = None,
) -> Any:
    if not isinstance(annot, str):
        return annot

    return eval(annot, locals, globals)


class _Missing:
    def __eq__(self, _: Any) -> Literal[False]:
        return False

    def __repr__(self) -> str:
        return "<Missing>"


Missing = _Missing()
