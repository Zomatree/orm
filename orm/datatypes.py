from typing import Literal

from .column import Column

__all__ = ("Text", "Int", "BigInt", "SmallInt", "Decimal", "Numeric", "Real", "Double")

Text = Column[Literal["text"], str]
Int = Column[Literal["integer"], int]
BigInt = Column[Literal["bigint"], int]
SmallInt = Column[Literal["smallint"], int]
Decimal = Column[Literal["decimal"], float]
Numeric = Column[Literal["numeric"], float]
Real = Column[Literal["real"], float]
Double = Column[Literal["double precision"], float]
