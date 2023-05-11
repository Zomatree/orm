from typing import Literal
from .column import Column

Text = Column[Literal["text"], str]
Int = Column[Literal["number"], int]
BigInt = Column[Literal["bigint"], int]
SmallInt = Column[Literal["smallint"], int]
Decimal = Column[Literal["decimal"], float]
Numeric = Column[Literal["numeric"], float]
Real = Column[Literal["real"], float]
Double = Column[Literal["double precision"], float]

