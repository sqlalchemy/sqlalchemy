from __future__ import annotations

import typing

from sqlalchemy import select
from sqlalchemy.ext.hybrid import hybrid_method
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column


class Base(DeclarativeBase):
    pass


class Interval(Base):
    __tablename__ = "interval"

    id: Mapped[int] = mapped_column(primary_key=True)
    start: Mapped[int]
    end: Mapped[int]

    def __init__(self, start: int, end: int):
        self.start = start
        self.end = end

    @hybrid_property
    def length(self) -> int:
        return self.end - self.start

    @hybrid_method
    def contains(self, point: int) -> int:
        return (self.start <= point) & (point <= self.end)

    @hybrid_method
    def intersects(self, other: Interval) -> int:
        return self.contains(other.start) | self.contains(other.end)

    @hybrid_method
    def fancy_thing(self, point: int, x: int, y: int) -> bool:
        return (self.start <= point) & (point <= self.end)


i1 = Interval(5, 10)
i2 = Interval(7, 12)

expr1 = Interval.length.in_([5, 10])

expr2 = Interval.contains(7)

expr3 = Interval.intersects(i2)

expr4 = Interval.fancy_thing(10, 12, 15)

# test that pep-612 actually works

# EXPECTED_MYPY: Too few arguments
Interval.fancy_thing(1, 2)

# EXPECTED_MYPY: Argument 2 has incompatible type
Interval.fancy_thing(1, "foo", 3)

stmt1 = select(Interval).where(expr1).where(expr4)

stmt2 = select(expr4)

if typing.TYPE_CHECKING:
    # EXPECTED_RE_TYPE: builtins.int\*?
    reveal_type(i1.length)

    # EXPECTED_RE_TYPE: sqlalchemy.*._HybridClassLevelAccessor\[builtins.int\*?\]
    reveal_type(Interval.length)

    # EXPECTED_RE_TYPE: sqlalchemy.*.BinaryExpression\[builtins.bool\*?\]
    reveal_type(expr1)

    # EXPECTED_RE_TYPE: sqlalchemy.*.SQLCoreOperations\[builtins.int\*?\]
    reveal_type(expr2)

    # EXPECTED_RE_TYPE: sqlalchemy.*.SQLCoreOperations\[builtins.int\*?\]
    reveal_type(expr3)

    # EXPECTED_TYPE: bool
    reveal_type(i1.fancy_thing(1, 2, 3))

    # EXPECTED_TYPE: SQLCoreOperations[bool]
    reveal_type(expr4)

    # EXPECTED_TYPE: Select[bool]
    reveal_type(stmt2)
