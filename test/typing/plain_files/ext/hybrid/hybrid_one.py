from __future__ import annotations

import typing
from typing import assert_type

from sqlalchemy import Select
from sqlalchemy import select
from sqlalchemy.ext.hybrid import _HybridClassLevelAccessor
from sqlalchemy.ext.hybrid import hybrid_method
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.sql.elements import SQLCoreOperations
from sqlalchemy.sql.expression import BinaryExpression


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
    assert_type(i1.length, int)

    assert_type(Interval.length, _HybridClassLevelAccessor[int])

    assert_type(expr1, BinaryExpression[bool])

    assert_type(expr2, SQLCoreOperations[int])

    assert_type(expr3, SQLCoreOperations[int])

    assert_type(i1.fancy_thing(1, 2, 3), bool)

    assert_type(expr4, SQLCoreOperations[bool])

    assert_type(stmt2, Select[bool])
