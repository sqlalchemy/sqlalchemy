from __future__ import annotations

import typing

from sqlalchemy import Float
from sqlalchemy import func
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.sql.expression import ColumnElement


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

    # im not sure if there's a way to get typing tools to not complain about
    # the re-defined name here, it handles it for plain @property
    # but im not sure if that's hardcoded
    # see https://github.com/python/typing/discussions/1102

    @hybrid_property
    def _inst_radius(self) -> float:
        return abs(self.length) / 2

    @_inst_radius.expression
    def radius(cls) -> ColumnElement[float]:
        f1 = func.abs(cls.length, type_=Float())

        expr = f1 / 2

        # while we are here, check some Float[] / div type stuff
        if typing.TYPE_CHECKING:
            # EXPECTED_RE_TYPE: sqlalchemy.*Function\[builtins.float\*?\]
            reveal_type(f1)

            # EXPECTED_RE_TYPE: sqlalchemy.*ColumnElement\[builtins.float\*?\]
            reveal_type(expr)
        return expr


i1 = Interval(5, 10)
i2 = Interval(7, 12)

l1: int = i1.length
rd: float = i2.radius

expr1 = Interval.length.in_([5, 10])

expr2 = Interval.radius

expr3 = Interval.radius.in_([0.5, 5.2])


if typing.TYPE_CHECKING:
    # EXPECTED_RE_TYPE: builtins.int\*?
    reveal_type(i1.length)

    # EXPECTED_RE_TYPE: builtins.float\*?
    reveal_type(i2.radius)

    # EXPECTED_RE_TYPE: sqlalchemy.*.SQLCoreOperations\[builtins.int\*?\]
    reveal_type(Interval.length)

    # EXPECTED_RE_TYPE: sqlalchemy.*.SQLCoreOperations\[builtins.float\*?\]
    reveal_type(Interval.radius)

    # EXPECTED_RE_TYPE: sqlalchemy.*.BinaryExpression\[builtins.bool\*?\]
    reveal_type(expr1)

    # EXPECTED_RE_TYPE: sqlalchemy.*.SQLCoreOperations\[builtins.float\*?\]
    reveal_type(expr2)

    # EXPECTED_RE_TYPE: sqlalchemy.*.BinaryExpression\[builtins.bool\*?\]
    reveal_type(expr3)
