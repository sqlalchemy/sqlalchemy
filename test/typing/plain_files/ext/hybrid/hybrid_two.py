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

    # old way - chain decorators + modifiers

    @hybrid_property
    def _inst_radius(self) -> float:
        return abs(self.length) / 2

    @_inst_radius.expression
    def old_radius(cls) -> ColumnElement[float]:
        f1 = func.abs(cls.length, type_=Float())

        expr = f1 / 2

        # while we are here, check some Float[] / div type stuff
        if typing.TYPE_CHECKING:
            # EXPECTED_RE_TYPE: sqlalchemy.*Function\[builtins.float\*?\]
            reveal_type(f1)

            # EXPECTED_RE_TYPE: sqlalchemy.*ColumnElement\[builtins.float\*?\]
            reveal_type(expr)
        return expr

    # new way - use the original decorator with inplace

    @hybrid_property
    def new_radius(self) -> float:
        return abs(self.length) / 2

    @new_radius.inplace.expression
    @classmethod
    def _new_radius_expr(cls) -> ColumnElement[float]:
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
rdo: float = i2.old_radius
rdn: float = i2.new_radius

expr1 = Interval.length.in_([5, 10])

expr2o = Interval.old_radius

expr2n = Interval.new_radius

expr3o = Interval.old_radius.in_([0.5, 5.2])
expr3n = Interval.new_radius.in_([0.5, 5.2])


if typing.TYPE_CHECKING:
    # EXPECTED_RE_TYPE: builtins.int\*?
    reveal_type(i1.length)

    # EXPECTED_RE_TYPE: builtins.float\*?
    reveal_type(i2.old_radius)

    # EXPECTED_RE_TYPE: builtins.float\*?
    reveal_type(i2.new_radius)

    # EXPECTED_RE_TYPE: sqlalchemy.*._HybridClassLevelAccessor\[builtins.int\*?\]
    reveal_type(Interval.length)

    # EXPECTED_RE_TYPE: sqlalchemy.*._HybridClassLevelAccessor\[builtins.float\*?\]
    reveal_type(Interval.old_radius)

    # EXPECTED_RE_TYPE: sqlalchemy.*._HybridClassLevelAccessor\[builtins.float\*?\]
    reveal_type(Interval.new_radius)

    # EXPECTED_RE_TYPE: sqlalchemy.*.BinaryExpression\[builtins.bool\*?\]
    reveal_type(expr1)

    # EXPECTED_RE_TYPE: sqlalchemy.*._HybridClassLevelAccessor\[builtins.float\*?\]
    reveal_type(expr2o)

    # EXPECTED_RE_TYPE: sqlalchemy.*._HybridClassLevelAccessor\[builtins.float\*?\]
    reveal_type(expr2n)

    # EXPECTED_RE_TYPE: sqlalchemy.*.BinaryExpression\[builtins.bool\*?\]
    reveal_type(expr3o)

    # EXPECTED_RE_TYPE: sqlalchemy.*.BinaryExpression\[builtins.bool\*?\]
    reveal_type(expr3n)

# test #9268


class Foo(Base):
    val: bool

    def needs_update_getter(self) -> bool:
        return self.val

    def needs_update_setter(self, value: bool) -> None:
        self.val = value

    needs_update: hybrid_property[bool] = hybrid_property(
        needs_update_getter,
        needs_update_setter,
    )
