from __future__ import annotations

from typing import Any
from typing import assert_type
from typing import TypeAlias

from sqlalchemy import MetaData
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped
from sqlalchemy.orm._orm_constructors import synonym
from sqlalchemy.orm.attributes import InstrumentedAttribute
from sqlalchemy.orm.decl_api import as_typed_table
from sqlalchemy.orm.properties import MappedColumn
from sqlalchemy.sql._annotated_cols import TypedColumns
from sqlalchemy.sql.schema import Column

T_A: TypeAlias = tuple[Any, ...]
meta = MetaData()


class Base(DeclarativeBase):
    pass


class A(Base):
    __tablename__ = "a"

    a: MappedColumn[int]
    b: MappedColumn[str]

    x: Mapped[str] = synonym("b")


class a_cols(A, TypedColumns):
    pass


assert_type(A.a, InstrumentedAttribute[int])
assert_type(A().a, int)
assert_type(A.x, InstrumentedAttribute[str])

assert_type(a_cols.a, InstrumentedAttribute[int])
assert_type(a_cols.b, InstrumentedAttribute[str])
assert_type(a_cols.x, InstrumentedAttribute[str])


def col_instance(arg: a_cols) -> None:
    assert_type(arg.a, Column[int])
    assert_type(arg.b, Column[str])
    assert_type(arg.x, str)


def test_as_typed_table() -> None:
    # plain class
    tbl = as_typed_table(A, a_cols)
    assert_type(tbl.c.a, Column[int])
    assert_type(tbl.c.b, Column[str])
    assert_type(tbl.c.metadata, MetaData)  # not great but inevitable

    # class with __typed_cols__
    class X(Base):
        __tablename__ = "b"

        x: MappedColumn[int]
        y: MappedColumn[str]
        __typed_cols__: x_cols

    class x_cols(X, TypedColumns):
        pass

    tblX = as_typed_table(X)
    assert_type(tblX.c.x, Column[int])
    assert_type(tblX.c.y, Column[str])
