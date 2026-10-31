"""SQL expression constructors that infer their python type from a
SQL type argument may be assigned to a wider declared type, such as
one that adds ``None`` or a supertype such as ``float`` for ``int``.
#13654

"""

from typing import assert_type

from sqlalchemy import ARRAY
from sqlalchemy import bindparam
from sqlalchemy import cast
from sqlalchemy import Column
from sqlalchemy import column
from sqlalchemy import ColumnElement
from sqlalchemy import Float
from sqlalchemy import func
from sqlalchemy import Integer
from sqlalchemy import label
from sqlalchemy import literal
from sqlalchemy import literal_column
from sqlalchemy import String
from sqlalchemy import try_cast
from sqlalchemy import type_coerce
from sqlalchemy.dialects import postgresql
from sqlalchemy.sql.elements import BinaryExpression
from sqlalchemy.sql.elements import BindParameter
from sqlalchemy.sql.elements import Cast
from sqlalchemy.sql.elements import ColumnClause
from sqlalchemy.sql.elements import Label
from sqlalchemy.sql.elements import TryCast
from sqlalchemy.sql.elements import TypeCoerce
from sqlalchemy.sql.functions import Function

x = column("x", Integer)
s = column("s", String)


def test_inferred_type() -> None:
    """with no declared type, the narrow type is still inferred"""

    assert_type(cast(s, Integer), Cast[int])
    assert_type(try_cast(s, Integer), TryCast[int])
    assert_type(type_coerce(s, Integer), TypeCoerce[int])
    assert_type(bindparam("p", type_=Integer), BindParameter[int])
    assert_type(literal(5, Integer), BindParameter[int])
    assert_type(column("c", Integer), ColumnClause[int])
    assert_type(literal_column("c", Integer), ColumnClause[int])
    assert_type(label("l", x, type_=Integer), Label[int])
    assert_type(s.cast(Integer), Cast[int])
    assert_type(x.op("+", return_type=Integer)(5), BinaryExpression[int])
    assert_type(func.foo(x, type_=Integer), Function[int])
    assert_type(Function("foo", x, type_=Integer), Function[int])
    assert_type(ARRAY(Integer), ARRAY[int])
    assert_type(postgresql.ARRAY(Integer), postgresql.ARRAY[int])
    assert_type(Column(Integer), Column[int])
    assert_type(Column("c", Integer), Column[int])
    assert_type(Column("c", type_=Integer), Column[int])


def test_declared_wider_type() -> None:
    """the narrow type may be widened by the declared type"""

    e1: Cast[int | None] = cast(s, Integer)
    e2: TryCast[int | None] = try_cast(s, Integer)
    e3: TypeCoerce[int | None] = type_coerce(s, Integer)
    e4: BindParameter[int | None] = bindparam("p", type_=Integer)
    e5: BindParameter[int | None] = literal(5, Integer)
    e6: ColumnClause[int | None] = column("c", Integer)
    e7: ColumnClause[int | None] = literal_column("c", Integer)
    e8: Label[int | None] = label("l", x, type_=Integer)
    e9: Cast[int | None] = s.cast(Integer)
    e10: Function[int | None] = func.foo(x, type_=Integer)
    e11: Function[int | None] = Function("foo", x, type_=Integer)
    e12: ARRAY[int | None] = ARRAY(Integer)
    e13: postgresql.ARRAY[int | None] = postgresql.ARRAY(Integer)
    e14: Column[int | None] = Column(Integer)
    e15: Column[int | None] = Column("c", Integer)
    e16: Column[int | None] = Column("c", type_=Integer)

    # same through the covariant ColumnElement
    c1: ColumnElement[int | None] = cast(s, Integer)
    c2: ColumnElement[int | None] = type_coerce(s, Integer)
    c3: ColumnElement[int | None] = bindparam("p", type_=Integer)
    c4: ColumnElement[int | None] = column("c", Integer)

    # type instance as well as type class
    i1: Cast[int | None] = cast(s, Integer())
    i2: ColumnClause[int | None] = column("c", Integer())
    i3: Column[int | None] = Column(Integer())

    print(e1, e2, e3, e4, e5, e6, e7, e8, e9, e10, e11, e12, e13)
    print(e14, e15, e16)
    print(c1, c2, c3, c4, i1, i2, i3)


def test_declared_supertype() -> None:
    """int is accepted where float is expected, so an Integer based
    expression may be declared as float"""

    f1: ColumnClause[float] = literal_column("c", Integer)
    f2: ColumnClause[float] = column("c", Integer)
    f3: Cast[float] = cast(s, Integer)
    f4: BindParameter[float] = bindparam("p", type_=Integer)
    f5: ColumnElement[float] = cast(s, Integer)
    f6: ColumnClause[float | None] = column("c", Integer)
    f7: Column[float] = Column(Integer)

    print(f1, f2, f3, f4, f5, f6, f7)


def test_declared_incompatible_type() -> None:
    """a declared type that is not wider is still rejected"""

    # EXPECTED_MYPY: Argument 2 to "cast" has incompatible type "Integer"
    # EXPECTED_MYPY: Following member(s) of "Integer" have conflicts:
    # EXPECTED_MYPY_RE: \s+_sa_typing_python_type: expected "str", got "int"
    e1: Cast[str] = cast(s, Integer())

    # EXPECTED_MYPY: Argument "type_" to "bindparam" has incompatible type "Integer"  # noqa: E501
    # EXPECTED_MYPY: Following member(s) of "Integer" have conflicts:
    # EXPECTED_MYPY_RE: \s+_sa_typing_python_type: expected "str", got "int"
    e2: BindParameter[str] = bindparam("p", type_=Integer())

    # EXPECTED_MYPY: Argument 2 to "column" has incompatible type "Integer"
    # EXPECTED_MYPY: Following member(s) of "Integer" have conflicts:
    # EXPECTED_MYPY_RE: \s+_sa_typing_python_type: expected "str", got "int"
    e3: ColumnClause[str] = column("c", Integer())

    # float is not accepted where int is expected
    # EXPECTED_MYPY: Argument 2 to "column" has incompatible type "Float[float]"  # noqa: E501
    e4: ColumnClause[int] = column("c", Float())

    # EXPECTED_MYPY: Argument 1 to "Column" has incompatible type "Integer"
    # EXPECTED_MYPY: Following member(s) of "Integer" have conflicts:
    # EXPECTED_MYPY_RE: \s+_sa_typing_python_type: expected "str", got "int"
    e5: Column[str] = Column(Integer())

    print(e1, e2, e3, e4, e5)
