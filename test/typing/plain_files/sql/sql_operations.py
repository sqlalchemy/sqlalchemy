from decimal import Decimal
import typing
from typing import Any
from typing import assert_type

from sqlalchemy import and_
from sqlalchemy import Boolean
from sqlalchemy import cast
from sqlalchemy import column
from sqlalchemy import DateTime
from sqlalchemy import false
from sqlalchemy import Float
from sqlalchemy import func
from sqlalchemy import Integer
from sqlalchemy import or_
from sqlalchemy import select
from sqlalchemy import String
from sqlalchemy import true
from sqlalchemy.sql.elements import ColumnElement
from sqlalchemy.sql.elements import UnaryExpression
from sqlalchemy.sql.expression import BinaryExpression
from sqlalchemy.sql.expression import ColumnClause


# builtin.pyi stubs define object.__eq__() as returning bool,  which
# can't be overridden (it's final).  So for us to type `__eq__()` and
# `__ne__()`, we have to use type: ignore[override].  Test if does this mean
# the typing tools don't know the type, or if they just ignore the error.
# (it's fortunately the former)
expr1 = column("x", Integer) == 10

c1 = column("a", String)

c2 = column("a", Integer)

expr2 = c2.in_([1, 2, 3])

expr2_set = c2.in_({1, 2, 3})

expr2_gen = c2.in_((x for x in (1, 2, 3)))

nexpr2 = c2.not_in([1, 2, 3])

nexpr2_set = c2.not_in({1, 2, 3})

nexpr2_gen = c2.not_in((x for x in (1, 2, 3)))

short_cir1 = and_(True, c2 == 5)
short_cir2 = or_(False, c2 == 5)

short_cir3 = and_(true(), c2 == 5)
short_cir4 = or_(false(), c2 == 5)

# EXPECTED_MYPY: Missing positional argument "initial_clause" in call to "and_"
no_empty_1 = and_()

# EXPECTED_MYPY: Missing positional argument "initial_clause" in call to "or_"
no_empty_2 = or_()

expr3 = c2 / 5

expr4 = -c2

expr5 = ~(c2 == 5)

q = column("q", Boolean)
expr6 = ~q

expr7 = c1 + "x"

expr8 = c2 + 10

stmt = select(column("q")).where(lambda: column("g") > 5).where(c2 == 5)

expr9 = c1.bool_op("@@")(func.to_tsquery("some & query"))


def test_issue_9418() -> None:
    and_(c1.is_(q))
    and_(c1.is_not(q))
    and_(c1.isnot(q))
    and_(c1.not_in(["x"]))
    and_(c1.notin_(["x"]))
    and_(c1.not_like("x"))
    and_(c1.notlike("x"))
    and_(c1.not_ilike("x"))
    and_(c1.notilike("x"))


def test_issue_9451() -> None:
    # issue #9451
    c1.cast(Integer)
    c1.cast(Float)
    c1.op("foobar")("operand").cast(DateTime)
    cast(c1, Float)
    cast(c1.op("foobar")("operand"), DateTime)


def test_issue_9650_char() -> None:
    and_(c1.contains("x"))
    and_(c1.startswith("x"))
    and_(c1.endswith("x"))
    and_(c1.icontains("x"))
    and_(c1.istartswith("x"))
    and_(c1.iendswith("x"))


def test_issue_9650_bitwise() -> None:
    assert_type(c2.bitwise_and(5), BinaryExpression[Any])
    assert_type(c2.bitwise_or(5), BinaryExpression[Any])
    assert_type(c2.bitwise_xor(5), BinaryExpression[Any])
    assert_type(c2.bitwise_not(), UnaryExpression[int])
    assert_type(c2.bitwise_lshift(5), BinaryExpression[Any])
    assert_type(c2.bitwise_rshift(5), BinaryExpression[Any])
    assert_type(c2 << 5, ColumnElement[int])
    assert_type(c2 >> 5, ColumnElement[int])


if typing.TYPE_CHECKING:
    # as far as if this is ColumnElement, BinaryElement, SQLCoreOperations,
    # that might change.  main thing is it's SomeSQLColThing[bool] and
    # not 'bool' or 'Any'.
    assert_type(expr1, ColumnElement[bool])

    assert_type(c1, ColumnClause[str])

    assert_type(c2, ColumnClause[int])

    assert_type(expr2, BinaryExpression[bool])

    assert_type(expr3, ColumnElement[float | Decimal])

    assert_type(expr4, UnaryExpression[int])

    assert_type(expr5, ColumnElement[bool])

    assert_type(expr6, ColumnElement[bool])

    assert_type(expr7, ColumnElement[str])

    assert_type(expr8, ColumnElement[int])

    assert_type(expr9, BinaryExpression[bool])
