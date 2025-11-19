"""tests for #8847

we want to assert that SQLColumnExpression can be used to represent
all SQL expressions generically, across Core and ORM, without using
unions.

"""

from __future__ import annotations

from typing import assert_type
from typing import Never
from typing import Unpack

from sqlalchemy import asc
from sqlalchemy import Column
from sqlalchemy import column
from sqlalchemy import ColumnElement
from sqlalchemy import desc
from sqlalchemy import except_
from sqlalchemy import except_all
from sqlalchemy import Integer
from sqlalchemy import intersect
from sqlalchemy import intersect_all
from sqlalchemy import literal
from sqlalchemy import MetaData
from sqlalchemy import Select
from sqlalchemy import select
from sqlalchemy import SQLColumnExpression
from sqlalchemy import String
from sqlalchemy import Table
from sqlalchemy import union
from sqlalchemy import union_all
from sqlalchemy.engine import Result
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import Session
from sqlalchemy.orm.query import RowReturningQuery
from sqlalchemy.sql.expression import BindParameter
from sqlalchemy.sql.expression import CompoundSelect


class Base(DeclarativeBase):
    pass


class User(Base):
    __tablename__ = "a"

    id: Mapped[int] = mapped_column(primary_key=True)
    email: Mapped[str]


user_table = Table(
    "user_table", MetaData(), Column("id", Integer), Column("email", String)
)


def receives_str_col_expr(expr: SQLColumnExpression[str]) -> None:
    pass


def receives_bool_col_expr(expr: SQLColumnExpression[bool]) -> None:
    pass


def orm_expr(email: str) -> SQLColumnExpression[bool]:
    return User.email == email


def core_expr(email: str) -> SQLColumnExpression[bool]:
    email_col: Column[str] = user_table.c.email
    return email_col == email


e1 = orm_expr("hi")

assert_type(e1, SQLColumnExpression[bool])

stmt = select(e1)

assert_type(stmt, Select[bool])

stmt = stmt.where(e1)


e2 = core_expr("hi")

assert_type(e2, SQLColumnExpression[bool])

stmt = select(e2)

assert_type(stmt, Select[bool])

stmt = stmt.where(e2)

stmt2 = select(User.id).order_by("email").group_by("email")
stmt2 = select(User.id).order_by("id", "email").group_by("email", "id")
stmt2 = (
    select(User.id).order_by(asc("id"), desc("email")).group_by("email", "id")
)
assert_type(stmt2, Select[int])

stmt2 = select(User.id).order_by(User.id).group_by(User.email)
stmt2 = (
    select(User.id).order_by(User.id, User.email).group_by(User.email, User.id)
)
assert_type(stmt2, Select[int])

stmt3 = select(User.id).exists().select()

assert_type(stmt3, Select[bool])


receives_str_col_expr(User.email)
receives_str_col_expr(User.email + "some expr")
receives_str_col_expr(User.email.label("x"))
receives_str_col_expr(User.email.label("x"))

receives_bool_col_expr(e1)
receives_bool_col_expr(e1.label("x"))
receives_bool_col_expr(User.email == "x")

receives_bool_col_expr(e2)
receives_bool_col_expr(e2.label("x"))
receives_bool_col_expr(user_table.c.email == "x")


# query

q1 = Session().query(User.id).order_by("email").group_by("email")
q1 = Session().query(User.id).order_by("id", "email").group_by("email", "id")
assert_type(q1, RowReturningQuery[int])

q1 = Session().query(User.id).order_by(User.id).group_by(User.email)
q1 = (
    Session()
    .query(User.id)
    .order_by(User.id, User.email)
    .group_by(User.email, User.id)
)
assert_type(q1, RowReturningQuery[int])

# test 9174
s9174_1 = select(User).with_for_update(of=User)
s9174_2 = select(User).with_for_update(of=User.id)
s9174_3 = select(User).with_for_update(of=[User.id, User.email])
s9174_4 = select(user_table).with_for_update(of=user_table)
s9174_5 = select(user_table).with_for_update(of=user_table.c.id)
s9174_6 = select(user_table).with_for_update(
    of=[user_table.c.id, user_table.c.email]
)

# with_for_update but for query
session = Session()
user = session.query(User).with_for_update(of=User)
user = session.query(User).with_for_update(of=User.id)
user = session.query(User).with_for_update(of=[User.id, User.email])
user = session.query(user_table).with_for_update(of=user_table)
user = session.query(user_table).with_for_update(of=user_table.c.id)
user = session.query(user_table).with_for_update(
    of=[user_table.c.id, user_table.c.email]
)

# test 12730 - tuples of DeclarativeBase classes


class A(Base):
    __tablename__ = "a"
    id: Mapped[int] = mapped_column(primary_key=True)


class B(Base):
    __tablename__ = "b"
    id: Mapped[int] = mapped_column(primary_key=True)


s12730_1 = select(A, B).with_for_update(of=(A, B))
s12730_2 = select(A, B).with_for_update(of=(A,))
s12730_3 = select(A, B).with_for_update(of=[A, B])
s12730_4 = session.query(A, B).with_for_update(of=(A, B))

# literal
assert_type(literal("5"), BindParameter[str])
assert_type(literal("5", None), BindParameter[str])
assert_type(literal("123", Integer), BindParameter[int])
assert_type(literal("123", Integer), BindParameter[int])


# hashable (issue #10353):

mydict = {
    Column("q"): "q",
    Column("q").desc(): "q",
    User.id: "q",
    literal("5"): "q",
    column("q"): "q",
}

# compound selects (issue #11922):

str_col = ColumnElement[str]()
int_col = ColumnElement[int]()

first_stmt = select(str_col, int_col)
second_stmt = select(str_col, int_col)
third_stmt = select(int_col, str_col)

assert_type(union(first_stmt, second_stmt), CompoundSelect[str, int])
assert_type(union_all(first_stmt, second_stmt), CompoundSelect[str, int])
assert_type(except_(first_stmt, second_stmt), CompoundSelect[str, int])
assert_type(except_all(first_stmt, second_stmt), CompoundSelect[str, int])
assert_type(intersect(first_stmt, second_stmt), CompoundSelect[str, int])
assert_type(intersect_all(first_stmt, second_stmt), CompoundSelect[str, int])

assert_type(
    Session().execute(union(first_stmt, second_stmt)), Result[str, int]
)
assert_type(
    Session().execute(union_all(first_stmt, second_stmt)), Result[str, int]
)

assert_type(first_stmt.union(second_stmt), CompoundSelect[str, int])
assert_type(first_stmt.union_all(second_stmt), CompoundSelect[str, int])
assert_type(first_stmt.except_(second_stmt), CompoundSelect[str, int])
assert_type(first_stmt.except_all(second_stmt), CompoundSelect[str, int])
assert_type(first_stmt.intersect(second_stmt), CompoundSelect[str, int])
assert_type(first_stmt.intersect_all(second_stmt), CompoundSelect[str, int])

# TODO: the following do not error because _SelectStatementForCompoundArgument
# includes untyped elements so the type checker falls back on them when
# the type does not match. Also for the standalone functions mypy
# looses the plot and returns a random type back. See TODO in the
# overloads

assert_type(
    union(first_stmt, third_stmt), CompoundSelect[Unpack[tuple[Never, ...]]]
)
assert_type(
    union_all(first_stmt, third_stmt),
    CompoundSelect[Unpack[tuple[Never, ...]]],
)
assert_type(
    except_(first_stmt, third_stmt), CompoundSelect[Unpack[tuple[Never, ...]]]
)
assert_type(
    except_all(first_stmt, third_stmt),
    CompoundSelect[Unpack[tuple[Never, ...]]],
)
assert_type(
    intersect(first_stmt, third_stmt),
    CompoundSelect[Unpack[tuple[Never, ...]]],
)
assert_type(
    intersect_all(first_stmt, third_stmt),
    CompoundSelect[Unpack[tuple[Never, ...]]],
)

assert_type(first_stmt.union(third_stmt), CompoundSelect[str, int])
assert_type(first_stmt.union_all(third_stmt), CompoundSelect[str, int])
assert_type(first_stmt.except_(third_stmt), CompoundSelect[str, int])
assert_type(first_stmt.except_all(third_stmt), CompoundSelect[str, int])
assert_type(first_stmt.intersect(third_stmt), CompoundSelect[str, int])
assert_type(first_stmt.intersect_all(third_stmt), CompoundSelect[str, int])
