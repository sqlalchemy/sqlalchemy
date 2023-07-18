"""tests for #8847

we want to assert that SQLColumnExpression can be used to represent
all SQL expressions generically, across Core and ORM, without using
unions.

"""


from __future__ import annotations

from sqlalchemy import asc
from sqlalchemy import Column
from sqlalchemy import desc
from sqlalchemy import Integer
from sqlalchemy import literal
from sqlalchemy import MetaData
from sqlalchemy import select
from sqlalchemy import SQLColumnExpression
from sqlalchemy import String
from sqlalchemy import Table
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import Session


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

# EXPECTED_TYPE: SQLColumnExpression[bool]
reveal_type(e1)

stmt = select(e1)

# EXPECTED_TYPE: Select[Tuple[bool]]
reveal_type(stmt)

stmt = stmt.where(e1)


e2 = core_expr("hi")

# EXPECTED_TYPE: SQLColumnExpression[bool]
reveal_type(e2)

stmt = select(e2)

# EXPECTED_TYPE: Select[Tuple[bool]]
reveal_type(stmt)

stmt = stmt.where(e2)

stmt2 = select(User.id).order_by("email").group_by("email")
stmt2 = select(User.id).order_by("id", "email").group_by("email", "id")
stmt2 = (
    select(User.id).order_by(asc("id"), desc("email")).group_by("email", "id")
)
# EXPECTED_TYPE: Select[Tuple[int]]
reveal_type(stmt2)

stmt2 = select(User.id).order_by(User.id).group_by(User.email)
stmt2 = (
    select(User.id).order_by(User.id, User.email).group_by(User.email, User.id)
)
# EXPECTED_TYPE: Select[Tuple[int]]
reveal_type(stmt2)


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
# EXPECTED_TYPE: RowReturningQuery[Tuple[int]]
reveal_type(q1)

q1 = Session().query(User.id).order_by(User.id).group_by(User.email)
q1 = (
    Session()
    .query(User.id)
    .order_by(User.id, User.email)
    .group_by(User.email, User.id)
)
# EXPECTED_TYPE: RowReturningQuery[Tuple[int]]
reveal_type(q1)

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

# literal
# EXPECTED_TYPE: BindParameter[str]
reveal_type(literal("5"))
# EXPECTED_TYPE: BindParameter[str]
reveal_type(literal("5", None))
# EXPECTED_TYPE: BindParameter[int]
reveal_type(literal("123", Integer))
# EXPECTED_TYPE: BindParameter[int]
reveal_type(literal("123", Integer))
