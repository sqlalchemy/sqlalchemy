from __future__ import annotations

from string.templatelib import Template
from typing import Any
from typing import assert_type
from typing import Optional
from typing import Unpack

from sqlalchemy import Column
from sqlalchemy import column
from sqlalchemy import create_engine
from sqlalchemy import delete
from sqlalchemy import exists
from sqlalchemy import func
from sqlalchemy import insert
from sqlalchemy import Integer
from sqlalchemy import join
from sqlalchemy import MetaData
from sqlalchemy import Select
from sqlalchemy import select
from sqlalchemy import String
from sqlalchemy import Table
from sqlalchemy import text
from sqlalchemy import tstring
from sqlalchemy import update
from sqlalchemy.engine import Result
from sqlalchemy.engine.row import Row
from sqlalchemy.orm import aliased
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import Session
from sqlalchemy.orm.query import Query
from sqlalchemy.orm.query import RowReturningQuery
from sqlalchemy.sql.dml import ReturningInsert
from sqlalchemy.sql.elements import KeyedColumnElement
from sqlalchemy.sql.elements import TString
from sqlalchemy.sql.expression import FromClause
from sqlalchemy.sql.expression import TextClause
from sqlalchemy.sql.selectable import ScalarSelect
from sqlalchemy.sql.selectable import TextualSelect


class Base(DeclarativeBase):
    pass


class User(Base):
    __tablename__ = "user"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str]
    data: Mapped[str]


user_table = Table(
    "user",
    MetaData(),
    Column("id", Integer, primary_key=True),
    Column("name", String, primary_key=True),
)

session = Session()

e = create_engine("sqlite://")
connection = e.connect()


def t_select_1() -> None:
    stmt = select(User.id, User.name).filter(User.id == 5)

    assert_type(stmt, Select[int, str])

    result = session.execute(stmt)

    assert_type(result, Result[int, str])


def t_select_2() -> None:
    stmt = (
        select(User)
        .filter(User.id == 5)
        .limit(1)
        .offset(3)
        .offset(None)
        .limit(None)
        .limit(User.id)
        .offset(User.id)
        .fetch(1)
        .fetch(None)
        .fetch(User.id)
    )

    assert_type(stmt, Select[User])

    result = session.execute(stmt)

    assert_type(result, Result[User])


def t_select_3() -> None:
    ua = aliased(User)

    # this will fail at runtime, but as we at the moment see aliased(_T)
    # as _T, typing tools see the constructor as fine.
    # this line would ideally have a typing error but we'd need the ability
    # for aliased() to return some namespace of User that's not User.
    # AsAliased superclass type was tested for this but it had its own
    # awkwardnesses that aren't really worth it
    ua(id=1, name="foo")

    assert_type(ua, type[User])

    stmt = select(ua.id, ua.name).filter(User.id == 5)

    assert_type(stmt, Select[int, str])

    result = session.execute(stmt)

    assert_type(result, Result[int, str])


def t_select_4() -> None:
    ua = aliased(User)
    stmt = select(ua, User).filter(User.id == 5)

    assert_type(stmt, Select[User, User])

    result = session.execute(stmt)

    assert_type(result, Result[User, User])


def t_legacy_query_single_entity() -> None:
    q1 = session.query(User).filter(User.id == 5)

    assert_type(q1, Query[User])

    assert_type(q1.get(5), Optional[User])

    assert_type(q1.one(), User)

    assert_type(q1.all(), list[User])

    # mypy switches to builtins.list for some reason here
    assert_type(q1.only_return_tuples(True).all(), list[Row[User]])

    assert_type(q1.tuples().all(), list[tuple[User]])


def t_legacy_query_cols_1() -> None:
    q1 = session.query(User.id, User.name).filter(User.id == 5)

    assert_type(q1, RowReturningQuery[int, str])

    assert_type(q1.one(), Row[int, str])

    r1 = q1.one()

    x, y = r1

    assert_type(x, int)

    assert_type(y, str)


def t_legacy_query_cols_tupleq_1() -> None:
    q1 = session.query(User.id, User.name).filter(User.id == 5)

    assert_type(q1, RowReturningQuery[int, str])

    q2 = q1.tuples()

    assert_type(q2.one(), tuple[int, str])

    r1 = q2.one()

    x, y = r1

    assert_type(x, int)

    assert_type(y, str)


def t_legacy_query_cols_1_with_entities() -> None:
    q1 = session.query(User).filter(User.id == 5)

    assert_type(q1, Query[User])

    q2 = q1.with_entities(User.id, User.name)

    assert_type(q2, RowReturningQuery[int, str])

    assert_type(q2.one(), Row[int, str])

    r1 = q2.one()

    x, y = r1

    assert_type(x, int)

    assert_type(y, str)


def t_select_with_only_cols() -> None:
    q1 = select(User).where(User.id == 5)

    assert_type(q1, Select[User])

    q2 = q1.with_only_columns(User.id, User.name)

    assert_type(q2, Select[int, str])

    row = connection.execute(q2).one()

    assert_type(row, Row[int, str])

    x, y = row

    assert_type(x, int)

    assert_type(y, str)


def t_legacy_query_cols_2() -> None:
    a1 = aliased(User)
    q1 = session.query(User, a1, User.name).filter(User.id == 5)

    assert_type(q1, RowReturningQuery[User, User, str])

    assert_type(q1.one(), Row[User, User, str])

    r1 = q1.one()

    x, y, z = r1

    assert_type(x, User)

    assert_type(y, User)

    assert_type(z, str)


def t_legacy_query_cols_2_with_entities() -> None:
    q1 = session.query(User)

    assert_type(q1, Query[User])

    a1 = aliased(User)
    q2 = q1.with_entities(User, a1, User.name).filter(User.id == 5)

    assert_type(q2, RowReturningQuery[User, User, str])

    assert_type(q2.one(), Row[User, User, str])

    r1 = q2.one()

    x, y, z = r1

    assert_type(x, User)

    assert_type(y, User)

    assert_type(z, str)


def t_select_add_col_loses_type() -> None:
    q1 = select(User.id, User.name).filter(User.id == 5)

    q2 = q1.add_columns(User.data)

    # note this should not match Select
    assert_type(q2, Select[Unpack[tuple[Any, ...]]])


def t_legacy_query_add_col_loses_type() -> None:
    q1 = session.query(User.id, User.name).filter(User.id == 5)

    q2 = q1.add_columns(User.data)

    # this should match only Any
    assert_type(q2, Query[Any])

    ua = aliased(User)
    q3 = q1.add_entity(ua)

    assert_type(q3, Query[Any])


def t_legacy_query_scalar_subquery() -> None:
    """scalar subquery should receive the type if first element is a
    column only"""
    q1 = session.query(User.id)

    q2 = q1.scalar_subquery()

    # this should be int but mypy can't see it due to the
    # overload that tries to match an entity.
    assert_type(q2, ScalarSelect[Any])

    q3 = session.query(User)

    q4 = q3.scalar_subquery()

    assert_type(q4, ScalarSelect[Any])

    q5 = session.query(User, User.name)

    q6 = q5.scalar_subquery()

    assert_type(q6, ScalarSelect[Any])

    # try to simulate the problem with select()
    q7 = session.query(User).only_return_tuples(True)
    q8 = q7.scalar_subquery()

    assert_type(q8, ScalarSelect[Any])


def t_select_scalar_subquery() -> None:
    """scalar subquery should receive the type if first element is a
    column only"""
    s1 = select(User.id)
    s2 = s1.scalar_subquery()

    # this should be int but mypy can't see it due to the
    # overload that tries to match an entity.
    assert_type(s2, ScalarSelect[Any])

    s3 = select(User)
    s4 = s3.scalar_subquery()

    # it's more important that mypy doesn't get a false positive of
    # 'User' here
    assert_type(s4, ScalarSelect[Any])


def t_select_w_core_selectables() -> None:
    """things that come from .c. or are FromClause objects currently are not
    typed.  Make sure we are still getting Select at least.

    """
    s1 = select(User.id, User.name).subquery()

    assert_type(s1.c.name, KeyedColumnElement[Any])

    s2 = select(User.id, s1.c.name)

    # this one unfortunately is not working in mypy.
    # pylance gets the correct type
    #   EXPECTED_TYPE: Select[tuple[int, Any]]
    # when experimenting with having a separate TypedSelect class for typing,
    # mypy would downgrade to Any rather than picking the basemost type.
    # with typing integrated into Select etc. we can at least get a Select
    # object back.
    assert_type(s2, Select[Unpack[tuple[Any, ...]]])

    # so a fully explicit type may be given
    s2_typed: Select[tuple[int, str]] = select(User.id, s1.c.name)

    assert_type(s2_typed, Select[tuple[int, str]])

    # plain FromClause etc we at least get Select
    s3 = select(s1)

    assert_type(s3, Select[Unpack[tuple[Any, ...]]])

    t1 = User.__table__
    assert t1 is not None

    assert_type(t1, FromClause)

    s4 = select(t1)

    assert_type(s4, Select[Unpack[tuple[Any, ...]]])


def t_dml_insert() -> None:
    s1 = insert(User).returning(User.id, User.name)

    r1 = session.execute(s1)

    assert_type(r1, Result[int, str])

    s2 = insert(User).returning(User)

    r2 = session.execute(s2)

    assert_type(r2, Result[User])

    s3 = insert(User).returning(func.foo(), column("q"))

    assert_type(s3, ReturningInsert[Unpack[tuple[Any, ...]]])

    r3 = session.execute(s3)

    assert_type(r3, Result[Unpack[tuple[Any, ...]]])


def t_dml_bare_insert() -> None:
    s1 = insert(User)
    r1 = session.execute(s1)
    assert_type(r1, Result[Unpack[tuple[Any, ...]]])


def t_dml_bare_update() -> None:
    s1 = update(User)
    r1 = session.execute(s1)
    assert_type(r1, Result[Unpack[tuple[Any, ...]]])


def t_dml_update_with_values() -> None:
    s1 = update(User).values({User.id: 123, User.data: "value"})
    r1 = session.execute(s1)
    assert_type(r1, Result[Unpack[tuple[Any, ...]]])


def t_dml_bare_delete() -> None:
    s1 = delete(User)
    r1 = session.execute(s1)
    assert_type(r1, Result[Unpack[tuple[Any, ...]]])


def t_dml_update() -> None:
    s1 = update(User).returning(User.id, User.name)

    r1 = session.execute(s1)

    assert_type(r1, Result[int, str])


def t_dml_delete() -> None:
    s1 = delete(User).returning(User.id, User.name)

    r1 = session.execute(s1)

    assert_type(r1, Result[int, str])


def t_from_statement_text() -> None:
    t = text("select * from user")

    assert_type(t, TextClause)

    select(User).from_statement(t)

    session.query(User).from_statement(t)

    ts = text("select * from user").columns(User.id, User.name)

    assert_type(ts, TextualSelect)

    select(User).from_statement(ts)

    session.query(User).from_statement(ts)

    ts2 = text("select * from user").columns(
        user_table.c.id, user_table.c.name
    )

    assert_type(ts2, TextualSelect)

    select(User).from_statement(ts2)

    session.query(User).from_statement(ts2)


def t_from_statement_tstring(templ: Template) -> None:
    t = tstring(templ)

    assert_type(t, TString)

    select(User).from_statement(t)

    session.query(User).from_statement(t)

    ts = tstring(templ).columns(User.id, User.name)

    assert_type(ts, TextualSelect)

    select(User).from_statement(ts)

    session.query(User).from_statement(ts)

    ts2 = tstring(templ).columns(user_table.c.id, user_table.c.name)

    assert_type(ts2, TextualSelect)

    select(User).from_statement(ts2)

    session.query(User).from_statement(ts2)


def t_aliased_fromclause() -> None:
    a1 = aliased(User, user_table)

    a2 = aliased(User, user_table.alias())

    a3 = aliased(User, join(user_table, user_table.alias()))

    a4 = aliased(user_table)

    assert_type(a1, type[User])

    assert_type(a2, type[User])

    assert_type(a3, type[User])

    assert_type(a4, FromClause)


def test_select_from() -> None:
    select(1).select_from(User).exists()
    exists(1).select_from(User).select()


def t_legacy_query_getitem() -> None:
    q1 = session.query(User).filter(User.id == 5)
    assert_type(q1[0], User)
    assert_type(q1[1:3], list[User])
