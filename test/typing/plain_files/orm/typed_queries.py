from __future__ import annotations

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
from sqlalchemy import update
from sqlalchemy.orm import aliased
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import Session


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

    # EXPECTED_TYPE: Select[int, str]
    reveal_type(stmt)

    result = session.execute(stmt)

    # EXPECTED_TYPE: .*Result[int, str].*
    reveal_type(result)


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

    # EXPECTED_TYPE: Select[User]
    reveal_type(stmt)

    result = session.execute(stmt)

    # EXPECTED_TYPE: .*Result[User].*
    reveal_type(result)


def t_select_3() -> None:
    ua = aliased(User)

    # this will fail at runtime, but as we at the moment see aliased(_T)
    # as _T, typing tools see the constructor as fine.
    # this line would ideally have a typing error but we'd need the ability
    # for aliased() to return some namespace of User that's not User.
    # AsAliased superclass type was tested for this but it had its own
    # awkwardnesses that aren't really worth it
    ua(id=1, name="foo")

    # EXPECTED_TYPE: type[User]
    reveal_type(ua)

    stmt = select(ua.id, ua.name).filter(User.id == 5)

    # EXPECTED_TYPE: Select[int, str]
    reveal_type(stmt)

    result = session.execute(stmt)

    # EXPECTED_TYPE: .*Result[int, str].*
    reveal_type(result)


def t_select_4() -> None:
    ua = aliased(User)
    stmt = select(ua, User).filter(User.id == 5)

    # EXPECTED_TYPE: Select[User, User]
    reveal_type(stmt)

    result = session.execute(stmt)

    # EXPECTED_TYPE: Result[User, User]
    reveal_type(result)


def t_legacy_query_single_entity() -> None:
    q1 = session.query(User).filter(User.id == 5)

    # EXPECTED_TYPE: Query[User]
    reveal_type(q1)

    # EXPECTED_TYPE: User
    reveal_type(q1.one())

    # EXPECTED_TYPE: list[User]
    reveal_type(q1.all())

    # mypy switches to builtins.list for some reason here
    # EXPECTED_TYPE: builtins.list[tuple[User, fallback=Row[User]]]
    reveal_type(q1.only_return_tuples(True).all())

    # EXPECTED_TYPE: list[tuple[User]]
    reveal_type(q1.tuples().all())


def t_legacy_query_cols_1() -> None:
    q1 = session.query(User.id, User.name).filter(User.id == 5)

    # EXPECTED_TYPE: RowReturningQuery[int, str]
    reveal_type(q1)

    # EXPECTED_TYPE: .*Row[int, str].*
    reveal_type(q1.one())

    r1 = q1.one()

    x, y = r1

    # EXPECTED_TYPE: int
    reveal_type(x)

    # EXPECTED_TYPE: str
    reveal_type(y)


def t_legacy_query_cols_tupleq_1() -> None:
    q1 = session.query(User.id, User.name).filter(User.id == 5)

    # EXPECTED_TYPE: RowReturningQuery[int, str]
    reveal_type(q1)

    q2 = q1.tuples()

    # EXPECTED_TYPE: tuple[int, str]
    reveal_type(q2.one())

    r1 = q2.one()

    x, y = r1

    # EXPECTED_TYPE: int
    reveal_type(x)

    # EXPECTED_TYPE: str
    reveal_type(y)


def t_legacy_query_cols_1_with_entities() -> None:
    q1 = session.query(User).filter(User.id == 5)

    # EXPECTED_TYPE: Query[User]
    reveal_type(q1)

    q2 = q1.with_entities(User.id, User.name)

    # EXPECTED_TYPE: RowReturningQuery[int, str]
    reveal_type(q2)

    # EXPECTED_TYPE: .*Row[int, str].*
    reveal_type(q2.one())

    r1 = q2.one()

    x, y = r1

    # EXPECTED_TYPE: int
    reveal_type(x)

    # EXPECTED_TYPE: str
    reveal_type(y)


def t_select_with_only_cols() -> None:
    q1 = select(User).where(User.id == 5)

    # EXPECTED_TYPE: Select[User]
    reveal_type(q1)

    q2 = q1.with_only_columns(User.id, User.name)

    # EXPECTED_TYPE: Select[int, str]
    reveal_type(q2)

    row = connection.execute(q2).one()

    # EXPECTED_TYPE: .*Row[int, str].*
    reveal_type(row)

    x, y = row

    # EXPECTED_TYPE: int
    reveal_type(x)

    # EXPECTED_TYPE: str
    reveal_type(y)


def t_legacy_query_cols_2() -> None:
    a1 = aliased(User)
    q1 = session.query(User, a1, User.name).filter(User.id == 5)

    # EXPECTED_TYPE: RowReturningQuery[User, User, str]
    reveal_type(q1)

    # EXPECTED_TYPE: .*Row[User, User, str].*
    reveal_type(q1.one())

    r1 = q1.one()

    x, y, z = r1

    # EXPECTED_TYPE: User
    reveal_type(x)

    # EXPECTED_TYPE: User
    reveal_type(y)

    # EXPECTED_TYPE: str
    reveal_type(z)


def t_legacy_query_cols_2_with_entities() -> None:
    q1 = session.query(User)

    # EXPECTED_TYPE: Query[User]
    reveal_type(q1)

    a1 = aliased(User)
    q2 = q1.with_entities(User, a1, User.name).filter(User.id == 5)

    # EXPECTED_TYPE: RowReturningQuery[User, User, str]
    reveal_type(q2)

    # EXPECTED_TYPE: .*Row[User, User, str].*
    reveal_type(q2.one())

    r1 = q2.one()

    x, y, z = r1

    # EXPECTED_TYPE: User
    reveal_type(x)

    # EXPECTED_TYPE: User
    reveal_type(y)

    # EXPECTED_TYPE: str
    reveal_type(z)


def t_select_add_col_loses_type() -> None:
    q1 = select(User.id, User.name).filter(User.id == 5)

    q2 = q1.add_columns(User.data)

    # note this should not match Select
    # EXPECTED_TYPE: Select[Unpack[.*tuple[Any, ...]]]
    reveal_type(q2)


def t_legacy_query_add_col_loses_type() -> None:
    q1 = session.query(User.id, User.name).filter(User.id == 5)

    q2 = q1.add_columns(User.data)

    # this should match only Any
    # EXPECTED_TYPE: Query[Any]
    reveal_type(q2)

    ua = aliased(User)
    q3 = q1.add_entity(ua)

    # EXPECTED_TYPE: Query[Any]
    reveal_type(q3)


def t_legacy_query_scalar_subquery() -> None:
    """scalar subquery should receive the type if first element is a
    column only"""
    q1 = session.query(User.id)

    q2 = q1.scalar_subquery()

    # this should be int but mypy can't see it due to the
    # overload that tries to match an entity.
    # EXPECTED_TYPE: ScalarSelect[Any]
    reveal_type(q2)

    q3 = session.query(User)

    q4 = q3.scalar_subquery()

    # EXPECTED_TYPE: ScalarSelect[Any]
    reveal_type(q4)

    q5 = session.query(User, User.name)

    q6 = q5.scalar_subquery()

    # EXPECTED_TYPE: ScalarSelect[Any]
    reveal_type(q6)

    # try to simulate the problem with select()
    q7 = session.query(User).only_return_tuples(True)
    q8 = q7.scalar_subquery()

    # EXPECTED_TYPE: ScalarSelect[Any]
    reveal_type(q8)


def t_select_scalar_subquery() -> None:
    """scalar subquery should receive the type if first element is a
    column only"""
    s1 = select(User.id)
    s2 = s1.scalar_subquery()

    # this should be int but mypy can't see it due to the
    # overload that tries to match an entity.
    # EXPECTED_TYPE: ScalarSelect[Any]
    reveal_type(s2)

    s3 = select(User)
    s4 = s3.scalar_subquery()

    # it's more important that mypy doesn't get a false positive of
    # 'User' here
    # EXPECTED_TYPE: ScalarSelect[Any]
    reveal_type(s4)


def t_select_w_core_selectables() -> None:
    """things that come from .c. or are FromClause objects currently are not
    typed.  Make sure we are still getting Select at least.

    """
    s1 = select(User.id, User.name).subquery()

    # EXPECTED_TYPE: KeyedColumnElement[Any]
    reveal_type(s1.c.name)

    s2 = select(User.id, s1.c.name)

    # this one unfortunately is not working in mypy.
    # pylance gets the correct type
    #   EXPECTED_TYPE: Select[tuple[int, Any]]
    # when experimenting with having a separate TypedSelect class for typing,
    # mypy would downgrade to Any rather than picking the basemost type.
    # with typing integrated into Select etc. we can at least get a Select
    # object back.
    # EXPECTED_TYPE: Select[Unpack[.*tuple[Any, ...]]]
    reveal_type(s2)

    # so a fully explicit type may be given
    s2_typed: Select[tuple[int, str]] = select(User.id, s1.c.name)

    # EXPECTED_TYPE: Select[tuple[int, str]]
    reveal_type(s2_typed)

    # plain FromClause etc we at least get Select
    s3 = select(s1)

    # EXPECTED_TYPE: Select[Unpack[.*tuple[Any, ...]]]
    reveal_type(s3)

    t1 = User.__table__
    assert t1 is not None

    # EXPECTED_TYPE: FromClause
    reveal_type(t1)

    s4 = select(t1)

    # EXPECTED_TYPE: Select[Unpack[.*tuple[Any, ...]]]
    reveal_type(s4)


def t_dml_insert() -> None:
    s1 = insert(User).returning(User.id, User.name)

    r1 = session.execute(s1)

    # EXPECTED_TYPE: Result[int, str]
    reveal_type(r1)

    s2 = insert(User).returning(User)

    r2 = session.execute(s2)

    # EXPECTED_TYPE: Result[User]
    reveal_type(r2)

    s3 = insert(User).returning(func.foo(), column("q"))

    # EXPECTED_TYPE: ReturningInsert[Unpack[.*tuple[Any, ...]]]
    reveal_type(s3)

    r3 = session.execute(s3)

    # EXPECTED_TYPE: Result[Unpack[.*tuple[Any, ...]]]
    reveal_type(r3)


def t_dml_bare_insert() -> None:
    s1 = insert(User)
    r1 = session.execute(s1)
    # EXPECTED_TYPE: Result[Unpack[.*tuple[Any, ...]]]
    reveal_type(r1)


def t_dml_bare_update() -> None:
    s1 = update(User)
    r1 = session.execute(s1)
    # EXPECTED_TYPE: Result[Unpack[.*tuple[Any, ...]]]
    reveal_type(r1)


def t_dml_update_with_values() -> None:
    s1 = update(User).values({User.id: 123, User.data: "value"})
    r1 = session.execute(s1)
    # EXPECTED_TYPE: Result[Unpack[.*tuple[Any, ...]]]
    reveal_type(r1)


def t_dml_bare_delete() -> None:
    s1 = delete(User)
    r1 = session.execute(s1)
    # EXPECTED_TYPE: Result[Unpack[.*tuple[Any, ...]]]
    reveal_type(r1)


def t_dml_update() -> None:
    s1 = update(User).returning(User.id, User.name)

    r1 = session.execute(s1)

    # EXPECTED_TYPE: Result[int, str]
    reveal_type(r1)


def t_dml_delete() -> None:
    s1 = delete(User).returning(User.id, User.name)

    r1 = session.execute(s1)

    # EXPECTED_TYPE: Result[int, str]
    reveal_type(r1)


def t_from_statement() -> None:
    t = text("select * from user")

    # EXPECTED_TYPE: TextClause
    reveal_type(t)

    select(User).from_statement(t)

    ts = text("select * from user").columns(User.id, User.name)

    # EXPECTED_TYPE: TextualSelect
    reveal_type(ts)

    select(User).from_statement(ts)

    ts2 = text("select * from user").columns(
        user_table.c.id, user_table.c.name
    )

    # EXPECTED_TYPE: TextualSelect
    reveal_type(ts2)

    select(User).from_statement(ts2)


def t_aliased_fromclause() -> None:
    a1 = aliased(User, user_table)

    a2 = aliased(User, user_table.alias())

    a3 = aliased(User, join(user_table, user_table.alias()))

    a4 = aliased(user_table)

    # EXPECTED_TYPE: type[User]
    reveal_type(a1)

    # EXPECTED_TYPE: type[User]
    reveal_type(a2)

    # EXPECTED_TYPE: type[User]
    reveal_type(a3)

    # EXPECTED_TYPE: FromClause
    reveal_type(a4)


def test_select_from() -> None:
    select(1).select_from(User).exists()
    exists(1).select_from(User).select()
