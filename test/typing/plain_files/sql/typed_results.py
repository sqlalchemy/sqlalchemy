from __future__ import annotations

import asyncio
from typing import Any
from typing import assert_type
from typing import cast
from typing import Optional
from typing import Sequence
from typing import Type
from typing import Unpack

from sqlalchemy import Column
from sqlalchemy import column
from sqlalchemy import create_engine
from sqlalchemy import func
from sqlalchemy import insert
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import NotNullable
from sqlalchemy import Nullable
from sqlalchemy import Select
from sqlalchemy import select
from sqlalchemy import String
from sqlalchemy import Table
from sqlalchemy import table
from sqlalchemy.engine import Result
from sqlalchemy.engine.cursor import CursorResult
from sqlalchemy.engine.result import MappingResult
from sqlalchemy.engine.result import ScalarResult
from sqlalchemy.engine.result import TupleResult
from sqlalchemy.engine.row import Row
from sqlalchemy.ext.asyncio import AsyncConnection
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.ext.asyncio.result import AsyncScalarResult
from sqlalchemy.ext.asyncio.result import AsyncTupleResult
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
    value: Mapped[Optional[str]]


t_user = Table(
    "user",
    MetaData(),
    Column("id", Integer, primary_key=True),
    Column("name", String),
)


e = create_engine("sqlite://")
ae = create_async_engine("sqlite+aiosqlite://")


connection = e.connect()
session = Session(connection)


async def async_connect() -> AsyncConnection:
    return await ae.connect()


# the thing with the \*? seems like it could go away
# as of mypy 0.950

async_connection = asyncio.run(async_connect())

assert_type(async_connection, AsyncConnection)

async_session = AsyncSession(async_connection)


# (variable) users1: Sequence[User]
users1 = session.scalars(select(User)).all()

# (variable) user: User
user = session.query(User).one()


user_iter = iter(session.scalars(select(User)))

assert_type(async_session, AsyncSession)


single_stmt = select(User.name).where(User.name == "foo")

assert_type(single_stmt, Select[str])

multi_stmt = select(User.id, User.name).where(User.name == "foo")

assert_type(multi_stmt, Select[int, str])


def t_result_ctxmanager() -> None:
    with connection.execute(select(column("q", Integer))) as r1:
        assert_type(r1, CursorResult[int])

        with r1.mappings() as r1m:
            assert_type(r1m, MappingResult)

    with connection.scalars(select(column("q", Integer))) as r2:
        assert_type(r2, ScalarResult[int])

    with session.execute(select(User.id)) as r3:
        assert_type(r3, Result[int])

    with session.scalars(select(User.id)) as r4:
        assert_type(r4, ScalarResult[int])


def t_mappings() -> None:
    r = connection.execute(select(t_user)).mappings().one()
    r["name"]  # string
    r.get(t_user.c.id)  # column

    r2 = connection.execute(select(User)).mappings().one()
    r2[User.id]  # orm attribute
    r2[User.__table__.c.id]  # form clause column

    m2 = User.id * 2
    s2 = User.__table__.c.id + 2
    fn = func.abs(User.id)
    r3 = connection.execute(select(m2, s2, fn)).mappings().one()
    r3[m2]  # col element
    r3[s2]  # also col element
    r3[fn]  # function


def t_entity_varieties() -> None:
    a1 = aliased(User)

    s1 = select(User.id, User, User.name).where(User.name == "foo")

    r1 = session.execute(s1)

    assert_type(r1, Result[int, User, str])

    s2 = select(User, a1).where(User.name == "foo")

    r2 = session.execute(s2)

    assert_type(r2, Result[User, User])

    row = r2.t.one()

    assert_type(row[0], User)
    assert_type(row[1], User)

    # testing that plain Mapped[x] gets picked up as well as
    # aliased class
    # there is unfortunately no way for attributes on an AliasedClass to be
    # automatically typed since they are dynamically generated
    a1_id = cast(Mapped[int], a1.id)
    s3 = select(User.id, a1_id, a1, User).where(User.name == "foo")
    assert_type(s3, Select[int, int, User, User])

    # testing Mapped[entity]
    some_mp = cast(Mapped[User], object())
    s4 = select(some_mp, a1, User).where(User.name == "foo")

    # NOTEXPECTED_RE_TYPE: sqlalchemy..*Select\*?\[User\*?, User\*?, User\*?\]

    # sqlalchemy.sql._gen_overloads.Select[User, User, User]

    assert_type(s4, Select[User, User, User])

    # test plain core expressions
    x = Column("x", Integer)
    y = x + 5

    s5 = select(x, y, User.name + "hi")

    assert_type(s5, Select[int, int, str])


def t_ambiguous_result_type_one() -> None:
    stmt = select(column("q", Integer), table("x", column("y")))

    assert_type(stmt, Select[Unpack[tuple[Any, ...]]])

    result = session.execute(stmt)

    assert_type(result, Result[Unpack[tuple[Any, ...]]])


def t_ambiguous_result_type_two() -> None:
    stmt = select(column("q"))

    assert_type(stmt, Select[Any])
    result = session.execute(stmt)

    assert_type(result, Result[Unpack[tuple[Any, ...]]])


def t_aliased() -> None:
    a1 = aliased(User)

    s1 = select(a1)
    assert_type(s1, Select[User])

    s4 = select(a1.name, a1, a1, User).where(User.name == "foo")
    assert_type(s4, Select[str, User, User, User])


def t_result_scalar_accessors() -> None:
    result = connection.execute(single_stmt)

    r1 = result.scalar()

    assert_type(r1, str | None)

    r2 = result.scalar_one()

    assert_type(r2, str)

    r3 = result.scalar_one_or_none()

    assert_type(r3, str | None)

    r4 = result.scalars()

    assert_type(r4, ScalarResult[str])

    r5 = result.scalars(0)

    assert_type(r5, ScalarResult[str])


async def t_async_result_scalar_accessors() -> None:
    result = await async_connection.stream(single_stmt)

    r1 = await result.scalar()

    assert_type(r1, str | None)

    r2 = await result.scalar_one()

    assert_type(r2, str)

    r3 = await result.scalar_one_or_none()

    assert_type(r3, str | None)

    r4 = result.scalars()

    assert_type(r4, AsyncScalarResult[str])

    r5 = result.scalars(0)

    assert_type(r5, AsyncScalarResult[str])


def t_result_insertmanyvalues_scalars() -> None:
    stmt = insert(User).returning(User.id)

    uids1 = connection.scalars(
        stmt,
        [
            {"name": "n1"},
            {"name": "n2"},
            {"name": "n3"},
        ],
    ).all()

    assert_type(uids1, Sequence[int])

    uids2 = (
        connection.execute(
            stmt,
            [
                {"name": "n1"},
                {"name": "n2"},
                {"name": "n3"},
            ],
        )
        .scalars()
        .all()
    )

    assert_type(uids2, Sequence[int])


async def t_async_result_insertmanyvalues_scalars() -> None:
    stmt = insert(User).returning(User.id)

    uids1 = (
        await async_connection.scalars(
            stmt,
            [
                {"name": "n1"},
                {"name": "n2"},
                {"name": "n3"},
            ],
        )
    ).all()

    assert_type(uids1, Sequence[int])

    uids2 = (
        (
            await async_connection.execute(
                stmt,
                [
                    {"name": "n1"},
                    {"name": "n2"},
                    {"name": "n3"},
                ],
            )
        )
        .scalars()
        .all()
    )

    assert_type(uids2, Sequence[int])


def t_connection_execute_multi_row_t() -> None:
    result = connection.execute(multi_stmt)

    assert_type(result, CursorResult[int, str])
    row = result.one()

    assert_type(row, Row[int, str])

    x, y = row.t

    assert_type(x, int)

    assert_type(y, str)


def t_connection_execute_multi() -> None:
    result = connection.execute(multi_stmt).t

    assert_type(result, TupleResult[tuple[int, str]])
    row = result.one()

    assert_type(row, tuple[int, str])

    x, y = row

    assert_type(x, int)

    assert_type(y, str)


def t_connection_execute_single() -> None:
    result = connection.execute(single_stmt).t

    assert_type(result, TupleResult[tuple[str]])
    row = result.one()

    assert_type(row, tuple[str])

    (x,) = row

    assert_type(x, str)


def t_connection_execute_single_row_scalar() -> None:
    result = connection.execute(single_stmt).t

    assert_type(result, TupleResult[tuple[str]])

    x = result.scalar()

    assert_type(x, str | None)


def t_connection_scalar() -> None:
    obj = connection.scalar(single_stmt)

    assert_type(obj, str | None)


def t_connection_scalars() -> None:
    result = connection.scalars(single_stmt)

    assert_type(result, ScalarResult[str])
    data = result.all()

    assert_type(data, Sequence[str])


def t_session_execute_multi() -> None:
    result = session.execute(multi_stmt).t

    assert_type(result, TupleResult[tuple[int, str]])
    row = result.one()

    assert_type(row, tuple[int, str])

    x, y = row

    assert_type(x, int)

    assert_type(y, str)


def t_session_execute_single() -> None:
    result = session.execute(single_stmt).t

    assert_type(result, TupleResult[tuple[str]])
    row = result.one()

    assert_type(row, tuple[str])

    (x,) = row

    assert_type(x, str)


def t_session_scalar() -> None:
    obj = session.scalar(single_stmt)

    assert_type(obj, str | None)


def t_session_scalars() -> None:
    result = session.scalars(single_stmt)

    assert_type(result, ScalarResult[str])
    data = result.all()

    assert_type(data, Sequence[str])


async def t_async_connection_execute_multi() -> None:
    result = (await async_connection.execute(multi_stmt)).t

    assert_type(result, TupleResult[tuple[int, str]])
    row = result.one()

    assert_type(row, tuple[int, str])

    x, y = row

    assert_type(x, int)

    assert_type(y, str)


async def t_async_connection_execute_single() -> None:
    result = (await async_connection.execute(single_stmt)).t

    assert_type(result, TupleResult[tuple[str]])

    row = result.one()

    assert_type(row, tuple[str])

    (x,) = row

    assert_type(x, str)


async def t_async_connection_scalar() -> None:
    obj = await async_connection.scalar(single_stmt)

    assert_type(obj, str | None)


async def t_async_connection_scalars() -> None:
    result = await async_connection.scalars(single_stmt)

    assert_type(result, ScalarResult[str])
    data = result.all()

    assert_type(data, Sequence[str])


async def t_async_session_execute_multi() -> None:
    result = (await async_session.execute(multi_stmt)).t

    assert_type(result, TupleResult[tuple[int, str]])
    row = result.one()

    assert_type(row, tuple[int, str])

    x, y = row

    assert_type(x, int)

    assert_type(y, str)


async def t_async_session_execute_single() -> None:
    result = (await async_session.execute(single_stmt)).t

    assert_type(result, TupleResult[tuple[str]])
    row = result.one()

    assert_type(row, tuple[str])

    (x,) = row

    assert_type(x, str)


async def t_async_session_scalar() -> None:
    obj = await async_session.scalar(single_stmt)

    assert_type(obj, str | None)


async def t_async_session_scalars() -> None:
    result = await async_session.scalars(single_stmt)

    assert_type(result, ScalarResult[str])
    data = result.all()

    assert_type(data, Sequence[str])


async def t_async_connection_stream_multi() -> None:
    result = (await async_connection.stream(multi_stmt)).t

    assert_type(result, AsyncTupleResult[tuple[int, str]])
    row = await result.one()

    assert_type(row, tuple[int, str])

    x, y = row

    assert_type(x, int)

    assert_type(y, str)


async def t_async_connection_stream_single() -> None:
    result = (await async_connection.stream(single_stmt)).t

    assert_type(result, AsyncTupleResult[tuple[str]])
    row = await result.one()

    assert_type(row, tuple[str])

    (x,) = row

    assert_type(x, str)


async def t_async_connection_stream_scalars() -> None:
    result = await async_connection.stream_scalars(single_stmt)

    assert_type(result, AsyncScalarResult[str])
    data = await result.all()

    assert_type(data, Sequence[str])


async def t_async_session_stream_multi() -> None:
    result = (await async_session.stream(multi_stmt)).t

    assert_type(result, AsyncTupleResult[tuple[int, str]])
    row = await result.one()

    assert_type(row, tuple[int, str])

    x, y = row

    assert_type(x, int)

    assert_type(y, str)


async def t_async_session_stream_single() -> None:
    result = (await async_session.stream(single_stmt)).t

    assert_type(result, AsyncTupleResult[tuple[str]])
    row = await result.one()

    assert_type(row, tuple[str])

    (x,) = row

    assert_type(x, str)


async def t_async_session_stream_scalars() -> None:
    result = await async_session.stream_scalars(single_stmt)

    assert_type(result, AsyncScalarResult[str])
    data = await result.all()

    assert_type(data, Sequence[str])


def test_outerjoin_10173() -> None:
    class Other(Base):
        __tablename__ = "other"

        id: Mapped[int] = mapped_column(primary_key=True)
        name: Mapped[str]

    stmt: Select[User, Other] = select(User, Other).outerjoin(
        Other, User.id == Other.id
    )
    stmt2: Select[User, Optional[Other]] = select(
        User, Nullable(Other)
    ).outerjoin(Other, User.id == Other.id)
    stmt3: Select[int, Optional[str]] = select(
        User.id, Nullable(Other.name)
    ).outerjoin(Other, User.id == Other.id)

    def go(W: Optional[Type[Other]]) -> None:
        stmt4: Select[str, Other] = select(
            NotNullable(User.value), NotNullable(W)
        ).where(User.value.is_not(None))
        print(stmt4)

    print(stmt, stmt2, stmt3)
