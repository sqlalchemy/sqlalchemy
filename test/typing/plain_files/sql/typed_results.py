from __future__ import annotations

import asyncio
from typing import cast
from typing import Optional
from typing import Tuple
from typing import Type

from sqlalchemy import Column
from sqlalchemy import column
from sqlalchemy import create_engine
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
from sqlalchemy.ext.asyncio import AsyncConnection
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.asyncio import create_async_engine
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

# EXPECTED_RE_TYPE: sqlalchemy..*AsyncConnection\*?
reveal_type(async_connection)

async_session = AsyncSession(async_connection)


# (variable) users1: Sequence[User]
users1 = session.scalars(select(User)).all()

# (variable) user: User
user = session.query(User).one()


user_iter = iter(session.scalars(select(User)))

# EXPECTED_RE_TYPE: sqlalchemy..*AsyncSession\*?
reveal_type(async_session)


single_stmt = select(User.name).where(User.name == "foo")

# EXPECTED_RE_TYPE: sqlalchemy..*Select\*?\[Tuple\[builtins.str\*?\]\]
reveal_type(single_stmt)

multi_stmt = select(User.id, User.name).where(User.name == "foo")

# EXPECTED_RE_TYPE: sqlalchemy..*Select\*?\[Tuple\[builtins.int\*?, builtins.str\*?\]\]
reveal_type(multi_stmt)


def t_result_ctxmanager() -> None:
    with connection.execute(select(column("q", Integer))) as r1:
        # EXPECTED_TYPE: CursorResult[Tuple[int]]
        reveal_type(r1)

        with r1.mappings() as r1m:
            # EXPECTED_TYPE: MappingResult
            reveal_type(r1m)

    with connection.scalars(select(column("q", Integer))) as r2:
        # EXPECTED_TYPE: ScalarResult[int]
        reveal_type(r2)

    with session.execute(select(User.id)) as r3:
        # EXPECTED_TYPE: Result[Tuple[int]]
        reveal_type(r3)

    with session.scalars(select(User.id)) as r4:
        # EXPECTED_TYPE: ScalarResult[int]
        reveal_type(r4)


def t_core_mappings() -> None:
    r = connection.execute(select(t_user)).mappings().one()
    r.get(t_user.c.id)


def t_entity_varieties() -> None:
    a1 = aliased(User)

    s1 = select(User.id, User, User.name).where(User.name == "foo")

    r1 = session.execute(s1)

    # EXPECTED_RE_TYPE: sqlalchemy..*.Result\[Tuple\[builtins.int\*?, typed_results.User\*?, builtins.str\*?\]\]
    reveal_type(r1)

    s2 = select(User, a1).where(User.name == "foo")

    r2 = session.execute(s2)

    # EXPECTED_RE_TYPE: sqlalchemy.*Result\[Tuple\[typed_results.User\*?, typed_results.User\*?\]\]
    reveal_type(r2)

    row = r2.t.one()

    # EXPECTED_RE_TYPE: .*typed_results.User\*?
    reveal_type(row[0])
    # EXPECTED_RE_TYPE: .*typed_results.User\*?
    reveal_type(row[1])

    # testing that plain Mapped[x] gets picked up as well as
    # aliased class
    # there is unfortunately no way for attributes on an AliasedClass to be
    # automatically typed since they are dynamically generated
    a1_id = cast(Mapped[int], a1.id)
    s3 = select(User.id, a1_id, a1, User).where(User.name == "foo")
    # EXPECTED_RE_TYPE: sqlalchemy.*Select\*?\[Tuple\[builtins.int\*?, builtins.int\*?, typed_results.User\*?, typed_results.User\*?\]\]
    reveal_type(s3)

    # testing Mapped[entity]
    some_mp = cast(Mapped[User], object())
    s4 = select(some_mp, a1, User).where(User.name == "foo")

    # NOTEXPECTED_RE_TYPE: sqlalchemy..*Select\*?\[Tuple\[typed_results.User\*?, typed_results.User\*?, typed_results.User\*?\]\]

    # sqlalchemy.sql._gen_overloads.Select[Tuple[typed_results.User, typed_results.User, typed_results.User]]

    # EXPECTED_TYPE: Select[Tuple[User, User, User]]
    reveal_type(s4)

    # test plain core expressions
    x = Column("x", Integer)
    y = x + 5

    s5 = select(x, y, User.name + "hi")

    # EXPECTED_RE_TYPE: sqlalchemy..*Select\*?\[Tuple\[builtins.int\*?, builtins.int\*?\, builtins.str\*?]\]
    reveal_type(s5)


def t_ambiguous_result_type_one() -> None:
    stmt = select(column("q", Integer), table("x", column("y")))

    # EXPECTED_TYPE: Select[Any]
    reveal_type(stmt)

    result = session.execute(stmt)

    # EXPECTED_TYPE: Result[Any]
    reveal_type(result)


def t_ambiguous_result_type_two() -> None:
    stmt = select(column("q"))

    # EXPECTED_TYPE: Select[Tuple[Any]]
    reveal_type(stmt)
    result = session.execute(stmt)

    # EXPECTED_TYPE: Result[Any]
    reveal_type(result)


def t_aliased() -> None:
    a1 = aliased(User)

    s1 = select(a1)
    # EXPECTED_TYPE: Select[Tuple[User]]
    reveal_type(s1)

    s4 = select(a1.name, a1, a1, User).where(User.name == "foo")
    # EXPECTED_TYPE: Select[Tuple[str, User, User, User]]
    reveal_type(s4)


def t_result_scalar_accessors() -> None:
    result = connection.execute(single_stmt)

    r1 = result.scalar()

    # EXPECTED_RE_TYPE: Union\[builtins.str\*?, None\]
    reveal_type(r1)

    r2 = result.scalar_one()

    # EXPECTED_RE_TYPE: builtins.str\*?
    reveal_type(r2)

    r3 = result.scalar_one_or_none()

    # EXPECTED_RE_TYPE: Union\[builtins.str\*?, None\]
    reveal_type(r3)

    r4 = result.scalars()

    # EXPECTED_RE_TYPE: sqlalchemy..*ScalarResult\[builtins.str.*?\]
    reveal_type(r4)

    r5 = result.scalars(0)

    # EXPECTED_RE_TYPE: sqlalchemy..*ScalarResult\[builtins.str.*?\]
    reveal_type(r5)


async def t_async_result_scalar_accessors() -> None:
    result = await async_connection.stream(single_stmt)

    r1 = await result.scalar()

    # EXPECTED_RE_TYPE: Union\[builtins.str\*?, None\]
    reveal_type(r1)

    r2 = await result.scalar_one()

    # EXPECTED_RE_TYPE: builtins.str\*?
    reveal_type(r2)

    r3 = await result.scalar_one_or_none()

    # EXPECTED_RE_TYPE: Union\[builtins.str\*?, None\]
    reveal_type(r3)

    r4 = result.scalars()

    # EXPECTED_RE_TYPE: sqlalchemy..*ScalarResult\[builtins.str.*?\]
    reveal_type(r4)

    r5 = result.scalars(0)

    # EXPECTED_RE_TYPE: sqlalchemy..*ScalarResult\[builtins.str.*?\]
    reveal_type(r5)


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

    # EXPECTED_TYPE: Sequence[int]
    reveal_type(uids1)

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

    # EXPECTED_TYPE: Sequence[int]
    reveal_type(uids2)


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

    # EXPECTED_TYPE: Sequence[int]
    reveal_type(uids1)

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

    # EXPECTED_TYPE: Sequence[int]
    reveal_type(uids2)


def t_connection_execute_multi_row_t() -> None:
    result = connection.execute(multi_stmt)

    # EXPECTED_RE_TYPE: sqlalchemy.*CursorResult\[Tuple\[builtins.int\*?, builtins.str\*?\]\]
    reveal_type(result)
    row = result.one()

    # EXPECTED_RE_TYPE: sqlalchemy.*Row\[Tuple\[builtins.int\*?, builtins.str\*?\]\]
    reveal_type(row)

    x, y = row.t

    # EXPECTED_RE_TYPE: builtins.int\*?
    reveal_type(x)

    # EXPECTED_RE_TYPE: builtins.str\*?
    reveal_type(y)


def t_connection_execute_multi() -> None:
    result = connection.execute(multi_stmt).t

    # EXPECTED_RE_TYPE: sqlalchemy.*TupleResult\[Tuple\[builtins.int\*?, builtins.str\*?\]\]
    reveal_type(result)
    row = result.one()

    # EXPECTED_RE_TYPE: Tuple\[builtins.int\*?, builtins.str\*?\]
    reveal_type(row)

    x, y = row

    # EXPECTED_RE_TYPE: builtins.int\*?
    reveal_type(x)

    # EXPECTED_RE_TYPE: builtins.str\*?
    reveal_type(y)


def t_connection_execute_single() -> None:
    result = connection.execute(single_stmt).t

    # EXPECTED_RE_TYPE: sqlalchemy.*TupleResult\[Tuple\[builtins.str\*?\]\]
    reveal_type(result)
    row = result.one()

    # EXPECTED_RE_TYPE: Tuple\[builtins.str\*?\]
    reveal_type(row)

    (x,) = row

    # EXPECTED_RE_TYPE: builtins.str\*?
    reveal_type(x)


def t_connection_execute_single_row_scalar() -> None:
    result = connection.execute(single_stmt).t

    # EXPECTED_RE_TYPE: sqlalchemy.*TupleResult\[Tuple\[builtins.str\*?\]\]
    reveal_type(result)

    x = result.scalar()

    # EXPECTED_RE_TYPE: Union\[builtins.str\*?, None\]
    reveal_type(x)


def t_connection_scalar() -> None:
    obj = connection.scalar(single_stmt)

    # EXPECTED_RE_TYPE: Union\[builtins.str\*?, None\]
    reveal_type(obj)


def t_connection_scalars() -> None:
    result = connection.scalars(single_stmt)

    # EXPECTED_RE_TYPE: sqlalchemy.*ScalarResult\[builtins.str\*?\]
    reveal_type(result)
    data = result.all()

    # EXPECTED_RE_TYPE: typing.Sequence\[builtins.str\*?\]
    reveal_type(data)


def t_session_execute_multi() -> None:
    result = session.execute(multi_stmt).t

    # EXPECTED_RE_TYPE: sqlalchemy.*TupleResult\[Tuple\[builtins.int\*?, builtins.str\*?\]\]
    reveal_type(result)
    row = result.one()

    # EXPECTED_RE_TYPE: Tuple\[builtins.int\*?, builtins.str\*?\]
    reveal_type(row)

    x, y = row

    # EXPECTED_RE_TYPE: builtins.int\*?
    reveal_type(x)

    # EXPECTED_RE_TYPE: builtins.str\*?
    reveal_type(y)


def t_session_execute_single() -> None:
    result = session.execute(single_stmt).t

    # EXPECTED_RE_TYPE: sqlalchemy.*TupleResult\[Tuple\[builtins.str\*?\]\]
    reveal_type(result)
    row = result.one()

    # EXPECTED_RE_TYPE: Tuple\[builtins.str\*?\]
    reveal_type(row)

    (x,) = row

    # EXPECTED_RE_TYPE: builtins.str\*?
    reveal_type(x)


def t_session_scalar() -> None:
    obj = session.scalar(single_stmt)

    # EXPECTED_RE_TYPE: Union\[builtins.str\*?, None\]
    reveal_type(obj)


def t_session_scalars() -> None:
    result = session.scalars(single_stmt)

    # EXPECTED_RE_TYPE: sqlalchemy.*ScalarResult\[builtins.str\*?\]
    reveal_type(result)
    data = result.all()

    # EXPECTED_RE_TYPE: typing.Sequence\[builtins.str\*?\]
    reveal_type(data)


async def t_async_connection_execute_multi() -> None:
    result = (await async_connection.execute(multi_stmt)).t

    # EXPECTED_RE_TYPE: sqlalchemy.*TupleResult\[Tuple\[builtins.int\*?, builtins.str\*?\]\]
    reveal_type(result)
    row = result.one()

    # EXPECTED_RE_TYPE: Tuple\[builtins.int\*?, builtins.str\*?\]
    reveal_type(row)

    x, y = row

    # EXPECTED_RE_TYPE: builtins.int\*?
    reveal_type(x)

    # EXPECTED_RE_TYPE: builtins.str\*?
    reveal_type(y)


async def t_async_connection_execute_single() -> None:
    result = (await async_connection.execute(single_stmt)).t

    # EXPECTED_RE_TYPE: sqlalchemy.*TupleResult\[Tuple\[builtins.str\*?\]\]
    reveal_type(result)

    row = result.one()

    # EXPECTED_RE_TYPE: Tuple\[builtins.str\*?\]
    reveal_type(row)

    (x,) = row

    # EXPECTED_RE_TYPE: builtins.str\*?
    reveal_type(x)


async def t_async_connection_scalar() -> None:
    obj = await async_connection.scalar(single_stmt)

    # EXPECTED_RE_TYPE: Union\[builtins.str\*?, None\]
    reveal_type(obj)


async def t_async_connection_scalars() -> None:
    result = await async_connection.scalars(single_stmt)

    # EXPECTED_RE_TYPE: sqlalchemy.*ScalarResult\*?\[builtins.str\*?\]
    reveal_type(result)
    data = result.all()

    # EXPECTED_RE_TYPE: typing.Sequence\[builtins.str\*?\]
    reveal_type(data)


async def t_async_session_execute_multi() -> None:
    result = (await async_session.execute(multi_stmt)).t

    # EXPECTED_RE_TYPE: sqlalchemy.*TupleResult\[Tuple\[builtins.int\*?, builtins.str\*?\]\]
    reveal_type(result)
    row = result.one()

    # EXPECTED_RE_TYPE: Tuple\[builtins.int\*?, builtins.str\*?\]
    reveal_type(row)

    x, y = row

    # EXPECTED_RE_TYPE: builtins.int\*?
    reveal_type(x)

    # EXPECTED_RE_TYPE: builtins.str\*?
    reveal_type(y)


async def t_async_session_execute_single() -> None:
    result = (await async_session.execute(single_stmt)).t

    # EXPECTED_RE_TYPE: sqlalchemy.*TupleResult\[Tuple\[builtins.str\*?\]\]
    reveal_type(result)
    row = result.one()

    # EXPECTED_RE_TYPE: Tuple\[builtins.str\*?\]
    reveal_type(row)

    (x,) = row

    # EXPECTED_RE_TYPE: builtins.str\*?
    reveal_type(x)


async def t_async_session_scalar() -> None:
    obj = await async_session.scalar(single_stmt)

    # EXPECTED_RE_TYPE: Union\[builtins.str\*?, None\]
    reveal_type(obj)


async def t_async_session_scalars() -> None:
    result = await async_session.scalars(single_stmt)

    # EXPECTED_RE_TYPE: sqlalchemy.*ScalarResult\*?\[builtins.str\*?\]
    reveal_type(result)
    data = result.all()

    # EXPECTED_RE_TYPE: typing.Sequence\[builtins.str\*?\]
    reveal_type(data)


async def t_async_connection_stream_multi() -> None:
    result = (await async_connection.stream(multi_stmt)).t

    # EXPECTED_RE_TYPE: sqlalchemy.*AsyncTupleResult\[Tuple\[builtins.int\*?, builtins.str\*?\]\]
    reveal_type(result)
    row = await result.one()

    # EXPECTED_RE_TYPE: Tuple\[builtins.int\*?, builtins.str\*?\]
    reveal_type(row)

    x, y = row

    # EXPECTED_RE_TYPE: builtins.int\*?
    reveal_type(x)

    # EXPECTED_RE_TYPE: builtins.str\*?
    reveal_type(y)


async def t_async_connection_stream_single() -> None:
    result = (await async_connection.stream(single_stmt)).t

    # EXPECTED_RE_TYPE: sqlalchemy.*AsyncTupleResult\[Tuple\[builtins.str\*?\]\]
    reveal_type(result)
    row = await result.one()

    # EXPECTED_RE_TYPE: Tuple\[builtins.str\*?\]
    reveal_type(row)

    (x,) = row

    # EXPECTED_RE_TYPE: builtins.str\*?
    reveal_type(x)


async def t_async_connection_stream_scalars() -> None:
    result = await async_connection.stream_scalars(single_stmt)

    # EXPECTED_RE_TYPE: sqlalchemy.*AsyncScalarResult\*?\[builtins.str\*?\]
    reveal_type(result)
    data = await result.all()

    # EXPECTED_RE_TYPE: typing.Sequence\*?\[builtins.str\*?\]
    reveal_type(data)


async def t_async_session_stream_multi() -> None:
    result = (await async_session.stream(multi_stmt)).t

    # EXPECTED_RE_TYPE: sqlalchemy.*TupleResult\[Tuple\[builtins.int\*?, builtins.str\*?\]\]
    reveal_type(result)
    row = await result.one()

    # EXPECTED_RE_TYPE: Tuple\[builtins.int\*?, builtins.str\*?\]
    reveal_type(row)

    x, y = row

    # EXPECTED_RE_TYPE: builtins.int\*?
    reveal_type(x)

    # EXPECTED_RE_TYPE: builtins.str\*?
    reveal_type(y)


async def t_async_session_stream_single() -> None:
    result = (await async_session.stream(single_stmt)).t

    # EXPECTED_RE_TYPE: sqlalchemy.*AsyncTupleResult\[Tuple\[builtins.str\*?\]\]
    reveal_type(result)
    row = await result.one()

    # EXPECTED_RE_TYPE: Tuple\[builtins.str\*?\]
    reveal_type(row)

    (x,) = row

    # EXPECTED_RE_TYPE: builtins.str\*?
    reveal_type(x)


async def t_async_session_stream_scalars() -> None:
    result = await async_session.stream_scalars(single_stmt)

    # EXPECTED_RE_TYPE: sqlalchemy.*AsyncScalarResult\*?\[builtins.str\*?\]
    reveal_type(result)
    data = await result.all()

    # EXPECTED_RE_TYPE: typing.Sequence\*?\[builtins.str\*?\]
    reveal_type(data)


def test_outerjoin_10173() -> None:
    class Other(Base):
        __tablename__ = "other"

        id: Mapped[int] = mapped_column(primary_key=True)
        name: Mapped[str]

    stmt: Select[Tuple[User, Other]] = select(User, Other).outerjoin(
        Other, User.id == Other.id
    )
    stmt2: Select[Tuple[User, Optional[Other]]] = select(
        User, Nullable(Other)
    ).outerjoin(Other, User.id == Other.id)
    stmt3: Select[Tuple[int, Optional[str]]] = select(
        User.id, Nullable(Other.name)
    ).outerjoin(Other, User.id == Other.id)

    def go(W: Optional[Type[Other]]) -> None:
        stmt4: Select[Tuple[str, Other]] = select(
            NotNullable(User.value), NotNullable(W)
        ).where(User.value.is_not(None))
        print(stmt4)

    print(stmt, stmt2, stmt3)
