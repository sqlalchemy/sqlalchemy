from __future__ import annotations

import asyncio
from typing import List

from sqlalchemy import create_engine
from sqlalchemy import ForeignKey
from sqlalchemy.ext.asyncio import async_scoped_session
from sqlalchemy.ext.asyncio import async_sessionmaker
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import relationship
from sqlalchemy.orm import scoped_session
from sqlalchemy.orm import Session
from sqlalchemy.orm import sessionmaker


class Base(DeclarativeBase):
    pass


class User(Base):
    __tablename__ = "user"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str]
    addresses: Mapped[List[Address]] = relationship(back_populates="user")


class Address(Base):
    __tablename__ = "address"

    id: Mapped[int] = mapped_column(primary_key=True)
    user_id = mapped_column(ForeignKey("user.id"))
    email: Mapped[str]

    user: Mapped[User] = relationship(back_populates="addresses")


e = create_engine("sqlite://")
Base.metadata.create_all(e)

with Session(e) as sess:
    u1 = User(name="u1")
    sess.add(u1)
    sess.add_all([Address(user=u1, email="e1"), Address(user=u1, email="e2")])
    sess.commit()

    q = sess.query(User).filter_by(id=7)

    # EXPECTED_TYPE: Query[User]
    reveal_type(q)

    rows1 = q.all()

    # EXPECTED_RE_TYPE: builtins.list\[.*User\*?\]
    reveal_type(rows1)

    q2 = sess.query(User.id).filter_by(id=7)
    rows2 = q2.all()

    # EXPECTED_TYPE: list[.*Row[.*int].*]
    reveal_type(rows2)

    # test #8280

    sess.query(User).update(
        {"name": User.name + " some name"}, synchronize_session="fetch"
    )
    sess.query(User).update(
        {"name": User.name + " some name"}, synchronize_session=False
    )
    sess.query(User).update(
        {"name": User.name + " some name"}, synchronize_session="evaluate"
    )

    sess.query(User).update(
        {"name": User.name + " some name"},
        # EXPECTED_MYPY: Argument "synchronize_session" to "update" of "Query" has incompatible type  # noqa: E501
        synchronize_session="invalid",
    )
    sess.query(User).update({"name": User.name + " some name"})

    # test #9125

    for row in sess.query(User.id, User.name):
        # EXPECTED_TYPE: .*Row[int, str].*
        reveal_type(row)

    for uobj1 in sess.query(User):
        # EXPECTED_TYPE: User
        reveal_type(uobj1)

    sess.query(User).limit(None).offset(None).limit(10).offset(10).limit(
        User.id
    ).offset(User.id)

    # test #11083

    with sess.begin() as tx:
        # EXPECTED_TYPE: SessionTransaction
        reveal_type(tx)

# more result tests in typed_results.py


def test_with_for_update() -> None:
    """test #9762"""
    sess = Session()
    ss = scoped_session(sessionmaker())

    sess.get(User, 1)
    sess.get(User, 1, with_for_update=True)
    ss.get(User, 1)
    ss.get(User, 1, with_for_update=True)

    u1 = User()
    sess.refresh(u1)
    sess.refresh(u1, with_for_update=True)
    ss.refresh(u1)
    ss.refresh(u1, with_for_update=True)


async def test_with_for_update_async() -> None:
    """test #9762"""
    sess = AsyncSession()
    ss = async_scoped_session(
        async_sessionmaker(), scopefunc=asyncio.current_task
    )

    await sess.get(User, 1)
    await sess.get(User, 1, with_for_update=True)

    await ss.get(User, 1)
    await ss.get(User, 1, with_for_update=True)

    u1 = User()
    await sess.refresh(u1)
    await sess.refresh(u1, with_for_update=True)

    await ss.refresh(u1)
    await ss.refresh(u1, with_for_update=True)


def test_exec_options() -> None:
    """test #10182"""

    session = Session()

    session.connection(
        execution_options={"isolation_level": "REPEATABLE READ"}
    )

    scoped = scoped_session(sessionmaker())

    scoped.connection(execution_options={"isolation_level": "REPEATABLE READ"})


async def async_test_exec_options() -> None:
    """test #10182"""

    session = AsyncSession()

    await session.connection(
        execution_options={"isolation_level": "REPEATABLE READ"}
    )

    scoped = async_scoped_session(
        async_sessionmaker(), scopefunc=asyncio.current_task
    )

    await scoped.connection(
        execution_options={"isolation_level": "REPEATABLE READ"}
    )
