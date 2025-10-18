"""test sessionmaker, originally for #7656"""

from typing import assert_type

from sqlalchemy import create_engine
from sqlalchemy import Engine
from sqlalchemy.ext.asyncio import async_scoped_session
from sqlalchemy.ext.asyncio import async_sessionmaker
from sqlalchemy.ext.asyncio import AsyncEngine
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.orm import QueryPropertyDescriptor
from sqlalchemy.orm import scoped_session
from sqlalchemy.orm import Session
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.query import Query


async_engine = create_async_engine("...")


class MyAsyncSession(AsyncSession):
    pass


def async_session_factory(
    engine: AsyncEngine,
) -> async_sessionmaker[MyAsyncSession]:
    return async_sessionmaker(engine, class_=MyAsyncSession)


def async_scoped_session_factory(
    engine: AsyncEngine,
) -> async_scoped_session[MyAsyncSession]:
    return async_scoped_session(
        async_sessionmaker(engine, class_=MyAsyncSession),
        scopefunc=lambda: None,
    )


async def async_main() -> None:
    fac = async_session_factory(async_engine)

    async with fac() as sess:
        assert_type(sess, MyAsyncSession)

    async with fac.begin() as sess:
        assert_type(sess, MyAsyncSession)

    scoped_fac = async_scoped_session_factory(async_engine)

    sess = scoped_fac()

    assert_type(sess, MyAsyncSession)


engine = create_engine("...")


class MySession(Session):
    pass


def session_factory(
    engine: Engine,
) -> sessionmaker[MySession]:
    return sessionmaker(engine, class_=MySession)


def scoped_session_factory(engine: Engine) -> scoped_session[MySession]:
    return scoped_session(sessionmaker(engine, class_=MySession))


def main() -> None:
    fac = session_factory(engine)

    with fac() as sess:
        assert_type(sess, MySession)

    with fac.begin() as sess:
        assert_type(sess, MySession)

    scoped_fac = scoped_session_factory(engine)

    sess = scoped_fac()
    assert_type(sess, MySession)


def test_8837_sync() -> None:
    sm = sessionmaker()

    assert_type(sm, sessionmaker[Session])

    session = sm()

    assert_type(session, Session)


def test_8837_async() -> None:
    as_ = async_sessionmaker()

    assert_type(as_, async_sessionmaker[AsyncSession])

    async_session = as_()

    assert_type(async_session, AsyncSession)


# test #9338
ss_9338 = scoped_session_factory(engine)

assert_type(ss_9338.query_property(), QueryPropertyDescriptor)
qp: QueryPropertyDescriptor = ss_9338.query_property()


class Foo:
    query = qp


assert_type(Foo.query, Query[Foo])

assert_type(Foo.query.all(), list[Foo])


class Bar:
    query: QueryPropertyDescriptor = ss_9338.query_property()


assert_type(Bar.query, Query[Bar])
