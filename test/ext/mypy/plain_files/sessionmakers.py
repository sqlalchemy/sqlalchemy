"""test #7656"""

from sqlalchemy import create_engine
from sqlalchemy import Engine
from sqlalchemy.ext.asyncio import async_scoped_session
from sqlalchemy.ext.asyncio import async_sessionmaker
from sqlalchemy.ext.asyncio import AsyncEngine
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.orm import scoped_session
from sqlalchemy.orm import Session
from sqlalchemy.orm import sessionmaker


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
        # EXPECTED_TYPE: MyAsyncSession
        reveal_type(sess)

    async with fac.begin() as sess:
        # EXPECTED_TYPE: MyAsyncSession
        reveal_type(sess)

    scoped_fac = async_scoped_session_factory(async_engine)

    sess = scoped_fac()

    # EXPECTED_TYPE: MyAsyncSession
    reveal_type(sess)


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
        # EXPECTED_TYPE: MySession
        reveal_type(sess)

    with fac.begin() as sess:
        # EXPECTED_TYPE: MySession
        reveal_type(sess)

    scoped_fac = scoped_session_factory(engine)

    sess = scoped_fac()
    # EXPECTED_TYPE: MySession
    reveal_type(sess)
