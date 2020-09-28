"""Illustrates use of the sqlalchemy.ext.asyncio.AsyncSession object
for asynchronous ORM use, including the optional run_sync() method.


"""

import asyncio

from sqlalchemy import Column
from sqlalchemy import ForeignKey
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.future import select
from sqlalchemy.orm import relationship

Base = declarative_base()


class A(Base):
    __tablename__ = "a"

    id = Column(Integer, primary_key=True)
    data = Column(String)
    bs = relationship("B")


class B(Base):
    __tablename__ = "b"
    id = Column(Integer, primary_key=True)
    a_id = Column(ForeignKey("a.id"))
    data = Column(String)


def run_queries(session):
    """A function written in "synchronous" style that will be invoked
    within the asyncio event loop.

    The session object passed is a traditional orm.Session object with
    synchronous interface.

    """

    stmt = select(A)

    result = session.execute(stmt)

    for a1 in result.scalars():
        print(a1)
        # lazy loads
        for b1 in a1.bs:
            print(b1)

    result = session.execute(select(A).order_by(A.id))

    a1 = result.scalars().first()

    a1.data = "new data"


async def async_main():
    """Main program function."""

    engine = create_async_engine(
        "postgresql+asyncpg://scott:tiger@localhost/test",
        echo=True,
    )
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)

    async with AsyncSession(engine) as session:
        async with session.begin():
            session.add_all(
                [
                    A(bs=[B(), B()], data="a1"),
                    A(bs=[B()], data="a2"),
                    A(bs=[B(), B()], data="a3"),
                ]
            )

        # we have the option to run a function written in sync style
        # within the AsyncSession.run_sync() method.  The function will
        # be passed a synchronous-style Session object and the function
        # can use traditional ORM patterns.
        await session.run_sync(run_queries)

        await session.commit()


asyncio.run(async_main())
