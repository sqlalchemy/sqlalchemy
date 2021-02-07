"""Illustrates use of the sqlalchemy.ext.asyncio.AsyncSession object
for asynchronous ORM use.

"""

import asyncio

from sqlalchemy import Column
from sqlalchemy import DateTime
from sqlalchemy import ForeignKey
from sqlalchemy import func
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.future import select
from sqlalchemy.orm import relationship
from sqlalchemy.orm import selectinload
from sqlalchemy.orm import sessionmaker

Base = declarative_base()


class A(Base):
    __tablename__ = "a"

    id = Column(Integer, primary_key=True)
    data = Column(String)
    create_date = Column(DateTime, server_default=func.now())
    bs = relationship("B")

    # required in order to access columns with server defaults
    # or SQL expression defaults, subsequent to a flush, without
    # triggering an expired load
    __mapper_args__ = {"eager_defaults": True}


class B(Base):
    __tablename__ = "b"
    id = Column(Integer, primary_key=True)
    a_id = Column(ForeignKey("a.id"))
    data = Column(String)


async def async_main():
    """Main program function."""

    engine = create_async_engine(
        "postgresql+asyncpg://scott:tiger@localhost/test",
        echo=True,
    )

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)

    # expire_on_commit=False will prevent attributes from being expired
    # after commit.
    async_session = sessionmaker(
        engine, expire_on_commit=False, class_=AsyncSession
    )

    async with async_session() as session:
        async with session.begin():
            session.add_all(
                [
                    A(bs=[B(), B()], data="a1"),
                    A(bs=[B()], data="a2"),
                    A(bs=[B(), B()], data="a3"),
                ]
            )

        # for relationship loading, eager loading should be applied.
        stmt = select(A).options(selectinload(A.bs))

        # AsyncSession.execute() is used for 2.0 style ORM execution
        # (same as the synchronous API).
        result = await session.execute(stmt)

        # result is a buffered Result object.
        for a1 in result.scalars():
            print(a1)
            print(f"created at: {a1.create_date}")
            for b1 in a1.bs:
                print(b1)

        # for streaming ORM results, AsyncSession.stream() may be used.
        result = await session.stream(stmt)

        # result is a streaming AsyncResult object.
        async for a1 in result.scalars():
            print(a1)
            for b1 in a1.bs:
                print(b1)

        result = await session.execute(select(A).order_by(A.id))

        a1 = result.scalars().first()

        a1.data = "new data"

        await session.commit()


asyncio.run(async_main())
