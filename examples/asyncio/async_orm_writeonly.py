"""Illustrates using **write only relationships** for simpler handling
of ORM collections under asyncio.

"""

from __future__ import annotations

import asyncio
import datetime
from typing import Optional

from sqlalchemy import ForeignKey
from sqlalchemy import func
from sqlalchemy.ext.asyncio import async_sessionmaker
from sqlalchemy.ext.asyncio import AsyncAttrs
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.future import select
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import relationship
from sqlalchemy.orm import WriteOnlyMapped


class Base(AsyncAttrs, DeclarativeBase):
    pass


class A(Base):
    __tablename__ = "a"

    id: Mapped[int] = mapped_column(primary_key=True)
    data: Mapped[Optional[str]]
    create_date: Mapped[datetime.datetime] = mapped_column(
        server_default=func.now()
    )

    # collection relationships are declared with WriteOnlyMapped.  There
    # is no separate collection type
    bs: WriteOnlyMapped[B] = relationship()


class B(Base):
    __tablename__ = "b"
    id: Mapped[int] = mapped_column(primary_key=True)
    a_id: Mapped[int] = mapped_column(ForeignKey("a.id"))
    data: Mapped[Optional[str]]


async def async_main():
    """Main program function."""

    engine = create_async_engine(
        "postgresql+asyncpg://scott:tiger@localhost/test",
        echo=True,
    )

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    async_session = async_sessionmaker(engine, expire_on_commit=False)

    async with async_session() as session:
        async with session.begin():
            # WriteOnlyMapped may be populated using any iterable,
            # e.g. lists, sets, etc.
            session.add_all(
                [
                    A(bs=[B(), B()], data="a1"),
                    A(bs=[B()], data="a2"),
                    A(bs=[B(), B()], data="a3"),
                ]
            )

        stmt = select(A)

        result = await session.scalars(stmt)

        for a1 in result:
            print(a1)
            print(f"created at: {a1.create_date}")

            # to iterate a collection, emit a SELECT statement
            for b1 in await session.scalars(a1.bs.select()):
                print(b1)

        result = await session.stream(stmt)

        async for a1 in result.scalars():
            print(a1)

            # similar using "streaming" (server side cursors)
            async for b1 in (await session.stream(a1.bs.select())).scalars():
                print(b1)

        await session.commit()
        result = await session.scalars(select(A).order_by(A.id))

        a1 = result.first()

        a1.data = "new data"


asyncio.run(async_main())
