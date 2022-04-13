"""Illustrates use of the sqlalchemy.ext.asyncio.AsyncSession object
for asynchronous ORM use.

"""
from __future__ import annotations

import asyncio
from typing import List
from typing import TYPE_CHECKING

from sqlalchemy import ForeignKey
from sqlalchemy.ext.asyncio import async_sessionmaker
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.future import select
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import relationship

if TYPE_CHECKING:
    from sqlalchemy import ScalarResult


class Base(DeclarativeBase):
    pass


class A(Base):
    __tablename__ = "a"

    id: Mapped[int] = mapped_column(primary_key=True)
    data: Mapped[str]
    bs: Mapped[List[B]] = relationship()


class B(Base):
    __tablename__ = "b"
    id: Mapped[int] = mapped_column(primary_key=True)
    a_id = mapped_column(ForeignKey("a.id"))
    data: Mapped[str]


async def async_main() -> None:
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

    async with async_session.begin() as session:
        session.add_all(
            [
                A(bs=[B(), B()], data="a1"),
                A(bs=[B()], data="a2"),
                A(bs=[B(), B()], data="a3"),
            ]
        )

    async with async_session() as session:

        result = await session.execute(select(A).order_by(A.id))

        r: ScalarResult[A] = result.scalars()
        a1 = r.one()

        a1.data = "new data"

        await session.commit()


asyncio.run(async_main())
