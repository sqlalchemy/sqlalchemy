"""
Illustrates how to run many statements concurrently using ``asyncio.gather()``
along many asyncio database connections, merging ORM results into a single
``AsyncSession``.

Note that this pattern loses all transactional safety and is also not
necessarily any more performant than using a single Session, as it adds
significant CPU-bound work both to maintain more database connections
and sessions, as well as within the merging of results from external sessions
into one.

Python is a CPU-intensive language even in trivial cases, so it is strongly
recommended that any workarounds for "speed" such as the one below are
carefully vetted to show that they do in fact improve performance vs a
traditional approach.

"""

import asyncio
import random

from sqlalchemy.ext.asyncio import async_sessionmaker
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.future import select
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import merge_frozen_result


class Base(DeclarativeBase):
    pass


class A(Base):
    __tablename__ = "a"

    id: Mapped[int] = mapped_column(primary_key=True)
    data: Mapped[str]

    def __repr__(self):
        id_, data = self.id, self.data
        return f"A({id_=}, {data=})"


async def run_out_of_band(async_sessionmaker, statement, merge_results=True):
    """run an ORM statement in a distinct session,
    returning the frozen results
    """

    async with async_sessionmaker() as oob_session:
        # use AUTOCOMMIT for each connection to reduce transaction
        # overhead / contention
        await oob_session.connection(
            execution_options={"isolation_level": "AUTOCOMMIT"}
        )

        result = await oob_session.execute(statement)

        if merge_results:
            return result.freeze()
        else:
            await result.close()


async def async_main():
    engine = create_async_engine(
        "postgresql+asyncpg://scott:tiger@localhost/test",
        echo=True,
    )

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)

    async_session = async_sessionmaker(engine, expire_on_commit=False)

    async with async_session() as session, session.begin():
        session.add_all([A(data="a_%d" % i) for i in range(100)])

    statements = [
        select(A).where(A.data == "a_%d" % random.choice(range(100)))
        for i in range(30)
    ]

    frozen_results = await asyncio.gather(
        *(
            run_out_of_band(async_session, statement)
            for statement in statements
        )
    )
    results = [
        # merge_results means the ORM objects from the result
        # will be merged back into the original session.
        # load=False means we can use the objects directly without
        # re-selecting them.  however this merge operation is still
        # more expensive CPU-wise than a regular ORM load because the
        # objects are copied into new instances
        (
            await session.run_sync(
                merge_frozen_result, statement, result, load=False
            )
        )()
        for statement, result in zip(statements, frozen_results)
    ]

    print(f"results: {[r.all() for r in results]}")


asyncio.run(async_main())
