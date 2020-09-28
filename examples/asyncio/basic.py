"""Illustrates the asyncio engine / connection interface.

In this example, we have an async engine created by
:func:`_engine.create_async_engine`.   We then use it using await
within a coroutine.

"""


import asyncio

from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import String
from sqlalchemy import Table
from sqlalchemy.ext.asyncio import create_async_engine


meta = MetaData()

t1 = Table(
    "t1", meta, Column("id", Integer, primary_key=True), Column("name", String)
)


async def async_main():
    # engine is an instance of AsyncEngine
    engine = create_async_engine(
        "postgresql+asyncpg://scott:tiger@localhost/test",
        echo=True,
    )

    # conn is an instance of AsyncConnection
    async with engine.begin() as conn:

        # to support SQLAlchemy DDL methods as well as legacy functions, the
        # AsyncConnection.run_sync() awaitable method will pass a "sync"
        # version of the AsyncConnection object to any synchronous method,
        # where synchronous IO calls will be transparently translated for
        # await.
        await conn.run_sync(meta.drop_all)
        await conn.run_sync(meta.create_all)

        # for normal statement execution, a traditional "await execute()"
        # pattern is used.
        await conn.execute(
            t1.insert(), [{"name": "some name 1"}, {"name": "some name 2"}]
        )

    async with engine.connect() as conn:

        # the default result object is the
        # sqlalchemy.engine.Result object
        result = await conn.execute(t1.select())

        # the results are buffered so no await call is necessary
        # for this case.
        print(result.fetchall())

        # for a streaming result that buffers only segments of the
        # result at time, the AsyncConnection.stream() method is used.
        # this returns a sqlalchemy.ext.asyncio.AsyncResult object.
        async_result = await conn.stream(t1.select())

        # this object supports async iteration and awaitable
        # versions of methods like .all(), fetchmany(), etc.
        async for row in async_result:
            print(row)


asyncio.run(async_main())
