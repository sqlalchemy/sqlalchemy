from typing import Any

from sqlalchemy import Connection
from sqlalchemy import Enum
from sqlalchemy import MetaData
from sqlalchemy import select
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine


def work_sync(conn: Connection, foo: int) -> Any:
    pass


async def asyncio() -> None:
    e = create_async_engine("sqlite://")

    # EXPECTED_TYPE: AsyncEngine
    reveal_type(e)

    async with e.connect() as conn:
        # EXPECTED_TYPE: AsyncConnection
        reveal_type(conn)

        result = await conn.execute(text("select * from table"))

        # EXPECTED_TYPE: CursorResult[Unpack[.*tuple[Any, ...]]]
        reveal_type(result)

        # stream with direct await
        async_result = await conn.stream(text("select * from table"))

        # EXPECTED_TYPE: AsyncResult[Unpack[.*tuple[Any, ...]]]
        reveal_type(async_result)

        # stream with context manager
        async with conn.stream(
            text("select * from table")
        ) as ctx_async_result:
            # EXPECTED_TYPE: AsyncResult[Unpack[.*tuple[Any, ...]]]
            reveal_type(ctx_async_result)

        # stream_scalars with direct await
        async_scalar_result = await conn.stream_scalars(
            text("select * from table")
        )

        # EXPECTED_TYPE: AsyncScalarResult[Any]
        reveal_type(async_scalar_result)

        # stream_scalars with context manager
        async with conn.stream_scalars(
            text("select * from table")
        ) as ctx_async_scalar_result:
            # EXPECTED_TYPE: AsyncScalarResult[Any]
            reveal_type(ctx_async_scalar_result)

    async with e.begin() as conn:
        # EXPECTED_TYPE: AsyncConnection
        reveal_type(conn)

        result = await conn.execute(text("select * from table"))

        # EXPECTED_TYPE: CursorResult[Unpack[.*tuple[Any, ...]]]
        reveal_type(result)

        await conn.run_sync(work_sync, 1)

        # EXPECTED_MYPY: Missing positional argument "foo" in call to "run_sync" of "AsyncConnection"
        await conn.run_sync(work_sync)

    ce = select(1).compile(e)
    ce.statement
    cc = select(1).compile(conn)
    cc.statement

    async with e.connect() as conn:
        metadata = MetaData()

        await conn.run_sync(metadata.create_all)
        await conn.run_sync(metadata.reflect)
        await conn.run_sync(metadata.drop_all)

        # Just to avoid creating new constructs manually:
        for _, table in metadata.tables.items():
            await conn.run_sync(table.create)
            await conn.run_sync(table.drop)

            # Indexes:
            for index in table.indexes:
                await conn.run_sync(index.create)
                await conn.run_sync(index.drop)

        # Test for enum types:
        enum = Enum("a", "b")
        await conn.run_sync(enum.create)
        await conn.run_sync(enum.drop)
