from sqlalchemy import create_engine
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine


def regular() -> None:

    e = create_engine("sqlite://")

    # EXPECTED_TYPE: Engine
    reveal_type(e)

    with e.connect() as conn:

        # EXPECTED_TYPE: Connection
        reveal_type(conn)

        result = conn.execute(text("select * from table"))

        # EXPECTED_TYPE: CursorResult[Any]
        reveal_type(result)

    with e.begin() as conn:

        # EXPECTED_TYPE: Connection
        reveal_type(conn)

        result = conn.execute(text("select * from table"))

        # EXPECTED_TYPE: CursorResult[Any]
        reveal_type(result)


async def asyncio() -> None:
    e = create_async_engine("sqlite://")

    # EXPECTED_TYPE: AsyncEngine
    reveal_type(e)

    async with e.connect() as conn:

        # EXPECTED_TYPE: AsyncConnection
        reveal_type(conn)

        result = await conn.execute(text("select * from table"))

        # EXPECTED_TYPE: CursorResult[Any]
        reveal_type(result)

        # stream with direct await
        async_result = await conn.stream(text("select * from table"))

        # EXPECTED_TYPE: AsyncResult[Any]
        reveal_type(async_result)

        # stream with context manager
        async with conn.stream(
            text("select * from table")
        ) as ctx_async_result:
            # EXPECTED_TYPE: AsyncResult[Any]
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

        # EXPECTED_TYPE: CursorResult[Any]
        reveal_type(result)
