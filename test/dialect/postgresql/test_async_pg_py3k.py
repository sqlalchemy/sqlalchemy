import random

from sqlalchemy import Column
from sqlalchemy import exc
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import String
from sqlalchemy import Table
from sqlalchemy import testing
from sqlalchemy.dialects.postgresql import ENUM
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.testing import async_test
from sqlalchemy.testing import engines
from sqlalchemy.testing import fixtures


class AsyncPgTest(fixtures.TestBase):
    __requires__ = ("async_dialect",)
    __only_on__ = "postgresql+asyncpg"

    @testing.fixture
    def async_engine(self):
        return create_async_engine(testing.db.url)

    @testing.fixture()
    def metadata(self):
        # TODO: remove when Iae6ab95938a7e92b6d42086aec534af27b5577d3
        # merges

        from sqlalchemy.testing import engines
        from sqlalchemy.sql import schema

        metadata = schema.MetaData()

        try:
            yield metadata
        finally:
            engines.drop_all_tables(metadata, testing.db)

    @async_test
    async def test_detect_stale_ddl_cache_raise_recover(
        self, metadata, async_engine
    ):
        async def async_setup(engine, strlen):
            metadata.clear()
            t1 = Table(
                "t1",
                metadata,
                Column("id", Integer, primary_key=True),
                Column("name", String(strlen)),
            )

            # conn is an instance of AsyncConnection
            async with engine.begin() as conn:
                await conn.run_sync(metadata.drop_all)
                await conn.run_sync(metadata.create_all)
                await conn.execute(
                    t1.insert(),
                    [{"name": "some name %d" % i} for i in range(500)],
                )

        meta = MetaData()

        t1 = Table(
            "t1",
            meta,
            Column("id", Integer, primary_key=True),
            Column("name", String),
        )

        await async_setup(async_engine, 30)

        second_engine = engines.testing_engine(asyncio=True)

        async with second_engine.connect() as conn:
            result = await conn.execute(
                t1.select()
                .where(t1.c.name.like("some name%"))
                .where(t1.c.id % 17 == 6)
            )

            rows = result.fetchall()
            assert len(rows) >= 29

        await async_setup(async_engine, 20)

        async with second_engine.connect() as conn:
            with testing.expect_raises_message(
                exc.NotSupportedError,
                r"cached statement plan is invalid due to a database schema "
                r"or configuration change \(SQLAlchemy asyncpg dialect "
                r"will now invalidate all prepared caches in response "
                r"to this exception\)",
            ):

                result = await conn.execute(
                    t1.select()
                    .where(t1.c.name.like("some name%"))
                    .where(t1.c.id % 17 == 6)
                )

        # works again
        async with second_engine.connect() as conn:
            result = await conn.execute(
                t1.select()
                .where(t1.c.name.like("some name%"))
                .where(t1.c.id % 17 == 6)
            )

            rows = result.fetchall()
            assert len(rows) >= 29

    @async_test
    async def test_detect_stale_type_cache_raise_recover(
        self, metadata, async_engine
    ):
        async def async_setup(engine, enums):
            metadata = MetaData()
            Table(
                "t1",
                metadata,
                Column("id", Integer, primary_key=True),
                Column("name", ENUM(*enums, name="my_enum")),
            )

            # conn is an instance of AsyncConnection
            async with engine.begin() as conn:
                await conn.run_sync(metadata.drop_all)
                await conn.run_sync(metadata.create_all)

        t1 = Table(
            "t1",
            metadata,
            Column("id", Integer, primary_key=True),
            Column(
                "name",
                ENUM(
                    *("beans", "means", "keens", "faux", "beau", "flow"),
                    name="my_enum"
                ),
            ),
        )

        await async_setup(async_engine, ("beans", "means", "keens"))

        second_engine = engines.testing_engine(
            asyncio=True,
            options={"connect_args": {"prepared_statement_cache_size": 0}},
        )

        async with second_engine.connect() as conn:
            await conn.execute(
                t1.insert(),
                [
                    {"name": random.choice(("beans", "means", "keens"))}
                    for i in range(10)
                ],
            )

        await async_setup(async_engine, ("faux", "beau", "flow"))

        async with second_engine.connect() as conn:
            with testing.expect_raises_message(
                exc.InternalError, "cache lookup failed for type"
            ):
                await conn.execute(
                    t1.insert(),
                    [
                        {"name": random.choice(("faux", "beau", "flow"))}
                        for i in range(10)
                    ],
                )

        # works again
        async with second_engine.connect() as conn:
            await conn.execute(
                t1.insert(),
                [
                    {"name": random.choice(("faux", "beau", "flow"))}
                    for i in range(10)
                ],
            )
