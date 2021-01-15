import random

from sqlalchemy import Column
from sqlalchemy import exc
from sqlalchemy import ForeignKey
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import select
from sqlalchemy import String
from sqlalchemy import Table
from sqlalchemy import testing
from sqlalchemy.dialects.postgresql import ENUM
from sqlalchemy.testing import async_test
from sqlalchemy.testing import eq_
from sqlalchemy.testing import fixtures


class AsyncPgTest(fixtures.TestBase):
    __requires__ = ("async_dialect",)
    __only_on__ = "postgresql+asyncpg"

    @async_test
    async def test_detect_stale_ddl_cache_raise_recover(
        self, metadata, async_testing_engine
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

        first_engine = async_testing_engine()
        second_engine = async_testing_engine()

        await async_setup(first_engine, 30)

        async with second_engine.connect() as conn:
            result = await conn.execute(
                t1.select()
                .where(t1.c.name.like("some name%"))
                .where(t1.c.id % 17 == 6)
            )

            rows = result.fetchall()
            assert len(rows) >= 29

        await async_setup(first_engine, 20)

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
        self, metadata, async_testing_engine
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

        first_engine = async_testing_engine()
        second_engine = async_testing_engine(
            options={"connect_args": {"prepared_statement_cache_size": 0}}
        )

        await async_setup(first_engine, ("beans", "means", "keens"))

        async with second_engine.connect() as conn:
            await conn.execute(
                t1.insert(),
                [
                    {"name": random.choice(("beans", "means", "keens"))}
                    for i in range(10)
                ],
            )

        await async_setup(first_engine, ("faux", "beau", "flow"))

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

    @async_test
    async def test_failed_commit_recover(self, metadata, async_testing_engine):

        Table("t1", metadata, Column("id", Integer, primary_key=True))

        t2 = Table(
            "t2",
            metadata,
            Column("id", Integer, primary_key=True),
            Column(
                "t1_id",
                Integer,
                ForeignKey("t1.id", deferrable=True, initially="deferred"),
            ),
        )

        engine = async_testing_engine()

        async with engine.connect() as conn:
            await conn.run_sync(metadata.create_all)

            await conn.execute(t2.insert().values(id=1, t1_id=2))

            with testing.expect_raises_message(
                exc.IntegrityError, 'insert or update on table "t2"'
            ):
                await conn.commit()

            await conn.rollback()

            eq_((await conn.execute(select(1))).scalar(), 1)

    @async_test
    async def test_rollback_twice_no_problem(
        self, metadata, async_testing_engine
    ):

        engine = async_testing_engine()

        async with engine.connect() as conn:

            trans = await conn.begin()

            await trans.rollback()

            await conn.rollback()

    @async_test
    async def test_closed_during_execute(self, metadata, async_testing_engine):

        engine = async_testing_engine()

        async with engine.connect() as conn:
            await conn.begin()

            with testing.expect_raises_message(
                exc.DBAPIError, "connection was closed"
            ):
                await conn.exec_driver_sql(
                    "select pg_terminate_backend(pg_backend_pid())"
                )

    @async_test
    async def test_failed_rollback_recover(
        self, metadata, async_testing_engine
    ):

        engine = async_testing_engine()

        async with engine.connect() as conn:
            await conn.begin()

            (await conn.execute(select(1))).scalar()

            raw_connection = await conn.get_raw_connection()
            # close the asyncpg transaction directly
            await raw_connection._transaction.rollback()

            with testing.expect_raises_message(
                exc.InterfaceError, "already rolled back"
            ):
                await conn.rollback()

            # recovers no problem

            await conn.begin()
            await conn.rollback()
