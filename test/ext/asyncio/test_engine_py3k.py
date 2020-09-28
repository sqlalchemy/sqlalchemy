import asyncio

from sqlalchemy import Column
from sqlalchemy import delete
from sqlalchemy import exc
from sqlalchemy import func
from sqlalchemy import Integer
from sqlalchemy import select
from sqlalchemy import String
from sqlalchemy import Table
from sqlalchemy import testing
from sqlalchemy import union_all
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.ext.asyncio import exc as asyncio_exc
from sqlalchemy.testing import async_test
from sqlalchemy.testing import eq_
from sqlalchemy.testing import fixtures
from sqlalchemy.testing.asyncio import assert_raises_message_async


class EngineFixture(fixtures.TablesTest):
    __requires__ = ("async_dialect",)

    @testing.fixture
    def async_engine(self):
        return create_async_engine(testing.db.url)

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "users",
            metadata,
            Column("user_id", Integer, primary_key=True, autoincrement=False),
            Column("user_name", String(20)),
        )

    @classmethod
    def insert_data(cls, connection):
        users = cls.tables.users
        with connection.begin():
            connection.execute(
                users.insert(),
                [
                    {"user_id": i, "user_name": "name%d" % i}
                    for i in range(1, 20)
                ],
            )


class AsyncEngineTest(EngineFixture):
    __backend__ = True

    @async_test
    async def test_init_once_concurrency(self, async_engine):
        c1 = async_engine.connect()
        c2 = async_engine.connect()
        await asyncio.wait([c1, c2])

    @async_test
    async def test_connect_ctxmanager(self, async_engine):
        async with async_engine.connect() as conn:
            result = await conn.execute(select(1))
            eq_(result.scalar(), 1)

    @async_test
    async def test_connect_plain(self, async_engine):
        conn = await async_engine.connect()
        try:
            result = await conn.execute(select(1))
            eq_(result.scalar(), 1)
        finally:
            await conn.close()

    @async_test
    async def test_connection_not_started(self, async_engine):

        conn = async_engine.connect()
        testing.assert_raises_message(
            asyncio_exc.AsyncContextNotStarted,
            "AsyncConnection context has not been started and "
            "object has not been awaited.",
            conn.begin,
        )

    @async_test
    async def test_transaction_commit(self, async_engine):
        users = self.tables.users

        async with async_engine.begin() as conn:
            await conn.execute(delete(users))

        async with async_engine.connect() as conn:
            eq_(await conn.scalar(select(func.count(users.c.user_id))), 0)

    @async_test
    async def test_savepoint_rollback_noctx(self, async_engine):
        users = self.tables.users

        async with async_engine.begin() as conn:

            savepoint = await conn.begin_nested()
            await conn.execute(delete(users))
            await savepoint.rollback()

        async with async_engine.connect() as conn:
            eq_(await conn.scalar(select(func.count(users.c.user_id))), 19)

    @async_test
    async def test_savepoint_commit_noctx(self, async_engine):
        users = self.tables.users

        async with async_engine.begin() as conn:

            savepoint = await conn.begin_nested()
            await conn.execute(delete(users))
            await savepoint.commit()

        async with async_engine.connect() as conn:
            eq_(await conn.scalar(select(func.count(users.c.user_id))), 0)

    @async_test
    async def test_transaction_rollback(self, async_engine):
        users = self.tables.users

        async with async_engine.connect() as conn:
            trans = conn.begin()
            await trans.start()
            await conn.execute(delete(users))
            await trans.rollback()

        async with async_engine.connect() as conn:
            eq_(await conn.scalar(select(func.count(users.c.user_id))), 19)

    @async_test
    async def test_conn_transaction_not_started(self, async_engine):

        async with async_engine.connect() as conn:
            trans = conn.begin()
            await assert_raises_message_async(
                asyncio_exc.AsyncContextNotStarted,
                "AsyncTransaction context has not been started "
                "and object has not been awaited.",
                trans.rollback(),
            )

    @async_test
    async def test_pool_exhausted(self, async_engine):
        engine = create_async_engine(
            testing.db.url,
            pool_size=1,
            max_overflow=0,
            pool_timeout=0.1,
        )
        async with engine.connect():
            await assert_raises_message_async(
                asyncio.TimeoutError,
                "",
                engine.connect(),
            )

    @async_test
    async def test_create_async_engine_server_side_cursor(self, async_engine):
        testing.assert_raises_message(
            asyncio_exc.AsyncMethodRequired,
            "Can't set server_side_cursors for async engine globally",
            create_async_engine,
            testing.db.url,
            server_side_cursors=True,
        )


class AsyncResultTest(EngineFixture):
    @testing.combinations(
        (None,), ("scalars",), ("mappings",), argnames="filter_"
    )
    @async_test
    async def test_all(self, async_engine, filter_):
        users = self.tables.users
        async with async_engine.connect() as conn:
            result = await conn.stream(select(users))

            if filter_ == "mappings":
                result = result.mappings()
            elif filter_ == "scalars":
                result = result.scalars(1)

            all_ = await result.all()
            if filter_ == "mappings":
                eq_(
                    all_,
                    [
                        {"user_id": i, "user_name": "name%d" % i}
                        for i in range(1, 20)
                    ],
                )
            elif filter_ == "scalars":
                eq_(
                    all_,
                    ["name%d" % i for i in range(1, 20)],
                )
            else:
                eq_(all_, [(i, "name%d" % i) for i in range(1, 20)])

    @testing.combinations(
        (None,), ("scalars",), ("mappings",), argnames="filter_"
    )
    @async_test
    async def test_aiter(self, async_engine, filter_):
        users = self.tables.users
        async with async_engine.connect() as conn:
            result = await conn.stream(select(users))

            if filter_ == "mappings":
                result = result.mappings()
            elif filter_ == "scalars":
                result = result.scalars(1)

            rows = []

            async for row in result:
                rows.append(row)

            if filter_ == "mappings":
                eq_(
                    rows,
                    [
                        {"user_id": i, "user_name": "name%d" % i}
                        for i in range(1, 20)
                    ],
                )
            elif filter_ == "scalars":
                eq_(
                    rows,
                    ["name%d" % i for i in range(1, 20)],
                )
            else:
                eq_(rows, [(i, "name%d" % i) for i in range(1, 20)])

    @testing.combinations((None,), ("mappings",), argnames="filter_")
    @async_test
    async def test_keys(self, async_engine, filter_):
        users = self.tables.users
        async with async_engine.connect() as conn:
            result = await conn.stream(select(users))

            if filter_ == "mappings":
                result = result.mappings()

            eq_(result.keys(), ["user_id", "user_name"])

    @async_test
    async def test_unique_all(self, async_engine):
        users = self.tables.users
        async with async_engine.connect() as conn:
            result = await conn.stream(
                union_all(select(users), select(users)).order_by(
                    users.c.user_id
                )
            )

            all_ = await result.unique().all()
            eq_(all_, [(i, "name%d" % i) for i in range(1, 20)])

    @async_test
    async def test_columns_all(self, async_engine):
        users = self.tables.users
        async with async_engine.connect() as conn:
            result = await conn.stream(select(users))

            all_ = await result.columns(1).all()
            eq_(all_, [("name%d" % i,) for i in range(1, 20)])

    @testing.combinations(
        (None,), ("scalars",), ("mappings",), argnames="filter_"
    )
    @async_test
    async def test_partitions(self, async_engine, filter_):
        users = self.tables.users
        async with async_engine.connect() as conn:
            result = await conn.stream(select(users))

            if filter_ == "mappings":
                result = result.mappings()
            elif filter_ == "scalars":
                result = result.scalars(1)

            check_result = []
            async for partition in result.partitions(5):
                check_result.append(partition)

            if filter_ == "mappings":
                eq_(
                    check_result,
                    [
                        [
                            {"user_id": i, "user_name": "name%d" % i}
                            for i in range(a, b)
                        ]
                        for (a, b) in [(1, 6), (6, 11), (11, 16), (16, 20)]
                    ],
                )
            elif filter_ == "scalars":
                eq_(
                    check_result,
                    [
                        ["name%d" % i for i in range(a, b)]
                        for (a, b) in [(1, 6), (6, 11), (11, 16), (16, 20)]
                    ],
                )
            else:
                eq_(
                    check_result,
                    [
                        [(i, "name%d" % i) for i in range(a, b)]
                        for (a, b) in [(1, 6), (6, 11), (11, 16), (16, 20)]
                    ],
                )

    @testing.combinations(
        (None,), ("scalars",), ("mappings",), argnames="filter_"
    )
    @async_test
    async def test_one_success(self, async_engine, filter_):
        users = self.tables.users
        async with async_engine.connect() as conn:
            result = await conn.stream(
                select(users).limit(1).order_by(users.c.user_name)
            )

            if filter_ == "mappings":
                result = result.mappings()
            elif filter_ == "scalars":
                result = result.scalars()
            u1 = await result.one()

            if filter_ == "mappings":
                eq_(u1, {"user_id": 1, "user_name": "name%d" % 1})
            elif filter_ == "scalars":
                eq_(u1, 1)
            else:
                eq_(u1, (1, "name%d" % 1))

    @async_test
    async def test_one_no_result(self, async_engine):
        users = self.tables.users
        async with async_engine.connect() as conn:
            result = await conn.stream(
                select(users).where(users.c.user_name == "nonexistent")
            )

            async def go():
                await result.one()

            await assert_raises_message_async(
                exc.NoResultFound,
                "No row was found when one was required",
                go(),
            )

    @async_test
    async def test_one_multi_result(self, async_engine):
        users = self.tables.users
        async with async_engine.connect() as conn:
            result = await conn.stream(
                select(users).where(users.c.user_name.in_(["name3", "name5"]))
            )

            async def go():
                await result.one()

            await assert_raises_message_async(
                exc.MultipleResultsFound,
                "Multiple rows were found when exactly one was required",
                go(),
            )
