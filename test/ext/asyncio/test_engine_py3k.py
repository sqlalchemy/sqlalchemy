import asyncio

from sqlalchemy import Column
from sqlalchemy import create_engine
from sqlalchemy import delete
from sqlalchemy import event
from sqlalchemy import exc
from sqlalchemy import func
from sqlalchemy import Integer
from sqlalchemy import select
from sqlalchemy import String
from sqlalchemy import Table
from sqlalchemy import testing
from sqlalchemy import text
from sqlalchemy import union_all
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.ext.asyncio import engine as _async_engine
from sqlalchemy.ext.asyncio import exc as asyncio_exc
from sqlalchemy.pool import AsyncAdaptedQueuePool
from sqlalchemy.testing import async_test
from sqlalchemy.testing import combinations
from sqlalchemy.testing import engines
from sqlalchemy.testing import eq_
from sqlalchemy.testing import expect_raises
from sqlalchemy.testing import expect_raises_message
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import is_
from sqlalchemy.testing import is_false
from sqlalchemy.testing import is_none
from sqlalchemy.testing import is_not
from sqlalchemy.testing import is_true
from sqlalchemy.testing import mock
from sqlalchemy.testing import ne_
from sqlalchemy.util.concurrency import greenlet_spawn


class EngineFixture(fixtures.TablesTest):
    __requires__ = ("async_dialect",)

    @testing.fixture
    def async_engine(self):
        return engines.testing_engine(asyncio=True, transfer_staticpool=True)

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
        connection.execute(
            users.insert(),
            [{"user_id": i, "user_name": "name%d" % i} for i in range(1, 20)],
        )


class AsyncEngineTest(EngineFixture):
    __backend__ = True

    @testing.fails("the failure is the test")
    @async_test
    async def test_we_are_definitely_running_async_tests(self, async_engine):
        async with async_engine.connect() as conn:
            eq_(await conn.scalar(text("select 1")), 2)

    def test_proxied_attrs_engine(self, async_engine):
        sync_engine = async_engine.sync_engine

        is_(async_engine.url, sync_engine.url)
        is_(async_engine.pool, sync_engine.pool)
        is_(async_engine.dialect, sync_engine.dialect)
        eq_(async_engine.name, sync_engine.name)
        eq_(async_engine.driver, sync_engine.driver)
        eq_(async_engine.echo, sync_engine.echo)

    @async_test
    async def test_engine_eq_ne(self, async_engine):
        e2 = _async_engine.AsyncEngine(async_engine.sync_engine)
        e3 = testing.engines.testing_engine(
            asyncio=True, transfer_staticpool=True
        )

        eq_(async_engine, e2)
        ne_(async_engine, e3)

        is_false(async_engine == None)

    @async_test
    async def test_connection_info(self, async_engine):

        async with async_engine.connect() as conn:
            conn.info["foo"] = "bar"

            eq_(conn.sync_connection.info, {"foo": "bar"})

    @async_test
    async def test_connection_eq_ne(self, async_engine):

        async with async_engine.connect() as conn:
            c2 = _async_engine.AsyncConnection(
                async_engine, conn.sync_connection
            )

            eq_(conn, c2)

            async with async_engine.connect() as c3:
                ne_(conn, c3)

            is_false(conn == None)

    @async_test
    async def test_transaction_eq_ne(self, async_engine):

        async with async_engine.connect() as conn:
            t1 = await conn.begin()

            t2 = _async_engine.AsyncTransaction._from_existing_transaction(
                conn, t1._proxied
            )

            eq_(t1, t2)

            is_false(t1 == None)

    def test_clear_compiled_cache(self, async_engine):
        async_engine.sync_engine._compiled_cache["foo"] = "bar"
        eq_(async_engine.sync_engine._compiled_cache["foo"], "bar")
        async_engine.clear_compiled_cache()
        assert "foo" not in async_engine.sync_engine._compiled_cache

    def test_execution_options(self, async_engine):
        a2 = async_engine.execution_options(foo="bar")
        assert isinstance(a2, _async_engine.AsyncEngine)
        eq_(a2.sync_engine._execution_options, {"foo": "bar"})
        eq_(async_engine.sync_engine._execution_options, {})

        """

            attr uri, pool, dialect, engine, name, driver, echo
            methods clear_compiled_cache, update_execution_options,
            execution_options, get_execution_options, dispose

        """

    @async_test
    async def test_proxied_attrs_connection(self, async_engine):
        conn = await async_engine.connect()

        sync_conn = conn.sync_connection

        is_(conn.engine, async_engine)
        is_(conn.closed, sync_conn.closed)
        is_(conn.dialect, async_engine.sync_engine.dialect)
        eq_(conn.default_isolation_level, sync_conn.default_isolation_level)

    @async_test
    async def test_transaction_accessor(self, async_engine):
        async with async_engine.connect() as conn:
            is_none(conn.get_transaction())
            is_false(conn.in_transaction())
            is_false(conn.in_nested_transaction())

            trans = await conn.begin()

            is_true(conn.in_transaction())
            is_false(conn.in_nested_transaction())

            is_(
                trans.sync_transaction, conn.get_transaction().sync_transaction
            )

            nested = await conn.begin_nested()

            is_true(conn.in_transaction())
            is_true(conn.in_nested_transaction())

            is_(
                conn.get_nested_transaction().sync_transaction,
                nested.sync_transaction,
            )
            eq_(conn.get_nested_transaction(), nested)

            is_(
                trans.sync_transaction, conn.get_transaction().sync_transaction
            )

            await nested.commit()

            is_true(conn.in_transaction())
            is_false(conn.in_nested_transaction())

            await trans.rollback()

            is_none(conn.get_transaction())
            is_false(conn.in_transaction())
            is_false(conn.in_nested_transaction())

    @testing.requires.queue_pool
    @async_test
    async def test_invalidate(self, async_engine):
        conn = await async_engine.connect()

        is_(conn.invalidated, False)

        connection_fairy = await conn.get_raw_connection()
        is_(connection_fairy.is_valid, True)
        dbapi_connection = connection_fairy.connection

        await conn.invalidate()

        if testing.against("postgresql+asyncpg"):
            assert dbapi_connection._connection.is_closed()

        new_fairy = await conn.get_raw_connection()
        is_not(new_fairy.connection, dbapi_connection)
        is_not(new_fairy, connection_fairy)
        is_(new_fairy.is_valid, True)
        is_(connection_fairy.is_valid, False)

    @async_test
    async def test_get_dbapi_connection_raise(self, async_engine):

        conn = await async_engine.connect()

        with testing.expect_raises_message(
            exc.InvalidRequestError,
            "AsyncConnection.connection accessor is not "
            "implemented as the attribute",
        ):
            conn.connection

    @async_test
    async def test_get_raw_connection(self, async_engine):

        conn = await async_engine.connect()

        pooled = await conn.get_raw_connection()
        is_(pooled, conn.sync_connection.connection)

    @async_test
    async def test_isolation_level(self, async_engine):
        conn = await async_engine.connect()

        sync_isolation_level = await greenlet_spawn(
            conn.sync_connection.get_isolation_level
        )
        isolation_level = await conn.get_isolation_level()

        eq_(isolation_level, sync_isolation_level)

        await conn.execution_options(isolation_level="SERIALIZABLE")
        isolation_level = await conn.get_isolation_level()

        eq_(isolation_level, "SERIALIZABLE")

        await conn.close()

    @testing.requires.queue_pool
    @async_test
    async def test_dispose(self, async_engine):
        c1 = await async_engine.connect()
        c2 = await async_engine.connect()

        await c1.close()
        await c2.close()

        p1 = async_engine.pool

        if isinstance(p1, AsyncAdaptedQueuePool):
            eq_(async_engine.pool.checkedin(), 2)

        await async_engine.dispose()
        if isinstance(p1, AsyncAdaptedQueuePool):
            eq_(async_engine.pool.checkedin(), 0)
        is_not(p1, async_engine.pool)

    @testing.requires.independent_connections
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
            with expect_raises_message(
                asyncio_exc.AsyncContextNotStarted,
                "AsyncTransaction context has not been started "
                "and object has not been awaited.",
            ):
                await trans.rollback(),

    @testing.requires.queue_pool
    @async_test
    async def test_pool_exhausted_some_timeout(self, async_engine):
        engine = create_async_engine(
            testing.db.url,
            pool_size=1,
            max_overflow=0,
            pool_timeout=0.1,
        )
        async with engine.connect():
            with expect_raises(exc.TimeoutError):
                await engine.connect()

    @testing.requires.queue_pool
    @async_test
    async def test_pool_exhausted_no_timeout(self, async_engine):
        engine = create_async_engine(
            testing.db.url,
            pool_size=1,
            max_overflow=0,
            pool_timeout=0,
        )
        async with engine.connect():
            with expect_raises(exc.TimeoutError):
                await engine.connect()

    @async_test
    async def test_create_async_engine_server_side_cursor(self, async_engine):
        testing.assert_raises_message(
            asyncio_exc.AsyncMethodRequired,
            "Can't set server_side_cursors for async engine globally",
            create_async_engine,
            testing.db.url,
            server_side_cursors=True,
        )


class AsyncEventTest(EngineFixture):
    """The engine events all run in their normal synchronous context.

    we do not provide an asyncio event interface at this time.

    """

    __backend__ = True

    @async_test
    async def test_no_async_listeners(self, async_engine):
        with testing.expect_raises_message(
            NotImplementedError,
            "asynchronous events are not implemented "
            "at this time.  Apply synchronous listeners to the "
            "AsyncEngine.sync_engine or "
            "AsyncConnection.sync_connection attributes.",
        ):
            event.listen(async_engine, "before_cursor_execute", mock.Mock())

        conn = await async_engine.connect()

        with testing.expect_raises_message(
            NotImplementedError,
            "asynchronous events are not implemented "
            "at this time.  Apply synchronous listeners to the "
            "AsyncEngine.sync_engine or "
            "AsyncConnection.sync_connection attributes.",
        ):
            event.listen(conn, "before_cursor_execute", mock.Mock())

    @async_test
    async def test_sync_before_cursor_execute_engine(self, async_engine):
        canary = mock.Mock()

        event.listen(async_engine.sync_engine, "before_cursor_execute", canary)

        async with async_engine.connect() as conn:
            sync_conn = conn.sync_connection
            await conn.execute(text("select 1"))

        eq_(
            canary.mock_calls,
            [mock.call(sync_conn, mock.ANY, "select 1", (), mock.ANY, False)],
        )

    @async_test
    async def test_sync_before_cursor_execute_connection(self, async_engine):
        canary = mock.Mock()

        async with async_engine.connect() as conn:
            sync_conn = conn.sync_connection

            event.listen(
                async_engine.sync_engine, "before_cursor_execute", canary
            )
            await conn.execute(text("select 1"))

        eq_(
            canary.mock_calls,
            [mock.call(sync_conn, mock.ANY, "select 1", (), mock.ANY, False)],
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

            await result.close()

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

            with expect_raises_message(
                exc.NoResultFound, "No row was found when one was required"
            ):
                await result.one()

    @async_test
    async def test_one_multi_result(self, async_engine):
        users = self.tables.users
        async with async_engine.connect() as conn:
            result = await conn.stream(
                select(users).where(users.c.user_name.in_(["name3", "name5"]))
            )

            with expect_raises_message(
                exc.MultipleResultsFound,
                "Multiple rows were found when exactly one was required",
            ):
                await result.one()


class TextSyncDBAPI(fixtures.TestBase):
    def test_sync_dbapi_raises(self):
        with expect_raises_message(
            exc.InvalidRequestError,
            "The asyncio extension requires an async driver to be used.",
        ):
            create_async_engine("sqlite:///:memory:")

    @testing.fixture
    def async_engine(self):
        engine = create_engine("sqlite:///:memory:", future=True)
        engine.dialect.is_async = True
        return _async_engine.AsyncEngine(engine)

    @async_test
    @combinations(
        lambda conn: conn.exec_driver_sql("select 1"),
        lambda conn: conn.stream(text("select 1")),
        lambda conn: conn.execute(text("select 1")),
        argnames="case",
    )
    async def test_sync_driver_execution(self, async_engine, case):
        with expect_raises_message(
            exc.AwaitRequired,
            "The current operation required an async execution but none was",
        ):
            async with async_engine.connect() as conn:
                await case(conn)

    @async_test
    async def test_sync_driver_run_sync(self, async_engine):
        async with async_engine.connect() as conn:
            res = await conn.run_sync(
                lambda conn: conn.scalar(text("select 1"))
            )
            assert res == 1
            assert await conn.run_sync(lambda _: 2) == 2
