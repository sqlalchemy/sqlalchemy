import asyncio
import inspect as stdlib_inspect
from unittest.mock import patch

from sqlalchemy import Column
from sqlalchemy import create_engine
from sqlalchemy import delete
from sqlalchemy import event
from sqlalchemy import exc
from sqlalchemy import func
from sqlalchemy import inspect
from sqlalchemy import Integer
from sqlalchemy import select
from sqlalchemy import String
from sqlalchemy import Table
from sqlalchemy import testing
from sqlalchemy import text
from sqlalchemy import union_all
from sqlalchemy.engine import cursor as _cursor
from sqlalchemy.ext.asyncio import async_engine_from_config
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.ext.asyncio import create_async_pool_from_url
from sqlalchemy.ext.asyncio import engine as _async_engine
from sqlalchemy.ext.asyncio import exc as async_exc
from sqlalchemy.ext.asyncio import exc as asyncio_exc
from sqlalchemy.ext.asyncio.base import ReversibleProxy
from sqlalchemy.ext.asyncio.engine import AsyncConnection
from sqlalchemy.ext.asyncio.engine import AsyncEngine
from sqlalchemy.pool import AsyncAdaptedQueuePool
from sqlalchemy.testing import assertions
from sqlalchemy.testing import async_test
from sqlalchemy.testing import combinations
from sqlalchemy.testing import config
from sqlalchemy.testing import engines
from sqlalchemy.testing import eq_
from sqlalchemy.testing import eq_regex
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
from sqlalchemy.util import greenlet_spawn


class AsyncFixture:
    @config.fixture(
        params=[
            (rollback, run_second_execute, begin_nested)
            for rollback in (True, False)
            for run_second_execute in (True, False)
            for begin_nested in (True, False)
        ]
    )
    def async_trans_ctx_manager_fixture(self, request, metadata):
        rollback, run_second_execute, begin_nested = request.param

        t = Table("test", metadata, Column("data", Integer))
        eng = getattr(self, "bind", None) or config.db

        t.create(eng)

        async def run_test(subject, trans_on_subject, execute_on_subject):
            async with subject.begin() as trans:
                if begin_nested:
                    if not config.requirements.savepoints.enabled:
                        config.skip_test("savepoints not enabled")
                    if execute_on_subject:
                        nested_trans = subject.begin_nested()
                    else:
                        nested_trans = trans.begin_nested()

                    async with nested_trans:
                        if execute_on_subject:
                            await subject.execute(t.insert(), {"data": 10})
                        else:
                            await trans.execute(t.insert(), {"data": 10})

                        # for nested trans, we always commit/rollback on the
                        # "nested trans" object itself.
                        # only Session(future=False) will affect savepoint
                        # transaction for session.commit/rollback

                        if rollback:
                            await nested_trans.rollback()
                        else:
                            await nested_trans.commit()

                        if run_second_execute:
                            with assertions.expect_raises_message(
                                exc.InvalidRequestError,
                                "Can't operate on closed transaction "
                                "inside context manager.  Please complete the "
                                "context manager "
                                "before emitting further commands.",
                            ):
                                if execute_on_subject:
                                    await subject.execute(
                                        t.insert(), {"data": 12}
                                    )
                                else:
                                    await trans.execute(
                                        t.insert(), {"data": 12}
                                    )

                    # outside the nested trans block, but still inside the
                    # transaction block, we can run SQL, and it will be
                    # committed
                    if execute_on_subject:
                        await subject.execute(t.insert(), {"data": 14})
                    else:
                        await trans.execute(t.insert(), {"data": 14})

                else:
                    if execute_on_subject:
                        await subject.execute(t.insert(), {"data": 10})
                    else:
                        await trans.execute(t.insert(), {"data": 10})

                    if trans_on_subject:
                        if rollback:
                            await subject.rollback()
                        else:
                            await subject.commit()
                    else:
                        if rollback:
                            await trans.rollback()
                        else:
                            await trans.commit()

                    if run_second_execute:
                        with assertions.expect_raises_message(
                            exc.InvalidRequestError,
                            "Can't operate on closed transaction inside "
                            "context "
                            "manager.  Please complete the context manager "
                            "before emitting further commands.",
                        ):
                            if execute_on_subject:
                                await subject.execute(t.insert(), {"data": 12})
                            else:
                                await trans.execute(t.insert(), {"data": 12})

            expected_committed = 0
            if begin_nested:
                # begin_nested variant, we inserted a row after the nested
                # block
                expected_committed += 1
            if not rollback:
                # not rollback variant, our row inserted in the target
                # block itself would be committed
                expected_committed += 1

            if execute_on_subject:
                eq_(
                    await subject.scalar(select(func.count()).select_from(t)),
                    expected_committed,
                )
            else:
                with subject.connect() as conn:
                    eq_(
                        await conn.scalar(select(func.count()).select_from(t)),
                        expected_committed,
                    )

        return run_test


class EngineFixture(AsyncFixture, fixtures.TablesTest):
    __requires__ = ("async_dialect",)

    @testing.fixture
    def async_engine(self):
        return engines.testing_engine(asyncio=True, transfer_staticpool=True)

    @testing.fixture
    def async_connection(self, async_engine):
        with async_engine.sync_engine.connect() as conn:
            yield AsyncConnection(async_engine, conn)

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

    @async_test
    async def test_interrupt_ctxmanager_connection(
        self, async_engine, async_trans_ctx_manager_fixture
    ):
        fn = async_trans_ctx_manager_fixture

        async with async_engine.connect() as conn:
            await fn(conn, trans_on_subject=False, execute_on_subject=True)

    def test_proxied_attrs_engine(self, async_engine):
        sync_engine = async_engine.sync_engine

        is_(async_engine.url, sync_engine.url)
        is_(async_engine.pool, sync_engine.pool)
        is_(async_engine.dialect, sync_engine.dialect)
        eq_(async_engine.name, sync_engine.name)
        eq_(async_engine.driver, sync_engine.driver)
        eq_(async_engine.echo, sync_engine.echo)

    @async_test
    async def test_run_async(self, async_engine):
        async def test_meth(async_driver_connection):
            # there's no method that's guaranteed to be on every
            # driver, so just stringify it and compare that to the
            # outside
            return str(async_driver_connection)

        def run_sync_to_async(connection):
            connection_fairy = connection.connection
            async_return = connection_fairy.run_async(
                lambda driver_connection: test_meth(driver_connection)
            )
            assert not stdlib_inspect.iscoroutine(async_return)
            return async_return

        async with async_engine.connect() as conn:
            driver_connection = (
                await conn.get_raw_connection()
            ).driver_connection
            res = await conn.run_sync(run_sync_to_async)
            assert not stdlib_inspect.iscoroutine(res)
            eq_(res, str(driver_connection))

    @async_test
    async def test_engine_eq_ne(self, async_engine):
        e2 = _async_engine.AsyncEngine(async_engine.sync_engine)
        e3 = engines.testing_engine(asyncio=True, transfer_staticpool=True)

        eq_(async_engine, e2)
        ne_(async_engine, e3)

        is_false(async_engine == None)

    @async_test
    async def test_no_attach_to_event_loop(self, testing_engine):
        """test #6409"""

        import asyncio
        import threading

        errs = []

        def go():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

            async def main():
                tasks = [task() for _ in range(2)]

                await asyncio.gather(*tasks)
                await engine.dispose()

            async def task():
                async with engine.begin() as connection:
                    result = await connection.execute(select(1))
                    result.all()

            try:
                engine = engines.testing_engine(
                    asyncio=True, transfer_staticpool=False
                )

                asyncio.run(main())
            except Exception as err:
                errs.append(err)

        t = threading.Thread(target=go)
        t.start()
        t.join()

        if errs:
            raise errs[0]

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

            t2 = _async_engine.AsyncTransaction._regenerate_proxy_for_target(
                t1._proxied
            )

            eq_(t1, t2)

            is_false(t1 == None)

    @testing.variation("simulate_gc", [True, False])
    def test_appropriate_warning_for_gced_connection(
        self, async_engine, simulate_gc
    ):
        """test #9237 which builds upon a not really complete solution
        added for #8419."""

        async def go():
            conn = await async_engine.connect()
            await conn.begin()
            await conn.execute(select(1))
            pool_connection = await conn.get_raw_connection()
            return pool_connection

        from sqlalchemy.util.concurrency import await_only

        pool_connection = await_only(go())

        rec = pool_connection._connection_record
        ref = rec.fairy_ref
        pool = pool_connection._pool
        echo = False

        if simulate_gc:
            # not using expect_warnings() here because we also want to do a
            # negative test for warnings, and we want to absolutely make sure
            # the thing here that emits the warning is the correct path
            from sqlalchemy.pool.base import _finalize_fairy

            with mock.patch.object(
                pool._dialect,
                "do_rollback",
                mock.Mock(side_effect=Exception("can't run rollback")),
            ), mock.patch("sqlalchemy.util.warn") as m:
                _finalize_fairy(
                    None, rec, pool, ref, echo, transaction_was_reset=False
                )

            if async_engine.dialect.has_terminate:
                expected_msg = (
                    "The garbage collector is trying to clean up.*which will "
                    "be terminated."
                )
            else:
                expected_msg = (
                    "The garbage collector is trying to clean up.*which will "
                    "be dropped, as it cannot be safely terminated."
                )

            # [1] == .args, not in 3.7
            eq_regex(m.mock_calls[0][1][0], expected_msg)
        else:
            # the warning emitted by the pool is inside of a try/except:
            # so it's impossible right now to have this warning "raise".
            # for now, test by using mock.patch

            with mock.patch("sqlalchemy.util.warn") as m:
                pool_connection.close()

            eq_(m.mock_calls, [])

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
        async with async_engine.connect() as conn:
            sync_conn = conn.sync_connection

            is_(conn.engine, async_engine)
            is_(conn.closed, sync_conn.closed)
            is_(conn.dialect, async_engine.sync_engine.dialect)
            eq_(
                conn.default_isolation_level, sync_conn.default_isolation_level
            )

    @async_test
    async def test_transaction_accessor(self, async_connection):
        conn = async_connection
        is_none(conn.get_transaction())
        is_false(conn.in_transaction())
        is_false(conn.in_nested_transaction())

        trans = await conn.begin()

        is_true(conn.in_transaction())
        is_false(conn.in_nested_transaction())

        is_(trans.sync_transaction, conn.get_transaction().sync_transaction)

        nested = await conn.begin_nested()

        is_true(conn.in_transaction())
        is_true(conn.in_nested_transaction())

        is_(
            conn.get_nested_transaction().sync_transaction,
            nested.sync_transaction,
        )
        eq_(conn.get_nested_transaction(), nested)

        is_(trans.sync_transaction, conn.get_transaction().sync_transaction)

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
        dbapi_connection = connection_fairy.dbapi_connection

        await conn.invalidate()

        if testing.against("postgresql+asyncpg"):
            assert dbapi_connection._connection.is_closed()

        new_fairy = await conn.get_raw_connection()
        is_not(new_fairy.dbapi_connection, dbapi_connection)
        is_not(new_fairy, connection_fairy)
        is_(new_fairy.is_valid, True)
        is_(connection_fairy.is_valid, False)
        await conn.close()

    @async_test
    async def test_get_dbapi_connection_raise(self, async_connection):
        with testing.expect_raises_message(
            exc.InvalidRequestError,
            "AsyncConnection.connection accessor is not "
            "implemented as the attribute",
        ):
            async_connection.connection

    @async_test
    async def test_get_raw_connection(self, async_connection):
        pooled = await async_connection.get_raw_connection()
        is_(pooled, async_connection.sync_connection.connection)

    @async_test
    async def test_isolation_level(self, async_connection):
        conn = async_connection
        sync_isolation_level = await greenlet_spawn(
            conn.sync_connection.get_isolation_level
        )
        isolation_level = await conn.get_isolation_level()

        eq_(isolation_level, sync_isolation_level)

        await conn.execution_options(isolation_level="SERIALIZABLE")
        isolation_level = await conn.get_isolation_level()

        eq_(isolation_level, "SERIALIZABLE")

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

    @testing.requires.queue_pool
    @async_test
    async def test_dispose_no_close(self, async_engine):
        c1 = await async_engine.connect()
        c2 = await async_engine.connect()

        await c1.close()
        await c2.close()

        p1 = async_engine.pool

        if isinstance(p1, AsyncAdaptedQueuePool):
            eq_(async_engine.pool.checkedin(), 2)

        await async_engine.dispose(close=False)

        # TODO: test that DBAPI connection was not closed

        if isinstance(p1, AsyncAdaptedQueuePool):
            eq_(async_engine.pool.checkedin(), 0)
        is_not(p1, async_engine.pool)

    @testing.requires.independent_connections
    @async_test
    async def test_init_once_concurrency(self, async_engine):
        async with async_engine.connect() as c1, async_engine.connect() as c2:
            coro = asyncio.gather(c1.scalar(select(1)), c2.scalar(select(2)))
            eq_(await coro, [1, 2])

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
        with expect_raises_message(
            asyncio_exc.AsyncMethodRequired,
            "Can't set server_side_cursors for async engine globally",
        ):
            create_async_engine(
                testing.db.url,
                server_side_cursors=True,
            )

    def test_async_engine_from_config(self):
        config = {
            "sqlalchemy.url": testing.db.url.render_as_string(
                hide_password=False
            ),
            "sqlalchemy.echo": "true",
        }
        engine = async_engine_from_config(config)
        assert engine.url == testing.db.url
        assert engine.echo is True
        assert engine.dialect.is_async is True

    def test_async_creator_and_creator(self):
        async def ac():
            return None

        def c():
            return None

        with expect_raises_message(
            exc.ArgumentError,
            "Can only specify one of 'async_creator' or 'creator', "
            "not both.",
        ):
            create_async_engine(testing.db.url, creator=c, async_creator=ac)

    @async_test
    async def test_async_creator_invoked(self, async_testing_engine):
        """test for #8215"""

        existing_creator = testing.db.pool._creator

        async def async_creator():
            sync_conn = await greenlet_spawn(existing_creator)
            return sync_conn.driver_connection

        async_creator = mock.Mock(side_effect=async_creator)

        eq_(async_creator.mock_calls, [])

        engine = async_testing_engine(options={"async_creator": async_creator})
        async with engine.connect() as conn:
            result = await conn.scalar(select(1))
            eq_(result, 1)

        eq_(async_creator.mock_calls, [mock.call()])

    @async_test
    async def test_async_creator_accepts_args_if_called_directly(
        self, async_testing_engine
    ):
        """supplemental test for #8215.

        The "async_creator" passed to create_async_engine() is expected to take
        no arguments, the same way as "creator" passed to create_engine()
        works.

        However, the ultimate "async_creator" received by the sync-emulating
        DBAPI *does* take arguments in its ``.connect()`` method, which will be
        all the other arguments passed to ``.connect()``.  This functionality
        is not currently used, however was decided that the creator should
        internally work this way for improved flexibility; see
        https://github.com/sqlalchemy/sqlalchemy/issues/8215#issuecomment-1181791539.
        That contract is tested here.

        """  # noqa: E501

        existing_creator = testing.db.pool._creator

        async def async_creator(x, y, *, z=None):
            sync_conn = await greenlet_spawn(existing_creator)
            return sync_conn.driver_connection

        async_creator = mock.Mock(side_effect=async_creator)

        async_dbapi = testing.db.dialect.loaded_dbapi

        conn = await greenlet_spawn(
            async_dbapi.connect, 5, y=10, z=8, async_creator_fn=async_creator
        )
        try:
            eq_(async_creator.mock_calls, [mock.call(5, y=10, z=8)])
        finally:
            await greenlet_spawn(conn.close)


class AsyncCreatePoolTest(fixtures.TestBase):
    @config.fixture
    def mock_create(self):
        with patch(
            "sqlalchemy.ext.asyncio.engine._create_pool_from_url",
        ) as p:
            yield p

    def test_url_only(self, mock_create):
        create_async_pool_from_url("sqlite://")
        mock_create.assert_called_once_with("sqlite://", _is_async=True)

    def test_pool_args(self, mock_create):
        create_async_pool_from_url("sqlite://", foo=99, echo=True)
        mock_create.assert_called_once_with(
            "sqlite://", foo=99, echo=True, _is_async=True
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

        async with async_engine.connect() as conn:
            with testing.expect_raises_message(
                NotImplementedError,
                "asynchronous events are not implemented "
                "at this time.  Apply synchronous listeners to the "
                "AsyncEngine.sync_engine or "
                "AsyncConnection.sync_connection attributes.",
            ):
                event.listen(conn, "before_cursor_execute", mock.Mock())

    @async_test
    async def test_no_async_listeners_dialect_event(self, async_engine):
        with testing.expect_raises_message(
            NotImplementedError,
            "asynchronous events are not implemented "
            "at this time.  Apply synchronous listeners to the "
            "AsyncEngine.sync_engine or "
            "AsyncConnection.sync_connection attributes.",
        ):
            event.listen(async_engine, "do_execute", mock.Mock())

    @async_test
    async def test_no_async_listeners_pool_event(self, async_engine):
        with testing.expect_raises_message(
            NotImplementedError,
            "asynchronous events are not implemented "
            "at this time.  Apply synchronous listeners to the "
            "AsyncEngine.sync_engine or "
            "AsyncConnection.sync_connection attributes.",
        ):
            event.listen(async_engine, "checkout", mock.Mock())

    @async_test
    async def test_sync_before_cursor_execute_engine(self, async_engine):
        canary = mock.Mock()

        event.listen(async_engine.sync_engine, "before_cursor_execute", canary)

        async with async_engine.connect() as conn:
            sync_conn = conn.sync_connection
            await conn.execute(text("select 1"))

        eq_(
            canary.mock_calls,
            [
                mock.call(
                    sync_conn, mock.ANY, "select 1", mock.ANY, mock.ANY, False
                )
            ],
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
            [
                mock.call(
                    sync_conn, mock.ANY, "select 1", mock.ANY, mock.ANY, False
                )
            ],
        )

    @async_test
    async def test_event_on_sync_connection(self, async_engine):
        canary = mock.Mock()

        async with async_engine.connect() as conn:
            event.listen(conn.sync_connection, "begin", canary)
            async with conn.begin():
                eq_(
                    canary.mock_calls,
                    [mock.call(conn.sync_connection)],
                )


class AsyncInspection(EngineFixture):
    __backend__ = True

    @async_test
    async def test_inspect_engine(self, async_engine):
        with testing.expect_raises_message(
            exc.NoInspectionAvailable,
            "Inspection on an AsyncEngine is currently not supported.",
        ):
            inspect(async_engine)

    @async_test
    async def test_inspect_connection(self, async_engine):
        async with async_engine.connect() as conn:
            with testing.expect_raises_message(
                exc.NoInspectionAvailable,
                "Inspection on an AsyncConnection is currently not supported.",
            ):
                inspect(conn)


class AsyncResultTest(EngineFixture):
    @async_test
    async def test_no_ss_cursor_w_execute(self, async_engine):
        users = self.tables.users
        async with async_engine.connect() as conn:
            conn = await conn.execution_options(stream_results=True)
            with expect_raises_message(
                async_exc.AsyncMethodRequired,
                r"Can't use the AsyncConnection.execute\(\) method with a "
                r"server-side cursor. Use the AsyncConnection.stream\(\) "
                r"method for an async streaming result set.",
            ):
                await conn.execute(select(users))

    @async_test
    async def test_no_ss_cursor_w_exec_driver_sql(self, async_engine):
        async with async_engine.connect() as conn:
            conn = await conn.execution_options(stream_results=True)
            with expect_raises_message(
                async_exc.AsyncMethodRequired,
                r"Can't use the AsyncConnection.exec_driver_sql\(\) "
                r"method with a "
                r"server-side cursor. Use the AsyncConnection.stream\(\) "
                r"method for an async streaming result set.",
            ):
                await conn.exec_driver_sql("SELECT * FROM users")

    @async_test
    async def test_stream_ctxmanager(self, async_engine):
        async with async_engine.connect() as conn:
            conn = await conn.execution_options(stream_results=True)

            async with conn.stream(select(self.tables.users)) as result:
                assert not result._real_result._soft_closed
                assert not result.closed
                with expect_raises_message(Exception, "hi"):
                    i = 0
                    async for row in result:
                        if i > 2:
                            raise Exception("hi")
                        i += 1
            assert result._real_result._soft_closed
            assert result.closed

    @async_test
    async def test_stream_scalars_ctxmanager(self, async_engine):
        async with async_engine.connect() as conn:
            conn = await conn.execution_options(stream_results=True)

            async with conn.stream_scalars(
                select(self.tables.users)
            ) as result:
                assert not result._real_result._soft_closed
                assert not result.closed
                with expect_raises_message(Exception, "hi"):
                    i = 0
                    async for scalar in result:
                        if i > 2:
                            raise Exception("hi")
                        i += 1
            assert result._real_result._soft_closed
            assert result.closed

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
        (None,),
        ("scalars",),
        ("stream_scalars",),
        ("mappings",),
        argnames="filter_",
    )
    @async_test
    async def test_aiter(self, async_engine, filter_):
        users = self.tables.users
        async with async_engine.connect() as conn:
            if filter_ == "stream_scalars":
                result = await conn.stream_scalars(select(users.c.user_name))
            else:
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
            elif filter_ in ("scalars", "stream_scalars"):
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
    @testing.combinations(None, 2, 5, 10, argnames="yield_per")
    @testing.combinations("method", "opt", argnames="yield_per_type")
    @async_test
    async def test_partitions(
        self, async_engine, filter_, yield_per, yield_per_type
    ):
        users = self.tables.users
        async with async_engine.connect() as conn:
            stmt = select(users)
            if yield_per and yield_per_type == "opt":
                stmt = stmt.execution_options(yield_per=yield_per)
            result = await conn.stream(stmt)

            if filter_ == "mappings":
                result = result.mappings()
            elif filter_ == "scalars":
                result = result.scalars(1)

            if yield_per and yield_per_type == "method":
                result = result.yield_per(yield_per)

            check_result = []

            # stream() sets stream_results unconditionally
            assert isinstance(
                result._real_result.cursor_strategy,
                _cursor.BufferedRowCursorFetchStrategy,
            )

            if yield_per:
                partition_size = yield_per

                eq_(result._real_result.cursor_strategy._bufsize, yield_per)

                async for partition in result.partitions():
                    check_result.append(partition)
            else:
                eq_(result._real_result.cursor_strategy._bufsize, 5)

                partition_size = 5
                async for partition in result.partitions(partition_size):
                    check_result.append(partition)

            ranges = [
                (i, min(20, i + partition_size))
                for i in range(1, 21, partition_size)
            ]

            if filter_ == "mappings":
                eq_(
                    check_result,
                    [
                        [
                            {"user_id": i, "user_name": "name%d" % i}
                            for i in range(a, b)
                        ]
                        for (a, b) in ranges
                    ],
                )
            elif filter_ == "scalars":
                eq_(
                    check_result,
                    [["name%d" % i for i in range(a, b)] for (a, b) in ranges],
                )
            else:
                eq_(
                    check_result,
                    [
                        [(i, "name%d" % i) for i in range(a, b)]
                        for (a, b) in ranges
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

    @testing.combinations(
        ("scalars",), ("stream_scalars",), argnames="filter_"
    )
    @async_test
    async def test_scalars(self, async_engine, filter_):
        users = self.tables.users
        async with async_engine.connect() as conn:
            if filter_ == "scalars":
                result = (await conn.scalars(select(users))).all()
            elif filter_ == "stream_scalars":
                result = await (await conn.stream_scalars(select(users))).all()

        eq_(result, list(range(1, 20)))


class TextSyncDBAPI(fixtures.TestBase):
    __requires__ = ("asyncio",)

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


class AsyncProxyTest(EngineFixture, fixtures.TestBase):
    @async_test
    async def test_get_transaction(self, async_engine):
        async with async_engine.connect() as conn:
            async with conn.begin() as trans:
                is_(trans.connection, conn)
                is_(conn.get_transaction(), trans)

    @async_test
    async def test_get_nested_transaction(self, async_engine):
        async with async_engine.connect() as conn:
            async with conn.begin() as trans:
                n1 = await conn.begin_nested()

                is_(conn.get_nested_transaction(), n1)

                n2 = await conn.begin_nested()

                is_(conn.get_nested_transaction(), n2)

                await n2.commit()

                is_(conn.get_nested_transaction(), n1)

                is_(conn.get_transaction(), trans)

    @async_test
    async def test_get_connection(self, async_engine):
        async with async_engine.connect() as conn:
            is_(
                AsyncConnection._retrieve_proxy_for_target(
                    conn.sync_connection
                ),
                conn,
            )

    def test_regenerate_connection(self, connection):
        async_connection = AsyncConnection._retrieve_proxy_for_target(
            connection
        )

        a2 = AsyncConnection._retrieve_proxy_for_target(connection)
        is_(async_connection, a2)
        is_not(async_connection, None)

        is_(async_connection.engine, a2.engine)
        is_not(async_connection.engine, None)

    @testing.requires.predictable_gc
    @async_test
    async def test_gc_engine(self, testing_engine):
        ReversibleProxy._proxy_objects.clear()

        eq_(len(ReversibleProxy._proxy_objects), 0)

        async_engine = AsyncEngine(testing.db)

        eq_(len(ReversibleProxy._proxy_objects), 1)

        del async_engine

        eq_(len(ReversibleProxy._proxy_objects), 0)

    @testing.requires.predictable_gc
    @async_test
    async def test_gc_conn(self, testing_engine):
        ReversibleProxy._proxy_objects.clear()

        async_engine = AsyncEngine(testing.db)

        eq_(len(ReversibleProxy._proxy_objects), 1)

        async with async_engine.connect() as conn:
            eq_(len(ReversibleProxy._proxy_objects), 2)

            async with conn.begin() as trans:
                eq_(len(ReversibleProxy._proxy_objects), 3)

            del trans

        del conn

        eq_(len(ReversibleProxy._proxy_objects), 1)

        del async_engine

        eq_(len(ReversibleProxy._proxy_objects), 0)

    def test_regen_conn_but_not_engine(self, async_engine):
        with async_engine.sync_engine.connect() as sync_conn:
            async_conn = AsyncConnection._retrieve_proxy_for_target(sync_conn)
            async_conn2 = AsyncConnection._retrieve_proxy_for_target(sync_conn)

            is_(async_conn, async_conn2)
            is_(async_conn.engine, async_engine)

    def test_regen_trans_but_not_conn(self, connection_no_trans):
        sync_conn = connection_no_trans

        async_conn = AsyncConnection._retrieve_proxy_for_target(sync_conn)

        trans = sync_conn.begin()

        async_t1 = async_conn.get_transaction()

        is_(async_t1.connection, async_conn)
        is_(async_t1.sync_transaction, trans)

        async_t2 = async_conn.get_transaction()
        is_(async_t1, async_t2)
