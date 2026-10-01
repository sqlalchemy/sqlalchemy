"""Cancellation safety for the asyncio dialects.

An asyncio task may be cancelled at any ``await``.  These tests cancel a
simple, entirely reasonable piece of user code at each of its IO points in
turn and assert that the connection pool comes out of it intact.

See :mod:`sqlalchemy.testing.cancellation` for how the cancellation point
is made deterministic and for what "intact" means precisely.

"""

import asyncio

from sqlalchemy import select
from sqlalchemy import testing
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.asyncio.base import _run_to_completion
from sqlalchemy.pool import AsyncAdaptedQueuePool
from sqlalchemy.pool.base import _finalize_fairy
from sqlalchemy.testing import async_test
from sqlalchemy.testing import cancellation
from sqlalchemy.testing import config
from sqlalchemy.testing import eq_
from sqlalchemy.testing import expect_raises
from sqlalchemy.testing import expect_warnings
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import is_true


async def _session_execute_commit(async_engine):
    """A fixture that illustrates the session execute / commit pattern
    shown in discussion #13542."""

    async with AsyncSession(async_engine) as session:
        await session.execute(select(1))
        await session.commit()


async def _connect_execute(async_engine):
    """A fixture that illustrates the connection / execute pattern shown
    in issue #12710."""

    async with async_engine.connect() as conn:
        await conn.execute(select(1))


class AsyncCancellationTest(fixtures.TestBase):
    __requires__ = ("async_dialect",)
    __backend__ = True

    @config.fixture()
    def engine_factory(self, async_testing_engine):
        # the pool class is pinned so that what these tests measure is the
        # dialect's behavior rather than whichever pool a given URL
        # defaults to; a StaticPool never releases its one connection and
        # so cannot exhibit these failures at all.  pool_timeout is given
        # explicitly and kept short as testing_engine() otherwise sets it
        # to zero, which a pool of size one cannot tolerate even when
        # nothing has gone wrong.
        return lambda: async_testing_engine(
            options={
                "poolclass": AsyncAdaptedQueuePool,
                "pool_size": 1,
                "max_overflow": 0,
                "pool_timeout": 1,
            }
        )

    @testing.crashes(
        "+aioodbc",
        "aioodbc abandons the cancelled pyodbc call in its executor "
        "thread; the next call on that connection frees the ODBC "
        "statement handle underneath it and msodbcsql segfaults",
    )
    @async_test
    async def test_cancel_anywhere(self, engine_factory):
        eq_(
            await cancellation.sweep(
                engine_factory, _session_execute_commit, warm=False
            ),
            [],
        )

    @testing.crashes(
        "+aioodbc",
        "aioodbc abandons the cancelled pyodbc call in its executor "
        "thread; the next call on that connection frees the ODBC "
        "statement handle underneath it and msodbcsql segfaults",
    )
    @testing.combinations((False,), (True,), argnames="warm")
    @async_test
    async def test_cancel_anywhere_connection(self, engine_factory, warm):
        """#12710"""
        eq_(
            await cancellation.sweep(
                engine_factory, _connect_execute, warm=warm
            ),
            [],
        )

    @testing.crashes(
        "+aioodbc",
        "aioodbc abandons the cancelled pyodbc call in its executor "
        "thread; the next call on that connection frees the ODBC "
        "statement handle underneath it and msodbcsql segfaults",
    )
    @async_test
    async def test_cancel_anywhere_warm_pool(self, engine_factory):
        eq_(
            await cancellation.sweep(
                engine_factory, _session_execute_commit, warm=True
            ),
            [],
        )

    @testing.only_on(
        ["postgresql", "oracle"],
        "terminate of a connection whose close was cancelled is only "
        "robust on the PostgreSQL and Oracle async drivers; aiosqlite "
        "deadlocks waiting on its stopped worker thread",
    )
    @testing.combinations((1,), (2,), argnames="number_of_cancels")
    @async_test
    async def test_cancel_anywhere_recycle(
        self, engine_factory, number_of_cancels
    ):
        """checkout closes the invalidated connection before opening a new
        one; with two cancellations the second lands in the cleanup of the
        first."""

        eq_(
            await cancellation.sweep(
                engine_factory,
                _session_execute_commit,
                warm=True,
                recycle=True,
                cancels=number_of_cancels,
            ),
            [],
        )

    @testing.only_on(
        ["postgresql", "oracle"],
        "terminate of a connection whose close was cancelled is only "
        "robust on the PostgreSQL and Oracle async drivers; aiosqlite "
        "deadlocks waiting on its stopped worker thread",
    )
    @testing.combinations((False,), (True,), argnames="warm")
    @async_test
    async def test_cancel_anywhere_twice(self, engine_factory, warm):
        eq_(
            await cancellation.sweep(
                engine_factory, _session_execute_commit, warm=warm, cancels=2
            ),
            [],
        )

    @testing.fails_if(
        lambda config: not config.db.dialect.has_terminate,
        "dialect provides no AsyncAdapt_terminate; tracked separately",
    )
    @async_test
    async def test_gc_of_checked_out_connection(self, engine_factory):
        async_engine = engine_factory()
        with cancellation.connection_accounting(async_engine) as accounting:
            pool_connection = await async_engine.raw_connection()
            record = pool_connection._connection_record

            record.fairy_ref = lambda: None
            assert record.needs_gc

            # invoke the finalizer the way the garbage collector would,
            # rather than dropping the reference and collecting, so that
            # the warning below is raised somewhere it can be caught; a
            # warning raised from within a weakref callback is unraisable
            with expect_warnings(
                "The garbage collector is trying to clean up.*"
            ):
                _finalize_fairy(
                    None,
                    record,
                    pool_connection._pool,
                    False,
                    transaction_was_reset=False,
                    is_gc_cleanup=True,
                )

            eq_(accounting.leaked, set())

        await async_engine.dispose()

    @testing.fails_on(
        ["+psycopg", "+oracledb<26", "+aioodbc"],
        "dialect has not been given AsyncAdapt_terminate; tracked separately",
    )
    def test_dialect_supports_terminate(self):
        is_true(config.db.dialect.has_terminate)


class RunToCompletionTest(fixtures.TestBase):
    """cleanup run by _run_to_completion() is waited for even when the
    calling task is cancelled.  #12710"""

    __requires__ = ("asyncio",)

    async def _run(self, number_of_cancels):
        started = asyncio.Event()
        release = asyncio.Event()
        completed = []

        async def cleanup():
            started.set()
            await release.wait()
            completed.append(True)
            return "result"

        async def caller():
            return await _run_to_completion(cleanup())

        task = asyncio.create_task(caller())
        await started.wait()
        for _ in range(number_of_cancels):
            task.cancel()
            await asyncio.sleep(0.01)

        # the caller has not returned while its cleanup is still running
        is_true(not task.done())

        release.set()
        return task, completed

    @async_test
    async def test_not_cancelled(self):
        task, completed = await self._run(0)
        eq_(await task, "result")
        eq_(completed, [True])

    @testing.combinations((1,), (2,), argnames="number_of_cancels")
    @async_test
    async def test_cancelled(self, number_of_cancels):
        task, completed = await self._run(number_of_cancels)
        with expect_raises(asyncio.CancelledError, check_context=False):
            await task
        eq_(completed, [True])

    @async_test
    async def test_cleanup_raises(self):
        async def cleanup():
            raise ValueError("cleanup failed")

        with expect_raises(ValueError):
            await _run_to_completion(cleanup())
