"""Cancellation safety for the asyncio dialects.

An asyncio task may be cancelled at any ``await``.  These tests cancel a
simple, entirely reasonable piece of user code at each of its IO points in
turn and assert that the connection pool comes out of it intact.

See :mod:`sqlalchemy.testing.cancellation` for how the cancellation point
is made deterministic and for what "intact" means precisely; note in
particular that pool *availability* immediately after a cancellation is
deliberately not asserted anywhere here.

"""

from sqlalchemy import select
from sqlalchemy import testing
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.pool import AsyncAdaptedQueuePool
from sqlalchemy.pool.base import _finalize_fairy
from sqlalchemy.testing import async_test
from sqlalchemy.testing import cancellation
from sqlalchemy.testing import config
from sqlalchemy.testing import eq_
from sqlalchemy.testing import expect_warnings
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import is_true


async def _session_execute_commit(async_engine):
    """The shape reported in discussion #13542."""

    async with AsyncSession(async_engine) as session:
        await session.execute(select(1))
        await session.commit()


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
    @async_test
    async def test_cancel_anywhere_warm_pool(self, engine_factory):
        eq_(
            await cancellation.sweep(
                engine_factory, _session_execute_commit, warm=True
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
                    record.fairy_ref,
                    False,
                    transaction_was_reset=False,
                )

            eq_(accounting.leaked, set())

        await async_engine.dispose()

    @testing.fails_on(
        ["+psycopg", "+oracledb", "+aioodbc"],
        "dialect has not been given AsyncAdapt_terminate; tracked separately",
    )
    def test_dialect_supports_terminate(self):
        is_true(config.db.dialect.has_terminate)
