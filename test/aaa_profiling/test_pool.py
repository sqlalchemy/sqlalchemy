from sqlalchemy import event
from sqlalchemy.pool import QueuePool
from sqlalchemy.testing import AssertsExecutionResults
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import profiling

pool = None


class QueuePoolTest(fixtures.TestBase, AssertsExecutionResults):
    __requires__ = ("cpython", "python_profiling_backend")

    class Connection:
        def rollback(self):
            pass

        def close(self):
            pass

    def setup_test(self):
        # create a throwaway pool which
        # has the effect of initializing
        # class-level event listeners on Pool,
        # if not present already.
        p1 = QueuePool(creator=self.Connection, pool_size=3, max_overflow=-1)
        p1.connect()

        global pool
        pool = QueuePool(creator=self.Connection, pool_size=3, max_overflow=-1)

        # make this a real world case where we have a "connect" handler
        @event.listens_for(pool, "connect")
        def do_connect(dbapi_conn, conn_record):
            pass

    @profiling.function_call_count(variance=0.10)
    def test_first_connect(self):
        pool.connect()

    def test_second_connect(self):
        conn = pool.connect()
        conn.close()

        @profiling.function_call_count(variance=0.10)
        def go():
            conn2 = pool.connect()
            return conn2

        go()
