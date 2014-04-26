from sqlalchemy import *
from sqlalchemy.testing import *
from sqlalchemy.pool import QueuePool
from sqlalchemy import pool as pool_module

class QueuePoolTest(fixtures.TestBase, AssertsExecutionResults):
    __requires__ = 'cpython',

    class Connection(object):
        def rollback(self):
            pass

        def close(self):
            pass

    def teardown(self):
        # the tests leave some fake connections
        # around which don't necessarily
        # get gc'ed as quickly as we'd like,
        # on backends like pypy, python3.2
        pool_module._refs.clear()

    def setup(self):
        # create a throwaway pool which
        # has the effect of initializing
        # class-level event listeners on Pool,
        # if not present already.
        p1 = QueuePool(creator=self.Connection,
                         pool_size=3, max_overflow=-1,
                         use_threadlocal=True)
        p1.connect()

        global pool
        pool = QueuePool(creator=self.Connection,
                         pool_size=3, max_overflow=-1,
                         use_threadlocal=True)


    @profiling.function_call_count()
    def test_first_connect(self):
        conn = pool.connect()

    def test_second_connect(self):
        conn = pool.connect()
        conn.close()

        @profiling.function_call_count()
        def go():
            conn2 = pool.connect()
            return conn2
        c2 = go()

    def test_second_samethread_connect(self):
        conn = pool.connect()

        @profiling.function_call_count()
        def go():
            return pool.connect()
        c2 = go()


