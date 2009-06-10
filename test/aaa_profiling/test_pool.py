from sqlalchemy import *
from sqlalchemy.test import *
from sqlalchemy.pool import QueuePool


class QueuePoolTest(TestBase, AssertsExecutionResults):
    class Connection(object):
        def close(self):
            pass

    def setup(self):
        global pool
        pool = QueuePool(creator=self.Connection,
                         pool_size=3, max_overflow=-1,
                         use_threadlocal=True)


    @profiling.function_call_count(54, {'2.4': 38})
    def test_first_connect(self):
        conn = pool.connect()

    def test_second_connect(self):
        conn = pool.connect()
        conn.close()

        @profiling.function_call_count(31, {'2.4': 21})
        def go():
            conn2 = pool.connect()
            return conn2
        c2 = go()

    def test_second_samethread_connect(self):
        conn = pool.connect()

        @profiling.function_call_count(5, {'2.4': 3})
        def go():
            return pool.connect()
        c2 = go()


