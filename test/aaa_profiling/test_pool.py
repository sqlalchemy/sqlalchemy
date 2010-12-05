from sqlalchemy import *
from sqlalchemy.test import *
from sqlalchemy.pool import QueuePool


class QueuePoolTest(TestBase, AssertsExecutionResults):
    class Connection(object):
        def rollback(self):
            pass
            
        def close(self):
            pass

    def setup(self):
        global pool
        pool = QueuePool(creator=self.Connection,
                         pool_size=3, max_overflow=-1,
                         use_threadlocal=True)


    @profiling.function_call_count(64, {'2.4': 42, '2.7':59, 
                                            '2.7+cextension':59,
                                            '3.0':65, '3.1':65},
                                            variance=.10)
    def test_first_connect(self):
        conn = pool.connect()

    def test_second_connect(self):
        conn = pool.connect()
        conn.close()

        @profiling.function_call_count(32, {'2.4': 21, '2.7':29,
                                            '2.7+cextension':29},
                                            variance=.10)
        def go():
            conn2 = pool.connect()
            return conn2
        c2 = go()

    def test_second_samethread_connect(self):
        conn = pool.connect()

        @profiling.function_call_count(5, {'2.4': 3, '3.0':6, '3.1':6})
        def go():
            return pool.connect()
        c2 = go()


