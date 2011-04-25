from sqlalchemy import *
from test.lib import *
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
        # around which dont necessarily 
        # get gc'ed as quickly as we'd like,
        # on backends like pypy, python3.2
        pool_module._refs.clear()

    def setup(self):
        global pool
        pool = QueuePool(creator=self.Connection,
                         pool_size=3, max_overflow=-1,
                         use_threadlocal=True)


    @profiling.function_call_count(72, {'2.4': 63, '2.7':67, 
                                            '2.7+cextension':67,
                                            '3.0':73, '3.1':73, 
                                            '3.2':55},
                                            variance=.10)
    def test_first_connect(self):
        conn = pool.connect()

    def test_second_connect(self):
        conn = pool.connect()
        conn.close()

        @profiling.function_call_count(32, {'2.4': 21, '2.7':29,
                                            '3.2':25,
                                            '2.7+cextension':29},
                                            variance=.10)
        def go():
            conn2 = pool.connect()
            return conn2
        c2 = go()

    def test_second_samethread_connect(self):
        conn = pool.connect()

        @profiling.function_call_count(6, {'2.4': 4, '3':7})
        def go():
            return pool.connect()
        c2 = go()


