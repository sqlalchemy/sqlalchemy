import testenv; testenv.configure_for_tests()
from sqlalchemy import *
from testlib import *
from sqlalchemy.pool import QueuePool


class QueuePoolTest(TestBase, AssertsExecutionResults):
    class Connection(object):
        def close(self):
            pass

    def setUp(self):
        global pool
        pool = QueuePool(creator=self.Connection,
                         pool_size=3, max_overflow=-1,
                         use_threadlocal=True)

    # the WeakValueDictionary used for the pool's "threadlocal" idea adds 1-6
    # method calls to each of these.  however its just a lot easier stability
    # wise than dealing with a strongly referencing dict of weakrefs.
    # [ticket:754] immediately got opened when we tried a dict of weakrefs,
    # and though the solution there is simple, it still doesn't solve the
    # issue of "dead" weakrefs sitting in the dict taking up space

    @profiling.function_call_count(63, {'2.3': 42, '2.4': 43})
    def test_first_connect(self):
        conn = pool.connect()

    def test_second_connect(self):
        conn = pool.connect()
        conn.close()

        @profiling.function_call_count(39, {'2.3': 26, '2.4': 26})
        def go():
            conn2 = pool.connect()
            return conn2
        c2 = go()

    def test_second_samethread_connect(self):
        conn = pool.connect()

        @profiling.function_call_count(7, {'2.3': 4, '2.4': 4})
        def go():
            return pool.connect()
        c2 = go()


if __name__ == '__main__':
    testenv.main()
