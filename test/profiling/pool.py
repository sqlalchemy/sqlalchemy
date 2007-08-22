import testbase
from sqlalchemy import *
from testlib import *
from sqlalchemy.pool import QueuePool
from sqlalchemy.databases import sqlite

class QueuePoolTest(AssertMixin):
    def setUp(self):
        global pool
        pool = QueuePool(creator = lambda: sqlite.SQLiteDialect.dbapi().connect(':memory:'), pool_size = 3, max_overflow = -1, use_threadlocal = True)

    # the WeakValueDictionary used for the pool's "threadlocal" idea adds 1-6 method calls to each of these.
    # however its just a lot easier stability wise than dealing with a strongly referencing dict of weakrefs.
    # [ticket:754] immediately got opened when we tried a dict of weakrefs, and though the solution there
    # is simple, it still doesn't solve the issue of "dead" weakrefs sitting in the dict taking up space
    
    @profiling.profiled('pooltest_connect', call_range=(40, 45), always=True)
    def test_first_connect(self):
        conn = pool.connect()

    def test_second_connect(self):
        conn = pool.connect()
        conn.close()

        @profiling.profiled('pooltest_second_connect', call_range=(24, 24), always=True)
        def go():
            conn2 = pool.connect()
            return conn2
        c2 = go()
        
    def test_second_samethread_connect(self):
        conn = pool.connect()
        
        @profiling.profiled('pooltest_samethread_connect', call_range=(4, 4), always=True)
        def go():
            return pool.connect()
        c2 = go()
            
if __name__ == '__main__':
    testbase.main()        
