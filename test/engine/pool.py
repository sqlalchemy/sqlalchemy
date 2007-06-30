import testbase
from testbase import PersistTest
import unittest, sys, os, time
import threading, thread

import sqlalchemy.pool as pool
import sqlalchemy.exceptions as exceptions

mcid = 1
class MockDBAPI(object):
    def __init__(self):
        self.throw_error = False
    def connect(self, argument):
        if self.throw_error:
            raise Exception("couldnt connect !")
        return MockConnection()
class MockConnection(object):
    def __init__(self):
        global mcid
        self.id = mcid
        self.closed = False
        mcid += 1
    def close(self):
        self.closed = True
    def rollback(self):
        pass
    def cursor(self):
        return MockCursor()
class MockCursor(object):
    def close(self):
        pass
mock_dbapi = MockDBAPI()
         
class PoolTest(PersistTest):
    
    def setUp(self):
        pool.clear_managers()

    def testmanager(self):
        manager = pool.manage(mock_dbapi, use_threadlocal=True)
        
        connection = manager.connect('foo.db')
        connection2 = manager.connect('foo.db')
        connection3 = manager.connect('bar.db')
        
        self.echo( "connection " + repr(connection))
        self.assert_(connection.cursor() is not None)
        self.assert_(connection is connection2)
        self.assert_(connection2 is not connection3)

    def testbadargs(self):
        manager = pool.manage(mock_dbapi)

        try:
            connection = manager.connect(None)
        except:
            pass
    
    def testnonthreadlocalmanager(self):
        manager = pool.manage(mock_dbapi, use_threadlocal = False)
        
        connection = manager.connect('foo.db')
        connection2 = manager.connect('foo.db')

        self.echo( "connection " + repr(connection))

        self.assert_(connection.cursor() is not None)
        self.assert_(connection is not connection2)

    def testqueuepool_del(self):
        self._do_testqueuepool(useclose=False)

    def testqueuepool_close(self):
        self._do_testqueuepool(useclose=True)

    def _do_testqueuepool(self, useclose=False):
        p = pool.QueuePool(creator = lambda: mock_dbapi.connect('foo.db'), pool_size = 3, max_overflow = -1, use_threadlocal = False)
    
        def status(pool):
            tup = (pool.size(), pool.checkedin(), pool.overflow(), pool.checkedout())
            self.echo( "Pool size: %d  Connections in pool: %d Current Overflow: %d Current Checked out connections: %d" % tup)
            return tup
                
        c1 = p.connect()
        self.assert_(status(p) == (3,0,-2,1))
        c2 = p.connect()
        self.assert_(status(p) == (3,0,-1,2))
        c3 = p.connect()
        self.assert_(status(p) == (3,0,0,3))
        c4 = p.connect()
        self.assert_(status(p) == (3,0,1,4))
        c5 = p.connect()
        self.assert_(status(p) == (3,0,2,5))
        c6 = p.connect()
        self.assert_(status(p) == (3,0,3,6))
        if useclose:
            c4.close()
            c3.close()
            c2.close()
        else:
            c4 = c3 = c2 = None
        self.assert_(status(p) == (3,3,3,3))
        if useclose:
            c1.close()
            c5.close()
            c6.close()
        else:
            c1 = c5 = c6 = None
        self.assert_(status(p) == (3,3,0,0))
        c1 = p.connect()
        c2 = p.connect()
        self.assert_(status(p) == (3, 1, 0, 2))
        if useclose:
            c2.close()
        else:
            c2 = None
        self.assert_(status(p) == (3, 2, 0, 1))
    
    def test_timeout(self):
        p = pool.QueuePool(creator = lambda: mock_dbapi.connect('foo.db'), pool_size = 3, max_overflow = 0, use_threadlocal = False, timeout=2)
        c1 = p.get()
        c2 = p.get()
        c3 = p.get()
        now = time.time()
        try:
            c4 = p.get()
            assert False
        except exceptions.TimeoutError, e:
            assert int(time.time() - now) == 2

    def _test_overflow(self, thread_count, max_overflow):
        def creator():
            time.sleep(.05)
            return mock_dbapi.connect('foo.db')
            
        p = pool.QueuePool(creator=creator,
                           pool_size=3, timeout=2,
                           max_overflow=max_overflow)
        peaks = []
        def whammy():
            for i in range(10):
                try:
                    con = p.connect()
                    time.sleep(.005)
                    peaks.append(p.overflow())
                    con.close()
                    del con
                except exceptions.TimeoutError:
                    pass
        threads = []
        for i in xrange(thread_count):
            th = threading.Thread(target=whammy)
            th.start()
            threads.append(th)
        for th in threads:
            th.join()

        self.assert_(max(peaks) <= max_overflow)

    def test_no_overflow(self):
        self._test_overflow(40, 0)

    def test_max_overflow(self):
        self._test_overflow(40, 5)
        
    def test_mixed_close(self):
        p = pool.QueuePool(creator = lambda: mock_dbapi.connect('foo.db'), pool_size = 3, max_overflow = -1, use_threadlocal = True)
        c1 = p.connect()
        c2 = p.connect()
        assert c1 is c2
        c1.close()
        c2 = None
        assert p.checkedout() == 1
        c1 = None
        assert p.checkedout() == 0
    
    def test_trick_the_counter(self):
        """this is a "flaw" in the connection pool; since threadlocal uses a single ConnectionFairy per thread
        with an open/close counter, you can fool the counter into giving you a ConnectionFairy with an
        ambiguous counter.  i.e. its not true reference counting."""
        p = pool.QueuePool(creator = lambda: mock_dbapi.connect('foo.db'), pool_size = 3, max_overflow = -1, use_threadlocal = True)
        c1 = p.connect()
        c2 = p.connect()
        assert c1 is c2
        c1.close()
        c2 = p.connect()
        c2.close()
        self.assert_(p.checkedout() != 0)

        c2.close()
        self.assert_(p.checkedout() == 0)

    def test_recycle(self):
        p = pool.QueuePool(creator = lambda: mock_dbapi.connect('foo.db'), pool_size = 1, max_overflow = 0, use_threadlocal = False, recycle=3)
        
        c1 = p.connect()
        c_id = id(c1.connection)
        c1.close()
        c2 = p.connect()
        assert id(c2.connection) == c_id
        c2.close()
        time.sleep(4)
        c3= p.connect()
        assert id(c3.connection) != c_id
    
    def test_invalidate(self):
        dbapi = MockDBAPI()
        p = pool.QueuePool(creator = lambda: dbapi.connect('foo.db'), pool_size = 1, max_overflow = 0, use_threadlocal = False)
        c1 = p.connect()
        c_id = c1.connection.id
        c1.close(); c1=None
        c1 = p.connect()
        assert c1.connection.id == c_id
        c1.invalidate()
        c1 = None
        
        c1 = p.connect()
        assert c1.connection.id != c_id

    def test_recreate(self):
        dbapi = MockDBAPI()
        p = pool.QueuePool(creator = lambda: dbapi.connect('foo.db'), pool_size = 1, max_overflow = 0, use_threadlocal = False)
        p2 = p.recreate()
        assert p2.size() == 1
        assert p2._use_threadlocal is False
        assert p2._max_overflow == 0
        
    def test_reconnect(self):
        """tests reconnect operations at the pool level.  SA's engine/dialect includes another 
        layer of reconnect support for 'database was lost' errors."""
        dbapi = MockDBAPI()
        p = pool.QueuePool(creator = lambda: dbapi.connect('foo.db'), pool_size = 1, max_overflow = 0, use_threadlocal = False)
        c1 = p.connect()
        c_id = c1.connection.id
        c1.close(); c1=None

        c1 = p.connect()
        assert c1.connection.id == c_id
        dbapi.raise_error = True
        c1.invalidate()
        c1 = None

        c1 = p.connect()
        assert c1.connection.id != c_id

    def test_detach(self):
        dbapi = MockDBAPI()
        p = pool.QueuePool(creator = lambda: dbapi.connect('foo.db'), pool_size = 1, max_overflow = 0, use_threadlocal = False)

        c1 = p.connect()
        c1.detach()
        c_id = c1.connection.id

        c2 = p.connect()
        assert c2.connection.id != c1.connection.id
        dbapi.raise_error = True

        c2.invalidate()
        c2 = None

        c2 = p.connect()
        assert c2.connection.id != c1.connection.id

        con = c1.connection

        assert not con.closed
        c1.close()
        assert con.closed
        
    def testthreadlocal_del(self):
        self._do_testthreadlocal(useclose=False)

    def testthreadlocal_close(self):
        self._do_testthreadlocal(useclose=True)

    def _do_testthreadlocal(self, useclose=False):
        for p in (
            pool.QueuePool(creator = lambda: mock_dbapi.connect('foo.db'), pool_size = 3, max_overflow = -1, use_threadlocal = True),
            pool.SingletonThreadPool(creator = lambda: mock_dbapi.connect('foo.db'), use_threadlocal = True)
        ):   
            c1 = p.connect()
            c2 = p.connect()
            self.assert_(c1 is c2)
            c3 = p.unique_connection()
            self.assert_(c3 is not c1)
            if useclose:
                c2.close()
            else:
                c2 = None
            c2 = p.connect()
            self.assert_(c1 is c2)
            self.assert_(c3 is not c1)
            if useclose:
                c2.close()
            else:
                c2 = None
        
            if useclose:
                c1 = p.connect()
                c2 = p.connect()
                c3 = p.connect()
                c3.close()
                c2.close()
                self.assert_(c1.connection is not None)
                c1.close()

            c1 = c2 = c3 = None
            
            # extra tests with QueuePool to insure connections get __del__()ed when dereferenced
            if isinstance(p, pool.QueuePool):
                self.assert_(p.checkedout() == 0)
                c1 = p.connect()
                c2 = p.connect()
                if useclose:
                    c2.close()
                    c1.close()
                else:
                    c2 = None
                    c1 = None
                self.assert_(p.checkedout() == 0)
            
    def tearDown(self):
       pool.clear_managers()
        
        
if __name__ == "__main__":
    testbase.main()        
