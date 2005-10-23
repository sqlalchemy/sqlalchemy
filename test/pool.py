from testbase import PersistTest
import unittest, sys, os

from pysqlite2 import dbapi2 as sqlite
import sqlalchemy.pool as pool

class PoolTest(PersistTest):
    
    def setUp(self):
        pool.clear_managers()
        
    def testmanager(self):
        manager = pool.manage(sqlite)
        
        connection = manager.connect('foo.db')
        connection2 = manager.connect('foo.db')
        connection3 = manager.connect('bar.db')
        
        self.echo( "connection " + repr(connection))
        self.assert_(connection.cursor() is not None)
        self.assert_(connection is connection2)
        self.assert_(connection2 is not connection3)

    def testbadargs(self):
        manager = pool.manage(sqlite)

        try:
            connection = manager.connect(None)
        except:
            pass
    
    def testnonthreadlocalmanager(self):
        manager = pool.manage(sqlite, use_threadlocal = False)
        
        connection = manager.connect('foo.db')
        connection2 = manager.connect('foo.db')

        self.echo( "connection " + repr(connection))

        self.assert_(connection.cursor() is not None)
        self.assert_(connection is not connection2)

    def testqueuepool(self):
        p = pool.QueuePool(creator = lambda: sqlite.connect('foo.db'), pool_size = 3, max_overflow = -1, use_threadlocal = False, echo = False)
    
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
        c4 = c3 = c2 = None
        self.assert_(status(p) == (3,3,3,3))
        c1 = c5 = c6 = None
        self.assert_(status(p) == (3,3,0,0))
        c1 = p.connect()
        c2 = p.connect()
        self.assert_(status(p) == (3, 1, 0, 2))
        c2 = None
        self.assert_(status(p) == (3, 2, 0, 1))
        
    def tearDown(self):
       pool.clear_managers()
       for file in ('foo.db', 'bar.db'):
            if os.access(file, os.F_OK):
                os.remove(file)
        
        
if __name__ == "__main__":
    unittest.main()        
