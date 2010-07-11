import threading, time
from sqlalchemy import pool, interfaces, create_engine, select
import sqlalchemy as tsa
from sqlalchemy.test import TestBase, testing
from sqlalchemy.test.util import gc_collect, lazy_gc
from sqlalchemy.test.testing import eq_

mcid = 1
class MockDBAPI(object):
    def __init__(self):
        self.throw_error = False
    def connect(self, *args, **kwargs):
        if self.throw_error:
            raise Exception("couldnt connect !")
        delay = kwargs.pop('delay', 0)
        if delay:
            time.sleep(delay)
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
    def execute(self, *args, **kw):
        pass
    def close(self):
        pass
mock_dbapi = MockDBAPI()


class PoolTestBase(TestBase):    
    def setup(self):
        pool.clear_managers()
        
    @classmethod
    def teardown_class(cls):
       pool.clear_managers()

class PoolTest(PoolTestBase):
    def testmanager(self):
        manager = pool.manage(mock_dbapi, use_threadlocal=True)

        connection = manager.connect('foo.db')
        connection2 = manager.connect('foo.db')
        connection3 = manager.connect('bar.db')

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

        self.assert_(connection.cursor() is not None)
        self.assert_(connection is not connection2)

    @testing.fails_on('+pyodbc',
                      "pyodbc cursor doesn't implement tuple __eq__")
    def test_cursor_iterable(self):
        conn = testing.db.raw_connection()
        cursor = conn.cursor()
        cursor.execute(str(select([1], bind=testing.db)))
        expected = [(1, )]
        for row in cursor:
            eq_(row, expected.pop(0))
    
    def test_no_connect_on_recreate(self):
        def creator():
            raise Exception("no creates allowed")
        
        for cls in (pool.SingletonThreadPool, pool.StaticPool, 
                    pool.QueuePool, pool.NullPool, pool.AssertionPool):
            p = cls(creator=creator)
            p.dispose()
            p.recreate()
        
            mock_dbapi = MockDBAPI()
            p = cls(creator=mock_dbapi.connect)
            conn = p.connect()
            conn.close()
            mock_dbapi.throw_error = True
            p.dispose()
            p.recreate()
            
            
    def testthreadlocal_del(self):
        self._do_testthreadlocal(useclose=False)

    def testthreadlocal_close(self):
        self._do_testthreadlocal(useclose=True)

    def _do_testthreadlocal(self, useclose=False):
        for p in pool.QueuePool(creator=mock_dbapi.connect,
                                pool_size=3, max_overflow=-1,
                                use_threadlocal=True), \
            pool.SingletonThreadPool(creator=mock_dbapi.connect,
                use_threadlocal=True):
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
                lazy_gc()
            if useclose:
                c1 = p.connect()
                c2 = p.connect()
                c3 = p.connect()
                c3.close()
                c2.close()
                self.assert_(c1.connection is not None)
                c1.close()
            c1 = c2 = c3 = None

            # extra tests with QueuePool to ensure connections get
            # __del__()ed when dereferenced

            if isinstance(p, pool.QueuePool):
                lazy_gc()
                self.assert_(p.checkedout() == 0)
                c1 = p.connect()
                c2 = p.connect()
                if useclose:
                    c2.close()
                    c1.close()
                else:
                    c2 = None
                    c1 = None
                    lazy_gc()
                self.assert_(p.checkedout() == 0)

    def test_properties(self):
        dbapi = MockDBAPI()
        p = pool.QueuePool(creator=lambda: dbapi.connect('foo.db'),
                           pool_size=1, max_overflow=0, use_threadlocal=False)

        c = p.connect()
        self.assert_(not c.info)
        self.assert_(c.info is c._connection_record.info)

        c.info['foo'] = 'bar'
        c.close()
        del c

        c = p.connect()
        self.assert_('foo' in c.info)

        c.invalidate()
        c = p.connect()
        self.assert_('foo' not in c.info)

        c.info['foo2'] = 'bar2'
        c.detach()
        self.assert_('foo2' in c.info)

        c2 = p.connect()
        self.assert_(c.connection is not c2.connection)
        self.assert_(not c2.info)
        self.assert_('foo2' in c.info)

    def test_listeners(self):
        dbapi = MockDBAPI()

        class InstrumentingListener(object):
            def __init__(self):
                if hasattr(self, 'connect'):
                    self.connect = self.inst_connect
                if hasattr(self, 'first_connect'):
                    self.first_connect = self.inst_first_connect
                if hasattr(self, 'checkout'):
                    self.checkout = self.inst_checkout
                if hasattr(self, 'checkin'):
                    self.checkin = self.inst_checkin
                self.clear()
            def clear(self):
                self.connected = []
                self.first_connected = []
                self.checked_out = []
                self.checked_in = []
            def assert_total(innerself, conn, fconn, cout, cin):
                eq_(len(innerself.connected), conn)
                eq_(len(innerself.first_connected), fconn)
                eq_(len(innerself.checked_out), cout)
                eq_(len(innerself.checked_in), cin)
            def assert_in(innerself, item, in_conn, in_fconn, 
                                                in_cout, in_cin):
                self.assert_((item in innerself.connected) == in_conn)
                self.assert_((item in innerself.first_connected) == in_fconn)
                self.assert_((item in innerself.checked_out) == in_cout)
                self.assert_((item in innerself.checked_in) == in_cin)
            def inst_connect(self, con, record):
                print "connect(%s, %s)" % (con, record)
                assert con is not None
                assert record is not None
                self.connected.append(con)
            def inst_first_connect(self, con, record):
                print "first_connect(%s, %s)" % (con, record)
                assert con is not None
                assert record is not None
                self.first_connected.append(con)
            def inst_checkout(self, con, record, proxy):
                print "checkout(%s, %s, %s)" % (con, record, proxy)
                assert con is not None
                assert record is not None
                assert proxy is not None
                self.checked_out.append(con)
            def inst_checkin(self, con, record):
                print "checkin(%s, %s)" % (con, record)
                # con can be None if invalidated
                assert record is not None
                self.checked_in.append(con)

        class ListenAll(tsa.interfaces.PoolListener, InstrumentingListener):
            pass
        class ListenConnect(InstrumentingListener):
            def connect(self, con, record):
                pass
        class ListenFirstConnect(InstrumentingListener):
            def first_connect(self, con, record):
                pass
        class ListenCheckOut(InstrumentingListener):
            def checkout(self, con, record, proxy, num):
                pass
        class ListenCheckIn(InstrumentingListener):
            def checkin(self, con, record):
                pass

        def _pool(**kw):
            return pool.QueuePool(creator=lambda: dbapi.connect('foo.db'),
                                  use_threadlocal=False, **kw)

        def assert_listeners(p, total, conn, fconn, cout, cin):
            for instance in (p, p.recreate()):
                self.assert_(len(instance.listeners) == total)
                self.assert_(len(instance._on_connect) == conn)
                self.assert_(len(instance._on_first_connect) == fconn)
                self.assert_(len(instance._on_checkout) == cout)
                self.assert_(len(instance._on_checkin) == cin)

        p = _pool()
        assert_listeners(p, 0, 0, 0, 0, 0)

        p.add_listener(ListenAll())
        assert_listeners(p, 1, 1, 1, 1, 1)

        p.add_listener(ListenConnect())
        assert_listeners(p, 2, 2, 1, 1, 1)

        p.add_listener(ListenFirstConnect())
        assert_listeners(p, 3, 2, 2, 1, 1)

        p.add_listener(ListenCheckOut())
        assert_listeners(p, 4, 2, 2, 2, 1)

        p.add_listener(ListenCheckIn())
        assert_listeners(p, 5, 2, 2, 2, 2)
        del p

        snoop = ListenAll()
        p = _pool(listeners=[snoop])
        assert_listeners(p, 1, 1, 1, 1, 1)

        c = p.connect()
        snoop.assert_total(1, 1, 1, 0)
        cc = c.connection
        snoop.assert_in(cc, True, True, True, False)
        c.close()
        snoop.assert_in(cc, True, True, True, True)
        del c, cc

        snoop.clear()

        # this one depends on immediate gc
        c = p.connect()
        cc = c.connection
        snoop.assert_in(cc, False, False, True, False)
        snoop.assert_total(0, 0, 1, 0)
        del c, cc
        lazy_gc()
        snoop.assert_total(0, 0, 1, 1)

        p.dispose()
        snoop.clear()

        c = p.connect()
        c.close()
        c = p.connect()
        snoop.assert_total(1, 0, 2, 1)
        c.close()
        snoop.assert_total(1, 0, 2, 2)

        # invalidation
        p.dispose()
        snoop.clear()

        c = p.connect()
        snoop.assert_total(1, 0, 1, 0)
        c.invalidate()
        snoop.assert_total(1, 0, 1, 1)
        c.close()
        snoop.assert_total(1, 0, 1, 1)
        del c
        lazy_gc()
        snoop.assert_total(1, 0, 1, 1)
        c = p.connect()
        snoop.assert_total(2, 0, 2, 1)
        c.close()
        del c
        lazy_gc()
        snoop.assert_total(2, 0, 2, 2)

        # detached
        p.dispose()
        snoop.clear()

        c = p.connect()
        snoop.assert_total(1, 0, 1, 0)
        c.detach()
        snoop.assert_total(1, 0, 1, 0)
        c.close()
        del c
        snoop.assert_total(1, 0, 1, 0)
        c = p.connect()
        snoop.assert_total(2, 0, 2, 0)
        c.close()
        del c
        snoop.assert_total(2, 0, 2, 1)

        # recreated
        p = p.recreate()
        snoop.clear()

        c = p.connect()
        snoop.assert_total(1, 1, 1, 0)
        c.close()
        snoop.assert_total(1, 1, 1, 1)
        c = p.connect()
        snoop.assert_total(1, 1, 2, 1)
        c.close()
        snoop.assert_total(1, 1, 2, 2)
    
    def test_listeners_callables(self):
        dbapi = MockDBAPI()

        def connect(dbapi_con, con_record):
            counts[0] += 1
        def checkout(dbapi_con, con_record, con_proxy):
            counts[1] += 1
        def checkin(dbapi_con, con_record):
            counts[2] += 1

        i_all = dict(connect=connect, checkout=checkout, checkin=checkin)
        i_connect = dict(connect=connect)
        i_checkout = dict(checkout=checkout)
        i_checkin = dict(checkin=checkin)

        for cls in (pool.QueuePool, pool.StaticPool):
            counts = [0, 0, 0]
            def _pool(**kw):
                return cls(creator=lambda: dbapi.connect('foo.db'),
                                      use_threadlocal=False, **kw)

            def assert_listeners(p, total, conn, cout, cin):
                for instance in (p, p.recreate()):
                    self.assert_(len(instance.listeners) == total)
                    self.assert_(len(instance._on_connect) == conn)
                    self.assert_(len(instance._on_checkout) == cout)
                    self.assert_(len(instance._on_checkin) == cin)

            p = _pool()
            assert_listeners(p, 0, 0, 0, 0)

            p.add_listener(i_all)
            assert_listeners(p, 1, 1, 1, 1)

            p.add_listener(i_connect)
            assert_listeners(p, 2, 2, 1, 1)

            p.add_listener(i_checkout)
            assert_listeners(p, 3, 2, 2, 1)

            p.add_listener(i_checkin)
            assert_listeners(p, 4, 2, 2, 2)
            del p

            p = _pool(listeners=[i_all])
            assert_listeners(p, 1, 1, 1, 1)

            c = p.connect()
            assert counts == [1, 1, 0]
            c.close()
            assert counts == [1, 1, 1]

            c = p.connect()
            assert counts == [1, 2, 1]
            p.add_listener(i_checkin)
            c.close()
            assert counts == [1, 2, 3]

    def test_listener_after_oninit(self):
        """Test that listeners are called after OnInit is removed"""
        called = []
        def listener(*args):
            called.append(True)
        listener.connect = listener
        engine = create_engine(testing.db.url)
        engine.pool.add_listener(listener)
        engine.execute(select([1])).close()
        assert called, "Listener not called on connect"


class QueuePoolTest(PoolTestBase):

    def testqueuepool_del(self):
        self._do_testqueuepool(useclose=False)

    def testqueuepool_close(self):
        self._do_testqueuepool(useclose=True)

    def _do_testqueuepool(self, useclose=False):
        p = pool.QueuePool(creator=mock_dbapi.connect, pool_size=3,
                           max_overflow=-1, use_threadlocal=False)

        def status(pool):
            tup = pool.size(), pool.checkedin(), pool.overflow(), \
                pool.checkedout()
            print 'Pool size: %d  Connections in pool: %d Current '\
                'Overflow: %d Current Checked out connections: %d' % tup
            return tup

        c1 = p.connect()
        self.assert_(status(p) == (3, 0, -2, 1))
        c2 = p.connect()
        self.assert_(status(p) == (3, 0, -1, 2))
        c3 = p.connect()
        self.assert_(status(p) == (3, 0, 0, 3))
        c4 = p.connect()
        self.assert_(status(p) == (3, 0, 1, 4))
        c5 = p.connect()
        self.assert_(status(p) == (3, 0, 2, 5))
        c6 = p.connect()
        self.assert_(status(p) == (3, 0, 3, 6))
        if useclose:
            c4.close()
            c3.close()
            c2.close()
        else:
            c4 = c3 = c2 = None
            lazy_gc()
        self.assert_(status(p) == (3, 3, 3, 3))
        if useclose:
            c1.close()
            c5.close()
            c6.close()
        else:
            c1 = c5 = c6 = None
            lazy_gc()
        self.assert_(status(p) == (3, 3, 0, 0))
        c1 = p.connect()
        c2 = p.connect()
        self.assert_(status(p) == (3, 1, 0, 2), status(p))
        if useclose:
            c2.close()
        else:
            c2 = None
            lazy_gc()
        self.assert_(status(p) == (3, 2, 0, 1))
        c1.close()
        lazy_gc()
        assert not pool._refs
       
    def test_timeout(self):
        p = pool.QueuePool(creator=mock_dbapi.connect, pool_size=3,
                           max_overflow=0, use_threadlocal=False,
                           timeout=2)
        c1 = p.connect()
        c2 = p.connect()
        c3 = p.connect()
        now = time.time()
        try:
            c4 = p.connect()
            assert False
        except tsa.exc.TimeoutError, e:
            assert int(time.time() - now) == 2

    def test_timeout_race(self):
        # test a race condition where the initial connecting threads all race
        # to queue.Empty, then block on the mutex.  each thread consumes a
        # connection as they go in.  when the limit is reached, the remaining
        # threads go in, and get TimeoutError; even though they never got to
        # wait for the timeout on queue.get().  the fix involves checking the
        # timeout again within the mutex, and if so, unlocking and throwing
        # them back to the start of do_get()
        p = pool.QueuePool(
                creator = lambda: mock_dbapi.connect(delay=.05), 
                pool_size = 2, 
                max_overflow = 1, use_threadlocal = False, timeout=3)
        timeouts = []
        def checkout():
            for x in xrange(1):
                now = time.time()
                try:
                    c1 = p.connect()
                except tsa.exc.TimeoutError, e:
                    timeouts.append(time.time() - now)
                    continue
                time.sleep(4)
                c1.close()  

        threads = []
        for i in xrange(10):
            th = threading.Thread(target=checkout)
            th.start()
            threads.append(th)
        for th in threads:
            th.join() 

        assert len(timeouts) > 0
        for t in timeouts:
            assert t >= 3, "Not all timeouts were >= 3 seconds %r" % timeouts
            # normally, the timeout should under 4 seconds,
            # but on a loaded down buildbot it can go up.
            assert t < 10, "Not all timeouts were < 10 seconds %r" % timeouts

    def _test_overflow(self, thread_count, max_overflow):
        gc_collect()
        
        def creator():
            time.sleep(.05)
            return mock_dbapi.connect()
 
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
                except tsa.exc.TimeoutError:
                    pass
        threads = []
        for i in xrange(thread_count):
            th = threading.Thread(target=whammy)
            th.start()
            threads.append(th)
        for th in threads:
            th.join()
 
        self.assert_(max(peaks) <= max_overflow)
        
        lazy_gc()
        assert not pool._refs
 
    def test_no_overflow(self):
        self._test_overflow(40, 0)
 
    def test_max_overflow(self):
        self._test_overflow(40, 5)
 
    def test_mixed_close(self):
        p = pool.QueuePool(creator=mock_dbapi.connect, pool_size=3,
                           max_overflow=-1, use_threadlocal=True)
        c1 = p.connect()
        c2 = p.connect()
        assert c1 is c2
        c1.close()
        c2 = None
        assert p.checkedout() == 1
        c1 = None
        lazy_gc()
        assert p.checkedout() == 0
        lazy_gc()
        assert not pool._refs

    def test_weakref_kaboom(self):
        p = pool.QueuePool(creator=mock_dbapi.connect, pool_size=3,
                           max_overflow=-1, use_threadlocal=True)
        c1 = p.connect()
        c2 = p.connect()
        c1.close()
        c2 = None
        del c1
        del c2
        gc_collect()
        assert p.checkedout() == 0
        c3 = p.connect()
        assert c3 is not None

    def test_trick_the_counter(self):
        """this is a "flaw" in the connection pool; since threadlocal
        uses a single ConnectionFairy per thread with an open/close
        counter, you can fool the counter into giving you a
        ConnectionFairy with an ambiguous counter.  i.e. its not true
        reference counting."""

        p = pool.QueuePool(creator=mock_dbapi.connect, pool_size=3,
                           max_overflow=-1, use_threadlocal=True)
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
        p = pool.QueuePool(creator=mock_dbapi.connect, pool_size=1,
                           max_overflow=0, use_threadlocal=False,
                           recycle=3)
        c1 = p.connect()
        c_id = id(c1.connection)
        c1.close()
        c2 = p.connect()
        assert id(c2.connection) == c_id
        c2.close()
        time.sleep(4)
        c3 = p.connect()
        assert id(c3.connection) != c_id

    def test_invalidate(self):
        dbapi = MockDBAPI()
        p = pool.QueuePool(creator=lambda : dbapi.connect('foo.db'),
                           pool_size=1, max_overflow=0,
                           use_threadlocal=False)
        c1 = p.connect()
        c_id = c1.connection.id
        c1.close()
        c1 = None
        c1 = p.connect()
        assert c1.connection.id == c_id
        c1.invalidate()
        c1 = None
        c1 = p.connect()
        assert c1.connection.id != c_id

    def test_recreate(self):
        dbapi = MockDBAPI()
        p = pool.QueuePool(creator=lambda : dbapi.connect('foo.db'),
                           pool_size=1, max_overflow=0,
                           use_threadlocal=False)
        p2 = p.recreate()
        assert p2.size() == 1
        assert p2._use_threadlocal is False
        assert p2._max_overflow == 0

    def test_reconnect(self):
        """tests reconnect operations at the pool level.  SA's
        engine/dialect includes another layer of reconnect support for
        'database was lost' errors."""

        dbapi = MockDBAPI()
        p = pool.QueuePool(creator=lambda : dbapi.connect('foo.db'),
                           pool_size=1, max_overflow=0,
                           use_threadlocal=False)
        c1 = p.connect()
        c_id = c1.connection.id
        c1.close()
        c1 = None
        c1 = p.connect()
        assert c1.connection.id == c_id
        dbapi.raise_error = True
        c1.invalidate()
        c1 = None
        c1 = p.connect()
        assert c1.connection.id != c_id

    def test_detach(self):
        dbapi = MockDBAPI()
        p = pool.QueuePool(creator=lambda : dbapi.connect('foo.db'),
                           pool_size=1, max_overflow=0,
                           use_threadlocal=False)
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

    def test_threadfairy(self):
        p = pool.QueuePool(creator=mock_dbapi.connect, pool_size=3,
                           max_overflow=-1, use_threadlocal=True)
        c1 = p.connect()
        c1.close()
        c2 = p.connect()
        assert c2.connection is not None

class SingletonThreadPoolTest(PoolTestBase):

    def test_cleanup(self):
        """test that the pool's connections are OK after cleanup() has
        been called."""

        p = pool.SingletonThreadPool(creator=mock_dbapi.connect,
                pool_size=3)

        def checkout():
            for x in xrange(10):
                c = p.connect()
                assert c
                c.cursor()
                c.close()
                time.sleep(.1)

        threads = []
        for i in xrange(10):
            th = threading.Thread(target=checkout)
            th.start()
            threads.append(th)
        for th in threads:
            th.join()
        assert len(p._all_conns) == 3

class NullPoolTest(PoolTestBase):
    def test_reconnect(self):
        dbapi = MockDBAPI()
        p = pool.NullPool(creator = lambda: dbapi.connect('foo.db'))
        c1 = p.connect()
        c_id = c1.connection.id
        c1.close(); c1=None

        c1 = p.connect()
        dbapi.raise_error = True
        c1.invalidate()
        c1 = None

        c1 = p.connect()
        assert c1.connection.id != c_id


class StaticPoolTest(PoolTestBase):
    def test_recreate(self):
        dbapi = MockDBAPI()
        creator = lambda: dbapi.connect('foo.db')
        p = pool.StaticPool(creator)
        p2 = p.recreate()
        assert p._creator is p2._creator
