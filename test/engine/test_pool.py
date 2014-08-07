import threading
import time
from sqlalchemy import pool, select, event
import sqlalchemy as tsa
from sqlalchemy import testing
from sqlalchemy.testing.util import gc_collect, lazy_gc
from sqlalchemy.testing import eq_, assert_raises, is_not_
from sqlalchemy.testing.engines import testing_engine
from sqlalchemy.testing import fixtures
import random
from sqlalchemy.testing.mock import Mock, call

join_timeout = 10


def MockDBAPI():
    def cursor():
        return Mock()

    def connect(*arg, **kw):
        return Mock(cursor=Mock(side_effect=cursor))

    def shutdown(value):
        if value:
            db.connect = Mock(side_effect=Exception("connect failed"))
        else:
            db.connect = Mock(side_effect=connect)

    db = Mock(
        connect=Mock(side_effect=connect),
        shutdown=shutdown, _shutdown=False)
    return db


class PoolTestBase(fixtures.TestBase):
    def setup(self):
        pool.clear_managers()

    @classmethod
    def teardown_class(cls):
        pool.clear_managers()

    def _queuepool_fixture(self, **kw):
        dbapi, pool = self._queuepool_dbapi_fixture(**kw)
        return pool

    def _queuepool_dbapi_fixture(self, **kw):
        dbapi = MockDBAPI()
        return dbapi, pool.QueuePool(creator=lambda: dbapi.connect('foo.db'),
                        **kw)

class PoolTest(PoolTestBase):
    def test_manager(self):
        manager = pool.manage(MockDBAPI(), use_threadlocal=True)

        c1 = manager.connect('foo.db')
        c2 = manager.connect('foo.db')
        c3 = manager.connect('bar.db')
        c4 = manager.connect("foo.db", bar="bat")
        c5 = manager.connect("foo.db", bar="hoho")
        c6 = manager.connect("foo.db", bar="bat")

        assert c1.cursor() is not None
        assert c1 is c2
        assert c1 is not c3
        assert c4 is c6
        assert c4 is not c5

    def test_manager_with_key(self):

        dbapi = MockDBAPI()
        manager = pool.manage(dbapi, use_threadlocal=True)

        c1 = manager.connect('foo.db', sa_pool_key="a")
        c2 = manager.connect('foo.db', sa_pool_key="b")
        c3 = manager.connect('bar.db', sa_pool_key="a")

        assert c1.cursor() is not None
        assert c1 is not c2
        assert c1 is c3

        eq_(dbapi.connect.mock_calls,
            [
                call("foo.db"),
                call("foo.db"),
            ]
        )


    def test_bad_args(self):
        manager = pool.manage(MockDBAPI())
        manager.connect(None)

    def test_non_thread_local_manager(self):
        manager = pool.manage(MockDBAPI(), use_threadlocal=False)

        connection = manager.connect('foo.db')
        connection2 = manager.connect('foo.db')

        self.assert_(connection.cursor() is not None)
        self.assert_(connection is not connection2)

    @testing.fails_on('+pyodbc',
                      "pyodbc cursor doesn't implement tuple __eq__")
    @testing.fails_on("+pg8000", "returns [1], not (1,)")
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
            p2 = p.recreate()
            assert p2.__class__ is cls

            mock_dbapi = MockDBAPI()
            p = cls(creator=mock_dbapi.connect)
            conn = p.connect()
            conn.close()
            mock_dbapi.connect.side_effect = Exception("error!")
            p.dispose()
            p.recreate()

    def test_threadlocal_del(self):
        self._do_testthreadlocal(useclose=False)

    def test_threadlocal_close(self):
        self._do_testthreadlocal(useclose=True)

    def _do_testthreadlocal(self, useclose=False):
        dbapi = MockDBAPI()
        for p in pool.QueuePool(creator=dbapi.connect,
                                pool_size=3, max_overflow=-1,
                                use_threadlocal=True), \
            pool.SingletonThreadPool(creator=dbapi.connect,
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

    def test_info(self):
        p = self._queuepool_fixture(pool_size=1, max_overflow=0)

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
        is_not_(c.connection, c2.connection)
        assert not c2.info
        assert 'foo2' in c.info


class PoolDialectTest(PoolTestBase):
    def _dialect(self):
        canary = []
        class PoolDialect(object):
            def do_rollback(self, dbapi_connection):
                canary.append('R')
                dbapi_connection.rollback()

            def do_commit(self, dbapi_connection):
                canary.append('C')
                dbapi_connection.commit()

            def do_close(self, dbapi_connection):
                canary.append('CL')
                dbapi_connection.close()
        return PoolDialect(), canary

    def _do_test(self, pool_cls, assertion):
        mock_dbapi = MockDBAPI()
        dialect, canary = self._dialect()

        p = pool_cls(creator=mock_dbapi.connect)
        p._dialect = dialect
        conn = p.connect()
        conn.close()
        p.dispose()
        p.recreate()
        conn = p.connect()
        conn.close()
        eq_(canary, assertion)

    def test_queue_pool(self):
        self._do_test(pool.QueuePool, ['R', 'CL', 'R'])

    def test_assertion_pool(self):
        self._do_test(pool.AssertionPool, ['R', 'CL', 'R'])

    def test_singleton_pool(self):
        self._do_test(pool.SingletonThreadPool, ['R', 'CL', 'R'])

    def test_null_pool(self):
        self._do_test(pool.NullPool, ['R', 'CL', 'R', 'CL'])

    def test_static_pool(self):
        self._do_test(pool.StaticPool, ['R', 'R'])


class PoolEventsTest(PoolTestBase):
    def _first_connect_event_fixture(self):
        p = self._queuepool_fixture()
        canary = []
        def first_connect(*arg, **kw):
            canary.append('first_connect')

        event.listen(p, 'first_connect', first_connect)

        return p, canary

    def _connect_event_fixture(self):
        p = self._queuepool_fixture()
        canary = []
        def connect(*arg, **kw):
            canary.append('connect')
        event.listen(p, 'connect', connect)

        return p, canary

    def _checkout_event_fixture(self):
        p = self._queuepool_fixture()
        canary = []
        def checkout(*arg, **kw):
            canary.append('checkout')
        event.listen(p, 'checkout', checkout)

        return p, canary

    def _checkin_event_fixture(self):
        p = self._queuepool_fixture()
        canary = []
        def checkin(*arg, **kw):
            canary.append('checkin')
        event.listen(p, 'checkin', checkin)

        return p, canary

    def _reset_event_fixture(self):
        p = self._queuepool_fixture()
        canary = []
        def reset(*arg, **kw):
            canary.append('reset')
        event.listen(p, 'reset', reset)

        return p, canary

    def _invalidate_event_fixture(self):
        p = self._queuepool_fixture()
        canary = Mock()
        event.listen(p, 'invalidate', canary)

        return p, canary

    def test_first_connect_event(self):
        p, canary = self._first_connect_event_fixture()

        c1 = p.connect()
        eq_(canary, ['first_connect'])

    def test_first_connect_event_fires_once(self):
        p, canary = self._first_connect_event_fixture()

        c1 = p.connect()
        c2 = p.connect()

        eq_(canary, ['first_connect'])

    def test_first_connect_on_previously_recreated(self):
        p, canary = self._first_connect_event_fixture()

        p2 = p.recreate()
        c1 = p.connect()
        c2 = p2.connect()

        eq_(canary, ['first_connect', 'first_connect'])

    def test_first_connect_on_subsequently_recreated(self):
        p, canary = self._first_connect_event_fixture()

        c1 = p.connect()
        p2 = p.recreate()
        c2 = p2.connect()

        eq_(canary, ['first_connect', 'first_connect'])

    def test_connect_event(self):
        p, canary = self._connect_event_fixture()

        c1 = p.connect()
        eq_(canary, ['connect'])

    def test_connect_event_fires_subsequent(self):
        p, canary = self._connect_event_fixture()

        c1 = p.connect()
        c2 = p.connect()

        eq_(canary, ['connect', 'connect'])

    def test_connect_on_previously_recreated(self):
        p, canary = self._connect_event_fixture()

        p2 = p.recreate()

        c1 = p.connect()
        c2 = p2.connect()

        eq_(canary, ['connect', 'connect'])

    def test_connect_on_subsequently_recreated(self):
        p, canary = self._connect_event_fixture()

        c1 = p.connect()
        p2 = p.recreate()
        c2 = p2.connect()

        eq_(canary, ['connect', 'connect'])

    def test_checkout_event(self):
        p, canary = self._checkout_event_fixture()

        c1 = p.connect()
        eq_(canary, ['checkout'])

    def test_checkout_event_fires_subsequent(self):
        p, canary = self._checkout_event_fixture()

        c1 = p.connect()
        c2 = p.connect()
        eq_(canary, ['checkout', 'checkout'])

    def test_checkout_event_on_subsequently_recreated(self):
        p, canary = self._checkout_event_fixture()

        c1 = p.connect()
        p2 = p.recreate()
        c2 = p2.connect()

        eq_(canary, ['checkout', 'checkout'])

    def test_checkin_event(self):
        p, canary = self._checkin_event_fixture()

        c1 = p.connect()
        eq_(canary, [])
        c1.close()
        eq_(canary, ['checkin'])

    def test_reset_event(self):
        p, canary = self._reset_event_fixture()

        c1 = p.connect()
        eq_(canary, [])
        c1.close()
        eq_(canary, ['reset'])

    def test_invalidate_event_no_exception(self):
        p, canary = self._invalidate_event_fixture()

        c1 = p.connect()
        c1.close()
        assert not canary.called
        c1 = p.connect()
        dbapi_con = c1.connection
        c1.invalidate()
        assert canary.call_args_list[0][0][0] is dbapi_con
        assert canary.call_args_list[0][0][2] is None

    def test_invalidate_event_exception(self):
        p, canary = self._invalidate_event_fixture()

        c1 = p.connect()
        c1.close()
        assert not canary.called
        c1 = p.connect()
        dbapi_con = c1.connection
        exc = Exception("hi")
        c1.invalidate(exc)
        assert canary.call_args_list[0][0][0] is dbapi_con
        assert canary.call_args_list[0][0][2] is exc

    def test_checkin_event_gc(self):
        p, canary = self._checkin_event_fixture()

        c1 = p.connect()
        eq_(canary, [])
        del c1
        lazy_gc()
        eq_(canary, ['checkin'])

    def test_checkin_event_on_subsequently_recreated(self):
        p, canary = self._checkin_event_fixture()

        c1 = p.connect()
        p2 = p.recreate()
        c2 = p2.connect()

        eq_(canary, [])

        c1.close()
        eq_(canary, ['checkin'])

        c2.close()
        eq_(canary, ['checkin', 'checkin'])

    def test_listen_targets_scope(self):
        canary = []
        def listen_one(*args):
            canary.append("listen_one")
        def listen_two(*args):
            canary.append("listen_two")
        def listen_three(*args):
            canary.append("listen_three")
        def listen_four(*args):
            canary.append("listen_four")

        engine = testing_engine(testing.db.url)
        event.listen(pool.Pool, 'connect', listen_one)
        event.listen(engine.pool, 'connect', listen_two)
        event.listen(engine, 'connect', listen_three)
        event.listen(engine.__class__, 'connect', listen_four)

        engine.execute(select([1])).close()
        eq_(
            canary,
            ["listen_one", "listen_four", "listen_two", "listen_three"]
        )

    def test_listen_targets_per_subclass(self):
        """test that listen() called on a subclass remains specific to that subclass."""

        canary = []
        def listen_one(*args):
            canary.append("listen_one")
        def listen_two(*args):
            canary.append("listen_two")
        def listen_three(*args):
            canary.append("listen_three")

        event.listen(pool.Pool, 'connect', listen_one)
        event.listen(pool.QueuePool, 'connect', listen_two)
        event.listen(pool.SingletonThreadPool, 'connect', listen_three)

        p1 = pool.QueuePool(creator=MockDBAPI().connect)
        p2 = pool.SingletonThreadPool(creator=MockDBAPI().connect)

        assert listen_one in p1.dispatch.connect
        assert listen_two in p1.dispatch.connect
        assert listen_three not in p1.dispatch.connect
        assert listen_one in p2.dispatch.connect
        assert listen_two not in p2.dispatch.connect
        assert listen_three in p2.dispatch.connect

        p1.connect()
        eq_(canary, ["listen_one", "listen_two"])
        p2.connect()
        eq_(canary, ["listen_one", "listen_two", "listen_one", "listen_three"])

    def teardown(self):
        # TODO: need to get remove() functionality
        # going
        pool.Pool.dispatch._clear()

class PoolFirstConnectSyncTest(PoolTestBase):
    # test [ticket:2964]

    def test_sync(self):
        pool = self._queuepool_fixture(pool_size=3, max_overflow=0)

        evt = Mock()

        @event.listens_for(pool, 'first_connect')
        def slow_first_connect(dbapi_con, rec):
            time.sleep(1)
            evt.first_connect()

        @event.listens_for(pool, 'connect')
        def on_connect(dbapi_con, rec):
            evt.connect()

        def checkout():
            for j in range(2):
                c1 = pool.connect()
                time.sleep(.02)
                c1.close()
                time.sleep(.02)

        threads = []
        for i in range(5):
            th = threading.Thread(target=checkout)
            th.start()
            threads.append(th)
        for th in threads:
            th.join(join_timeout)

        eq_(evt.mock_calls,
                [call.first_connect(), call.connect(), call.connect(), call.connect()]
            )



class DeprecatedPoolListenerTest(PoolTestBase):
    @testing.requires.predictable_gc
    @testing.uses_deprecated(r".*Use event.listen")
    def test_listeners(self):
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
                print("connect(%s, %s)" % (con, record))
                assert con is not None
                assert record is not None
                self.connected.append(con)
            def inst_first_connect(self, con, record):
                print("first_connect(%s, %s)" % (con, record))
                assert con is not None
                assert record is not None
                self.first_connected.append(con)
            def inst_checkout(self, con, record, proxy):
                print("checkout(%s, %s, %s)" % (con, record, proxy))
                assert con is not None
                assert record is not None
                assert proxy is not None
                self.checked_out.append(con)
            def inst_checkin(self, con, record):
                print("checkin(%s, %s)" % (con, record))
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

        def assert_listeners(p, total, conn, fconn, cout, cin):
            for instance in (p, p.recreate()):
                self.assert_(len(instance.dispatch.connect) == conn)
                self.assert_(len(instance.dispatch.first_connect) == fconn)
                self.assert_(len(instance.dispatch.checkout) == cout)
                self.assert_(len(instance.dispatch.checkin) == cin)

        p = self._queuepool_fixture()
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
        p = self._queuepool_fixture(listeners=[snoop])
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

    @testing.uses_deprecated(r".*Use event.listen")
    def test_listeners_callables(self):
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

            def assert_listeners(p, total, conn, cout, cin):
                for instance in (p, p.recreate()):
                    eq_(len(instance.dispatch.connect), conn)
                    eq_(len(instance.dispatch.checkout), cout)
                    eq_(len(instance.dispatch.checkin), cin)

            p = self._queuepool_fixture()
            assert_listeners(p, 0, 0, 0, 0)

            p.add_listener(i_all)
            assert_listeners(p, 1, 1, 1, 1)

            p.add_listener(i_connect)
            assert_listeners(p, 2, 1, 1, 1)

            p.add_listener(i_checkout)
            assert_listeners(p, 3, 1, 1, 1)

            p.add_listener(i_checkin)
            assert_listeners(p, 4, 1, 1, 1)
            del p

            p = self._queuepool_fixture(listeners=[i_all])
            assert_listeners(p, 1, 1, 1, 1)

            c = p.connect()
            assert counts == [1, 1, 0]
            c.close()
            assert counts == [1, 1, 1]

            c = p.connect()
            assert counts == [1, 2, 1]
            p.add_listener(i_checkin)
            c.close()
            assert counts == [1, 2, 2]


class QueuePoolTest(PoolTestBase):

    def test_queuepool_del(self):
        self._do_testqueuepool(useclose=False)

    def test_queuepool_close(self):
        self._do_testqueuepool(useclose=True)

    def _do_testqueuepool(self, useclose=False):
        p = self._queuepool_fixture(pool_size=3,
                           max_overflow=-1)

        def status(pool):
            tup = pool.size(), pool.checkedin(), pool.overflow(), \
                pool.checkedout()
            print('Pool size: %d  Connections in pool: %d Current '\
                'Overflow: %d Current Checked out connections: %d' % tup)
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
        p = self._queuepool_fixture(pool_size=3,
                           max_overflow=0,
                           timeout=2)
        c1 = p.connect()
        c2 = p.connect()
        c3 = p.connect()
        now = time.time()
        try:
            c4 = p.connect()
            assert False
        except tsa.exc.TimeoutError:
            assert int(time.time() - now) == 2

    @testing.requires.threading_with_mock
    def test_timeout_race(self):
        # test a race condition where the initial connecting threads all race
        # to queue.Empty, then block on the mutex.  each thread consumes a
        # connection as they go in.  when the limit is reached, the remaining
        # threads go in, and get TimeoutError; even though they never got to
        # wait for the timeout on queue.get().  the fix involves checking the
        # timeout again within the mutex, and if so, unlocking and throwing
        # them back to the start of do_get()
        dbapi = MockDBAPI()
        p = pool.QueuePool(
                creator=lambda: dbapi.connect(delay=.05),
                pool_size=2,
                max_overflow=1, use_threadlocal=False, timeout=3)
        timeouts = []
        def checkout():
            for x in range(1):
                now = time.time()
                try:
                    c1 = p.connect()
                except tsa.exc.TimeoutError:
                    timeouts.append(time.time() - now)
                    continue
                time.sleep(4)
                c1.close()

        threads = []
        for i in range(10):
            th = threading.Thread(target=checkout)
            th.start()
            threads.append(th)
        for th in threads:
            th.join(join_timeout)

        assert len(timeouts) > 0
        for t in timeouts:
            assert t >= 3, "Not all timeouts were >= 3 seconds %r" % timeouts
            # normally, the timeout should under 4 seconds,
            # but on a loaded down buildbot it can go up.
            assert t < 14, "Not all timeouts were < 14 seconds %r" % timeouts

    def _test_overflow(self, thread_count, max_overflow):
        gc_collect()

        dbapi = MockDBAPI()
        mutex = threading.Lock()
        def creator():
            time.sleep(.05)
            with mutex:
                return dbapi.connect()

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
        for i in range(thread_count):
            th = threading.Thread(target=whammy)
            th.start()
            threads.append(th)
        for th in threads:
            th.join(join_timeout)

        self.assert_(max(peaks) <= max_overflow)

        lazy_gc()
        assert not pool._refs


    def test_overflow_reset_on_failed_connect(self):
        dbapi = Mock()

        def failing_dbapi():
            time.sleep(2)
            raise Exception("connection failed")

        creator = dbapi.connect
        def create():
            return creator()

        p = pool.QueuePool(creator=create, pool_size=2, max_overflow=3)
        c1 = p.connect()
        c2 = p.connect()
        c3 = p.connect()
        eq_(p._overflow, 1)
        creator = failing_dbapi
        assert_raises(Exception, p.connect)
        eq_(p._overflow, 1)

    @testing.requires.threading_with_mock
    def test_hanging_connect_within_overflow(self):
        """test that a single connect() call which is hanging
        does not block other connections from proceeding."""

        dbapi = Mock()
        mutex = threading.Lock()

        def hanging_dbapi():
            time.sleep(2)
            with mutex:
                return dbapi.connect()

        def fast_dbapi():
            with mutex:
                return dbapi.connect()

        creator = threading.local()

        def create():
            return creator.mock_connector()

        def run_test(name, pool, should_hang):
            if should_hang:
                creator.mock_connector = hanging_dbapi
            else:
                creator.mock_connector = fast_dbapi

            conn = pool.connect()
            conn.operation(name)
            time.sleep(1)
            conn.close()

        p = pool.QueuePool(creator=create, pool_size=2, max_overflow=3)

        threads = [
            threading.Thread(
                        target=run_test, args=("success_one", p, False)),
            threading.Thread(
                        target=run_test, args=("success_two", p, False)),
            threading.Thread(
                        target=run_test, args=("overflow_one", p, True)),
            threading.Thread(
                        target=run_test, args=("overflow_two", p, False)),
            threading.Thread(
                        target=run_test, args=("overflow_three", p, False))
        ]
        for t in threads:
            t.start()
            time.sleep(.2)

        for t in threads:
            t.join(timeout=join_timeout)
        eq_(
            dbapi.connect().operation.mock_calls,
            [call("success_one"), call("success_two"),
                call("overflow_two"), call("overflow_three"),
                call("overflow_one")]
        )


    @testing.requires.threading_with_mock
    def test_waiters_handled(self):
        """test that threads waiting for connections are
        handled when the pool is replaced.

        """
        mutex = threading.Lock()
        dbapi = MockDBAPI()
        def creator():
            mutex.acquire()
            try:
                return dbapi.connect()
            finally:
                mutex.release()

        success = []
        for timeout in (None, 30):
            for max_overflow in (0, -1, 3):
                p = pool.QueuePool(creator=creator,
                                   pool_size=2, timeout=timeout,
                                   max_overflow=max_overflow)
                def waiter(p, timeout, max_overflow):
                    success_key = (timeout, max_overflow)
                    conn = p.connect()
                    success.append(success_key)
                    time.sleep(.1)
                    conn.close()

                c1 = p.connect()
                c2 = p.connect()

                threads = []
                for i in range(2):
                    t = threading.Thread(target=waiter,
                                    args=(p, timeout, max_overflow))
                    t.daemon = True
                    t.start()
                    threads.append(t)

                # this sleep makes sure that the
                # two waiter threads hit upon wait()
                # inside the queue, before we invalidate the other
                # two conns
                time.sleep(.2)
                p._invalidate(c2)

                for t in threads:
                    t.join(join_timeout)

        eq_(len(success), 12, "successes: %s" % success)

    @testing.requires.threading_with_mock
    def test_notify_waiters(self):
        dbapi = MockDBAPI()

        canary = []
        def creator():
            canary.append(1)
            return dbapi.connect()
        p1 = pool.QueuePool(creator=creator,
                           pool_size=1, timeout=None,
                           max_overflow=0)
        def waiter(p):
            conn = p.connect()
            canary.append(2)
            time.sleep(.5)
            conn.close()

        c1 = p1.connect()

        threads = []
        for i in range(5):
            t = threading.Thread(target=waiter, args=(p1, ))
            t.start()
            threads.append(t)
        time.sleep(.5)
        eq_(canary, [1])

        # this also calls invalidate()
        # on c1
        p1._invalidate(c1)

        for t in threads:
            t.join(join_timeout)

        eq_(canary, [1, 1, 2, 2, 2, 2, 2])

    def test_dispose_closes_pooled(self):
        dbapi = MockDBAPI()

        p = pool.QueuePool(creator=dbapi.connect,
                           pool_size=2, timeout=None,
                           max_overflow=0)
        c1 = p.connect()
        c2 = p.connect()
        c1_con = c1.connection
        c2_con = c2.connection

        c1.close()

        eq_(c1_con.close.call_count, 0)
        eq_(c2_con.close.call_count, 0)

        p.dispose()

        eq_(c1_con.close.call_count, 1)
        eq_(c2_con.close.call_count, 0)

        # currently, if a ConnectionFairy is closed
        # after the pool has been disposed, there's no
        # flag that states it should be invalidated
        # immediately - it just gets returned to the
        # pool normally...
        c2.close()
        eq_(c1_con.close.call_count, 1)
        eq_(c2_con.close.call_count, 0)

        # ...and that's the one we'll get back next.
        c3 = p.connect()
        assert c3.connection is c2_con

    @testing.requires.threading_with_mock
    def test_no_overflow(self):
        self._test_overflow(40, 0)

    @testing.requires.threading_with_mock
    def test_max_overflow(self):
        self._test_overflow(40, 5)

    def test_mixed_close(self):
        pool._refs.clear()
        p = self._queuepool_fixture(pool_size=3, max_overflow=-1, use_threadlocal=True)
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

    def test_overflow_no_gc_tlocal(self):
        self._test_overflow_no_gc(True)

    def test_overflow_no_gc(self):
        self._test_overflow_no_gc(False)

    def _test_overflow_no_gc(self, threadlocal):
        p = self._queuepool_fixture(pool_size=2,
                           max_overflow=2)

        # disable weakref collection of the
        # underlying connections
        strong_refs = set()
        def _conn():
            c = p.connect()
            strong_refs.add(c.connection)
            return c

        for j in range(5):
            # open 4 conns at a time.  each time this
            # will yield two pooled connections + two
            # overflow connections.
            conns = [_conn() for i in range(4)]
            for c in conns:
                c.close()

        # doing that for a total of 5 times yields
        # ten overflow connections closed plus the
        # two pooled connections unclosed.

        eq_(
            set([c.close.call_count for c in strong_refs]),
            set([1, 1, 1, 1, 1, 1, 1, 0, 1, 1, 1, 0])
        )

    @testing.requires.predictable_gc
    def test_weakref_kaboom(self):
        p = self._queuepool_fixture(pool_size=3,
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

        p = self._queuepool_fixture(pool_size=3,
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
        p = self._queuepool_fixture(pool_size=1,
                           max_overflow=0,
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

    def test_recycle_on_invalidate(self):
        p = self._queuepool_fixture(pool_size=1,
                           max_overflow=0)
        c1 = p.connect()
        c_id = id(c1.connection)
        c1.close()
        c2 = p.connect()
        assert id(c2.connection) == c_id

        p._invalidate(c2)
        c2.close()
        time.sleep(.5)
        c3 = p.connect()
        assert id(c3.connection) != c_id

    def _assert_cleanup_on_pooled_reconnect(self, dbapi, p):
        # p is QueuePool with size=1, max_overflow=2,
        # and one connection in the pool that will need to
        # reconnect when next used (either due to recycle or invalidate)
        eq_(p.checkedout(), 0)
        eq_(p._overflow, 0)
        dbapi.shutdown(True)
        assert_raises(
            Exception,
            p.connect
        )
        eq_(p._overflow, 0)
        eq_(p.checkedout(), 0)  # and not 1

        dbapi.shutdown(False)

        c1 = p.connect()
        assert p._pool.empty()  # poolsize is one, so we're empty OK
        c2 = p.connect()
        eq_(p._overflow, 1)  # and not 2

        # this hangs if p._overflow is 2
        c3 = p.connect()

    def test_error_on_pooled_reconnect_cleanup_invalidate(self):
        dbapi, p = self._queuepool_dbapi_fixture(pool_size=1, max_overflow=2)
        c1 = p.connect()
        c1.invalidate()
        c1.close()
        self._assert_cleanup_on_pooled_reconnect(dbapi, p)

    def test_error_on_pooled_reconnect_cleanup_recycle(self):
        dbapi, p = self._queuepool_dbapi_fixture(pool_size=1,
                                        max_overflow=2, recycle=1)
        c1 = p.connect()
        c1.close()
        time.sleep(1)
        self._assert_cleanup_on_pooled_reconnect(dbapi, p)

    def test_recycle_pool_no_race(self):
        def slow_close():
            slow_closing_connection._slow_close()
            time.sleep(.5)

        slow_closing_connection = Mock()
        slow_closing_connection.connect.return_value.close = slow_close

        class Error(Exception):
            pass

        dialect = Mock()
        dialect.is_disconnect = lambda *arg, **kw: True
        dialect.dbapi.Error = Error

        pools = []
        class TrackQueuePool(pool.QueuePool):
            def __init__(self, *arg, **kw):
                pools.append(self)
                super(TrackQueuePool, self).__init__(*arg, **kw)

        def creator():
            return slow_closing_connection.connect()
        p1 = TrackQueuePool(creator=creator, pool_size=20)

        from sqlalchemy import create_engine
        eng = create_engine(testing.db.url, pool=p1, _initialize=False)
        eng.dialect = dialect

        # 15 total connections
        conns = [eng.connect() for i in range(15)]

        # return 8 back to the pool
        for conn in conns[3:10]:
            conn.close()

        def attempt(conn):
            time.sleep(random.random())
            try:
                conn._handle_dbapi_exception(Error(), "statement", {}, Mock(), Mock())
            except tsa.exc.DBAPIError:
                pass

        # run an error + invalidate operation on the remaining 7 open connections
        threads = []
        for conn in conns:
            t = threading.Thread(target=attempt, args=(conn, ))
            t.start()
            threads.append(t)

        for t in threads:
            t.join()

        # return all 15 connections to the pool
        for conn in conns:
            conn.close()

        # re-open 15 total connections
        conns = [eng.connect() for i in range(15)]

        # 15 connections have been fully closed due to invalidate
        assert slow_closing_connection._slow_close.call_count == 15

        # 15 initial connections + 15 reconnections
        assert slow_closing_connection.connect.call_count == 30
        assert len(pools) <= 2, len(pools)

    def test_invalidate(self):
        p = self._queuepool_fixture(pool_size=1, max_overflow=0)
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
        p = self._queuepool_fixture(reset_on_return=None, pool_size=1, max_overflow=0)
        p2 = p.recreate()
        assert p2.size() == 1
        assert p2._reset_on_return is pool.reset_none
        assert p2._use_threadlocal is False
        assert p2._max_overflow == 0

    def test_reconnect(self):
        """tests reconnect operations at the pool level.  SA's
        engine/dialect includes another layer of reconnect support for
        'database was lost' errors."""

        dbapi, p = self._queuepool_dbapi_fixture(pool_size=1, max_overflow=0)
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
        dbapi, p = self._queuepool_dbapi_fixture(pool_size=1, max_overflow=0)
        c1 = p.connect()
        c1.detach()
        c2 = p.connect()
        eq_(dbapi.connect.mock_calls, [call("foo.db"), call("foo.db")])

        c1_con = c1.connection
        assert c1_con is not None
        eq_(c1_con.close.call_count, 0)
        c1.close()
        eq_(c1_con.close.call_count, 1)

    def test_detach_via_invalidate(self):
        dbapi, p = self._queuepool_dbapi_fixture(pool_size=1, max_overflow=0)

        c1 = p.connect()
        c1_con = c1.connection
        c1.invalidate()
        assert c1.connection is None
        eq_(c1_con.close.call_count, 1)

        c2 = p.connect()
        assert c2.connection is not c1_con
        c2_con = c2.connection

        c2.close()
        eq_(c2_con.close.call_count, 0)

    def test_threadfairy(self):
        p = self._queuepool_fixture(pool_size=3, max_overflow=-1, use_threadlocal=True)
        c1 = p.connect()
        c1.close()
        c2 = p.connect()
        assert c2.connection is not None

class ResetOnReturnTest(PoolTestBase):
    def _fixture(self, **kw):
        dbapi = Mock()
        return dbapi, pool.QueuePool(creator=lambda: dbapi.connect('foo.db'), **kw)

    def test_plain_rollback(self):
        dbapi, p = self._fixture(reset_on_return='rollback')

        c1 = p.connect()
        c1.close()
        assert dbapi.connect().rollback.called
        assert not dbapi.connect().commit.called

    def test_plain_commit(self):
        dbapi, p = self._fixture(reset_on_return='commit')

        c1 = p.connect()
        c1.close()
        assert not dbapi.connect().rollback.called
        assert dbapi.connect().commit.called

    def test_plain_none(self):
        dbapi, p = self._fixture(reset_on_return=None)

        c1 = p.connect()
        c1.close()
        assert not dbapi.connect().rollback.called
        assert not dbapi.connect().commit.called

    def test_agent_rollback(self):
        dbapi, p = self._fixture(reset_on_return='rollback')

        class Agent(object):
            def __init__(self, conn):
                self.conn = conn

            def rollback(self):
                self.conn.special_rollback()

            def commit(self):
                self.conn.special_commit()

        c1 = p.connect()
        c1._reset_agent = Agent(c1)
        c1.close()

        assert dbapi.connect().special_rollback.called
        assert not dbapi.connect().special_commit.called

        assert not dbapi.connect().rollback.called
        assert not dbapi.connect().commit.called

        c1 = p.connect()
        c1.close()
        eq_(dbapi.connect().special_rollback.call_count, 1)
        eq_(dbapi.connect().special_commit.call_count, 0)

        assert dbapi.connect().rollback.called
        assert not dbapi.connect().commit.called

    def test_agent_commit(self):
        dbapi, p = self._fixture(reset_on_return='commit')

        class Agent(object):
            def __init__(self, conn):
                self.conn = conn

            def rollback(self):
                self.conn.special_rollback()

            def commit(self):
                self.conn.special_commit()

        c1 = p.connect()
        c1._reset_agent = Agent(c1)
        c1.close()
        assert not dbapi.connect().special_rollback.called
        assert dbapi.connect().special_commit.called

        assert not dbapi.connect().rollback.called
        assert not dbapi.connect().commit.called

        c1 = p.connect()
        c1.close()

        eq_(dbapi.connect().special_rollback.call_count, 0)
        eq_(dbapi.connect().special_commit.call_count, 1)
        assert not dbapi.connect().rollback.called
        assert dbapi.connect().commit.called

class SingletonThreadPoolTest(PoolTestBase):

    @testing.requires.threading_with_mock
    def test_cleanup(self):
        self._test_cleanup(False)

    @testing.requires.threading_with_mock
    def test_cleanup_no_gc(self):
        self._test_cleanup(True)

    def _test_cleanup(self, strong_refs):
        """test that the pool's connections are OK after cleanup() has
        been called."""

        dbapi = MockDBAPI()

        lock = threading.Lock()
        def creator():
            # the mock iterator isn't threadsafe...
            with lock:
                return dbapi.connect()
        p = pool.SingletonThreadPool(creator=creator, pool_size=3)

        if strong_refs:
            sr = set()
            def _conn():
                c = p.connect()
                sr.add(c.connection)
                return c
        else:
            def _conn():
                return p.connect()

        def checkout():
            for x in range(10):
                c = _conn()
                assert c
                c.cursor()
                c.close()
                time.sleep(.1)

        threads = []
        for i in range(10):
            th = threading.Thread(target=checkout)
            th.start()
            threads.append(th)
        for th in threads:
            th.join(join_timeout)
        assert len(p._all_conns) == 3

        if strong_refs:
            still_opened = len([c for c in sr if not c.close.call_count])
            eq_(still_opened, 3)

class AssertionPoolTest(PoolTestBase):
    def test_connect_error(self):
        dbapi = MockDBAPI()
        p = pool.AssertionPool(creator=lambda: dbapi.connect('foo.db'))
        c1 = p.connect()
        assert_raises(AssertionError, p.connect)

    def test_connect_multiple(self):
        dbapi = MockDBAPI()
        p = pool.AssertionPool(creator=lambda: dbapi.connect('foo.db'))
        c1 = p.connect()
        c1.close()
        c2 = p.connect()
        c2.close()

        c3 = p.connect()
        assert_raises(AssertionError, p.connect)

class NullPoolTest(PoolTestBase):
    def test_reconnect(self):
        dbapi = MockDBAPI()
        p = pool.NullPool(creator=lambda: dbapi.connect('foo.db'))
        c1 = p.connect()

        c1.close()
        c1 = None

        c1 = p.connect()
        c1.invalidate()
        c1 = None

        c1 = p.connect()
        dbapi.connect.assert_has_calls([
                            call('foo.db'),
                            call('foo.db')],
                            any_order=True)


class StaticPoolTest(PoolTestBase):
    def test_recreate(self):
        dbapi = MockDBAPI()
        creator = lambda: dbapi.connect('foo.db')
        p = pool.StaticPool(creator)
        p2 = p.recreate()
        assert p._creator is p2._creator
