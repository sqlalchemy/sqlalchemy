import collections
import random
import threading
import time
import weakref

import sqlalchemy as tsa
from sqlalchemy import event
from sqlalchemy import pool
from sqlalchemy import select
from sqlalchemy import testing
from sqlalchemy.engine import default
from sqlalchemy.pool.impl import _AsyncConnDialect
from sqlalchemy.testing import assert_raises
from sqlalchemy.testing import assert_raises_context_ok
from sqlalchemy.testing import assert_raises_message
from sqlalchemy.testing import eq_
from sqlalchemy.testing import expect_raises
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import is_
from sqlalchemy.testing import is_none
from sqlalchemy.testing import is_not
from sqlalchemy.testing import is_not_none
from sqlalchemy.testing import is_true
from sqlalchemy.testing import mock
from sqlalchemy.testing.engines import testing_engine
from sqlalchemy.testing.mock import ANY
from sqlalchemy.testing.mock import call
from sqlalchemy.testing.mock import Mock
from sqlalchemy.testing.mock import patch
from sqlalchemy.testing.util import gc_collect
from sqlalchemy.testing.util import lazy_gc

join_timeout = 10


def MockDBAPI():  # noqa
    def cursor():
        return Mock()

    def connect(*arg, **kw):
        def close():
            conn.closed = True

        # mock seems like it might have an issue logging
        # call_count correctly under threading, not sure.
        # adding a side_effect for close seems to help.
        conn = Mock(
            cursor=Mock(side_effect=cursor),
            close=Mock(side_effect=close),
            closed=False,
        )
        return conn

    def shutdown(value):
        if value:
            db.connect = Mock(side_effect=Exception("connect failed"))
        else:
            db.connect = Mock(side_effect=connect)
        db.is_shutdown = value

    db = Mock(
        connect=Mock(side_effect=connect), shutdown=shutdown, is_shutdown=False
    )
    return db


class PoolTestBase(fixtures.TestBase):
    def setup_test(self):
        pool.clear_managers()
        self._teardown_conns = []

    def teardown_test(self):
        for ref in self._teardown_conns:
            conn = ref()
            if conn:
                conn.close()

    @classmethod
    def teardown_test_class(cls):
        pool.clear_managers()

    def _with_teardown(self, connection):
        self._teardown_conns.append(weakref.ref(connection))
        return connection

    def _queuepool_fixture(self, **kw):
        dbapi, pool = self._queuepool_dbapi_fixture(**kw)
        return pool

    def _queuepool_dbapi_fixture(self, **kw):
        dbapi = MockDBAPI()
        _is_asyncio = kw.pop("_is_asyncio", False)
        p = pool.QueuePool(creator=lambda: dbapi.connect("foo.db"), **kw)
        if _is_asyncio:
            p._is_asyncio = True
            p._dialect = _AsyncConnDialect()
        return dbapi, p


class PoolTest(PoolTestBase):
    @testing.fails_on(
        "+pyodbc", "pyodbc cursor doesn't implement tuple __eq__"
    )
    @testing.fails_on("+pg8000", "returns [1], not (1,)")
    def test_cursor_iterable(self):
        conn = testing.db.raw_connection()
        cursor = conn.cursor()
        cursor.execute(str(select(1).compile(testing.db)))
        expected = [(1,)]
        for row in cursor:
            eq_(row, expected.pop(0))

    def test_no_connect_on_recreate(self):
        def creator():
            raise Exception("no creates allowed")

        for cls in (
            pool.SingletonThreadPool,
            pool.StaticPool,
            pool.QueuePool,
            pool.NullPool,
            pool.AssertionPool,
        ):
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

    def test_info(self):
        p = self._queuepool_fixture(pool_size=1, max_overflow=0)

        c = p.connect()
        self.assert_(not c.info)
        self.assert_(c.info is c._connection_record.info)

        c.info["foo"] = "bar"
        c.close()
        del c

        c = p.connect()
        self.assert_("foo" in c.info)

        c.invalidate()
        c = p.connect()
        self.assert_("foo" not in c.info)

        c.info["foo2"] = "bar2"
        c.detach()
        self.assert_("foo2" in c.info)

        c2 = p.connect()
        is_not(c.connection, c2.connection)
        assert not c2.info
        assert "foo2" in c.info

    def test_rec_info(self):
        p = self._queuepool_fixture(pool_size=1, max_overflow=0)

        c = p.connect()
        self.assert_(not c.record_info)
        self.assert_(c.record_info is c._connection_record.record_info)

        c.record_info["foo"] = "bar"
        c.close()
        del c

        c = p.connect()
        self.assert_("foo" in c.record_info)

        c.invalidate()
        c = p.connect()
        self.assert_("foo" in c.record_info)

        c.record_info["foo2"] = "bar2"
        c.detach()
        is_(c.record_info, None)
        is_(c._connection_record, None)

        c2 = p.connect()

        assert c2.record_info
        assert "foo2" in c2.record_info

    def test_rec_unconnected(self):
        # test production of a _ConnectionRecord with an
        # initially unconnected state.

        dbapi = MockDBAPI()
        p1 = pool.Pool(creator=lambda: dbapi.connect("foo.db"))

        r1 = pool._ConnectionRecord(p1, connect=False)

        assert not r1.connection
        c1 = r1.get_connection()
        is_(c1, r1.connection)

    def test_rec_close_reopen(self):
        # test that _ConnectionRecord.close() allows
        # the record to be reusable
        dbapi = MockDBAPI()
        p1 = pool.Pool(creator=lambda: dbapi.connect("foo.db"))

        r1 = pool._ConnectionRecord(p1)

        c1 = r1.connection
        c2 = r1.get_connection()
        is_(c1, c2)

        r1.close()

        assert not r1.connection
        eq_(c1.mock_calls, [call.close()])

        c2 = r1.get_connection()

        is_not(c1, c2)
        is_(c2, r1.connection)

        eq_(c2.mock_calls, [])

    @testing.combinations(
        (
            pool.QueuePool,
            dict(pool_size=8, max_overflow=10, timeout=25, use_lifo=True),
        ),
        (pool.QueuePool, {}),
        (pool.NullPool, {}),
        (pool.SingletonThreadPool, {}),
        (pool.StaticPool, {}),
        (pool.AssertionPool, {}),
    )
    def test_recreate_state(self, pool_cls, pool_args):
        creator = object()
        pool_args["pre_ping"] = True
        pool_args["reset_on_return"] = "commit"
        pool_args["recycle"] = 35
        pool_args["logging_name"] = "somepool"
        pool_args["dialect"] = default.DefaultDialect()
        pool_args["echo"] = "debug"

        p1 = pool_cls(creator=creator, **pool_args)

        cls_keys = dir(pool_cls)

        d1 = dict(p1.__dict__)

        p2 = p1.recreate()

        d2 = dict(p2.__dict__)

        for k in cls_keys:
            d1.pop(k, None)
            d2.pop(k, None)

        for k in (
            "_invoke_creator",
            "_pool",
            "_overflow_lock",
            "_fairy",
            "_conn",
            "logger",
        ):
            if k in d2:
                d2[k] = mock.ANY

        eq_(d1, d2)

        eq_(p1.echo, p2.echo)
        is_(p1._dialect, p2._dialect)

        if "use_lifo" in pool_args:
            eq_(p1._pool.use_lifo, p2._pool.use_lifo)


class PoolDialectTest(PoolTestBase):
    def _dialect(self):
        canary = []

        class PoolDialect(object):
            is_async = False

            def do_rollback(self, dbapi_connection):
                canary.append("R")
                dbapi_connection.rollback()

            def do_commit(self, dbapi_connection):
                canary.append("C")
                dbapi_connection.commit()

            def do_close(self, dbapi_connection):
                canary.append("CL")
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
        self._do_test(pool.QueuePool, ["R", "CL", "R"])

    def test_assertion_pool(self):
        self._do_test(pool.AssertionPool, ["R", "CL", "R"])

    def test_singleton_pool(self):
        self._do_test(pool.SingletonThreadPool, ["R", "CL", "R"])

    def test_null_pool(self):
        self._do_test(pool.NullPool, ["R", "CL", "R", "CL"])

    def test_static_pool(self):
        self._do_test(pool.StaticPool, ["R", "CL", "R"])


class PoolEventsTest(PoolTestBase):
    def _first_connect_event_fixture(self):
        p = self._queuepool_fixture()
        canary = []

        def first_connect(*arg, **kw):
            canary.append("first_connect")

        event.listen(p, "first_connect", first_connect)

        return p, canary

    def _connect_event_fixture(self):
        p = self._queuepool_fixture()
        canary = []

        def connect(*arg, **kw):
            canary.append("connect")

        event.listen(p, "connect", connect)

        return p, canary

    def _checkout_event_fixture(self):
        p = self._queuepool_fixture()
        canary = []

        def checkout(*arg, **kw):
            canary.append("checkout")

        event.listen(p, "checkout", checkout)

        return p, canary

    def _checkin_event_fixture(self, _is_asyncio=False):
        p = self._queuepool_fixture(_is_asyncio=_is_asyncio)
        canary = []

        @event.listens_for(p, "checkin")
        def checkin(*arg, **kw):
            canary.append("checkin")

        @event.listens_for(p, "close_detached")
        def close_detached(*arg, **kw):
            canary.append("close_detached")

        @event.listens_for(p, "detach")
        def detach(*arg, **kw):
            canary.append("detach")

        return p, canary

    def _reset_event_fixture(self):
        p = self._queuepool_fixture()
        canary = []

        def reset(*arg, **kw):
            canary.append("reset")

        event.listen(p, "reset", reset)

        return p, canary

    def _invalidate_event_fixture(self):
        p = self._queuepool_fixture()
        canary = Mock()
        event.listen(p, "invalidate", canary)

        return p, canary

    def _soft_invalidate_event_fixture(self):
        p = self._queuepool_fixture()
        canary = Mock()
        event.listen(p, "soft_invalidate", canary)

        return p, canary

    def _close_event_fixture(self):
        p = self._queuepool_fixture()
        canary = Mock()
        event.listen(p, "close", canary)

        return p, canary

    def _detach_event_fixture(self):
        p = self._queuepool_fixture()
        canary = Mock()
        event.listen(p, "detach", canary)

        return p, canary

    def _close_detached_event_fixture(self):
        p = self._queuepool_fixture()
        canary = Mock()
        event.listen(p, "close_detached", canary)

        return p, canary

    def test_close(self):
        p, canary = self._close_event_fixture()

        c1 = p.connect()

        connection = c1.connection
        rec = c1._connection_record

        c1.close()

        eq_(canary.mock_calls, [])

        p.dispose()
        eq_(canary.mock_calls, [call(connection, rec)])

    def test_detach(self):
        p, canary = self._detach_event_fixture()

        c1 = p.connect()

        connection = c1.connection
        rec = c1._connection_record

        c1.detach()

        eq_(canary.mock_calls, [call(connection, rec)])

    def test_detach_close(self):
        p, canary = self._close_detached_event_fixture()

        c1 = p.connect()

        connection = c1.connection

        c1.detach()

        c1.close()
        eq_(canary.mock_calls, [call(connection)])

    def test_first_connect_event(self):
        p, canary = self._first_connect_event_fixture()

        p.connect()
        eq_(canary, ["first_connect"])

    def test_first_connect_event_fires_once(self):
        p, canary = self._first_connect_event_fixture()

        p.connect()
        p.connect()

        eq_(canary, ["first_connect"])

    def test_first_connect_on_previously_recreated(self):
        p, canary = self._first_connect_event_fixture()

        p2 = p.recreate()
        p.connect()
        p2.connect()

        eq_(canary, ["first_connect", "first_connect"])

    def test_first_connect_on_subsequently_recreated(self):
        p, canary = self._first_connect_event_fixture()

        p.connect()
        p2 = p.recreate()
        p2.connect()

        eq_(canary, ["first_connect", "first_connect"])

    def test_connect_event(self):
        p, canary = self._connect_event_fixture()

        p.connect()
        eq_(canary, ["connect"])

    def test_connect_insert_event(self):
        p = self._queuepool_fixture()
        canary = []

        def connect_one(*arg, **kw):
            canary.append("connect_one")

        def connect_two(*arg, **kw):
            canary.append("connect_two")

        def connect_three(*arg, **kw):
            canary.append("connect_three")

        event.listen(p, "connect", connect_one)
        event.listen(p, "connect", connect_two, insert=True)
        event.listen(p, "connect", connect_three)

        p.connect()
        eq_(canary, ["connect_two", "connect_one", "connect_three"])

    def test_connect_event_fires_subsequent(self):
        p, canary = self._connect_event_fixture()

        c1 = p.connect()  # noqa
        c2 = p.connect()  # noqa

        eq_(canary, ["connect", "connect"])

    def test_connect_on_previously_recreated(self):
        p, canary = self._connect_event_fixture()

        p2 = p.recreate()

        p.connect()
        p2.connect()

        eq_(canary, ["connect", "connect"])

    def test_connect_on_subsequently_recreated(self):
        p, canary = self._connect_event_fixture()

        p.connect()
        p2 = p.recreate()
        p2.connect()

        eq_(canary, ["connect", "connect"])

    def test_checkout_event(self):
        p, canary = self._checkout_event_fixture()

        p.connect()
        eq_(canary, ["checkout"])

    def test_checkout_event_fires_subsequent(self):
        p, canary = self._checkout_event_fixture()

        p.connect()
        p.connect()
        eq_(canary, ["checkout", "checkout"])

    def test_checkout_event_on_subsequently_recreated(self):
        p, canary = self._checkout_event_fixture()

        p.connect()
        p2 = p.recreate()
        p2.connect()

        eq_(canary, ["checkout", "checkout"])

    def test_checkin_event(self):
        p, canary = self._checkin_event_fixture()

        c1 = p.connect()
        eq_(canary, [])
        c1.close()
        eq_(canary, ["checkin"])

    def test_reset_event(self):
        p, canary = self._reset_event_fixture()

        c1 = p.connect()
        eq_(canary, [])
        c1.close()
        eq_(canary, ["reset"])

    def test_soft_invalidate_event_no_exception(self):
        p, canary = self._soft_invalidate_event_fixture()

        c1 = p.connect()
        c1.close()
        assert not canary.called
        c1 = p.connect()
        dbapi_con = c1.connection
        c1.invalidate(soft=True)
        assert canary.call_args_list[0][0][0] is dbapi_con
        assert canary.call_args_list[0][0][2] is None

    def test_soft_invalidate_event_exception(self):
        p, canary = self._soft_invalidate_event_fixture()

        c1 = p.connect()
        c1.close()
        assert not canary.called
        c1 = p.connect()
        dbapi_con = c1.connection
        exc = Exception("hi")
        c1.invalidate(exc, soft=True)
        assert canary.call_args_list[0][0][0] is dbapi_con
        assert canary.call_args_list[0][0][2] is exc

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

    @testing.combinations((True, testing.requires.python3), (False,))
    def test_checkin_event_gc(self, detach_gced):
        p, canary = self._checkin_event_fixture(_is_asyncio=detach_gced)

        c1 = p.connect()

        dbapi_connection = weakref.ref(c1.connection)

        eq_(canary, [])
        del c1
        lazy_gc()

        if detach_gced:
            # "close_detached" is not called because for asyncio the
            # connection is just lost.
            eq_(canary, ["detach"])

        else:
            eq_(canary, ["checkin"])

        gc_collect()
        if detach_gced:
            is_none(dbapi_connection())
        else:
            is_not_none(dbapi_connection())

    def test_checkin_event_on_subsequently_recreated(self):
        p, canary = self._checkin_event_fixture()

        c1 = p.connect()
        p2 = p.recreate()
        c2 = p2.connect()

        eq_(canary, [])

        c1.close()
        eq_(canary, ["checkin"])

        c2.close()
        eq_(canary, ["checkin", "checkin"])

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
        event.listen(pool.Pool, "connect", listen_one)
        event.listen(engine.pool, "connect", listen_two)
        event.listen(engine, "connect", listen_three)
        event.listen(engine.__class__, "connect", listen_four)

        engine.execute(select(1)).close()
        eq_(
            canary, ["listen_one", "listen_four", "listen_two", "listen_three"]
        )

    def test_listen_targets_per_subclass(self):
        """test that listen() called on a subclass remains specific to
        that subclass."""

        canary = []

        def listen_one(*args):
            canary.append("listen_one")

        def listen_two(*args):
            canary.append("listen_two")

        def listen_three(*args):
            canary.append("listen_three")

        event.listen(pool.Pool, "connect", listen_one)
        event.listen(pool.QueuePool, "connect", listen_two)
        event.listen(pool.SingletonThreadPool, "connect", listen_three)

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

    def test_connect_event_fails_invalidates(self):
        fail = False

        def listen_one(conn, rec):
            if fail:
                raise Exception("it failed")

        def listen_two(conn, rec):
            rec.info["important_flag"] = True

        p1 = pool.QueuePool(
            creator=MockDBAPI().connect, pool_size=1, max_overflow=0
        )
        event.listen(p1, "connect", listen_one)
        event.listen(p1, "connect", listen_two)

        conn = p1.connect()
        eq_(conn.info["important_flag"], True)
        conn.invalidate()
        conn.close()

        fail = True
        assert_raises(Exception, p1.connect)

        fail = False

        conn = p1.connect()
        eq_(conn.info["important_flag"], True)
        conn.close()

    def teardown_test(self):
        # TODO: need to get remove() functionality
        # going
        pool.Pool.dispatch._clear()


class PoolFirstConnectSyncTest(PoolTestBase):
    """test for :ticket:`2964`, where the pool would not mutex the
    initialization of the dialect.

    Unfortunately, as discussed in :ticket:`6337`, this test suite did not
    ensure that the ``Engine`` itself actually uses the "first_connect" event,
    so when :ticket:`5497` came along, the "first_connect" event was no longer
    used and no test detected the re-introduction of the exact same race
    condition, which was now worse as the un-initialized dialect would now
    pollute the SQL cache causing the application to not work at all.

    A new suite has therefore been added in test/engine/test_execute.py->
    OnConnectTest::test_initialize_connect_race to ensure that the engine
    in total synchronizes the "first_connect" process, which now works
    using a new events feature _exec_w_sync_on_first_run.

    """

    @testing.requires.timing_intensive
    def test_sync(self):
        pool = self._queuepool_fixture(pool_size=3, max_overflow=0)

        evt = Mock()

        @event.listens_for(pool, "first_connect")
        def slow_first_connect(dbapi_con, rec):
            time.sleep(1)
            evt.first_connect()

        @event.listens_for(pool, "connect")
        def on_connect(dbapi_con, rec):
            evt.connect()

        def checkout():
            for j in range(2):
                c1 = pool.connect()
                time.sleep(0.02)
                c1.close()
                time.sleep(0.02)

        threads = []

        # what we're trying to do here is have concurrent use of
        # all three pooled connections at once, and the thing we want
        # to test is that first_connect() finishes completely before
        # any of the connections get returned.   so first_connect()
        # sleeps for one second, then pings the mock.  the threads should
        # not have made it to the "checkout() event for that one second.
        for i in range(5):
            th = threading.Thread(target=checkout)
            th.start()
            threads.append(th)
        for th in threads:
            th.join(join_timeout)

        # there is a very unlikely condition observed in CI on windows
        # where even though we have five threads above all calling upon the
        # pool, we didn't get concurrent use of all three connections, two
        # connections were enough.  so here we purposely just check out
        # all three at once just to get a consistent test result.
        make_sure_all_three_are_connected = [pool.connect() for i in range(3)]
        for conn in make_sure_all_three_are_connected:
            conn.close()

        eq_(
            evt.mock_calls,
            [
                call.first_connect(),
                call.connect(),
                call.connect(),
                call.connect(),
            ],
        )


class QueuePoolTest(PoolTestBase):
    def test_queuepool_del(self):
        self._do_testqueuepool(useclose=False)

    def test_queuepool_close(self):
        self._do_testqueuepool(useclose=True)

    def _do_testqueuepool(self, useclose=False):
        p = self._queuepool_fixture(pool_size=3, max_overflow=-1)

        def status(pool):
            return (
                pool.size(),
                pool.checkedin(),
                pool.overflow(),
                pool.checkedout(),
            )

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
        eq_(status(p), (3, 3, 3, 3))
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

    def test_timeout_accessor(self):
        expected_timeout = 123
        p = self._queuepool_fixture(timeout=expected_timeout)
        eq_(p.timeout(), expected_timeout)

    @testing.requires.timing_intensive
    def test_timeout(self):
        p = self._queuepool_fixture(pool_size=3, max_overflow=0, timeout=2)
        c1 = p.connect()  # noqa
        c2 = p.connect()  # noqa
        c3 = p.connect()  # noqa
        now = time.time()

        assert_raises(tsa.exc.TimeoutError, p.connect)
        assert int(time.time() - now) == 2

    @testing.requires.timing_intensive
    def test_timeout_subsecond_precision(self):
        p = self._queuepool_fixture(pool_size=1, max_overflow=0, timeout=0.5)
        c1 = p.connect()  # noqa
        with expect_raises(tsa.exc.TimeoutError):
            now = time.time()
            c2 = p.connect()  # noqa
        # Python timing is not very accurate, the time diff should be very
        # close to 0.5s but we give 200ms of slack.
        assert 0.3 <= time.time() - now <= 0.7, "Pool timeout not respected"

    @testing.requires.threading_with_mock
    @testing.requires.timing_intensive
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
            creator=lambda: dbapi.connect(delay=0.05),
            pool_size=2,
            max_overflow=1,
            timeout=3,
        )
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
        reaper = testing.engines.ConnectionKiller()

        dbapi = MockDBAPI()
        mutex = threading.Lock()

        def creator():
            time.sleep(0.05)
            with mutex:
                return dbapi.connect()

        p = pool.QueuePool(
            creator=creator, pool_size=3, timeout=2, max_overflow=max_overflow
        )
        reaper.add_pool(p)
        peaks = []

        def whammy():
            for i in range(10):
                try:
                    con = p.connect()
                    time.sleep(0.005)
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

        reaper.assert_all_closed()

    def test_overflow_reset_on_failed_connect(self):
        dbapi = Mock()

        def failing_dbapi():
            raise Exception("connection failed")

        creator = dbapi.connect

        def create():
            return creator()

        p = pool.QueuePool(creator=create, pool_size=2, max_overflow=3)
        c1 = self._with_teardown(p.connect())  # noqa
        c2 = self._with_teardown(p.connect())  # noqa
        c3 = self._with_teardown(p.connect())  # noqa
        eq_(p._overflow, 1)
        creator = failing_dbapi
        assert_raises(Exception, p.connect)
        eq_(p._overflow, 1)

    @testing.requires.threading_with_mock
    @testing.requires.timing_intensive
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
            threading.Thread(target=run_test, args=("success_one", p, False)),
            threading.Thread(target=run_test, args=("success_two", p, False)),
            threading.Thread(target=run_test, args=("overflow_one", p, True)),
            threading.Thread(target=run_test, args=("overflow_two", p, False)),
            threading.Thread(
                target=run_test, args=("overflow_three", p, False)
            ),
        ]
        for t in threads:
            t.start()
            time.sleep(0.2)

        for t in threads:
            t.join(timeout=join_timeout)
        eq_(
            dbapi.connect().operation.mock_calls,
            [
                call("success_one"),
                call("success_two"),
                call("overflow_two"),
                call("overflow_three"),
                call("overflow_one"),
            ],
        )

    @testing.requires.threading_with_mock
    @testing.requires.timing_intensive
    def test_waiters_handled(self):
        """test that threads waiting for connections are
        handled when the pool is replaced.

        """
        mutex = threading.Lock()
        dbapi = MockDBAPI()

        def creator():
            with mutex:
                return dbapi.connect()

        success = []
        for timeout in (None, 30):
            for max_overflow in (0, -1, 3):
                p = pool.QueuePool(
                    creator=creator,
                    pool_size=2,
                    timeout=timeout,
                    max_overflow=max_overflow,
                )

                def waiter(p, timeout, max_overflow):
                    success_key = (timeout, max_overflow)
                    conn = p.connect()
                    success.append(success_key)
                    time.sleep(0.1)
                    conn.close()

                c1 = p.connect()  # noqa
                c2 = p.connect()

                threads = []
                for i in range(2):
                    t = threading.Thread(
                        target=waiter, args=(p, timeout, max_overflow)
                    )
                    t.daemon = True
                    t.start()
                    threads.append(t)

                # this sleep makes sure that the
                # two waiter threads hit upon wait()
                # inside the queue, before we invalidate the other
                # two conns
                time.sleep(0.2)
                p._invalidate(c2)

                for t in threads:
                    t.join(join_timeout)

        eq_(len(success), 12, "successes: %s" % success)

    def test_connrec_invalidated_within_checkout_no_race(self):
        """Test that a concurrent ConnectionRecord.invalidate() which
        occurs after the ConnectionFairy has called
        _ConnectionRecord.checkout()
        but before the ConnectionFairy tests "fairy.connection is None"
        will not result in an InvalidRequestError.

        This use case assumes that a listener on the checkout() event
        will be raising DisconnectionError so that a reconnect attempt
        may occur.

        """
        dbapi = MockDBAPI()

        def creator():
            return dbapi.connect()

        p = pool.QueuePool(creator=creator, pool_size=1, max_overflow=0)

        conn = p.connect()
        conn.close()

        _existing_checkout = pool._ConnectionRecord.checkout

        @classmethod
        def _decorate_existing_checkout(cls, *arg, **kw):
            fairy = _existing_checkout(*arg, **kw)
            connrec = fairy._connection_record
            connrec.invalidate()
            return fairy

        with patch(
            "sqlalchemy.pool._ConnectionRecord.checkout",
            _decorate_existing_checkout,
        ):
            conn = p.connect()
            is_(conn._connection_record.connection, None)
        conn.close()

    @testing.requires.threading_with_mock
    @testing.requires.timing_intensive
    def test_notify_waiters(self):
        dbapi = MockDBAPI()

        canary = []

        def creator():
            canary.append(1)
            return dbapi.connect()

        p1 = pool.QueuePool(
            creator=creator, pool_size=1, timeout=None, max_overflow=0
        )

        def waiter(p):
            conn = p.connect()
            canary.append(2)
            time.sleep(0.5)
            conn.close()

        c1 = p1.connect()

        threads = []
        for i in range(5):
            t = threading.Thread(target=waiter, args=(p1,))
            t.start()
            threads.append(t)
        time.sleep(0.5)
        eq_(canary, [1])

        # this also calls invalidate()
        # on c1
        p1._invalidate(c1)

        for t in threads:
            t.join(join_timeout)

        eq_(canary, [1, 1, 2, 2, 2, 2, 2])

    def test_dispose_closes_pooled(self):
        dbapi = MockDBAPI()

        p = pool.QueuePool(
            creator=dbapi.connect, pool_size=2, timeout=None, max_overflow=0
        )
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
    @testing.requires.timing_intensive
    def test_no_overflow(self):
        self._test_overflow(40, 0)

    @testing.requires.threading_with_mock
    @testing.requires.timing_intensive
    def test_max_overflow(self):
        self._test_overflow(40, 5)

    def test_overflow_no_gc(self):
        p = self._queuepool_fixture(pool_size=2, max_overflow=2)

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
            set([1, 1, 1, 1, 1, 1, 1, 0, 1, 1, 1, 0]),
        )

    def test_recycle(self):
        with patch("sqlalchemy.pool.base.time.time") as mock:
            mock.return_value = 10000

            p = self._queuepool_fixture(
                pool_size=1, max_overflow=0, recycle=30
            )
            c1 = p.connect()
            c_ref = weakref.ref(c1.connection)
            c1.close()
            mock.return_value = 10001
            c2 = p.connect()

            is_(c2.connection, c_ref())
            c2.close()

            mock.return_value = 10035
            c3 = p.connect()
            is_not(c3.connection, c_ref())

    @testing.requires.timing_intensive
    def test_recycle_on_invalidate(self):
        p = self._queuepool_fixture(pool_size=1, max_overflow=0)
        c1 = p.connect()
        c_ref = weakref.ref(c1.connection)
        c1.close()
        c2 = p.connect()
        is_(c2.connection, c_ref())

        c2_rec = c2._connection_record
        p._invalidate(c2)
        assert c2_rec.connection is None
        c2.close()
        time.sleep(0.5)
        c3 = p.connect()

        is_not(c3.connection, c_ref())

    @testing.requires.timing_intensive
    def test_recycle_on_soft_invalidate(self):
        p = self._queuepool_fixture(pool_size=1, max_overflow=0)
        c1 = p.connect()
        c_ref = weakref.ref(c1.connection)
        c1.close()
        c2 = p.connect()
        is_(c2.connection, c_ref())

        c2_rec = c2._connection_record

        # ensure pool invalidate time will be later than starttime
        # for ConnectionRecord objects above
        time.sleep(0.1)
        c2.invalidate(soft=True)

        is_(c2_rec.connection, c2.connection)

        c2.close()

        c3 = p.connect()
        is_not(c3.connection, c_ref())
        is_(c3._connection_record, c2_rec)
        is_(c2_rec.connection, c3.connection)

    def _no_wr_finalize(self):
        finalize_fairy = pool._finalize_fairy

        def assert_no_wr_callback(
            connection, connection_record, pool, ref, echo, fairy=None
        ):
            if fairy is None:
                raise AssertionError(
                    "finalize fairy was called as a weakref callback"
                )
            return finalize_fairy(
                connection, connection_record, pool, ref, echo, fairy
            )

        return patch.object(pool, "_finalize_fairy", assert_no_wr_callback)

    def _assert_cleanup_on_pooled_reconnect(self, dbapi, p):
        # p is QueuePool with size=1, max_overflow=2,
        # and one connection in the pool that will need to
        # reconnect when next used (either due to recycle or invalidate)

        with self._no_wr_finalize():
            eq_(p.checkedout(), 0)
            eq_(p._overflow, 0)
            dbapi.shutdown(True)
            assert_raises_context_ok(Exception, p.connect)
            eq_(p._overflow, 0)

            eq_(p.checkedout(), 0)  # and not 1

            dbapi.shutdown(False)

            c1 = self._with_teardown(p.connect())  # noqa
            assert p._pool.empty()  # poolsize is one, so we're empty OK
            c2 = self._with_teardown(p.connect())  # noqa
            eq_(p._overflow, 1)  # and not 2

            # this hangs if p._overflow is 2
            c3 = self._with_teardown(p.connect())

            c3.close()

    def test_error_on_pooled_reconnect_cleanup_invalidate(self):
        dbapi, p = self._queuepool_dbapi_fixture(pool_size=1, max_overflow=2)
        c1 = p.connect()
        c1.invalidate()
        c1.close()
        self._assert_cleanup_on_pooled_reconnect(dbapi, p)

    @testing.requires.timing_intensive
    def test_error_on_pooled_reconnect_cleanup_recycle(self):
        dbapi, p = self._queuepool_dbapi_fixture(
            pool_size=1, max_overflow=2, recycle=1
        )
        c1 = p.connect()
        c1.close()
        time.sleep(1.5)
        self._assert_cleanup_on_pooled_reconnect(dbapi, p)

    @testing.requires.timing_intensive
    def test_connect_handler_not_called_for_recycled(self):
        """test [ticket:3497]"""

        dbapi, p = self._queuepool_dbapi_fixture(pool_size=2, max_overflow=2)

        canary = Mock()

        c1 = p.connect()
        c2 = p.connect()

        c1.close()
        c2.close()

        dbapi.shutdown(True)

        # ensure pool invalidate time will be later than starttime
        # for ConnectionRecord objects above
        time.sleep(0.1)

        bad = p.connect()
        p._invalidate(bad)
        bad.close()
        assert p._invalidate_time

        event.listen(p, "connect", canary.connect)
        event.listen(p, "checkout", canary.checkout)

        assert_raises(Exception, p.connect)

        p._pool.queue = collections.deque(
            [c for c in p._pool.queue if c.connection is not None]
        )

        dbapi.shutdown(False)
        c = p.connect()
        c.close()

        eq_(
            canary.mock_calls,
            [call.connect(ANY, ANY), call.checkout(ANY, ANY, ANY)],
        )

    @testing.requires.timing_intensive
    def test_connect_checkout_handler_always_gets_info(self):
        """test [ticket:3497]"""

        dbapi, p = self._queuepool_dbapi_fixture(pool_size=2, max_overflow=2)

        c1 = p.connect()
        c2 = p.connect()

        c1.close()
        c2.close()

        dbapi.shutdown(True)

        # ensure pool invalidate time will be later than starttime
        # for ConnectionRecord objects above
        time.sleep(0.1)

        bad = p.connect()
        p._invalidate(bad)
        bad.close()
        assert p._invalidate_time

        @event.listens_for(p, "connect")
        def connect(conn, conn_rec):
            conn_rec.info["x"] = True

        @event.listens_for(p, "checkout")
        def checkout(conn, conn_rec, conn_f):
            assert "x" in conn_rec.info

        assert_raises(Exception, p.connect)

        p._pool.queue = collections.deque(
            [c for c in p._pool.queue if c.connection is not None]
        )

        dbapi.shutdown(False)
        c = p.connect()
        c.close()

    def test_error_on_pooled_reconnect_cleanup_wcheckout_event(self):
        dbapi, p = self._queuepool_dbapi_fixture(pool_size=1, max_overflow=2)

        c1 = p.connect()
        c1.close()

        @event.listens_for(p, "checkout")
        def handle_checkout_event(dbapi_con, con_record, con_proxy):
            if dbapi.is_shutdown:
                raise tsa.exc.DisconnectionError()

        self._assert_cleanup_on_pooled_reconnect(dbapi, p)

    @testing.combinations((True, testing.requires.python3), (False,))
    def test_userspace_disconnectionerror_weakref_finalizer(self, detach_gced):
        dbapi, pool = self._queuepool_dbapi_fixture(
            pool_size=1, max_overflow=2, _is_asyncio=detach_gced
        )

        if detach_gced:
            pool._dialect.is_async = True

        @event.listens_for(pool, "checkout")
        def handle_checkout_event(dbapi_con, con_record, con_proxy):
            if getattr(dbapi_con, "boom") == "yes":
                raise tsa.exc.DisconnectionError()

        conn = pool.connect()
        old_dbapi_conn = conn.connection
        conn.close()

        eq_(old_dbapi_conn.mock_calls, [call.rollback()])

        old_dbapi_conn.boom = "yes"

        conn = pool.connect()
        dbapi_conn = conn.connection
        del conn
        gc_collect()

        if detach_gced:
            # new connection was detached + abandoned on return
            eq_(dbapi_conn.mock_calls, [])
        else:
            # new connection reset and returned to pool
            eq_(dbapi_conn.mock_calls, [call.rollback()])

        # old connection was just closed - did not get an
        # erroneous reset on return
        eq_(old_dbapi_conn.mock_calls, [call.rollback(), call.close()])

    @testing.requires.timing_intensive
    def test_recycle_pool_no_race(self):
        def slow_close():
            slow_closing_connection._slow_close()
            time.sleep(0.5)

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
                conn._handle_dbapi_exception(
                    Error(), "statement", {}, Mock(), Mock()
                )
            except tsa.exc.DBAPIError:
                pass

        # run an error + invalidate operation on the remaining 7 open
        # connections
        threads = []
        for conn in conns:
            t = threading.Thread(target=attempt, args=(conn,))
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
        p = self._queuepool_fixture(
            reset_on_return=None, pool_size=1, max_overflow=0
        )
        p2 = p.recreate()
        assert p2.size() == 1
        assert p2._reset_on_return is pool.reset_none
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
        c2 = p.connect()  # noqa
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

    def test_no_double_checkin(self):
        p = self._queuepool_fixture(pool_size=1)

        c1 = p.connect()
        rec = c1._connection_record
        c1.close()
        assert_raises_message(
            Warning, "Double checkin attempted on %s" % rec, rec.checkin
        )

    def test_lifo(self):
        c1, c2, c3 = Mock(), Mock(), Mock()
        connections = [c1, c2, c3]

        def creator():
            return connections.pop(0)

        p = pool.QueuePool(creator, use_lifo=True)

        pc1 = p.connect()
        pc2 = p.connect()
        pc3 = p.connect()

        pc1.close()
        pc2.close()
        pc3.close()

        for i in range(5):
            pc1 = p.connect()
            is_(pc1.connection, c3)
            pc1.close()

            pc1 = p.connect()
            is_(pc1.connection, c3)

            pc2 = p.connect()
            is_(pc2.connection, c2)
            pc2.close()

            pc3 = p.connect()
            is_(pc3.connection, c2)

            pc2 = p.connect()
            is_(pc2.connection, c1)

            pc2.close()
            pc3.close()
            pc1.close()

    def test_fifo(self):
        c1, c2, c3 = Mock(), Mock(), Mock()
        connections = [c1, c2, c3]

        def creator():
            return connections.pop(0)

        p = pool.QueuePool(creator)

        pc1 = p.connect()
        pc2 = p.connect()
        pc3 = p.connect()

        pc1.close()
        pc2.close()
        pc3.close()

        pc1 = p.connect()
        is_(pc1.connection, c1)
        pc1.close()

        pc1 = p.connect()
        is_(pc1.connection, c2)

        pc2 = p.connect()
        is_(pc2.connection, c3)
        pc2.close()

        pc3 = p.connect()
        is_(pc3.connection, c1)

        pc2 = p.connect()
        is_(pc2.connection, c3)

        pc2.close()
        pc3.close()
        pc1.close()


class ResetOnReturnTest(PoolTestBase):
    def _fixture(self, **kw):
        dbapi = Mock()
        return (
            dbapi,
            pool.QueuePool(creator=lambda: dbapi.connect("foo.db"), **kw),
        )

    def test_plain_rollback(self):
        dbapi, p = self._fixture(reset_on_return="rollback")

        c1 = p.connect()
        c1.close()
        assert dbapi.connect().rollback.called
        assert not dbapi.connect().commit.called

    def test_plain_commit(self):
        dbapi, p = self._fixture(reset_on_return="commit")

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


class SingletonThreadPoolTest(PoolTestBase):
    @testing.requires.threading_with_mock
    def test_cleanup(self):
        self._test_cleanup(False)

    #   TODO: the SingletonThreadPool cleanup method
    #   has an unfixed race condition within the "cleanup" system that
    #   leads to this test being off by one connection under load; in any
    #   case, this connection will be closed once it is garbage collected.
    #   this pool is not a production-level pool and is only used for the
    #   SQLite "memory" connection, and is not very useful under actual
    #   multi-threaded conditions
    #    @testing.requires.threading_with_mock
    #    def test_cleanup_no_gc(self):
    #       self._test_cleanup(True)

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
                time.sleep(0.01)

        threads = []
        for i in range(10):
            th = threading.Thread(target=checkout)
            th.start()
            threads.append(th)
        for th in threads:
            th.join(join_timeout)

        lp = len(p._all_conns)
        is_true(3 <= lp <= 4)

        if strong_refs:
            still_opened = len([c for c in sr if not c.close.call_count])
            eq_(still_opened, 3)

    def test_no_rollback_from_nested_connections(self):
        dbapi = MockDBAPI()

        lock = threading.Lock()

        def creator():
            # the mock iterator isn't threadsafe...
            with lock:
                return dbapi.connect()

        p = pool.SingletonThreadPool(creator=creator, pool_size=3)

        c1 = p.connect()
        mock_conn = c1.connection

        c2 = p.connect()
        is_(c1, c2)

        c2.close()

        eq_(mock_conn.mock_calls, [])
        c1.close()

        eq_(mock_conn.mock_calls, [call.rollback()])


class AssertionPoolTest(PoolTestBase):
    def test_connect_error(self):
        dbapi = MockDBAPI()
        p = pool.AssertionPool(creator=lambda: dbapi.connect("foo.db"))
        c1 = p.connect()  # noqa
        assert_raises(AssertionError, p.connect)

    def test_connect_multiple(self):
        dbapi = MockDBAPI()
        p = pool.AssertionPool(creator=lambda: dbapi.connect("foo.db"))
        c1 = p.connect()
        c1.close()
        c2 = p.connect()
        c2.close()

        c3 = p.connect()  # noqa
        assert_raises(AssertionError, p.connect)


class NullPoolTest(PoolTestBase):
    def test_reconnect(self):
        dbapi = MockDBAPI()
        p = pool.NullPool(creator=lambda: dbapi.connect("foo.db"))
        c1 = p.connect()

        c1.close()
        c1 = None

        c1 = p.connect()
        c1.invalidate()
        c1 = None

        c1 = p.connect()
        dbapi.connect.assert_has_calls(
            [call("foo.db"), call("foo.db")], any_order=True
        )


class StaticPoolTest(PoolTestBase):
    def test_recreate(self):
        dbapi = MockDBAPI()

        def creator():
            return dbapi.connect("foo.db")

        p = pool.StaticPool(creator)
        p2 = p.recreate()
        assert p._creator is p2._creator

    def test_connect(self):
        dbapi = MockDBAPI()

        def creator():
            return dbapi.connect("foo.db")

        p = pool.StaticPool(creator)

        c1 = p.connect()
        conn = c1.connection
        c1.close()

        c2 = p.connect()
        is_(conn, c2.connection)


class CreatorCompatibilityTest(PoolTestBase):
    def test_creator_callable_outside_noarg(self):
        e = testing_engine()

        creator = e.pool._creator
        try:
            conn = creator()
        finally:
            conn.close()

    def test_creator_callable_outside_witharg(self):
        e = testing_engine()

        creator = e.pool._creator
        try:
            conn = creator(Mock())
        finally:
            conn.close()

    def test_creator_patching_arg_to_noarg(self):
        e = testing_engine()
        creator = e.pool._creator
        try:
            # the creator is the two-arg form
            conn = creator(Mock())
        finally:
            conn.close()

        def mock_create():
            return creator()

        conn = e.connect()
        conn.invalidate()
        conn.close()

        # test that the 'should_wrap_creator' status
        # will dynamically switch if the _creator is monkeypatched.

        # patch it with a zero-arg form
        with patch.object(e.pool, "_creator", mock_create):
            conn = e.connect()
            conn.invalidate()
            conn.close()

        conn = e.connect()
        conn.close()
