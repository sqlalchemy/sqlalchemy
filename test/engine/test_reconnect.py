import time

import sqlalchemy as tsa
from sqlalchemy import create_engine
from sqlalchemy import event
from sqlalchemy import exc
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import pool
from sqlalchemy import select
from sqlalchemy import String
from sqlalchemy import testing
from sqlalchemy import util
from sqlalchemy.engine import url
from sqlalchemy.testing import assert_raises
from sqlalchemy.testing import assert_raises_message
from sqlalchemy.testing import assert_raises_message_context_ok
from sqlalchemy.testing import engines
from sqlalchemy.testing import eq_
from sqlalchemy.testing import expect_raises
from sqlalchemy.testing import expect_warnings
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import is_
from sqlalchemy.testing import is_false
from sqlalchemy.testing import is_true
from sqlalchemy.testing import mock
from sqlalchemy.testing import ne_
from sqlalchemy.testing.engines import testing_engine
from sqlalchemy.testing.mock import call
from sqlalchemy.testing.mock import Mock
from sqlalchemy.testing.mock import patch
from sqlalchemy.testing.schema import Column
from sqlalchemy.testing.schema import Table
from sqlalchemy.testing.util import gc_collect


class MockError(Exception):
    pass


class MockDisconnect(MockError):
    pass


class MockExitIsh(BaseException):
    pass


def mock_connection():
    def mock_cursor():
        def execute(*args, **kwargs):
            if conn.explode == "execute":
                raise MockDisconnect("Lost the DB connection on execute")
            elif conn.explode == "interrupt":
                conn.explode = "explode_no_disconnect"
                raise MockExitIsh("Keyboard / greenlet / etc interruption")
            elif conn.explode == "interrupt_dont_break":
                conn.explode = None
                raise MockExitIsh("Keyboard / greenlet / etc interruption")
            elif conn.explode in (
                "execute_no_disconnect",
                "explode_no_disconnect",
            ):
                raise MockError(
                    "something broke on execute but we didn't lose the "
                    "connection"
                )
            elif conn.explode in (
                "rollback",
                "rollback_no_disconnect",
                "explode_no_disconnect",
            ):
                raise MockError(
                    "something broke on execute but we didn't lose the "
                    "connection"
                )
            elif args and "SELECT" in args[0]:
                cursor.description = [("foo", None, None, None, None, None)]
            else:
                return

        def close():
            cursor.fetchall = cursor.fetchone = Mock(
                side_effect=MockError("cursor closed")
            )

        cursor = Mock(
            execute=Mock(side_effect=execute), close=Mock(side_effect=close)
        )
        return cursor

    def cursor():
        while True:
            yield mock_cursor()

    def rollback():
        if conn.explode == "rollback":
            raise MockDisconnect("Lost the DB connection on rollback")
        if conn.explode == "rollback_no_disconnect":
            raise MockError(
                "something broke on rollback but we didn't lose the "
                "connection"
            )
        else:
            return

    def commit():
        if conn.explode == "commit":
            raise MockDisconnect("Lost the DB connection on commit")
        elif conn.explode == "commit_no_disconnect":
            raise MockError(
                "something broke on commit but we didn't lose the "
                "connection"
            )
        else:
            return

    conn = Mock(
        rollback=Mock(side_effect=rollback),
        commit=Mock(side_effect=commit),
        cursor=Mock(side_effect=cursor()),
    )
    return conn


def MockDBAPI():
    connections = []
    stopped = [False]

    def connect():
        while True:
            if stopped[0]:
                raise MockDisconnect("database is stopped")
            conn = mock_connection()
            connections.append(conn)
            yield conn

    def shutdown(explode="execute", stop=False):
        stopped[0] = stop
        for c in connections:
            c.explode = explode

    def restart():
        stopped[0] = False
        connections[:] = []

    def dispose():
        stopped[0] = False
        for c in connections:
            c.explode = None
        connections[:] = []

    return Mock(
        connect=Mock(side_effect=connect()),
        shutdown=Mock(side_effect=shutdown),
        dispose=Mock(side_effect=dispose),
        restart=Mock(side_effect=restart),
        paramstyle="named",
        connections=connections,
        Error=MockError,
    )


class PrePingMockTest(fixtures.TestBase):
    def setup_test(self):
        self.dbapi = MockDBAPI()

    def _pool_fixture(self, pre_ping, pool_kw=None):
        dialect = url.make_url(
            "postgresql://foo:bar@localhost/test"
        ).get_dialect()()
        dialect.dbapi = self.dbapi
        _pool = pool.QueuePool(
            creator=lambda: self.dbapi.connect("foo.db"),
            pre_ping=pre_ping,
            dialect=dialect,
            **(pool_kw if pool_kw else {})
        )

        dialect.is_disconnect = lambda e, conn, cursor: isinstance(
            e, MockDisconnect
        )
        return _pool

    def teardown_test(self):
        self.dbapi.dispose()

    def test_ping_not_on_first_connect(self):
        pool = self._pool_fixture(
            pre_ping=True, pool_kw=dict(pool_size=1, max_overflow=0)
        )

        conn = pool.connect()
        dbapi_conn = conn.connection
        eq_(dbapi_conn.mock_calls, [])
        conn.close()

        # no ping, so no cursor() call.
        eq_(dbapi_conn.mock_calls, [call.rollback()])

        conn = pool.connect()
        is_(conn.connection, dbapi_conn)

        # ping, so cursor() call.
        eq_(dbapi_conn.mock_calls, [call.rollback(), call.cursor()])

        conn.close()

        conn = pool.connect()
        is_(conn.connection, dbapi_conn)

        # ping, so cursor() call.
        eq_(
            dbapi_conn.mock_calls,
            [call.rollback(), call.cursor(), call.rollback(), call.cursor()],
        )

        conn.close()

    def test_ping_not_on_reconnect(self):
        pool = self._pool_fixture(
            pre_ping=True, pool_kw=dict(pool_size=1, max_overflow=0)
        )

        conn = pool.connect()
        dbapi_conn = conn.connection
        conn_rec = conn._connection_record
        eq_(dbapi_conn.mock_calls, [])
        conn.close()

        conn = pool.connect()
        is_(conn.connection, dbapi_conn)
        # ping, so cursor() call.
        eq_(dbapi_conn.mock_calls, [call.rollback(), call.cursor()])

        conn.invalidate()

        is_(conn.connection, None)

        # connect again, make sure we're on the same connection record
        conn = pool.connect()
        is_(conn._connection_record, conn_rec)

        # no ping
        dbapi_conn = conn.connection
        eq_(dbapi_conn.mock_calls, [])

    def test_connect_across_restart(self):
        pool = self._pool_fixture(pre_ping=True)

        conn = pool.connect()
        stale_connection = conn.connection
        conn.close()

        self.dbapi.shutdown("execute")
        self.dbapi.restart()

        conn = pool.connect()
        cursor = conn.cursor()
        cursor.execute("hi")

        stale_cursor = stale_connection.cursor()
        assert_raises(MockDisconnect, stale_cursor.execute, "hi")

    def test_raise_db_is_stopped(self):
        pool = self._pool_fixture(pre_ping=True)

        conn = pool.connect()
        conn.close()

        self.dbapi.shutdown("execute", stop=True)

        assert_raises_message_context_ok(
            MockDisconnect, "database is stopped", pool.connect
        )

    def test_waits_til_exec_wo_ping_db_is_stopped(self):
        pool = self._pool_fixture(pre_ping=False)

        conn = pool.connect()
        conn.close()

        self.dbapi.shutdown("execute", stop=True)

        conn = pool.connect()

        cursor = conn.cursor()
        assert_raises_message(
            MockDisconnect,
            "Lost the DB connection on execute",
            cursor.execute,
            "foo",
        )

    def test_waits_til_exec_wo_ping_db_is_restarted(self):
        pool = self._pool_fixture(pre_ping=False)

        conn = pool.connect()
        conn.close()

        self.dbapi.shutdown("execute", stop=True)
        self.dbapi.restart()

        conn = pool.connect()

        cursor = conn.cursor()
        assert_raises_message(
            MockDisconnect,
            "Lost the DB connection on execute",
            cursor.execute,
            "foo",
        )

    @testing.requires.predictable_gc
    def test_pre_ping_weakref_finalizer(self):
        pool = self._pool_fixture(pre_ping=True)

        conn = pool.connect()
        old_dbapi_conn = conn.connection
        conn.close()

        # no cursor() because no pre ping
        eq_(old_dbapi_conn.mock_calls, [call.rollback()])

        conn = pool.connect()
        conn.close()

        # connect again, we see pre-ping
        eq_(
            old_dbapi_conn.mock_calls,
            [call.rollback(), call.cursor(), call.rollback()],
        )

        self.dbapi.shutdown("execute", stop=True)
        self.dbapi.restart()

        conn = pool.connect()
        dbapi_conn = conn.connection
        del conn
        gc_collect()

        # new connection was reset on return appropriately
        eq_(dbapi_conn.mock_calls, [call.rollback()])

        # old connection was just closed - did not get an
        # erroneous reset on return
        eq_(
            old_dbapi_conn.mock_calls,
            [
                call.rollback(),
                call.cursor(),
                call.rollback(),
                call.cursor(),
                call.close(),
            ],
        )


class MockReconnectTest(fixtures.TestBase):
    def setup_test(self):
        self.dbapi = MockDBAPI()

        self.db = testing_engine(
            "postgresql://foo:bar@localhost/test",
            options=dict(module=self.dbapi, _initialize=False),
        )

        self.mock_connect = call(
            host="localhost", password="bar", user="foo", database="test"
        )
        # monkeypatch disconnect checker
        self.db.dialect.is_disconnect = lambda e, conn, cursor: isinstance(
            e, MockDisconnect
        )

    def teardown_test(self):
        self.dbapi.dispose()

    def test_reconnect(self):
        """test that an 'is_disconnect' condition will invalidate the
        connection, and additionally dispose the previous connection
        pool and recreate."""

        # make a connection

        conn = self.db.connect()

        # connection works

        conn.execute(select(1))

        # create a second connection within the pool, which we'll ensure
        # also goes away

        conn2 = self.db.connect()
        conn2.close()

        # two connections opened total now

        assert len(self.dbapi.connections) == 2

        # set it to fail

        self.dbapi.shutdown()

        # force windows monotonic timer to definitely increment
        time.sleep(0.5)

        # close on DBAPI connection occurs here, as it is detected
        # as invalid.
        assert_raises(tsa.exc.DBAPIError, conn.execute, select(1))

        # assert was invalidated

        assert not conn.closed
        assert conn.invalidated

        # close shouldn't break

        conn.close()

        # ensure one connection closed...
        eq_(
            [c.close.mock_calls for c in self.dbapi.connections],
            [[call()], []],
        )

        conn = self.db.connect()

        eq_(
            [c.close.mock_calls for c in self.dbapi.connections],
            [[call()], [call()], []],
        )

        conn.execute(select(1))
        conn.close()

        eq_(
            [c.close.mock_calls for c in self.dbapi.connections],
            [[call()], [call()], []],
        )

    def test_invalidate_on_execute_trans(self):
        conn = self.db.connect()
        trans = conn.begin()
        self.dbapi.shutdown()

        assert_raises(tsa.exc.DBAPIError, conn.execute, select(1))

        eq_([c.close.mock_calls for c in self.dbapi.connections], [[call()]])
        assert not conn.closed
        assert conn.invalidated
        assert trans.is_active
        assert_raises_message(
            tsa.exc.PendingRollbackError,
            "Can't reconnect until invalid transaction is rolled back",
            conn.execute,
            select(1),
        )
        assert trans.is_active

        assert_raises_message(
            tsa.exc.PendingRollbackError,
            "Can't reconnect until invalid transaction is rolled back",
            trans.commit,
        )

        # now it's inactive...
        assert not trans.is_active

        # but still associated with the connection
        assert_raises_message(
            tsa.exc.PendingRollbackError,
            "Can't reconnect until invalid transaction is rolled back",
            conn.execute,
            select(1),
        )
        assert not trans.is_active

        # still can't commit... error stays the same
        assert_raises_message(
            tsa.exc.PendingRollbackError,
            "Can't reconnect until invalid transaction is rolled back",
            trans.commit,
        )

        trans.rollback()
        assert not trans.is_active
        conn.execute(select(1))
        assert not conn.invalidated
        eq_(
            [c.close.mock_calls for c in self.dbapi.connections],
            [[call()], []],
        )

    def test_invalidate_on_commit_trans(self):
        conn = self.db.connect()
        trans = conn.begin()
        self.dbapi.shutdown("commit")

        assert_raises(tsa.exc.DBAPIError, trans.commit)

        assert not conn.closed
        assert conn.invalidated
        assert not trans.is_active

        # error stays consistent
        assert_raises_message(
            tsa.exc.PendingRollbackError,
            "Can't reconnect until invalid transaction is rolled back",
            conn.execute,
            select(1),
        )
        assert not trans.is_active

        assert_raises_message(
            tsa.exc.PendingRollbackError,
            "Can't reconnect until invalid transaction is rolled back",
            trans.commit,
        )

        assert not trans.is_active

        assert_raises_message(
            tsa.exc.PendingRollbackError,
            "Can't reconnect until invalid transaction is rolled back",
            conn.execute,
            select(1),
        )
        assert not trans.is_active

        trans.rollback()
        assert not trans.is_active
        conn.execute(select(1))
        assert not conn.invalidated

    def test_commit_fails_contextmanager(self):
        # this test is also performed in test/engine/test_transaction.py
        # using real connections
        conn = self.db.connect()

        def go():
            with conn.begin():
                self.dbapi.shutdown("commit_no_disconnect")

        assert_raises(tsa.exc.DBAPIError, go)

        assert not conn.in_transaction()

    def test_commit_fails_trans(self):
        # this test is also performed in test/engine/test_transaction.py
        # using real connections

        conn = self.db.connect()
        trans = conn.begin()
        self.dbapi.shutdown("commit_no_disconnect")

        assert_raises(tsa.exc.DBAPIError, trans.commit)

        assert not conn.closed
        assert not conn.invalidated
        assert not trans.is_active

        # error stays consistent
        assert_raises_message(
            tsa.exc.PendingRollbackError,
            "This connection is on an inactive transaction.  Please rollback",
            conn.execute,
            select(1),
        )
        assert not trans.is_active

        assert_raises_message(
            tsa.exc.PendingRollbackError,
            "This connection is on an inactive transaction.  Please rollback",
            trans.commit,
        )

        assert not trans.is_active

        assert_raises_message(
            tsa.exc.PendingRollbackError,
            "This connection is on an inactive transaction.  Please rollback",
            conn.execute,
            select(1),
        )
        assert not trans.is_active

        trans.rollback()
        assert not trans.is_active
        conn.execute(select(1))
        assert not conn.invalidated

    def test_invalidate_dont_call_finalizer(self):
        conn = self.db.connect()
        finalizer = mock.Mock()
        conn.connection._connection_record.finalize_callback.append(finalizer)
        conn.invalidate()
        assert conn.invalidated
        eq_(finalizer.call_count, 0)

    def test_conn_reusable(self):
        conn = self.db.connect()

        conn.execute(select(1))

        eq_(self.dbapi.connect.mock_calls, [self.mock_connect])

        self.dbapi.shutdown()

        assert_raises(tsa.exc.DBAPIError, conn.execute, select(1))

        assert not conn.closed
        assert conn.invalidated

        eq_([c.close.mock_calls for c in self.dbapi.connections], [[call()]])

        # test reconnects
        conn.execute(select(1))
        assert not conn.invalidated

        eq_(
            [c.close.mock_calls for c in self.dbapi.connections],
            [[call()], []],
        )

    def test_invalidated_close(self):
        conn = self.db.connect()

        self.dbapi.shutdown()

        assert_raises(tsa.exc.DBAPIError, conn.execute, select(1))

        conn.close()
        assert conn.closed
        assert not conn.invalidated
        assert_raises_message(
            tsa.exc.ResourceClosedError,
            "This Connection is closed",
            conn.execute,
            select(1),
        )

    def test_noreconnect_execute_plus_closewresult(self):
        conn = self.db.connect(close_with_result=True)

        self.dbapi.shutdown("execute_no_disconnect")

        # raises error
        assert_raises_message(
            tsa.exc.DBAPIError,
            "something broke on execute but we didn't lose the connection",
            conn.execute,
            select(1),
        )

        assert conn.closed
        assert not conn.invalidated

    def test_noreconnect_rollback_plus_closewresult(self):
        conn = self.db.connect(close_with_result=True)

        self.dbapi.shutdown("rollback_no_disconnect")

        # raises error
        with expect_warnings(
            "An exception has occurred during handling .*"
            "something broke on execute but we didn't lose the connection",
            py2konly=True,
        ):
            assert_raises_message(
                tsa.exc.DBAPIError,
                "something broke on rollback but we didn't "
                "lose the connection",
                conn.execute,
                select(1),
            )

        assert conn.closed
        assert not conn.invalidated

        assert_raises_message(
            tsa.exc.ResourceClosedError,
            "This Connection is closed",
            conn.execute,
            select(1),
        )

    def test_reconnect_on_reentrant(self):
        conn = self.db.connect()

        conn.execute(select(1))

        assert len(self.dbapi.connections) == 1

        self.dbapi.shutdown("rollback")

        # raises error
        with expect_warnings(
            "An exception has occurred during handling .*"
            "something broke on execute but we didn't lose the connection",
            py2konly=True,
        ):
            assert_raises_message(
                tsa.exc.DBAPIError,
                "Lost the DB connection on rollback",
                conn.execute,
                select(1),
            )

        assert not conn.closed
        assert conn.invalidated

    def test_reconnect_on_reentrant_plus_closewresult(self):
        conn = self.db.connect(close_with_result=True)

        self.dbapi.shutdown("rollback")

        # raises error
        with expect_warnings(
            "An exception has occurred during handling .*"
            "something broke on execute but we didn't lose the connection",
            py2konly=True,
        ):
            assert_raises_message(
                tsa.exc.DBAPIError,
                "Lost the DB connection on rollback",
                conn.execute,
                select(1),
            )

        assert conn.closed
        assert not conn.invalidated

        assert_raises_message(
            tsa.exc.ResourceClosedError,
            "This Connection is closed",
            conn.execute,
            select(1),
        )

    def test_check_disconnect_no_cursor(self):
        conn = self.db.connect()
        result = conn.execute(select(1))
        result.cursor.close()
        conn.close()

        assert_raises_message(
            tsa.exc.DBAPIError, "cursor closed", list, result
        )

    def test_dialect_initialize_once(self):
        from sqlalchemy.engine.url import URL
        from sqlalchemy.engine.default import DefaultDialect

        dbapi = self.dbapi

        class MyURL(URL):
            def _get_entrypoint(self):
                return Dialect

            def get_dialect(self):
                return Dialect

        class Dialect(DefaultDialect):
            initialize = Mock()

        engine = create_engine(MyURL.create("foo://"), module=dbapi)
        engine.connect()

        # note that the dispose() call replaces the old pool with a new one;
        # this is to test that even though a single pool is using
        # dispatch.exec_once(), by replacing the pool with a new one, the event
        # would normally fire again onless once=True is set on the original
        # listen as well.

        engine.dispose()
        engine.connect()
        eq_(Dialect.initialize.call_count, 1)

    def test_dialect_initialize_retry_if_exception(self):
        from sqlalchemy.engine.url import URL
        from sqlalchemy.engine.default import DefaultDialect

        dbapi = self.dbapi

        class MyURL(URL):
            def _get_entrypoint(self):
                return Dialect

            def get_dialect(self):
                return Dialect

        class Dialect(DefaultDialect):
            initialize = Mock()

        # note that the first_connect hook is only invoked when the pool
        # makes a new DBAPI connection, and not when it checks out an existing
        # connection.  So there is a dependency here that if the initializer
        # raises an exception, the pool-level connection attempt is also
        # failed, meaning no DBAPI connection is pooled.  If the first_connect
        # exception raise did not prevent the connection from being pooled,
        # there could be the case where the pool could return that connection
        # on a subsequent attempt without initialization having proceeded.

        Dialect.initialize.side_effect = TypeError
        engine = create_engine(MyURL.create("foo://"), module=dbapi)

        assert_raises(TypeError, engine.connect)
        eq_(Dialect.initialize.call_count, 1)
        is_true(engine.pool._pool.empty())

        assert_raises(TypeError, engine.connect)
        eq_(Dialect.initialize.call_count, 2)
        is_true(engine.pool._pool.empty())

        engine.dispose()

        assert_raises(TypeError, engine.connect)
        eq_(Dialect.initialize.call_count, 3)
        is_true(engine.pool._pool.empty())

        Dialect.initialize.side_effect = None

        conn = engine.connect()
        eq_(Dialect.initialize.call_count, 4)
        conn.close()
        is_false(engine.pool._pool.empty())

        conn = engine.connect()
        eq_(Dialect.initialize.call_count, 4)
        conn.close()
        is_false(engine.pool._pool.empty())

        engine.dispose()
        conn = engine.connect()

        eq_(Dialect.initialize.call_count, 4)
        conn.close()
        is_false(engine.pool._pool.empty())

    def test_invalidate_conn_w_contextmanager_interrupt(self):
        # test [ticket:3803]
        pool = self.db.pool

        conn = self.db.connect()
        self.dbapi.shutdown("interrupt")

        def go():
            with conn.begin():
                conn.execute(select(1))

        assert_raises(MockExitIsh, go)

        assert conn.invalidated

        eq_(pool._invalidate_time, 0)  # pool not invalidated

        conn.execute(select(1))
        assert not conn.invalidated

    def test_invalidate_conn_interrupt_nodisconnect_workaround(self):
        # test [ticket:3803] workaround for no disconnect on keyboard interrupt

        @event.listens_for(self.db, "handle_error")
        def cancel_disconnect(ctx):
            ctx.is_disconnect = False

        pool = self.db.pool

        conn = self.db.connect()
        self.dbapi.shutdown("interrupt_dont_break")

        def go():
            with conn.begin():
                conn.execute(select(1))

        assert_raises(MockExitIsh, go)

        assert not conn.invalidated

        eq_(pool._invalidate_time, 0)  # pool not invalidated

        conn.execute(select(1))
        assert not conn.invalidated

    def test_invalidate_conn_w_contextmanager_disconnect(self):
        # test [ticket:3803] change maintains old behavior

        pool = self.db.pool

        conn = self.db.connect()
        self.dbapi.shutdown("execute")

        def go():
            with conn.begin():
                conn.execute(select(1))

        assert_raises(exc.DBAPIError, go)  # wraps a MockDisconnect

        assert conn.invalidated

        ne_(pool._invalidate_time, 0)  # pool is invalidated

        conn.execute(select(1))
        assert not conn.invalidated


class CursorErrTest(fixtures.TestBase):
    # this isn't really a "reconnect" test, it's more of
    # a generic "recovery".   maybe this test suite should have been
    # named "test_error_recovery".
    def _fixture(self, explode_on_exec, initialize):
        class DBAPIError(Exception):
            pass

        def MockDBAPI():
            def cursor():
                while True:
                    if explode_on_exec:
                        yield Mock(
                            description=[],
                            close=Mock(side_effect=DBAPIError("explode")),
                            execute=Mock(side_effect=DBAPIError("explode")),
                        )
                    else:
                        yield Mock(
                            description=[],
                            close=Mock(side_effect=Exception("explode")),
                        )

            def connect():
                while True:
                    yield Mock(
                        spec=["cursor", "commit", "rollback", "close"],
                        cursor=Mock(side_effect=cursor()),
                    )

            return Mock(
                Error=DBAPIError,
                paramstyle="qmark",
                connect=Mock(side_effect=connect()),
            )

        dbapi = MockDBAPI()

        from sqlalchemy.engine import default

        url = Mock(
            get_dialect=lambda: default.DefaultDialect,
            _get_entrypoint=lambda: default.DefaultDialect,
            _instantiate_plugins=lambda kwargs: (url, [], kwargs),
            translate_connect_args=lambda: {},
            query={},
        )
        eng = testing_engine(
            url, options=dict(module=dbapi, _initialize=initialize)
        )
        eng.pool.logger = Mock()

        def get_default_schema_name(connection):
            try:
                cursor = connection.connection.cursor()
                connection._cursor_execute(cursor, "statement", {})
                cursor.close()
            except exc.DBAPIError:
                util.warn("Exception attempting to detect")

        eng.dialect._get_default_schema_name = get_default_schema_name
        return eng

    def test_cursor_explode(self):
        db = self._fixture(False, False)
        conn = db.connect()
        result = conn.exec_driver_sql("select foo")
        result.close()
        conn.close()
        eq_(
            db.pool.logger.error.mock_calls,
            [call("Error closing cursor", exc_info=True)],
        )

    def test_cursor_shutdown_in_initialize(self):
        db = self._fixture(True, True)
        assert_raises_message_context_ok(
            exc.SAWarning, "Exception attempting to detect", db.connect
        )
        eq_(
            db.pool.logger.error.mock_calls,
            [call("Error closing cursor", exc_info=True)],
        )


def _assert_invalidated(fn, *args):
    try:
        fn(*args)
        assert False
    except tsa.exc.DBAPIError as e:
        if not e.connection_invalidated:
            raise


class RealReconnectTest(fixtures.TestBase):
    __backend__ = True
    __requires__ = "graceful_disconnects", "ad_hoc_engines"

    def setup_test(self):
        self.engine = engines.reconnecting_engine()

    def teardown_test(self):
        self.engine.dispose()

    def test_reconnect(self):
        with self.engine.connect() as conn:

            eq_(conn.execute(select(1)).scalar(), 1)
            assert not conn.closed

            self.engine.test_shutdown()

            _assert_invalidated(conn.execute, select(1))

            assert not conn.closed
            assert conn.invalidated

            assert conn.invalidated
            eq_(conn.execute(select(1)).scalar(), 1)
            assert not conn.invalidated

            # one more time
            self.engine.test_shutdown()
            _assert_invalidated(conn.execute, select(1))

            assert conn.invalidated
            eq_(conn.execute(select(1)).scalar(), 1)
            assert not conn.invalidated

    @testing.requires.independent_connections
    def test_multiple_invalidate(self):
        c1 = self.engine.connect()
        c2 = self.engine.connect()

        eq_(c1.execute(select(1)).scalar(), 1)

        self.engine.test_shutdown()

        _assert_invalidated(c1.execute, select(1))

        p2 = self.engine.pool

        _assert_invalidated(c2.execute, select(1))

        # pool isn't replaced
        assert self.engine.pool is p2

    def test_branched_invalidate_branch_to_parent(self):
        with self.engine.connect() as c1:

            with patch.object(self.engine.pool, "logger") as logger:
                c1_branch = c1.connect()
                eq_(c1_branch.execute(select(1)).scalar(), 1)

                self.engine.test_shutdown()

                _assert_invalidated(c1_branch.execute, select(1))
                assert c1.invalidated
                assert c1_branch.invalidated

                c1_branch._revalidate_connection()
                assert not c1.invalidated
                assert not c1_branch.invalidated

            assert "Invalidate connection" in logger.mock_calls[0][1][0]

    def test_branched_invalidate_parent_to_branch(self):
        with self.engine.connect() as c1:

            c1_branch = c1.connect()
            eq_(c1_branch.execute(select(1)).scalar(), 1)

            self.engine.test_shutdown()

            _assert_invalidated(c1.execute, select(1))
            assert c1.invalidated
            assert c1_branch.invalidated

            c1._revalidate_connection()
            assert not c1.invalidated
            assert not c1_branch.invalidated

    def test_branch_invalidate_state(self):
        with self.engine.connect() as c1:

            c1_branch = c1.connect()

            eq_(c1_branch.execute(select(1)).scalar(), 1)

            self.engine.test_shutdown()

            _assert_invalidated(c1_branch.execute, select(1))
            assert not c1_branch.closed
            assert not c1_branch._still_open_and_dbapi_connection_is_valid

    def test_ensure_is_disconnect_gets_connection(self):
        def is_disconnect(e, conn, cursor):
            # connection is still present
            assert conn.connection is not None
            # the error usually occurs on connection.cursor(),
            # though MySQLdb we get a non-working cursor.
            # assert cursor is None

        self.engine.dialect.is_disconnect = is_disconnect

        with self.engine.connect() as conn:
            self.engine.test_shutdown()
            with expect_warnings(
                "An exception has occurred during handling .*", py2konly=True
            ):
                assert_raises(tsa.exc.DBAPIError, conn.execute, select(1))

    def test_rollback_on_invalid_plain(self):
        with self.engine.connect() as conn:
            trans = conn.begin()
            conn.invalidate()
            trans.rollback()

    @testing.requires.two_phase_transactions
    def test_rollback_on_invalid_twophase(self):
        with self.engine.connect() as conn:
            trans = conn.begin_twophase()
            conn.invalidate()
            trans.rollback()

    @testing.requires.savepoints
    def test_rollback_on_invalid_savepoint(self):
        with self.engine.connect() as conn:
            conn.begin()
            trans2 = conn.begin_nested()
            conn.invalidate()
            trans2.rollback()

    def test_invalidate_twice(self):
        with self.engine.connect() as conn:
            conn.invalidate()
            conn.invalidate()

    @testing.skip_if(
        [lambda: util.py3k, "oracle+cx_oracle"], "Crashes on py3k+cx_oracle"
    )
    def test_explode_in_initializer(self):
        engine = engines.testing_engine()

        def broken_initialize(connection):
            connection.exec_driver_sql("select fake_stuff from _fake_table")

        engine.dialect.initialize = broken_initialize

        # raises a DBAPIError, not an AttributeError
        assert_raises(exc.DBAPIError, engine.connect)

    @testing.skip_if(
        [lambda: util.py3k, "oracle+cx_oracle"], "Crashes on py3k+cx_oracle"
    )
    def test_explode_in_initializer_disconnect(self):
        engine = engines.testing_engine()

        def broken_initialize(connection):
            connection.exec_driver_sql("select fake_stuff from _fake_table")

        engine.dialect.initialize = broken_initialize

        def is_disconnect(e, conn, cursor):
            return True

        engine.dialect.is_disconnect = is_disconnect

        # invalidate() also doesn't screw up
        assert_raises(exc.DBAPIError, engine.connect)

    def test_null_pool(self):
        engine = engines.reconnecting_engine(
            options=dict(poolclass=pool.NullPool)
        )
        with engine.connect() as conn:
            eq_(conn.execute(select(1)).scalar(), 1)
            assert not conn.closed
            engine.test_shutdown()
            _assert_invalidated(conn.execute, select(1))
            assert not conn.closed
            assert conn.invalidated
            eq_(conn.execute(select(1)).scalar(), 1)
            assert not conn.invalidated

    def test_close(self):
        with self.engine.connect() as conn:
            eq_(conn.execute(select(1)).scalar(), 1)
            assert not conn.closed

            self.engine.test_shutdown()

            _assert_invalidated(conn.execute, select(1))

        with self.engine.connect() as conn:
            eq_(conn.execute(select(1)).scalar(), 1)

    def test_with_transaction(self):
        with self.engine.connect() as conn:
            trans = conn.begin()
            assert trans.is_valid
            eq_(conn.execute(select(1)).scalar(), 1)
            assert not conn.closed
            self.engine.test_shutdown()
            _assert_invalidated(conn.execute, select(1))
            assert not conn.closed
            assert conn.invalidated
            assert trans.is_active
            assert not trans.is_valid

            assert_raises_message(
                tsa.exc.PendingRollbackError,
                "Can't reconnect until invalid transaction is rolled back",
                conn.execute,
                select(1),
            )
            assert trans.is_active
            assert not trans.is_valid

            assert_raises_message(
                tsa.exc.PendingRollbackError,
                "Can't reconnect until invalid transaction is rolled back",
                trans.commit,
            )

            # becomes inactive
            assert not trans.is_active
            assert not trans.is_valid

            # still asks us to rollback
            assert_raises_message(
                tsa.exc.PendingRollbackError,
                "Can't reconnect until invalid transaction is rolled back",
                conn.execute,
                select(1),
            )

            # still asks us..
            assert_raises_message(
                tsa.exc.PendingRollbackError,
                "Can't reconnect until invalid transaction is rolled back",
                trans.commit,
            )

            # still...it's being consistent in what it is asking.
            assert_raises_message(
                tsa.exc.PendingRollbackError,
                "Can't reconnect until invalid transaction is rolled back",
                conn.execute,
                select(1),
            )

            #  OK!
            trans.rollback()
            assert not trans.is_active
            assert not trans.is_valid

            # conn still invalid but we can reconnect
            assert conn.invalidated
            eq_(conn.execute(select(1)).scalar(), 1)
            assert not conn.invalidated


class RecycleTest(fixtures.TestBase):
    __backend__ = True

    def test_basic(self):
        engine = engines.reconnecting_engine()

        conn = engine.connect()
        eq_(conn.execute(select(1)).scalar(), 1)
        conn.close()

        # set the pool recycle down to 1.
        # we aren't doing this inline with the
        # engine create since cx_oracle takes way
        # too long to create the 1st connection and don't
        # want to build a huge delay into this test.

        engine.pool._recycle = 1

        # kill the DB connection
        engine.test_shutdown()

        # wait until past the recycle period
        time.sleep(2)

        # can connect, no exception
        conn = engine.connect()
        eq_(conn.execute(select(1)).scalar(), 1)
        conn.close()


class PrePingRealTest(fixtures.TestBase):
    __backend__ = True

    def test_pre_ping_db_is_restarted(self):
        engine = engines.reconnecting_engine(options={"pool_pre_ping": True})

        conn = engine.connect()
        eq_(conn.execute(select(1)).scalar(), 1)
        stale_connection = conn.connection.connection
        conn.close()

        engine.test_shutdown()
        engine.test_restart()

        conn = engine.connect()
        eq_(conn.execute(select(1)).scalar(), 1)
        conn.close()

        with expect_raises(engine.dialect.dbapi.Error, check_context=False):
            curs = stale_connection.cursor()
            curs.execute("select 1")

    def test_pre_ping_db_stays_shutdown(self):
        engine = engines.reconnecting_engine(options={"pool_pre_ping": True})

        if isinstance(engine.pool, pool.QueuePool):
            eq_(engine.pool.checkedin(), 0)
            eq_(engine.pool._overflow, -5)

        conn = engine.connect()
        eq_(conn.execute(select(1)).scalar(), 1)
        conn.close()

        if isinstance(engine.pool, pool.QueuePool):
            eq_(engine.pool.checkedin(), 1)
            eq_(engine.pool._overflow, -4)

        engine.test_shutdown(stop=True)

        assert_raises(exc.DBAPIError, engine.connect)

        if isinstance(engine.pool, pool.QueuePool):
            eq_(engine.pool.checkedin(), 1)
            eq_(engine.pool._overflow, -4)


class InvalidateDuringResultTest(fixtures.TestBase):
    __backend__ = True

    def setup_test(self):
        self.engine = engines.reconnecting_engine()
        self.meta = MetaData()
        table = Table(
            "sometable",
            self.meta,
            Column("id", Integer, primary_key=True),
            Column("name", String(50)),
        )

        with self.engine.begin() as conn:
            self.meta.create_all(conn)
            conn.execute(
                table.insert(),
                [{"id": i, "name": "row %d" % i} for i in range(1, 100)],
            )

    def teardown_test(self):
        with self.engine.begin() as conn:
            self.meta.drop_all(conn)
        self.engine.dispose()

    @testing.crashes(
        "oracle",
        "cx_oracle 6 doesn't allow a close like this due to open cursors",
    )
    @testing.fails_if(
        [
            "+mariadbconnector",
            "+mysqlconnector",
            "+mysqldb",
            "+cymysql",
            "+pymysql",
            "+pg8000",
            "+asyncpg",
            "+aiosqlite",
            "+aiomysql",
        ],
        "Buffers the result set and doesn't check for connection close",
    )
    def test_invalidate_on_results(self):
        conn = self.engine.connect()
        result = conn.exec_driver_sql("select * from sometable")
        for x in range(20):
            result.fetchone()
        self.engine.test_shutdown()
        try:
            _assert_invalidated(result.fetchone)
            assert conn.invalidated
        finally:
            conn.invalidate()


class ReconnectRecipeTest(fixtures.TestBase):
    """Test for the reconnect recipe given at doc/build/faq/connections.rst.

    Make sure the above document is updated if changes are made here.

    """

    # this recipe works on PostgreSQL also but only if the connection
    # is cut off from the server side, otherwise the connection.cursor()
    # method rightly fails because we explicitly closed the connection.
    # since we don't have a fixture
    # that can do this we currently rely on the MySQL drivers that allow
    # us to call cursor() even when the connection were closed.   In order
    # to get a real "cut the server off" kind of fixture we'd need to do
    # something in provisioning that seeks out the TCP connection at the
    # OS level and kills it.
    __only_on__ = ("mysql+mysqldb", "mysql+pymysql")

    future = False

    def make_engine(self, engine):
        num_retries = 3
        retry_interval = 0.5

        def _run_with_retries(fn, context, cursor, statement, *arg, **kw):
            for retry in range(num_retries + 1):
                try:
                    fn(cursor, statement, context=context, *arg)
                except engine.dialect.dbapi.Error as raw_dbapi_err:
                    connection = context.root_connection
                    if engine.dialect.is_disconnect(
                        raw_dbapi_err, connection, cursor
                    ):
                        if retry > num_retries:
                            raise
                        engine.logger.error(
                            "disconnection error, retrying operation",
                            exc_info=True,
                        )
                        connection.invalidate()

                        if self.future:
                            connection.rollback()
                        else:
                            trans = connection.get_transaction()
                            if trans:
                                trans.rollback()

                        time.sleep(retry_interval)
                        context.cursor = (
                            cursor
                        ) = connection.connection.cursor()
                    else:
                        raise
                else:
                    return True

        e = engine.execution_options(isolation_level="AUTOCOMMIT")

        @event.listens_for(e, "do_execute_no_params")
        def do_execute_no_params(cursor, statement, context):
            return _run_with_retries(
                context.dialect.do_execute_no_params,
                context,
                cursor,
                statement,
            )

        @event.listens_for(e, "do_execute")
        def do_execute(cursor, statement, parameters, context):
            return _run_with_retries(
                context.dialect.do_execute,
                context,
                cursor,
                statement,
                parameters,
            )

        return e

    __backend__ = True

    def setup_test(self):
        self.engine = engines.reconnecting_engine(
            options=dict(future=self.future)
        )
        self.meta = MetaData()
        self.table = Table(
            "sometable",
            self.meta,
            Column("id", Integer, primary_key=True),
            Column("name", String(50)),
        )
        self.meta.create_all(self.engine)

    def teardown_test(self):
        self.meta.drop_all(self.engine)
        self.engine.dispose()

    def test_restart_on_execute_no_txn(self):
        engine = self.make_engine(self.engine)

        with engine.connect() as conn:
            eq_(conn.execute(select(1)).scalar(), 1)

            self.engine.test_shutdown()
            self.engine.test_restart()

            eq_(conn.execute(select(1)).scalar(), 1)

    def test_restart_on_execute_txn(self):
        engine = self.make_engine(self.engine)

        with engine.begin() as conn:
            eq_(conn.execute(select(1)).scalar(), 1)

            self.engine.test_shutdown()
            self.engine.test_restart()

            eq_(conn.execute(select(1)).scalar(), 1)

    def test_autocommits_txn(self):
        engine = self.make_engine(self.engine)

        with engine.begin() as conn:
            conn.execute(
                self.table.insert(),
                [
                    {"id": 1, "name": "some name 1"},
                    {"id": 2, "name": "some name 2"},
                    {"id": 3, "name": "some name 3"},
                ],
            )

            self.engine.test_shutdown()
            self.engine.test_restart()

            eq_(
                conn.execute(
                    select(self.table).order_by(self.table.c.id)
                ).fetchall(),
                [(1, "some name 1"), (2, "some name 2"), (3, "some name 3")],
            )

    def test_fail_on_executemany_txn(self):
        engine = self.make_engine(self.engine)

        with engine.begin() as conn:
            conn.execute(
                self.table.insert(),
                [
                    {"id": 1, "name": "some name 1"},
                    {"id": 2, "name": "some name 2"},
                    {"id": 3, "name": "some name 3"},
                ],
            )

            self.engine.test_shutdown()
            self.engine.test_restart()

            assert_raises(
                exc.DBAPIError,
                conn.execute,
                self.table.insert(),
                [
                    {"id": 4, "name": "some name 4"},
                    {"id": 5, "name": "some name 5"},
                    {"id": 6, "name": "some name 6"},
                ],
            )
            if self.future:
                conn.rollback()
            else:
                trans = conn.get_transaction()
                trans.rollback()


class FutureReconnectRecipeTest(ReconnectRecipeTest):
    future = True
