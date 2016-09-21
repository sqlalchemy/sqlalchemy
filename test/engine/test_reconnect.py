from sqlalchemy.testing import eq_, ne_, assert_raises, assert_raises_message
import time
from sqlalchemy import (
    select, MetaData, Integer, String, create_engine, pool, exc, util)
from sqlalchemy.testing.schema import Table, Column
import sqlalchemy as tsa
from sqlalchemy import testing
from sqlalchemy.testing import mock
from sqlalchemy.testing import engines
from sqlalchemy.testing import fixtures
from sqlalchemy.testing.engines import testing_engine
from sqlalchemy.testing.mock import Mock, call, patch
from sqlalchemy import event


class MockError(Exception):
    pass


class MockDisconnect(MockError):
    pass


class MockExitIsh(BaseException):
    pass


def mock_connection():
    def mock_cursor():
        def execute(*args, **kwargs):
            if conn.explode == 'execute':
                raise MockDisconnect("Lost the DB connection on execute")
            elif conn.explode == 'interrupt':
                conn.explode = "explode_no_disconnect"
                raise MockExitIsh("Keyboard / greenlet / etc interruption")
            elif conn.explode == 'interrupt_dont_break':
                conn.explode = None
                raise MockExitIsh("Keyboard / greenlet / etc interruption")
            elif conn.explode in ('execute_no_disconnect',
                                  'explode_no_disconnect'):
                raise MockError(
                    "something broke on execute but we didn't lose the "
                    "connection")
            elif conn.explode in ('rollback', 'rollback_no_disconnect',
                                  'explode_no_disconnect'):
                raise MockError(
                    "something broke on execute but we didn't lose the "
                    "connection")
            elif args and "SELECT" in args[0]:
                cursor.description = [('foo', None, None, None, None, None)]
            else:
                return

        def close():
            cursor.fetchall = cursor.fetchone = \
                Mock(side_effect=MockError("cursor closed"))
        cursor = Mock(
            execute=Mock(side_effect=execute),
            close=Mock(side_effect=close))
        return cursor

    def cursor():
        while True:
            yield mock_cursor()

    def rollback():
        if conn.explode == 'rollback':
            raise MockDisconnect("Lost the DB connection on rollback")
        if conn.explode == 'rollback_no_disconnect':
            raise MockError(
                "something broke on rollback but we didn't lose the "
                "connection")
        else:
            return

    conn = Mock(
        rollback=Mock(side_effect=rollback),
        cursor=Mock(side_effect=cursor()))
    return conn


def MockDBAPI():
    connections = []

    def connect():
        while True:
            conn = mock_connection()
            connections.append(conn)
            yield conn

    def shutdown(explode='execute'):
        for c in connections:
            c.explode = explode

    def dispose():
        for c in connections:
            c.explode = None
        connections[:] = []

    return Mock(
        connect=Mock(side_effect=connect()),
        shutdown=Mock(side_effect=shutdown),
        dispose=Mock(side_effect=dispose),
        paramstyle='named',
        connections=connections,
        Error=MockError)


class MockReconnectTest(fixtures.TestBase):
    def setup(self):
        self.dbapi = MockDBAPI()

        self.db = testing_engine(
            'postgresql://foo:bar@localhost/test',
            options=dict(module=self.dbapi, _initialize=False))

        self.mock_connect = call(
            host='localhost', password='bar', user='foo', database='test')
        # monkeypatch disconnect checker
        self.db.dialect.is_disconnect = \
            lambda e, conn, cursor: isinstance(e, MockDisconnect)

    def teardown(self):
        self.dbapi.dispose()

    def test_reconnect(self):
        """test that an 'is_disconnect' condition will invalidate the
        connection, and additionally dispose the previous connection
        pool and recreate."""

        db_pool = self.db.pool

        # make a connection

        conn = self.db.connect()

        # connection works

        conn.execute(select([1]))

        # create a second connection within the pool, which we'll ensure
        # also goes away

        conn2 = self.db.connect()
        conn2.close()

        # two connections opened total now

        assert len(self.dbapi.connections) == 2

        # set it to fail

        self.dbapi.shutdown()
        assert_raises(
            tsa.exc.DBAPIError,
            conn.execute, select([1])
        )

        # assert was invalidated

        assert not conn.closed
        assert conn.invalidated

        # close shouldn't break

        conn.close()

        # ensure one connection closed...
        eq_(
            [c.close.mock_calls for c in self.dbapi.connections],
            [[call()], []]
        )

        conn = self.db.connect()

        eq_(
            [c.close.mock_calls for c in self.dbapi.connections],
            [[call()], [call()], []]
        )

        conn.execute(select([1]))
        conn.close()

        eq_(
            [c.close.mock_calls for c in self.dbapi.connections],
            [[call()], [call()], []]
        )

    def test_invalidate_trans(self):
        conn = self.db.connect()
        trans = conn.begin()
        self.dbapi.shutdown()

        assert_raises(
            tsa.exc.DBAPIError,
            conn.execute, select([1])
        )

        eq_(
            [c.close.mock_calls for c in self.dbapi.connections],
            [[call()]]
        )
        assert not conn.closed
        assert conn.invalidated
        assert trans.is_active
        assert_raises_message(
            tsa.exc.StatementError,
            "Can't reconnect until invalid transaction is rolled back",
            conn.execute, select([1])
        )
        assert trans.is_active

        assert_raises_message(
            tsa.exc.InvalidRequestError,
            "Can't reconnect until invalid transaction is rolled back",
            trans.commit)

        assert trans.is_active
        trans.rollback()
        assert not trans.is_active
        conn.execute(select([1]))
        assert not conn.invalidated
        eq_(
            [c.close.mock_calls for c in self.dbapi.connections],
            [[call()], []]
        )

    def test_invalidate_dont_call_finalizer(self):
        conn = self.db.connect()
        finalizer = mock.Mock()
        conn.connection._connection_record.\
            finalize_callback.append(finalizer)
        conn.invalidate()
        assert conn.invalidated
        eq_(finalizer.call_count, 0)

    def test_conn_reusable(self):
        conn = self.db.connect()

        conn.execute(select([1]))

        eq_(
            self.dbapi.connect.mock_calls,
            [self.mock_connect]
        )

        self.dbapi.shutdown()

        assert_raises(
            tsa.exc.DBAPIError,
            conn.execute, select([1])
        )

        assert not conn.closed
        assert conn.invalidated

        eq_(
            [c.close.mock_calls for c in self.dbapi.connections],
            [[call()]]
        )

        # test reconnects
        conn.execute(select([1]))
        assert not conn.invalidated

        eq_(
            [c.close.mock_calls for c in self.dbapi.connections],
            [[call()], []]
        )

    def test_invalidated_close(self):
        conn = self.db.connect()

        self.dbapi.shutdown()

        assert_raises(
            tsa.exc.DBAPIError,
            conn.execute, select([1])
        )

        conn.close()
        assert conn.closed
        assert conn.invalidated
        assert_raises_message(
            tsa.exc.StatementError,
            "This Connection is closed",
            conn.execute, select([1])
        )

    def test_noreconnect_execute_plus_closewresult(self):
        conn = self.db.connect(close_with_result=True)

        self.dbapi.shutdown("execute_no_disconnect")

        # raises error
        assert_raises_message(
            tsa.exc.DBAPIError,
            "something broke on execute but we didn't lose the connection",
            conn.execute, select([1])
        )

        assert conn.closed
        assert not conn.invalidated

    def test_noreconnect_rollback_plus_closewresult(self):
        conn = self.db.connect(close_with_result=True)

        self.dbapi.shutdown("rollback_no_disconnect")

        # raises error
        assert_raises_message(
            tsa.exc.DBAPIError,
            "something broke on rollback but we didn't lose the connection",
            conn.execute, select([1])
        )

        assert conn.closed
        assert not conn.invalidated

        assert_raises_message(
            tsa.exc.StatementError,
            "This Connection is closed",
            conn.execute, select([1])
        )

    def test_reconnect_on_reentrant(self):
        conn = self.db.connect()

        conn.execute(select([1]))

        assert len(self.dbapi.connections) == 1

        self.dbapi.shutdown("rollback")

        # raises error
        assert_raises_message(
            tsa.exc.DBAPIError,
            "Lost the DB connection on rollback",
            conn.execute, select([1])
        )

        assert not conn.closed
        assert conn.invalidated

    def test_reconnect_on_reentrant_plus_closewresult(self):
        conn = self.db.connect(close_with_result=True)

        self.dbapi.shutdown("rollback")

        # raises error
        assert_raises_message(
            tsa.exc.DBAPIError,
            "Lost the DB connection on rollback",
            conn.execute, select([1])
        )

        assert conn.closed
        assert conn.invalidated

        assert_raises_message(
            tsa.exc.StatementError,
            "This Connection is closed",
            conn.execute, select([1])
        )

    def test_check_disconnect_no_cursor(self):
        conn = self.db.connect()
        result = conn.execute(select([1]))
        result.cursor.close()
        conn.close()

        assert_raises_message(
            tsa.exc.DBAPIError,
            "cursor closed",
            list, result
        )

    def test_dialect_initialize_once(self):
        from sqlalchemy.engine.url import URL
        from sqlalchemy.engine.default import DefaultDialect
        dbapi = self.dbapi

        mock_dialect = Mock()

        class MyURL(URL):
            def _get_entrypoint(self):
                return Dialect

            def get_dialect(self):
                return Dialect

        class Dialect(DefaultDialect):
            initialize = Mock()

        engine = create_engine(MyURL("foo://"), module=dbapi)
        c1 = engine.connect()
        engine.dispose()
        c2 = engine.connect()
        eq_(Dialect.initialize.call_count, 1)

    def test_invalidate_conn_w_contextmanager_interrupt(self):
        # test [ticket:3803]
        pool = self.db.pool

        conn = self.db.connect()
        self.dbapi.shutdown("interrupt")

        def go():
            with conn.begin():
                conn.execute(select([1]))

        assert_raises(
            MockExitIsh,
            go
        )

        assert conn.invalidated

        eq_(pool._invalidate_time, 0)  # pool not invalidated

        conn.execute(select([1]))
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
                conn.execute(select([1]))

        assert_raises(
            MockExitIsh,
            go
        )

        assert not conn.invalidated

        eq_(pool._invalidate_time, 0)  # pool not invalidated

        conn.execute(select([1]))
        assert not conn.invalidated

    def test_invalidate_conn_w_contextmanager_disconnect(self):
        # test [ticket:3803] change maintains old behavior

        pool = self.db.pool

        conn = self.db.connect()
        self.dbapi.shutdown("execute")

        def go():
            with conn.begin():
                conn.execute(select([1]))

        assert_raises(
            exc.DBAPIError,  # wraps a MockDisconnect
            go
        )

        assert conn.invalidated

        ne_(pool._invalidate_time, 0)  # pool is invalidated

        conn.execute(select([1]))
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
                            execute=Mock(side_effect=DBAPIError("explode"))
                        )
                    else:
                        yield Mock(
                            description=[],
                            close=Mock(side_effect=Exception("explode")),
                        )

            def connect():
                while True:
                    yield Mock(
                        spec=['cursor', 'commit', 'rollback', 'close'],
                        cursor=Mock(side_effect=cursor()),)

            return Mock(
                Error=DBAPIError, paramstyle='qmark',
                connect=Mock(side_effect=connect()))
        dbapi = MockDBAPI()

        from sqlalchemy.engine import default
        url = Mock(
            get_dialect=lambda: default.DefaultDialect,
            _get_entrypoint=lambda: default.DefaultDialect,
            _instantiate_plugins=lambda kwargs: (),
            translate_connect_args=lambda: {}, query={},)
        eng = testing_engine(
            url, options=dict(module=dbapi, _initialize=initialize))
        eng.pool.logger = Mock()
        return eng

    def test_cursor_explode(self):
        db = self._fixture(False, False)
        conn = db.connect()
        result = conn.execute("select foo")
        result.close()
        conn.close()
        eq_(
            db.pool.logger.error.mock_calls,
            [call('Error closing cursor', exc_info=True)]
        )

    def test_cursor_shutdown_in_initialize(self):
        db = self._fixture(True, True)
        assert_raises_message(
            exc.SAWarning,
            "Exception attempting to detect",
            db.connect
        )
        eq_(
            db.pool.logger.error.mock_calls,
            [call('Error closing cursor', exc_info=True)]
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
    __requires__ = 'graceful_disconnects',

    def setup(self):
        self.engine = engines.reconnecting_engine()

    def teardown(self):
        self.engine.dispose()

    def test_reconnect(self):
        conn = self.engine.connect()

        eq_(conn.execute(select([1])).scalar(), 1)
        assert not conn.closed

        self.engine.test_shutdown()

        _assert_invalidated(conn.execute, select([1]))

        assert not conn.closed
        assert conn.invalidated

        assert conn.invalidated
        eq_(conn.execute(select([1])).scalar(), 1)
        assert not conn.invalidated

        # one more time
        self.engine.test_shutdown()
        _assert_invalidated(conn.execute, select([1]))

        assert conn.invalidated
        eq_(conn.execute(select([1])).scalar(), 1)
        assert not conn.invalidated

        conn.close()

    def test_multiple_invalidate(self):
        c1 = self.engine.connect()
        c2 = self.engine.connect()

        eq_(c1.execute(select([1])).scalar(), 1)

        p1 = self.engine.pool
        self.engine.test_shutdown()

        _assert_invalidated(c1.execute, select([1]))

        p2 = self.engine.pool

        _assert_invalidated(c2.execute, select([1]))

        # pool isn't replaced
        assert self.engine.pool is p2

    def test_branched_invalidate_branch_to_parent(self):
        c1 = self.engine.connect()

        with patch.object(self.engine.pool, "logger") as logger:
            c1_branch = c1.connect()
            eq_(c1_branch.execute(select([1])).scalar(), 1)

            self.engine.test_shutdown()

            _assert_invalidated(c1_branch.execute, select([1]))
            assert c1.invalidated
            assert c1_branch.invalidated

            c1_branch._revalidate_connection()
            assert not c1.invalidated
            assert not c1_branch.invalidated

        assert "Invalidate connection" in logger.mock_calls[0][1][0]

    def test_branched_invalidate_parent_to_branch(self):
        c1 = self.engine.connect()

        c1_branch = c1.connect()
        eq_(c1_branch.execute(select([1])).scalar(), 1)

        self.engine.test_shutdown()

        _assert_invalidated(c1.execute, select([1]))
        assert c1.invalidated
        assert c1_branch.invalidated

        c1._revalidate_connection()
        assert not c1.invalidated
        assert not c1_branch.invalidated

    def test_branch_invalidate_state(self):
        c1 = self.engine.connect()

        c1_branch = c1.connect()

        eq_(c1_branch.execute(select([1])).scalar(), 1)

        self.engine.test_shutdown()

        _assert_invalidated(c1_branch.execute, select([1]))
        assert not c1_branch.closed
        assert not c1_branch._connection_is_valid

    def test_ensure_is_disconnect_gets_connection(self):
        def is_disconnect(e, conn, cursor):
            # connection is still present
            assert conn.connection is not None
            # the error usually occurs on connection.cursor(),
            # though MySQLdb we get a non-working cursor.
            # assert cursor is None

        self.engine.dialect.is_disconnect = is_disconnect
        conn = self.engine.connect()
        self.engine.test_shutdown()
        assert_raises(
            tsa.exc.DBAPIError,
            conn.execute, select([1])
        )

    def test_rollback_on_invalid_plain(self):
        conn = self.engine.connect()
        trans = conn.begin()
        conn.invalidate()
        trans.rollback()

    @testing.requires.two_phase_transactions
    def test_rollback_on_invalid_twophase(self):
        conn = self.engine.connect()
        trans = conn.begin_twophase()
        conn.invalidate()
        trans.rollback()

    @testing.requires.savepoints
    def test_rollback_on_invalid_savepoint(self):
        conn = self.engine.connect()
        trans = conn.begin()
        trans2 = conn.begin_nested()
        conn.invalidate()
        trans2.rollback()

    def test_invalidate_twice(self):
        conn = self.engine.connect()
        conn.invalidate()
        conn.invalidate()

    @testing.skip_if(
        [lambda: util.py3k, "oracle+cx_oracle"],
        "Crashes on py3k+cx_oracle")
    def test_explode_in_initializer(self):
        engine = engines.testing_engine()

        def broken_initialize(connection):
            connection.execute("select fake_stuff from _fake_table")

        engine.dialect.initialize = broken_initialize

        # raises a DBAPIError, not an AttributeError
        assert_raises(exc.DBAPIError, engine.connect)

    @testing.skip_if(
        [lambda: util.py3k, "oracle+cx_oracle"],
        "Crashes on py3k+cx_oracle")
    def test_explode_in_initializer_disconnect(self):
        engine = engines.testing_engine()

        def broken_initialize(connection):
            connection.execute("select fake_stuff from _fake_table")

        engine.dialect.initialize = broken_initialize

        p1 = engine.pool

        def is_disconnect(e, conn, cursor):
            return True

        engine.dialect.is_disconnect = is_disconnect

        # invalidate() also doesn't screw up
        assert_raises(exc.DBAPIError, engine.connect)

    def test_null_pool(self):
        engine = \
            engines.reconnecting_engine(options=dict(poolclass=pool.NullPool))
        conn = engine.connect()
        eq_(conn.execute(select([1])).scalar(), 1)
        assert not conn.closed
        engine.test_shutdown()
        _assert_invalidated(conn.execute, select([1]))
        assert not conn.closed
        assert conn.invalidated
        eq_(conn.execute(select([1])).scalar(), 1)
        assert not conn.invalidated

    def test_close(self):
        conn = self.engine.connect()
        eq_(conn.execute(select([1])).scalar(), 1)
        assert not conn.closed

        self.engine.test_shutdown()

        _assert_invalidated(conn.execute, select([1]))

        conn.close()
        conn = self.engine.connect()
        eq_(conn.execute(select([1])).scalar(), 1)

    def test_with_transaction(self):
        conn = self.engine.connect()
        trans = conn.begin()
        eq_(conn.execute(select([1])).scalar(), 1)
        assert not conn.closed
        self.engine.test_shutdown()
        _assert_invalidated(conn.execute, select([1]))
        assert not conn.closed
        assert conn.invalidated
        assert trans.is_active
        assert_raises_message(
            tsa.exc.StatementError,
            "Can't reconnect until invalid transaction is rolled back",
            conn.execute, select([1]))
        assert trans.is_active
        assert_raises_message(
            tsa.exc.InvalidRequestError,
            "Can't reconnect until invalid transaction is rolled back",
            trans.commit
        )
        assert trans.is_active
        trans.rollback()
        assert not trans.is_active
        assert conn.invalidated
        eq_(conn.execute(select([1])).scalar(), 1)
        assert not conn.invalidated


class RecycleTest(fixtures.TestBase):
    __backend__ = True

    def test_basic(self):
        for threadlocal in False, True:
            engine = engines.reconnecting_engine(
                options={'pool_threadlocal': threadlocal})

            conn = engine.contextual_connect()
            eq_(conn.execute(select([1])).scalar(), 1)
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
            conn = engine.contextual_connect()
            eq_(conn.execute(select([1])).scalar(), 1)
            conn.close()


class InvalidateDuringResultTest(fixtures.TestBase):
    __backend__ = True

    def setup(self):
        self.engine = engines.reconnecting_engine()
        self.meta = MetaData(self.engine)
        table = Table(
            'sometable', self.meta,
            Column('id', Integer, primary_key=True),
            Column('name', String(50)))
        self.meta.create_all()
        table.insert().execute(
            [{'id': i, 'name': 'row %d' % i} for i in range(1, 100)]
        )

    def teardown(self):
        self.meta.drop_all()
        self.engine.dispose()

    @testing.fails_if([
        '+mysqlconnector', '+mysqldb', '+cymysql', '+pymysql', '+pg8000'],
        "Buffers the result set and doesn't check for connection close")
    def test_invalidate_on_results(self):
        conn = self.engine.connect()
        result = conn.execute('select * from sometable')
        for x in range(20):
            result.fetchone()
        self.engine.test_shutdown()
        _assert_invalidated(result.fetchone)
        assert conn.invalidated
