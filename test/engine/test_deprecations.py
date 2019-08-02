import re

import sqlalchemy as tsa
from sqlalchemy import column
from sqlalchemy import create_engine
from sqlalchemy import event
from sqlalchemy import ForeignKey
from sqlalchemy import func
from sqlalchemy import Integer
from sqlalchemy import literal
from sqlalchemy import MetaData
from sqlalchemy import pool
from sqlalchemy import select
from sqlalchemy import String
from sqlalchemy import testing
from sqlalchemy import text
from sqlalchemy import TypeDecorator
from sqlalchemy.engine.base import Engine
from sqlalchemy.engine.mock import MockConnection
from sqlalchemy.interfaces import ConnectionProxy
from sqlalchemy.testing import assert_raises
from sqlalchemy.testing import assert_raises_message
from sqlalchemy.testing import engines
from sqlalchemy.testing import eq_
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import is_false
from sqlalchemy.testing import is_true
from sqlalchemy.testing.mock import Mock
from sqlalchemy.testing.schema import Column
from sqlalchemy.testing.schema import Table
from sqlalchemy.testing.util import lazy_gc


class SomeException(Exception):
    pass


class CreateEngineTest(fixtures.TestBase):
    def test_strategy_keyword_mock(self):
        def executor(x, y):
            pass

        with testing.expect_deprecated(
            "The create_engine.strategy keyword is deprecated, and the "
            "only argument accepted is 'mock'"
        ):
            e = create_engine(
                "postgresql://", strategy="mock", executor=executor
            )

        assert isinstance(e, MockConnection)

    def test_strategy_keyword_unknown(self):
        with testing.expect_deprecated(
            "The create_engine.strategy keyword is deprecated, and the "
            "only argument accepted is 'mock'"
        ):
            assert_raises_message(
                tsa.exc.ArgumentError,
                "unknown strategy: 'threadlocal'",
                create_engine,
                "postgresql://",
                strategy="threadlocal",
            )


def _proxy_execute_deprecated():
    return (
        testing.expect_deprecated("ConnectionProxy.execute is deprecated."),
        testing.expect_deprecated(
            "ConnectionProxy.cursor_execute is deprecated."
        ),
    )


class TransactionTest(fixtures.TestBase):
    __backend__ = True

    @classmethod
    def setup_class(cls):
        metadata = MetaData()
        cls.users = Table(
            "query_users",
            metadata,
            Column("user_id", Integer, primary_key=True),
            Column("user_name", String(20)),
            test_needs_acid=True,
        )
        cls.users.create(testing.db)

    def teardown(self):
        testing.db.execute(self.users.delete()).close()

    @classmethod
    def teardown_class(cls):
        cls.users.drop(testing.db)

    def test_transaction_container(self):
        users = self.users

        def go(conn, table, data):
            for d in data:
                conn.execute(table.insert(), d)

        with testing.expect_deprecated(
            r"The Engine.transaction\(\) method is deprecated"
        ):
            testing.db.transaction(
                go, users, [dict(user_id=1, user_name="user1")]
            )

        with testing.db.connect() as conn:
            eq_(conn.execute(users.select()).fetchall(), [(1, "user1")])
        with testing.expect_deprecated(
            r"The Engine.transaction\(\) method is deprecated"
        ):
            assert_raises(
                tsa.exc.DBAPIError,
                testing.db.transaction,
                go,
                users,
                [
                    {"user_id": 2, "user_name": "user2"},
                    {"user_id": 1, "user_name": "user3"},
                ],
            )
        with testing.db.connect() as conn:
            eq_(conn.execute(users.select()).fetchall(), [(1, "user1")])


class ProxyConnectionTest(fixtures.TestBase):

    """These are the same tests as EngineEventsTest, except using
    the deprecated ConnectionProxy interface.

    """

    __requires__ = ("ad_hoc_engines",)
    __prefer_requires__ = ("two_phase_transactions",)

    @testing.uses_deprecated(r".*Use event.listen")
    @testing.fails_on("firebird", "Data type unknown")
    def test_proxy(self):

        stmts = []
        cursor_stmts = []

        class MyProxy(ConnectionProxy):
            def execute(
                self, conn, execute, clauseelement, *multiparams, **params
            ):
                stmts.append((str(clauseelement), params, multiparams))
                return execute(clauseelement, *multiparams, **params)

            def cursor_execute(
                self,
                execute,
                cursor,
                statement,
                parameters,
                context,
                executemany,
            ):
                cursor_stmts.append((str(statement), parameters, None))
                return execute(cursor, statement, parameters, context)

        def assert_stmts(expected, received):
            for stmt, params, posn in expected:
                if not received:
                    assert False, "Nothing available for stmt: %s" % stmt
                while received:
                    teststmt, testparams, testmultiparams = received.pop(0)
                    teststmt = (
                        re.compile(r"[\n\t ]+", re.M)
                        .sub(" ", teststmt)
                        .strip()
                    )
                    if teststmt.startswith(stmt) and (
                        testparams == params or testparams == posn
                    ):
                        break

        with testing.expect_deprecated(
            "ConnectionProxy.execute is deprecated.",
            "ConnectionProxy.cursor_execute is deprecated.",
        ):
            plain_engine = engines.testing_engine(
                options=dict(implicit_returning=False, proxy=MyProxy())
            )

        for engine in (plain_engine,):
            m = MetaData(engine)
            t1 = Table(
                "t1",
                m,
                Column("c1", Integer, primary_key=True),
                Column(
                    "c2",
                    String(50),
                    default=func.lower("Foo"),
                    primary_key=True,
                ),
            )
            m.create_all()
            try:
                t1.insert().execute(c1=5, c2="some data")
                t1.insert().execute(c1=6)
                eq_(
                    engine.execute("select * from t1").fetchall(),
                    [(5, "some data"), (6, "foo")],
                )
            finally:
                m.drop_all()
            engine.dispose()
            compiled = [
                ("CREATE TABLE t1", {}, None),
                (
                    "INSERT INTO t1 (c1, c2)",
                    {"c2": "some data", "c1": 5},
                    None,
                ),
                ("INSERT INTO t1 (c1, c2)", {"c1": 6}, None),
                ("select * from t1", {}, None),
                ("DROP TABLE t1", {}, None),
            ]

            cursor = [
                ("CREATE TABLE t1", {}, ()),
                (
                    "INSERT INTO t1 (c1, c2)",
                    {"c2": "some data", "c1": 5},
                    (5, "some data"),
                ),
                ("SELECT lower", {"lower_1": "Foo"}, ("Foo",)),
                (
                    "INSERT INTO t1 (c1, c2)",
                    {"c2": "foo", "c1": 6},
                    (6, "foo"),
                ),
                ("select * from t1", {}, ()),
                ("DROP TABLE t1", {}, ()),
            ]

            assert_stmts(compiled, stmts)
            assert_stmts(cursor, cursor_stmts)

    @testing.uses_deprecated(r".*Use event.listen")
    def test_options(self):
        canary = []

        class TrackProxy(ConnectionProxy):
            def __getattribute__(self, key):
                fn = object.__getattribute__(self, key)

                def go(*arg, **kw):
                    canary.append(fn.__name__)
                    return fn(*arg, **kw)

                return go

        with testing.expect_deprecated(
            *[
                "ConnectionProxy.%s is deprecated" % name
                for name in [
                    "execute",
                    "cursor_execute",
                    "begin",
                    "rollback",
                    "commit",
                    "savepoint",
                    "rollback_savepoint",
                    "release_savepoint",
                    "begin_twophase",
                    "prepare_twophase",
                    "rollback_twophase",
                    "commit_twophase",
                ]
            ]
        ):
            engine = engines.testing_engine(options={"proxy": TrackProxy()})
        conn = engine.connect()
        c2 = conn.execution_options(foo="bar")
        eq_(c2._execution_options, {"foo": "bar"})
        c2.execute(select([1]))
        c3 = c2.execution_options(bar="bat")
        eq_(c3._execution_options, {"foo": "bar", "bar": "bat"})
        eq_(canary, ["execute", "cursor_execute"])

    @testing.uses_deprecated(r".*Use event.listen")
    def test_transactional(self):
        canary = []

        class TrackProxy(ConnectionProxy):
            def __getattribute__(self, key):
                fn = object.__getattribute__(self, key)

                def go(*arg, **kw):
                    canary.append(fn.__name__)
                    return fn(*arg, **kw)

                return go

        with testing.expect_deprecated(
            *[
                "ConnectionProxy.%s is deprecated" % name
                for name in [
                    "execute",
                    "cursor_execute",
                    "begin",
                    "rollback",
                    "commit",
                    "savepoint",
                    "rollback_savepoint",
                    "release_savepoint",
                    "begin_twophase",
                    "prepare_twophase",
                    "rollback_twophase",
                    "commit_twophase",
                ]
            ]
        ):
            engine = engines.testing_engine(options={"proxy": TrackProxy()})
        conn = engine.connect()
        trans = conn.begin()
        conn.execute(select([1]))
        trans.rollback()
        trans = conn.begin()
        conn.execute(select([1]))
        trans.commit()

        eq_(
            canary,
            [
                "begin",
                "execute",
                "cursor_execute",
                "rollback",
                "begin",
                "execute",
                "cursor_execute",
                "commit",
            ],
        )

    @testing.uses_deprecated(r".*Use event.listen")
    @testing.requires.savepoints
    @testing.requires.two_phase_transactions
    def test_transactional_advanced(self):
        canary = []

        class TrackProxy(ConnectionProxy):
            def __getattribute__(self, key):
                fn = object.__getattribute__(self, key)

                def go(*arg, **kw):
                    canary.append(fn.__name__)
                    return fn(*arg, **kw)

                return go

        with testing.expect_deprecated(
            *[
                "ConnectionProxy.%s is deprecated" % name
                for name in [
                    "execute",
                    "cursor_execute",
                    "begin",
                    "rollback",
                    "commit",
                    "savepoint",
                    "rollback_savepoint",
                    "release_savepoint",
                    "begin_twophase",
                    "prepare_twophase",
                    "rollback_twophase",
                    "commit_twophase",
                ]
            ]
        ):
            engine = engines.testing_engine(options={"proxy": TrackProxy()})
        conn = engine.connect()

        trans = conn.begin()
        trans2 = conn.begin_nested()
        conn.execute(select([1]))
        trans2.rollback()
        trans2 = conn.begin_nested()
        conn.execute(select([1]))
        trans2.commit()
        trans.rollback()

        trans = conn.begin_twophase()
        conn.execute(select([1]))
        trans.prepare()
        trans.commit()

        canary = [t for t in canary if t not in ("cursor_execute", "execute")]
        eq_(
            canary,
            [
                "begin",
                "savepoint",
                "rollback_savepoint",
                "savepoint",
                "release_savepoint",
                "rollback",
                "begin_twophase",
                "prepare_twophase",
                "commit_twophase",
            ],
        )


class HandleInvalidatedOnConnectTest(fixtures.TestBase):
    __requires__ = ("sqlite",)

    def setUp(self):
        e = create_engine("sqlite://")

        connection = Mock(get_server_version_info=Mock(return_value="5.0"))

        def connect(*args, **kwargs):
            return connection

        dbapi = Mock(
            sqlite_version_info=(99, 9, 9),
            version_info=(99, 9, 9),
            sqlite_version="99.9.9",
            paramstyle="named",
            connect=Mock(side_effect=connect),
        )

        sqlite3 = e.dialect.dbapi
        dbapi.Error = (sqlite3.Error,)
        dbapi.ProgrammingError = sqlite3.ProgrammingError

        self.dbapi = dbapi
        self.ProgrammingError = sqlite3.ProgrammingError


class HandleErrorTest(fixtures.TestBase):
    __requires__ = ("ad_hoc_engines",)
    __backend__ = True

    def tearDown(self):
        Engine.dispatch._clear()
        Engine._has_events = False

    def test_legacy_dbapi_error(self):
        engine = engines.testing_engine()
        canary = Mock()

        with testing.expect_deprecated(
            r"The ConnectionEvents.dbapi_error\(\) event is deprecated"
        ):
            event.listen(engine, "dbapi_error", canary)

        with engine.connect() as conn:
            try:
                conn.execute("SELECT FOO FROM I_DONT_EXIST")
                assert False
            except tsa.exc.DBAPIError as e:
                eq_(canary.mock_calls[0][1][5], e.orig)
                eq_(canary.mock_calls[0][1][2], "SELECT FOO FROM I_DONT_EXIST")

    def test_legacy_dbapi_error_no_ad_hoc_context(self):
        engine = engines.testing_engine()

        listener = Mock(return_value=None)
        with testing.expect_deprecated(
            r"The ConnectionEvents.dbapi_error\(\) event is deprecated"
        ):
            event.listen(engine, "dbapi_error", listener)

        nope = SomeException("nope")

        class MyType(TypeDecorator):
            impl = Integer

            def process_bind_param(self, value, dialect):
                raise nope

        with engine.connect() as conn:
            assert_raises_message(
                tsa.exc.StatementError,
                r"\(.*SomeException\) " r"nope\n\[SQL\: u?SELECT 1 ",
                conn.execute,
                select([1]).where(column("foo") == literal("bar", MyType())),
            )
        # no legacy event
        eq_(listener.mock_calls, [])

    def test_legacy_dbapi_error_non_dbapi_error(self):
        engine = engines.testing_engine()

        listener = Mock(return_value=None)
        with testing.expect_deprecated(
            r"The ConnectionEvents.dbapi_error\(\) event is deprecated"
        ):
            event.listen(engine, "dbapi_error", listener)

        nope = TypeError("I'm not a DBAPI error")
        with engine.connect() as c:
            c.connection.cursor = Mock(
                return_value=Mock(execute=Mock(side_effect=nope))
            )

            assert_raises_message(
                TypeError, "I'm not a DBAPI error", c.execute, "select "
            )
        # no legacy event
        eq_(listener.mock_calls, [])


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
    def setup(self):
        pool.clear_managers()
        self._teardown_conns = []

    def teardown(self):
        for ref in self._teardown_conns:
            conn = ref()
            if conn:
                conn.close()

    @classmethod
    def teardown_class(cls):
        pool.clear_managers()

    def _queuepool_fixture(self, **kw):
        dbapi, pool = self._queuepool_dbapi_fixture(**kw)
        return pool

    def _queuepool_dbapi_fixture(self, **kw):
        dbapi = MockDBAPI()
        return (
            dbapi,
            pool.QueuePool(creator=lambda: dbapi.connect("foo.db"), **kw),
        )


class DeprecatedPoolListenerTest(PoolTestBase):
    @testing.requires.predictable_gc
    @testing.uses_deprecated(
        r".*Use the PoolEvents", r".*'listeners' argument .* is deprecated"
    )
    def test_listeners(self):
        class InstrumentingListener(object):
            def __init__(self):
                if hasattr(self, "connect"):
                    self.connect = self.inst_connect
                if hasattr(self, "first_connect"):
                    self.first_connect = self.inst_first_connect
                if hasattr(self, "checkout"):
                    self.checkout = self.inst_checkout
                if hasattr(self, "checkin"):
                    self.checkin = self.inst_checkin
                self.clear()

            def clear(self):
                self.connected = []
                self.first_connected = []
                self.checked_out = []
                self.checked_in = []

            def assert_total(self, conn, fconn, cout, cin):
                eq_(len(self.connected), conn)
                eq_(len(self.first_connected), fconn)
                eq_(len(self.checked_out), cout)
                eq_(len(self.checked_in), cin)

            def assert_in(self, item, in_conn, in_fconn, in_cout, in_cin):
                eq_((item in self.connected), in_conn)
                eq_((item in self.first_connected), in_fconn)
                eq_((item in self.checked_out), in_cout)
                eq_((item in self.checked_in), in_cin)

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

        with testing.expect_deprecated(
            *[
                "PoolListener.%s is deprecated." % name
                for name in ["connect", "first_connect", "checkout", "checkin"]
            ]
        ):
            p.add_listener(ListenAll())
        assert_listeners(p, 1, 1, 1, 1, 1)

        with testing.expect_deprecated(
            *["PoolListener.%s is deprecated." % name for name in ["connect"]]
        ):
            p.add_listener(ListenConnect())
        assert_listeners(p, 2, 2, 1, 1, 1)

        with testing.expect_deprecated(
            *[
                "PoolListener.%s is deprecated." % name
                for name in ["first_connect"]
            ]
        ):
            p.add_listener(ListenFirstConnect())
        assert_listeners(p, 3, 2, 2, 1, 1)

        with testing.expect_deprecated(
            *["PoolListener.%s is deprecated." % name for name in ["checkout"]]
        ):
            p.add_listener(ListenCheckOut())
        assert_listeners(p, 4, 2, 2, 2, 1)

        with testing.expect_deprecated(
            *["PoolListener.%s is deprecated." % name for name in ["checkin"]]
        ):
            p.add_listener(ListenCheckIn())
        assert_listeners(p, 5, 2, 2, 2, 2)
        del p

        snoop = ListenAll()

        with testing.expect_deprecated(
            *[
                "PoolListener.%s is deprecated." % name
                for name in ["connect", "first_connect", "checkout", "checkin"]
            ]
            + [
                "PoolListener is deprecated in favor of the PoolEvents "
                "listener interface.  The Pool.listeners parameter "
                "will be removed"
            ]
        ):
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

    @testing.uses_deprecated(r".*Use the PoolEvents")
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

            with testing.expect_deprecated(
                *[
                    "PoolListener.%s is deprecated." % name
                    for name in ["connect", "checkout", "checkin"]
                ]
            ):
                p.add_listener(i_all)
            assert_listeners(p, 1, 1, 1, 1)

            with testing.expect_deprecated(
                *[
                    "PoolListener.%s is deprecated." % name
                    for name in ["connect"]
                ]
            ):
                p.add_listener(i_connect)
            assert_listeners(p, 2, 1, 1, 1)

            with testing.expect_deprecated(
                *[
                    "PoolListener.%s is deprecated." % name
                    for name in ["checkout"]
                ]
            ):
                p.add_listener(i_checkout)
            assert_listeners(p, 3, 1, 1, 1)

            with testing.expect_deprecated(
                *[
                    "PoolListener.%s is deprecated." % name
                    for name in ["checkin"]
                ]
            ):
                p.add_listener(i_checkin)
            assert_listeners(p, 4, 1, 1, 1)
            del p

            with testing.expect_deprecated(
                *[
                    "PoolListener.%s is deprecated." % name
                    for name in ["connect", "checkout", "checkin"]
                ]
                + [".*The Pool.listeners parameter will be removed"]
            ):
                p = self._queuepool_fixture(listeners=[i_all])
            assert_listeners(p, 1, 1, 1, 1)

            c = p.connect()
            assert counts == [1, 1, 0]
            c.close()
            assert counts == [1, 1, 1]

            c = p.connect()
            assert counts == [1, 2, 1]
            with testing.expect_deprecated(
                *[
                    "PoolListener.%s is deprecated." % name
                    for name in ["checkin"]
                ]
            ):
                p.add_listener(i_checkin)
            c.close()
            assert counts == [1, 2, 2]


class ExplicitAutoCommitDeprecatedTest(fixtures.TestBase):

    """test the 'autocommit' flag on select() and text() objects.

    Requires PostgreSQL so that we may define a custom function which
    modifies the database. """

    __only_on__ = "postgresql"

    @classmethod
    def setup_class(cls):
        global metadata, foo
        metadata = MetaData(testing.db)
        foo = Table(
            "foo",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("data", String(100)),
        )
        metadata.create_all()
        testing.db.execute(
            "create function insert_foo(varchar) "
            "returns integer as 'insert into foo(data) "
            "values ($1);select 1;' language sql"
        )

    def teardown(self):
        foo.delete().execute().close()

    @classmethod
    def teardown_class(cls):
        testing.db.execute("drop function insert_foo(varchar)")
        metadata.drop_all()

    def test_explicit_compiled(self):
        conn1 = testing.db.connect()
        conn2 = testing.db.connect()
        with testing.expect_deprecated(
            "The select.autocommit parameter is deprecated"
        ):
            conn1.execute(select([func.insert_foo("data1")], autocommit=True))
        assert conn2.execute(select([foo.c.data])).fetchall() == [("data1",)]
        with testing.expect_deprecated(
            r"The SelectBase.autocommit\(\) method is deprecated,"
        ):
            conn1.execute(select([func.insert_foo("data2")]).autocommit())
        assert conn2.execute(select([foo.c.data])).fetchall() == [
            ("data1",),
            ("data2",),
        ]
        conn1.close()
        conn2.close()

    def test_explicit_text(self):
        conn1 = testing.db.connect()
        conn2 = testing.db.connect()
        with testing.expect_deprecated(
            "The text.autocommit parameter is deprecated"
        ):
            conn1.execute(
                text("select insert_foo('moredata')", autocommit=True)
            )
        assert conn2.execute(select([foo.c.data])).fetchall() == [
            ("moredata",)
        ]
        conn1.close()
        conn2.close()


class DeprecatedEngineFeatureTest(fixtures.TablesTest):
    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        cls.table = Table(
            "exec_test",
            metadata,
            Column("a", Integer),
            Column("b", Integer),
            test_needs_acid=True,
        )

    def _trans_fn(self, is_transaction=False):
        def go(conn, x, value=None):
            if is_transaction:
                conn = conn.connection
            conn.execute(self.table.insert().values(a=x, b=value))

        return go

    def _trans_rollback_fn(self, is_transaction=False):
        def go(conn, x, value=None):
            if is_transaction:
                conn = conn.connection
            conn.execute(self.table.insert().values(a=x, b=value))
            raise SomeException("breakage")

        return go

    def _assert_no_data(self):
        eq_(
            testing.db.scalar(
                select([func.count("*")]).select_from(self.table)
            ),
            0,
        )

    def _assert_fn(self, x, value=None):
        eq_(testing.db.execute(self.table.select()).fetchall(), [(x, value)])

    def test_transaction_engine_fn_commit(self):
        fn = self._trans_fn()
        with testing.expect_deprecated(r"The Engine.transaction\(\) method"):
            testing.db.transaction(fn, 5, value=8)
        self._assert_fn(5, value=8)

    def test_transaction_engine_fn_rollback(self):
        fn = self._trans_rollback_fn()
        with testing.expect_deprecated(
            r"The Engine.transaction\(\) method is deprecated"
        ):
            assert_raises_message(
                Exception, "breakage", testing.db.transaction, fn, 5, value=8
            )
        self._assert_no_data()

    def test_transaction_connection_fn_commit(self):
        fn = self._trans_fn()
        with testing.db.connect() as conn:
            with testing.expect_deprecated(
                r"The Connection.transaction\(\) method is deprecated"
            ):
                conn.transaction(fn, 5, value=8)
            self._assert_fn(5, value=8)

    def test_transaction_connection_fn_rollback(self):
        fn = self._trans_rollback_fn()
        with testing.db.connect() as conn:
            with testing.expect_deprecated(r""):
                assert_raises(Exception, conn.transaction, fn, 5, value=8)
        self._assert_no_data()


class DeprecatedReflectionTest(fixtures.TablesTest):
    @classmethod
    def define_tables(cls, metadata):
        Table(
            "user",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("name", String(50)),
        )
        Table(
            "address",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("user_id", ForeignKey("user.id")),
            Column("email", String(50)),
        )

    def test_exists(self):
        dont_exist = Table("dont_exist", MetaData())
        with testing.expect_deprecated(
            r"The Table.exists\(\) method is deprecated"
        ):
            is_false(dont_exist.exists(testing.db))

        user = self.tables.user
        with testing.expect_deprecated(
            r"The Table.exists\(\) method is deprecated"
        ):
            is_true(user.exists(testing.db))

    def test_create_drop_explicit(self):
        metadata = MetaData()
        table = Table("test_table", metadata, Column("foo", Integer))
        for bind in (testing.db, testing.db.connect()):
            for args in [([], {"bind": bind}), ([bind], {})]:
                metadata.create_all(*args[0], **args[1])
                with testing.expect_deprecated(
                    r"The Table.exists\(\) method is deprecated"
                ):
                    assert table.exists(*args[0], **args[1])
                metadata.drop_all(*args[0], **args[1])
                table.create(*args[0], **args[1])
                table.drop(*args[0], **args[1])
                with testing.expect_deprecated(
                    r"The Table.exists\(\) method is deprecated"
                ):
                    assert not table.exists(*args[0], **args[1])

    def test_create_drop_err_table(self):
        metadata = MetaData()
        table = Table("test_table", metadata, Column("foo", Integer))

        with testing.expect_deprecated(
            r"The Table.exists\(\) method is deprecated"
        ):
            assert_raises_message(
                tsa.exc.UnboundExecutionError,
                (
                    "Table object 'test_table' is not bound to an Engine or "
                    "Connection."
                ),
                table.exists,
            )

    def test_engine_has_table(self):
        with testing.expect_deprecated(
            r"The Engine.has_table\(\) method is deprecated"
        ):
            is_false(testing.db.has_table("dont_exist"))

        with testing.expect_deprecated(
            r"The Engine.has_table\(\) method is deprecated"
        ):
            is_true(testing.db.has_table("user"))

    def test_engine_table_names(self):
        metadata = self.metadata

        with testing.expect_deprecated(
            r"The Engine.table_names\(\) method is deprecated"
        ):
            table_names = testing.db.table_names()
        is_true(set(table_names).issuperset(metadata.tables))
