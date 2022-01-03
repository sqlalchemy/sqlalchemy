import re
from unittest.mock import Mock

import sqlalchemy as tsa
from sqlalchemy import create_engine
from sqlalchemy import event
from sqlalchemy import exc
from sqlalchemy import insert
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import pool
from sqlalchemy import select
from sqlalchemy import String
from sqlalchemy import testing
from sqlalchemy.engine import BindTyping
from sqlalchemy.engine import reflection
from sqlalchemy.engine.base import Connection
from sqlalchemy.engine.base import Engine
from sqlalchemy.engine.default import DefaultDialect
from sqlalchemy.engine.mock import MockConnection
from sqlalchemy.testing import assert_raises
from sqlalchemy.testing import assert_raises_message
from sqlalchemy.testing import assertions
from sqlalchemy.testing import config
from sqlalchemy.testing import engines
from sqlalchemy.testing import eq_
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import is_
from sqlalchemy.testing import is_instance_of
from sqlalchemy.testing import mock
from sqlalchemy.testing.assertions import expect_deprecated
from sqlalchemy.testing.assertions import expect_raises_message
from sqlalchemy.testing.engines import testing_engine
from sqlalchemy.testing.schema import Column
from sqlalchemy.testing.schema import Table


def _string_deprecation_expect():
    return testing.expect_deprecated_20(
        r"Passing a string to Connection.execute\(\) is deprecated "
        r"and will be removed in version 2.0"
    )


class SomeException(Exception):
    pass


class ConnectionlessDeprecationTest(fixtures.TestBase):
    """test various things associated with "connectionless" executions."""

    def check_usage(self, inspector):
        with inspector._operation_context() as conn:
            is_instance_of(conn, Connection)

    def test_inspector_constructor_engine(self):
        with testing.expect_deprecated(
            r"The __init__\(\) method on Inspector is deprecated and will "
            r"be removed in a future release."
        ):
            i1 = reflection.Inspector(testing.db)

        is_(i1.bind, testing.db)
        self.check_usage(i1)

    def test_inspector_constructor_connection(self):
        with testing.db.connect() as conn:
            with testing.expect_deprecated(
                r"The __init__\(\) method on Inspector is deprecated and "
                r"will be removed in a future release."
            ):
                i1 = reflection.Inspector(conn)

            is_(i1.bind, conn)
            is_(i1.engine, testing.db)
            self.check_usage(i1)

    def test_inspector_from_engine(self):
        with testing.expect_deprecated(
            r"The from_engine\(\) method on Inspector is deprecated and will "
            r"be removed in a future release."
        ):
            i1 = reflection.Inspector.from_engine(testing.db)

        is_(i1.bind, testing.db)
        self.check_usage(i1)


class CreateEngineTest(fixtures.TestBase):
    def test_strategy_keyword_mock(self):
        def executor(x, y):
            pass

        with testing.expect_deprecated(
            "The create_engine.strategy keyword is deprecated, and the "
            "only argument accepted is 'mock'"
        ):
            e = create_engine(
                "postgresql+psycopg2://", strategy="mock", executor=executor
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
                "postgresql+psycopg2://",
                strategy="threadlocal",
            )

    def test_empty_in_keyword(self):
        with testing.expect_deprecated(
            "The create_engine.empty_in_strategy keyword is deprecated, "
            "and no longer has any effect."
        ):
            create_engine(
                "postgresql+psycopg2://",
                empty_in_strategy="static",
                module=Mock(),
                _initialize=False,
            )

    def test_dialect_use_setinputsizes_attr(self):
        class MyDialect(DefaultDialect):
            use_setinputsizes = True

        with testing.expect_deprecated(
            "The dialect-level use_setinputsizes attribute is deprecated."
        ):
            md = MyDialect()
        is_(md.bind_typing, BindTyping.SETINPUTSIZES)


class HandleInvalidatedOnConnectTest(fixtures.TestBase):
    __requires__ = ("sqlite",)

    def setup_test(self):
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

    def _queuepool_fixture(self, **kw):
        dbapi, pool = self._queuepool_dbapi_fixture(**kw)
        return pool

    def _queuepool_dbapi_fixture(self, **kw):
        dbapi = MockDBAPI()
        return (
            dbapi,
            pool.QueuePool(creator=lambda: dbapi.connect("foo.db"), **kw),
        )


def select1(db):
    return str(select(1).compile(dialect=db.dialect))


class EngineEventsTest(fixtures.TestBase):
    __requires__ = ("ad_hoc_engines",)
    __backend__ = True

    def teardown_test(self):
        Engine.dispatch._clear()
        Engine._has_events = False

    def _assert_stmts(self, expected, received):
        list(received)
        for stmt, params, posn in expected:
            if not received:
                assert False, "Nothing available for stmt: %s" % stmt
            while received:
                teststmt, testparams, testmultiparams = received.pop(0)
                teststmt = (
                    re.compile(r"[\n\t ]+", re.M).sub(" ", teststmt).strip()
                )
                if teststmt.startswith(stmt) and (
                    testparams == params or testparams == posn
                ):
                    break

    def test_engine_connect(self, testing_engine):
        e1 = testing_engine(config.db_url)

        canary = Mock()

        def thing(conn, branch):
            canary(conn, branch)

        event.listen(e1, "engine_connect", thing)

        msg = (
            r"The argument signature for the "
            r'"ConnectionEvents.engine_connect" event listener has changed as '
            r"of version 2.0, and conversion for the old argument signature "
            r"will be removed in a future release.  The new signature is "
            r'"def engine_connect\(conn\)'
        )

        with expect_deprecated(msg):
            c1 = e1.connect()
        c1.close()

        with expect_deprecated(msg):
            c2 = e1.connect()
        c2.close()

        eq_(canary.mock_calls, [mock.call(c1, False), mock.call(c2, False)])

    def test_retval_flag(self):
        canary = []

        def tracker(name):
            def go(conn, *args, **kw):
                canary.append(name)

            return go

        def execute(conn, clauseelement, multiparams, params):
            canary.append("execute")
            return clauseelement, multiparams, params

        def cursor_execute(
            conn, cursor, statement, parameters, context, executemany
        ):
            canary.append("cursor_execute")
            return statement, parameters

        engine = engines.testing_engine()

        assert_raises(
            tsa.exc.ArgumentError,
            event.listen,
            engine,
            "begin",
            tracker("begin"),
            retval=True,
        )

        event.listen(engine, "before_execute", execute, retval=True)
        event.listen(
            engine, "before_cursor_execute", cursor_execute, retval=True
        )

        with testing.expect_deprecated(
            r"The argument signature for the "
            r"\"ConnectionEvents.before_execute\" event listener",
        ):
            with engine.connect() as conn:
                conn.execute(select(1))
        eq_(canary, ["execute", "cursor_execute"])

    def test_argument_format_execute(self):
        def before_execute(conn, clauseelement, multiparams, params):
            assert isinstance(multiparams, (list, tuple))
            assert isinstance(params, dict)

        def after_execute(conn, clauseelement, multiparams, params, result):
            assert isinstance(multiparams, (list, tuple))
            assert isinstance(params, dict)

        e1 = testing_engine(config.db_url)
        event.listen(e1, "before_execute", before_execute)
        event.listen(e1, "after_execute", after_execute)

        with testing.expect_deprecated(
            r"The argument signature for the "
            r"\"ConnectionEvents.before_execute\" event listener",
            r"The argument signature for the "
            r"\"ConnectionEvents.after_execute\" event listener",
        ):
            with e1.connect() as conn:
                result = conn.execute(select(1))
                result.close()


ce_implicit_returning = (
    r"The create_engine.implicit_returning parameter is deprecated "
    r"and will be removed in a future release."
)


class ImplicitReturningFlagTest(fixtures.TestBase):
    __backend__ = True

    @testing.combinations(True, False, None, argnames="implicit_returning")
    def test_implicit_returning_engine_parameter(self, implicit_returning):
        if implicit_returning is None:
            e = engines.testing_engine()
        else:
            with assertions.expect_deprecated(ce_implicit_returning):
                e = engines.testing_engine(
                    options={"implicit_returning": implicit_returning}
                )

        if implicit_returning is None:
            eq_(
                e.dialect.implicit_returning,
                testing.db.dialect.implicit_returning,
            )
        else:
            eq_(e.dialect.implicit_returning, implicit_returning)

        t = Table(
            "t",
            MetaData(),
            Column("id", Integer, primary_key=True),
            Column("data", String(50)),
        )

        t2 = Table(
            "t",
            MetaData(),
            Column("id", Integer, primary_key=True),
            Column("data", String(50)),
            implicit_returning=False,
        )

        with e.connect() as conn:
            stmt = insert(t).values(data="data")

            if implicit_returning:
                if not testing.requires.returning.enabled:
                    with expect_raises_message(
                        exc.CompileError, "RETURNING is not supported"
                    ):
                        stmt.compile(conn)
                else:
                    eq_(stmt.compile(conn).returning, [t.c.id])
            elif (
                implicit_returning is None
                and testing.db.dialect.implicit_returning
            ):
                eq_(stmt.compile(conn).returning, [t.c.id])
            else:
                eq_(stmt.compile(conn).returning, [])

            # table setting it to False disables it
            stmt2 = insert(t2).values(data="data")
            eq_(stmt2.compile(conn).returning, [])
