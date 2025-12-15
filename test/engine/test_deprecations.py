import re
from unittest.mock import Mock

import sqlalchemy as tsa
from sqlalchemy import create_engine
from sqlalchemy import event
from sqlalchemy import pool
from sqlalchemy import select
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
from sqlalchemy.testing.engines import testing_engine


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
    @testing.requires.sqlite
    def test_dbapi_clsmethod_renamed(self):
        """The dbapi() class method is renamed to import_dbapi(),
        so that the .dbapi attribute can be exclusively an instance
        attribute.

        """

        from sqlalchemy.dialects.sqlite import pysqlite
        from sqlalchemy.dialects import registry

        canary = mock.Mock()

        class MyDialect(pysqlite.SQLiteDialect_pysqlite):
            @classmethod
            def dbapi(cls):
                canary()
                return __import__("sqlite3")

        tokens = __name__.split(".")

        global dialect
        dialect = MyDialect

        registry.register(
            "mockdialect1.sqlite", ".".join(tokens[0:-1]), tokens[-1]
        )

        with expect_deprecated(
            r"The dbapi\(\) classmethod on dialect classes has "
            r"been renamed to import_dbapi\(\).  Implement an "
            r"import_dbapi\(\) classmethod directly on class "
            r".*MyDialect.* to remove this warning; the old "
            r".dbapi\(\) classmethod may be maintained for backwards "
            r"compatibility."
        ):
            e = create_engine("mockdialect1+sqlite://")

        eq_(canary.mock_calls, [mock.call()])
        sqlite3 = __import__("sqlite3")
        is_(e.dialect.dbapi, sqlite3)

    @testing.requires.sqlite
    def test_no_warning_for_dual_dbapi_clsmethod(self):
        """The dbapi() class method is renamed to import_dbapi(),
        so that the .dbapi attribute can be exclusively an instance
        attribute.

        Dialect classes will likely have both a dbapi() classmethod
        as well as an import_dbapi() class method to maintain
        cross-compatibility.  Make sure these updated classes don't get a
        warning and that the new method is used.

        """

        from sqlalchemy.dialects.sqlite import pysqlite
        from sqlalchemy.dialects import registry

        canary = mock.Mock()

        class MyDialect(pysqlite.SQLiteDialect_pysqlite):
            @classmethod
            def dbapi(cls):
                canary.dbapi()
                return __import__("sqlite3")

            @classmethod
            def import_dbapi(cls):
                canary.import_dbapi()
                return __import__("sqlite3")

        tokens = __name__.split(".")

        global dialect
        dialect = MyDialect

        registry.register(
            "mockdialect2.sqlite", ".".join(tokens[0:-1]), tokens[-1]
        )

        # no warning
        e = create_engine("mockdialect2+sqlite://")

        eq_(canary.mock_calls, [mock.call.import_dbapi()])
        sqlite3 = __import__("sqlite3")
        is_(e.dialect.dbapi, sqlite3)

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


class PoolTest(fixtures.TestBase):
    def test_connection_rec_connection(self):
        dbapi = MockDBAPI()
        p1 = pool.Pool(creator=lambda: dbapi.connect("foo.db"))

        rec = pool._ConnectionRecord(p1)

        with expect_deprecated(
            "The _ConnectionRecord.connection attribute is deprecated; "
            "please use 'driver_connection'"
        ):
            is_(rec.connection, rec.dbapi_connection)

    def test_connection_fairy_connection(self):
        dbapi = MockDBAPI()
        p1 = pool.QueuePool(creator=lambda: dbapi.connect("foo.db"))

        fairy = p1.connect()

        with expect_deprecated(
            "The _ConnectionFairy.connection attribute is deprecated; "
            "please use 'driver_connection'"
        ):
            is_(fairy.connection, fairy.dbapi_connection)


class ResetEventTest(fixtures.TestBase):
    def _fixture(self, **kw):
        dbapi = Mock()
        return (
            dbapi,
            pool.QueuePool(creator=lambda: dbapi.connect("foo.db"), **kw),
        )

    def _engine_fixture(self, **kw):
        dbapi = Mock()

        return dbapi, create_engine(
            "mysql://",
            module=dbapi,
            creator=lambda: dbapi.connect("foo.db"),
            _initialize=False,
        )

    def test_custom(self):
        dbapi, p = self._fixture(reset_on_return=None)

        @event.listens_for(p, "reset")
        def custom_reset(dbapi_conn, record):
            dbapi_conn.special_reset_method()

        c1 = p.connect()
        with expect_deprecated(
            'The argument signature for the "PoolEvents.reset" event '
            "listener has changed as of version 2.0"
        ):
            c1.close()

        assert dbapi.connect().special_reset_method.called
        assert not dbapi.connect().rollback.called
        assert not dbapi.connect().commit.called

    @testing.combinations(True, False, argnames="use_engine_transaction")
    def test_custom_via_engine(self, use_engine_transaction):
        dbapi, engine = self._engine_fixture(reset_on_return=None)

        @event.listens_for(engine, "reset")
        def custom_reset(dbapi_conn, record):
            dbapi_conn.special_reset_method()

        c1 = engine.connect()
        if use_engine_transaction:
            c1.begin()

        with expect_deprecated(
            'The argument signature for the "PoolEvents.reset" event '
            "listener has changed as of version 2.0"
        ):
            c1.close()
        assert dbapi.connect().rollback.called

        assert dbapi.connect().special_reset_method.called


class EngineEventsTest(fixtures.TestBase):
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
            engines.testing_engine()
        else:
            with assertions.expect_deprecated(ce_implicit_returning):
                engines.testing_engine(
                    options={"implicit_returning": implicit_returning}
                )

            # parameter has no effect
