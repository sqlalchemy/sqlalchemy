import re

import sqlalchemy as tsa
import sqlalchemy as sa
from sqlalchemy import create_engine
from sqlalchemy import DDL
from sqlalchemy import engine
from sqlalchemy import event
from sqlalchemy import exc
from sqlalchemy import ForeignKey
from sqlalchemy import func
from sqlalchemy import inspect
from sqlalchemy import INT
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import pool
from sqlalchemy import select
from sqlalchemy import String
from sqlalchemy import testing
from sqlalchemy import text
from sqlalchemy import ThreadLocalMetaData
from sqlalchemy import VARCHAR
from sqlalchemy.engine import reflection
from sqlalchemy.engine.base import Connection
from sqlalchemy.engine.base import Engine
from sqlalchemy.engine.mock import MockConnection
from sqlalchemy.testing import assert_raises
from sqlalchemy.testing import assert_raises_message
from sqlalchemy.testing import config
from sqlalchemy.testing import engines
from sqlalchemy.testing import eq_
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import is_
from sqlalchemy.testing import is_false
from sqlalchemy.testing import is_instance_of
from sqlalchemy.testing import is_true
from sqlalchemy.testing import mock
from sqlalchemy.testing.engines import testing_engine
from sqlalchemy.testing.mock import Mock
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

    def test_bind_close_engine(self):
        e = testing.db
        with e.connect() as conn:
            assert not conn.closed
        assert conn.closed

    def test_bind_create_drop_err_metadata(self):
        metadata = MetaData()
        Table("test_table", metadata, Column("foo", Integer))
        for meth in [metadata.create_all, metadata.drop_all]:
            assert_raises_message(
                exc.UnboundExecutionError,
                "MetaData object is not bound to an Engine or Connection.",
                meth,
            )

    def test_bind_create_drop_err_table(self):
        metadata = MetaData()
        table = Table("test_table", metadata, Column("foo", Integer))

        for meth in [table.create, table.drop]:
            assert_raises_message(
                exc.UnboundExecutionError,
                (
                    "Table object 'test_table' is not bound to an Engine or "
                    "Connection."
                ),
                meth,
            )

    def test_bind_create_drop_bound(self):

        for meta in (MetaData, ThreadLocalMetaData):
            for bind in (testing.db, testing.db.connect()):
                if meta is ThreadLocalMetaData:
                    with testing.expect_deprecated(
                        "ThreadLocalMetaData is deprecated"
                    ):
                        metadata = meta()
                else:
                    metadata = meta()
                table = Table("test_table", metadata, Column("foo", Integer))
                metadata.bind = bind
                assert metadata.bind is table.bind is bind
                metadata.create_all()

                with testing.expect_deprecated(
                    r"The Table.exists\(\) method is deprecated and will "
                    "be removed in a future release."
                ):
                    assert table.exists()
                metadata.drop_all()
                table.create()
                table.drop()
                with testing.expect_deprecated(
                    r"The Table.exists\(\) method is deprecated and will "
                    "be removed in a future release."
                ):
                    assert not table.exists()

                if meta is ThreadLocalMetaData:
                    with testing.expect_deprecated(
                        "ThreadLocalMetaData is deprecated"
                    ):
                        metadata = meta()
                else:
                    metadata = meta()

                table = Table("test_table", metadata, Column("foo", Integer))

                metadata.bind = bind

                assert metadata.bind is table.bind is bind
                metadata.create_all()
                with testing.expect_deprecated(
                    r"The Table.exists\(\) method is deprecated and will "
                    "be removed in a future release."
                ):
                    assert table.exists()
                metadata.drop_all()
                table.create()
                table.drop()
                with testing.expect_deprecated(
                    r"The Table.exists\(\) method is deprecated and will "
                    "be removed in a future release."
                ):
                    assert not table.exists()
                if isinstance(bind, engine.Connection):
                    bind.close()

    def test_bind_create_drop_constructor_bound(self):
        for bind in (testing.db, testing.db.connect()):
            try:
                for args in (([bind], {}), ([], {"bind": bind})):
                    metadata = MetaData(*args[0], **args[1])
                    table = Table(
                        "test_table", metadata, Column("foo", Integer)
                    )
                    assert metadata.bind is table.bind is bind
                    metadata.create_all()
                    is_true(inspect(bind).has_table(table.name))
                    metadata.drop_all()
                    table.create()
                    table.drop()
                    is_false(inspect(bind).has_table(table.name))
            finally:
                if isinstance(bind, engine.Connection):
                    bind.close()

    def test_bind_implicit_execution(self):
        metadata = MetaData()
        table = Table(
            "test_table",
            metadata,
            Column("foo", Integer),
            test_needs_acid=True,
        )
        conn = testing.db.connect()
        metadata.create_all(bind=conn)
        try:
            trans = conn.begin()
            metadata.bind = conn
            t = table.insert()
            assert t.bind is conn
            table.insert().execute(foo=5)
            table.insert().execute(foo=6)
            table.insert().execute(foo=7)
            trans.rollback()
            metadata.bind = None
            assert (
                conn.exec_driver_sql(
                    "select count(*) from test_table"
                ).scalar()
                == 0
            )
        finally:
            metadata.drop_all(bind=conn)

    def test_bind_clauseelement(self):
        metadata = MetaData()
        table = Table("test_table", metadata, Column("foo", Integer))
        metadata.create_all(bind=testing.db)
        try:
            for elem in [
                table.select,
                lambda **kwargs: sa.func.current_timestamp(**kwargs).select(),
                # func.current_timestamp().select,
                lambda **kwargs: text("select * from test_table", **kwargs),
            ]:
                for bind in (testing.db, testing.db.connect()):
                    try:
                        with testing.expect_deprecated_20(
                            "The .*bind argument is deprecated"
                        ):
                            e = elem(bind=bind)
                        assert e.bind is bind
                        e.execute().close()
                    finally:
                        if isinstance(bind, engine.Connection):
                            bind.close()

                e = elem()
                assert e.bind is None
                assert_raises(exc.UnboundExecutionError, e.execute)
        finally:
            if isinstance(bind, engine.Connection):
                bind.close()
            metadata.drop_all(bind=testing.db)

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

    def test_bind_close_conn(self):
        e = testing.db
        conn = e.connect()

        with testing.expect_deprecated_20(
            r"The Connection.connect\(\) method is considered",
            r"The .close\(\) method on a so-called 'branched' connection is "
            r"deprecated as of 1.4, as are 'branched' connections overall, "
            r"and will be removed in a future release.",
        ):
            with conn.connect() as c2:
                assert not c2.closed
        assert not conn.closed
        assert c2.closed

    @testing.provide_metadata
    def test_explicit_connectionless_execute(self):
        table = Table("t", self.metadata, Column("a", Integer))
        table.create(testing.db)

        stmt = table.insert().values(a=1)
        with testing.expect_deprecated_20(
            r"The Engine.execute\(\) method is considered legacy",
        ):
            testing.db.execute(stmt)

        stmt = select(table)
        with testing.expect_deprecated_20(
            r"The Engine.execute\(\) method is considered legacy",
        ):
            eq_(testing.db.execute(stmt).fetchall(), [(1,)])

    @testing.provide_metadata
    def test_implicit_execute(self):
        table = Table("t", self.metadata, Column("a", Integer))
        table.create(testing.db)

        stmt = table.insert().values(a=1)
        with testing.expect_deprecated_20(
            r"The Executable.execute\(\) method is considered legacy",
        ):
            stmt.execute()

        stmt = select(table)
        with testing.expect_deprecated_20(
            r"The Executable.execute\(\) method is considered legacy",
        ):
            eq_(stmt.execute().fetchall(), [(1,)])


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

    def test_empty_in_keyword(self):
        with testing.expect_deprecated(
            "The create_engine.empty_in_strategy keyword is deprecated, "
            "and no longer has any effect."
        ):
            create_engine(
                "postgresql://",
                empty_in_strategy="static",
                module=Mock(),
                _initialize=False,
            )


class TransactionTest(fixtures.TablesTest):
    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "users",
            metadata,
            Column("user_id", Integer, primary_key=True),
            Column("user_name", String(20)),
            test_needs_acid=True,
        )
        Table("inserttable", metadata, Column("data", String(20)))

    def test_transaction_container(self):
        users = self.tables.users

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

    def test_implicit_autocommit_compiled(self):
        users = self.tables.users

        with testing.db.connect() as conn:
            with testing.expect_deprecated_20(
                "The current statement is being autocommitted "
                "using implicit autocommit."
            ):
                conn.execute(
                    users.insert(), {"user_id": 1, "user_name": "user3"}
                )

    def test_implicit_autocommit_text(self):
        with testing.db.connect() as conn:
            with testing.expect_deprecated_20(
                "The current statement is being autocommitted "
                "using implicit autocommit."
            ):
                conn.execute(
                    text("insert into inserttable (data) values ('thedata')")
                )

    def test_implicit_autocommit_driversql(self):
        with testing.db.connect() as conn:
            with testing.expect_deprecated_20(
                "The current statement is being autocommitted "
                "using implicit autocommit."
            ):
                conn.exec_driver_sql(
                    "insert into inserttable (data) values ('thedata')"
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


def select1(db):
    return str(select(1).compile(dialect=db.dialect))


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
        with testing.db.connect() as conn:
            eq_(
                conn.scalar(select(func.count("*")).select_from(self.table)),
                0,
            )

    def _assert_fn(self, x, value=None):
        with testing.db.connect() as conn:
            eq_(conn.execute(self.table.select()).fetchall(), [(x, value)])

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

    def test_execute_plain_string(self):
        with _string_deprecation_expect():
            testing.db.execute(select1(testing.db)).scalar()

    def test_execute_plain_string_events(self):

        m1 = Mock()
        select1_str = select1(testing.db)
        with _string_deprecation_expect():
            with testing.db.connect() as conn:
                event.listen(conn, "before_execute", m1.before_execute)
                event.listen(conn, "after_execute", m1.after_execute)
                result = conn.execute(select1_str)
        eq_(
            m1.mock_calls,
            [
                mock.call.before_execute(mock.ANY, select1_str, [], {}, {}),
                mock.call.after_execute(
                    mock.ANY, select1_str, [], {}, {}, result
                ),
            ],
        )

    def test_scalar_plain_string(self):
        with _string_deprecation_expect():
            testing.db.scalar(select1(testing.db))

    # Tests for the warning when non dict params are used
    # @testing.combinations(42, (42,))
    # def test_execute_positional_non_dicts(self, args):
    #     with testing.expect_deprecated(
    #         r"Usage of tuple or scalars as positional arguments of "
    #     ):
    #         testing.db.execute(text(select1(testing.db)), args).scalar()

    # @testing.combinations(42, (42,))
    # def test_scalar_positional_non_dicts(self, args):
    #     with testing.expect_deprecated(
    #         r"Usage of tuple or scalars as positional arguments of "
    #     ):
    #         testing.db.scalar(text(select1(testing.db)), args)


class DeprecatedConnectionFeatureTest(fixtures.TablesTest):
    __backend__ = True

    def test_execute_plain_string(self):
        with _string_deprecation_expect():
            with testing.db.connect() as conn:
                conn.execute(select1(testing.db)).scalar()

    def test_scalar_plain_string(self):
        with _string_deprecation_expect():
            with testing.db.connect() as conn:
                conn.scalar(select1(testing.db))

    # Tests for the warning when non dict params are used
    # @testing.combinations(42, (42,))
    # def test_execute_positional_non_dicts(self, args):
    #     with testing.expect_deprecated(
    #         r"Usage of tuple or scalars as positional arguments of "
    #     ):
    #         with testing.db.connect() as conn:
    #             conn.execute(text(select1(testing.db)), args).scalar()

    # @testing.combinations(42, (42,))
    # def test_scalar_positional_non_dicts(self, args):
    #     with testing.expect_deprecated(
    #         r"Usage of tuple or scalars as positional arguments of "
    #     ):
    #         with testing.db.connect() as conn:
    #             conn.scalar(text(select1(testing.db)), args)


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

    def test_reflecttable(self):
        inspector = inspect(testing.db)
        metadata = self.metadata
        table = Table("user", metadata)
        with testing.expect_deprecated_20(
            r"The Inspector.reflecttable\(\) method is considered "
        ):
            res = inspector.reflecttable(table, None)
        exp = inspector.reflect_table(table, None)

        eq_(res, exp)


class ExecutionOptionsTest(fixtures.TestBase):
    def test_branched_connection_execution_options(self):
        engine = engines.testing_engine("sqlite://")

        conn = engine.connect()
        c2 = conn.execution_options(foo="bar")

        with testing.expect_deprecated_20(
            r"The Connection.connect\(\) method is considered "
        ):
            c2_branch = c2.connect()
        eq_(c2_branch._execution_options, {"foo": "bar"})


class RawExecuteTest(fixtures.TablesTest):
    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "users",
            metadata,
            Column("user_id", INT, primary_key=True, autoincrement=False),
            Column("user_name", VARCHAR(20)),
        )
        Table(
            "users_autoinc",
            metadata,
            Column(
                "user_id", INT, primary_key=True, test_needs_autoincrement=True
            ),
            Column("user_name", VARCHAR(20)),
        )

    def test_no_params_option(self, connection):
        stmt = (
            "SELECT '%'"
            + testing.db.dialect.statement_compiler(
                testing.db.dialect, None
            ).default_from()
        )

        with _string_deprecation_expect():
            result = (
                connection.execution_options(no_parameters=True)
                .execute(stmt)
                .scalar()
            )
        eq_(result, "%")

    @testing.requires.qmark_paramstyle
    def test_raw_qmark(self, connection):
        conn = connection

        with _string_deprecation_expect():
            conn.execute(
                "insert into users (user_id, user_name) " "values (?, ?)",
                (1, "jack"),
            )
        with _string_deprecation_expect():
            conn.execute(
                "insert into users (user_id, user_name) " "values (?, ?)",
                [2, "fred"],
            )

        with _string_deprecation_expect():
            conn.execute(
                "insert into users (user_id, user_name) " "values (?, ?)",
                [3, "ed"],
                [4, "horse"],
            )
        with _string_deprecation_expect():
            conn.execute(
                "insert into users (user_id, user_name) " "values (?, ?)",
                (5, "barney"),
                (6, "donkey"),
            )

        with _string_deprecation_expect():
            conn.execute(
                "insert into users (user_id, user_name) " "values (?, ?)",
                7,
                "sally",
            )

        with _string_deprecation_expect():
            res = conn.execute("select * from users order by user_id")
        assert res.fetchall() == [
            (1, "jack"),
            (2, "fred"),
            (3, "ed"),
            (4, "horse"),
            (5, "barney"),
            (6, "donkey"),
            (7, "sally"),
        ]
        for multiparam, param in [
            (("jack", "fred"), {}),
            ((["jack", "fred"],), {}),
        ]:
            with _string_deprecation_expect():
                res = conn.execute(
                    "select * from users where user_name=? or "
                    "user_name=? order by user_id",
                    *multiparam,
                    **param
                )
            assert res.fetchall() == [(1, "jack"), (2, "fred")]

        with _string_deprecation_expect():
            res = conn.execute("select * from users where user_name=?", "jack")
        assert res.fetchall() == [(1, "jack")]

    @testing.requires.format_paramstyle
    def test_raw_sprintf(self, connection):
        conn = connection
        with _string_deprecation_expect():
            conn.execute(
                "insert into users (user_id, user_name) " "values (%s, %s)",
                [1, "jack"],
            )
        with _string_deprecation_expect():
            conn.execute(
                "insert into users (user_id, user_name) " "values (%s, %s)",
                [2, "ed"],
                [3, "horse"],
            )
        with _string_deprecation_expect():
            conn.execute(
                "insert into users (user_id, user_name) " "values (%s, %s)",
                4,
                "sally",
            )
        with _string_deprecation_expect():
            conn.execute("insert into users (user_id) values (%s)", 5)
        with _string_deprecation_expect():
            res = conn.execute("select * from users order by user_id")
            assert res.fetchall() == [
                (1, "jack"),
                (2, "ed"),
                (3, "horse"),
                (4, "sally"),
                (5, None),
            ]
        for multiparam, param in [
            (("jack", "ed"), {}),
            ((["jack", "ed"],), {}),
        ]:
            with _string_deprecation_expect():
                res = conn.execute(
                    "select * from users where user_name=%s or "
                    "user_name=%s order by user_id",
                    *multiparam,
                    **param
                )
                assert res.fetchall() == [(1, "jack"), (2, "ed")]
        with _string_deprecation_expect():
            res = conn.execute(
                "select * from users where user_name=%s", "jack"
            )
        assert res.fetchall() == [(1, "jack")]

    @testing.requires.pyformat_paramstyle
    def test_raw_python(self, connection):
        conn = connection
        with _string_deprecation_expect():
            conn.execute(
                "insert into users (user_id, user_name) "
                "values (%(id)s, %(name)s)",
                {"id": 1, "name": "jack"},
            )
        with _string_deprecation_expect():
            conn.execute(
                "insert into users (user_id, user_name) "
                "values (%(id)s, %(name)s)",
                {"id": 2, "name": "ed"},
                {"id": 3, "name": "horse"},
            )
        with _string_deprecation_expect():
            conn.execute(
                "insert into users (user_id, user_name) "
                "values (%(id)s, %(name)s)",
                id=4,
                name="sally",
            )
        with _string_deprecation_expect():
            res = conn.execute("select * from users order by user_id")
            assert res.fetchall() == [
                (1, "jack"),
                (2, "ed"),
                (3, "horse"),
                (4, "sally"),
            ]

    @testing.requires.named_paramstyle
    def test_raw_named(self, connection):
        conn = connection
        with _string_deprecation_expect():
            conn.execute(
                "insert into users (user_id, user_name) "
                "values (:id, :name)",
                {"id": 1, "name": "jack"},
            )
        with _string_deprecation_expect():
            conn.execute(
                "insert into users (user_id, user_name) "
                "values (:id, :name)",
                {"id": 2, "name": "ed"},
                {"id": 3, "name": "horse"},
            )
        with _string_deprecation_expect():
            conn.execute(
                "insert into users (user_id, user_name) "
                "values (:id, :name)",
                id=4,
                name="sally",
            )
        with _string_deprecation_expect():
            res = conn.execute("select * from users order by user_id")
            assert res.fetchall() == [
                (1, "jack"),
                (2, "ed"),
                (3, "horse"),
                (4, "sally"),
            ]


class DeprecatedExecParamsTest(fixtures.TablesTest):
    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "users",
            metadata,
            Column("user_id", INT, primary_key=True, autoincrement=False),
            Column("user_name", VARCHAR(20)),
        )

        Table(
            "users_autoinc",
            metadata,
            Column(
                "user_id", INT, primary_key=True, test_needs_autoincrement=True
            ),
            Column("user_name", VARCHAR(20)),
        )

    def test_kwargs(self, connection):
        users = self.tables.users

        with testing.expect_deprecated_20(
            r"The connection.execute\(\) method in "
            "SQLAlchemy 2.0 will accept parameters as a single "
        ):
            connection.execute(
                users.insert(), user_id=5, user_name="some name"
            )

        eq_(connection.execute(select(users)).all(), [(5, "some name")])

    def test_positional_dicts(self, connection):
        users = self.tables.users

        with testing.expect_deprecated_20(
            r"The connection.execute\(\) method in "
            "SQLAlchemy 2.0 will accept parameters as a single "
        ):
            connection.execute(
                users.insert(),
                {"user_id": 5, "user_name": "some name"},
                {"user_id": 6, "user_name": "some other name"},
            )

        eq_(
            connection.execute(select(users).order_by(users.c.user_id)).all(),
            [(5, "some name"), (6, "some other name")],
        )

    def test_single_scalar(self, connection):

        users = self.tables.users_autoinc

        with testing.expect_deprecated_20(
            r"The connection.execute\(\) method in "
            "SQLAlchemy 2.0 will accept parameters as a single "
        ):
            # TODO: I'm not even sure what this exec format is or how
            # it worked if at all
            connection.execute(users.insert(), "some name")

        eq_(
            connection.execute(select(users).order_by(users.c.user_id)).all(),
            [(1, None)],
        )


class EngineEventsTest(fixtures.TestBase):
    __requires__ = ("ad_hoc_engines",)
    __backend__ = True

    def tearDown(self):
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
            engine.execute(select(1))
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
            e1.execute(select(1))


class DDLExecutionTest(fixtures.TestBase):
    def setup(self):
        self.engine = engines.mock_engine()
        self.metadata = MetaData(self.engine)
        self.users = Table(
            "users",
            self.metadata,
            Column("user_id", Integer, primary_key=True),
            Column("user_name", String(40)),
        )

    @testing.requires.sqlite
    def test_ddl_execute(self):
        engine = create_engine("sqlite:///")
        cx = engine.connect()
        table = self.users
        ddl = DDL("SELECT 1")

        eng_msg = r"The Engine.execute\(\) method is considered legacy"
        ddl_msg = r"The DDL.execute\(\) method is considered legacy"
        for spec in (
            (engine.execute, ddl, eng_msg),
            (engine.execute, ddl, table, eng_msg),
            (ddl.execute, engine, ddl_msg),
            (ddl.execute, engine, table, ddl_msg),
            (ddl.execute, cx, ddl_msg),
            (ddl.execute, cx, table, ddl_msg),
        ):
            fn = spec[0]
            arg = spec[1:-1]
            warning = spec[-1]

            with testing.expect_deprecated_20(warning):
                r = fn(*arg)
            eq_(list(r), [(1,)])

        for fn, kw in ((ddl.execute, {}), (ddl.execute, dict(target=table))):
            with testing.expect_deprecated_20(ddl_msg):
                assert_raises(exc.UnboundExecutionError, fn, **kw)

        for bind in engine, cx:
            ddl.bind = bind
            for fn, kw in (
                (ddl.execute, {}),
                (ddl.execute, dict(target=table)),
            ):
                with testing.expect_deprecated_20(ddl_msg):
                    r = fn(**kw)
                eq_(list(r), [(1,)])
