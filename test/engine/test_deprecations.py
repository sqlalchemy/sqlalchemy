import re

import sqlalchemy as tsa
import sqlalchemy as sa
from sqlalchemy import bindparam
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
from .test_transaction import ResetFixture


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
            with testing.expect_deprecated_20(
                "The ``bind`` argument for schema methods that invoke SQL"
            ):
                assert_raises_message(
                    exc.UnboundExecutionError,
                    "MetaData object is not bound to an Engine or Connection.",
                    meth,
                )

    def test_bind_create_drop_err_table(self):
        metadata = MetaData()
        table = Table("test_table", metadata, Column("foo", Integer))

        for meth in [table.create, table.drop]:
            with testing.expect_deprecated_20(
                "The ``bind`` argument for schema methods that invoke SQL"
            ):
                assert_raises_message(
                    exc.UnboundExecutionError,
                    (
                        "Table object 'test_table' is not bound to an "
                        "Engine or Connection."
                    ),
                    meth,
                )

    def test_bind_create_drop_bound(self):

        for meta in (MetaData, ThreadLocalMetaData):
            for bind in (testing.db, testing.db.connect()):
                if isinstance(bind, engine.Connection):
                    bind.begin()

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
                with testing.expect_deprecated_20(
                    "The ``bind`` argument for schema methods that invoke SQL"
                ):
                    metadata.create_all()

                with testing.expect_deprecated(
                    r"The Table.exists\(\) method is deprecated and will "
                    "be removed in a future release."
                ):
                    assert table.exists()
                with testing.expect_deprecated_20(
                    "The ``bind`` argument for schema methods that invoke SQL"
                ):
                    metadata.drop_all()
                with testing.expect_deprecated_20(
                    "The ``bind`` argument for schema methods that invoke SQL"
                ):
                    table.create()
                with testing.expect_deprecated_20(
                    "The ``bind`` argument for schema methods that invoke SQL"
                ):
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
                with testing.expect_deprecated_20(
                    "The ``bind`` argument for schema methods that invoke SQL"
                ):
                    metadata.create_all()
                with testing.expect_deprecated(
                    r"The Table.exists\(\) method is deprecated and will "
                    "be removed in a future release."
                ):
                    assert table.exists()
                with testing.expect_deprecated_20(
                    "The ``bind`` argument for schema methods that invoke SQL"
                ):
                    metadata.drop_all()
                with testing.expect_deprecated_20(
                    "The ``bind`` argument for schema methods that invoke SQL"
                ):
                    table.create()
                with testing.expect_deprecated_20(
                    "The ``bind`` argument for schema methods that invoke SQL"
                ):
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
            if isinstance(bind, engine.Connection):
                bind.begin()
            try:
                for args in (([bind], {}), ([], {"bind": bind})):
                    with testing.expect_deprecated_20(
                        "The MetaData.bind argument is deprecated "
                    ):
                        metadata = MetaData(*args[0], **args[1])
                    table = Table(
                        "test_table", metadata, Column("foo", Integer)
                    )
                    assert metadata.bind is table.bind is bind
                    with testing.expect_deprecated_20(
                        "The ``bind`` argument for schema methods "
                        "that invoke SQL"
                    ):
                        metadata.create_all()
                    is_true(inspect(bind).has_table(table.name))
                    with testing.expect_deprecated_20(
                        "The ``bind`` argument for schema methods "
                        "that invoke SQL"
                    ):
                        metadata.drop_all()
                    with testing.expect_deprecated_20(
                        "The ``bind`` argument for schema methods "
                        "that invoke SQL"
                    ):
                        table.create()
                    with testing.expect_deprecated_20(
                        "The ``bind`` argument for schema methods "
                        "that invoke SQL"
                    ):
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
        with conn.begin():
            metadata.create_all(bind=conn)
        try:
            trans = conn.begin()
            metadata.bind = conn
            t = table.insert()
            assert t.bind is conn
            with testing.expect_deprecated_20(
                r"The Executable.execute\(\) method is considered legacy"
            ):
                table.insert().execute(foo=5)
            with testing.expect_deprecated_20(
                r"The Executable.execute\(\) method is considered legacy"
            ):
                table.insert().execute(foo=6)
            with testing.expect_deprecated_20(
                r"The Executable.execute\(\) method is considered legacy"
            ):
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
            with conn.begin():
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
                        with testing.expect_deprecated_20(
                            r"The Executable.execute\(\) method is "
                            "considered legacy"
                        ):
                            e.execute().close()
                    finally:
                        if isinstance(bind, engine.Connection):
                            bind.close()

                e = elem()
                assert e.bind is None
                with testing.expect_deprecated_20(
                    r"The Executable.execute\(\) method is considered legacy"
                ):
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

    def test_implicit_execute(self, metadata):
        table = Table("t", metadata, Column("a", Integer))
        table.create(testing.db)

        metadata.bind = testing.db
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


class TransactionTest(ResetFixture, fixtures.TablesTest):
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

    @testing.fixture
    def local_connection(self):
        with testing.db.connect() as conn:
            yield conn

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

    def test_begin_begin_rollback_rollback(self, reset_agent):
        with reset_agent.engine.connect() as connection:
            trans = connection.begin()
            with testing.expect_deprecated_20(
                r"Calling .begin\(\) when a transaction is already "
                "begun, creating a 'sub' transaction"
            ):
                trans2 = connection.begin()
            trans2.rollback()
            trans.rollback()
        eq_(
            reset_agent.mock_calls,
            [
                mock.call.rollback(connection),
                mock.call.do_rollback(mock.ANY),
                mock.call.do_rollback(mock.ANY),
            ],
        )

    def test_begin_begin_commit_commit(self, reset_agent):
        with reset_agent.engine.connect() as connection:
            trans = connection.begin()
            with testing.expect_deprecated_20(
                r"Calling .begin\(\) when a transaction is already "
                "begun, creating a 'sub' transaction"
            ):
                trans2 = connection.begin()
            trans2.commit()
            trans.commit()
        eq_(
            reset_agent.mock_calls,
            [
                mock.call.commit(connection),
                mock.call.do_commit(mock.ANY),
                mock.call.do_rollback(mock.ANY),
            ],
        )

    def test_branch_nested_rollback(self, local_connection):
        connection = local_connection
        users = self.tables.users
        connection.begin()
        branched = connection.connect()
        assert branched.in_transaction()
        branched.execute(users.insert(), dict(user_id=1, user_name="user1"))
        with testing.expect_deprecated_20(
            r"Calling .begin\(\) when a transaction is already "
            "begun, creating a 'sub' transaction"
        ):
            nested = branched.begin()
        branched.execute(users.insert(), dict(user_id=2, user_name="user2"))
        nested.rollback()
        assert not connection.in_transaction()

        assert_raises_message(
            exc.InvalidRequestError,
            "This connection is on an inactive transaction.  Please",
            connection.exec_driver_sql,
            "select 1",
        )

    @testing.requires.savepoints
    def test_savepoint_cancelled_by_toplevel_marker(self, local_connection):
        conn = local_connection
        users = self.tables.users
        trans = conn.begin()
        conn.execute(users.insert(), {"user_id": 1, "user_name": "name"})

        with testing.expect_deprecated_20(
            r"Calling .begin\(\) when a transaction is already "
            "begun, creating a 'sub' transaction"
        ):
            mk1 = conn.begin()

        sp1 = conn.begin_nested()
        conn.execute(users.insert(), {"user_id": 2, "user_name": "name2"})

        mk1.rollback()

        assert not sp1.is_active
        assert not trans.is_active
        assert conn._transaction is trans
        assert conn._nested_transaction is None

        with testing.db.connect() as conn:
            eq_(
                conn.scalar(select(func.count(1)).select_from(users)),
                0,
            )

    @testing.requires.savepoints
    def test_rollback_to_subtransaction(self, local_connection):
        connection = local_connection
        users = self.tables.users
        transaction = connection.begin()
        connection.execute(users.insert(), dict(user_id=1, user_name="user1"))
        trans2 = connection.begin_nested()
        connection.execute(users.insert(), dict(user_id=2, user_name="user2"))

        with testing.expect_deprecated_20(
            r"Calling .begin\(\) when a transaction is already "
            "begun, creating a 'sub' transaction"
        ):
            trans3 = connection.begin()
        connection.execute(users.insert(), dict(user_id=3, user_name="user3"))
        trans3.rollback()

        assert_raises_message(
            exc.InvalidRequestError,
            "This connection is on an inactive savepoint transaction.",
            connection.exec_driver_sql,
            "select 1",
        )
        trans2.rollback()
        assert connection._nested_transaction is None

        connection.execute(users.insert(), dict(user_id=4, user_name="user4"))
        transaction.commit()
        eq_(
            connection.execute(
                select(users.c.user_id).order_by(users.c.user_id)
            ).fetchall(),
            [(1,), (4,)],
        )

    # PG emergency shutdown:
    # select * from pg_prepared_xacts
    # ROLLBACK PREPARED '<xid>'
    # MySQL emergency shutdown:
    # for arg in `mysql -u root -e "xa recover" | cut -c 8-100 |
    #     grep sa`; do mysql -u root -e "xa rollback '$arg'"; done
    @testing.requires.skip_mysql_on_windows
    @testing.requires.two_phase_transactions
    @testing.requires.savepoints
    def test_mixed_two_phase_transaction(self, local_connection):
        connection = local_connection
        users = self.tables.users
        transaction = connection.begin_twophase()
        connection.execute(users.insert(), dict(user_id=1, user_name="user1"))
        with testing.expect_deprecated_20(
            r"Calling .begin\(\) when a transaction is already "
            "begun, creating a 'sub' transaction"
        ):
            transaction2 = connection.begin()
        connection.execute(users.insert(), dict(user_id=2, user_name="user2"))
        transaction3 = connection.begin_nested()
        connection.execute(users.insert(), dict(user_id=3, user_name="user3"))
        with testing.expect_deprecated_20(
            r"Calling .begin\(\) when a transaction is already "
            "begun, creating a 'sub' transaction"
        ):
            transaction4 = connection.begin()
        connection.execute(users.insert(), dict(user_id=4, user_name="user4"))
        transaction4.commit()
        transaction3.rollback()
        connection.execute(users.insert(), dict(user_id=5, user_name="user5"))
        transaction2.commit()
        transaction.prepare()
        transaction.commit()
        eq_(
            connection.execute(
                select(users.c.user_id).order_by(users.c.user_id)
            ).fetchall(),
            [(1,), (2,), (5,)],
        )

    @testing.requires.savepoints
    def test_inactive_due_to_subtransaction_on_nested_no_commit(
        self, local_connection
    ):
        connection = local_connection
        trans = connection.begin()

        nested = connection.begin_nested()

        with testing.expect_deprecated_20(
            r"Calling .begin\(\) when a transaction is already "
            "begun, creating a 'sub' transaction"
        ):
            trans2 = connection.begin()
        trans2.rollback()

        assert_raises_message(
            exc.InvalidRequestError,
            "This connection is on an inactive savepoint transaction.  "
            "Please rollback",
            nested.commit,
        )
        trans.commit()

        assert_raises_message(
            exc.InvalidRequestError,
            "This nested transaction is inactive",
            nested.commit,
        )

    def test_close(self, local_connection):
        connection = local_connection
        users = self.tables.users
        transaction = connection.begin()
        connection.execute(users.insert(), dict(user_id=1, user_name="user1"))
        connection.execute(users.insert(), dict(user_id=2, user_name="user2"))
        connection.execute(users.insert(), dict(user_id=3, user_name="user3"))
        with testing.expect_deprecated_20(
            r"Calling .begin\(\) when a transaction is already "
            "begun, creating a 'sub' transaction"
        ):
            trans2 = connection.begin()
        connection.execute(users.insert(), dict(user_id=4, user_name="user4"))
        connection.execute(users.insert(), dict(user_id=5, user_name="user5"))
        assert connection.in_transaction()
        trans2.close()
        assert connection.in_transaction()
        transaction.commit()
        assert not connection.in_transaction()
        self.assert_(
            connection.exec_driver_sql(
                "select count(*) from " "users"
            ).scalar()
            == 5
        )
        result = connection.exec_driver_sql("select * from users")
        assert len(result.fetchall()) == 5

    def test_close2(self, local_connection):
        connection = local_connection
        users = self.tables.users
        transaction = connection.begin()
        connection.execute(users.insert(), dict(user_id=1, user_name="user1"))
        connection.execute(users.insert(), dict(user_id=2, user_name="user2"))
        connection.execute(users.insert(), dict(user_id=3, user_name="user3"))
        with testing.expect_deprecated_20(
            r"Calling .begin\(\) when a transaction is already "
            "begun, creating a 'sub' transaction"
        ):
            trans2 = connection.begin()
        connection.execute(users.insert(), dict(user_id=4, user_name="user4"))
        connection.execute(users.insert(), dict(user_id=5, user_name="user5"))
        assert connection.in_transaction()
        trans2.close()
        assert connection.in_transaction()
        transaction.close()
        assert not connection.in_transaction()
        self.assert_(
            connection.exec_driver_sql(
                "select count(*) from " "users"
            ).scalar()
            == 0
        )
        result = connection.exec_driver_sql("select * from users")
        assert len(result.fetchall()) == 0

    def test_inactive_due_to_subtransaction_no_commit(self, local_connection):
        connection = local_connection
        trans = connection.begin()
        with testing.expect_deprecated_20(
            r"Calling .begin\(\) when a transaction is already "
            "begun, creating a 'sub' transaction"
        ):
            trans2 = connection.begin()
        trans2.rollback()
        assert_raises_message(
            exc.InvalidRequestError,
            "This connection is on an inactive transaction.  Please rollback",
            trans.commit,
        )

        trans.rollback()

        assert_raises_message(
            exc.InvalidRequestError,
            "This transaction is inactive",
            trans.commit,
        )

    def test_nested_rollback(self, local_connection):
        connection = local_connection
        users = self.tables.users
        try:
            transaction = connection.begin()
            try:
                connection.execute(
                    users.insert(), dict(user_id=1, user_name="user1")
                )
                connection.execute(
                    users.insert(), dict(user_id=2, user_name="user2")
                )
                connection.execute(
                    users.insert(), dict(user_id=3, user_name="user3")
                )
                with testing.expect_deprecated_20(
                    r"Calling .begin\(\) when a transaction is already "
                    "begun, creating a 'sub' transaction"
                ):
                    trans2 = connection.begin()
                try:
                    connection.execute(
                        users.insert(), dict(user_id=4, user_name="user4")
                    )
                    connection.execute(
                        users.insert(), dict(user_id=5, user_name="user5")
                    )
                    raise Exception("uh oh")
                    trans2.commit()
                except Exception:
                    trans2.rollback()
                    raise
                transaction.rollback()
            except Exception:
                transaction.rollback()
                raise
        except Exception as e:
            # and not "This transaction is inactive"
            # comment moved here to fix pep8
            assert str(e) == "uh oh"
        else:
            assert False

    def test_nesting(self, local_connection):
        connection = local_connection
        users = self.tables.users
        transaction = connection.begin()
        connection.execute(users.insert(), dict(user_id=1, user_name="user1"))
        connection.execute(users.insert(), dict(user_id=2, user_name="user2"))
        connection.execute(users.insert(), dict(user_id=3, user_name="user3"))
        with testing.expect_deprecated_20(
            r"Calling .begin\(\) when a transaction is already "
            "begun, creating a 'sub' transaction"
        ):
            trans2 = connection.begin()
        connection.execute(users.insert(), dict(user_id=4, user_name="user4"))
        connection.execute(users.insert(), dict(user_id=5, user_name="user5"))
        trans2.commit()
        transaction.rollback()
        self.assert_(
            connection.exec_driver_sql(
                "select count(*) from " "users"
            ).scalar()
            == 0
        )
        result = connection.exec_driver_sql("select * from users")
        assert len(result.fetchall()) == 0

    def test_no_marker_on_inactive_trans(self, local_connection):
        conn = local_connection
        conn.begin()

        with testing.expect_deprecated_20(
            r"Calling .begin\(\) when a transaction is already "
            "begun, creating a 'sub' transaction"
        ):
            mk1 = conn.begin()

        mk1.rollback()

        assert_raises_message(
            exc.InvalidRequestError,
            "the current transaction on this connection is inactive.",
            conn.begin,
        )

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

    def test_branch_autorollback(self, local_connection):
        connection = local_connection
        users = self.tables.users
        branched = connection.connect()
        with testing.expect_deprecated_20(
            "The current statement is being autocommitted using "
            "implicit autocommit"
        ):
            branched.execute(
                users.insert(), dict(user_id=1, user_name="user1")
            )
        assert_raises(
            exc.DBAPIError,
            branched.execute,
            users.insert(),
            dict(user_id=1, user_name="user1"),
        )
        # can continue w/o issue
        with testing.expect_deprecated_20(
            "The current statement is being autocommitted using "
            "implicit autocommit"
        ):
            branched.execute(
                users.insert(), dict(user_id=2, user_name="user2")
            )

    def test_branch_orig_rollback(self, local_connection):
        connection = local_connection
        users = self.tables.users
        branched = connection.connect()
        with testing.expect_deprecated_20(
            "The current statement is being autocommitted using "
            "implicit autocommit"
        ):
            branched.execute(
                users.insert(), dict(user_id=1, user_name="user1")
            )
        nested = branched.begin()
        assert branched.in_transaction()
        branched.execute(users.insert(), dict(user_id=2, user_name="user2"))
        nested.rollback()
        eq_(
            connection.exec_driver_sql("select count(*) from users").scalar(),
            1,
        )

    @testing.requires.independent_connections
    def test_branch_autocommit(self, local_connection):
        users = self.tables.users
        with testing.db.connect() as connection:
            branched = connection.connect()
            with testing.expect_deprecated_20(
                "The current statement is being autocommitted using "
                "implicit autocommit"
            ):
                branched.execute(
                    users.insert(), dict(user_id=1, user_name="user1")
                )

        eq_(
            local_connection.execute(
                text("select count(*) from users")
            ).scalar(),
            1,
        )

    @testing.requires.savepoints
    def test_branch_savepoint_rollback(self, local_connection):
        connection = local_connection
        users = self.tables.users
        trans = connection.begin()
        branched = connection.connect()
        assert branched.in_transaction()
        branched.execute(users.insert(), dict(user_id=1, user_name="user1"))
        nested = branched.begin_nested()
        branched.execute(users.insert(), dict(user_id=2, user_name="user2"))
        nested.rollback()
        assert connection.in_transaction()
        trans.commit()
        eq_(
            connection.exec_driver_sql("select count(*) from users").scalar(),
            1,
        )

    @testing.requires.two_phase_transactions
    def test_branch_twophase_rollback(self, local_connection):
        connection = local_connection
        users = self.tables.users
        branched = connection.connect()
        assert not branched.in_transaction()
        with testing.expect_deprecated_20(
            r"The current statement is being autocommitted using "
            "implicit autocommit"
        ):
            branched.execute(
                users.insert(), dict(user_id=1, user_name="user1")
            )
        nested = branched.begin_twophase()
        branched.execute(users.insert(), dict(user_id=2, user_name="user2"))
        nested.rollback()
        assert not connection.in_transaction()
        eq_(
            connection.exec_driver_sql("select count(*) from users").scalar(),
            1,
        )


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
        bind = testing.db
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
        metadata = self.tables_test_metadata

        with testing.expect_deprecated(
            r"The Engine.table_names\(\) method is deprecated"
        ):
            table_names = testing.db.table_names()
        is_true(set(table_names).issuperset(metadata.tables))

    def test_reflecttable(self):
        inspector = inspect(testing.db)
        metadata = MetaData()

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

    @testing.requires.empty_inserts
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

    @testing.combinations(
        ((), {"z": 10}, [], {"z": 10}, testing.requires.legacy_engine),
    )
    def test_modify_parameters_from_event_one(
        self, multiparams, params, expected_multiparams, expected_params
    ):
        # this is testing both the normalization added to parameters
        # as of I97cb4d06adfcc6b889f10d01cc7775925cffb116 as well as
        # that the return value from the event is taken as the new set
        # of parameters.
        def before_execute(
            conn, clauseelement, multiparams, params, execution_options
        ):
            eq_(multiparams, expected_multiparams)
            eq_(params, expected_params)
            return clauseelement, (), {"q": "15"}

        def after_execute(
            conn, clauseelement, multiparams, params, result, execution_options
        ):
            eq_(multiparams, ())
            eq_(params, {"q": "15"})

        e1 = testing_engine(config.db_url)
        event.listen(e1, "before_execute", before_execute, retval=True)
        event.listen(e1, "after_execute", after_execute)

        with e1.connect() as conn:
            with testing.expect_deprecated_20(
                r"The connection\.execute\(\) method"
            ):
                result = conn.execute(
                    select(bindparam("q", type_=String)),
                    *multiparams,
                    **params
                )
            eq_(result.all(), [("15",)])

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
            result = e1.execute(select(1))
            result.close()


class DDLExecutionTest(fixtures.TestBase):
    def setup_test(self):
        self.engine = engines.mock_engine()
        self.metadata = MetaData()
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
        ddl_msg = r"The DDLElement.execute\(\) method is considered legacy"
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


class AutocommitKeywordFixture(object):
    def _test_keyword(self, keyword, expected=True):
        dbapi = Mock(
            connect=Mock(
                return_value=Mock(
                    cursor=Mock(return_value=Mock(description=()))
                )
            )
        )
        engine = engines.testing_engine(
            options={"_initialize": False, "pool_reset_on_return": None}
        )
        engine.dialect.dbapi = dbapi

        with engine.connect() as conn:
            if expected:
                with testing.expect_deprecated_20(
                    "The current statement is being autocommitted "
                    "using implicit autocommit"
                ):
                    conn.exec_driver_sql(
                        "%s something table something" % keyword
                    )
            else:
                conn.exec_driver_sql("%s something table something" % keyword)

            if expected:
                eq_(
                    [n for (n, k, s) in dbapi.connect().mock_calls],
                    ["cursor", "commit"],
                )
            else:
                eq_(
                    [n for (n, k, s) in dbapi.connect().mock_calls], ["cursor"]
                )


class AutocommitTextTest(AutocommitKeywordFixture, fixtures.TestBase):
    __backend__ = True

    def test_update(self):
        self._test_keyword("UPDATE")

    def test_insert(self):
        self._test_keyword("INSERT")

    def test_delete(self):
        self._test_keyword("DELETE")

    def test_alter(self):
        self._test_keyword("ALTER TABLE")

    def test_create(self):
        self._test_keyword("CREATE TABLE foobar")

    def test_drop(self):
        self._test_keyword("DROP TABLE foobar")

    def test_select(self):
        self._test_keyword("SELECT foo FROM table", False)


class ExplicitAutoCommitTest(fixtures.TablesTest):

    """test the 'autocommit' flag on select() and text() objects.

    Requires PostgreSQL so that we may define a custom function which
    modifies the database."""

    __only_on__ = "postgresql"

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "foo",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("data", String(100)),
        )

        event.listen(
            metadata,
            "after_create",
            DDL(
                "create function insert_foo(varchar) "
                "returns integer as 'insert into foo(data) "
                "values ($1);select 1;' language sql"
            ),
        )
        event.listen(
            metadata, "before_drop", DDL("drop function insert_foo(varchar)")
        )

    def test_control(self):

        # test that not using autocommit does not commit
        foo = self.tables.foo

        conn1 = testing.db.connect()
        conn2 = testing.db.connect()
        conn1.execute(select(func.insert_foo("data1")))
        assert conn2.execute(select(foo.c.data)).fetchall() == []
        conn1.execute(text("select insert_foo('moredata')"))
        assert conn2.execute(select(foo.c.data)).fetchall() == []
        trans = conn1.begin()
        trans.commit()
        assert conn2.execute(select(foo.c.data)).fetchall() == [
            ("data1",),
            ("moredata",),
        ]
        conn1.close()
        conn2.close()

    def test_explicit_compiled(self):
        foo = self.tables.foo

        conn1 = testing.db.connect()
        conn2 = testing.db.connect()

        with testing.expect_deprecated_20(
            "The current statement is being autocommitted using "
            "implicit autocommit"
        ):
            conn1.execute(
                select(func.insert_foo("data1")).execution_options(
                    autocommit=True
                )
            )
        assert conn2.execute(select(foo.c.data)).fetchall() == [("data1",)]
        conn1.close()
        conn2.close()

    def test_explicit_connection(self):
        foo = self.tables.foo

        conn1 = testing.db.connect()
        conn2 = testing.db.connect()
        with testing.expect_deprecated_20(
            "The current statement is being autocommitted using "
            "implicit autocommit"
        ):
            conn1.execution_options(autocommit=True).execute(
                select(func.insert_foo("data1"))
            )
        eq_(conn2.execute(select(foo.c.data)).fetchall(), [("data1",)])

        # connection supersedes statement

        conn1.execution_options(autocommit=False).execute(
            select(func.insert_foo("data2")).execution_options(autocommit=True)
        )
        eq_(conn2.execute(select(foo.c.data)).fetchall(), [("data1",)])

        # ditto

        with testing.expect_deprecated_20(
            "The current statement is being autocommitted using "
            "implicit autocommit"
        ):
            conn1.execution_options(autocommit=True).execute(
                select(func.insert_foo("data3")).execution_options(
                    autocommit=False
                )
            )
        eq_(
            conn2.execute(select(foo.c.data)).fetchall(),
            [("data1",), ("data2",), ("data3",)],
        )
        conn1.close()
        conn2.close()

    def test_explicit_text(self):
        foo = self.tables.foo

        conn1 = testing.db.connect()
        conn2 = testing.db.connect()
        with testing.expect_deprecated_20(
            "The current statement is being autocommitted using "
            "implicit autocommit"
        ):
            conn1.execute(
                text("select insert_foo('moredata')").execution_options(
                    autocommit=True
                )
            )
        assert conn2.execute(select(foo.c.data)).fetchall() == [("moredata",)]
        conn1.close()
        conn2.close()

    def test_implicit_text(self):
        foo = self.tables.foo

        conn1 = testing.db.connect()
        conn2 = testing.db.connect()
        with testing.expect_deprecated_20(
            "The current statement is being autocommitted using "
            "implicit autocommit"
        ):
            conn1.execute(
                text("insert into foo (data) values ('implicitdata')")
            )
        assert conn2.execute(select(foo.c.data)).fetchall() == [
            ("implicitdata",)
        ]
        conn1.close()
        conn2.close()
