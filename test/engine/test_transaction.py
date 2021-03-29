import sys

from sqlalchemy import event
from sqlalchemy import exc
from sqlalchemy import func
from sqlalchemy import INT
from sqlalchemy import MetaData
from sqlalchemy import pool as _pool
from sqlalchemy import select
from sqlalchemy import testing
from sqlalchemy import util
from sqlalchemy import VARCHAR
from sqlalchemy.engine import base
from sqlalchemy.engine import characteristics
from sqlalchemy.engine import default
from sqlalchemy.engine import url
from sqlalchemy.testing import assert_raises_message
from sqlalchemy.testing import eq_
from sqlalchemy.testing import expect_warnings
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import mock
from sqlalchemy.testing import ne_
from sqlalchemy.testing.assertions import expect_deprecated_20
from sqlalchemy.testing.assertions import expect_raises_message
from sqlalchemy.testing.engines import testing_engine
from sqlalchemy.testing.schema import Column
from sqlalchemy.testing.schema import Table


class TransactionTest(fixtures.TablesTest):
    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "users",
            metadata,
            Column("user_id", INT, primary_key=True),
            Column("user_name", VARCHAR(20)),
            test_needs_acid=True,
        )

    @testing.fixture
    def local_connection(self):
        with testing.db.connect() as conn:
            yield conn

    def test_commits(self, local_connection):
        users = self.tables.users
        connection = local_connection
        transaction = connection.begin()
        connection.execute(users.insert(), dict(user_id=1, user_name="user1"))
        transaction.commit()

        transaction = connection.begin()
        connection.execute(users.insert(), dict(user_id=2, user_name="user2"))
        connection.execute(users.insert(), dict(user_id=3, user_name="user3"))
        transaction.commit()

        transaction = connection.begin()
        result = connection.exec_driver_sql("select * from users")
        assert len(result.fetchall()) == 3
        transaction.commit()
        connection.close()

    def test_rollback(self, local_connection):
        """test a basic rollback"""

        users = self.tables.users
        connection = local_connection
        transaction = connection.begin()
        connection.execute(users.insert(), dict(user_id=1, user_name="user1"))
        connection.execute(users.insert(), dict(user_id=2, user_name="user2"))
        connection.execute(users.insert(), dict(user_id=3, user_name="user3"))
        transaction.rollback()
        result = connection.exec_driver_sql("select * from users")
        assert len(result.fetchall()) == 0

    def test_raise(self, local_connection):
        connection = local_connection
        users = self.tables.users

        transaction = connection.begin()
        try:
            connection.execute(
                users.insert(), dict(user_id=1, user_name="user1")
            )
            connection.execute(
                users.insert(), dict(user_id=2, user_name="user2")
            )
            connection.execute(
                users.insert(), dict(user_id=1, user_name="user3")
            )
            transaction.commit()
            assert False
        except Exception as e:
            print("Exception: ", e)
            transaction.rollback()

        result = connection.exec_driver_sql("select * from users")
        assert len(result.fetchall()) == 0

    def test_rollback_end_ctx_manager_autocommit(self, local_connection):
        m1 = mock.Mock()

        event.listen(local_connection, "rollback", m1.rollback)
        event.listen(local_connection, "commit", m1.commit)

        with local_connection.begin() as trans:
            assert local_connection.in_transaction()
            trans.rollback()
            assert not local_connection.in_transaction()

            # would be subject to autocommit
            local_connection.execute(select(1))

            assert not local_connection.in_transaction()

    @testing.combinations((True,), (False,), argnames="roll_back_in_block")
    def test_ctxmanager_rolls_back(self, local_connection, roll_back_in_block):
        m1 = mock.Mock()

        event.listen(local_connection, "rollback", m1.rollback)
        event.listen(local_connection, "commit", m1.commit)

        with expect_raises_message(Exception, "test"):
            with local_connection.begin() as trans:
                if roll_back_in_block:
                    trans.rollback()

                if 1 == 1:
                    raise Exception("test")

        assert not trans.is_active
        assert not local_connection.in_transaction()
        assert trans._deactivated_from_connection

        eq_(m1.mock_calls, [mock.call.rollback(local_connection)])

    @testing.combinations((True,), (False,), argnames="roll_back_in_block")
    def test_ctxmanager_rolls_back_legacy_marker(
        self, local_connection, roll_back_in_block
    ):
        m1 = mock.Mock()

        event.listen(local_connection, "rollback", m1.rollback)
        event.listen(local_connection, "commit", m1.commit)

        with expect_deprecated_20(
            r"Calling .begin\(\) when a transaction is already begun"
        ):
            with local_connection.begin() as trans:
                with expect_raises_message(Exception, "test"):
                    with local_connection.begin() as marker_trans:
                        if roll_back_in_block:
                            marker_trans.rollback()
                        if 1 == 1:
                            raise Exception("test")

                    assert not marker_trans.is_active
                    assert marker_trans._deactivated_from_connection

                assert not trans._deactivated_from_connection
                assert not trans.is_active
                assert not local_connection.in_transaction()

        eq_(m1.mock_calls, [mock.call.rollback(local_connection)])

    @testing.combinations((True,), (False,), argnames="roll_back_in_block")
    @testing.requires.savepoints
    def test_ctxmanager_rolls_back_savepoint(
        self, local_connection, roll_back_in_block
    ):
        m1 = mock.Mock()

        event.listen(
            local_connection, "rollback_savepoint", m1.rollback_savepoint
        )
        event.listen(local_connection, "rollback", m1.rollback)
        event.listen(local_connection, "commit", m1.commit)

        with local_connection.begin() as trans:
            with expect_raises_message(Exception, "test"):
                with local_connection.begin_nested() as nested_trans:
                    if roll_back_in_block:
                        nested_trans.rollback()
                    if 1 == 1:
                        raise Exception("test")

                assert not nested_trans.is_active
                assert nested_trans._deactivated_from_connection

            assert trans.is_active
            assert local_connection.in_transaction()
            assert not trans._deactivated_from_connection

        eq_(
            m1.mock_calls,
            [
                mock.call.rollback_savepoint(
                    local_connection, mock.ANY, mock.ANY
                ),
                mock.call.commit(local_connection),
            ],
        )

    def test_deactivated_warning_straight(self, local_connection):
        with expect_warnings(
            "transaction already deassociated from connection"
        ):
            trans = local_connection.begin()
            trans.rollback()
            trans.rollback()

    @testing.requires.savepoints
    def test_deactivated_savepoint_warning_straight(self, local_connection):
        with expect_warnings(
            "nested transaction already deassociated from connection"
        ):
            with local_connection.begin():
                savepoint = local_connection.begin_nested()
                savepoint.rollback()
                savepoint.rollback()

    def test_commit_fails_flat(self, local_connection):
        connection = local_connection

        t1 = connection.begin()

        with mock.patch.object(
            connection,
            "_commit_impl",
            mock.Mock(side_effect=exc.DBAPIError("failure", None, None, None)),
        ):
            assert_raises_message(exc.DBAPIError, r"failure", t1.commit)

        assert not t1.is_active
        t1.rollback()  # no error

    def test_commit_fails_ctxmanager(self, local_connection):
        connection = local_connection

        transaction = [None]

        def go():
            with mock.patch.object(
                connection,
                "_commit_impl",
                mock.Mock(
                    side_effect=exc.DBAPIError("failure", None, None, None)
                ),
            ):
                with connection.begin() as t1:
                    transaction[0] = t1

        assert_raises_message(exc.DBAPIError, r"failure", go)

        t1 = transaction[0]
        assert not t1.is_active
        with expect_warnings(
            "transaction already deassociated from connection"
        ):
            t1.rollback()  # no error

    @testing.requires.savepoints_w_release
    def test_savepoint_rollback_fails_flat(self, local_connection):
        connection = local_connection
        t1 = connection.begin()

        s1 = connection.begin_nested()

        # force the "commit" of the savepoint that occurs
        # when the "with" block fails, e.g.
        # the RELEASE, to fail, because the savepoint is already
        # released.
        connection.dialect.do_release_savepoint(connection, s1._savepoint)

        assert_raises_message(
            exc.DBAPIError, r".*SQL\:.*ROLLBACK TO SAVEPOINT", s1.rollback
        )

        assert not s1.is_active

        with testing.expect_warnings("nested transaction already"):
            s1.rollback()  # no error (though it warns)

        t1.commit()  # no error

    @testing.requires.savepoints_w_release
    def test_savepoint_release_fails_flat(self):
        with testing.db.connect() as connection:
            t1 = connection.begin()

            s1 = connection.begin_nested()

            # force the "commit" of the savepoint that occurs
            # when the "with" block fails, e.g.
            # the RELEASE, to fail, because the savepoint is already
            # released.
            connection.dialect.do_release_savepoint(connection, s1._savepoint)

            assert_raises_message(
                exc.DBAPIError, r".*SQL\:.*RELEASE SAVEPOINT", s1.commit
            )

            assert not s1.is_active
            s1.rollback()  # no error.  prior to 1.4 this would try to rollback

            t1.commit()  # no error

    @testing.requires.savepoints_w_release
    def test_savepoint_release_fails_ctxmanager(self, local_connection):
        connection = local_connection
        connection.begin()

        savepoint = [None]

        def go():

            with connection.begin_nested() as sp:
                savepoint[0] = sp
                # force the "commit" of the savepoint that occurs
                # when the "with" block fails, e.g.
                # the RELEASE, to fail, because the savepoint is already
                # released.
                connection.dialect.do_release_savepoint(
                    connection, sp._savepoint
                )

        # prior to SQLAlchemy 1.4, the above release would fail
        # and then the savepoint would try to rollback, and that failed
        # also, causing a long exception chain that under Python 2
        # was particularly hard to diagnose, leading to issue
        # #2696 which eventually impacted Openstack, and we
        # had to add warnings that show what the "context" for an
        # exception was.   The SQL for the exception was
        # ROLLBACK TO SAVEPOINT, and up the exception chain would be
        # the RELEASE failing.
        #
        # now, when the savepoint "commit" fails, it sets itself as
        # inactive.   so it does not try to rollback and it cleans
        # itself out appropriately.
        #

        exc_ = assert_raises_message(
            exc.DBAPIError, r".*SQL\:.*RELEASE SAVEPOINT", go
        )
        savepoint = savepoint[0]
        assert not savepoint.is_active

        if util.py3k:
            # ensure cause comes from the DBAPI
            assert isinstance(exc_.__cause__, testing.db.dialect.dbapi.Error)

    def test_retains_through_options(self, local_connection):
        connection = local_connection
        users = self.tables.users
        transaction = connection.begin()
        connection.execute(users.insert(), dict(user_id=1, user_name="user1"))
        conn2 = connection.execution_options(dummy=True)
        conn2.execute(users.insert(), dict(user_id=2, user_name="user2"))
        transaction.rollback()
        eq_(
            connection.exec_driver_sql("select count(*) from users").scalar(),
            0,
        )

    def test_with_interface(self, local_connection):
        connection = local_connection
        users = self.tables.users
        trans = connection.begin()
        connection.execute(users.insert(), dict(user_id=1, user_name="user1"))
        connection.execute(users.insert(), dict(user_id=2, user_name="user2"))
        try:
            connection.execute(
                users.insert(), dict(user_id=2, user_name="user2.5")
            )
        except Exception:
            trans.__exit__(*sys.exc_info())

        assert not trans.is_active
        self.assert_(
            connection.exec_driver_sql(
                "select count(*) from " "users"
            ).scalar()
            == 0
        )

        trans = connection.begin()
        connection.execute(users.insert(), dict(user_id=1, user_name="user1"))
        trans.__exit__(None, None, None)
        assert not trans.is_active
        self.assert_(
            connection.exec_driver_sql(
                "select count(*) from " "users"
            ).scalar()
            == 1
        )

    def test_close(self, local_connection):
        connection = local_connection
        users = self.tables.users
        transaction = connection.begin()
        connection.execute(users.insert(), dict(user_id=1, user_name="user1"))
        connection.execute(users.insert(), dict(user_id=2, user_name="user2"))
        connection.execute(users.insert(), dict(user_id=3, user_name="user3"))
        assert connection.in_transaction()
        transaction.commit()
        assert not connection.in_transaction()
        result = connection.exec_driver_sql("select * from users")
        eq_(len(result.fetchall()), 3)

    def test_close2(self, local_connection):
        connection = local_connection
        users = self.tables.users
        transaction = connection.begin()
        connection.execute(users.insert(), dict(user_id=1, user_name="user1"))
        connection.execute(users.insert(), dict(user_id=2, user_name="user2"))
        connection.execute(users.insert(), dict(user_id=3, user_name="user3"))
        assert connection.in_transaction()
        transaction.close()
        assert not connection.in_transaction()
        result = connection.exec_driver_sql("select * from users")
        assert len(result.fetchall()) == 0

    @testing.requires.savepoints
    def test_nested_subtransaction_rollback(self, local_connection):
        connection = local_connection
        users = self.tables.users
        transaction = connection.begin()
        connection.execute(users.insert(), dict(user_id=1, user_name="user1"))
        trans2 = connection.begin_nested()
        connection.execute(users.insert(), dict(user_id=2, user_name="user2"))
        trans2.rollback()
        connection.execute(users.insert(), dict(user_id=3, user_name="user3"))
        transaction.commit()
        eq_(
            connection.execute(
                select(users.c.user_id).order_by(users.c.user_id)
            ).fetchall(),
            [(1,), (3,)],
        )

    @testing.requires.savepoints
    def test_nested_subtransaction_commit(self, local_connection):
        connection = local_connection
        users = self.tables.users
        transaction = connection.begin()
        connection.execute(users.insert(), dict(user_id=1, user_name="user1"))
        trans2 = connection.begin_nested()
        connection.execute(users.insert(), dict(user_id=2, user_name="user2"))
        trans2.commit()
        connection.execute(users.insert(), dict(user_id=3, user_name="user3"))
        transaction.commit()
        eq_(
            connection.execute(
                select(users.c.user_id).order_by(users.c.user_id)
            ).fetchall(),
            [(1,), (2,), (3,)],
        )

    @testing.requires.two_phase_transactions
    def test_two_phase_transaction(self, local_connection):
        connection = local_connection
        users = self.tables.users
        transaction = connection.begin_twophase()
        connection.execute(users.insert(), dict(user_id=1, user_name="user1"))
        transaction.prepare()
        transaction.commit()
        transaction = connection.begin_twophase()
        connection.execute(users.insert(), dict(user_id=2, user_name="user2"))
        transaction.commit()
        transaction.close()
        transaction = connection.begin_twophase()
        connection.execute(users.insert(), dict(user_id=3, user_name="user3"))
        transaction.rollback()
        transaction = connection.begin_twophase()
        connection.execute(users.insert(), dict(user_id=4, user_name="user4"))
        transaction.prepare()
        transaction.rollback()
        transaction.close()
        eq_(
            connection.execute(
                select(users.c.user_id).order_by(users.c.user_id)
            ).fetchall(),
            [(1,), (2,)],
        )

    @testing.requires.two_phase_transactions
    @testing.requires.two_phase_recovery
    def test_two_phase_recover(self):
        users = self.tables.users

        # 2020, still can't get this to work w/ modern MySQL or MariaDB.
        # the XA RECOVER comes back as bytes, OK, convert to string,
        # XA COMMIT then says Unknown XID. Also, the drivers seem to be
        # killing off the XID if I use the connection.invalidate() before
        # trying to access in another connection.    Not really worth it
        # unless someone wants to step through how mysqlclient / pymysql
        # support this correctly.

        connection = testing.db.connect()

        transaction = connection.begin_twophase()
        connection.execute(
            users.insert(), dict(dict(user_id=1, user_name="user1"))
        )
        transaction.prepare()
        connection.invalidate()

        with testing.db.connect() as connection2:
            eq_(
                connection2.execute(
                    select(users.c.user_id).order_by(users.c.user_id)
                ).fetchall(),
                [],
            )
        # recover_twophase needs to be run in a new transaction
        with testing.db.connect() as connection2:
            recoverables = connection2.recover_twophase()
            assert transaction.xid in recoverables
            connection2.commit_prepared(transaction.xid, recover=True)

            eq_(
                connection2.execute(
                    select(users.c.user_id).order_by(users.c.user_id)
                ).fetchall(),
                [(1,)],
            )

    @testing.requires.two_phase_transactions
    def test_multiple_two_phase(self, local_connection):
        conn = local_connection
        users = self.tables.users
        xa = conn.begin_twophase()
        conn.execute(users.insert(), dict(user_id=1, user_name="user1"))
        xa.prepare()
        xa.commit()
        xa = conn.begin_twophase()
        conn.execute(users.insert(), dict(user_id=2, user_name="user2"))
        xa.prepare()
        xa.rollback()
        xa = conn.begin_twophase()
        conn.execute(users.insert(), dict(user_id=3, user_name="user3"))
        xa.rollback()
        xa = conn.begin_twophase()
        conn.execute(users.insert(), dict(user_id=4, user_name="user4"))
        xa.prepare()
        xa.commit()
        result = conn.execute(
            select(users.c.user_name).order_by(users.c.user_id)
        )
        eq_(result.fetchall(), [("user1",), ("user4",)])

    @testing.requires.two_phase_transactions
    def test_reset_rollback_two_phase_no_rollback(self):
        # test [ticket:2907], essentially that the
        # TwoPhaseTransaction is given the job of "reset on return"
        # so that picky backends like MySQL correctly clear out
        # their state when a connection is closed without handling
        # the transaction explicitly.
        users = self.tables.users

        eng = testing_engine()

        # MySQL raises if you call straight rollback() on
        # a connection with an XID present
        @event.listens_for(eng, "invalidate")
        def conn_invalidated(dbapi_con, con_record, exception):
            dbapi_con.close()
            raise exception

        with eng.connect() as conn:
            rec = conn.connection._connection_record
            raw_dbapi_con = rec.connection
            conn.begin_twophase()
            conn.execute(users.insert(), dict(user_id=1, user_name="user1"))

        assert rec.connection is raw_dbapi_con

        with eng.connect() as conn:
            result = conn.execute(
                select(users.c.user_name).order_by(users.c.user_id)
            )
            eq_(result.fetchall(), [])


class AutoRollbackTest(fixtures.TestBase):
    __backend__ = True

    @classmethod
    def setup_test_class(cls):
        global metadata
        metadata = MetaData()

    @classmethod
    def teardown_test_class(cls):
        metadata.drop_all(testing.db)

    def test_rollback_deadlock(self):
        """test that returning connections to the pool clears any object
        locks."""

        conn1 = testing.db.connect()
        conn2 = testing.db.connect()
        users = Table(
            "deadlock_users",
            metadata,
            Column("user_id", INT, primary_key=True),
            Column("user_name", VARCHAR(20)),
            test_needs_acid=True,
        )
        with conn1.begin():
            users.create(conn1)
        conn1.exec_driver_sql("select * from deadlock_users")
        conn1.close()

        # without auto-rollback in the connection pool's return() logic,
        # this deadlocks in PostgreSQL, because conn1 is returned to the
        # pool but still has a lock on "deadlock_users". comment out the
        # rollback in pool/ConnectionFairy._close() to see !

        with conn2.begin():
            users.drop(conn2)
        conn2.close()


class IsolationLevelTest(fixtures.TestBase):
    __requires__ = ("isolation_level", "ad_hoc_engines")
    __backend__ = True

    def _default_isolation_level(self):
        return testing.requires.get_isolation_levels(testing.config)["default"]

    def _non_default_isolation_level(self):
        levels = testing.requires.get_isolation_levels(testing.config)

        default = levels["default"]
        supported = levels["supported"]

        s = set(supported).difference(["AUTOCOMMIT", default])
        if s:
            return s.pop()
        else:
            assert False, "no non-default isolation level available"

    def test_engine_param_stays(self):

        eng = testing_engine()
        isolation_level = eng.dialect.get_isolation_level(
            eng.connect().connection
        )
        level = self._non_default_isolation_level()

        ne_(isolation_level, level)

        eng = testing_engine(options=dict(isolation_level=level))
        eq_(eng.dialect.get_isolation_level(eng.connect().connection), level)

        # check that it stays
        conn = eng.connect()
        eq_(eng.dialect.get_isolation_level(conn.connection), level)
        conn.close()

        conn = eng.connect()
        eq_(eng.dialect.get_isolation_level(conn.connection), level)
        conn.close()

    def test_default_level(self):
        eng = testing_engine(options=dict())
        isolation_level = eng.dialect.get_isolation_level(
            eng.connect().connection
        )
        eq_(isolation_level, self._default_isolation_level())

    def test_reset_level(self):
        eng = testing_engine(options=dict())
        conn = eng.connect()
        eq_(
            eng.dialect.get_isolation_level(conn.connection),
            self._default_isolation_level(),
        )

        eng.dialect.set_isolation_level(
            conn.connection, self._non_default_isolation_level()
        )
        eq_(
            eng.dialect.get_isolation_level(conn.connection),
            self._non_default_isolation_level(),
        )

        eng.dialect.reset_isolation_level(conn.connection)
        eq_(
            eng.dialect.get_isolation_level(conn.connection),
            self._default_isolation_level(),
        )

        conn.close()

    def test_reset_level_with_setting(self):
        eng = testing_engine(
            options=dict(isolation_level=self._non_default_isolation_level())
        )
        conn = eng.connect()
        eq_(
            eng.dialect.get_isolation_level(conn.connection),
            self._non_default_isolation_level(),
        )
        eng.dialect.set_isolation_level(
            conn.connection, self._default_isolation_level()
        )
        eq_(
            eng.dialect.get_isolation_level(conn.connection),
            self._default_isolation_level(),
        )
        eng.dialect.reset_isolation_level(conn.connection)
        eq_(
            eng.dialect.get_isolation_level(conn.connection),
            self._non_default_isolation_level(),
        )
        conn.close()

    def test_invalid_level(self):
        eng = testing_engine(options=dict(isolation_level="FOO"))
        assert_raises_message(
            exc.ArgumentError,
            "Invalid value '%s' for isolation_level. "
            "Valid isolation levels for %s are %s"
            % (
                "FOO",
                eng.dialect.name,
                ", ".join(eng.dialect._isolation_lookup),
            ),
            eng.connect,
        )

    def test_connection_invalidated(self):
        eng = testing_engine()
        conn = eng.connect()
        c2 = conn.execution_options(
            isolation_level=self._non_default_isolation_level()
        )
        c2.invalidate()
        c2.connection

        # TODO: do we want to rebuild the previous isolation?
        # for now, this is current behavior so we will leave it.
        eq_(c2.get_isolation_level(), self._default_isolation_level())

    def test_per_connection(self):
        from sqlalchemy.pool import QueuePool

        eng = testing_engine(
            options=dict(poolclass=QueuePool, pool_size=2, max_overflow=0)
        )

        c1 = eng.connect()
        c1 = c1.execution_options(
            isolation_level=self._non_default_isolation_level()
        )
        c2 = eng.connect()
        eq_(
            eng.dialect.get_isolation_level(c1.connection),
            self._non_default_isolation_level(),
        )
        eq_(
            eng.dialect.get_isolation_level(c2.connection),
            self._default_isolation_level(),
        )
        c1.close()
        c2.close()
        c3 = eng.connect()
        eq_(
            eng.dialect.get_isolation_level(c3.connection),
            self._default_isolation_level(),
        )
        c4 = eng.connect()
        eq_(
            eng.dialect.get_isolation_level(c4.connection),
            self._default_isolation_level(),
        )

        c3.close()
        c4.close()

    def test_warning_in_transaction(self):
        eng = testing_engine()
        c1 = eng.connect()
        with expect_warnings(
            "Connection is already established with a Transaction; "
            "setting isolation_level may implicitly rollback or commit "
            "the existing transaction, or have no effect until next "
            "transaction"
        ):
            with c1.begin():
                c1 = c1.execution_options(
                    isolation_level=self._non_default_isolation_level()
                )

                eq_(
                    eng.dialect.get_isolation_level(c1.connection),
                    self._non_default_isolation_level(),
                )
        # stays outside of transaction
        eq_(
            eng.dialect.get_isolation_level(c1.connection),
            self._non_default_isolation_level(),
        )

    def test_per_statement_bzzt(self):
        assert_raises_message(
            exc.ArgumentError,
            r"'isolation_level' execution option may only be specified "
            r"on Connection.execution_options\(\), or "
            r"per-engine using the isolation_level "
            r"argument to create_engine\(\).",
            select(1).execution_options,
            isolation_level=self._non_default_isolation_level(),
        )

    def test_per_engine(self):
        # new in 0.9
        eng = testing_engine(
            testing.db.url,
            options=dict(
                execution_options={
                    "isolation_level": self._non_default_isolation_level()
                }
            ),
        )
        conn = eng.connect()
        eq_(
            eng.dialect.get_isolation_level(conn.connection),
            self._non_default_isolation_level(),
        )

    def test_per_option_engine(self):
        eng = testing_engine(testing.db.url).execution_options(
            isolation_level=self._non_default_isolation_level()
        )

        conn = eng.connect()
        eq_(
            eng.dialect.get_isolation_level(conn.connection),
            self._non_default_isolation_level(),
        )

    def test_isolation_level_accessors_connection_default(self):
        eng = testing_engine(testing.db.url)
        with eng.connect() as conn:
            eq_(conn.default_isolation_level, self._default_isolation_level())
        with eng.connect() as conn:
            eq_(conn.get_isolation_level(), self._default_isolation_level())

    def test_isolation_level_accessors_connection_option_modified(self):
        eng = testing_engine(testing.db.url)
        with eng.connect() as conn:
            c2 = conn.execution_options(
                isolation_level=self._non_default_isolation_level()
            )
            eq_(conn.default_isolation_level, self._default_isolation_level())
            eq_(
                conn.get_isolation_level(), self._non_default_isolation_level()
            )
            eq_(c2.get_isolation_level(), self._non_default_isolation_level())


class ConnectionCharacteristicTest(fixtures.TestBase):
    @testing.fixture
    def characteristic_fixture(self):
        class FooCharacteristic(characteristics.ConnectionCharacteristic):
            transactional = True

            def reset_characteristic(self, dialect, dbapi_conn):

                dialect.reset_foo(dbapi_conn)

            def set_characteristic(self, dialect, dbapi_conn, value):

                dialect.set_foo(dbapi_conn, value)

            def get_characteristic(self, dialect, dbapi_conn):
                return dialect.get_foo(dbapi_conn)

        class FooDialect(default.DefaultDialect):
            connection_characteristics = util.immutabledict(
                {"foo": FooCharacteristic()}
            )

            def reset_foo(self, dbapi_conn):
                dbapi_conn.foo = "original_value"

            def set_foo(self, dbapi_conn, value):
                dbapi_conn.foo = value

            def get_foo(self, dbapi_conn):
                return dbapi_conn.foo

        connection = mock.Mock()

        def creator():
            connection.foo = "original_value"
            return connection

        pool = _pool.SingletonThreadPool(creator=creator)
        u = url.make_url("foo://")
        return base.Engine(pool, FooDialect(), u), connection

    def test_engine_param_stays(self, characteristic_fixture):

        engine, connection = characteristic_fixture

        foo_level = engine.dialect.get_foo(engine.connect().connection)

        new_level = "new_level"

        ne_(foo_level, new_level)

        eng = engine.execution_options(foo=new_level)
        eq_(eng.dialect.get_foo(eng.connect().connection), new_level)

        # check that it stays
        conn = eng.connect()
        eq_(eng.dialect.get_foo(conn.connection), new_level)
        conn.close()

        conn = eng.connect()
        eq_(eng.dialect.get_foo(conn.connection), new_level)
        conn.close()

    def test_default_level(self, characteristic_fixture):
        engine, connection = characteristic_fixture

        eq_(
            engine.dialect.get_foo(engine.connect().connection),
            "original_value",
        )

    def test_connection_invalidated(self, characteristic_fixture):
        engine, connection = characteristic_fixture

        conn = engine.connect()
        c2 = conn.execution_options(foo="new_value")
        eq_(connection.foo, "new_value")
        c2.invalidate()
        c2.connection

        eq_(connection.foo, "original_value")

    def test_warning_in_transaction(self, characteristic_fixture):
        engine, connection = characteristic_fixture

        c1 = engine.connect()
        with expect_warnings(
            "Connection is already established with a Transaction; "
            "setting foo may implicitly rollback or commit "
            "the existing transaction, or have no effect until next "
            "transaction"
        ):
            with c1.begin():
                c1 = c1.execution_options(foo="new_foo")

                eq_(
                    engine.dialect.get_foo(c1.connection),
                    "new_foo",
                )
        # stays outside of transaction
        eq_(engine.dialect.get_foo(c1.connection), "new_foo")

    @testing.fails("no error is raised yet here.")
    def test_per_statement_bzzt(self, characteristic_fixture):
        engine, connection = characteristic_fixture

        # this would need some on-execute mechanism to look inside of
        # the characteristics list.   unfortunately this would
        # add some latency.

        assert_raises_message(
            exc.ArgumentError,
            r"'foo' execution option may only be specified "
            r"on Connection.execution_options\(\), or "
            r"per-engine using the isolation_level "
            r"argument to create_engine\(\).",
            connection.execute,
            select([1]).execution_options(foo="bar"),
        )

    def test_per_engine(self, characteristic_fixture):

        engine, connection = characteristic_fixture

        pool, dialect, url = engine.pool, engine.dialect, engine.url

        eng = base.Engine(
            pool, dialect, url, execution_options={"foo": "new_value"}
        )

        conn = eng.connect()
        eq_(eng.dialect.get_foo(conn.connection), "new_value")

    def test_per_option_engine(self, characteristic_fixture):

        engine, connection = characteristic_fixture

        eng = engine.execution_options(foo="new_value")

        conn = eng.connect()
        eq_(
            eng.dialect.get_foo(conn.connection),
            "new_value",
        )


class ResetFixture(object):
    @testing.fixture()
    def reset_agent(self, testing_engine):
        engine = testing_engine()
        engine.connect().close()

        harness = mock.Mock(
            do_rollback=mock.Mock(side_effect=testing.db.dialect.do_rollback),
            do_commit=mock.Mock(side_effect=testing.db.dialect.do_commit),
            engine=engine,
        )
        event.listen(engine, "rollback", harness.rollback)
        event.listen(engine, "commit", harness.commit)
        event.listen(engine, "rollback_savepoint", harness.rollback_savepoint)
        event.listen(engine, "rollback_twophase", harness.rollback_twophase)
        event.listen(engine, "commit_twophase", harness.commit_twophase)

        with mock.patch.object(
            engine.dialect, "do_rollback", harness.do_rollback
        ), mock.patch.object(engine.dialect, "do_commit", harness.do_commit):
            yield harness

        event.remove(engine, "rollback", harness.rollback)
        event.remove(engine, "commit", harness.commit)
        event.remove(engine, "rollback_savepoint", harness.rollback_savepoint)
        event.remove(engine, "rollback_twophase", harness.rollback_twophase)
        event.remove(engine, "commit_twophase", harness.commit_twophase)


class ResetAgentTest(ResetFixture, fixtures.TestBase):
    # many of these tests illustate rollback-on-return being redundant
    # vs. what the transaction just did, however this is to ensure
    # even if statements were invoked on the DBAPI connection directly,
    # the state is cleared.    options to optimize this with clear
    # docs etc. should be added.

    __backend__ = True

    def test_begin_close(self, reset_agent):
        with reset_agent.engine.connect() as connection:
            trans = connection.begin()
        assert not trans.is_active
        eq_(
            reset_agent.mock_calls,
            [mock.call.rollback(connection), mock.call.do_rollback(mock.ANY)],
        )

    def test_begin_rollback(self, reset_agent):
        with reset_agent.engine.connect() as connection:
            trans = connection.begin()
            trans.rollback()
        eq_(
            reset_agent.mock_calls,
            [
                mock.call.rollback(connection),
                mock.call.do_rollback(mock.ANY),
                mock.call.do_rollback(mock.ANY),
            ],
        )

    def test_begin_commit(self, reset_agent):
        with reset_agent.engine.connect() as connection:
            trans = connection.begin()
            trans.commit()
        eq_(
            reset_agent.mock_calls,
            [
                mock.call.commit(connection),
                mock.call.do_commit(mock.ANY),
                mock.call.do_rollback(mock.ANY),
            ],
        )

    def test_trans_close(self, reset_agent):
        with reset_agent.engine.connect() as connection:
            trans = connection.begin()
            trans.close()
        eq_(
            reset_agent.mock_calls,
            [
                mock.call.rollback(connection),
                mock.call.do_rollback(mock.ANY),
                mock.call.do_rollback(mock.ANY),
            ],
        )

    @testing.requires.savepoints
    def test_begin_nested_trans_close_one(self, reset_agent):
        with reset_agent.engine.connect() as connection:
            t1 = connection.begin()
            t2 = connection.begin_nested()
            assert connection._nested_transaction is t2
            assert connection._transaction is t1
            t2.close()
            assert connection._nested_transaction is None
            assert connection._transaction is t1
            t1.close()
        assert not t1.is_active
        eq_(
            reset_agent.mock_calls,
            [
                mock.call.rollback_savepoint(connection, mock.ANY, mock.ANY),
                mock.call.rollback(connection),
                mock.call.do_rollback(mock.ANY),
                mock.call.do_rollback(mock.ANY),
            ],
        )

    @testing.requires.savepoints
    def test_begin_nested_trans_close_two(self, reset_agent):
        with reset_agent.engine.connect() as connection:
            t1 = connection.begin()
            t2 = connection.begin_nested()
            assert connection._nested_transaction is t2
            assert connection._transaction is t1

            t1.close()

            assert connection._nested_transaction is None
            assert connection._transaction is None

        assert not t1.is_active
        eq_(
            reset_agent.mock_calls,
            [
                mock.call.rollback(connection),
                mock.call.do_rollback(mock.ANY),
                mock.call.do_rollback(mock.ANY),
            ],
        )

    @testing.requires.savepoints
    def test_begin_nested_trans_rollback(self, reset_agent):
        with reset_agent.engine.connect() as connection:
            t1 = connection.begin()
            t2 = connection.begin_nested()
            assert connection._nested_transaction is t2
            assert connection._transaction is t1
            t2.close()
            assert connection._nested_transaction is None
            assert connection._transaction is t1
            t1.rollback()
            assert connection._transaction is None
        assert not t2.is_active
        assert not t1.is_active
        eq_(
            reset_agent.mock_calls,
            [
                mock.call.rollback_savepoint(connection, mock.ANY, mock.ANY),
                mock.call.rollback(connection),
                mock.call.do_rollback(mock.ANY),
                mock.call.do_rollback(mock.ANY),
            ],
        )

    @testing.requires.savepoints
    def test_begin_nested_close(self, reset_agent):
        with reset_agent.engine.connect() as connection:
            trans = connection.begin_nested()
        assert not trans.is_active
        eq_(
            reset_agent.mock_calls,
            [
                mock.call.rollback(connection),
                mock.call.do_rollback(mock.ANY),
            ],
        )

    @testing.requires.savepoints
    def test_begin_begin_nested_close(self, reset_agent):
        with reset_agent.engine.connect() as connection:
            trans = connection.begin()
            trans2 = connection.begin_nested()
        assert not trans2.is_active
        assert not trans.is_active
        eq_(
            reset_agent.mock_calls,
            [
                mock.call.rollback(connection),
                mock.call.do_rollback(mock.ANY),
            ],
        )

    @testing.requires.savepoints
    def test_begin_begin_nested_rollback_commit(self, reset_agent):
        with reset_agent.engine.connect() as connection:
            trans = connection.begin()
            trans2 = connection.begin_nested()
            trans2.rollback()
            trans.commit()
        eq_(
            reset_agent.mock_calls,
            [
                mock.call.rollback_savepoint(connection, mock.ANY, mock.ANY),
                mock.call.commit(connection),
                mock.call.do_commit(mock.ANY),
                mock.call.do_rollback(mock.ANY),
            ],
        )

    @testing.requires.savepoints
    def test_begin_begin_nested_rollback_rollback(self, reset_agent):
        with reset_agent.engine.connect() as connection:
            trans = connection.begin()
            trans2 = connection.begin_nested()
            trans2.rollback()
            trans.rollback()
        eq_(
            reset_agent.mock_calls,
            [
                mock.call.rollback_savepoint(connection, mock.ANY, mock.ANY),
                mock.call.rollback(connection),
                mock.call.do_rollback(mock.ANY),
                mock.call.do_rollback(mock.ANY),
            ],
        )

    @testing.requires.two_phase_transactions
    def test_reset_via_agent_begin_twophase(self, reset_agent):
        with reset_agent.engine.connect() as connection:
            trans = connection.begin_twophase()  # noqa

        # pg8000 rolls back via the rollback_twophase
        eq_(
            reset_agent.mock_calls[0],
            mock.call.rollback_twophase(connection, mock.ANY, mock.ANY),
        )

    @testing.requires.two_phase_transactions
    def test_reset_via_agent_begin_twophase_commit(self, reset_agent):
        with reset_agent.engine.connect() as connection:
            trans = connection.begin_twophase()
            trans.commit()
        eq_(
            reset_agent.mock_calls[0],
            mock.call.commit_twophase(connection, mock.ANY, mock.ANY),
        )

        eq_(reset_agent.mock_calls[-1], mock.call.do_rollback(mock.ANY))

    @testing.requires.two_phase_transactions
    def test_reset_via_agent_begin_twophase_rollback(self, reset_agent):
        with reset_agent.engine.connect() as connection:
            trans = connection.begin_twophase()
            trans.rollback()
        eq_(
            reset_agent.mock_calls[0:2],
            [
                mock.call.rollback_twophase(connection, mock.ANY, mock.ANY),
                mock.call.do_rollback(mock.ANY),
            ],
        )

        eq_(reset_agent.mock_calls[-1], mock.call.do_rollback(mock.ANY))


class FutureResetAgentTest(
    ResetFixture, fixtures.FutureEngineMixin, fixtures.TestBase
):

    __backend__ = True

    def test_reset_agent_no_conn_transaction(self, reset_agent):
        with reset_agent.engine.connect():
            pass

        eq_(reset_agent.mock_calls, [mock.call.do_rollback(mock.ANY)])

    def test_begin_close(self, reset_agent):
        with reset_agent.engine.connect() as connection:
            trans = connection.begin()

        assert not trans.is_active
        eq_(
            reset_agent.mock_calls,
            [mock.call.rollback(connection), mock.call.do_rollback(mock.ANY)],
        )

    def test_begin_rollback(self, reset_agent):
        with reset_agent.engine.connect() as connection:
            trans = connection.begin()
            trans.rollback()
        assert not trans.is_active
        eq_(
            reset_agent.mock_calls,
            [
                mock.call.rollback(connection),
                mock.call.do_rollback(mock.ANY),
                mock.call.do_rollback(mock.ANY),
            ],
        )

    def test_begin_commit(self, reset_agent):
        with reset_agent.engine.connect() as connection:
            trans = connection.begin()
            trans.commit()
        assert not trans.is_active
        eq_(
            reset_agent.mock_calls,
            [
                mock.call.commit(connection),
                mock.call.do_commit(mock.ANY),
                mock.call.do_rollback(mock.ANY),
            ],
        )

    @testing.requires.savepoints
    def test_begin_nested_close(self, reset_agent):
        with reset_agent.engine.connect() as connection:
            trans = connection.begin_nested()
        # it's a savepoint, but root made sure it closed
        assert not trans.is_active
        eq_(
            reset_agent.mock_calls,
            [mock.call.rollback(connection), mock.call.do_rollback(mock.ANY)],
        )

    @testing.requires.savepoints
    def test_begin_begin_nested_close(self, reset_agent):
        with reset_agent.engine.connect() as connection:
            trans = connection.begin()
            trans2 = connection.begin_nested()
        assert not trans2.is_active
        assert not trans.is_active
        eq_(
            reset_agent.mock_calls,
            [mock.call.rollback(connection), mock.call.do_rollback(mock.ANY)],
        )

    @testing.requires.savepoints
    def test_begin_begin_nested_rollback_commit(self, reset_agent):
        with reset_agent.engine.connect() as connection:
            trans = connection.begin()
            trans2 = connection.begin_nested()
            trans2.rollback()  # this is not a connection level event
            trans.commit()
        eq_(
            reset_agent.mock_calls,
            [
                mock.call.rollback_savepoint(connection, mock.ANY, None),
                mock.call.commit(connection),
                mock.call.do_commit(mock.ANY),
                mock.call.do_rollback(mock.ANY),
            ],
        )

    @testing.requires.savepoints
    def test_begin_begin_nested_rollback_rollback(self, reset_agent):
        with reset_agent.engine.connect() as connection:
            trans = connection.begin()
            trans2 = connection.begin_nested()
            trans2.rollback()
            trans.rollback()
        eq_(
            reset_agent.mock_calls,
            [
                mock.call.rollback_savepoint(connection, mock.ANY, mock.ANY),
                mock.call.rollback(connection),
                mock.call.do_rollback(mock.ANY),
                mock.call.do_rollback(mock.ANY),  # this is the reset on return
            ],
        )

    @testing.requires.two_phase_transactions
    def test_reset_via_agent_begin_twophase(self, reset_agent):
        with reset_agent.engine.connect() as connection:
            trans = connection.begin_twophase()
        assert not trans.is_active
        # pg8000 uses the rollback_twophase as the full rollback.
        eq_(
            reset_agent.mock_calls[0],
            mock.call.rollback_twophase(connection, mock.ANY, False),
        )

    @testing.requires.two_phase_transactions
    def test_reset_via_agent_begin_twophase_commit(self, reset_agent):
        with reset_agent.engine.connect() as connection:
            trans = connection.begin_twophase()
            trans.commit()

        # again pg8000 vs. other PG drivers have different API
        eq_(
            reset_agent.mock_calls[0],
            mock.call.commit_twophase(connection, mock.ANY, False),
        )

        eq_(reset_agent.mock_calls[-1], mock.call.do_rollback(mock.ANY))

    @testing.requires.two_phase_transactions
    def test_reset_via_agent_begin_twophase_rollback(self, reset_agent):
        with reset_agent.engine.connect() as connection:
            trans = connection.begin_twophase()
            trans.rollback()

        # pg8000 vs. the other postgresql drivers have different
        # twophase implementations.  the base postgresql driver emits
        # "ROLLBACK PREPARED" explicitly then calls do_rollback().
        # pg8000 has a dedicated API method.  so we get either one or
        # two do_rollback() at the end, just need at least one.
        eq_(
            reset_agent.mock_calls[0:2],
            [
                mock.call.rollback_twophase(connection, mock.ANY, False),
                mock.call.do_rollback(mock.ANY),
                # mock.call.do_rollback(mock.ANY),
            ],
        )
        eq_(reset_agent.mock_calls[-1], mock.call.do_rollback(mock.ANY))


class FutureTransactionTest(fixtures.FutureEngineMixin, fixtures.TablesTest):
    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "users",
            metadata,
            Column("user_id", INT, primary_key=True, autoincrement=False),
            Column("user_name", VARCHAR(20)),
            test_needs_acid=True,
        )
        Table(
            "users_autoinc",
            metadata,
            Column(
                "user_id", INT, primary_key=True, test_needs_autoincrement=True
            ),
            Column("user_name", VARCHAR(20)),
            test_needs_acid=True,
        )

    @testing.fixture
    def local_connection(self):
        with testing.db.connect() as conn:
            yield conn

    def test_autobegin_rollback(self):
        users = self.tables.users
        with testing.db.connect() as conn:
            conn.execute(users.insert(), {"user_id": 1, "user_name": "name"})
            conn.rollback()

            eq_(conn.scalar(select(func.count(1)).select_from(users)), 0)

    @testing.requires.autocommit
    def test_autocommit_isolation_level(self):
        users = self.tables.users

        with testing.db.connect().execution_options(
            isolation_level="AUTOCOMMIT"
        ) as conn:
            conn.execute(users.insert(), {"user_id": 1, "user_name": "name"})
            conn.rollback()

        with testing.db.connect() as conn:
            eq_(
                conn.scalar(select(func.count(1)).select_from(users)),
                1,
            )

    @testing.requires.autocommit
    def test_no_autocommit_w_begin(self):

        with testing.db.begin() as conn:
            assert_raises_message(
                exc.InvalidRequestError,
                "This connection has already begun a transaction; "
                "isolation_level may not be altered until transaction end",
                conn.execution_options,
                isolation_level="AUTOCOMMIT",
            )

    @testing.requires.autocommit
    def test_no_autocommit_w_autobegin(self):

        with testing.db.connect() as conn:
            conn.execute(select(1))

            assert_raises_message(
                exc.InvalidRequestError,
                "This connection has already begun a transaction; "
                "isolation_level may not be altered until transaction end",
                conn.execution_options,
                isolation_level="AUTOCOMMIT",
            )

            conn.rollback()

            conn.execution_options(isolation_level="AUTOCOMMIT")

    def test_autobegin_commit(self):
        users = self.tables.users

        with testing.db.connect() as conn:

            assert not conn.in_transaction()
            conn.execute(users.insert(), {"user_id": 1, "user_name": "name"})

            assert conn.in_transaction()
            conn.commit()

            assert not conn.in_transaction()

            eq_(
                conn.scalar(select(func.count(1)).select_from(users)),
                1,
            )

            conn.execute(users.insert(), {"user_id": 2, "user_name": "name 2"})

            eq_(
                conn.scalar(select(func.count(1)).select_from(users)),
                2,
            )

            assert conn.in_transaction()
            conn.rollback()
            assert not conn.in_transaction()

            eq_(
                conn.scalar(select(func.count(1)).select_from(users)),
                1,
            )

    def test_rollback_on_close(self):
        canary = mock.Mock()
        with testing.db.connect() as conn:
            event.listen(conn, "rollback", canary)
            conn.execute(select(1))
            assert conn.in_transaction()

        eq_(canary.mock_calls, [mock.call(conn)])

    def test_no_on_close_no_transaction(self):
        canary = mock.Mock()
        with testing.db.connect() as conn:
            event.listen(conn, "rollback", canary)
            conn.execute(select(1))
            conn.rollback()
            assert not conn.in_transaction()

        eq_(canary.mock_calls, [mock.call(conn)])

    def test_rollback_on_exception(self):
        canary = mock.Mock()
        try:
            with testing.db.connect() as conn:
                event.listen(conn, "rollback", canary)
                conn.execute(select(1))
                assert conn.in_transaction()
                raise Exception("some error")
            assert False
        except:
            pass

        eq_(canary.mock_calls, [mock.call(conn)])

    def test_rollback_on_exception_if_no_trans(self):
        canary = mock.Mock()
        try:
            with testing.db.connect() as conn:
                event.listen(conn, "rollback", canary)
                assert not conn.in_transaction()
                raise Exception("some error")
            assert False
        except:
            pass

        eq_(canary.mock_calls, [])

    def test_commit_no_begin(self):
        with testing.db.connect() as conn:
            assert not conn.in_transaction()
            conn.commit()

    @testing.requires.independent_connections
    def test_commit_inactive(self):
        with testing.db.connect() as conn:
            conn.begin()
            conn.invalidate()

            assert_raises_message(
                exc.InvalidRequestError, "Can't reconnect until", conn.commit
            )

    @testing.requires.independent_connections
    def test_rollback_inactive(self):
        users = self.tables.users
        with testing.db.connect() as conn:

            conn.execute(users.insert(), {"user_id": 1, "user_name": "name"})
            conn.commit()

            conn.execute(users.insert(), {"user_id": 2, "user_name": "name2"})

            conn.invalidate()

            assert_raises_message(
                exc.PendingRollbackError,
                "Can't reconnect",
                conn.execute,
                select(1),
            )

            conn.rollback()
            eq_(
                conn.scalar(select(func.count(1)).select_from(users)),
                1,
            )

    def test_rollback_no_begin(self):
        with testing.db.connect() as conn:
            assert not conn.in_transaction()
            conn.rollback()

    def test_rollback_end_ctx_manager(self):
        with testing.db.begin() as conn:
            assert conn.in_transaction()
            conn.rollback()
            assert not conn.in_transaction()

    def test_rollback_end_ctx_manager_autobegin(self, local_connection):
        m1 = mock.Mock()

        event.listen(local_connection, "rollback", m1.rollback)
        event.listen(local_connection, "commit", m1.commit)

        with local_connection.begin() as trans:
            assert local_connection.in_transaction()
            trans.rollback()
            assert not local_connection.in_transaction()

            # autobegin
            local_connection.execute(select(1))

            assert local_connection.in_transaction()

    @testing.combinations((True,), (False,), argnames="roll_back_in_block")
    def test_ctxmanager_rolls_back(self, local_connection, roll_back_in_block):
        m1 = mock.Mock()

        event.listen(local_connection, "rollback", m1.rollback)
        event.listen(local_connection, "commit", m1.commit)

        with expect_raises_message(Exception, "test"):
            with local_connection.begin() as trans:
                if roll_back_in_block:
                    trans.rollback()

                if 1 == 1:
                    raise Exception("test")

        assert not trans.is_active
        assert not local_connection.in_transaction()
        assert trans._deactivated_from_connection

        eq_(m1.mock_calls, [mock.call.rollback(local_connection)])

    def test_explicit_begin(self):
        users = self.tables.users

        with testing.db.connect() as conn:
            assert not conn.in_transaction()
            conn.begin()
            assert conn.in_transaction()
            conn.execute(users.insert(), {"user_id": 1, "user_name": "name"})
            conn.commit()

            eq_(
                conn.scalar(select(func.count(1)).select_from(users)),
                1,
            )

    def test_no_double_begin(self):
        with testing.db.connect() as conn:
            conn.begin()

            assert_raises_message(
                exc.InvalidRequestError,
                "a transaction is already begun for this connection",
                conn.begin,
            )

    def test_no_autocommit(self):
        users = self.tables.users

        with testing.db.connect() as conn:
            conn.execute(users.insert(), {"user_id": 1, "user_name": "name"})

        with testing.db.connect() as conn:
            eq_(
                conn.scalar(select(func.count(1)).select_from(users)),
                0,
            )

    def test_begin_block(self):
        users = self.tables.users

        with testing.db.begin() as conn:
            conn.execute(users.insert(), {"user_id": 1, "user_name": "name"})

        with testing.db.connect() as conn:
            eq_(
                conn.scalar(select(func.count(1)).select_from(users)),
                1,
            )

    @testing.requires.savepoints
    def test_savepoint_one(self):
        users = self.tables.users

        with testing.db.begin() as conn:
            conn.execute(users.insert(), {"user_id": 1, "user_name": "name"})

            savepoint = conn.begin_nested()
            conn.execute(users.insert(), {"user_id": 2, "user_name": "name2"})

            eq_(
                conn.scalar(select(func.count(1)).select_from(users)),
                2,
            )
            savepoint.rollback()

            eq_(
                conn.scalar(select(func.count(1)).select_from(users)),
                1,
            )

        with testing.db.connect() as conn:
            eq_(
                conn.scalar(select(func.count(1)).select_from(users)),
                1,
            )

    @testing.requires.savepoints
    def test_savepoint_two(self):
        users = self.tables.users

        with testing.db.begin() as conn:
            conn.execute(users.insert(), {"user_id": 1, "user_name": "name"})

            savepoint = conn.begin_nested()
            conn.execute(users.insert(), {"user_id": 2, "user_name": "name2"})

            eq_(
                conn.scalar(select(func.count(1)).select_from(users)),
                2,
            )
            savepoint.commit()

            eq_(
                conn.scalar(select(func.count(1)).select_from(users)),
                2,
            )

        with testing.db.connect() as conn:
            eq_(
                conn.scalar(select(func.count(1)).select_from(users)),
                2,
            )

    @testing.requires.savepoints
    def test_savepoint_three(self):
        users = self.tables.users

        with testing.db.begin() as conn:
            conn.execute(users.insert(), {"user_id": 1, "user_name": "name"})

            conn.begin_nested()
            conn.execute(users.insert(), {"user_id": 2, "user_name": "name2"})

            conn.rollback()

            assert not conn.in_transaction()

        with testing.db.connect() as conn:
            eq_(
                conn.scalar(select(func.count(1)).select_from(users)),
                0,
            )

    @testing.requires.savepoints
    def test_savepoint_four(self):
        users = self.tables.users

        with testing.db.begin() as conn:
            conn.execute(users.insert(), {"user_id": 1, "user_name": "name"})

            sp1 = conn.begin_nested()
            conn.execute(users.insert(), {"user_id": 2, "user_name": "name2"})

            sp2 = conn.begin_nested()
            conn.execute(users.insert(), {"user_id": 3, "user_name": "name3"})

            sp2.rollback()

            assert not sp2.is_active
            assert sp1.is_active
            assert conn.in_transaction()

        assert not sp1.is_active

        with testing.db.connect() as conn:
            eq_(
                conn.scalar(select(func.count(1)).select_from(users)),
                2,
            )

    @testing.requires.savepoints
    def test_savepoint_five(self):
        users = self.tables.users

        with testing.db.begin() as conn:
            conn.execute(users.insert(), {"user_id": 1, "user_name": "name"})

            conn.begin_nested()
            conn.execute(users.insert(), {"user_id": 2, "user_name": "name2"})

            sp2 = conn.begin_nested()
            conn.execute(users.insert(), {"user_id": 3, "user_name": "name3"})

            sp2.commit()

            assert conn.in_transaction()

        with testing.db.connect() as conn:
            eq_(
                conn.scalar(select(func.count(1)).select_from(users)),
                3,
            )

    @testing.requires.savepoints
    def test_savepoint_six(self):
        users = self.tables.users

        with testing.db.begin() as conn:
            conn.execute(users.insert(), {"user_id": 1, "user_name": "name"})

            sp1 = conn.begin_nested()
            conn.execute(users.insert(), {"user_id": 2, "user_name": "name2"})

            assert conn._nested_transaction is sp1

            sp2 = conn.begin_nested()
            conn.execute(users.insert(), {"user_id": 3, "user_name": "name3"})

            assert conn._nested_transaction is sp2

            sp2.commit()

            assert conn._nested_transaction is sp1

            sp1.rollback()

            assert conn._nested_transaction is None

            assert conn.in_transaction()

        with testing.db.connect() as conn:
            eq_(
                conn.scalar(select(func.count(1)).select_from(users)),
                1,
            )

    @testing.requires.savepoints
    def test_savepoint_seven(self):
        users = self.tables.users

        conn = testing.db.connect()
        trans = conn.begin()
        conn.execute(users.insert(), {"user_id": 1, "user_name": "name"})

        sp1 = conn.begin_nested()
        conn.execute(users.insert(), {"user_id": 2, "user_name": "name2"})

        sp2 = conn.begin_nested()
        conn.execute(users.insert(), {"user_id": 3, "user_name": "name3"})

        assert conn.in_transaction()

        trans.close()

        assert not sp1.is_active
        assert not sp2.is_active
        assert not trans.is_active
        assert conn._transaction is None
        assert conn._nested_transaction is None

        with testing.db.connect() as conn:
            eq_(
                conn.scalar(select(func.count(1)).select_from(users)),
                0,
            )
