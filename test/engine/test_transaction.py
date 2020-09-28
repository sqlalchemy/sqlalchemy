import sys

from sqlalchemy import create_engine
from sqlalchemy import event
from sqlalchemy import exc
from sqlalchemy import func
from sqlalchemy import INT
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import select
from sqlalchemy import String
from sqlalchemy import testing
from sqlalchemy import text
from sqlalchemy import VARCHAR
from sqlalchemy.testing import assert_raises
from sqlalchemy.testing import assert_raises_message
from sqlalchemy.testing import eq_
from sqlalchemy.testing import expect_warnings
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import ne_
from sqlalchemy.testing.engines import testing_engine
from sqlalchemy.testing.schema import Column
from sqlalchemy.testing.schema import Table


users, metadata = None, None


class TransactionTest(fixtures.TestBase):
    __backend__ = True

    @classmethod
    def setup_class(cls):
        global users, metadata
        metadata = MetaData()
        users = Table(
            "query_users",
            metadata,
            Column("user_id", INT, primary_key=True),
            Column("user_name", VARCHAR(20)),
            test_needs_acid=True,
        )
        users.create(testing.db)

    def teardown(self):
        testing.db.execute(users.delete()).close()

    @classmethod
    def teardown_class(cls):
        users.drop(testing.db)

    def test_commits(self):
        connection = testing.db.connect()
        transaction = connection.begin()
        connection.execute(users.insert(), user_id=1, user_name="user1")
        transaction.commit()

        transaction = connection.begin()
        connection.execute(users.insert(), user_id=2, user_name="user2")
        connection.execute(users.insert(), user_id=3, user_name="user3")
        transaction.commit()

        transaction = connection.begin()
        result = connection.execute("select * from query_users")
        assert len(result.fetchall()) == 3
        transaction.commit()
        connection.close()

    def test_rollback(self):
        """test a basic rollback"""

        connection = testing.db.connect()
        transaction = connection.begin()
        connection.execute(users.insert(), user_id=1, user_name="user1")
        connection.execute(users.insert(), user_id=2, user_name="user2")
        connection.execute(users.insert(), user_id=3, user_name="user3")
        transaction.rollback()

        result = connection.execute("select * from query_users")
        assert len(result.fetchall()) == 0
        connection.close()

    def test_raise(self):
        connection = testing.db.connect()

        transaction = connection.begin()
        try:
            connection.execute(users.insert(), user_id=1, user_name="user1")
            connection.execute(users.insert(), user_id=2, user_name="user2")
            connection.execute(users.insert(), user_id=1, user_name="user3")
            transaction.commit()
            assert False
        except Exception as e:
            print("Exception: ", e)
            transaction.rollback()

        result = connection.execute("select * from query_users")
        assert len(result.fetchall()) == 0
        connection.close()

    def test_transaction_container(self):
        def go(conn, table, data):
            for d in data:
                conn.execute(table.insert(), d)

        testing.db.transaction(go, users, [dict(user_id=1, user_name="user1")])
        eq_(testing.db.execute(users.select()).fetchall(), [(1, "user1")])
        assert_raises(
            exc.DBAPIError,
            testing.db.transaction,
            go,
            users,
            [
                {"user_id": 2, "user_name": "user2"},
                {"user_id": 1, "user_name": "user3"},
            ],
        )
        eq_(testing.db.execute(users.select()).fetchall(), [(1, "user1")])

    def test_nested_rollback(self):
        connection = testing.db.connect()
        try:
            transaction = connection.begin()
            try:
                connection.execute(
                    users.insert(), user_id=1, user_name="user1"
                )
                connection.execute(
                    users.insert(), user_id=2, user_name="user2"
                )
                connection.execute(
                    users.insert(), user_id=3, user_name="user3"
                )
                trans2 = connection.begin()
                try:
                    connection.execute(
                        users.insert(), user_id=4, user_name="user4"
                    )
                    connection.execute(
                        users.insert(), user_id=5, user_name="user5"
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
            try:
                # and not "This transaction is inactive"
                # comment moved here to fix pep8
                assert str(e) == "uh oh"
            finally:
                connection.close()

    def test_branch_nested_rollback(self):
        connection = testing.db.connect()
        try:
            connection.begin()
            branched = connection.connect()
            assert branched.in_transaction()
            branched.execute(users.insert(), user_id=1, user_name="user1")
            nested = branched.begin()
            branched.execute(users.insert(), user_id=2, user_name="user2")
            nested.rollback()
            assert not connection.in_transaction()
            eq_(connection.scalar("select count(*) from query_users"), 0)

        finally:
            connection.close()

    def test_branch_autorollback(self):
        connection = testing.db.connect()
        try:
            branched = connection.connect()
            branched.execute(users.insert(), user_id=1, user_name="user1")
            try:
                branched.execute(users.insert(), user_id=1, user_name="user1")
            except exc.DBAPIError:
                pass
        finally:
            connection.close()

    def test_branch_orig_rollback(self):
        connection = testing.db.connect()
        try:
            branched = connection.connect()
            branched.execute(users.insert(), user_id=1, user_name="user1")
            nested = branched.begin()
            assert branched.in_transaction()
            branched.execute(users.insert(), user_id=2, user_name="user2")
            nested.rollback()
            eq_(connection.scalar("select count(*) from query_users"), 1)

        finally:
            connection.close()

    def test_branch_autocommit(self):
        connection = testing.db.connect()
        try:
            branched = connection.connect()
            branched.execute(users.insert(), user_id=1, user_name="user1")
        finally:
            connection.close()
        eq_(testing.db.scalar("select count(*) from query_users"), 1)

    @testing.requires.savepoints
    def test_branch_savepoint_rollback(self):
        connection = testing.db.connect()
        try:
            trans = connection.begin()
            branched = connection.connect()
            assert branched.in_transaction()
            branched.execute(users.insert(), user_id=1, user_name="user1")
            nested = branched.begin_nested()
            branched.execute(users.insert(), user_id=2, user_name="user2")
            nested.rollback()
            assert connection.in_transaction()
            trans.commit()
            eq_(connection.scalar("select count(*) from query_users"), 1)

        finally:
            connection.close()

    @testing.requires.two_phase_transactions
    def test_branch_twophase_rollback(self):
        connection = testing.db.connect()
        try:
            branched = connection.connect()
            assert not branched.in_transaction()
            branched.execute(users.insert(), user_id=1, user_name="user1")
            nested = branched.begin_twophase()
            branched.execute(users.insert(), user_id=2, user_name="user2")
            nested.rollback()
            assert not connection.in_transaction()
            eq_(connection.scalar("select count(*) from query_users"), 1)

        finally:
            connection.close()

    @testing.requires.python2
    @testing.requires.savepoints_w_release
    def test_savepoint_release_fails_warning(self):
        with testing.db.connect() as connection:
            connection.begin()

            with expect_warnings(
                "An exception has occurred during handling of a previous "
                "exception.  The previous exception "
                r"is:.*..SQL\:.*RELEASE SAVEPOINT"
            ):

                def go():
                    with connection.begin_nested() as savepoint:
                        connection.dialect.do_release_savepoint(
                            connection, savepoint._savepoint
                        )

                assert_raises_message(
                    exc.DBAPIError, r".*SQL\:.*ROLLBACK TO SAVEPOINT", go
                )

    def test_retains_through_options(self):
        connection = testing.db.connect()
        try:
            transaction = connection.begin()
            connection.execute(users.insert(), user_id=1, user_name="user1")
            conn2 = connection.execution_options(dummy=True)
            conn2.execute(users.insert(), user_id=2, user_name="user2")
            transaction.rollback()
            eq_(connection.scalar("select count(*) from query_users"), 0)
        finally:
            connection.close()

    def test_nesting(self):
        connection = testing.db.connect()
        transaction = connection.begin()
        connection.execute(users.insert(), user_id=1, user_name="user1")
        connection.execute(users.insert(), user_id=2, user_name="user2")
        connection.execute(users.insert(), user_id=3, user_name="user3")
        trans2 = connection.begin()
        connection.execute(users.insert(), user_id=4, user_name="user4")
        connection.execute(users.insert(), user_id=5, user_name="user5")
        trans2.commit()
        transaction.rollback()
        self.assert_(
            connection.scalar("select count(*) from " "query_users") == 0
        )
        result = connection.execute("select * from query_users")
        assert len(result.fetchall()) == 0
        connection.close()

    def test_with_interface(self):
        connection = testing.db.connect()
        trans = connection.begin()
        connection.execute(users.insert(), user_id=1, user_name="user1")
        connection.execute(users.insert(), user_id=2, user_name="user2")
        try:
            connection.execute(users.insert(), user_id=2, user_name="user2.5")
        except Exception:
            trans.__exit__(*sys.exc_info())

        assert not trans.is_active
        self.assert_(
            connection.scalar("select count(*) from " "query_users") == 0
        )

        trans = connection.begin()
        connection.execute(users.insert(), user_id=1, user_name="user1")
        trans.__exit__(None, None, None)
        assert not trans.is_active
        self.assert_(
            connection.scalar("select count(*) from " "query_users") == 1
        )
        connection.close()

    def test_close(self):
        connection = testing.db.connect()
        transaction = connection.begin()
        connection.execute(users.insert(), user_id=1, user_name="user1")
        connection.execute(users.insert(), user_id=2, user_name="user2")
        connection.execute(users.insert(), user_id=3, user_name="user3")
        trans2 = connection.begin()
        connection.execute(users.insert(), user_id=4, user_name="user4")
        connection.execute(users.insert(), user_id=5, user_name="user5")
        assert connection.in_transaction()
        trans2.close()
        assert connection.in_transaction()
        transaction.commit()
        assert not connection.in_transaction()
        self.assert_(
            connection.scalar("select count(*) from " "query_users") == 5
        )
        result = connection.execute("select * from query_users")
        assert len(result.fetchall()) == 5
        connection.close()

    def test_close2(self):
        connection = testing.db.connect()
        transaction = connection.begin()
        connection.execute(users.insert(), user_id=1, user_name="user1")
        connection.execute(users.insert(), user_id=2, user_name="user2")
        connection.execute(users.insert(), user_id=3, user_name="user3")
        trans2 = connection.begin()
        connection.execute(users.insert(), user_id=4, user_name="user4")
        connection.execute(users.insert(), user_id=5, user_name="user5")
        assert connection.in_transaction()
        trans2.close()
        assert connection.in_transaction()
        transaction.close()
        assert not connection.in_transaction()
        self.assert_(
            connection.scalar("select count(*) from " "query_users") == 0
        )
        result = connection.execute("select * from query_users")
        assert len(result.fetchall()) == 0
        connection.close()

    @testing.requires.savepoints
    def test_nested_subtransaction_rollback(self):
        connection = testing.db.connect()
        transaction = connection.begin()
        connection.execute(users.insert(), user_id=1, user_name="user1")
        trans2 = connection.begin_nested()
        connection.execute(users.insert(), user_id=2, user_name="user2")
        trans2.rollback()
        connection.execute(users.insert(), user_id=3, user_name="user3")
        transaction.commit()
        eq_(
            connection.execute(
                select([users.c.user_id]).order_by(users.c.user_id)
            ).fetchall(),
            [(1,), (3,)],
        )
        connection.close()

    @testing.requires.savepoints
    @testing.crashes(
        "oracle+zxjdbc",
        "Errors out and causes subsequent tests to " "deadlock",
    )
    def test_nested_subtransaction_commit(self):
        connection = testing.db.connect()
        transaction = connection.begin()
        connection.execute(users.insert(), user_id=1, user_name="user1")
        trans2 = connection.begin_nested()
        connection.execute(users.insert(), user_id=2, user_name="user2")
        trans2.commit()
        connection.execute(users.insert(), user_id=3, user_name="user3")
        transaction.commit()
        eq_(
            connection.execute(
                select([users.c.user_id]).order_by(users.c.user_id)
            ).fetchall(),
            [(1,), (2,), (3,)],
        )
        connection.close()

    @testing.requires.savepoints
    def test_rollback_to_subtransaction(self):
        connection = testing.db.connect()
        transaction = connection.begin()
        connection.execute(users.insert(), user_id=1, user_name="user1")
        connection.begin_nested()
        connection.execute(users.insert(), user_id=2, user_name="user2")
        trans3 = connection.begin()
        connection.execute(users.insert(), user_id=3, user_name="user3")
        trans3.rollback()
        connection.execute(users.insert(), user_id=4, user_name="user4")
        transaction.commit()
        eq_(
            connection.execute(
                select([users.c.user_id]).order_by(users.c.user_id)
            ).fetchall(),
            [(1,), (4,)],
        )
        connection.close()

    @testing.requires.two_phase_transactions
    def test_two_phase_transaction(self):
        connection = testing.db.connect()
        transaction = connection.begin_twophase()
        connection.execute(users.insert(), user_id=1, user_name="user1")
        transaction.prepare()
        transaction.commit()
        transaction = connection.begin_twophase()
        connection.execute(users.insert(), user_id=2, user_name="user2")
        transaction.commit()
        transaction.close()
        transaction = connection.begin_twophase()
        connection.execute(users.insert(), user_id=3, user_name="user3")
        transaction.rollback()
        transaction = connection.begin_twophase()
        connection.execute(users.insert(), user_id=4, user_name="user4")
        transaction.prepare()
        transaction.rollback()
        transaction.close()
        eq_(
            connection.execute(
                select([users.c.user_id]).order_by(users.c.user_id)
            ).fetchall(),
            [(1,), (2,)],
        )
        connection.close()

    # PG emergency shutdown:
    # select * from pg_prepared_xacts
    # ROLLBACK PREPARED '<xid>'
    # MySQL emergency shutdown:
    # for arg in `mysql -u root -e "xa recover" | cut -c 8-100 |
    #     grep sa`; do mysql -u root -e "xa rollback '$arg'"; done
    @testing.crashes("mysql", "Crashing on 5.5, not worth it")
    @testing.requires.skip_mysql_on_windows
    @testing.requires.two_phase_transactions
    @testing.requires.savepoints
    def test_mixed_two_phase_transaction(self):
        connection = testing.db.connect()
        transaction = connection.begin_twophase()
        connection.execute(users.insert(), user_id=1, user_name="user1")
        transaction2 = connection.begin()
        connection.execute(users.insert(), user_id=2, user_name="user2")
        transaction3 = connection.begin_nested()
        connection.execute(users.insert(), user_id=3, user_name="user3")
        transaction4 = connection.begin()
        connection.execute(users.insert(), user_id=4, user_name="user4")
        transaction4.commit()
        transaction3.rollback()
        connection.execute(users.insert(), user_id=5, user_name="user5")
        transaction2.commit()
        transaction.prepare()
        transaction.commit()
        eq_(
            connection.execute(
                select([users.c.user_id]).order_by(users.c.user_id)
            ).fetchall(),
            [(1,), (2,), (5,)],
        )
        connection.close()

    @testing.requires.two_phase_transactions
    @testing.requires.two_phase_recovery
    def test_two_phase_recover(self):

        # MySQL recovery doesn't currently seem to work correctly
        # Prepared transactions disappear when connections are closed
        # and even when they aren't it doesn't seem possible to use the
        # recovery id.

        connection = testing.db.connect()
        transaction = connection.begin_twophase()
        connection.execute(users.insert(), user_id=1, user_name="user1")
        transaction.prepare()
        connection.invalidate()

        connection2 = testing.db.connect()
        eq_(
            connection2.execution_options(autocommit=True)
            .execute(select([users.c.user_id]).order_by(users.c.user_id))
            .fetchall(),
            [],
        )
        recoverables = connection2.recover_twophase()
        assert transaction.xid in recoverables
        connection2.commit_prepared(transaction.xid, recover=True)
        eq_(
            connection2.execute(
                select([users.c.user_id]).order_by(users.c.user_id)
            ).fetchall(),
            [(1,)],
        )
        connection2.close()

    @testing.requires.two_phase_transactions
    def test_multiple_two_phase(self):
        conn = testing.db.connect()
        xa = conn.begin_twophase()
        conn.execute(users.insert(), user_id=1, user_name="user1")
        xa.prepare()
        xa.commit()
        xa = conn.begin_twophase()
        conn.execute(users.insert(), user_id=2, user_name="user2")
        xa.prepare()
        xa.rollback()
        xa = conn.begin_twophase()
        conn.execute(users.insert(), user_id=3, user_name="user3")
        xa.rollback()
        xa = conn.begin_twophase()
        conn.execute(users.insert(), user_id=4, user_name="user4")
        xa.prepare()
        xa.commit()
        result = conn.execute(
            select([users.c.user_name]).order_by(users.c.user_id)
        )
        eq_(result.fetchall(), [("user1",), ("user4",)])
        conn.close()

    @testing.requires.two_phase_transactions
    def test_reset_rollback_two_phase_no_rollback(self):
        # test [ticket:2907], essentially that the
        # TwoPhaseTransaction is given the job of "reset on return"
        # so that picky backends like MySQL correctly clear out
        # their state when a connection is closed without handling
        # the transaction explicitly.

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
            conn.execute(users.insert(), user_id=1, user_name="user1")

        assert rec.connection is raw_dbapi_con

        with eng.connect() as conn:
            result = conn.execute(
                select([users.c.user_name]).order_by(users.c.user_id)
            )
            eq_(result.fetchall(), [])


class ResetAgentTest(fixtures.TestBase):
    __backend__ = True

    def test_begin_close(self):
        with testing.db.connect() as connection:
            trans = connection.begin()
            assert connection.connection._reset_agent is trans
        assert not trans.is_active

    def test_begin_rollback(self):
        with testing.db.connect() as connection:
            trans = connection.begin()
            assert connection.connection._reset_agent is trans
            trans.rollback()
            assert connection.connection._reset_agent is None

    def test_begin_commit(self):
        with testing.db.connect() as connection:
            trans = connection.begin()
            assert connection.connection._reset_agent is trans
            trans.commit()
            assert connection.connection._reset_agent is None

    def test_trans_close(self):
        with testing.db.connect() as connection:
            trans = connection.begin()
            assert connection.connection._reset_agent is trans
            trans.close()
            assert connection.connection._reset_agent is None

    def test_trans_reset_agent_broken_ensure(self):
        eng = testing_engine()
        conn = eng.connect()
        trans = conn.begin()
        assert conn.connection._reset_agent is trans
        trans.is_active = False

        with expect_warnings("Reset agent is not active"):
            conn.close()

    def test_trans_commit_reset_agent_broken_ensure(self):
        eng = testing_engine(options={"pool_reset_on_return": "commit"})
        conn = eng.connect()
        trans = conn.begin()
        assert conn.connection._reset_agent is trans
        trans.is_active = False

        with expect_warnings("Reset agent is not active"):
            conn.close()

    @testing.requires.savepoints
    def test_begin_nested_trans_close_one(self):
        with testing.db.connect() as connection:
            t1 = connection.begin()
            assert connection.connection._reset_agent is t1
            t2 = connection.begin_nested()
            assert connection.connection._reset_agent is t1
            assert connection._Connection__transaction is t2
            t2.close()
            assert connection._Connection__transaction is t1
            assert connection.connection._reset_agent is t1
            t1.close()
            assert connection.connection._reset_agent is None
        assert not t1.is_active

    @testing.requires.savepoints
    def test_begin_nested_trans_close_two(self):
        with testing.db.connect() as connection:
            t1 = connection.begin()
            assert connection.connection._reset_agent is t1
            t2 = connection.begin_nested()
            assert connection.connection._reset_agent is t1
            assert connection._Connection__transaction is t2

            assert connection.connection._reset_agent is t1
            t1.close()
            assert connection.connection._reset_agent is None
        assert not t1.is_active

    @testing.requires.savepoints
    def test_begin_nested_trans_rollback(self):
        with testing.db.connect() as connection:
            t1 = connection.begin()
            assert connection.connection._reset_agent is t1
            t2 = connection.begin_nested()
            assert connection.connection._reset_agent is t1
            assert connection._Connection__transaction is t2
            t2.close()
            assert connection._Connection__transaction is t1
            assert connection.connection._reset_agent is t1
            t1.rollback()
            assert connection.connection._reset_agent is None
        assert not t1.is_active

    @testing.requires.savepoints
    def test_begin_nested_close(self):
        with testing.db.connect() as connection:
            trans = connection.begin_nested()
            assert connection.connection._reset_agent is trans
        assert not trans.is_active

    @testing.requires.savepoints
    def test_begin_begin_nested_close(self):
        with testing.db.connect() as connection:
            trans = connection.begin()
            trans2 = connection.begin_nested()
            assert connection.connection._reset_agent is trans
        assert trans2.is_active  # was never closed
        assert not trans.is_active

    @testing.requires.savepoints
    def test_begin_begin_nested_rollback_commit(self):
        with testing.db.connect() as connection:
            trans = connection.begin()
            trans2 = connection.begin_nested()
            assert connection.connection._reset_agent is trans
            trans2.rollback()
            assert connection.connection._reset_agent is trans
            trans.commit()
            assert connection.connection._reset_agent is None

    @testing.requires.savepoints
    def test_begin_begin_nested_rollback_rollback(self):
        with testing.db.connect() as connection:
            trans = connection.begin()
            trans2 = connection.begin_nested()
            assert connection.connection._reset_agent is trans
            trans2.rollback()
            assert connection.connection._reset_agent is trans
            trans.rollback()
            assert connection.connection._reset_agent is None

    def test_begin_begin_rollback_rollback(self):
        with testing.db.connect() as connection:
            trans = connection.begin()
            trans2 = connection.begin()
            assert connection.connection._reset_agent is trans
            trans2.rollback()
            assert connection.connection._reset_agent is None
            trans.rollback()
            assert connection.connection._reset_agent is None

    def test_begin_begin_commit_commit(self):
        with testing.db.connect() as connection:
            trans = connection.begin()
            trans2 = connection.begin()
            assert connection.connection._reset_agent is trans
            trans2.commit()
            assert connection.connection._reset_agent is trans
            trans.commit()
            assert connection.connection._reset_agent is None

    @testing.requires.two_phase_transactions
    def test_reset_via_agent_begin_twophase(self):
        with testing.db.connect() as connection:
            trans = connection.begin_twophase()
            assert connection.connection._reset_agent is trans

    @testing.requires.two_phase_transactions
    def test_reset_via_agent_begin_twophase_commit(self):
        with testing.db.connect() as connection:
            trans = connection.begin_twophase()
            assert connection.connection._reset_agent is trans
            trans.commit()
            assert connection.connection._reset_agent is None

    @testing.requires.two_phase_transactions
    def test_reset_via_agent_begin_twophase_rollback(self):
        with testing.db.connect() as connection:
            trans = connection.begin_twophase()
            assert connection.connection._reset_agent is trans
            trans.rollback()
            assert connection.connection._reset_agent is None


class AutoRollbackTest(fixtures.TestBase):
    __backend__ = True

    @classmethod
    def setup_class(cls):
        global metadata
        metadata = MetaData()

    @classmethod
    def teardown_class(cls):
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
        users.create(conn1)
        conn1.execute("select * from deadlock_users")
        conn1.close()

        # without auto-rollback in the connection pool's return() logic,
        # this deadlocks in PostgreSQL, because conn1 is returned to the
        # pool but still has a lock on "deadlock_users". comment out the
        # rollback in pool/ConnectionFairy._close() to see !

        users.drop(conn2)
        conn2.close()


class ExplicitAutoCommitTest(fixtures.TestBase):

    """test the 'autocommit' flag on select() and text() objects.

    Requires PostgreSQL so that we may define a custom function which
    modifies the database."""

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

    def test_control(self):

        # test that not using autocommit does not commit

        conn1 = testing.db.connect()
        conn2 = testing.db.connect()
        conn1.execute(select([func.insert_foo("data1")]))
        assert conn2.execute(select([foo.c.data])).fetchall() == []
        conn1.execute(text("select insert_foo('moredata')"))
        assert conn2.execute(select([foo.c.data])).fetchall() == []
        trans = conn1.begin()
        trans.commit()
        assert conn2.execute(select([foo.c.data])).fetchall() == [
            ("data1",),
            ("moredata",),
        ]
        conn1.close()
        conn2.close()

    def test_explicit_compiled(self):
        conn1 = testing.db.connect()
        conn2 = testing.db.connect()
        conn1.execute(
            select([func.insert_foo("data1")]).execution_options(
                autocommit=True
            )
        )
        assert conn2.execute(select([foo.c.data])).fetchall() == [("data1",)]
        conn1.close()
        conn2.close()

    def test_explicit_connection(self):
        conn1 = testing.db.connect()
        conn2 = testing.db.connect()
        conn1.execution_options(autocommit=True).execute(
            select([func.insert_foo("data1")])
        )
        eq_(conn2.execute(select([foo.c.data])).fetchall(), [("data1",)])

        # connection supersedes statement

        conn1.execution_options(autocommit=False).execute(
            select([func.insert_foo("data2")]).execution_options(
                autocommit=True
            )
        )
        eq_(conn2.execute(select([foo.c.data])).fetchall(), [("data1",)])

        # ditto

        conn1.execution_options(autocommit=True).execute(
            select([func.insert_foo("data3")]).execution_options(
                autocommit=False
            )
        )
        eq_(
            conn2.execute(select([foo.c.data])).fetchall(),
            [("data1",), ("data2",), ("data3",)],
        )
        conn1.close()
        conn2.close()

    def test_explicit_text(self):
        conn1 = testing.db.connect()
        conn2 = testing.db.connect()
        conn1.execute(
            text("select insert_foo('moredata')").execution_options(
                autocommit=True
            )
        )
        assert conn2.execute(select([foo.c.data])).fetchall() == [
            ("moredata",)
        ]
        conn1.close()
        conn2.close()

    def test_implicit_text(self):
        conn1 = testing.db.connect()
        conn2 = testing.db.connect()
        conn1.execute(text("insert into foo (data) values ('implicitdata')"))
        assert conn2.execute(select([foo.c.data])).fetchall() == [
            ("implicitdata",)
        ]
        conn1.close()
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
            select([1]).execution_options,
            isolation_level=self._non_default_isolation_level(),
        )

    def test_per_engine(self):
        # new in 0.9
        eng = create_engine(
            testing.db.url,
            execution_options={
                "isolation_level": self._non_default_isolation_level()
            },
        )
        conn = eng.connect()
        eq_(
            eng.dialect.get_isolation_level(conn.connection),
            self._non_default_isolation_level(),
        )

    def test_per_option_engine(self):
        eng = create_engine(testing.db.url).execution_options(
            isolation_level=self._non_default_isolation_level()
        )

        conn = eng.connect()
        eq_(
            eng.dialect.get_isolation_level(conn.connection),
            self._non_default_isolation_level(),
        )

    def test_isolation_level_accessors_connection_default(self):
        eng = create_engine(testing.db.url)
        with eng.connect() as conn:
            eq_(conn.default_isolation_level, self._default_isolation_level())
        with eng.connect() as conn:
            eq_(conn.get_isolation_level(), self._default_isolation_level())

    def test_isolation_level_accessors_connection_option_modified(self):
        eng = create_engine(testing.db.url)
        with eng.connect() as conn:
            c2 = conn.execution_options(
                isolation_level=self._non_default_isolation_level()
            )
            eq_(conn.default_isolation_level, self._default_isolation_level())
            eq_(
                conn.get_isolation_level(), self._non_default_isolation_level()
            )
            eq_(c2.get_isolation_level(), self._non_default_isolation_level())
