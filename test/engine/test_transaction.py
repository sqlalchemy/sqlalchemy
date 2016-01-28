from sqlalchemy.testing import eq_, assert_raises, \
    assert_raises_message, ne_, expect_warnings
import sys
from sqlalchemy import event
from sqlalchemy.testing.engines import testing_engine
from sqlalchemy import create_engine, MetaData, INT, VARCHAR, Sequence, \
    select, Integer, String, func, text, exc
from sqlalchemy.testing.schema import Table
from sqlalchemy.testing.schema import Column
from sqlalchemy import testing
from sqlalchemy.testing import fixtures


users, metadata = None, None


class TransactionTest(fixtures.TestBase):
    __backend__ = True

    @classmethod
    def setup_class(cls):
        global users, metadata
        metadata = MetaData()
        users = Table('query_users', metadata,
            Column('user_id', INT, primary_key=True),
            Column('user_name', VARCHAR(20)),
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
        connection.execute(users.insert(), user_id=1, user_name='user1')
        transaction.commit()

        transaction = connection.begin()
        connection.execute(users.insert(), user_id=2, user_name='user2')
        connection.execute(users.insert(), user_id=3, user_name='user3')
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
        connection.execute(users.insert(), user_id=1, user_name='user1')
        connection.execute(users.insert(), user_id=2, user_name='user2')
        connection.execute(users.insert(), user_id=3, user_name='user3')
        transaction.rollback()

        result = connection.execute("select * from query_users")
        assert len(result.fetchall()) == 0
        connection.close()

    def test_raise(self):
        connection = testing.db.connect()

        transaction = connection.begin()
        try:
            connection.execute(users.insert(), user_id=1, user_name='user1')
            connection.execute(users.insert(), user_id=2, user_name='user2')
            connection.execute(users.insert(), user_id=1, user_name='user3')
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

        testing.db.transaction(go, users, [dict(user_id=1,
                               user_name='user1')])
        eq_(testing.db.execute(users.select()).fetchall(), [(1, 'user1'
            )])
        assert_raises(exc.DBAPIError, testing.db.transaction, go,
                      users, [{'user_id': 2, 'user_name': 'user2'},
                      {'user_id': 1, 'user_name': 'user3'}])
        eq_(testing.db.execute(users.select()).fetchall(), [(1, 'user1'
            )])

    def test_nested_rollback(self):
        connection = testing.db.connect()
        try:
            transaction = connection.begin()
            try:
                connection.execute(users.insert(), user_id=1,
                                   user_name='user1')
                connection.execute(users.insert(), user_id=2,
                                   user_name='user2')
                connection.execute(users.insert(), user_id=3,
                                   user_name='user3')
                trans2 = connection.begin()
                try:
                    connection.execute(users.insert(), user_id=4,
                            user_name='user4')
                    connection.execute(users.insert(), user_id=5,
                            user_name='user5')
                    raise Exception('uh oh')
                    trans2.commit()
                except:
                    trans2.rollback()
                    raise
                transaction.rollback()
            except Exception as e:
                transaction.rollback()
                raise
        except Exception as e:
            try:
                assert str(e) == 'uh oh'  # and not "This transaction is
                                          # inactive"
            finally:
                connection.close()

    def test_branch_nested_rollback(self):
        connection = testing.db.connect()
        try:
            connection.begin()
            branched = connection.connect()
            assert branched.in_transaction()
            branched.execute(users.insert(), user_id=1, user_name='user1')
            nested = branched.begin()
            branched.execute(users.insert(), user_id=2, user_name='user2')
            nested.rollback()
            assert not connection.in_transaction()
            eq_(connection.scalar("select count(*) from query_users"), 0)

        finally:
            connection.close()

    def test_branch_autorollback(self):
        connection = testing.db.connect()
        try:
            branched = connection.connect()
            branched.execute(users.insert(), user_id=1, user_name='user1')
            try:
                branched.execute(users.insert(), user_id=1, user_name='user1')
            except exc.DBAPIError:
                pass
        finally:
            connection.close()

    def test_branch_orig_rollback(self):
        connection = testing.db.connect()
        try:
            branched = connection.connect()
            branched.execute(users.insert(), user_id=1, user_name='user1')
            nested = branched.begin()
            assert branched.in_transaction()
            branched.execute(users.insert(), user_id=2, user_name='user2')
            nested.rollback()
            eq_(connection.scalar("select count(*) from query_users"), 1)

        finally:
            connection.close()

    def test_branch_autocommit(self):
        connection = testing.db.connect()
        try:
            branched = connection.connect()
            branched.execute(users.insert(), user_id=1, user_name='user1')
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
            branched.execute(users.insert(), user_id=1, user_name='user1')
            nested = branched.begin_nested()
            branched.execute(users.insert(), user_id=2, user_name='user2')
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
            branched.execute(users.insert(), user_id=1, user_name='user1')
            nested = branched.begin_twophase()
            branched.execute(users.insert(), user_id=2, user_name='user2')
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
                "is:.*..SQL\:.*RELEASE SAVEPOINT"
            ):
                def go():
                    with connection.begin_nested() as savepoint:
                        connection.dialect.do_release_savepoint(
                            connection, savepoint._savepoint)
                assert_raises_message(
                    exc.DBAPIError,
                    ".*SQL\:.*ROLLBACK TO SAVEPOINT",
                    go
                )

    def test_retains_through_options(self):
        connection = testing.db.connect()
        try:
            transaction = connection.begin()
            connection.execute(users.insert(), user_id=1, user_name='user1')
            conn2 = connection.execution_options(dummy=True)
            conn2.execute(users.insert(), user_id=2, user_name='user2')
            transaction.rollback()
            eq_(connection.scalar("select count(*) from query_users"), 0)
        finally:
            connection.close()

    def test_nesting(self):
        connection = testing.db.connect()
        transaction = connection.begin()
        connection.execute(users.insert(), user_id=1, user_name='user1')
        connection.execute(users.insert(), user_id=2, user_name='user2')
        connection.execute(users.insert(), user_id=3, user_name='user3')
        trans2 = connection.begin()
        connection.execute(users.insert(), user_id=4, user_name='user4')
        connection.execute(users.insert(), user_id=5, user_name='user5')
        trans2.commit()
        transaction.rollback()
        self.assert_(connection.scalar('select count(*) from '
                     'query_users') == 0)
        result = connection.execute('select * from query_users')
        assert len(result.fetchall()) == 0
        connection.close()

    def test_with_interface(self):
        connection = testing.db.connect()
        trans = connection.begin()
        connection.execute(users.insert(), user_id=1, user_name='user1')
        connection.execute(users.insert(), user_id=2, user_name='user2')
        try:
            connection.execute(users.insert(), user_id=2, user_name='user2.5')
        except Exception as e:
            trans.__exit__(*sys.exc_info())

        assert not trans.is_active
        self.assert_(connection.scalar('select count(*) from '
                     'query_users') == 0)

        trans = connection.begin()
        connection.execute(users.insert(), user_id=1, user_name='user1')
        trans.__exit__(None, None, None)
        assert not trans.is_active
        self.assert_(connection.scalar('select count(*) from '
                     'query_users') == 1)
        connection.close()

    def test_close(self):
        connection = testing.db.connect()
        transaction = connection.begin()
        connection.execute(users.insert(), user_id=1, user_name='user1')
        connection.execute(users.insert(), user_id=2, user_name='user2')
        connection.execute(users.insert(), user_id=3, user_name='user3')
        trans2 = connection.begin()
        connection.execute(users.insert(), user_id=4, user_name='user4')
        connection.execute(users.insert(), user_id=5, user_name='user5')
        assert connection.in_transaction()
        trans2.close()
        assert connection.in_transaction()
        transaction.commit()
        assert not connection.in_transaction()
        self.assert_(connection.scalar('select count(*) from '
                     'query_users') == 5)
        result = connection.execute('select * from query_users')
        assert len(result.fetchall()) == 5
        connection.close()

    def test_close2(self):
        connection = testing.db.connect()
        transaction = connection.begin()
        connection.execute(users.insert(), user_id=1, user_name='user1')
        connection.execute(users.insert(), user_id=2, user_name='user2')
        connection.execute(users.insert(), user_id=3, user_name='user3')
        trans2 = connection.begin()
        connection.execute(users.insert(), user_id=4, user_name='user4')
        connection.execute(users.insert(), user_id=5, user_name='user5')
        assert connection.in_transaction()
        trans2.close()
        assert connection.in_transaction()
        transaction.close()
        assert not connection.in_transaction()
        self.assert_(connection.scalar('select count(*) from '
                     'query_users') == 0)
        result = connection.execute('select * from query_users')
        assert len(result.fetchall()) == 0
        connection.close()

    @testing.requires.savepoints
    def test_nested_subtransaction_rollback(self):
        connection = testing.db.connect()
        transaction = connection.begin()
        connection.execute(users.insert(), user_id=1, user_name='user1')
        trans2 = connection.begin_nested()
        connection.execute(users.insert(), user_id=2, user_name='user2')
        trans2.rollback()
        connection.execute(users.insert(), user_id=3, user_name='user3')
        transaction.commit()
        eq_(connection.execute(select([users.c.user_id]).
            order_by(users.c.user_id)).fetchall(),
            [(1, ), (3, )])
        connection.close()

    @testing.requires.savepoints
    @testing.crashes('oracle+zxjdbc',
                     'Errors out and causes subsequent tests to '
                     'deadlock')
    def test_nested_subtransaction_commit(self):
        connection = testing.db.connect()
        transaction = connection.begin()
        connection.execute(users.insert(), user_id=1, user_name='user1')
        trans2 = connection.begin_nested()
        connection.execute(users.insert(), user_id=2, user_name='user2')
        trans2.commit()
        connection.execute(users.insert(), user_id=3, user_name='user3')
        transaction.commit()
        eq_(connection.execute(select([users.c.user_id]).
            order_by(users.c.user_id)).fetchall(),
            [(1, ), (2, ), (3, )])
        connection.close()

    @testing.requires.savepoints
    def test_rollback_to_subtransaction(self):
        connection = testing.db.connect()
        transaction = connection.begin()
        connection.execute(users.insert(), user_id=1, user_name='user1')
        trans2 = connection.begin_nested()
        connection.execute(users.insert(), user_id=2, user_name='user2')
        trans3 = connection.begin()
        connection.execute(users.insert(), user_id=3, user_name='user3')
        trans3.rollback()
        connection.execute(users.insert(), user_id=4, user_name='user4')
        transaction.commit()
        eq_(connection.execute(select([users.c.user_id]).
            order_by(users.c.user_id)).fetchall(),
            [(1, ), (4, )])
        connection.close()

    @testing.requires.two_phase_transactions
    def test_two_phase_transaction(self):
        connection = testing.db.connect()
        transaction = connection.begin_twophase()
        connection.execute(users.insert(), user_id=1, user_name='user1')
        transaction.prepare()
        transaction.commit()
        transaction = connection.begin_twophase()
        connection.execute(users.insert(), user_id=2, user_name='user2')
        transaction.commit()
        transaction.close()
        transaction = connection.begin_twophase()
        connection.execute(users.insert(), user_id=3, user_name='user3')
        transaction.rollback()
        transaction = connection.begin_twophase()
        connection.execute(users.insert(), user_id=4, user_name='user4')
        transaction.prepare()
        transaction.rollback()
        transaction.close()
        eq_(connection.execute(select([users.c.user_id]).
            order_by(users.c.user_id)).fetchall(),
            [(1, ), (2, )])
        connection.close()

    # PG emergency shutdown:
    # select * from pg_prepared_xacts
    # ROLLBACK PREPARED '<xid>'
    @testing.crashes('mysql', 'Crashing on 5.5, not worth it')
    @testing.requires.skip_mysql_on_windows
    @testing.requires.two_phase_transactions
    @testing.requires.savepoints
    def test_mixed_two_phase_transaction(self):
        connection = testing.db.connect()
        transaction = connection.begin_twophase()
        connection.execute(users.insert(), user_id=1, user_name='user1')
        transaction2 = connection.begin()
        connection.execute(users.insert(), user_id=2, user_name='user2')
        transaction3 = connection.begin_nested()
        connection.execute(users.insert(), user_id=3, user_name='user3')
        transaction4 = connection.begin()
        connection.execute(users.insert(), user_id=4, user_name='user4')
        transaction4.commit()
        transaction3.rollback()
        connection.execute(users.insert(), user_id=5, user_name='user5')
        transaction2.commit()
        transaction.prepare()
        transaction.commit()
        eq_(connection.execute(select([users.c.user_id]).
            order_by(users.c.user_id)).fetchall(),
            [(1, ), (2, ), (5, )])
        connection.close()

    @testing.requires.two_phase_transactions
    @testing.crashes('mysql+oursql',
                     'Times out in full test runs only, causing '
                     'subsequent tests to fail')
    @testing.crashes('mysql+zxjdbc',
                     'Deadlocks, causing subsequent tests to fail')
    @testing.fails_on('mysql', 'FIXME: unknown')
    def test_two_phase_recover(self):

        # MySQL recovery doesn't currently seem to work correctly
        # Prepared transactions disappear when connections are closed
        # and even when they aren't it doesn't seem possible to use the
        # recovery id.

        connection = testing.db.connect()
        transaction = connection.begin_twophase()
        connection.execute(users.insert(), user_id=1, user_name='user1')
        transaction.prepare()
        connection.invalidate()

        connection2 = testing.db.connect()
        eq_(
            connection2.execution_options(autocommit=True).
            execute(select([users.c.user_id]).
            order_by(users.c.user_id)).fetchall(), [])
        recoverables = connection2.recover_twophase()
        assert transaction.xid in recoverables
        connection2.commit_prepared(transaction.xid, recover=True)
        eq_(connection2.execute(select([users.c.user_id]).
            order_by(users.c.user_id)).fetchall(),
            [(1, )])
        connection2.close()

    @testing.requires.two_phase_transactions
    def test_multiple_two_phase(self):
        conn = testing.db.connect()
        xa = conn.begin_twophase()
        conn.execute(users.insert(), user_id=1, user_name='user1')
        xa.prepare()
        xa.commit()
        xa = conn.begin_twophase()
        conn.execute(users.insert(), user_id=2, user_name='user2')
        xa.prepare()
        xa.rollback()
        xa = conn.begin_twophase()
        conn.execute(users.insert(), user_id=3, user_name='user3')
        xa.rollback()
        xa = conn.begin_twophase()
        conn.execute(users.insert(), user_id=4, user_name='user4')
        xa.prepare()
        xa.commit()
        result = \
            conn.execute(select([users.c.user_name]).
                order_by(users.c.user_id))
        eq_(result.fetchall(), [('user1', ), ('user4', )])
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
            xa = conn.begin_twophase()
            conn.execute(users.insert(), user_id=1, user_name='user1')

        assert rec.connection is raw_dbapi_con

        with eng.connect() as conn:
            result = \
                conn.execute(select([users.c.user_name]).
                    order_by(users.c.user_id))
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
        users = Table('deadlock_users', metadata, Column('user_id',
                      INT, primary_key=True), Column('user_name',
                      VARCHAR(20)), test_needs_acid=True)
        users.create(conn1)
        conn1.execute('select * from deadlock_users')
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
    modifies the database. """

    __only_on__ = 'postgresql'

    @classmethod
    def setup_class(cls):
        global metadata, foo
        metadata = MetaData(testing.db)
        foo = Table('foo', metadata, Column('id', Integer,
                    primary_key=True), Column('data', String(100)))
        metadata.create_all()
        testing.db.execute("create function insert_foo(varchar) "
                           "returns integer as 'insert into foo(data) "
                           "values ($1);select 1;' language sql")

    def teardown(self):
        foo.delete().execute().close()

    @classmethod
    def teardown_class(cls):
        testing.db.execute('drop function insert_foo(varchar)')
        metadata.drop_all()

    def test_control(self):

        # test that not using autocommit does not commit

        conn1 = testing.db.connect()
        conn2 = testing.db.connect()
        conn1.execute(select([func.insert_foo('data1')]))
        assert conn2.execute(select([foo.c.data])).fetchall() == []
        conn1.execute(text("select insert_foo('moredata')"))
        assert conn2.execute(select([foo.c.data])).fetchall() == []
        trans = conn1.begin()
        trans.commit()
        assert conn2.execute(select([foo.c.data])).fetchall() \
            == [('data1', ), ('moredata', )]
        conn1.close()
        conn2.close()

    def test_explicit_compiled(self):
        conn1 = testing.db.connect()
        conn2 = testing.db.connect()
        conn1.execute(select([func.insert_foo('data1'
                      )]).execution_options(autocommit=True))
        assert conn2.execute(select([foo.c.data])).fetchall() \
            == [('data1', )]
        conn1.close()
        conn2.close()

    def test_explicit_connection(self):
        conn1 = testing.db.connect()
        conn2 = testing.db.connect()
        conn1.execution_options(autocommit=True).\
            execute(select([func.insert_foo('data1'
                )]))
        eq_(conn2.execute(select([foo.c.data])).fetchall(), [('data1',
            )])

        # connection supersedes statement

        conn1.execution_options(autocommit=False).\
            execute(select([func.insert_foo('data2'
                )]).execution_options(autocommit=True))
        eq_(conn2.execute(select([foo.c.data])).fetchall(), [('data1',
            )])

        # ditto

        conn1.execution_options(autocommit=True).\
            execute(select([func.insert_foo('data3'
                )]).execution_options(autocommit=False))
        eq_(conn2.execute(select([foo.c.data])).fetchall(), [('data1',
            ), ('data2', ), ('data3', )])
        conn1.close()
        conn2.close()

    def test_explicit_text(self):
        conn1 = testing.db.connect()
        conn2 = testing.db.connect()
        conn1.execute(text("select insert_foo('moredata')"
                      ).execution_options(autocommit=True))
        assert conn2.execute(select([foo.c.data])).fetchall() \
            == [('moredata', )]
        conn1.close()
        conn2.close()

    @testing.uses_deprecated(r'autocommit on select\(\) is deprecated',
                             r'``autocommit\(\)`` is deprecated')
    def test_explicit_compiled_deprecated(self):
        conn1 = testing.db.connect()
        conn2 = testing.db.connect()
        conn1.execute(select([func.insert_foo('data1')],
                      autocommit=True))
        assert conn2.execute(select([foo.c.data])).fetchall() \
            == [('data1', )]
        conn1.execute(select([func.insert_foo('data2')]).autocommit())
        assert conn2.execute(select([foo.c.data])).fetchall() \
            == [('data1', ), ('data2', )]
        conn1.close()
        conn2.close()

    @testing.uses_deprecated(r'autocommit on text\(\) is deprecated')
    def test_explicit_text_deprecated(self):
        conn1 = testing.db.connect()
        conn2 = testing.db.connect()
        conn1.execute(text("select insert_foo('moredata')",
                      autocommit=True))
        assert conn2.execute(select([foo.c.data])).fetchall() \
            == [('moredata', )]
        conn1.close()
        conn2.close()

    def test_implicit_text(self):
        conn1 = testing.db.connect()
        conn2 = testing.db.connect()
        conn1.execute(text("insert into foo (data) values "
                      "('implicitdata')"))
        assert conn2.execute(select([foo.c.data])).fetchall() \
            == [('implicitdata', )]
        conn1.close()
        conn2.close()


tlengine = None


class TLTransactionTest(fixtures.TestBase):
    __requires__ = ('ad_hoc_engines', )
    __backend__ = True

    @classmethod
    def setup_class(cls):
        global users, metadata, tlengine
        tlengine = testing_engine(options=dict(strategy='threadlocal'))
        metadata = MetaData()
        users = Table('query_users', metadata, Column('user_id', INT,
                      Sequence('query_users_id_seq', optional=True),
                      primary_key=True), Column('user_name',
                      VARCHAR(20)), test_needs_acid=True)
        metadata.create_all(tlengine)

    def teardown(self):
        tlengine.execute(users.delete()).close()

    @classmethod
    def teardown_class(cls):
        tlengine.close()
        metadata.drop_all(tlengine)
        tlengine.dispose()

    def setup(self):

        # ensure tests start with engine closed

        tlengine.close()

    @testing.crashes('oracle', 'TNS error of unknown origin occurs on the buildbot.')
    def test_rollback_no_trans(self):
        tlengine = testing_engine(options=dict(strategy="threadlocal"))

        # shouldn't fail
        tlengine.rollback()

        tlengine.begin()
        tlengine.rollback()

        # shouldn't fail
        tlengine.rollback()

    def test_commit_no_trans(self):
        tlengine = testing_engine(options=dict(strategy="threadlocal"))

        # shouldn't fail
        tlengine.commit()

        tlengine.begin()
        tlengine.rollback()

        # shouldn't fail
        tlengine.commit()

    def test_prepare_no_trans(self):
        tlengine = testing_engine(options=dict(strategy="threadlocal"))

        # shouldn't fail
        tlengine.prepare()

        tlengine.begin()
        tlengine.rollback()

        # shouldn't fail
        tlengine.prepare()

    def test_connection_close(self):
        """test that when connections are closed for real, transactions
        are rolled back and disposed."""

        c = tlengine.contextual_connect()
        c.begin()
        assert c.in_transaction()
        c.close()
        assert not c.in_transaction()

    def test_transaction_close(self):
        c = tlengine.contextual_connect()
        t = c.begin()
        tlengine.execute(users.insert(), user_id=1, user_name='user1')
        tlengine.execute(users.insert(), user_id=2, user_name='user2')
        t2 = c.begin()
        tlengine.execute(users.insert(), user_id=3, user_name='user3')
        tlengine.execute(users.insert(), user_id=4, user_name='user4')
        t2.close()
        result = c.execute('select * from query_users')
        assert len(result.fetchall()) == 4
        t.close()
        external_connection = tlengine.connect()
        result = external_connection.execute('select * from query_users'
                )
        try:
            assert len(result.fetchall()) == 0
        finally:
            c.close()
            external_connection.close()

    def test_rollback(self):
        """test a basic rollback"""

        tlengine.begin()
        tlengine.execute(users.insert(), user_id=1, user_name='user1')
        tlengine.execute(users.insert(), user_id=2, user_name='user2')
        tlengine.execute(users.insert(), user_id=3, user_name='user3')
        tlengine.rollback()
        external_connection = tlengine.connect()
        result = external_connection.execute('select * from query_users'
                )
        try:
            assert len(result.fetchall()) == 0
        finally:
            external_connection.close()

    def test_commit(self):
        """test a basic commit"""

        tlengine.begin()
        tlengine.execute(users.insert(), user_id=1, user_name='user1')
        tlengine.execute(users.insert(), user_id=2, user_name='user2')
        tlengine.execute(users.insert(), user_id=3, user_name='user3')
        tlengine.commit()
        external_connection = tlengine.connect()
        result = external_connection.execute('select * from query_users'
                )
        try:
            assert len(result.fetchall()) == 3
        finally:
            external_connection.close()

    def test_with_interface(self):
        trans = tlengine.begin()
        tlengine.execute(users.insert(), user_id=1, user_name='user1')
        tlengine.execute(users.insert(), user_id=2, user_name='user2')
        trans.commit()

        trans = tlengine.begin()
        tlengine.execute(users.insert(), user_id=3, user_name='user3')
        trans.__exit__(Exception, "fake", None)
        trans = tlengine.begin()
        tlengine.execute(users.insert(), user_id=4, user_name='user4')
        trans.__exit__(None, None, None)
        eq_(
            tlengine.execute(users.select().order_by(users.c.user_id)).fetchall(),
            [
                (1, 'user1'),
                (2, 'user2'),
                (4, 'user4'),
            ]
        )

    def test_commits(self):
        connection = tlengine.connect()
        assert connection.execute('select count(*) from query_users'
                                  ).scalar() == 0
        connection.close()
        connection = tlengine.contextual_connect()
        transaction = connection.begin()
        connection.execute(users.insert(), user_id=1, user_name='user1')
        transaction.commit()
        transaction = connection.begin()
        connection.execute(users.insert(), user_id=2, user_name='user2')
        connection.execute(users.insert(), user_id=3, user_name='user3')
        transaction.commit()
        transaction = connection.begin()
        result = connection.execute('select * from query_users')
        l = result.fetchall()
        assert len(l) == 3, 'expected 3 got %d' % len(l)
        transaction.commit()
        connection.close()

    def test_rollback_off_conn(self):

        # test that a TLTransaction opened off a TLConnection allows
        # that TLConnection to be aware of the transactional context

        conn = tlengine.contextual_connect()
        trans = conn.begin()
        conn.execute(users.insert(), user_id=1, user_name='user1')
        conn.execute(users.insert(), user_id=2, user_name='user2')
        conn.execute(users.insert(), user_id=3, user_name='user3')
        trans.rollback()
        external_connection = tlengine.connect()
        result = external_connection.execute('select * from query_users'
                )
        try:
            assert len(result.fetchall()) == 0
        finally:
            conn.close()
            external_connection.close()

    def test_morerollback_off_conn(self):

        # test that an existing TLConnection automatically takes place
        # in a TLTransaction opened on a second TLConnection

        conn = tlengine.contextual_connect()
        conn2 = tlengine.contextual_connect()
        trans = conn2.begin()
        conn.execute(users.insert(), user_id=1, user_name='user1')
        conn.execute(users.insert(), user_id=2, user_name='user2')
        conn.execute(users.insert(), user_id=3, user_name='user3')
        trans.rollback()
        external_connection = tlengine.connect()
        result = external_connection.execute('select * from query_users'
                )
        try:
            assert len(result.fetchall()) == 0
        finally:
            conn.close()
            conn2.close()
            external_connection.close()

    def test_commit_off_connection(self):
        conn = tlengine.contextual_connect()
        trans = conn.begin()
        conn.execute(users.insert(), user_id=1, user_name='user1')
        conn.execute(users.insert(), user_id=2, user_name='user2')
        conn.execute(users.insert(), user_id=3, user_name='user3')
        trans.commit()
        external_connection = tlengine.connect()
        result = external_connection.execute('select * from query_users'
                )
        try:
            assert len(result.fetchall()) == 3
        finally:
            conn.close()
            external_connection.close()

    def test_nesting_rollback(self):
        """tests nesting of transactions, rollback at the end"""

        external_connection = tlengine.connect()
        self.assert_(external_connection.connection
                     is not tlengine.contextual_connect().connection)
        tlengine.begin()
        tlengine.execute(users.insert(), user_id=1, user_name='user1')
        tlengine.execute(users.insert(), user_id=2, user_name='user2')
        tlengine.execute(users.insert(), user_id=3, user_name='user3')
        tlengine.begin()
        tlengine.execute(users.insert(), user_id=4, user_name='user4')
        tlengine.execute(users.insert(), user_id=5, user_name='user5')
        tlengine.commit()
        tlengine.rollback()
        try:
            self.assert_(external_connection.scalar(
                        'select count(*) from query_users'
                         ) == 0)
        finally:
            external_connection.close()

    def test_nesting_commit(self):
        """tests nesting of transactions, commit at the end."""

        external_connection = tlengine.connect()
        self.assert_(external_connection.connection
                     is not tlengine.contextual_connect().connection)
        tlengine.begin()
        tlengine.execute(users.insert(), user_id=1, user_name='user1')
        tlengine.execute(users.insert(), user_id=2, user_name='user2')
        tlengine.execute(users.insert(), user_id=3, user_name='user3')
        tlengine.begin()
        tlengine.execute(users.insert(), user_id=4, user_name='user4')
        tlengine.execute(users.insert(), user_id=5, user_name='user5')
        tlengine.commit()
        tlengine.commit()
        try:
            self.assert_(external_connection.scalar(
                        'select count(*) from query_users'
                         ) == 5)
        finally:
            external_connection.close()

    def test_mixed_nesting(self):
        """tests nesting of transactions off the TLEngine directly
        inside of transactions off the connection from the TLEngine"""

        external_connection = tlengine.connect()
        self.assert_(external_connection.connection
                     is not tlengine.contextual_connect().connection)
        conn = tlengine.contextual_connect()
        trans = conn.begin()
        trans2 = conn.begin()
        tlengine.execute(users.insert(), user_id=1, user_name='user1')
        tlengine.execute(users.insert(), user_id=2, user_name='user2')
        tlengine.execute(users.insert(), user_id=3, user_name='user3')
        tlengine.begin()
        tlengine.execute(users.insert(), user_id=4, user_name='user4')
        tlengine.begin()
        tlengine.execute(users.insert(), user_id=5, user_name='user5')
        tlengine.execute(users.insert(), user_id=6, user_name='user6')
        tlengine.execute(users.insert(), user_id=7, user_name='user7')
        tlengine.commit()
        tlengine.execute(users.insert(), user_id=8, user_name='user8')
        tlengine.commit()
        trans2.commit()
        trans.rollback()
        conn.close()
        try:
            self.assert_(external_connection.scalar(
                        'select count(*) from query_users'
                         ) == 0)
        finally:
            external_connection.close()

    def test_more_mixed_nesting(self):
        """tests nesting of transactions off the connection from the
        TLEngine inside of transactions off the TLEngine directly."""

        external_connection = tlengine.connect()
        self.assert_(external_connection.connection
                     is not tlengine.contextual_connect().connection)
        tlengine.begin()
        connection = tlengine.contextual_connect()
        connection.execute(users.insert(), user_id=1, user_name='user1')
        tlengine.begin()
        connection.execute(users.insert(), user_id=2, user_name='user2')
        connection.execute(users.insert(), user_id=3, user_name='user3')
        trans = connection.begin()
        connection.execute(users.insert(), user_id=4, user_name='user4')
        connection.execute(users.insert(), user_id=5, user_name='user5')
        trans.commit()
        tlengine.commit()
        tlengine.rollback()
        connection.close()
        try:
            self.assert_(external_connection.scalar(
                        'select count(*) from query_users'
                         ) == 0)
        finally:
            external_connection.close()

    @testing.requires.savepoints
    def test_nested_subtransaction_rollback(self):
        tlengine.begin()
        tlengine.execute(users.insert(), user_id=1, user_name='user1')
        tlengine.begin_nested()
        tlengine.execute(users.insert(), user_id=2, user_name='user2')
        tlengine.rollback()
        tlengine.execute(users.insert(), user_id=3, user_name='user3')
        tlengine.commit()
        tlengine.close()
        eq_(tlengine.execute(select([users.c.user_id]).
            order_by(users.c.user_id)).fetchall(),
            [(1, ), (3, )])
        tlengine.close()

    @testing.requires.savepoints
    @testing.crashes('oracle+zxjdbc',
                     'Errors out and causes subsequent tests to '
                     'deadlock')
    def test_nested_subtransaction_commit(self):
        tlengine.begin()
        tlengine.execute(users.insert(), user_id=1, user_name='user1')
        tlengine.begin_nested()
        tlengine.execute(users.insert(), user_id=2, user_name='user2')
        tlengine.commit()
        tlengine.execute(users.insert(), user_id=3, user_name='user3')
        tlengine.commit()
        tlengine.close()
        eq_(tlengine.execute(select([users.c.user_id]).
            order_by(users.c.user_id)).fetchall(),
            [(1, ), (2, ), (3, )])
        tlengine.close()

    @testing.requires.savepoints
    def test_rollback_to_subtransaction(self):
        tlengine.begin()
        tlengine.execute(users.insert(), user_id=1, user_name='user1')
        tlengine.begin_nested()
        tlengine.execute(users.insert(), user_id=2, user_name='user2')
        tlengine.begin()
        tlengine.execute(users.insert(), user_id=3, user_name='user3')
        tlengine.rollback()
        tlengine.rollback()
        tlengine.execute(users.insert(), user_id=4, user_name='user4')
        tlengine.commit()
        tlengine.close()
        eq_(tlengine.execute(select([users.c.user_id]).
            order_by(users.c.user_id)).fetchall(),
            [(1, ), (4, )])
        tlengine.close()

    def test_connections(self):
        """tests that contextual_connect is threadlocal"""

        c1 = tlengine.contextual_connect()
        c2 = tlengine.contextual_connect()
        assert c1.connection is c2.connection
        c2.close()
        assert not c1.closed
        assert not tlengine.closed

    @testing.requires.independent_cursors
    def test_result_closing(self):
        """tests that contextual_connect is threadlocal"""

        r1 = tlengine.execute(select([1]))
        r2 = tlengine.execute(select([1]))
        row1 = r1.fetchone()
        row2 = r2.fetchone()
        r1.close()
        assert r2.connection is r1.connection
        assert not r2.connection.closed
        assert not tlengine.closed

        # close again, nothing happens since resultproxy calls close()
        # only once

        r1.close()
        assert r2.connection is r1.connection
        assert not r2.connection.closed
        assert not tlengine.closed
        r2.close()
        assert r2.connection.closed
        assert tlengine.closed

    @testing.crashes('oracle+cx_oracle', 'intermittent failures on the buildbot')
    def test_dispose(self):
        eng = testing_engine(options=dict(strategy='threadlocal'))
        result = eng.execute(select([1]))
        eng.dispose()
        eng.execute(select([1]))

    @testing.requires.two_phase_transactions
    def test_two_phase_transaction(self):
        tlengine.begin_twophase()
        tlengine.execute(users.insert(), user_id=1, user_name='user1')
        tlengine.prepare()
        tlengine.commit()
        tlengine.begin_twophase()
        tlengine.execute(users.insert(), user_id=2, user_name='user2')
        tlengine.commit()
        tlengine.begin_twophase()
        tlengine.execute(users.insert(), user_id=3, user_name='user3')
        tlengine.rollback()
        tlengine.begin_twophase()
        tlengine.execute(users.insert(), user_id=4, user_name='user4')
        tlengine.prepare()
        tlengine.rollback()
        eq_(tlengine.execute(select([users.c.user_id]).
            order_by(users.c.user_id)).fetchall(),
            [(1, ), (2, )])


class IsolationLevelTest(fixtures.TestBase):
    __requires__ = ('isolation_level', 'ad_hoc_engines')
    __backend__ = True

    def _default_isolation_level(self):
        if testing.against('sqlite'):
            return 'SERIALIZABLE'
        elif testing.against('postgresql'):
            return 'READ COMMITTED'
        elif testing.against('mysql'):
            return "REPEATABLE READ"
        else:
            assert False, "default isolation level not known"

    def _non_default_isolation_level(self):
        if testing.against('sqlite'):
            return 'READ UNCOMMITTED'
        elif testing.against('postgresql'):
            return 'SERIALIZABLE'
        elif testing.against('mysql'):
            return "SERIALIZABLE"
        else:
            assert False, "non default isolation level not known"

    def test_engine_param_stays(self):

        eng = testing_engine()
        isolation_level = eng.dialect.get_isolation_level(
            eng.connect().connection)
        level = self._non_default_isolation_level()

        ne_(isolation_level, level)

        eng = testing_engine(options=dict(isolation_level=level))
        eq_(
            eng.dialect.get_isolation_level(
                eng.connect().connection),
            level
        )

        # check that it stays
        conn = eng.connect()
        eq_(
            eng.dialect.get_isolation_level(conn.connection),
            level
        )
        conn.close()

        conn = eng.connect()
        eq_(
            eng.dialect.get_isolation_level(conn.connection),
            level
        )
        conn.close()

    def test_default_level(self):
        eng = testing_engine(options=dict())
        isolation_level = eng.dialect.get_isolation_level(
            eng.connect().connection)
        eq_(isolation_level, self._default_isolation_level())

    def test_reset_level(self):
        eng = testing_engine(options=dict())
        conn = eng.connect()
        eq_(
            eng.dialect.get_isolation_level(conn.connection),
            self._default_isolation_level()
        )

        eng.dialect.set_isolation_level(
            conn.connection, self._non_default_isolation_level()
        )
        eq_(
            eng.dialect.get_isolation_level(conn.connection),
            self._non_default_isolation_level()
        )

        eng.dialect.reset_isolation_level(conn.connection)
        eq_(
            eng.dialect.get_isolation_level(conn.connection),
            self._default_isolation_level()
        )

        conn.close()

    def test_reset_level_with_setting(self):
        eng = testing_engine(
            options=dict(
                isolation_level=self._non_default_isolation_level()))
        conn = eng.connect()
        eq_(eng.dialect.get_isolation_level(conn.connection),
            self._non_default_isolation_level())
        eng.dialect.set_isolation_level(
            conn.connection,
            self._default_isolation_level())
        eq_(eng.dialect.get_isolation_level(conn.connection),
            self._default_isolation_level())
        eng.dialect.reset_isolation_level(conn.connection)
        eq_(eng.dialect.get_isolation_level(conn.connection),
            self._non_default_isolation_level())
        conn.close()

    def test_invalid_level(self):
        eng = testing_engine(options=dict(isolation_level='FOO'))
        assert_raises_message(
            exc.ArgumentError,
            "Invalid value '%s' for isolation_level. "
            "Valid isolation levels for %s are %s" %
            ("FOO",
             eng.dialect.name, ", ".join(eng.dialect._isolation_lookup)),
            eng.connect
        )

    def test_connection_invalidated(self):
        eng = testing_engine()
        conn = eng.connect()
        c2 = conn.execution_options(
            isolation_level=self._non_default_isolation_level())
        c2.invalidate()
        c2.connection

        # TODO: do we want to rebuild the previous isolation?
        # for now, this is current behavior so we will leave it.
        eq_(c2.get_isolation_level(), self._default_isolation_level())

    def test_per_connection(self):
        from sqlalchemy.pool import QueuePool
        eng = testing_engine(
            options=dict(
                poolclass=QueuePool,
                pool_size=2, max_overflow=0))

        c1 = eng.connect()
        c1 = c1.execution_options(
            isolation_level=self._non_default_isolation_level()
        )
        c2 = eng.connect()
        eq_(
            eng.dialect.get_isolation_level(c1.connection),
            self._non_default_isolation_level()
        )
        eq_(
            eng.dialect.get_isolation_level(c2.connection),
            self._default_isolation_level()
        )
        c1.close()
        c2.close()
        c3 = eng.connect()
        eq_(
            eng.dialect.get_isolation_level(c3.connection),
            self._default_isolation_level()
        )
        c4 = eng.connect()
        eq_(
            eng.dialect.get_isolation_level(c4.connection),
            self._default_isolation_level()
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
                    self._non_default_isolation_level()
                )
        # stays outside of transaction
        eq_(
            eng.dialect.get_isolation_level(c1.connection),
            self._non_default_isolation_level()
        )

    def test_per_statement_bzzt(self):
        assert_raises_message(
            exc.ArgumentError,
            r"'isolation_level' execution option may only be specified "
            r"on Connection.execution_options\(\), or "
            r"per-engine using the isolation_level "
            r"argument to create_engine\(\).",
            select([1]).execution_options,
            isolation_level=self._non_default_isolation_level()
        )

    def test_per_engine(self):
        # new in 0.9
        eng = create_engine(
            testing.db.url,
            execution_options={
                'isolation_level':
                    self._non_default_isolation_level()}
        )
        conn = eng.connect()
        eq_(
            eng.dialect.get_isolation_level(conn.connection),
            self._non_default_isolation_level()
        )

    def test_isolation_level_accessors_connection_default(self):
        eng = create_engine(
            testing.db.url
        )
        with eng.connect() as conn:
            eq_(conn.default_isolation_level, self._default_isolation_level())
        with eng.connect() as conn:
            eq_(conn.get_isolation_level(), self._default_isolation_level())

    def test_isolation_level_accessors_connection_option_modified(self):
        eng = create_engine(
            testing.db.url
        )
        with eng.connect() as conn:
            c2 = conn.execution_options(
                isolation_level=self._non_default_isolation_level())
            eq_(conn.default_isolation_level, self._default_isolation_level())
            eq_(conn.get_isolation_level(),
                self._non_default_isolation_level())
            eq_(c2.get_isolation_level(), self._non_default_isolation_level())
