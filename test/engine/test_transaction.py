from test.lib.testing import eq_, assert_raises, \
    assert_raises_message, ne_
import sys
import time
import threading
from test.lib.engines import testing_engine
from sqlalchemy import create_engine, MetaData, INT, VARCHAR, Sequence, \
    select, Integer, String, func, text, exc
from test.lib.schema import Table
from test.lib.schema import Column
from test.lib import testing
from test.lib import fixtures


users, metadata = None, None
class TransactionTest(fixtures.TestBase):
    @classmethod
    def setup_class(cls):
        global users, metadata
        metadata = MetaData()
        users = Table('query_users', metadata,
            Column('user_id', INT, primary_key = True),
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
        except Exception , e:
            print "Exception: ", e
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
            except Exception, e:
                transaction.rollback()
                raise
        except Exception, e:
            try:
                assert str(e) == 'uh oh'  # and not "This transaction is
                                          # inactive"
            finally:
                connection.close()

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
        except Exception, e:
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
        connection.close()
        connection2 = testing.db.connect()
        eq_(connection2.execute(select([users.c.user_id]).
            order_by(users.c.user_id)).fetchall(),
            [])
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

class AutoRollbackTest(fixtures.TestBase):

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
                             r'autocommit\(\) is deprecated')
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
        inside of tranasctions off the connection from the TLEngine"""

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
        TLEngine inside of tranasctions off thbe TLEngine directly."""

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

counters = None


class ForUpdateTest(fixtures.TestBase):
    __requires__ = 'ad_hoc_engines',

    @classmethod
    def setup_class(cls):
        global counters, metadata
        metadata = MetaData()
        counters = Table('forupdate_counters', metadata,
                         Column('counter_id', INT, primary_key=True),
                         Column('counter_value', INT),
                         test_needs_acid=True)
        counters.create(testing.db)

    def teardown(self):
        testing.db.execute(counters.delete()).close()

    @classmethod
    def teardown_class(cls):
        counters.drop(testing.db)

    def increment(
        self,
        count,
        errors,
        update_style=True,
        delay=0.005,
        ):
        con = testing.db.connect()
        sel = counters.select(for_update=update_style,
                              whereclause=counters.c.counter_id == 1)
        for i in xrange(count):
            trans = con.begin()
            try:
                existing = con.execute(sel).first()
                incr = existing['counter_value'] + 1
                time.sleep(delay)
                con.execute(counters.update(counters.c.counter_id == 1,
                            values={'counter_value': incr}))
                time.sleep(delay)
                readback = con.execute(sel).first()
                if readback['counter_value'] != incr:
                    raise AssertionError('Got %s post-update, expected '
                            '%s' % (readback['counter_value'], incr))
                trans.commit()
            except Exception, e:
                trans.rollback()
                errors.append(e)
                break
        con.close()

    @testing.crashes('mssql', 'FIXME: unknown')
    @testing.crashes('firebird', 'FIXME: unknown')
    @testing.crashes('sybase', 'FIXME: unknown')
    @testing.crashes('access', 'FIXME: unknown')
    @testing.requires.independent_connections
    def test_queued_update(self):
        """Test SELECT FOR UPDATE with concurrent modifications.

        Runs concurrent modifications on a single row in the users
        table, with each mutator trying to increment a value stored in
        user_name.

        """

        db = testing.db
        db.execute(counters.insert(), counter_id=1, counter_value=0)
        iterations, thread_count = 10, 5
        threads, errors = [], []
        for i in xrange(thread_count):
            thrd = threading.Thread(target=self.increment,
                                    args=(iterations, ),
                                    kwargs={'errors': errors,
                                    'update_style': True})
            thrd.start()
            threads.append(thrd)
        for thrd in threads:
            thrd.join()
        for e in errors:
            sys.stdout.write('Failure: %s\n' % e)
        self.assert_(len(errors) == 0)
        sel = counters.select(whereclause=counters.c.counter_id == 1)
        final = db.execute(sel).first()
        self.assert_(final['counter_value'] == iterations
                     * thread_count)

    def overlap(
        self,
        ids,
        errors,
        update_style,
        ):
        sel = counters.select(for_update=update_style,
                              whereclause=counters.c.counter_id.in_(ids))
        con = testing.db.connect()
        trans = con.begin()
        try:
            rows = con.execute(sel).fetchall()
            time.sleep(0.25)
            trans.commit()
        except Exception, e:
            trans.rollback()
            errors.append(e)
        con.close()

    def _threaded_overlap(
        self,
        thread_count,
        groups,
        update_style=True,
        pool=5,
        ):
        db = testing.db
        for cid in range(pool - 1):
            db.execute(counters.insert(), counter_id=cid + 1,
                       counter_value=0)
        errors, threads = [], []
        for i in xrange(thread_count):
            thrd = threading.Thread(target=self.overlap,
                                    args=(groups.pop(0), errors,
                                    update_style))
            thrd.start()
            threads.append(thrd)
        for thrd in threads:
            thrd.join()
        return errors

    @testing.crashes('mssql', 'FIXME: unknown')
    @testing.crashes('firebird', 'FIXME: unknown')
    @testing.crashes('sybase', 'FIXME: unknown')
    @testing.crashes('access', 'FIXME: unknown')
    @testing.requires.independent_connections
    def test_queued_select(self):
        """Simple SELECT FOR UPDATE conflict test"""

        errors = self._threaded_overlap(2, [(1, 2, 3), (3, 4, 5)])
        for e in errors:
            sys.stderr.write('Failure: %s\n' % e)
        self.assert_(len(errors) == 0)

    @testing.crashes('mssql', 'FIXME: unknown')
    @testing.fails_on('mysql', 'No support for NOWAIT')
    @testing.crashes('firebird', 'FIXME: unknown')
    @testing.crashes('sybase', 'FIXME: unknown')
    @testing.crashes('access', 'FIXME: unknown')
    @testing.requires.independent_connections
    def test_nowait_select(self):
        """Simple SELECT FOR UPDATE NOWAIT conflict test"""

        errors = self._threaded_overlap(2, [(1, 2, 3), (3, 4, 5)],
                update_style='nowait')
        self.assert_(len(errors) != 0)

class IsolationLevelTest(fixtures.TestBase):
    __requires__ = ('isolation_level', 'ad_hoc_engines')

    def _default_isolation_level(self):
        if testing.against('sqlite'):
            return 'SERIALIZABLE'
        elif testing.against('postgresql'):
            return 'READ COMMITTED'
        else:
            assert False, "default isolation level not known"

    def _non_default_isolation_level(self):
        if testing.against('sqlite'):
            return 'READ UNCOMMITTED'
        elif testing.against('postgresql'):
            return 'SERIALIZABLE'
        else:
            assert False, "non default isolation level not known"

    def test_engine_param_stays(self):

        eng = testing_engine()
        isolation_level = eng.dialect.get_isolation_level(eng.connect().connection)
        level = self._non_default_isolation_level()

        ne_(isolation_level, level)

        eng = testing_engine(options=dict(isolation_level=level))
        eq_(
            eng.dialect.get_isolation_level(eng.connect().connection),
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
        isolation_level = eng.dialect.get_isolation_level(eng.connect().connection)
        eq_(isolation_level, self._default_isolation_level())

    def test_reset_level(self):
        eng = testing_engine(options=dict())
        conn = eng.connect()
        eq_(eng.dialect.get_isolation_level(conn.connection), self._default_isolation_level())

        eng.dialect.set_isolation_level(conn.connection, self._non_default_isolation_level())
        eq_(eng.dialect.get_isolation_level(conn.connection), self._non_default_isolation_level())

        eng.dialect.reset_isolation_level(conn.connection)
        eq_(eng.dialect.get_isolation_level(conn.connection), self._default_isolation_level())

        conn.close()

    def test_reset_level_with_setting(self):
        eng = testing_engine(options=dict(isolation_level=self._non_default_isolation_level()))
        conn = eng.connect()
        eq_(eng.dialect.get_isolation_level(conn.connection), self._non_default_isolation_level())

        eng.dialect.set_isolation_level(conn.connection, self._default_isolation_level())
        eq_(eng.dialect.get_isolation_level(conn.connection), self._default_isolation_level())

        eng.dialect.reset_isolation_level(conn.connection)
        eq_(eng.dialect.get_isolation_level(conn.connection), self._non_default_isolation_level())

        conn.close()

    def test_invalid_level(self):
        eng = testing_engine(options=dict(isolation_level='FOO'))
        assert_raises_message(
            exc.ArgumentError, 
                "Invalid value '%s' for isolation_level. "
                "Valid isolation levels for %s are %s" % 
                ("FOO", eng.dialect.name, ", ".join(eng.dialect._isolation_lookup)),
            eng.connect)

    def test_per_connection(self):
        from sqlalchemy.pool import QueuePool
        eng = testing_engine(options=dict(poolclass=QueuePool, pool_size=2, max_overflow=0))

        c1 = eng.connect()
        c1 = c1.execution_options(isolation_level=self._non_default_isolation_level())

        c2 = eng.connect()
        eq_(eng.dialect.get_isolation_level(c1.connection), self._non_default_isolation_level())
        eq_(eng.dialect.get_isolation_level(c2.connection), self._default_isolation_level())

        c1.close()
        c2.close()
        c3 = eng.connect()
        eq_(eng.dialect.get_isolation_level(c3.connection), self._default_isolation_level())

        c4 = eng.connect()
        eq_(eng.dialect.get_isolation_level(c4.connection), self._default_isolation_level())

        c3.close()
        c4.close()

    def test_per_statement_bzzt(self):
        assert_raises_message(
            exc.ArgumentError,
            r"'isolation_level' execution option may only be specified "
            r"on Connection.execution_options\(\), or "
            r"per-engine using the isolation_level "
            r"argument to create_engine\(\).",
            select([1]).execution_options, isolation_level=self._non_default_isolation_level()
        )


    def test_per_engine_bzzt(self):
        assert_raises_message(
            exc.ArgumentError,
            r"'isolation_level' execution option may "
            r"only be specified on Connection.execution_options\(\). "
            r"To set engine-wide isolation level, "
            r"use the isolation_level argument to create_engine\(\).",
            create_engine,
            testing.db.url, execution_options={'isolation_level':self._non_default_isolation_level}
        )
