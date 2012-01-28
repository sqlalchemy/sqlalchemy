
from test.lib.testing import eq_, assert_raises, \
    assert_raises_message, assert_warnings
from sqlalchemy import *
from sqlalchemy.orm import attributes
from sqlalchemy import exc as sa_exc, event
from sqlalchemy.orm import exc as orm_exc
from sqlalchemy.orm import *
from test.lib.util import gc_collect
from test.lib import testing
from test.lib import fixtures
from test.lib import engines
from test.orm._fixtures import FixtureTest


class SessionTransactionTest(FixtureTest):
    run_inserts = None

    def test_no_close_transaction_on_flush(self):
        User, users = self.classes.User, self.tables.users

        c = testing.db.connect()
        try:
            mapper(User, users)
            s = create_session(bind=c)
            s.begin()
            tran = s.transaction
            s.add(User(name='first'))
            s.flush()
            c.execute("select * from users")
            u = User(name='two')
            s.add(u)
            s.flush()
            u = User(name='third')
            s.add(u)
            s.flush()
            assert s.transaction is tran
            tran.close()
        finally:
            c.close()

    @engines.close_open_connections
    def test_subtransaction_on_external(self):
        users, User = self.tables.users, self.classes.User

        mapper(User, users)
        conn = testing.db.connect()
        trans = conn.begin()
        sess = create_session(bind=conn, autocommit=False, autoflush=True)
        sess.begin(subtransactions=True)
        u = User(name='ed')
        sess.add(u)
        sess.flush()
        sess.commit() # commit does nothing
        trans.rollback() # rolls back
        assert len(sess.query(User).all()) == 0
        sess.close()

    @testing.requires.savepoints
    @engines.close_open_connections
    def test_external_nested_transaction(self):
        users, User = self.tables.users, self.classes.User

        mapper(User, users)
        try:
            conn = testing.db.connect()
            trans = conn.begin()
            sess = create_session(bind=conn, autocommit=False, 
                                autoflush=True)
            u1 = User(name='u1')
            sess.add(u1)
            sess.flush()

            sess.begin_nested()
            u2 = User(name='u2')
            sess.add(u2)
            sess.flush()
            sess.rollback()

            trans.commit()
            assert len(sess.query(User).all()) == 1
        except:
            conn.close()
            raise

    @testing.requires.savepoints
    def test_heavy_nesting(self):
        users = self.tables.users

        session = create_session(bind=testing.db)
        session.begin()
        session.connection().execute(users.insert().values(name='user1'
                ))
        session.begin(subtransactions=True)
        session.begin_nested()
        session.connection().execute(users.insert().values(name='user2'
                ))
        assert session.connection().execute('select count(1) from users'
                ).scalar() == 2
        session.rollback()
        assert session.connection().execute('select count(1) from users'
                ).scalar() == 1
        session.connection().execute(users.insert().values(name='user3'
                ))
        session.commit()
        assert session.connection().execute('select count(1) from users'
                ).scalar() == 2

    @testing.requires.independent_connections
    def test_transactions_isolated(self):
        User, users = self.classes.User, self.tables.users

        mapper(User, users)
        users.delete().execute()

        s1 = create_session(bind=testing.db, autocommit=False)
        s2 = create_session(bind=testing.db, autocommit=False)
        u1 = User(name='u1')
        s1.add(u1)
        s1.flush()

        assert s2.query(User).all() == []

    @testing.requires.two_phase_transactions
    def test_twophase(self):
        users, Address, addresses, User = (self.tables.users,
                                self.classes.Address,
                                self.tables.addresses,
                                self.classes.User)

        # TODO: mock up a failure condition here
        # to ensure a rollback succeeds
        mapper(User, users)
        mapper(Address, addresses)

        engine2 = engines.testing_engine()
        sess = create_session(autocommit=True, autoflush=False, 
                            twophase=True)
        sess.bind_mapper(User, testing.db)
        sess.bind_mapper(Address, engine2)
        sess.begin()
        u1 = User(name='u1')
        a1 = Address(email_address='u1@e')
        sess.add_all((u1, a1))
        sess.commit()
        sess.close()
        engine2.dispose()
        assert users.count().scalar() == 1
        assert addresses.count().scalar() == 1

    def test_subtransaction_on_noautocommit(self):
        User, users = self.classes.User, self.tables.users

        mapper(User, users)
        sess = create_session(autocommit=False, autoflush=True)
        sess.begin(subtransactions=True)
        u = User(name='u1')
        sess.add(u)
        sess.flush()
        sess.commit() # commit does nothing
        sess.rollback() # rolls back
        assert len(sess.query(User).all()) == 0
        sess.close()

    @testing.requires.savepoints
    def test_nested_transaction(self):
        User, users = self.classes.User, self.tables.users

        mapper(User, users)
        sess = create_session()
        sess.begin()

        u = User(name='u1')
        sess.add(u)
        sess.flush()

        sess.begin_nested()  # nested transaction

        u2 = User(name='u2')
        sess.add(u2)
        sess.flush()

        sess.rollback()

        sess.commit()
        assert len(sess.query(User).all()) == 1
        sess.close()

    @testing.requires.savepoints
    def test_nested_autotrans(self):
        User, users = self.classes.User, self.tables.users

        mapper(User, users)
        sess = create_session(autocommit=False)
        u = User(name='u1')
        sess.add(u)
        sess.flush()

        sess.begin_nested()  # nested transaction

        u2 = User(name='u2')
        sess.add(u2)
        sess.flush()

        sess.rollback()

        sess.commit()
        assert len(sess.query(User).all()) == 1
        sess.close()

    @testing.requires.savepoints
    def test_nested_transaction_connection_add(self):
        users, User = self.tables.users, self.classes.User

        mapper(User, users)

        sess = create_session(autocommit=True)

        sess.begin()
        sess.begin_nested()

        u1 = User(name='u1')
        sess.add(u1)
        sess.flush()

        sess.rollback()

        u2 = User(name='u2')
        sess.add(u2)

        sess.commit()

        eq_(set(sess.query(User).all()), set([u2]))

        sess.begin()
        sess.begin_nested()

        u3 = User(name='u3')
        sess.add(u3)
        sess.commit() # commit the nested transaction
        sess.rollback()

        eq_(set(sess.query(User).all()), set([u2]))

        sess.close()

    @testing.requires.savepoints
    def test_mixed_transaction_control(self):
        users, User = self.tables.users, self.classes.User

        mapper(User, users)

        sess = create_session(autocommit=True)

        sess.begin()
        sess.begin_nested()
        transaction = sess.begin(subtransactions=True)

        sess.add(User(name='u1'))

        transaction.commit()
        sess.commit()
        sess.commit()

        sess.close()

        eq_(len(sess.query(User).all()), 1)

        t1 = sess.begin()
        t2 = sess.begin_nested()

        sess.add(User(name='u2'))

        t2.commit()
        assert sess.transaction is t1

        sess.close()

    @testing.requires.savepoints
    def test_mixed_transaction_close(self):
        users, User = self.tables.users, self.classes.User

        mapper(User, users)

        sess = create_session(autocommit=False)

        sess.begin_nested()

        sess.add(User(name='u1'))
        sess.flush()

        sess.close()

        sess.add(User(name='u2'))
        sess.commit()

        sess.close()

        eq_(len(sess.query(User).all()), 1)

    def test_error_on_using_inactive_session_commands(self):
        users, User = self.tables.users, self.classes.User

        mapper(User, users)
        sess = create_session(autocommit=True)
        sess.begin()
        sess.begin(subtransactions=True)
        sess.add(User(name='u1'))
        sess.flush()
        sess.rollback()
        assert_raises_message(sa_exc.InvalidRequestError,
                              "This Session's transaction has been "
                              r"rolled back by a nested rollback\(\) "
                              "call.  To begin a new transaction, "
                              r"issue Session.rollback\(\) first.",
                              sess.begin, subtransactions=True)
        sess.close()

    def _inactive_flushed_session_fixture(self):
        users, User = self.tables.users, self.classes.User

        mapper(User, users)
        sess = Session()
        u1 = User(id=1, name='u1')
        sess.add(u1)
        sess.commit()

        sess.add(User(id=1, name='u2'))
        assert_raises(
            orm_exc.FlushError, sess.flush
        )
        return sess, u1

    def test_warning_on_using_inactive_session_new(self):
        User = self.classes.User

        sess, u1 = self._inactive_flushed_session_fixture()
        u2 = User(name='u2')
        sess.add(u2)
        def go():
            sess.rollback()
        assert_warnings(go, 
            ["Session's state has been changed on a "
            "non-active transaction - this state "
            "will be discarded."],
        )
        assert u2 not in sess
        assert u1 in sess

    def test_warning_on_using_inactive_session_dirty(self):
        sess, u1 = self._inactive_flushed_session_fixture()
        u1.name = 'newname'
        def go():
            sess.rollback()
        assert_warnings(go, 
            ["Session's state has been changed on a "
            "non-active transaction - this state "
            "will be discarded."],
        )
        assert u1 in sess
        assert u1 not in sess.dirty

    def test_warning_on_using_inactive_session_delete(self):
        sess, u1 = self._inactive_flushed_session_fixture()
        sess.delete(u1)
        def go():
            sess.rollback()
        assert_warnings(go, 
            ["Session's state has been changed on a "
            "non-active transaction - this state "
            "will be discarded."],
        )
        assert u1 in sess
        assert u1 not in sess.deleted

    def test_preserve_flush_error(self):
        User = self.classes.User

        sess, u1 = self._inactive_flushed_session_fixture()

        for i in range(5):
            assert_raises_message(sa_exc.InvalidRequestError,
                              "^This Session's transaction has been "
                              r"rolled back due to a previous exception "
                              "during flush. To "
                              "begin a new transaction with this "
                              "Session, first issue "
                              r"Session.rollback\(\). Original exception "
                              "was:",
                              sess.commit)
        sess.rollback()
        sess.add(User(id=5, name='some name'))
        sess.commit()

    def test_no_autocommit_with_explicit_commit(self):
        User, users = self.classes.User, self.tables.users

        mapper(User, users)
        session = create_session(autocommit=False)
        session.add(User(name='ed'))
        session.transaction.commit()
        assert session.transaction is not None, \
            'autocommit=False should start a new transaction'

class _LocalFixture(FixtureTest):
    run_setup_mappers = 'once'
    run_inserts = None
    session = sessionmaker()

    @classmethod
    def setup_mappers(cls):
        User, Address = cls.classes.User, cls.classes.Address
        users, addresses = cls.tables.users, cls.tables.addresses
        mapper(User, users, properties={
            'addresses':relationship(Address, backref='user',
                                 cascade="all, delete-orphan", 
                                    order_by=addresses.c.id),
            })
        mapper(Address, addresses)


class FixtureDataTest(_LocalFixture):
    run_inserts = 'each'

    def test_attrs_on_rollback(self):
        User = self.classes.User
        sess = self.session()
        u1 = sess.query(User).get(7)
        u1.name = 'ed'
        sess.rollback()
        eq_(u1.name, 'jack')

    def test_commit_persistent(self):
        User = self.classes.User
        sess = self.session()
        u1 = sess.query(User).get(7)
        u1.name = 'ed'
        sess.flush()
        sess.commit()
        eq_(u1.name, 'ed')

    def test_concurrent_commit_persistent(self):
        User = self.classes.User
        s1 = self.session()
        u1 = s1.query(User).get(7)
        u1.name = 'ed'
        s1.commit()

        s2 = self.session()
        u2 = s2.query(User).get(7)
        assert u2.name == 'ed'
        u2.name = 'will'
        s2.commit()

        assert u1.name == 'will'

class AutoExpireTest(_LocalFixture):

    def test_expunge_pending_on_rollback(self):
        User = self.classes.User
        sess = self.session()
        u2= User(name='newuser')
        sess.add(u2)
        assert u2 in sess
        sess.rollback()
        assert u2 not in sess

    def test_trans_pending_cleared_on_commit(self):
        User = self.classes.User
        sess = self.session()
        u2= User(name='newuser')
        sess.add(u2)
        assert u2 in sess
        sess.commit()
        assert u2 in sess
        u3 = User(name='anotheruser')
        sess.add(u3)
        sess.rollback()
        assert u3 not in sess
        assert u2 in sess

    def test_update_deleted_on_rollback(self):
        User = self.classes.User
        s = self.session()
        u1 = User(name='ed')
        s.add(u1)
        s.commit()

        # this actually tests that the delete() operation,
        # when cascaded to the "addresses" collection, does not
        # trigger a flush (via lazyload) before the cascade is complete.
        s.delete(u1)
        assert u1 in s.deleted
        s.rollback()
        assert u1 in s
        assert u1 not in s.deleted

    def test_gced_delete_on_rollback(self):
        User, users = self.classes.User, self.tables.users

        s = self.session()
        u1 = User(name='ed')
        s.add(u1)
        s.commit()

        s.delete(u1)
        u1_state = attributes.instance_state(u1)
        assert u1_state in s.identity_map.all_states()
        assert u1_state in s._deleted
        s.flush()
        assert u1_state not in s.identity_map.all_states()
        assert u1_state not in s._deleted
        del u1
        gc_collect()
        assert u1_state.obj() is None

        s.rollback()
        assert u1_state in s.identity_map.all_states()
        u1 = s.query(User).filter_by(name='ed').one()
        assert u1_state not in s.identity_map.all_states()
        assert s.scalar(users.count()) == 1
        s.delete(u1)
        s.flush()
        assert s.scalar(users.count()) == 0
        s.commit()

    def test_trans_deleted_cleared_on_rollback(self):
        User = self.classes.User
        s = self.session()
        u1 = User(name='ed')
        s.add(u1)
        s.commit()

        s.delete(u1)
        s.commit()
        assert u1 not in s
        s.rollback()
        assert u1 not in s

    def test_update_deleted_on_rollback_cascade(self):
        User, Address = self.classes.User, self.classes.Address

        s = self.session()
        u1 = User(name='ed', addresses=[Address(email_address='foo')])
        s.add(u1)
        s.commit()

        s.delete(u1)
        assert u1 in s.deleted
        assert u1.addresses[0] in s.deleted
        s.rollback()
        assert u1 in s
        assert u1 not in s.deleted
        assert u1.addresses[0] not in s.deleted

    def test_update_deleted_on_rollback_orphan(self):
        User, Address = self.classes.User, self.classes.Address

        s = self.session()
        u1 = User(name='ed', addresses=[Address(email_address='foo')])
        s.add(u1)
        s.commit()

        a1 = u1.addresses[0]
        u1.addresses.remove(a1)

        s.flush()
        eq_(s.query(Address).filter(Address.email_address=='foo').all(), 
                [])
        s.rollback()
        assert a1 not in s.deleted
        assert u1.addresses == [a1]

    def test_commit_pending(self):
        User = self.classes.User
        sess = self.session()
        u1 = User(name='newuser')
        sess.add(u1)
        sess.flush()
        sess.commit()
        eq_(u1.name, 'newuser')


    def test_concurrent_commit_pending(self):
        User = self.classes.User
        s1 = self.session()
        u1 = User(name='edward')
        s1.add(u1)
        s1.commit()

        s2 = self.session()
        u2 = s2.query(User).filter(User.name=='edward').one()
        u2.name = 'will'
        s2.commit()

        assert u1.name == 'will'

class TwoPhaseTest(_LocalFixture):

    @testing.requires.two_phase_transactions
    def test_rollback_on_prepare(self):
        User = self.classes.User
        s = self.session(twophase=True)

        u = User(name='ed')
        s.add(u)
        s.prepare()
        s.rollback()

        assert u not in s

class RollbackRecoverTest(_LocalFixture):

    def test_pk_violation(self):
        User, Address = self.classes.User, self.classes.Address
        s = self.session()
        a1 = Address(email_address='foo')
        u1 = User(id=1, name='ed', addresses=[a1])
        s.add(u1)
        s.commit()

        a2 = Address(email_address='bar')
        u2 = User(id=1, name='jack', addresses=[a2])

        u1.name = 'edward'
        a1.email_address = 'foober'
        s.add(u2)
        assert_raises(sa_exc.FlushError, s.commit)
        assert_raises(sa_exc.InvalidRequestError, s.commit)
        s.rollback()
        assert u2 not in s
        assert a2 not in s
        assert u1 in s
        assert a1 in s
        assert u1.name == 'ed'
        assert a1.email_address == 'foo'
        u1.name = 'edward'
        a1.email_address = 'foober'
        s.commit()
        eq_(
            s.query(User).all(),
            [User(id=1, name='edward', 
                addresses=[Address(email_address='foober')])]
        )

    @testing.requires.savepoints
    def test_pk_violation_with_savepoint(self):
        User, Address = self.classes.User, self.classes.Address
        s = self.session()
        a1 = Address(email_address='foo')
        u1 = User(id=1, name='ed', addresses=[a1])
        s.add(u1)
        s.commit()

        a2 = Address(email_address='bar')
        u2 = User(id=1, name='jack', addresses=[a2])

        u1.name = 'edward'
        a1.email_address = 'foober'
        s.begin_nested()
        s.add(u2)
        assert_raises(sa_exc.FlushError, s.commit)
        assert_raises(sa_exc.InvalidRequestError, s.commit)
        s.rollback()
        assert u2 not in s
        assert a2 not in s
        assert u1 in s
        assert a1 in s

        s.commit()
        eq_(
            s.query(User).all(),
            [User(id=1, name='edward', 
                addresses=[Address(email_address='foober')])]
        )


class SavepointTest(_LocalFixture):

    @testing.requires.savepoints
    def test_savepoint_rollback(self):
        User = self.classes.User
        s = self.session()
        u1 = User(name='ed')
        u2 = User(name='jack')
        s.add_all([u1, u2])

        s.begin_nested()
        u3 = User(name='wendy')
        u4 = User(name='foo')
        u1.name = 'edward'
        u2.name = 'jackward'
        s.add_all([u3, u4])
        eq_(s.query(User.name).order_by(User.id).all(), 
                    [('edward',), ('jackward',), ('wendy',), ('foo',)])
        s.rollback()
        assert u1.name == 'ed'
        assert u2.name == 'jack'
        eq_(s.query(User.name).order_by(User.id).all(), 
                    [('ed',), ('jack',)])
        s.commit()
        assert u1.name == 'ed'
        assert u2.name == 'jack'
        eq_(s.query(User.name).order_by(User.id).all(), 
                    [('ed',), ('jack',)])

    @testing.requires.savepoints
    def test_savepoint_delete(self):
        User = self.classes.User
        s = self.session()
        u1 = User(name='ed')
        s.add(u1)
        s.commit()
        eq_(s.query(User).filter_by(name='ed').count(), 1)
        s.begin_nested()
        s.delete(u1)
        s.commit()
        eq_(s.query(User).filter_by(name='ed').count(), 0)
        s.commit()

    @testing.requires.savepoints
    def test_savepoint_commit(self):
        User = self.classes.User
        s = self.session()
        u1 = User(name='ed')
        u2 = User(name='jack')
        s.add_all([u1, u2])

        s.begin_nested()
        u3 = User(name='wendy')
        u4 = User(name='foo')
        u1.name = 'edward'
        u2.name = 'jackward'
        s.add_all([u3, u4])
        eq_(s.query(User.name).order_by(User.id).all(), 
                [('edward',), ('jackward',), ('wendy',), ('foo',)])
        s.commit()
        def go():
            assert u1.name == 'edward'
            assert u2.name == 'jackward'
            eq_(s.query(User.name).order_by(User.id).all(), 
                    [('edward',), ('jackward',), ('wendy',), ('foo',)])
        self.assert_sql_count(testing.db, go, 1)

        s.commit()
        eq_(s.query(User.name).order_by(User.id).all(), 
                [('edward',), ('jackward',), ('wendy',), ('foo',)])

    @testing.requires.savepoints
    def test_savepoint_rollback_collections(self):
        User, Address = self.classes.User, self.classes.Address
        s = self.session()
        u1 = User(name='ed', addresses=[Address(email_address='foo')])
        s.add(u1)
        s.commit()

        u1.name='edward'
        u1.addresses.append(Address(email_address='bar'))
        s.begin_nested()
        u2 = User(name='jack', addresses=[Address(email_address='bat')])
        s.add(u2)
        eq_(s.query(User).order_by(User.id).all(),
            [
                User(name='edward', addresses=[Address(email_address='foo'), 
                                        Address(email_address='bar')]),
                User(name='jack', addresses=[Address(email_address='bat')])
            ]
        )
        s.rollback()
        eq_(s.query(User).order_by(User.id).all(),
            [
                User(name='edward', addresses=[Address(email_address='foo'), 
                                        Address(email_address='bar')]),
            ]
        )
        s.commit()
        eq_(s.query(User).order_by(User.id).all(),
            [
                User(name='edward', addresses=[Address(email_address='foo'), 
                                        Address(email_address='bar')]),
            ]
        )

    @testing.requires.savepoints
    def test_savepoint_commit_collections(self):
        User, Address = self.classes.User, self.classes.Address
        s = self.session()
        u1 = User(name='ed', addresses=[Address(email_address='foo')])
        s.add(u1)
        s.commit()

        u1.name='edward'
        u1.addresses.append(Address(email_address='bar'))
        s.begin_nested()
        u2 = User(name='jack', addresses=[Address(email_address='bat')])
        s.add(u2)
        eq_(s.query(User).order_by(User.id).all(),
            [
                User(name='edward', addresses=[Address(email_address='foo'), Address(email_address='bar')]),
                User(name='jack', addresses=[Address(email_address='bat')])
            ]
        )
        s.commit()
        eq_(s.query(User).order_by(User.id).all(),
            [
                User(name='edward', addresses=[Address(email_address='foo'), Address(email_address='bar')]),
                User(name='jack', addresses=[Address(email_address='bat')])
            ]
        )
        s.commit()
        eq_(s.query(User).order_by(User.id).all(),
            [
                User(name='edward', addresses=[Address(email_address='foo'), Address(email_address='bar')]),
                User(name='jack', addresses=[Address(email_address='bat')])
            ]
        )

    @testing.requires.savepoints
    def test_expunge_pending_on_rollback(self):
        User = self.classes.User
        sess = self.session()

        sess.begin_nested()
        u2= User(name='newuser')
        sess.add(u2)
        assert u2 in sess
        sess.rollback()
        assert u2 not in sess

    @testing.requires.savepoints
    def test_update_deleted_on_rollback(self):
        User = self.classes.User
        s = self.session()
        u1 = User(name='ed')
        s.add(u1)
        s.commit()

        s.begin_nested()
        s.delete(u1)
        assert u1 in s.deleted
        s.rollback()
        assert u1 in s
        assert u1 not in s.deleted


class AccountingFlagsTest(_LocalFixture):
    def test_no_expire_on_commit(self):
        User, users = self.classes.User, self.tables.users

        sess = sessionmaker(expire_on_commit=False)()
        u1 = User(name='ed')
        sess.add(u1)
        sess.commit()

        testing.db.execute(users.update(users.c.name=='ed').values(name='edward'))

        assert u1.name == 'ed'
        sess.expire_all()
        assert u1.name == 'edward'

    def test_rollback_no_accounting(self):
        User, users = self.classes.User, self.tables.users

        sess = sessionmaker(_enable_transaction_accounting=False)()
        u1 = User(name='ed')
        sess.add(u1)
        sess.commit()

        u1.name = 'edwardo'
        sess.rollback()

        testing.db.execute(users.update(users.c.name=='ed').values(name='edward'))

        assert u1.name == 'edwardo'
        sess.expire_all()
        assert u1.name == 'edward'

    def test_commit_no_accounting(self):
        User, users = self.classes.User, self.tables.users

        sess = sessionmaker(_enable_transaction_accounting=False)()
        u1 = User(name='ed')
        sess.add(u1)
        sess.commit()

        u1.name = 'edwardo'
        sess.rollback()

        testing.db.execute(users.update(users.c.name=='ed').values(name='edward'))

        assert u1.name == 'edwardo'
        sess.commit()

        assert testing.db.execute(select([users.c.name])).fetchall() == [('edwardo',)]
        assert u1.name == 'edwardo'

        sess.delete(u1)
        sess.commit()

    def test_preflush_no_accounting(self):
        User, users = self.classes.User, self.tables.users

        sess = Session(_enable_transaction_accounting=False, 
                        autocommit=True, autoflush=False)
        u1 = User(name='ed')
        sess.add(u1)
        sess.flush()

        sess.begin()
        u1.name = 'edwardo'
        u2 = User(name="some other user")
        sess.add(u2)

        sess.rollback()

        sess.begin()
        assert testing.db.execute(select([users.c.name])).fetchall() == [('ed',)]


class AutoCommitTest(_LocalFixture):
    def test_begin_nested_requires_trans(self):
        sess = create_session(autocommit=True)
        assert_raises(sa_exc.InvalidRequestError, sess.begin_nested)

    def test_begin_preflush(self):
        User = self.classes.User
        sess = create_session(autocommit=True)

        u1 = User(name='ed')
        sess.add(u1)

        sess.begin()
        u2 = User(name='some other user')
        sess.add(u2)
        sess.rollback()
        assert u2 not in sess
        assert u1 in sess
        assert sess.query(User).filter_by(name='ed').one() is u1

    def test_accounting_commit_fails_add(self):
        User = self.classes.User
        sess = create_session(autocommit=True)

        fail = False
        def fail_fn(*arg, **kw):
            if fail:
                raise Exception("commit fails")

        event.listen(sess, "after_flush_postexec", fail_fn)
        u1 = User(name='ed')
        sess.add(u1)

        fail = True
        assert_raises(
            Exception,
            sess.flush
        )
        fail = False

        assert u1 not in sess
        u1new = User(id=2, name='fred')
        sess.add(u1new)
        sess.add(u1)
        sess.flush()
        assert u1 in sess
        eq_(
            sess.query(User.name).order_by(User.name).all(),
            [('ed', ), ('fred',)]
        )

    def test_accounting_commit_fails_delete(self):
        User = self.classes.User
        sess = create_session(autocommit=True)

        fail = False
        def fail_fn(*arg, **kw):
            if fail:
                raise Exception("commit fails")

        event.listen(sess, "after_flush_postexec", fail_fn)
        u1 = User(name='ed')
        sess.add(u1)
        sess.flush()

        sess.delete(u1)
        fail = True
        assert_raises(
            Exception,
            sess.flush
        )
        fail = False

        assert u1 in sess
        assert u1 not in sess.deleted
        sess.delete(u1)
        sess.flush()
        assert u1 not in sess
        eq_(
            sess.query(User.name).order_by(User.name).all(),
            []
        )

    @testing.requires.updateable_autoincrement_pks
    def test_accounting_no_select_needed(self):
        """test that flush accounting works on non-expired instances
        when autocommit=True/expire_on_commit=True."""

        User = self.classes.User
        sess = create_session(autocommit=True, expire_on_commit=True)

        u1 = User(id=1, name='ed')
        sess.add(u1)
        sess.flush()

        u1.id = 3
        u1.name = 'fred'
        self.assert_sql_count(testing.db, sess.flush, 1)
        assert 'id' not in u1.__dict__
        eq_(u1.id, 3)

class NaturalPKRollbackTest(fixtures.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        Table('users', metadata,
            Column('name', String(50), primary_key=True)
        )

    @classmethod
    def setup_classes(cls):
        class User(cls.Comparable):
            pass

    def test_rollback_recover(self):
        users, User = self.tables.users, self.classes.User

        mapper(User, users)

        session = sessionmaker()()

        u1, u2, u3= \
            User(name='u1'),\
            User(name='u2'),\
            User(name='u3')

        session.add_all([u1, u2, u3])

        session.commit()

        session.delete(u2)
        u4 = User(name='u2')
        session.add(u4)
        session.flush()

        u5 = User(name='u3')
        session.add(u5)
        assert_raises(orm_exc.FlushError, session.flush)

        assert u5 not in session
        assert u2 not in session.deleted

        session.rollback()





