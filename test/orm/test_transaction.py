from __future__ import with_statement
from sqlalchemy import (
    testing, exc as sa_exc, event, String, Column, Table, select, func)
from sqlalchemy.testing import (
    fixtures, engines, eq_, assert_raises, assert_raises_message,
    assert_warnings, mock, expect_warnings, is_, is_not_)
from sqlalchemy.orm import (
    exc as orm_exc, Session, mapper, sessionmaker, create_session,
    relationship, attributes, session as _session)
from sqlalchemy.testing.util import gc_collect
from test.orm._fixtures import FixtureTest
from sqlalchemy import inspect

class SessionTransactionTest(FixtureTest):
    run_inserts = None
    __backend__ = True

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
        sess.commit()  # commit does nothing
        trans.rollback()  # rolls back
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
    def test_nested_accounting_new_items_removed(self):
        User, users = self.classes.User, self.tables.users

        mapper(User, users)

        session = create_session(bind=testing.db)
        session.begin()
        session.begin_nested()
        u1 = User(name='u1')
        session.add(u1)
        session.commit()
        assert u1 in session
        session.rollback()
        assert u1 not in session

    @testing.requires.savepoints
    def test_nested_accounting_deleted_items_restored(self):
        User, users = self.classes.User, self.tables.users

        mapper(User, users)

        session = create_session(bind=testing.db)
        session.begin()
        u1 = User(name='u1')
        session.add(u1)
        session.commit()

        session.begin()
        u1 = session.query(User).first()

        session.begin_nested()
        session.delete(u1)
        session.commit()
        assert u1 not in session
        session.rollback()
        assert u1 in session

    @testing.requires.savepoints
    def test_heavy_nesting(self):
        users = self.tables.users

        session = create_session(bind=testing.db)
        session.begin()
        session.connection().execute(users.insert().values(
            name='user1'))
        session.begin(subtransactions=True)
        session.begin_nested()
        session.connection().execute(users.insert().values(
            name='user2'))
        assert session.connection().execute(
            'select count(1) from users').scalar() == 2
        session.rollback()
        assert session.connection().execute(
            'select count(1) from users').scalar() == 1
        session.connection().execute(users.insert().values(
            name='user3'))
        session.commit()
        assert session.connection().execute(
            'select count(1) from users').scalar() == 2

    @testing.requires.savepoints
    def test_dirty_state_transferred_deep_nesting(self):
        User, users = self.classes.User, self.tables.users

        mapper(User, users)

        s = Session(testing.db)
        u1 = User(name='u1')
        s.add(u1)
        s.commit()

        nt1 = s.begin_nested()
        nt2 = s.begin_nested()
        u1.name = 'u2'
        assert attributes.instance_state(u1) not in nt2._dirty
        assert attributes.instance_state(u1) not in nt1._dirty
        s.flush()
        assert attributes.instance_state(u1) in nt2._dirty
        assert attributes.instance_state(u1) not in nt1._dirty

        s.commit()
        assert attributes.instance_state(u1) in nt2._dirty
        assert attributes.instance_state(u1) in nt1._dirty

        s.rollback()
        assert attributes.instance_state(u1).expired
        eq_(u1.name, 'u1')

    @testing.requires.independent_connections
    def test_transactions_isolated(self):
        User, users = self.classes.User, self.tables.users

        mapper(User, users)

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
        eq_(select([func.count('*')]).select_from(users).scalar(), 1)
        eq_(select([func.count('*')]).select_from(addresses).scalar(), 1)

    @testing.requires.independent_connections
    def test_invalidate(self):
        User, users = self.classes.User, self.tables.users
        mapper(User, users)
        sess = Session()
        u = User(name='u1')
        sess.add(u)
        sess.flush()
        c1 = sess.connection(User)

        sess.invalidate()
        assert c1.invalidated

        eq_(sess.query(User).all(), [])
        c2 = sess.connection(User)
        assert not c2.invalidated

    def test_subtransaction_on_noautocommit(self):
        User, users = self.classes.User, self.tables.users

        mapper(User, users)
        sess = create_session(autocommit=False, autoflush=True)
        sess.begin(subtransactions=True)
        u = User(name='u1')
        sess.add(u)
        sess.flush()
        sess.commit()  # commit does nothing
        sess.rollback()  # rolls back
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
        sess.commit()  # commit the nested transaction
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

    def test_continue_flushing_on_commit(self):
        """test that post-flush actions get flushed also if
        we're in commit()"""
        users, User = self.tables.users, self.classes.User

        mapper(User, users)
        sess = Session()

        to_flush = [User(name='ed'), User(name='jack'), User(name='wendy')]

        @event.listens_for(sess, "after_flush_postexec")
        def add_another_user(session, ctx):
            if to_flush:
                session.add(to_flush.pop(0))

        x = [1]

        @event.listens_for(sess, "after_commit")  # noqa
        def add_another_user(session):
            x[0] += 1

        sess.add(to_flush.pop())
        sess.commit()
        eq_(x, [2])
        eq_(
            sess.scalar(select([func.count(users.c.id)])), 3
        )

    def test_continue_flushing_guard(self):
        users, User = self.tables.users, self.classes.User

        mapper(User, users)
        sess = Session()

        @event.listens_for(sess, "after_flush_postexec")
        def add_another_user(session, ctx):
            session.add(User(name='x'))
        sess.add(User(name='x'))
        assert_raises_message(
            orm_exc.FlushError,
            "Over 100 subsequent flushes have occurred",
            sess.commit
        )

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

    def test_no_sql_during_commit(self):
        sess = create_session(bind=testing.db, autocommit=False)

        @event.listens_for(sess, "after_commit")
        def go(session):
            session.execute("select 1")
        assert_raises_message(
            sa_exc.InvalidRequestError,
            "This session is in 'committed' state; no further "
            "SQL can be emitted within this transaction.",
            sess.commit)

    def test_no_sql_during_prepare(self):
        sess = create_session(bind=testing.db, autocommit=False, twophase=True)

        sess.prepare()

        assert_raises_message(
            sa_exc.InvalidRequestError,
            "This session is in 'prepared' state; no further "
            "SQL can be emitted within this transaction.",
            sess.execute, "select 1")

    def test_no_prepare_wo_twophase(self):
        sess = create_session(bind=testing.db, autocommit=False)

        assert_raises_message(sa_exc.InvalidRequestError,
                              "'twophase' mode not enabled, or not root "
                              "transaction; can't prepare.",
                              sess.prepare)

    def test_closed_status_check(self):
        sess = create_session()
        trans = sess.begin()
        trans.rollback()
        assert_raises_message(
            sa_exc.ResourceClosedError, "This transaction is closed",
            trans.rollback)
        assert_raises_message(
            sa_exc.ResourceClosedError, "This transaction is closed",
            trans.commit)

    def test_deactive_status_check(self):
        sess = create_session()
        trans = sess.begin()
        trans2 = sess.begin(subtransactions=True)
        trans2.rollback()
        assert_raises_message(
            sa_exc.InvalidRequestError,
            "This Session's transaction has been rolled back by a nested "
            "rollback\(\) call.  To begin a new transaction, issue "
            "Session.rollback\(\) first.",
            trans.commit
        )

    def test_deactive_status_check_w_exception(self):
        sess = create_session()
        trans = sess.begin()
        trans2 = sess.begin(subtransactions=True)
        try:
            raise Exception("test")
        except:
            trans2.rollback(_capture_exception=True)
        assert_raises_message(
            sa_exc.InvalidRequestError,
            "This Session's transaction has been rolled back due to a "
            "previous exception during flush. To begin a new transaction "
            "with this Session, first issue Session.rollback\(\). "
            "Original exception was: test",
            trans.commit
        )

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

    def test_execution_options_begin_transaction(self):
        bind = mock.Mock()
        sess = Session(bind=bind)
        c1 = sess.connection(execution_options={'isolation_level': 'FOO'})
        eq_(
            bind.mock_calls,
            [
                mock.call.contextual_connect(),
                mock.call.contextual_connect().
                execution_options(isolation_level='FOO'),
                mock.call.contextual_connect().execution_options().begin()
            ]
        )
        eq_(c1, bind.contextual_connect().execution_options())

    def test_execution_options_ignored_mid_transaction(self):
        bind = mock.Mock()
        conn = mock.Mock(engine=bind)
        bind.contextual_connect = mock.Mock(return_value=conn)
        sess = Session(bind=bind)
        sess.execute("select 1")
        with expect_warnings(
                "Connection is already established for the "
                "given bind; execution_options ignored"):
            sess.connection(execution_options={'isolation_level': 'FOO'})

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

    def test_warning_on_using_inactive_session_rollback_evt(self):
        users, User = self.tables.users, self.classes.User

        mapper(User, users)
        sess = Session()
        u1 = User(id=1, name='u1')
        sess.add(u1)
        sess.commit()

        u3 = User(name='u3')

        @event.listens_for(sess, "after_rollback")
        def evt(s):
            sess.add(u3)

        sess.add(User(id=1, name='u2'))

        def go():
            assert_raises(
                orm_exc.FlushError, sess.flush
            )

        assert_warnings(go,
                        ["Session's state has been changed on a "
                         "non-active transaction - this state "
                         "will be discarded."],
                        )
        assert u3 not in sess

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

    @testing.requires.python2
    @testing.requires.savepoints_w_release
    def test_report_primary_error_when_rollback_fails(self):
        User, users = self.classes.User, self.tables.users

        mapper(User, users)

        session = Session(testing.db)

        with expect_warnings(".*during handling of a previous exception.*"):
            session.begin_nested()
            savepoint = session.\
                connection()._Connection__transaction._savepoint

            # force the savepoint to disappear
            session.connection().dialect.do_release_savepoint(
                session.connection(), savepoint
            )

            # now do a broken flush
            session.add_all([User(id=1), User(id=1)])

            assert_raises_message(
                sa_exc.DBAPIError,
                "ROLLBACK TO SAVEPOINT ",
                session.flush
            )


class _LocalFixture(FixtureTest):
    run_setup_mappers = 'once'
    run_inserts = None
    session = sessionmaker()

    @classmethod
    def setup_mappers(cls):
        User, Address = cls.classes.User, cls.classes.Address
        users, addresses = cls.tables.users, cls.tables.addresses
        mapper(
            User, users, properties={
                'addresses': relationship(
                    Address, backref='user', cascade="all, delete-orphan",
                    order_by=addresses.c.id),
            })
        mapper(Address, addresses)


class FixtureDataTest(_LocalFixture):
    run_inserts = 'each'
    __backend__ = True

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


class CleanSavepointTest(FixtureTest):

    """test the behavior for [ticket:2452] - rollback on begin_nested()
    only expires objects tracked as being modified in that transaction.

    """
    run_inserts = None
    __backend__ = True

    def _run_test(self, update_fn):
        User, users = self.classes.User, self.tables.users

        mapper(User, users)

        s = Session(bind=testing.db)
        u1 = User(name='u1')
        u2 = User(name='u2')
        s.add_all([u1, u2])
        s.commit()
        u1.name
        u2.name
        s.begin_nested()
        update_fn(s, u2)
        eq_(u2.name, 'u2modified')
        s.rollback()
        eq_(u1.__dict__['name'], 'u1')
        assert 'name' not in u2.__dict__
        eq_(u2.name, 'u2')

    @testing.requires.savepoints
    def test_rollback_ignores_clean_on_savepoint(self):

        def update_fn(s, u2):
            u2.name = 'u2modified'
        self._run_test(update_fn)

    @testing.requires.savepoints
    def test_rollback_ignores_clean_on_savepoint_agg_upd_eval(self):
        User = self.classes.User

        def update_fn(s, u2):
            s.query(User).filter_by(name='u2').update(
                dict(name='u2modified'), synchronize_session='evaluate')
        self._run_test(update_fn)

    @testing.requires.savepoints
    def test_rollback_ignores_clean_on_savepoint_agg_upd_fetch(self):
        User = self.classes.User

        def update_fn(s, u2):
            s.query(User).filter_by(name='u2').update(
                dict(name='u2modified'),
                synchronize_session='fetch')
        self._run_test(update_fn)


class ContextManagerTest(FixtureTest):
    run_inserts = None
    __backend__ = True

    @testing.requires.savepoints
    @engines.close_open_connections
    def test_contextmanager_nested_rollback(self):
        users, User = self.tables.users, self.classes.User

        mapper(User, users)

        sess = Session()

        def go():
            with sess.begin_nested():
                sess.add(User())   # name can't be null
                sess.flush()

        # and not InvalidRequestError
        assert_raises(
            sa_exc.DBAPIError,
            go
        )

        with sess.begin_nested():
            sess.add(User(name='u1'))

        eq_(sess.query(User).count(), 1)

    def test_contextmanager_commit(self):
        users, User = self.tables.users, self.classes.User

        mapper(User, users)

        sess = Session(autocommit=True)
        with sess.begin():
            sess.add(User(name='u1'))

        sess.rollback()
        eq_(sess.query(User).count(), 1)

    def test_contextmanager_rollback(self):
        users, User = self.tables.users, self.classes.User

        mapper(User, users)

        sess = Session(autocommit=True)

        def go():
            with sess.begin():
                sess.add(User())  # name can't be null
        assert_raises(
            sa_exc.DBAPIError,
            go
        )

        eq_(sess.query(User).count(), 0)

        with sess.begin():
            sess.add(User(name='u1'))
        eq_(sess.query(User).count(), 1)


class AutoExpireTest(_LocalFixture):
    __backend__ = True

    def test_expunge_pending_on_rollback(self):
        User = self.classes.User
        sess = self.session()
        u2 = User(name='newuser')
        sess.add(u2)
        assert u2 in sess
        sess.rollback()
        assert u2 not in sess

    def test_trans_pending_cleared_on_commit(self):
        User = self.classes.User
        sess = self.session()
        u2 = User(name='newuser')
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

    @testing.requires.predictable_gc
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
        # new in 1.1, not in identity map if the object was
        # gc'ed and we restore snapshot; we've changed update_impl
        # to just skip this object
        assert u1_state not in s.identity_map.all_states()

        # in any version, the state is replaced by the query
        # because the identity map would switch it
        u1 = s.query(User).filter_by(name='ed').one()
        assert u1_state not in s.identity_map.all_states()

        eq_(s.scalar(select([func.count('*')]).select_from(users)), 1)
        s.delete(u1)
        s.flush()
        eq_(s.scalar(select([func.count('*')]).select_from(users)), 0)
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
        eq_(s.query(Address).filter(Address.email_address == 'foo').all(), [])
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
        u2 = s2.query(User).filter(User.name == 'edward').one()
        u2.name = 'will'
        s2.commit()

        assert u1.name == 'will'


class TwoPhaseTest(_LocalFixture):
    __backend__ = True

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
    __backend__ = True

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
        assert_raises(orm_exc.FlushError, s.commit)
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
        assert_raises(orm_exc.FlushError, s.commit)
        assert_raises(sa_exc.InvalidRequestError, s.commit)
        s.rollback()
        assert u2 not in s
        assert a2 not in s
        assert u1 in s
        assert a1 in s

        s.commit()
        eq_(
            s.query(User).all(),
            [
                User(
                    id=1, name='edward',
                    addresses=[Address(email_address='foober')])])


class SavepointTest(_LocalFixture):
    __backend__ = True

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
        eq_(
            s.query(User.name).order_by(User.id).all(),
            [('edward',), ('jackward',), ('wendy',), ('foo',)])
        s.rollback()
        assert u1.name == 'ed'
        assert u2.name == 'jack'
        eq_(
            s.query(User.name).order_by(User.id).all(),
            [('ed',), ('jack',)])
        s.commit()
        assert u1.name == 'ed'
        assert u2.name == 'jack'
        eq_(s.query(User.name).order_by(User.id).all(), [('ed',), ('jack',)])

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
        eq_(
            s.query(User.name).order_by(User.id).all(),
            [('edward',), ('jackward',), ('wendy',), ('foo',)])
        s.commit()

        def go():
            assert u1.name == 'edward'
            assert u2.name == 'jackward'
            eq_(
                s.query(User.name).order_by(User.id).all(),
                [('edward',), ('jackward',), ('wendy',), ('foo',)])
        self.assert_sql_count(testing.db, go, 1)

        s.commit()
        eq_(
            s.query(User.name).order_by(User.id).all(),
            [('edward',), ('jackward',), ('wendy',), ('foo',)])

    @testing.requires.savepoints
    def test_savepoint_rollback_collections(self):
        User, Address = self.classes.User, self.classes.Address
        s = self.session()
        u1 = User(name='ed', addresses=[Address(email_address='foo')])
        s.add(u1)
        s.commit()

        u1.name = 'edward'
        u1.addresses.append(Address(email_address='bar'))
        s.begin_nested()
        u2 = User(name='jack', addresses=[Address(email_address='bat')])
        s.add(u2)
        eq_(
            s.query(User).order_by(User.id).all(),
            [
                User(
                    name='edward',
                    addresses=[
                        Address(email_address='foo'),
                        Address(email_address='bar')]),
                User(name='jack', addresses=[Address(email_address='bat')])
            ])
        s.rollback()
        eq_(
            s.query(User).order_by(User.id).all(),
            [
                User(
                    name='edward',
                    addresses=[
                        Address(email_address='foo'),
                        Address(email_address='bar')]),
            ])
        s.commit()
        eq_(
            s.query(User).order_by(User.id).all(),
            [
                User(
                    name='edward',
                    addresses=[
                        Address(email_address='foo'),
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

        u1.name = 'edward'
        u1.addresses.append(Address(email_address='bar'))
        s.begin_nested()
        u2 = User(name='jack', addresses=[Address(email_address='bat')])
        s.add(u2)
        eq_(
            s.query(User).order_by(User.id).all(),
            [
                User(
                    name='edward',
                    addresses=[
                        Address(email_address='foo'),
                        Address(email_address='bar')]),
                User(name='jack', addresses=[Address(email_address='bat')])
            ]
        )
        s.commit()
        eq_(
            s.query(User).order_by(User.id).all(),
            [
                User(
                    name='edward',
                    addresses=[
                        Address(email_address='foo'),
                        Address(email_address='bar')]),
                User(name='jack', addresses=[Address(email_address='bat')])
            ]
        )
        s.commit()
        eq_(
            s.query(User).order_by(User.id).all(),
            [
                User(
                    name='edward',
                    addresses=[
                        Address(email_address='foo'),
                        Address(email_address='bar')]),
                User(name='jack', addresses=[Address(email_address='bat')])
            ]
        )

    @testing.requires.savepoints
    def test_expunge_pending_on_rollback(self):
        User = self.classes.User
        sess = self.session()

        sess.begin_nested()
        u2 = User(name='newuser')
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

    @testing.requires.savepoints_w_release
    def test_savepoint_lost_still_runs(self):
        User = self.classes.User
        s = self.session(bind=self.bind)
        trans = s.begin_nested()
        s.connection()
        u1 = User(name='ed')
        s.add(u1)

        # kill off the transaction
        nested_trans = trans._connections[self.bind][1]
        nested_trans._do_commit()

        is_(s.transaction, trans)
        assert_raises(
            sa_exc.DBAPIError,
            s.rollback
        )

        assert u1 not in s.new

        is_(trans._state, _session.CLOSED)
        is_not_(s.transaction, trans)
        is_(s.transaction._state, _session.ACTIVE)

        is_(s.transaction.nested, False)

        is_(s.transaction._parent, None)


class AccountingFlagsTest(_LocalFixture):
    __backend__ = True

    def test_no_expire_on_commit(self):
        User, users = self.classes.User, self.tables.users

        sess = sessionmaker(expire_on_commit=False)()
        u1 = User(name='ed')
        sess.add(u1)
        sess.commit()

        testing.db.execute(
            users.update(users.c.name == 'ed').values(name='edward'))

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

        testing.db.execute(
            users.update(users.c.name == 'ed').values(name='edward'))

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

        testing.db.execute(
            users.update(users.c.name == 'ed').values(name='edward'))

        assert u1.name == 'edwardo'
        sess.commit()

        assert testing.db.execute(select([users.c.name])).fetchall() == \
            [('edwardo',)]
        assert u1.name == 'edwardo'

        sess.delete(u1)
        sess.commit()

    def test_preflush_no_accounting(self):
        User, users = self.classes.User, self.tables.users

        sess = Session(
            _enable_transaction_accounting=False, autocommit=True,
            autoflush=False)
        u1 = User(name='ed')
        sess.add(u1)
        sess.flush()

        sess.begin()
        u1.name = 'edwardo'
        u2 = User(name="some other user")
        sess.add(u2)

        sess.rollback()

        sess.begin()
        assert testing.db.execute(select([users.c.name])).fetchall() == \
            [('ed',)]


class AutoCommitTest(_LocalFixture):
    __backend__ = True

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
    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        Table('users', metadata, Column('name', String(50), primary_key=True))

    @classmethod
    def setup_classes(cls):
        class User(cls.Comparable):
            pass

    def test_rollback_recover(self):
        users, User = self.tables.users, self.classes.User

        mapper(User, users)

        session = sessionmaker()()

        u1, u2, u3 = User(name='u1'), User(name='u2'), User(name='u3')

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

    def test_reloaded_deleted_checked_for_expiry(self):
        """test issue #3677"""
        users, User = self.tables.users, self.classes.User

        mapper(User, users)

        u1 = User(name='u1')

        s = Session()
        s.add(u1)
        s.flush()
        del u1
        gc_collect()

        u1 = s.query(User).first()  # noqa

        s.rollback()

        u2 = User(name='u1')
        s.add(u2)
        s.commit()

        assert inspect(u2).persistent

    def test_key_replaced_by_update(self):
        users, User = self.tables.users, self.classes.User

        mapper(User, users)

        u1 = User(name='u1')
        u2 = User(name='u2')

        s = Session()
        s.add_all([u1, u2])
        s.commit()

        s.delete(u1)
        s.flush()

        u2.name = 'u1'
        s.flush()

        assert u1 not in s
        s.rollback()

        assert u1 in s
        assert u2 in s

        assert s.identity_map[(User, ('u1',))] is u1
        assert s.identity_map[(User, ('u2',))] is u2

    def test_multiple_key_replaced_by_update(self):
        users, User = self.tables.users, self.classes.User

        mapper(User, users)

        u1 = User(name='u1')
        u2 = User(name='u2')
        u3 = User(name='u3')

        s = Session()
        s.add_all([u1, u2, u3])
        s.commit()

        s.delete(u1)
        s.delete(u2)
        s.flush()

        u3.name = 'u1'
        s.flush()

        u3.name = 'u2'
        s.flush()

        s.rollback()

        assert u1 in s
        assert u2 in s
        assert u3 in s

        assert s.identity_map[(User, ('u1',))] is u1
        assert s.identity_map[(User, ('u2',))] is u2
        assert s.identity_map[(User, ('u3',))] is u3

    def test_key_replaced_by_oob_insert(self):
        users, User = self.tables.users, self.classes.User

        mapper(User, users)

        u1 = User(name='u1')

        s = Session()
        s.add(u1)
        s.commit()

        s.delete(u1)
        s.flush()

        s.execute(users.insert().values(name='u1'))
        u2 = s.query(User).get('u1')

        assert u1 not in s
        s.rollback()

        assert u1 in s
        assert u2 not in s

        assert s.identity_map[(User, ('u1',))] is u1
