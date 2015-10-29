from sqlalchemy.testing import eq_, assert_raises, \
    assert_raises_message
from sqlalchemy.testing.util import gc_collect
from sqlalchemy.testing import pickleable
from sqlalchemy.util import pickle
import inspect
from sqlalchemy.orm import create_session, sessionmaker, attributes, \
    make_transient, make_transient_to_detached, Session
import sqlalchemy as sa
from sqlalchemy.testing import engines, config
from sqlalchemy import testing
from sqlalchemy import Integer, String, Sequence
from sqlalchemy.testing.schema import Table, Column
from sqlalchemy.orm import mapper, relationship, backref, joinedload, \
    exc as orm_exc, object_session, was_deleted
from sqlalchemy.util import pypy
from sqlalchemy.testing import fixtures
from test.orm import _fixtures
from sqlalchemy import event, ForeignKey
from sqlalchemy.util.compat import inspect_getargspec


class ExecutionTest(_fixtures.FixtureTest):
    run_inserts = None
    __backend__ = True

    @testing.requires.sequences
    def test_sequence_execute(self):
        seq = Sequence("some_sequence")
        seq.create(testing.db)
        try:
            sess = create_session(bind=testing.db)
            eq_(sess.execute(seq), 1)
        finally:
            seq.drop(testing.db)

    def test_textual_execute(self):
        """test that Session.execute() converts to text()"""

        users = self.tables.users


        sess = create_session(bind=self.metadata.bind)
        users.insert().execute(id=7, name='jack')

        # use :bindparam style
        eq_(sess.execute("select * from users where id=:id",
                         {'id': 7}).fetchall(),
            [(7, 'jack')])


        # use :bindparam style
        eq_(sess.scalar("select id from users where id=:id",
                         {'id': 7}),
            7)

    def test_parameter_execute(self):
        users = self.tables.users
        sess = Session(bind=testing.db)
        sess.execute(users.insert(), [
                {"id": 7, "name": "u7"},
                {"id": 8, "name": "u8"}
            ]
        )
        sess.execute(users.insert(), {"id": 9, "name": "u9"})
        eq_(
            sess.execute(sa.select([users.c.id]).\
                    order_by(users.c.id)).fetchall(),
            [(7, ), (8, ), (9, )]
        )


class TransScopingTest(_fixtures.FixtureTest):
    run_inserts = None
    __prefer_requires__ = "independent_connections",

    def test_no_close_on_flush(self):
        """Flush() doesn't close a connection the session didn't open"""

        User, users = self.classes.User, self.tables.users

        c = testing.db.connect()
        c.execute("select * from users")

        mapper(User, users)
        s = create_session(bind=c)
        s.add(User(name='first'))
        s.flush()
        c.execute("select * from users")

    def test_close(self):
        """close() doesn't close a connection the session didn't open"""

        User, users = self.classes.User, self.tables.users

        c = testing.db.connect()
        c.execute("select * from users")

        mapper(User, users)
        s = create_session(bind=c)
        s.add(User(name='first'))
        s.flush()
        c.execute("select * from users")
        s.close()
        c.execute("select * from users")

    @testing.requires.independent_connections
    @engines.close_open_connections
    def test_transaction(self):
        User, users = self.classes.User, self.tables.users

        mapper(User, users)
        conn1 = testing.db.connect()
        conn2 = testing.db.connect()

        sess = create_session(autocommit=False, bind=conn1)
        u = User(name='x')
        sess.add(u)
        sess.flush()
        assert conn1.execute("select count(1) from users").scalar() == 1
        assert conn2.execute("select count(1) from users").scalar() == 0
        sess.commit()
        assert conn1.execute("select count(1) from users").scalar() == 1

        assert testing.db.connect().execute('select count(1) from users'
                ).scalar() == 1
        sess.close()

class SessionUtilTest(_fixtures.FixtureTest):
    run_inserts = None

    def test_object_session_raises(self):
        User = self.classes.User

        assert_raises(
            orm_exc.UnmappedInstanceError,
            object_session,
            object()
        )

        assert_raises(
            orm_exc.UnmappedInstanceError,
            object_session,
            User()
        )

    def test_make_transient(self):
        users, User = self.tables.users, self.classes.User

        mapper(User, users)
        sess = create_session()
        sess.add(User(name='test'))
        sess.flush()

        u1 = sess.query(User).first()
        make_transient(u1)
        assert u1 not in sess
        sess.add(u1)
        assert u1 in sess.new

        u1 = sess.query(User).first()
        sess.expunge(u1)
        make_transient(u1)
        sess.add(u1)
        assert u1 in sess.new

        # test expired attributes
        # get unexpired
        u1 = sess.query(User).first()
        sess.expire(u1)
        make_transient(u1)
        assert u1.id is None
        assert u1.name is None

        # works twice
        make_transient(u1)

        sess.close()

        u1.name = 'test2'
        sess.add(u1)
        sess.flush()
        assert u1 in sess
        sess.delete(u1)
        sess.flush()
        assert u1 not in sess

        assert_raises(sa.exc.InvalidRequestError, sess.add, u1)
        make_transient(u1)
        sess.add(u1)
        sess.flush()
        assert u1 in sess

    def test_make_transient_plus_rollback(self):
        # test for [ticket:2182]
        users, User = self.tables.users, self.classes.User

        mapper(User, users)
        sess = Session()
        u1 = User(name='test')
        sess.add(u1)
        sess.commit()

        sess.delete(u1)
        sess.flush()
        make_transient(u1)
        sess.rollback()
        assert attributes.instance_state(u1).transient

    def test_make_transient_to_detached(self):
        users, User = self.tables.users, self.classes.User

        mapper(User, users)
        sess = Session()
        u1 = User(id=1, name='test')
        sess.add(u1)
        sess.commit()
        sess.close()

        u2 = User(id=1)
        make_transient_to_detached(u2)
        assert 'id' in u2.__dict__
        sess.add(u2)
        eq_(u2.name, "test")

    def test_make_transient_to_detached_no_session_allowed(self):
        users, User = self.tables.users, self.classes.User

        mapper(User, users)
        sess = Session()
        u1 = User(id=1, name='test')
        sess.add(u1)
        assert_raises_message(
            sa.exc.InvalidRequestError,
            "Given object must be transient",
            make_transient_to_detached, u1
        )

    def test_make_transient_to_detached_no_key_allowed(self):
        users, User = self.tables.users, self.classes.User

        mapper(User, users)
        sess = Session()
        u1 = User(id=1, name='test')
        sess.add(u1)
        sess.commit()
        sess.expunge(u1)
        assert_raises_message(
            sa.exc.InvalidRequestError,
            "Given object must be transient",
            make_transient_to_detached, u1
        )


class SessionStateTest(_fixtures.FixtureTest):
    run_inserts = None

    __prefer_requires__ = ('independent_connections', )

    def test_info(self):
        s = Session()
        eq_(s.info, {})

        maker = sessionmaker(info={"global": True, "s1": 5})

        s1 = maker()
        s2 = maker(info={"s1": 6, "s2": True})

        eq_(s1.info, {"global": True, "s1": 5})
        eq_(s2.info, {"global": True, "s1": 6, "s2": True})
        s2.info["global"] = False
        s2.info["s1"] = 7

        s3 = maker()
        eq_(s3.info, {"global": True, "s1": 5})

        maker2 = sessionmaker()
        s4 = maker2(info={'s4': 8})
        eq_(s4.info, {'s4': 8})

    @testing.requires.independent_connections
    @engines.close_open_connections
    def test_autoflush(self):
        User, users = self.classes.User, self.tables.users

        bind = self.metadata.bind
        mapper(User, users)
        conn1 = bind.connect()
        conn2 = bind.connect()

        sess = create_session(bind=conn1, autocommit=False, autoflush=True)
        u = User()
        u.name = 'ed'
        sess.add(u)
        u2 = sess.query(User).filter_by(name='ed').one()
        assert u2 is u
        eq_(conn1.execute("select count(1) from users").scalar(), 1)
        eq_(conn2.execute("select count(1) from users").scalar(), 0)
        sess.commit()
        eq_(conn1.execute("select count(1) from users").scalar(), 1)
        eq_(bind.connect().execute("select count(1) from users").scalar(), 1)
        sess.close()

    def test_with_no_autoflush(self):
        User, users = self.classes.User, self.tables.users

        mapper(User, users)
        sess = Session()

        u = User()
        u.name = 'ed'
        sess.add(u)
        def go(obj):
            assert u not in sess.query(User).all()
        testing.run_as_contextmanager(sess.no_autoflush, go)
        assert u in sess.new
        assert u in sess.query(User).all()
        assert u not in sess.new


    def test_deleted_flag(self):
        users, User = self.tables.users, self.classes.User

        mapper(User, users)

        sess = sessionmaker()()

        u1 = User(name='u1')
        sess.add(u1)
        sess.commit()

        sess.delete(u1)
        sess.flush()
        assert u1 not in sess
        assert_raises(sa.exc.InvalidRequestError, sess.add, u1)
        sess.rollback()
        assert u1 in sess

        sess.delete(u1)
        sess.commit()
        assert u1 not in sess
        assert_raises(sa.exc.InvalidRequestError, sess.add, u1)

        make_transient(u1)
        sess.add(u1)
        sess.commit()

        eq_(sess.query(User).count(), 1)

    def test_autoflush_expressions(self):
        """test that an expression which is dependent on object state is
        evaluated after the session autoflushes.   This is the lambda
        inside of strategies.py lazy_clause.

        """

        users, Address, addresses, User = (self.tables.users,
                                self.classes.Address,
                                self.tables.addresses,
                                self.classes.User)

        mapper(User, users, properties={
            'addresses': relationship(Address, backref="user")})
        mapper(Address, addresses)

        sess = create_session(autoflush=True, autocommit=False)
        u = User(name='ed', addresses=[Address(email_address='foo')])
        sess.add(u)
        eq_(sess.query(Address).filter(Address.user == u).one(),
            Address(email_address='foo'))

        # still works after "u" is garbage collected
        sess.commit()
        sess.close()
        u = sess.query(User).get(u.id)
        q = sess.query(Address).filter(Address.user == u)
        del u
        gc_collect()
        eq_(q.one(), Address(email_address='foo'))



    @testing.requires.independent_connections
    @engines.close_open_connections
    def test_autoflush_unbound(self):
        User, users = self.classes.User, self.tables.users

        mapper(User, users)
        try:
            sess = create_session(autocommit=False, autoflush=True)
            u = User()
            u.name = 'ed'
            sess.add(u)
            u2 = sess.query(User).filter_by(name='ed').one()
            assert u2 is u
            assert sess.execute('select count(1) from users',
                                mapper=User).scalar() == 1
            assert testing.db.connect().execute('select count(1) from '
                    'users').scalar() == 0
            sess.commit()
            assert sess.execute('select count(1) from users',
                                mapper=User).scalar() == 1
            assert testing.db.connect().execute('select count(1) from '
                    'users').scalar() == 1
            sess.close()
        except:
            sess.rollback()
            raise

    @engines.close_open_connections
    def test_autoflush_2(self):
        User, users = self.classes.User, self.tables.users

        mapper(User, users)
        conn1 = testing.db.connect()
        sess = create_session(bind=conn1, autocommit=False,
                              autoflush=True)
        u = User()
        u.name = 'ed'
        sess.add(u)
        sess.commit()
        assert conn1.execute('select count(1) from users').scalar() == 1
        assert testing.db.connect().execute('select count(1) from users'
                ).scalar() == 1
        sess.commit()


    def test_autocommit_doesnt_raise_on_pending(self):
        User, users = self.classes.User, self.tables.users

        mapper(User, users)
        session = create_session(autocommit=True)

        session.add(User(name='ed'))

        session.begin()
        session.flush()
        session.commit()

    def test_active_flag(self):
        sess = create_session(bind=config.db, autocommit=True)
        assert not sess.is_active
        sess.begin()
        assert sess.is_active
        sess.rollback()
        assert not sess.is_active


    @engines.close_open_connections
    def test_add_delete(self):
        User, Address, addresses, users = (self.classes.User,
                                self.classes.Address,
                                self.tables.addresses,
                                self.tables.users)


        s = create_session()
        mapper(User, users, properties={
            'addresses': relationship(Address, cascade="all, delete")
        })
        mapper(Address, addresses)

        user = User(name='u1')

        assert_raises_message(sa.exc.InvalidRequestError,
                              'is not persisted', s.delete, user)

        s.add(user)
        s.flush()
        user = s.query(User).one()
        s.expunge(user)
        assert user not in s

        # modify outside of session, assert changes remain/get saved
        user.name = "fred"
        s.add(user)
        assert user in s
        assert user in s.dirty
        s.flush()
        s.expunge_all()
        assert s.query(User).count() == 1
        user = s.query(User).one()
        assert user.name == 'fred'

        # ensure its not dirty if no changes occur
        s.expunge_all()
        assert user not in s
        s.add(user)
        assert user in s
        assert user not in s.dirty

        s2 = create_session()
        assert_raises_message(sa.exc.InvalidRequestError,
                              'is already attached to session',
                              s2.delete, user)
        u2 = s2.query(User).get(user.id)
        assert_raises_message(sa.exc.InvalidRequestError,
                              'another instance with key', s.delete, u2)
        s.expire(user)
        s.expunge(user)
        assert user not in s
        s.delete(user)
        assert user in s

        s.flush()
        assert user not in s
        assert s.query(User).count() == 0

    def test_already_attached(self):
        User = self.classes.User
        users = self.tables.users
        mapper(User, users)

        s1 = Session()
        s2 = Session()

        u1 = User(id=1, name='u1')
        make_transient_to_detached(u1)  # shorthand for actually persisting it
        s1.add(u1)

        assert_raises_message(
            sa.exc.InvalidRequestError,
            "Object '<User.*?>' is already attached to session",
            s2.add, u1
        )
        assert u1 not in s2
        assert not s2.identity_map.keys()

    @testing.uses_deprecated()
    def test_identity_conflict(self):
        users, User = self.tables.users, self.classes.User

        mapper(User, users)
        for s in (
            create_session(),
            create_session(weak_identity_map=False),
        ):
            users.delete().execute()
            u1 = User(name="ed")
            s.add(u1)
            s.flush()
            s.expunge(u1)
            u2 = s.query(User).first()
            s.expunge(u2)
            s.identity_map.add(sa.orm.attributes.instance_state(u1))

            assert_raises(AssertionError, s.identity_map.add,
                          sa.orm.attributes.instance_state(u2))

    def test_pickled_update(self):
        users, User = self.tables.users, pickleable.User

        mapper(User, users)
        sess1 = create_session()
        sess2 = create_session()
        u1 = User(name='u1')
        sess1.add(u1)
        assert_raises_message(sa.exc.InvalidRequestError,
                              'already attached to session', sess2.add,
                              u1)
        u2 = pickle.loads(pickle.dumps(u1))
        sess2.add(u2)

    def test_duplicate_update(self):
        users, User = self.tables.users, self.classes.User

        mapper(User, users)
        Session = sessionmaker()
        sess = Session()

        u1 = User(name='u1')
        sess.add(u1)
        sess.flush()
        assert u1.id is not None

        sess.expunge(u1)

        assert u1 not in sess
        assert Session.object_session(u1) is None

        u2 = sess.query(User).get(u1.id)
        assert u2 is not None and u2 is not u1
        assert u2 in sess

        assert_raises(AssertionError, lambda: sess.add(u1))

        sess.expunge(u2)
        assert u2 not in sess
        assert Session.object_session(u2) is None

        u1.name = "John"
        u2.name = "Doe"

        sess.add(u1)
        assert u1 in sess
        assert Session.object_session(u1) is sess

        sess.flush()

        sess.expunge_all()

        u3 = sess.query(User).get(u1.id)
        assert u3 is not u1 and u3 is not u2 and u3.name == u1.name

    def test_no_double_save(self):
        users = self.tables.users

        sess = create_session()
        class Foo(object):
            def __init__(self):
                sess.add(self)
        class Bar(Foo):
            def __init__(self):
                sess.add(self)
                Foo.__init__(self)
        mapper(Foo, users)
        mapper(Bar, users)

        b = Bar()
        assert b in sess
        assert len(list(sess)) == 1

    def test_identity_map_mutate(self):
        users, User = self.tables.users, self.classes.User

        mapper(User, users)

        sess = Session()

        sess.add_all([User(name='u1'), User(name='u2'), User(name='u3')])
        sess.commit()

        # TODO: what are we testing here ?   that iteritems() can
        # withstand a change?  should this be
        # more directly attempting to manipulate the identity_map ?
        u1, u2, u3 = sess.query(User).all()
        for i, (key, value) in enumerate(iter(sess.identity_map.items())):
            if i == 2:
                del u3
                gc_collect()

    def _test_extra_dirty_state(self):
        users, User = self.tables.users, self.classes.User
        m = mapper(User, users)

        s = Session()

        @event.listens_for(m, "after_update")
        def e(mapper, conn, target):
            sess = object_session(target)
            for entry in list(sess.identity_map.values()):
                entry.name = "5"

        a1, a2 = User(name="1"), User(name="2")

        s.add_all([a1, a2])
        s.commit()

        a1.name = "3"
        return s, a1, a2

    def test_extra_dirty_state_post_flush_warning(self):
        s, a1, a2 = self._test_extra_dirty_state()
        assert_raises_message(
            sa.exc.SAWarning,
            "Attribute history events accumulated on 1 previously "
            "clean instances",
            s.commit
        )

    def test_extra_dirty_state_post_flush_state(self):
        s, a1, a2 = self._test_extra_dirty_state()
        canary = []

        @event.listens_for(s, "after_flush_postexec")
        def e(sess, ctx):
            canary.append(bool(sess.identity_map._modified))

        @testing.emits_warning("Attribute")
        def go():
            s.commit()
        go()
        eq_(canary, [False])

    def test_deleted_auto_expunged(self):
        users, User = self.tables.users, self.classes.User

        mapper(User, users)
        sess = Session()
        sess.add(User(name='x'))
        sess.commit()

        u1 = sess.query(User).first()
        sess.delete(u1)

        assert not was_deleted(u1)
        sess.flush()

        assert was_deleted(u1)
        assert u1 not in sess
        assert object_session(u1) is sess
        sess.commit()

        assert object_session(u1) is None

    def test_explicit_expunge_pending(self):
        users, User = self.tables.users, self.classes.User

        mapper(User, users)
        sess = Session()
        u1 = User(name='x')
        sess.add(u1)

        sess.flush()
        sess.expunge(u1)

        assert u1 not in sess
        assert object_session(u1) is None

        sess.rollback()

        assert u1 not in sess
        assert object_session(u1) is None

    def test_explicit_expunge_deleted(self):
        users, User = self.tables.users, self.classes.User

        mapper(User, users)
        sess = Session()
        sess.add(User(name='x'))
        sess.commit()

        u1 = sess.query(User).first()
        sess.delete(u1)

        sess.flush()

        assert was_deleted(u1)
        assert u1 not in sess
        assert object_session(u1) is sess

        sess.expunge(u1)
        assert was_deleted(u1)
        assert u1 not in sess
        assert object_session(u1) is None

        sess.rollback()
        assert was_deleted(u1)
        assert u1 not in sess
        assert object_session(u1) is None


class SessionStateWFixtureTest(_fixtures.FixtureTest):
    __backend__ = True

    def test_autoflush_rollback(self):
        Address, addresses, users, User = (self.classes.Address,
                                self.tables.addresses,
                                self.tables.users,
                                self.classes.User)

        mapper(Address, addresses)
        mapper(User, users, properties={
            'addresses': relationship(Address)})

        sess = create_session(autocommit=False, autoflush=True)
        u = sess.query(User).get(8)
        newad = Address(email_address='a new address')
        u.addresses.append(newad)
        u.name = 'some new name'
        assert u.name == 'some new name'
        assert len(u.addresses) == 4
        assert newad in u.addresses
        sess.rollback()
        assert u.name == 'ed'
        assert len(u.addresses) == 3

        assert newad not in u.addresses
        # pending objects don't get expired
        assert newad.email_address == 'a new address'

    def test_expunge_cascade(self):
        Address, addresses, users, User = (self.classes.Address,
                                self.tables.addresses,
                                self.tables.users,
                                self.classes.User)

        mapper(Address, addresses)
        mapper(User, users, properties={
            'addresses': relationship(Address,
                                 backref=backref("user", cascade="all"),
                                 cascade="all")})

        session = create_session()
        u = session.query(User).filter_by(id=7).one()

        # get everything to load in both directions
        print([a.user for a in u.addresses])

        # then see if expunge fails
        session.expunge(u)

        assert sa.orm.object_session(u) is None
        assert sa.orm.attributes.instance_state(u).session_id is None
        for a in u.addresses:
            assert sa.orm.object_session(a) is None
            assert sa.orm.attributes.instance_state(a).session_id is None


class NoCyclesOnTransientDetachedTest(_fixtures.FixtureTest):
    """Test the instance_state._strong_obj link that it
    is present only on persistent/pending objects and never
    transient/detached.

    """
    run_inserts = None

    def setup(self):
        mapper(self.classes.User, self.tables.users)

    def _assert_modified(self, u1):
        assert sa.orm.attributes.instance_state(u1).modified

    def _assert_not_modified(self, u1):
        assert not sa.orm.attributes.instance_state(u1).modified

    def _assert_cycle(self, u1):
        assert sa.orm.attributes.instance_state(u1)._strong_obj is not None

    def _assert_no_cycle(self, u1):
        assert sa.orm.attributes.instance_state(u1)._strong_obj is None

    def _persistent_fixture(self):
        User = self.classes.User
        u1 = User()
        u1.name = "ed"
        sess = Session()
        sess.add(u1)
        sess.flush()
        return sess, u1

    def test_transient(self):
        User = self.classes.User
        u1 = User()
        u1.name = 'ed'
        self._assert_no_cycle(u1)
        self._assert_modified(u1)

    def test_transient_to_pending(self):
        User = self.classes.User
        u1 = User()
        u1.name = 'ed'
        self._assert_modified(u1)
        self._assert_no_cycle(u1)
        sess = Session()
        sess.add(u1)
        self._assert_cycle(u1)
        sess.flush()
        self._assert_no_cycle(u1)
        self._assert_not_modified(u1)

    def test_dirty_persistent_to_detached_via_expunge(self):
        sess, u1 = self._persistent_fixture()
        u1.name = 'edchanged'
        self._assert_cycle(u1)
        sess.expunge(u1)
        self._assert_no_cycle(u1)

    def test_dirty_persistent_to_detached_via_close(self):
        sess, u1 = self._persistent_fixture()
        u1.name = 'edchanged'
        self._assert_cycle(u1)
        sess.close()
        self._assert_no_cycle(u1)

    def test_clean_persistent_to_detached_via_close(self):
        sess, u1 = self._persistent_fixture()
        self._assert_no_cycle(u1)
        self._assert_not_modified(u1)
        sess.close()
        u1.name = 'edchanged'
        self._assert_modified(u1)
        self._assert_no_cycle(u1)

    def test_detached_to_dirty_deleted(self):
        sess, u1 = self._persistent_fixture()
        sess.expunge(u1)
        u1.name = 'edchanged'
        self._assert_no_cycle(u1)
        sess.delete(u1)
        self._assert_cycle(u1)

    def test_detached_to_dirty_persistent(self):
        sess, u1 = self._persistent_fixture()
        sess.expunge(u1)
        u1.name = 'edchanged'
        self._assert_modified(u1)
        self._assert_no_cycle(u1)
        sess.add(u1)
        self._assert_cycle(u1)
        self._assert_modified(u1)

    def test_detached_to_clean_persistent(self):
        sess, u1 = self._persistent_fixture()
        sess.expunge(u1)
        self._assert_no_cycle(u1)
        self._assert_not_modified(u1)
        sess.add(u1)
        self._assert_no_cycle(u1)
        self._assert_not_modified(u1)

    def test_move_persistent_clean(self):
        sess, u1 = self._persistent_fixture()
        sess.close()
        s2 = Session()
        s2.add(u1)
        self._assert_no_cycle(u1)
        self._assert_not_modified(u1)

    def test_move_persistent_dirty(self):
        sess, u1 = self._persistent_fixture()
        u1.name = 'edchanged'
        self._assert_cycle(u1)
        self._assert_modified(u1)
        sess.close()
        self._assert_no_cycle(u1)
        s2 = Session()
        s2.add(u1)
        self._assert_cycle(u1)
        self._assert_modified(u1)

    @testing.requires.predictable_gc
    def test_move_gc_session_persistent_dirty(self):
        sess, u1 = self._persistent_fixture()
        u1.name = 'edchanged'
        self._assert_cycle(u1)
        self._assert_modified(u1)
        del sess
        gc_collect()
        self._assert_cycle(u1)
        s2 = Session()
        s2.add(u1)
        self._assert_cycle(u1)
        self._assert_modified(u1)

    def test_persistent_dirty_to_expired(self):
        sess, u1 = self._persistent_fixture()
        u1.name = 'edchanged'
        self._assert_cycle(u1)
        self._assert_modified(u1)
        sess.expire(u1)
        self._assert_no_cycle(u1)
        self._assert_not_modified(u1)

class WeakIdentityMapTest(_fixtures.FixtureTest):
    run_inserts = None

    @testing.requires.predictable_gc
    def test_weakref(self):
        """test the weak-referencing identity map, which strongly-
        references modified items."""

        users, User = self.tables.users, self.classes.User


        s = create_session()
        mapper(User, users)

        s.add(User(name='ed'))
        s.flush()
        assert not s.dirty

        user = s.query(User).one()
        del user
        gc_collect()
        assert len(s.identity_map) == 0

        user = s.query(User).one()
        user.name = 'fred'
        del user
        gc_collect()
        assert len(s.identity_map) == 1
        assert len(s.dirty) == 1
        assert None not in s.dirty
        s.flush()
        gc_collect()
        assert not s.dirty
        assert not s.identity_map

        user = s.query(User).one()
        assert user.name == 'fred'
        assert s.identity_map

    @testing.requires.predictable_gc
    def test_weakref_pickled(self):
        users, User = self.tables.users, pickleable.User

        s = create_session()
        mapper(User, users)

        s.add(User(name='ed'))
        s.flush()
        assert not s.dirty

        user = s.query(User).one()
        user.name = 'fred'
        s.expunge(user)

        u2 = pickle.loads(pickle.dumps(user))

        del user
        s.add(u2)

        del u2
        gc_collect()

        assert len(s.identity_map) == 1
        assert len(s.dirty) == 1
        assert None not in s.dirty
        s.flush()
        gc_collect()
        assert not s.dirty

        assert not s.identity_map

    @testing.requires.predictable_gc
    def test_weakref_with_cycles_o2m(self):
        Address, addresses, users, User = (self.classes.Address,
                                self.tables.addresses,
                                self.tables.users,
                                self.classes.User)

        s = sessionmaker()()
        mapper(User, users, properties={
            "addresses": relationship(Address, backref="user")
        })
        mapper(Address, addresses)
        s.add(User(name="ed", addresses=[Address(email_address="ed1")]))
        s.commit()

        user = s.query(User).options(joinedload(User.addresses)).one()
        user.addresses[0].user  # lazyload
        eq_(user, User(name="ed", addresses=[Address(email_address="ed1")]))

        del user
        gc_collect()
        assert len(s.identity_map) == 0

        user = s.query(User).options(joinedload(User.addresses)).one()
        user.addresses[0].email_address = 'ed2'
        user.addresses[0].user  # lazyload
        del user
        gc_collect()
        assert len(s.identity_map) == 2

        s.commit()
        user = s.query(User).options(joinedload(User.addresses)).one()
        eq_(user, User(name="ed", addresses=[Address(email_address="ed2")]))

    @testing.requires.predictable_gc
    def test_weakref_with_cycles_o2o(self):
        Address, addresses, users, User = (self.classes.Address,
                                self.tables.addresses,
                                self.tables.users,
                                self.classes.User)

        s = sessionmaker()()
        mapper(User, users, properties={
            "address": relationship(Address, backref="user", uselist=False)
        })
        mapper(Address, addresses)
        s.add(User(name="ed", address=Address(email_address="ed1")))
        s.commit()

        user = s.query(User).options(joinedload(User.address)).one()
        user.address.user
        eq_(user, User(name="ed", address=Address(email_address="ed1")))

        del user
        gc_collect()
        assert len(s.identity_map) == 0

        user = s.query(User).options(joinedload(User.address)).one()
        user.address.email_address = 'ed2'
        user.address.user  # lazyload

        del user
        gc_collect()
        assert len(s.identity_map) == 2

        s.commit()
        user = s.query(User).options(joinedload(User.address)).one()
        eq_(user, User(name="ed", address=Address(email_address="ed2")))

    def test_auto_detach_on_gc_session(self):
        users, User = self.tables.users, self.classes.User

        mapper(User, users)

        sess = Session()

        u1 = User(name='u1')
        sess.add(u1)
        sess.commit()

        # can't add u1 to Session,
        # already belongs to u2
        s2 = Session()
        assert_raises_message(
            sa.exc.InvalidRequestError,
            r".*is already attached to session",
            s2.add, u1
        )

        # garbage collect sess
        del sess
        gc_collect()

        # s2 lets it in now despite u1 having
        # session_key
        s2.add(u1)
        assert u1 in s2


class StrongIdentityMapTest(_fixtures.FixtureTest):
    run_inserts = None

    @testing.uses_deprecated()
    def test_strong_ref(self):
        users, User = self.tables.users, self.classes.User

        s = create_session(weak_identity_map=False)
        mapper(User, users)

        # save user
        s.add(User(name='u1'))
        s.flush()
        user = s.query(User).one()
        user = None
        print(s.identity_map)
        gc_collect()
        assert len(s.identity_map) == 1

        user = s.query(User).one()
        assert not s.identity_map._modified
        user.name = 'u2'
        assert s.identity_map._modified
        s.flush()
        eq_(users.select().execute().fetchall(), [(user.id, 'u2')])

    @testing.uses_deprecated()
    @testing.fails_if(lambda: pypy, "pypy has a real GC")
    @testing.fails_on('+zxjdbc', 'http://www.sqlalchemy.org/trac/ticket/1473')
    def test_prune(self):
        users, User = self.tables.users, self.classes.User

        s = create_session(weak_identity_map=False)
        mapper(User, users)

        for o in [User(name='u%s' % x) for x in range(10)]:
            s.add(o)
        # o is still live after this loop...

        self.assert_(len(s.identity_map) == 0)
        self.assert_(s.prune() == 0)
        s.flush()
        gc_collect()
        self.assert_(s.prune() == 9)
        self.assert_(len(s.identity_map) == 1)

        id = o.id
        del o
        self.assert_(s.prune() == 1)
        self.assert_(len(s.identity_map) == 0)

        u = s.query(User).get(id)
        self.assert_(s.prune() == 0)
        self.assert_(len(s.identity_map) == 1)
        u.name = 'squiznart'
        del u
        self.assert_(s.prune() == 0)
        self.assert_(len(s.identity_map) == 1)
        s.flush()
        self.assert_(s.prune() == 1)
        self.assert_(len(s.identity_map) == 0)

        s.add(User(name='x'))
        self.assert_(s.prune() == 0)
        self.assert_(len(s.identity_map) == 0)
        s.flush()
        self.assert_(len(s.identity_map) == 1)
        self.assert_(s.prune() == 1)
        self.assert_(len(s.identity_map) == 0)

        u = s.query(User).get(id)
        s.delete(u)
        del u
        self.assert_(s.prune() == 0)
        self.assert_(len(s.identity_map) == 1)
        s.flush()
        self.assert_(s.prune() == 0)
        self.assert_(len(s.identity_map) == 0)


class IsModifiedTest(_fixtures.FixtureTest):
    run_inserts = None

    def _default_mapping_fixture(self):
        User, Address = self.classes.User, self.classes.Address
        users, addresses = self.tables.users, self.tables.addresses
        mapper(User, users, properties={
            "addresses": relationship(Address)
        })
        mapper(Address, addresses)
        return User, Address

    def test_is_modified(self):
        User, Address = self._default_mapping_fixture()

        s = create_session()

        # save user
        u = User(name='fred')
        s.add(u)
        s.flush()
        s.expunge_all()

        user = s.query(User).one()
        assert user not in s.dirty
        assert not s.is_modified(user)
        user.name = 'fred'
        assert user in s.dirty
        assert not s.is_modified(user)
        user.name = 'ed'
        assert user in s.dirty
        assert s.is_modified(user)
        s.flush()
        assert user not in s.dirty
        assert not s.is_modified(user)

        a = Address()
        user.addresses.append(a)
        assert user in s.dirty
        assert s.is_modified(user)
        assert not s.is_modified(user, include_collections=False)

    def test_is_modified_passive_off(self):
        """as of 0.8 no SQL is emitted for is_modified()
        regardless of the passive flag"""

        User, Address = self._default_mapping_fixture()

        s = Session()
        u = User(name='fred', addresses=[
                    Address(email_address='foo')])
        s.add(u)
        s.commit()

        u.id
        def go():
            assert not s.is_modified(u)
        self.assert_sql_count(
            testing.db,
            go,
            0
        )

        s.expire_all()
        u.name = 'newname'

        # can't predict result here
        # deterministically, depending on if
        # 'name' or 'addresses' is tested first
        mod = s.is_modified(u)
        addresses_loaded = 'addresses' in u.__dict__
        assert mod is not addresses_loaded

    def test_is_modified_passive_on(self):
        User, Address = self._default_mapping_fixture()

        s = Session()
        u = User(name='fred', addresses=[Address(email_address='foo')])
        s.add(u)
        s.commit()

        u.id
        def go():
            assert not s.is_modified(u, passive=True)
        self.assert_sql_count(
            testing.db,
            go,
            0
        )

        u.name = 'newname'
        def go():
            assert s.is_modified(u, passive=True)
        self.assert_sql_count(
            testing.db,
            go,
            0
        )

    def test_is_modified_syn(self):
        User, users = self.classes.User, self.tables.users

        s = sessionmaker()()

        mapper(User, users, properties={'uname': sa.orm.synonym('name')})
        u = User(uname='fred')
        assert s.is_modified(u)
        s.add(u)
        s.commit()
        assert not s.is_modified(u)

class DisposedStates(fixtures.MappedTest):
    run_setup_mappers = 'once'
    run_inserts = 'once'
    run_deletes = None

    @classmethod
    def define_tables(cls, metadata):
        Table('t1', metadata, Column('id', Integer,
                   primary_key=True, test_needs_autoincrement=True),
                   Column('data', String(50)))

    @classmethod
    def setup_classes(cls):
        class T(cls.Basic):
            def __init__(self, data):
                self.data = data
        mapper(T, cls.tables.t1)

    def teardown(self):
        from sqlalchemy.orm.session import _sessions
        _sessions.clear()
        super(DisposedStates, self).teardown()

    def _set_imap_in_disposal(self, sess, *objs):
        """remove selected objects from the given session, as though
        they were dereferenced and removed from WeakIdentityMap.

        Hardcodes the identity map's "all_states()" method to return the
        full list of states.  This simulates the all_states() method
        returning results, afterwhich some of the states get garbage
        collected (this normally only happens during asynchronous gc).
        The Session now has one or more InstanceState's which have been
        removed from the identity map and disposed.

        Will the Session not trip over this ???  Stay tuned.

        """

        all_states = sess.identity_map.all_states()
        sess.identity_map.all_states = lambda: all_states
        for obj in objs:
            state = attributes.instance_state(obj)
            sess.identity_map.discard(state)
            state._dispose()

    def _test_session(self, **kwargs):
        T = self.classes.T
        sess = create_session(**kwargs)

        data = o1, o2, o3, o4, o5 = [T('t1'), T('t2'), T('t3'), T('t4'
                ), T('t5')]

        sess.add_all(data)

        sess.flush()

        o1.data = 't1modified'
        o5.data = 't5modified'

        self._set_imap_in_disposal(sess, o2, o4, o5)
        return sess

    def test_flush(self):
        self._test_session().flush()

    def test_clear(self):
        self._test_session().expunge_all()

    def test_close(self):
        self._test_session().close()

    def test_invalidate(self):
        self._test_session().invalidate()

    def test_expunge_all(self):
        self._test_session().expunge_all()

    def test_expire_all(self):
        self._test_session().expire_all()

    def test_rollback(self):
        sess = self._test_session(autocommit=False, expire_on_commit=True)
        sess.commit()

        sess.rollback()


class SessionInterface(fixtures.TestBase):
    """Bogus args to Session methods produce actionable exceptions."""

    # TODO: expand with message body assertions.

    _class_methods = set((
        'connection', 'execute', 'get_bind', 'scalar'))

    def _public_session_methods(self):
        Session = sa.orm.session.Session

        blacklist = set(('begin', 'query'))

        ok = set()
        for meth in Session.public_methods:
            if meth in blacklist:
                continue
            spec = inspect_getargspec(getattr(Session, meth))
            if len(spec[0]) > 1 or spec[1]:
                ok.add(meth)
        return ok

    def _map_it(self, cls):
        return mapper(cls, Table('t', sa.MetaData(), Column('id',
                      Integer, primary_key=True,
                      test_needs_autoincrement=True)))

    def _test_instance_guards(self, user_arg):
        watchdog = set()

        def x_raises_(obj, method, *args, **kw):
            watchdog.add(method)
            callable_ = getattr(obj, method)
            assert_raises(sa.orm.exc.UnmappedInstanceError,
                              callable_, *args, **kw)

        def raises_(method, *args, **kw):
            x_raises_(create_session(), method, *args, **kw)

        raises_('__contains__', user_arg)

        raises_('add', user_arg)

        raises_('add_all', (user_arg,))

        raises_('delete', user_arg)

        raises_('expire', user_arg)

        raises_('expunge', user_arg)

        # flush will no-op without something in the unit of work
        def _():
            class OK(object):
                pass
            self._map_it(OK)

            s = create_session()
            s.add(OK())
            x_raises_(s, 'flush', (user_arg,))
        _()

        raises_('is_modified', user_arg)

        raises_('merge', user_arg)

        raises_('refresh', user_arg)

        instance_methods = self._public_session_methods() \
            - self._class_methods - set([
                'bulk_update_mappings', 'bulk_insert_mappings',
                'bulk_save_objects'])

        eq_(watchdog, instance_methods,
            watchdog.symmetric_difference(instance_methods))

    def _test_class_guards(self, user_arg, is_class=True):
        watchdog = set()

        def raises_(method, *args, **kw):
            watchdog.add(method)
            callable_ = getattr(create_session(), method)
            if is_class:
                assert_raises(
                    sa.orm.exc.UnmappedClassError,
                    callable_, *args, **kw)
            else:
                assert_raises(
                    sa.exc.NoInspectionAvailable, callable_, *args, **kw)

        raises_('connection', mapper=user_arg)

        raises_('execute', 'SELECT 1', mapper=user_arg)

        raises_('get_bind', mapper=user_arg)

        raises_('scalar', 'SELECT 1', mapper=user_arg)

        eq_(watchdog, self._class_methods,
            watchdog.symmetric_difference(self._class_methods))

    def test_unmapped_instance(self):
        class Unmapped(object):
            pass

        self._test_instance_guards(Unmapped())
        self._test_class_guards(Unmapped)

    def test_unmapped_primitives(self):
        for prim in ('doh', 123, ('t', 'u', 'p', 'l', 'e')):
            self._test_instance_guards(prim)
            self._test_class_guards(prim, is_class=False)

    def test_unmapped_class_for_instance(self):
        class Unmapped(object):
            pass

        self._test_instance_guards(Unmapped)
        self._test_class_guards(Unmapped)

    def test_mapped_class_for_instance(self):
        class Mapped(object):
            pass
        self._map_it(Mapped)

        self._test_instance_guards(Mapped)
        # no class guards- it would pass.

    def test_missing_state(self):
        class Mapped(object):
            pass
        early = Mapped()
        self._map_it(Mapped)

        self._test_instance_guards(early)
        self._test_class_guards(early, is_class=False)


class TLTransactionTest(fixtures.MappedTest):
    run_dispose_bind = 'once'
    __backend__ = True

    @classmethod
    def setup_bind(cls):
        return engines.testing_engine(options=dict(strategy='threadlocal'))

    @classmethod
    def define_tables(cls, metadata):
        Table('users', metadata, Column('id', Integer,
              primary_key=True, test_needs_autoincrement=True),
              Column('name', String(20)), test_needs_acid=True)

    @classmethod
    def setup_classes(cls):
        class User(cls.Basic):
            pass

    @classmethod
    def setup_mappers(cls):
        users, User = cls.tables.users, cls.classes.User

        mapper(User, users)

    @testing.exclude('mysql', '<', (5, 0, 3), 'FIXME: unknown')
    def test_session_nesting(self):
        User = self.classes.User

        sess = create_session(bind=self.bind)
        self.bind.begin()
        u = User(name='ed')
        sess.add(u)
        sess.flush()
        self.bind.commit()



class FlushWarningsTest(fixtures.MappedTest):
    run_setup_mappers = 'each'

    @classmethod
    def define_tables(cls, metadata):
        Table('user', metadata,
                Column('id', Integer, primary_key=True,
                            test_needs_autoincrement=True),
                Column('name', String(20))
            )

        Table('address', metadata,
                Column('id', Integer, primary_key=True,
                            test_needs_autoincrement=True),
                Column('user_id', Integer, ForeignKey('user.id')),
                Column('email', String(20))
            )

    @classmethod
    def setup_classes(cls):
        class User(cls.Basic):
            pass
        class Address(cls.Basic):
            pass

    @classmethod
    def setup_mappers(cls):
        user, User = cls.tables.user, cls.classes.User
        address, Address = cls.tables.address, cls.classes.Address
        mapper(User, user, properties={
                'addresses': relationship(Address, backref="user")
            })
        mapper(Address, address)

    def test_o2m_cascade_add(self):
        Address = self.classes.Address
        def evt(mapper, conn, instance):
            instance.addresses.append(Address(email='x1'))
        self._test(evt, "collection append")

    def test_o2m_cascade_remove(self):
        def evt(mapper, conn, instance):
            del instance.addresses[0]
        self._test(evt, "collection remove")

    def test_m2o_cascade_add(self):
        User = self.classes.User
        def evt(mapper, conn, instance):
            instance.addresses[0].user = User(name='u2')
        self._test(evt, "related attribute set")

    def test_m2o_cascade_remove(self):
        def evt(mapper, conn, instance):
            a1 = instance.addresses[0]
            del a1.user
        self._test(evt, "related attribute delete")

    def test_plain_add(self):
        Address = self.classes.Address
        def evt(mapper, conn, instance):
            object_session(instance).add(Address(email='x1'))
        self._test(evt, "Session.add\(\)")

    def test_plain_merge(self):
        Address = self.classes.Address
        def evt(mapper, conn, instance):
            object_session(instance).merge(Address(email='x1'))
        self._test(evt, "Session.merge\(\)")

    def test_plain_delete(self):
        Address = self.classes.Address
        def evt(mapper, conn, instance):
            object_session(instance).delete(Address(email='x1'))
        self._test(evt, "Session.delete\(\)")

    def _test(self, fn, method):
        User = self.classes.User
        Address = self.classes.Address

        s = Session()
        event.listen(User, "after_insert", fn)

        u1 = User(name='u1', addresses=[Address(name='a1')])
        s.add(u1)
        assert_raises_message(
            sa.exc.SAWarning,
            "Usage of the '%s'" % method,
            s.commit
        )
