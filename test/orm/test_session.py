import inspect as _py_inspect

import sqlalchemy as sa
from sqlalchemy import event
from sqlalchemy import ForeignKey
from sqlalchemy import inspect
from sqlalchemy import Integer
from sqlalchemy import select
from sqlalchemy import Sequence
from sqlalchemy import String
from sqlalchemy import testing
from sqlalchemy import text
from sqlalchemy.orm import attributes
from sqlalchemy.orm import backref
from sqlalchemy.orm import close_all_sessions
from sqlalchemy.orm import exc as orm_exc
from sqlalchemy.orm import joinedload
from sqlalchemy.orm import make_transient
from sqlalchemy.orm import make_transient_to_detached
from sqlalchemy.orm import mapper
from sqlalchemy.orm import object_session
from sqlalchemy.orm import relationship
from sqlalchemy.orm import Session
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import was_deleted
from sqlalchemy.testing import assert_raises
from sqlalchemy.testing import assert_raises_message
from sqlalchemy.testing import assertions
from sqlalchemy.testing import config
from sqlalchemy.testing import engines
from sqlalchemy.testing import eq_
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import is_
from sqlalchemy.testing import is_not
from sqlalchemy.testing import is_true
from sqlalchemy.testing import mock
from sqlalchemy.testing import pickleable
from sqlalchemy.testing.fixtures import fixture_session
from sqlalchemy.testing.schema import Column
from sqlalchemy.testing.schema import Table
from sqlalchemy.testing.util import gc_collect
from sqlalchemy.util import pickle
from sqlalchemy.util.compat import inspect_getfullargspec
from test.orm import _fixtures


class ExecutionTest(_fixtures.FixtureTest):
    run_inserts = None
    __backend__ = True

    @testing.requires.sequences
    def test_sequence_execute(self, connection):
        seq = Sequence("some_sequence")
        seq.create(connection)
        try:
            sess = Session(connection)
            eq_(sess.execute(seq), connection.dialect.default_sequence_base)
        finally:
            seq.drop(connection)

    def test_parameter_execute(self):
        users = self.tables.users
        sess = Session(bind=testing.db)
        sess.execute(
            users.insert(), [{"id": 7, "name": "u7"}, {"id": 8, "name": "u8"}]
        )
        sess.execute(users.insert(), {"id": 9, "name": "u9"})
        eq_(
            sess.execute(
                sa.select(users.c.id).order_by(users.c.id)
            ).fetchall(),
            [(7,), (8,), (9,)],
        )


class TransScopingTest(_fixtures.FixtureTest):
    run_inserts = None
    __prefer_requires__ = ("independent_connections",)

    def test_no_close_on_flush(self):
        """Flush() doesn't close a connection the session didn't open"""

        User, users = self.classes.User, self.tables.users

        c = testing.db.connect()
        c.exec_driver_sql("select * from users")

        mapper(User, users)
        s = Session(bind=c)
        s.add(User(name="first"))
        s.flush()
        c.exec_driver_sql("select * from users")

    def test_close(self):
        """close() doesn't close a connection the session didn't open"""

        User, users = self.classes.User, self.tables.users

        c = testing.db.connect()
        c.exec_driver_sql("select * from users")

        mapper(User, users)
        s = Session(bind=c)
        s.add(User(name="first"))
        s.flush()
        c.exec_driver_sql("select * from users")
        s.close()
        c.exec_driver_sql("select * from users")

    def test_autobegin_execute(self):
        # test the new autobegin behavior introduced in #5074
        s = Session(testing.db)

        is_(s._transaction, None)

        s.execute(select(1))
        is_not(s._transaction, None)

        s.commit()
        is_(s._transaction, None)

        s.execute(select(1))
        is_not(s._transaction, None)

        s.close()
        is_(s._transaction, None)

        s.execute(select(1))
        is_not(s._transaction, None)

        s.close()
        is_(s._transaction, None)

    def test_autobegin_flush(self):
        # test the new autobegin behavior introduced in #5074
        User, users = self.classes.User, self.tables.users

        mapper(User, users)

        s = Session(testing.db)

        is_(s._transaction, None)

        # empty flush, nothing happens
        s.flush()
        is_(s._transaction, None)

        s.add(User(id=1, name="name"))
        s.flush()
        is_not(s._transaction, None)
        s.commit()
        is_(s._transaction, None)

    def test_autobegin_begin_method(self):
        s = Session(testing.db)

        s.begin()  # OK

        assert_raises_message(
            sa.exc.InvalidRequestError,
            "A transaction is already begun on this Session.",
            s.begin,
        )

    @testing.requires.independent_connections
    @engines.close_open_connections
    def test_transaction(self):
        User, users = self.classes.User, self.tables.users

        mapper(User, users)
        conn1 = testing.db.connect()
        conn2 = testing.db.connect()

        sess = Session(autocommit=False, bind=conn1)
        u = User(name="x")
        sess.add(u)
        sess.flush()
        assert (
            conn1.exec_driver_sql("select count(1) from users").scalar() == 1
        )
        assert (
            conn2.exec_driver_sql("select count(1) from users").scalar() == 0
        )
        sess.commit()
        assert (
            conn1.exec_driver_sql("select count(1) from users").scalar() == 1
        )

        assert (
            testing.db.connect()
            .exec_driver_sql("select count(1) from users")
            .scalar()
            == 1
        )
        sess.close()


class SessionUtilTest(_fixtures.FixtureTest):
    run_inserts = None

    def test_close_all_sessions(self):
        users, User = self.tables.users, self.classes.User

        mapper(User, users)

        s1 = fixture_session()
        u1 = User()
        s1.add(u1)

        s2 = fixture_session()
        u2 = User()
        s2.add(u2)

        assert u1 in s1
        assert u2 in s2

        close_all_sessions()

        assert u1 not in s1
        assert u2 not in s2

    def test_session_close_all_deprecated(self):
        users, User = self.tables.users, self.classes.User

        mapper(User, users)

        s1 = fixture_session()
        u1 = User()
        s1.add(u1)

        s2 = fixture_session()
        u2 = User()
        s2.add(u2)

        assert u1 in s1
        assert u2 in s2

        with assertions.expect_deprecated(
            r"The Session.close_all\(\) method is deprecated and will "
            "be removed in a future release. "
        ):
            Session.close_all()

        assert u1 not in s1
        assert u2 not in s2

    def test_object_session_raises(self):
        User = self.classes.User

        assert_raises(orm_exc.UnmappedInstanceError, object_session, object())

        assert_raises(orm_exc.UnmappedInstanceError, object_session, User())

    def test_make_transient(self):
        users, User = self.tables.users, self.classes.User

        mapper(User, users)
        sess = fixture_session(autoflush=False)
        sess.add(User(name="test"))
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

        u1.name = "test2"
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
        sess = fixture_session()
        u1 = User(name="test")
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
        sess = fixture_session()
        u1 = User(id=1, name="test")
        sess.add(u1)
        sess.commit()
        sess.close()

        u2 = User(id=1)
        make_transient_to_detached(u2)
        assert "id" in u2.__dict__
        sess.add(u2)
        eq_(u2.name, "test")

    def test_make_transient_to_detached_no_session_allowed(self):
        users, User = self.tables.users, self.classes.User

        mapper(User, users)
        sess = fixture_session()
        u1 = User(id=1, name="test")
        sess.add(u1)
        assert_raises_message(
            sa.exc.InvalidRequestError,
            "Given object must be transient",
            make_transient_to_detached,
            u1,
        )

    def test_make_transient_to_detached_no_key_allowed(self):
        users, User = self.tables.users, self.classes.User

        mapper(User, users)
        sess = fixture_session()
        u1 = User(id=1, name="test")
        sess.add(u1)
        sess.commit()
        sess.expunge(u1)
        assert_raises_message(
            sa.exc.InvalidRequestError,
            "Given object must be transient",
            make_transient_to_detached,
            u1,
        )


class SessionStateTest(_fixtures.FixtureTest):
    run_inserts = None

    __prefer_requires__ = ("independent_connections",)

    def test_info(self):
        s = fixture_session()
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
        s4 = maker2(info={"s4": 8})
        eq_(s4.info, {"s4": 8})

    @testing.requires.independent_connections
    @engines.close_open_connections
    def test_autoflush(self):
        User, users = self.classes.User, self.tables.users

        bind = testing.db
        mapper(User, users)
        conn1 = bind.connect()
        conn2 = bind.connect()

        sess = Session(bind=conn1, autocommit=False, autoflush=True)
        u = User()
        u.name = "ed"
        sess.add(u)
        u2 = sess.query(User).filter_by(name="ed").one()
        assert u2 is u
        eq_(conn1.exec_driver_sql("select count(1) from users").scalar(), 1)
        eq_(conn2.exec_driver_sql("select count(1) from users").scalar(), 0)
        sess.commit()
        eq_(conn1.exec_driver_sql("select count(1) from users").scalar(), 1)
        eq_(
            bind.connect()
            .exec_driver_sql("select count(1) from users")
            .scalar(),
            1,
        )
        sess.close()

    def test_with_no_autoflush(self):
        User, users = self.classes.User, self.tables.users

        mapper(User, users)
        sess = fixture_session()

        u = User()
        u.name = "ed"
        sess.add(u)

        def go(obj):
            assert u not in sess.query(User).all()

        testing.run_as_contextmanager(sess.no_autoflush, go)
        assert u in sess.new
        assert u in sess.query(User).all()
        assert u not in sess.new

    def test_with_no_autoflush_after_exception(self):
        sess = Session(autoflush=True)

        assert_raises(
            ZeroDivisionError,
            testing.run_as_contextmanager,
            sess.no_autoflush,
            lambda obj: 1 / 0,
        )

        is_true(sess.autoflush)

    def test_autoflush_exception_addition(self):
        User, users = self.classes.User, self.tables.users
        Address, addresses = self.classes.Address, self.tables.addresses
        mapper(User, users, properties={"addresses": relationship(Address)})
        mapper(Address, addresses)

        s = Session(testing.db)

        u1 = User(name="first")

        s.add(u1)
        s.commit()

        u1.addresses.append(Address(email=None))

        # will raise for null email address
        assert_raises_message(
            sa.exc.DBAPIError,
            ".*raised as a result of Query-invoked autoflush; consider using "
            "a session.no_autoflush block.*",
            s.query(User).first,
        )

    def test_deleted_flag(self):
        users, User = self.tables.users, self.classes.User

        mapper(User, users)

        sess = fixture_session()

        u1 = User(name="u1")
        sess.add(u1)
        sess.commit()

        sess.delete(u1)
        sess.flush()
        assert u1 not in sess
        assert_raises(sa.exc.InvalidRequestError, sess.add, u1)
        assert sess.in_transaction()
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

    @testing.requires.sane_rowcount
    def test_deleted_adds_to_imap_unconditionally(self):
        users, User = self.tables.users, self.classes.User

        mapper(User, users)

        sess = fixture_session()
        u1 = User(name="u1")
        sess.add(u1)
        sess.commit()

        sess.delete(u1)
        sess.flush()

        # object is not in session
        assert u1 not in sess

        # but it *is* attached
        assert u1._sa_instance_state.session_id == sess.hash_key

        # mark as deleted again
        sess.delete(u1)

        # in the session again
        assert u1 in sess

        # commit proceeds w/ warning
        with assertions.expect_warnings(
            "DELETE statement on table 'users' "
            r"expected to delete 1 row\(s\); 0 were matched."
        ):
            sess.commit()

    @testing.requires.independent_connections
    @engines.close_open_connections
    def test_autoflush_unbound(self):
        User, users = self.classes.User, self.tables.users

        mapper(User, users)
        with fixture_session(autocommit=False, autoflush=True) as sess:
            u = User()
            u.name = "ed"
            sess.add(u)
            u2 = sess.query(User).filter_by(name="ed").one()
            is_(u2, u)
            eq_(
                sess.execute(
                    text("select count(1) from users"),
                    bind_arguments=dict(mapper=User),
                ).scalar(),
                1,
            )
            eq_(
                testing.db.connect()
                .exec_driver_sql("select count(1) from users")
                .scalar(),
                0,
            )
            sess.commit()
            eq_(
                sess.execute(
                    text("select count(1) from users"),
                    bind_arguments=dict(mapper=User),
                ).scalar(),
                1,
            )
            eq_(
                testing.db.connect()
                .exec_driver_sql("select count(1) from users")
                .scalar(),
                1,
            )

    @engines.close_open_connections
    def test_autoflush_2(self):
        User, users = self.classes.User, self.tables.users

        mapper(User, users)
        conn1 = testing.db.connect()
        sess = Session(bind=conn1, autocommit=False, autoflush=True)
        u = User()
        u.name = "ed"
        sess.add(u)
        sess.commit()
        assert (
            conn1.exec_driver_sql("select count(1) from users").scalar() == 1
        )
        assert (
            testing.db.connect()
            .exec_driver_sql("select count(1) from users")
            .scalar()
            == 1
        )
        sess.commit()

    def test_autocommit_doesnt_raise_on_pending(self):
        User, users = self.classes.User, self.tables.users

        mapper(User, users)
        session = Session(testing.db, autocommit=True)

        session.add(User(name="ed"))

        session.begin()
        session.flush()
        session.commit()

    def test_active_flag_autocommit(self):
        sess = Session(bind=config.db, autocommit=True)
        assert not sess.is_active
        sess.begin()
        assert sess.is_active
        sess.rollback()
        assert not sess.is_active

    def test_active_flag_autobegin(self):
        sess = Session(bind=config.db, autocommit=False)
        assert sess.is_active
        assert not sess.in_transaction()
        sess.begin()
        assert sess.is_active
        sess.rollback()
        assert sess.is_active

    def test_active_flag_autobegin_future(self):
        sess = Session(bind=config.db, future=True)
        assert sess.is_active
        assert not sess.in_transaction()
        sess.begin()
        assert sess.is_active
        sess.rollback()
        assert sess.is_active

    def test_active_flag_partial_rollback(self):
        sess = Session(bind=config.db, autocommit=False)
        assert sess.is_active
        assert not sess.in_transaction()
        sess.begin()
        assert sess.is_active
        sess.begin(_subtrans=True)
        sess.rollback()
        assert not sess.is_active

        sess.rollback()
        assert sess.is_active

    @engines.close_open_connections
    def test_add_delete(self):
        User, Address, addresses, users = (
            self.classes.User,
            self.classes.Address,
            self.tables.addresses,
            self.tables.users,
        )

        s = fixture_session()
        mapper(
            User,
            users,
            properties={
                "addresses": relationship(Address, cascade="all, delete")
            },
        )
        mapper(Address, addresses)

        user = User(name="u1")

        assert_raises_message(
            sa.exc.InvalidRequestError, "is not persisted", s.delete, user
        )

        s.add(user)
        s.commit()
        user = s.query(User).one()
        s.expunge(user)
        assert user not in s

        # modify outside of session, assert changes remain/get saved
        user.name = "fred"
        s.add(user)
        assert user in s
        assert user in s.dirty
        s.commit()
        assert s.query(User).count() == 1
        user = s.query(User).one()
        assert user.name == "fred"

        # ensure its not dirty if no changes occur
        s.expunge_all()
        assert user not in s
        s.add(user)
        assert user in s
        assert user not in s.dirty

        s2 = fixture_session()
        assert_raises_message(
            sa.exc.InvalidRequestError,
            "is already attached to session",
            s2.delete,
            user,
        )
        u2 = s2.query(User).get(user.id)
        s2.expunge(u2)
        assert_raises_message(
            sa.exc.InvalidRequestError,
            "another instance .* is already present",
            s.delete,
            u2,
        )
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

        s1 = fixture_session()
        s2 = fixture_session()

        u1 = User(id=1, name="u1")
        make_transient_to_detached(u1)  # shorthand for actually persisting it
        s1.add(u1)

        assert_raises_message(
            sa.exc.InvalidRequestError,
            "Object '<User.*?>' is already attached to session",
            s2.add,
            u1,
        )
        assert u1 not in s2
        assert not s2.identity_map.keys()

    def test_identity_conflict(self):
        users, User = self.tables.users, self.classes.User

        mapper(User, users)

        with fixture_session() as s:
            s.execute(users.delete())
            u1 = User(name="ed")
            s.add(u1)
            s.flush()
            s.expunge(u1)
            u2 = s.query(User).first()
            s.expunge(u2)
            s.identity_map.add(sa.orm.attributes.instance_state(u1))

            assert_raises_message(
                sa.exc.InvalidRequestError,
                "Can't attach instance <User.*?>; another instance "
                "with key .*? is already "
                "present in this session.",
                s.identity_map.add,
                sa.orm.attributes.instance_state(u2),
            )

    def test_internal_identity_conflict_warning_weak(self):
        # test for issue #4890
        # see also test_naturalpks::ReversePKsTest::test_reverse
        users, User = self.tables.users, self.classes.User
        addresses, Address = self.tables.addresses, self.classes.Address

        mapper(
            User,
            users,
            properties={"addresses": relationship(Address, backref="user")},
        )
        mapper(Address, addresses)

        session = fixture_session()

        @event.listens_for(session, "after_flush")
        def load_collections(session, flush_context):
            for target in set(session.new).union(session.dirty):
                if isinstance(target, User):
                    target.addresses

        u1 = User(name="u1")
        a1 = Address(email_address="e1", user=u1)
        session.add_all([u1, a1])
        session.flush()

        session.expire_all()

        # create new Address via backref, so that u1.addresses remains
        # expired and a2 is in pending mutations
        a2 = Address(email_address="e2", user=u1)
        assert "addresses" not in inspect(u1).dict
        assert a2 in inspect(u1)._pending_mutations["addresses"].added_items

        with assertions.expect_warnings(
            r"Identity map already had an identity "
            r"for \(.*Address.*\), replacing"
        ):
            session.flush()

    def test_pickled_update(self):
        users, User = self.tables.users, pickleable.User

        mapper(User, users)
        sess1 = fixture_session()
        sess2 = fixture_session()
        u1 = User(name="u1")
        sess1.add(u1)
        assert_raises_message(
            sa.exc.InvalidRequestError,
            "already attached to session",
            sess2.add,
            u1,
        )
        u2 = pickle.loads(pickle.dumps(u1))
        sess2.add(u2)

    def test_duplicate_update(self):
        users, User = self.tables.users, self.classes.User

        mapper(User, users)
        Session = sessionmaker()
        sess = fixture_session()

        u1 = User(name="u1")
        sess.add(u1)
        sess.flush()
        assert u1.id is not None

        sess.expunge(u1)

        assert u1 not in sess
        assert Session.object_session(u1) is None

        u2 = sess.query(User).get(u1.id)
        assert u2 is not None and u2 is not u1
        assert u2 in sess

        assert_raises_message(
            sa.exc.InvalidRequestError,
            "Can't attach instance <User.*?>; another instance "
            "with key .*? is already "
            "present in this session.",
            sess.add,
            u1,
        )

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

        sess = fixture_session()

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

        sess = fixture_session()

        sess.add_all([User(name="u1"), User(name="u2"), User(name="u3")])
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

        s = fixture_session()

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
            s.commit,
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
        sess = fixture_session()
        sess.add(User(name="x"))
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
        sess = fixture_session()
        u1 = User(name="x")
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
        sess = fixture_session()
        sess.add(User(name="x"))
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


class DeferredRelationshipExpressionTest(_fixtures.FixtureTest):
    run_inserts = None
    run_deletes = "each"

    @classmethod
    def setup_mappers(cls):
        users, Address, addresses, User = (
            cls.tables.users,
            cls.classes.Address,
            cls.tables.addresses,
            cls.classes.User,
        )

        mapper(
            User,
            users,
            properties={"addresses": relationship(Address, backref="user")},
        )
        mapper(Address, addresses)

    def test_deferred_expression_unflushed(self):
        """test that an expression which is dependent on object state is
        evaluated after the session autoflushes.   This is the lambda
        inside of strategies.py lazy_clause.

        """
        User, Address = self.classes("User", "Address")

        sess = fixture_session(autoflush=True, autocommit=False)
        u = User(name="ed", addresses=[Address(email_address="foo")])
        sess.add(u)
        eq_(
            sess.query(Address).filter(Address.user == u).one(),
            Address(email_address="foo"),
        )

    def test_deferred_expression_obj_was_gced(self):
        User, Address = self.classes("User", "Address")

        sess = fixture_session(
            autoflush=True, autocommit=False, expire_on_commit=False
        )
        u = User(name="ed", addresses=[Address(email_address="foo")])
        sess.add(u)

        sess.commit()
        sess.close()
        u = sess.query(User).get(u.id)
        q = sess.query(Address).filter(Address.user == u)
        del u
        gc_collect()
        eq_(q.one(), Address(email_address="foo"))

    def test_deferred_expression_favors_immediate(self):
        """Test that a deferred expression will return an immediate value
        if available, rather than invoking after the object is detached

        """

        User, Address = self.classes("User", "Address")

        sess = fixture_session(
            autoflush=True, autocommit=False, expire_on_commit=False
        )
        u = User(name="ed", addresses=[Address(email_address="foo")])
        sess.add(u)
        sess.commit()

        q = sess.query(Address).filter(Address.user == u)
        sess.expire(u)
        sess.expunge(u)
        eq_(q.one(), Address(email_address="foo"))

    def test_deferred_expression_obj_was_never_flushed(self):
        User, Address = self.classes("User", "Address")

        sess = fixture_session(autoflush=True, autocommit=False)
        u = User(name="ed", addresses=[Address(email_address="foo")])

        assert_raises_message(
            sa.exc.InvalidRequestError,
            "Can't resolve value for column users.id on object "
            ".User.*.; no value has been set for this column",
            (Address.user == u).left.callable,
        )

        q = sess.query(Address).filter(Address.user == u)
        assert_raises_message(
            sa.exc.StatementError,
            "Can't resolve value for column users.id on object "
            ".User.*.; no value has been set for this column",
            q.one,
        )

    def test_deferred_expression_transient_but_manually_set(self):
        User, Address = self.classes("User", "Address")

        u = User(id=5, name="ed", addresses=[Address(email_address="foo")])

        expr = Address.user == u
        eq_(expr.left.callable(), 5)

    def test_deferred_expression_unflushed_obj_became_detached_unexpired(self):
        User, Address = self.classes("User", "Address")

        sess = fixture_session(autoflush=True, autocommit=False)
        u = User(name="ed", addresses=[Address(email_address="foo")])

        q = sess.query(Address).filter(Address.user == u)

        sess.add(u)
        sess.flush()

        sess.expunge(u)
        eq_(q.one(), Address(email_address="foo"))

    def test_deferred_expression_unflushed_obj_became_detached_expired(self):
        User, Address = self.classes("User", "Address")

        sess = fixture_session(autoflush=True, autocommit=False)
        u = User(name="ed", addresses=[Address(email_address="foo")])

        q = sess.query(Address).filter(Address.user == u)

        sess.add(u)
        sess.flush()

        sess.expire(u)
        sess.expunge(u)
        eq_(q.one(), Address(email_address="foo"))

    def test_deferred_expr_unflushed_obj_became_detached_expired_by_key(self):
        User, Address = self.classes("User", "Address")

        sess = fixture_session(autoflush=True, autocommit=False)
        u = User(name="ed", addresses=[Address(email_address="foo")])

        q = sess.query(Address).filter(Address.user == u)

        sess.add(u)
        sess.flush()

        sess.expire(u, ["id"])
        sess.expunge(u)
        eq_(q.one(), Address(email_address="foo"))

    def test_deferred_expression_expired_obj_became_detached_expired(self):
        User, Address = self.classes("User", "Address")

        sess = fixture_session(
            autoflush=True, autocommit=False, expire_on_commit=True
        )
        u = User(name="ed", addresses=[Address(email_address="foo")])

        sess.add(u)
        sess.commit()

        assert "id" not in u.__dict__  # it's expired

        # should not emit SQL
        def go():
            Address.user == u

        self.assert_sql_count(testing.db, go, 0)

        # create the expression here, but note we weren't tracking 'id'
        # yet so we don't have the old value
        q = sess.query(Address).filter(Address.user == u)

        sess.expunge(u)
        assert_raises_message(
            sa.exc.StatementError,
            "Can't resolve value for column users.id on object "
            ".User.*.; the object is detached and the value was expired",
            q.one,
        )


class SessionStateWFixtureTest(_fixtures.FixtureTest):
    __backend__ = True

    def test_autoflush_rollback(self):
        Address, addresses, users, User = (
            self.classes.Address,
            self.tables.addresses,
            self.tables.users,
            self.classes.User,
        )

        mapper(Address, addresses)
        mapper(User, users, properties={"addresses": relationship(Address)})

        sess = fixture_session(autocommit=False, autoflush=True)
        u = sess.query(User).get(8)
        newad = Address(email_address="a new address")
        u.addresses.append(newad)
        u.name = "some new name"
        assert u.name == "some new name"
        assert len(u.addresses) == 4
        assert newad in u.addresses
        sess.rollback()
        assert u.name == "ed"
        assert len(u.addresses) == 3

        assert newad not in u.addresses
        # pending objects don't get expired
        assert newad.email_address == "a new address"

    def test_expunge_cascade(self):
        Address, addresses, users, User = (
            self.classes.Address,
            self.tables.addresses,
            self.tables.users,
            self.classes.User,
        )

        mapper(Address, addresses)
        mapper(
            User,
            users,
            properties={
                "addresses": relationship(
                    Address,
                    backref=backref("user", cascade="all"),
                    cascade="all",
                )
            },
        )

        session = fixture_session()
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

    def setup_test(self):
        mapper(self.classes.User, self.tables.users)

    def _assert_modified(self, u1):
        assert sa.orm.attributes.instance_state(u1).modified

    def _assert_not_modified(self, u1):
        assert not sa.orm.attributes.instance_state(u1).modified

    def _assert_cycle(self, u1):
        assert sa.orm.attributes.instance_state(u1)._strong_obj is not None

    def _assert_no_cycle(self, u1):
        assert sa.orm.attributes.instance_state(u1)._strong_obj is None

    def _persistent_fixture(self, gc_collect=False):
        User = self.classes.User
        u1 = User()
        u1.name = "ed"
        if gc_collect:
            sess = Session(testing.db)
        else:
            sess = fixture_session()
        sess.add(u1)
        sess.flush()
        return sess, u1

    def test_transient(self):
        User = self.classes.User
        u1 = User()
        u1.name = "ed"
        self._assert_no_cycle(u1)
        self._assert_modified(u1)

    def test_transient_to_pending(self):
        User = self.classes.User
        u1 = User()
        u1.name = "ed"
        self._assert_modified(u1)
        self._assert_no_cycle(u1)
        sess = fixture_session()
        sess.add(u1)
        self._assert_cycle(u1)
        sess.flush()
        self._assert_no_cycle(u1)
        self._assert_not_modified(u1)

    def test_dirty_persistent_to_detached_via_expunge(self):
        sess, u1 = self._persistent_fixture()
        u1.name = "edchanged"
        self._assert_cycle(u1)
        sess.expunge(u1)
        self._assert_no_cycle(u1)

    def test_dirty_persistent_to_detached_via_close(self):
        sess, u1 = self._persistent_fixture()
        u1.name = "edchanged"
        self._assert_cycle(u1)
        sess.close()
        self._assert_no_cycle(u1)

    def test_clean_persistent_to_detached_via_close(self):
        sess, u1 = self._persistent_fixture()
        self._assert_no_cycle(u1)
        self._assert_not_modified(u1)
        sess.close()
        u1.name = "edchanged"
        self._assert_modified(u1)
        self._assert_no_cycle(u1)

    def test_detached_to_dirty_deleted(self):
        sess, u1 = self._persistent_fixture()
        sess.expunge(u1)
        u1.name = "edchanged"
        self._assert_no_cycle(u1)
        sess.delete(u1)
        self._assert_cycle(u1)

    def test_detached_to_dirty_persistent(self):
        sess, u1 = self._persistent_fixture()
        sess.expunge(u1)
        u1.name = "edchanged"
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
        s2 = fixture_session()
        s2.add(u1)
        self._assert_no_cycle(u1)
        self._assert_not_modified(u1)

    def test_move_persistent_dirty(self):
        sess, u1 = self._persistent_fixture()
        u1.name = "edchanged"
        self._assert_cycle(u1)
        self._assert_modified(u1)
        sess.close()
        self._assert_no_cycle(u1)
        s2 = fixture_session()
        s2.add(u1)
        self._assert_cycle(u1)
        self._assert_modified(u1)

    @testing.requires.predictable_gc
    def test_move_gc_session_persistent_dirty(self):
        sess, u1 = self._persistent_fixture(gc_collect=True)
        u1.name = "edchanged"
        self._assert_cycle(u1)
        self._assert_modified(u1)
        del sess
        gc_collect()
        self._assert_cycle(u1)
        s2 = Session(testing.db)
        s2.add(u1)
        self._assert_cycle(u1)
        self._assert_modified(u1)

    def test_persistent_dirty_to_expired(self):
        sess, u1 = self._persistent_fixture()
        u1.name = "edchanged"
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

        s = fixture_session()
        mapper(User, users)

        s.add(User(name="ed"))
        s.flush()
        assert not s.dirty

        user = s.query(User).one()
        del user
        gc_collect()
        assert len(s.identity_map) == 0

        user = s.query(User).one()
        user.name = "fred"
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
        assert user.name == "fred"
        assert s.identity_map

    @testing.requires.predictable_gc
    def test_weakref_pickled(self):
        users, User = self.tables.users, pickleable.User

        s = fixture_session()
        mapper(User, users)

        s.add(User(name="ed"))
        s.flush()
        assert not s.dirty

        user = s.query(User).one()
        user.name = "fred"
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
        Address, addresses, users, User = (
            self.classes.Address,
            self.tables.addresses,
            self.tables.users,
            self.classes.User,
        )

        s = fixture_session()
        mapper(
            User,
            users,
            properties={"addresses": relationship(Address, backref="user")},
        )
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
        user.addresses[0].email_address = "ed2"
        user.addresses[0].user  # lazyload
        del user
        gc_collect()
        assert len(s.identity_map) == 2

        s.commit()
        user = s.query(User).options(joinedload(User.addresses)).one()
        eq_(user, User(name="ed", addresses=[Address(email_address="ed2")]))

    @testing.requires.predictable_gc
    def test_weakref_with_cycles_o2o(self):
        Address, addresses, users, User = (
            self.classes.Address,
            self.tables.addresses,
            self.tables.users,
            self.classes.User,
        )

        s = fixture_session()
        mapper(
            User,
            users,
            properties={
                "address": relationship(Address, backref="user", uselist=False)
            },
        )
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
        user.address.email_address = "ed2"
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

        sess = Session(testing.db)

        u1 = User(name="u1")
        sess.add(u1)
        sess.commit()

        # can't add u1 to Session,
        # already belongs to u2
        s2 = Session(testing.db)
        assert_raises_message(
            sa.exc.InvalidRequestError,
            r".*is already attached to session",
            s2.add,
            u1,
        )

        # garbage collect sess
        del sess
        gc_collect()

        # s2 lets it in now despite u1 having
        # session_key
        s2.add(u1)
        assert u1 in s2

    def test_fast_discard_race(self):
        # test issue #4068
        users, User = self.tables.users, self.classes.User

        mapper(User, users)

        sess = fixture_session()

        u1 = User(name="u1")
        sess.add(u1)
        sess.commit()

        u1_state = u1._sa_instance_state
        ref = u1_state.obj
        u1_state.obj = lambda: None

        u2 = sess.query(User).first()
        u1_state._cleanup(ref)

        u3 = sess.query(User).first()

        is_(u2, u3)

        u2_state = u2._sa_instance_state
        ref = u2_state.obj
        u2_state.obj = lambda: None
        u2_state._cleanup(ref)
        assert not sess.identity_map.contains_state(u2._sa_instance_state)


class IsModifiedTest(_fixtures.FixtureTest):
    run_inserts = None

    def _default_mapping_fixture(self):
        User, Address = self.classes.User, self.classes.Address
        users, addresses = self.tables.users, self.tables.addresses
        mapper(User, users, properties={"addresses": relationship(Address)})
        mapper(Address, addresses)
        return User, Address

    def test_is_modified(self):
        User, Address = self._default_mapping_fixture()

        s = fixture_session()

        # save user
        u = User(name="fred")
        s.add(u)
        s.flush()
        s.expunge_all()

        user = s.query(User).one()
        assert user not in s.dirty
        assert not s.is_modified(user)
        user.name = "fred"
        assert user in s.dirty
        assert not s.is_modified(user)
        user.name = "ed"
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

        s = fixture_session()
        u = User(name="fred", addresses=[Address(email_address="foo")])
        s.add(u)
        s.commit()

        u.id

        def go():
            assert not s.is_modified(u)

        self.assert_sql_count(testing.db, go, 0)

        s.expire_all()
        u.name = "newname"

        # can't predict result here
        # deterministically, depending on if
        # 'name' or 'addresses' is tested first
        mod = s.is_modified(u)
        addresses_loaded = "addresses" in u.__dict__
        assert mod is not addresses_loaded

    def test_is_modified_syn(self):
        User, users = self.classes.User, self.tables.users

        s = fixture_session()

        mapper(User, users, properties={"uname": sa.orm.synonym("name")})
        u = User(uname="fred")
        assert s.is_modified(u)
        s.add(u)
        s.commit()
        assert not s.is_modified(u)


class DisposedStates(fixtures.MappedTest):
    run_setup_mappers = "once"
    run_inserts = "once"
    run_deletes = None

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "t1",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("data", String(50)),
        )

    @classmethod
    def setup_classes(cls):
        class T(cls.Basic):
            def __init__(self, data):
                self.data = data

        mapper(T, cls.tables.t1)

    def teardown_test(self):
        from sqlalchemy.orm.session import _sessions

        _sessions.clear()

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

        sess = Session(config.db, **kwargs)

        data = o1, o2, o3, o4, o5 = [
            T("t1"),
            T("t2"),
            T("t3"),
            T("t4"),
            T("t5"),
        ]

        sess.add_all(data)

        sess.commit()

        o1.data = "t1modified"
        o5.data = "t5modified"

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

    _class_methods = set(("connection", "execute", "get_bind", "scalar"))

    def _public_session_methods(self):
        Session = sa.orm.session.Session

        blocklist = {"begin", "query", "bind_mapper", "get", "bind_table"}
        specials = {"__iter__", "__contains__"}
        ok = set()
        for name in dir(Session):
            if (
                name in Session.__dict__
                and (not name.startswith("_") or name in specials)
                and (
                    _py_inspect.ismethod(getattr(Session, name))
                    or _py_inspect.isfunction(getattr(Session, name))
                )
            ):
                if name in blocklist:
                    continue
                spec = inspect_getfullargspec(getattr(Session, name))
                if len(spec[0]) > 1 or spec[1]:
                    ok.add(name)
        return ok

    def _map_it(self, cls):
        return mapper(
            cls,
            Table(
                "t",
                sa.MetaData(),
                Column(
                    "id",
                    Integer,
                    primary_key=True,
                    test_needs_autoincrement=True,
                ),
            ),
        )

    def _test_instance_guards(self, user_arg):
        watchdog = set()

        def x_raises_(obj, method, *args, **kw):
            watchdog.add(method)
            callable_ = getattr(obj, method)
            assert_raises(
                sa.orm.exc.UnmappedInstanceError, callable_, *args, **kw
            )

        def raises_(method, *args, **kw):
            x_raises_(fixture_session(), method, *args, **kw)

        for name in [
            "__contains__",
            "is_modified",
            "merge",
            "refresh",
            "add",
            "delete",
            "expire",
            "expunge",
            "enable_relationship_loading",
        ]:
            raises_(name, user_arg)

        raises_("add_all", (user_arg,))

        # flush will no-op without something in the unit of work
        def _():
            class OK(object):
                pass

            self._map_it(OK)

            s = fixture_session()
            s.add(OK())
            x_raises_(s, "flush", (user_arg,))

        _()

        instance_methods = (
            self._public_session_methods()
            - self._class_methods
            - set(
                [
                    "bulk_update_mappings",
                    "bulk_insert_mappings",
                    "bulk_save_objects",
                ]
            )
        )

        eq_(
            watchdog,
            instance_methods,
            watchdog.symmetric_difference(instance_methods),
        )

    def _test_class_guards(self, user_arg, is_class=True):
        watchdog = set()

        def raises_(method, *args, **kw):
            watchdog.add(method)
            callable_ = getattr(
                Session(),
                method,
            )
            if is_class:
                assert_raises(
                    sa.orm.exc.UnmappedClassError, callable_, *args, **kw
                )
            else:
                assert_raises(
                    sa.exc.NoInspectionAvailable, callable_, *args, **kw
                )

        raises_("connection", bind_arguments=dict(mapper=user_arg))

        raises_(
            "execute", text("SELECT 1"), bind_arguments=dict(mapper=user_arg)
        )

        raises_("get_bind", mapper=user_arg)

        raises_(
            "scalar", text("SELECT 1"), bind_arguments=dict(mapper=user_arg)
        )

        eq_(
            watchdog,
            self._class_methods,
            watchdog.symmetric_difference(self._class_methods),
        )

    def test_unmapped_instance(self):
        class Unmapped(object):
            pass

        self._test_instance_guards(Unmapped())
        self._test_class_guards(Unmapped)

    def test_unmapped_primitives(self):
        for prim in ("doh", 123, ("t", "u", "p", "l", "e")):
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

    def test_refresh_arg_signature(self):
        class Mapped(object):
            pass

        self._map_it(Mapped)

        m1 = Mapped()
        s = fixture_session()

        with mock.patch.object(s, "_validate_persistent"):
            assert_raises_message(
                sa.exc.ArgumentError,
                "with_for_update should be the boolean value True, "
                "or a dictionary with options",
                s.refresh,
                m1,
                with_for_update={},
            )

            with mock.patch(
                "sqlalchemy.orm.session.loading.load_on_ident"
            ) as load_on_ident:
                s.refresh(m1, with_for_update={"read": True})
                s.refresh(m1, with_for_update=True)
                s.refresh(m1, with_for_update=False)
                s.refresh(m1)

            from sqlalchemy.orm.query import ForUpdateArg

            eq_(
                [
                    call[-1]["with_for_update"]
                    for call in load_on_ident.mock_calls
                ],
                [ForUpdateArg(read=True), ForUpdateArg(), None, None],
            )


class FlushWarningsTest(fixtures.MappedTest):
    run_setup_mappers = "each"

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "user",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("name", String(20)),
        )

        Table(
            "address",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("user_id", Integer, ForeignKey("user.id")),
            Column("email", String(20)),
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
        mapper(
            User,
            user,
            properties={"addresses": relationship(Address, backref="user")},
        )
        mapper(Address, address)

    def test_o2m_cascade_add(self):
        Address = self.classes.Address

        def evt(mapper, conn, instance):
            instance.addresses.append(Address(email="x1"))

        self._test(evt, "collection append")

    def test_o2m_cascade_remove(self):
        def evt(mapper, conn, instance):
            del instance.addresses[0]

        self._test(evt, "collection remove")

    def test_m2o_cascade_add(self):
        User = self.classes.User

        def evt(mapper, conn, instance):
            instance.addresses[0].user = User(name="u2")

        self._test(evt, "related attribute set")

    def test_m2o_cascade_remove(self):
        def evt(mapper, conn, instance):
            a1 = instance.addresses[0]
            del a1.user

        self._test(evt, "related attribute delete")

    def test_plain_add(self):
        Address = self.classes.Address

        def evt(mapper, conn, instance):
            object_session(instance).add(Address(email="x1"))

        self._test(evt, r"Session.add\(\)")

    def test_plain_merge(self):
        Address = self.classes.Address

        def evt(mapper, conn, instance):
            object_session(instance).merge(Address(email="x1"))

        self._test(evt, r"Session.merge\(\)")

    def test_plain_delete(self):
        Address = self.classes.Address

        def evt(mapper, conn, instance):
            object_session(instance).delete(Address(email="x1"))

        self._test(evt, r"Session.delete\(\)")

    def _test(self, fn, method):
        User = self.classes.User
        Address = self.classes.Address

        s = fixture_session()
        event.listen(User, "after_insert", fn)

        u1 = User(name="u1", addresses=[Address(name="a1")])
        s.add(u1)
        assert_raises_message(
            sa.exc.SAWarning, "Usage of the '%s'" % method, s.commit
        )
