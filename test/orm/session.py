import testenv; testenv.configure_for_tests()
import gc
import pickle
from sqlalchemy.orm import create_session, sessionmaker
from testlib import engines, sa, testing
from testlib.sa import Table, Column, Integer, String
from testlib.sa.orm import mapper, relation, backref
from testlib.testing import eq_
from testlib.compat import set
from engine import _base as engine_base
from orm import _base, _fixtures


class SessionTest(_fixtures.FixtureTest):
    run_inserts = None

    @testing.resolve_artifact_names
    def test_no_close_on_flush(self):
        """Flush() doesn't close a connection the session didn't open"""
        c = testing.db.connect()
        c.execute("select * from users")

        mapper(User, users)
        s = create_session(bind=c)
        s.save(User(name='first'))
        s.flush()
        c.execute("select * from users")

    @testing.resolve_artifact_names
    def test_close(self):
        """close() doesn't close a connection the session didn't open"""
        c = testing.db.connect()
        c.execute("select * from users")

        mapper(User, users)
        s = create_session(bind=c)
        s.save(User(name='first'))
        s.flush()
        c.execute("select * from users")
        s.close()
        c.execute("select * from users")

    @testing.resolve_artifact_names
    def test_no_close_transaction_on_flulsh(self):
        c = testing.db.connect()
        try:
            mapper(User, users)
            s = create_session(bind=c)
            s.begin()
            tran = s.transaction
            s.save(User(name='first'))
            s.flush()
            c.execute("select * from users")
            u = User(name='two')
            s.save(u)
            s.flush()
            u = User(name='third')
            s.save(u)
            s.flush()
            assert s.transaction is tran
            tran.close()
        finally:
            c.close()

    @testing.resolve_artifact_names
    def test_expunge_cascade(self):
        mapper(Address, addresses)
        mapper(User, users, properties={
            'addresses':relation(Address,
                                 backref=backref("user", cascade="all"),
                                 cascade="all")})

        _fixtures.run_inserts_for(users)
        _fixtures.run_inserts_for(addresses)

        session = create_session()
        u = session.query(User).filter_by(id=7).one()

        # get everything to load in both directions
        print [a.user for a in u.addresses]

        # then see if expunge fails
        session.expunge(u)

        assert sa.orm.object_session(u) is None
        assert sa.orm.attributes.instance_state(u).session_id is None
        for a in u.addresses:
            assert sa.orm.object_session(a) is None
            assert sa.orm.attributes.instance_state(a).session_id is None

    @engines.close_open_connections
    @testing.resolve_artifact_names
    def test_table_binds_from_expression(self):
        """Session can extract Table objects from ClauseElements and match them to tables."""

        mapper(Address, addresses)
        mapper(User, users, properties={
            'addresses':relation(Address,
                                 backref=backref("user", cascade="all"),
                                 cascade="all")})

        Session = sessionmaker(binds={users: self.metadata.bind,
                                      addresses: self.metadata.bind})
        sess = Session()

        sess.execute(users.insert(), params=dict(id=1, name='ed'))
        eq_(sess.execute(users.select(users.c.id == 1)).fetchall(),
            [(1, 'ed')])

        eq_(sess.execute(users.select(User.id == 1)).fetchall(),
            [(1, 'ed')])

        sess.close()

    @engines.close_open_connections
    @testing.resolve_artifact_names
    def test_mapped_binds_from_expression(self):
        """Session can extract Table objects from ClauseElements and match them to tables."""

        mapper(Address, addresses)
        mapper(User, users, properties={
            'addresses':relation(Address,
                                 backref=backref("user", cascade="all"),
                                 cascade="all")})

        Session = sessionmaker(binds={User: self.metadata.bind,
                                      Address: self.metadata.bind})
        sess = Session()

        sess.execute(users.insert(), params=dict(id=1, name='ed'))
        eq_(sess.execute(users.select(users.c.id == 1)).fetchall(),
            [(1, 'ed')])

        eq_(sess.execute(users.select(User.id == 1)).fetchall(),
            [(1, 'ed')])

        sess.close()

    @engines.close_open_connections
    @testing.resolve_artifact_names
    def test_bind_from_metadata(self):
        mapper(User, users)

        session = create_session()
        session.execute(users.insert(), dict(name='Johnny'))

        assert len(session.query(User).filter_by(name='Johnny').all()) == 1

        session.execute(users.delete())

        assert len(session.query(User).filter_by(name='Johnny').all()) == 0
        session.close()

    @testing.unsupported('mssql', 'test causes mssql to hang')
    @testing.unsupported('sqlite', 'needs true independent connections')
    @engines.close_open_connections
    @testing.resolve_artifact_names
    def test_transaction(self):
        mapper(User, users)
        conn1 = testing.db.connect()
        conn2 = testing.db.connect()

        sess = create_session(autocommit=False, bind=conn1)
        u = User(name='x')
        sess.save(u)
        sess.flush()
        assert conn1.execute("select count(1) from users").scalar() == 1
        assert conn2.execute("select count(1) from users").scalar() == 0
        sess.commit()
        assert conn1.execute("select count(1) from users").scalar() == 1
        assert testing.db.connect().execute("select count(1) from users").scalar() == 1
        sess.close()

    @testing.unsupported('mssql', 'test causes mssql to hang')
    @testing.unsupported('sqlite', 'needs true independent connections')
    @engines.close_open_connections
    @testing.resolve_artifact_names
    def test_autoflush(self):
        bind = self.metadata.bind
        mapper(User, users)
        conn1 = bind.connect()
        conn2 = bind.connect()

        sess = create_session(bind=conn1, autocommit=False, autoflush=True)
        u = User()
        u.name='ed'
        sess.save(u)
        u2 = sess.query(User).filter_by(name='ed').one()
        assert u2 is u
        eq_(conn1.execute("select count(1) from users").scalar(), 1)
        eq_(conn2.execute("select count(1) from users").scalar(),  0)
        sess.commit()
        eq_(conn1.execute("select count(1) from users").scalar(), 1)
        eq_(bind.connect().execute("select count(1) from users").scalar(), 1)
        sess.close()

    @testing.resolve_artifact_names
    def test_autoflush_expressions(self):
        mapper(User, users, properties={
            'addresses':relation(Address, backref="user")})
        mapper(Address, addresses)

        sess = create_session(autoflush=True, autocommit=False)
        u = User(name='ed', addresses=[Address(email_address='foo')])
        sess.save(u)
        eq_(sess.query(Address).filter(Address.user==u).one(),
            Address(email_address='foo'))

    @testing.unsupported('mssql', 'test causes mssql to hang')
    @testing.unsupported('sqlite', 'needs true independent connections')
    @engines.close_open_connections
    @testing.resolve_artifact_names
    def test_autoflush_unbound(self):
        mapper(User, users)

        try:
            sess = create_session(autocommit=False, autoflush=True)
            u = User()
            u.name='ed'
            sess.save(u)
            u2 = sess.query(User).filter_by(name='ed').one()
            assert u2 is u
            assert sess.execute("select count(1) from users", mapper=User).scalar() == 1
            assert testing.db.connect().execute("select count(1) from users").scalar() == 0
            sess.commit()
            assert sess.execute("select count(1) from users", mapper=User).scalar() == 1
            assert testing.db.connect().execute("select count(1) from users").scalar() == 1
            sess.close()
        except:
            sess.rollback()
            raise

    @engines.close_open_connections
    @testing.resolve_artifact_names
    def test_autoflush_2(self):
        mapper(User, users)
        conn1 = testing.db.connect()
        conn2 = testing.db.connect()

        sess = create_session(bind=conn1, autocommit=False, autoflush=True)
        u = User()
        u.name='ed'
        sess.save(u)
        sess.commit()
        assert conn1.execute("select count(1) from users").scalar() == 1
        assert testing.db.connect().execute("select count(1) from users").scalar() == 1
        sess.commit()

    @testing.resolve_artifact_names
    def test_autoflush_rollback(self):
        mapper(Address, addresses)
        mapper(User, users, properties={
            'addresses':relation(Address)})

        _fixtures.run_inserts_for(users)
        _fixtures.run_inserts_for(addresses)

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
        # pending objects dont get expired
        assert newad.email_address == 'a new address'

    @testing.resolve_artifact_names
    def test_textual_execute(self):
        """test that Session.execute() converts to text()"""

        sess = create_session(bind=self.metadata.bind)
        users.insert().execute(id=7, name='jack')

        # use :bindparam style
        eq_(sess.execute("select * from users where id=:id",
                         {'id':7}).fetchall(),
            [(7, u'jack')])

    @engines.close_open_connections
    @testing.resolve_artifact_names
    def test_subtransaction_on_external(self):
        mapper(User, users)
        conn = testing.db.connect()
        trans = conn.begin()
        sess = create_session(bind=conn, autocommit=False, autoflush=True)
        sess.begin(subtransactions=True)
        u = User(name='ed')
        sess.save(u)
        sess.flush()
        sess.commit() # commit does nothing
        trans.rollback() # rolls back
        assert len(sess.query(User).all()) == 0
        sess.close()

    @testing.requires.savepoints
    @engines.close_open_connections
    @testing.resolve_artifact_names
    def test_external_nested_transaction(self):
        mapper(User, users)
        try:
            conn = testing.db.connect()
            trans = conn.begin()
            sess = create_session(bind=conn, autocommit=False, autoflush=True)
            u1 = User(name='u1')
            sess.save(u1)
            sess.flush()

            sess.begin_nested()
            u2 = User(name='u2')
            sess.save(u2)
            sess.flush()
            sess.rollback()

            trans.commit()
            assert len(sess.query(User).all()) == 1
        except:
            conn.close()
            raise

    @testing.requires.savepoints
    @testing.resolve_artifact_names
    def test_heavy_nesting(self):
        session = create_session(bind=testing.db)

        session.begin()
        session.connection().execute("insert into users (name) values ('user1')")

        session.begin(subtransactions=True)

        session.begin_nested()

        session.connection().execute("insert into users (name) values ('user2')")
        assert session.connection().execute("select count(1) from users").scalar() == 2

        session.rollback()
        assert session.connection().execute("select count(1) from users").scalar() == 1
        session.connection().execute("insert into users (name) values ('user3')")

        session.commit()
        assert session.connection().execute("select count(1) from users").scalar() == 2


    @testing.requires.two_phase_transactions
    @testing.resolve_artifact_names
    def test_twophase(self):
        # TODO: mock up a failure condition here
        # to ensure a rollback succeeds
        mapper(User, users)
        mapper(Address, addresses)

        engine2 = engines.testing_engine()
        sess = create_session(autocommit=True, autoflush=False, twophase=True)
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

    @testing.resolve_artifact_names
    def test_subtransaction_on_noautocommit(self):
        mapper(User, users)
        sess = create_session(autocommit=False, autoflush=True)
        sess.begin(subtransactions=True)
        u = User(name='u1')
        sess.save(u)
        sess.flush()
        sess.commit() # commit does nothing
        sess.rollback() # rolls back
        assert len(sess.query(User).all()) == 0
        sess.close()

    @testing.requires.savepoints
    @testing.resolve_artifact_names
    def test_nested_transaction(self):
        mapper(User, users)
        sess = create_session()
        sess.begin()

        u = User(name='u1')
        sess.save(u)
        sess.flush()

        sess.begin_nested()  # nested transaction

        u2 = User(name='u2')
        sess.save(u2)
        sess.flush()

        sess.rollback()

        sess.commit()
        assert len(sess.query(User).all()) == 1
        sess.close()

    @testing.requires.savepoints
    @testing.resolve_artifact_names
    def test_nested_autotrans(self):
        mapper(User, users)
        sess = create_session(autocommit=False)
        u = User(name='u1')
        sess.save(u)
        sess.flush()

        sess.begin_nested()  # nested transaction

        u2 = User(name='u2')
        sess.save(u2)
        sess.flush()

        sess.rollback()

        sess.commit()
        assert len(sess.query(User).all()) == 1
        sess.close()

    @testing.requires.savepoints
    @testing.resolve_artifact_names
    def test_nested_transaction_connection_add(self):
        mapper(User, users)

        sess = create_session(autocommit=True)

        sess.begin()
        sess.begin_nested()

        u1 = User(name='u1')
        sess.save(u1)
        sess.flush()

        sess.rollback()

        u2 = User(name='u2')
        sess.save(u2)

        sess.commit()

        self.assertEquals(set(sess.query(User).all()), set([u2]))

        sess.begin()
        sess.begin_nested()

        u3 = User(name='u3')
        sess.save(u3)
        sess.commit() # commit the nested transaction
        sess.rollback()

        self.assertEquals(set(sess.query(User).all()), set([u2]))

        sess.close()

    @testing.requires.savepoints
    @testing.resolve_artifact_names
    def test_mixed_transaction_control(self):
        mapper(User, users)

        sess = create_session(autocommit=True)

        sess.begin()
        sess.begin_nested()
        transaction = sess.begin(subtransactions=True)

        sess.save(User(name='u1'))

        transaction.commit()
        sess.commit()
        sess.commit()

        sess.close()

        self.assertEquals(len(sess.query(User).all()), 1)

        t1 = sess.begin()
        t2 = sess.begin_nested()

        sess.save(User(name='u2'))

        t2.commit()
        assert sess.transaction is t1

        sess.close()

    @testing.requires.savepoints
    @testing.resolve_artifact_names
    def test_mixed_transaction_close(self):
        mapper(User, users)

        sess = create_session(autocommit=False)

        sess.begin_nested()

        sess.save(User(name='u1'))
        sess.flush()

        sess.close()

        sess.save(User(name='u2'))
        sess.commit()

        sess.close()

        self.assertEquals(len(sess.query(User).all()), 1)

    @testing.resolve_artifact_names
    def test_error_on_using_inactive_session(self):
        mapper(User, users)

        sess = create_session(autocommit=True)

        sess.begin()
        sess.begin(subtransactions=True)

        sess.save(User(name='u1'))
        sess.flush()

        sess.rollback()
        self.assertRaisesMessage(sa.exc.InvalidRequestError, "inactive due to a rollback in a subtransaction", sess.begin, subtransactions=True)
        sess.close()

    @engines.close_open_connections
    @testing.resolve_artifact_names
    def test_bound_connection(self):
        mapper(User, users)
        c = testing.db.connect()
        sess = create_session(bind=c)
        sess.begin()
        transaction = sess.transaction
        u = User(name='u1')
        sess.save(u)
        sess.flush()
        assert transaction._connection_for_bind(testing.db) is transaction._connection_for_bind(c) is c

        self.assertRaisesMessage(sa.exc.InvalidRequestError, "Session already has a Connection associated", transaction._connection_for_bind, testing.db.connect())

        transaction.rollback()
        assert len(sess.query(User).all()) == 0
        sess.close()

    @testing.resolve_artifact_names
    def test_bound_connection_transactional(self):
        mapper(User, users)
        c = testing.db.connect()

        sess = create_session(bind=c, autocommit=False)
        u = User(name='u1')
        sess.save(u)
        sess.flush()
        sess.close()
        assert not c.in_transaction()
        assert c.scalar("select count(1) from users") == 0

        sess = create_session(bind=c, autocommit=False)
        u = User(name='u2')
        sess.save(u)
        sess.flush()
        sess.commit()
        assert not c.in_transaction()
        assert c.scalar("select count(1) from users") == 1
        c.execute("delete from users")
        assert c.scalar("select count(1) from users") == 0

        c = testing.db.connect()

        trans = c.begin()
        sess = create_session(bind=c, autocommit=True)
        u = User(name='u3')
        sess.save(u)
        sess.flush()
        assert c.in_transaction()
        trans.commit()
        assert not c.in_transaction()
        assert c.scalar("select count(1) from users") == 1


    @engines.close_open_connections
    @testing.resolve_artifact_names
    def test_save_update_delete(self):

        s = create_session()
        mapper(User, users)

        user = User(name='u1')

        self.assertRaisesMessage(sa.exc.InvalidRequestError, "is not persisted", s.update, user)
        self.assertRaisesMessage(sa.exc.InvalidRequestError, "is not persisted", s.delete, user)

        s.save(user)
        s.flush()
        user = s.query(User).one()
        s.expunge(user)
        assert user not in s

        # modify outside of session, assert changes remain/get saved
        user.name = "fred"
        s.update(user)
        assert user in s
        assert user in s.dirty
        s.flush()
        s.clear()
        assert s.query(User).count() == 1
        user = s.query(User).one()
        assert user.name == 'fred'

        # ensure its not dirty if no changes occur
        s.clear()
        assert user not in s
        s.update(user)
        assert user in s
        assert user not in s.dirty

        self.assertRaisesMessage(sa.exc.InvalidRequestError, "is already persistent", s.save, user)

        s2 = create_session()
        self.assertRaisesMessage(sa.exc.InvalidRequestError, "is already attached to session", s2.delete, user)

        u2 = s2.query(User).get(user.id)
        self.assertRaisesMessage(sa.exc.InvalidRequestError, "already persisted with a different identity", s.delete, u2)

        s.delete(user)
        s.flush()
        assert user not in s
        assert s.query(User).count() == 0

    @testing.resolve_artifact_names
    def test_is_modified(self):
        s = create_session()

        mapper(User, users, properties={'addresses':relation(Address)})
        mapper(Address, addresses)

        # save user
        u = User(name='fred')
        s.save(u)
        s.flush()
        s.clear()

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


    @testing.resolve_artifact_names
    def test_weak_ref(self):
        """test the weak-referencing identity map, which strongly-references modified items."""

        s = create_session()
        mapper(User, users)

        s.save(User(name='ed'))
        s.flush()
        assert not s.dirty

        user = s.query(User).one()
        del user
        gc.collect()
        assert len(s.identity_map) == 0

        user = s.query(User).one()
        user.name = 'fred'
        del user
        gc.collect()
        assert len(s.identity_map) == 1
        assert len(s.dirty) == 1

        s.flush()
        gc.collect()
        assert not s.dirty
        assert not s.identity_map

        user = s.query(User).one()
        assert user.name == 'fred'
        assert s.identity_map

    @testing.resolve_artifact_names
    def test_strong_ref(self):
        s = create_session(weak_identity_map=False)
        mapper(User, users)

        # save user
        s.save(User(name='u1'))
        s.flush()
        user = s.query(User).one()
        user = None
        print s.identity_map
        import gc
        gc.collect()
        assert len(s.identity_map) == 1

    @testing.resolve_artifact_names
    def test_prune(self):
        s = create_session(weak_identity_map=False)
        mapper(User, users)

        for o in [User(name='u%s' % x) for x in xrange(10)]:
            s.save(o)
        # o is still live after this loop...

        self.assert_(len(s.identity_map) == 0)
        self.assert_(s.prune() == 0)
        s.flush()
        import gc
        gc.collect()
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

        s.save(User(name='x'))
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

    @testing.resolve_artifact_names
    def test_no_save_cascade_1(self):
        mapper(Address, addresses)
        mapper(User, users, properties=dict(
            addresses=relation(Address, cascade="none", backref="user")))
        s = create_session()

        u = User(name='u1')
        s.save(u)
        a = Address(email_address='u1@e')
        u.addresses.append(a)
        assert u in s
        assert a not in s
        s.flush()
        print "\n".join([repr(x.__dict__) for x in s])
        s.clear()
        assert s.query(User).one().id == u.id
        assert s.query(Address).first() is None

    @testing.resolve_artifact_names
    def test_no_save_cascade_2(self):
        mapper(Address, addresses)
        mapper(User, users, properties=dict(
            addresses=relation(Address,
                               cascade="all",
                               backref=backref("user", cascade="none"))))

        s = create_session()
        u = User(name='u1')
        a = Address(email_address='u1@e')
        a.user = u
        s.save(a)
        assert u not in s
        assert a in s
        s.flush()
        s.clear()
        assert s.query(Address).one().id == a.id
        assert s.query(User).first() is None

    @testing.resolve_artifact_names
    def test_identity_key_1(self):
        mapper(User, users)
        mapper(User, users, entity_name="en")
        s = create_session()
        key = s.identity_key(User, 1)
        eq_(key, (User, (1,), None))
        key = s.identity_key(User, 1, "en")
        eq_(key, (User, (1,), "en"))
        key = s.identity_key(User, 1, entity_name="en")
        eq_(key, (User, (1,), "en"))
        key = s.identity_key(User, ident=1, entity_name="en")
        eq_(key, (User, (1,), "en"))

    @testing.resolve_artifact_names
    def test_identity_key_2(self):
        mapper(User, users)
        s = create_session()
        u = User(name='u1')
        s.save(u)
        s.flush()
        key = s.identity_key(instance=u)
        eq_(key, (User, (u.id,), None))

    @testing.resolve_artifact_names
    def test_identity_key_3(self):
        mapper(User, users)
        mapper(User, users, entity_name="en")
        s = create_session()
        row = {users.c.id: 1, users.c.name: "Frank"}
        key = s.identity_key(User, row=row)
        eq_(key, (User, (1,), None))
        key = s.identity_key(User, row=row, entity_name="en")
        eq_(key, (User, (1,), "en"))

    @testing.resolve_artifact_names
    def test_extension(self):
        mapper(User, users)
        log = []
        class MyExt(sa.orm.session.SessionExtension):
            def before_commit(self, session):
                log.append('before_commit')
            def after_commit(self, session):
                log.append('after_commit')
            def after_rollback(self, session):
                log.append('after_rollback')
            def before_flush(self, session, flush_context, objects):
                log.append('before_flush')
            def after_flush(self, session, flush_context):
                log.append('after_flush')
            def after_flush_postexec(self, session, flush_context):
                log.append('after_flush_postexec')
            def after_begin(self, session, transaction, connection):
                log.append('after_begin')
        sess = create_session(extension = MyExt())
        u = User(name='u1')
        sess.save(u)
        sess.flush()
        assert log == ['before_flush', 'after_begin', 'after_flush', 'before_commit', 'after_commit', 'after_flush_postexec']

        log = []
        sess = create_session(autocommit=False, extension=MyExt())
        u = User(name='u1')
        sess.save(u)
        sess.flush()
        assert log == ['before_flush', 'after_begin', 'after_flush', 'after_flush_postexec']

        log = []
        u.name = 'ed'
        sess.commit()
        assert log == ['before_commit', 'before_flush', 'after_flush', 'after_flush_postexec', 'after_commit']

        log = []
        sess.commit()
        assert log == ['before_commit', 'after_commit']
        
        log = []
        sess = create_session(autocommit=False, extension=MyExt(), bind=testing.db)
        conn = sess.connection()
        assert log == ['after_begin']

    @testing.resolve_artifact_names
    def test_pickled_update(self):
        mapper(User, users)
        sess1 = create_session()
        sess2 = create_session()

        u1 = User(name='u1')
        sess1.save(u1)

        self.assertRaisesMessage(sa.exc.InvalidRequestError, "already attached to session", sess2.save, u1)

        u2 = pickle.loads(pickle.dumps(u1))

        sess2.save(u2)

    @testing.resolve_artifact_names
    def test_duplicate_update(self):
        mapper(User, users)
        Session = sessionmaker()
        sess = Session()

        u1 = User(name='u1')
        sess.save(u1)
        sess.flush()
        assert u1.id is not None

        sess.expunge(u1)

        assert u1 not in sess
        assert Session.object_session(u1) is None

        u2 = sess.query(User).get(u1.id)
        assert u2 is not None and u2 is not u1
        assert u2 in sess

        self.assertRaises(Exception, lambda: sess.update(u1))

        sess.expunge(u2)
        assert u2 not in sess
        assert Session.object_session(u2) is None

        u1.name = "John"
        u2.name = "Doe"

        sess.update(u1)
        assert u1 in sess
        assert Session.object_session(u1) is sess

        sess.flush()

        sess.clear()

        u3 = sess.query(User).get(u1.id)
        assert u3 is not u1 and u3 is not u2 and u3.name == u1.name

    @testing.resolve_artifact_names
    def test_no_double_save(self):
        sess = create_session()
        class Foo(object):
            def __init__(self):
                sess.save(self)
        class Bar(Foo):
            def __init__(self):
                sess.save(self)
                Foo.__init__(self)
        mapper(Foo, users)
        mapper(Bar, users)

        b = Bar()
        assert b in sess
        assert len(list(sess)) == 1


class TLTransactionTest(engine_base.AltEngineTest, _base.MappedTest):
    def create_engine(self):
        return engines.testing_engine(options=dict(strategy='threadlocal'))

    def define_tables(self, metadata):
        Table('users', metadata,
              Column('id', Integer, primary_key=True),
              Column('name', String(20)),
              test_needs_acid=True)

    def setup_classes(self):
        class User(_base.BasicEntity):
            pass

    @testing.resolve_artifact_names
    def setup_mappers(self):
        mapper(User, users)

    def setUpAll(self):
        engine_base.AltEngineTest.setUpAll(self)
        _base.MappedTest.setUpAll(self)


    def tearDownAll(self):
        _base.MappedTest.tearDownAll(self)
        engine_base.AltEngineTest.tearDownAll(self)

    @testing.exclude('mysql', '<', (5, 0, 3), 'FIXME: unknown')
    @testing.resolve_artifact_names
    def test_session_nesting(self):
        sess = create_session(bind=self.engine)
        self.engine.begin()
        u = User(name='ed')
        sess.save(u)
        sess.flush()
        self.engine.commit()


if __name__ == "__main__":
    testenv.main()
