import testenv; testenv.configure_for_tests()
import gc
import inspect
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
        s.add(User(name='first'))
        s.flush()
        c.execute("select * from users")

    @testing.resolve_artifact_names
    def test_close(self):
        """close() doesn't close a connection the session didn't open"""
        c = testing.db.connect()
        c.execute("select * from users")

        mapper(User, users)
        s = create_session(bind=c)
        s.add(User(name='first'))
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

    @testing.crashes('mssql', 'test causes mssql to hang')
    @testing.requires.independent_connections
    @engines.close_open_connections
    @testing.resolve_artifact_names
    def test_transaction(self):
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
        assert testing.db.connect().execute("select count(1) from users").scalar() == 1
        sess.close()

    @testing.crashes('mssql', 'test causes mssql to hang')
    @testing.requires.independent_connections
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
        sess.add(u)
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
        sess.add(u)
        eq_(sess.query(Address).filter(Address.user==u).one(),
            Address(email_address='foo'))

    @testing.crashes('mssql', 'test causes mssql to hang')
    @testing.requires.independent_connections
    @engines.close_open_connections
    @testing.resolve_artifact_names
    def test_autoflush_unbound(self):
        mapper(User, users)

        try:
            sess = create_session(autocommit=False, autoflush=True)
            u = User()
            u.name='ed'
            sess.add(u)
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
        sess.add(u)
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
    def test_autocommit_doesnt_raise_on_pending(self):
        mapper(User, users)
        session = create_session(autocommit=True)

        session.add(User(name='ed'))

        session.begin()
        session.flush()
        session.commit()
        
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
        sess.add(u)
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

    @testing.fails_on('sqlite')
    @testing.resolve_artifact_names
    def test_transactions_isolated(self):
        mapper(User, users)
        users.delete().execute()
        
        s1 = create_session(bind=testing.db, autocommit=False)
        s2 = create_session(bind=testing.db, autocommit=False)
        u1 = User(name='u1')
        s1.add(u1)
        s1.flush()
        
        assert s2.query(User).all() == []
        
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
        sess.add(u)
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
    @testing.resolve_artifact_names
    def test_nested_autotrans(self):
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
    @testing.resolve_artifact_names
    def test_nested_transaction_connection_add(self):
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

        self.assertEquals(set(sess.query(User).all()), set([u2]))

        sess.begin()
        sess.begin_nested()

        u3 = User(name='u3')
        sess.add(u3)
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

        sess.add(User(name='u1'))

        transaction.commit()
        sess.commit()
        sess.commit()

        sess.close()

        self.assertEquals(len(sess.query(User).all()), 1)

        t1 = sess.begin()
        t2 = sess.begin_nested()

        sess.add(User(name='u2'))

        t2.commit()
        assert sess.transaction is t1

        sess.close()

    @testing.requires.savepoints
    @testing.resolve_artifact_names
    def test_mixed_transaction_close(self):
        mapper(User, users)

        sess = create_session(autocommit=False)

        sess.begin_nested()

        sess.add(User(name='u1'))
        sess.flush()

        sess.close()

        sess.add(User(name='u2'))
        sess.commit()

        sess.close()

        self.assertEquals(len(sess.query(User).all()), 1)

    @testing.resolve_artifact_names
    def test_error_on_using_inactive_session(self):
        mapper(User, users)

        sess = create_session(autocommit=True)

        sess.begin()
        sess.begin(subtransactions=True)

        sess.add(User(name='u1'))
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
        sess.add(u)
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
        sess.add(u)
        sess.flush()
        sess.close()
        assert not c.in_transaction()
        assert c.scalar("select count(1) from users") == 0

        sess = create_session(bind=c, autocommit=False)
        u = User(name='u2')
        sess.add(u)
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
        sess.add(u)
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

        s.add(user)
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
        s.add(u)
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

        s.add(User(name='ed'))
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
        s.add(User(name='u1'))
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
            s.add(o)
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

    @testing.resolve_artifact_names
    def test_no_save_cascade_1(self):
        mapper(Address, addresses)
        mapper(User, users, properties=dict(
            addresses=relation(Address, cascade="none", backref="user")))
        s = create_session()

        u = User(name='u1')
        s.add(u)
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
        s.add(a)
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
        s.add(u)
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
        sess.add(u)
        sess.flush()
        assert log == ['before_flush', 'after_begin', 'after_flush', 'before_commit', 'after_commit', 'after_flush_postexec']

        log = []
        sess = create_session(autocommit=False, extension=MyExt())
        u = User(name='u1')
        sess.add(u)
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
        sess1.add(u1)

        self.assertRaisesMessage(sa.exc.InvalidRequestError, "already attached to session", sess2.add, u1)

        u2 = pickle.loads(pickle.dumps(u1))

        sess2.add(u2)

    @testing.resolve_artifact_names
    def test_duplicate_update(self):
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


class SessionInterface(testing.TestBase):
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
            spec = inspect.getargspec(getattr(Session, meth))
            if len(spec[0]) > 1 or spec[1]:
                ok.add(meth)
        return ok

    def _map_it(self, cls):
        return mapper(cls, Table('t', sa.MetaData(),
                                 Column('id', Integer, primary_key=True)))

    def _test_instance_guards(self, user_arg):
        watchdog = set()

        def x_raises_(obj, method, *args, **kw):
            watchdog.add(method)
            callable_ = getattr(obj, method)
            self.assertRaises(sa.orm.exc.UnmappedInstanceError,
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

        raises_('save', user_arg)

        raises_('save_or_update', user_arg)

        raises_('update', user_arg)

        instance_methods = self._public_session_methods() - self._class_methods

        eq_(watchdog, instance_methods,
            watchdog.symmetric_difference(instance_methods))

    def _test_class_guards(self, user_arg):
        watchdog = set()

        def raises_(method, *args, **kw):
            watchdog.add(method)
            callable_ = getattr(create_session(), method)
            self.assertRaises(sa.orm.exc.UnmappedClassError,
                              callable_, *args, **kw)

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
            self._test_class_guards(prim)

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
        self._test_class_guards(early)


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
        sess.add(u)
        sess.flush()
        self.engine.commit()


if __name__ == "__main__":
    testenv.main()
