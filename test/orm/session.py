import testenv; testenv.configure_for_tests()
import gc
import pickle
from sqlalchemy import *
from sqlalchemy import exc as sa_exc, util
from sqlalchemy.orm import *
from sqlalchemy.orm import attributes
from sqlalchemy.orm.session import SessionExtension
from sqlalchemy.orm.session import Session as SessionCls
from testlib import *
from testlib.tables import *
from testlib import fixtures, tables


class SessionTest(TestBase, AssertsExecutionResults):
    def setUpAll(self):
        tables.create()

    def tearDownAll(self):
        tables.drop()

    def tearDown(self):
        SessionCls.close_all()
        tables.delete()
        clear_mappers()

    def setUp(self):
        pass

    def test_close(self):
        """test that flush() doesn't close a connection the session didn't open"""

        c = testing.db.connect()
        class User(object):pass
        mapper(User, users)
        s = create_session(bind=c)
        s.save(User())
        s.flush()
        c.execute("select * from users")
        u = User()
        s.save(u)
        s.user_name = 'some user'
        s.flush()
        u = User()
        s.save(u)
        s.user_name = 'some other user'
        s.flush()

    def test_close_two(self):
        c = testing.db.connect()
        try:
            class User(object):pass
            mapper(User, users)
            s = create_session(bind=c)
            s.begin()
            tran = s.transaction
            s.save(User())
            s.flush()
            c.execute("select * from users")
            u = User()
            s.save(u)
            s.user_name = 'some user'
            s.flush()
            u = User()
            s.save(u)
            s.user_name = 'some other user'
            s.flush()
            assert s.transaction is tran
            tran.close()
        finally:
            c.close()

    def test_expunge_cascade(self):
        tables.data()
        mapper(Address, addresses)
        mapper(User, users, properties={
            'addresses':relation(Address, backref=backref("user", cascade="all"), cascade="all")
        })
        session = create_session()
        u = session.query(User).filter_by(user_id=7).one()

        # get everything to load in both directions
        print [a.user for a in u.addresses]

        # then see if expunge fails
        session.expunge(u)

        assert object_session(u) is attributes.instance_state(u).session_id is None
        for a in u.addresses:
            assert object_session(a) is attributes.instance_state(a).session_id is None

    @engines.close_open_connections
    def test_binds_from_expression(self):
        """test that Session can extract Table objects from ClauseElements and match them to tables."""

        Session = sessionmaker(binds={users:testing.db, addresses:testing.db})
        sess = Session()
        sess.execute(users.insert(), params=dict(user_id=1, user_name='ed'))
        assert sess.execute(users.select()).fetchall() == [(1, 'ed')]

        mapper(Address, addresses)
        mapper(User, users, properties={
            'addresses':relation(Address, backref=backref("user", cascade="all"), cascade="all")
        })
        Session = sessionmaker(binds={User:testing.db, Address:testing.db})
        sess.execute(users.insert(), params=dict(user_id=2, user_name='fred'))
        assert sess.execute(users.select()).fetchall() == [(1, 'ed'), (2, 'fred')]
        sess.close()

    @engines.close_open_connections
    def test_bind_from_metadata(self):
        Session = sessionmaker()
        sess = Session()
        mapper(User, users)

        sess.execute(users.insert(), dict(user_name='Johnny'))

        assert len(sess.query(User).all()) == 1

        sess.execute(users.delete())

        assert len(sess.query(User).all()) == 0
        sess.close()

    @testing.unsupported('sqlite', 'mssql') # TEMP: test causes mssql to hang
    @engines.close_open_connections
    def test_transaction(self):
        class User(object):pass
        mapper(User, users)
        conn1 = testing.db.connect()
        conn2 = testing.db.connect()

        sess = create_session(autocommit=False, bind=conn1)
        u = User()
        sess.save(u)
        sess.flush()
        assert conn1.execute("select count(1) from users").scalar() == 1
        assert conn2.execute("select count(1) from users").scalar() == 0
        sess.commit()
        assert conn1.execute("select count(1) from users").scalar() == 1
        assert testing.db.connect().execute("select count(1) from users").scalar() == 1
        sess.close()

    @testing.unsupported('sqlite', 'mssql') # TEMP: test causes mssql to hang
    @engines.close_open_connections
    def test_autoflush(self):
        class User(object):pass
        mapper(User, users)
        conn1 = testing.db.connect()
        conn2 = testing.db.connect()

        sess = create_session(bind=conn1, autocommit=False, autoflush=True)
        u = User()
        u.user_name='ed'
        sess.save(u)
        u2 = sess.query(User).filter_by(user_name='ed').one()
        assert u2 is u
        assert conn1.execute("select count(1) from users").scalar() == 1
        assert conn2.execute("select count(1) from users").scalar() == 0
        sess.commit()
        assert conn1.execute("select count(1) from users").scalar() == 1
        assert testing.db.connect().execute("select count(1) from users").scalar() == 1
        sess.close()

    def test_autoflush_expressions(self):
        class User(fixtures.Base):
            pass
        class Address(fixtures.Base):
            pass
        mapper(User, users, properties={
            'addresses':relation(Address, backref="user")
        })
        mapper(Address, addresses)

        sess = create_session(autoflush=True, autocommit=False)
        u = User(user_name='ed', addresses=[Address(email_address='foo')])
        sess.save(u)
        self.assertEquals(sess.query(Address).filter(Address.user==u).one(), Address(email_address='foo'))

    @testing.unsupported('sqlite', 'mssql') # TEMP: test causes mssql to hang
    @engines.close_open_connections
    def test_autoflush_unbound(self):
        class User(object):pass
        mapper(User, users)

        try:
            sess = create_session(autocommit=False, autoflush=True)
            u = User()
            u.user_name='ed'
            sess.save(u)
            u2 = sess.query(User).filter_by(user_name='ed').one()
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
    def test_autoflush_2(self):
        class User(object):pass
        mapper(User, users)
        conn1 = testing.db.connect()
        conn2 = testing.db.connect()

        sess = create_session(bind=conn1, autocommit=False, autoflush=True)
        u = User()
        u.user_name='ed'
        sess.save(u)
        sess.commit()
        assert conn1.execute("select count(1) from users").scalar() == 1
        assert testing.db.connect().execute("select count(1) from users").scalar() == 1
        sess.commit()

    def test_autoflush_rollback(self):
        tables.data()
        mapper(Address, addresses)
        mapper(User, users, properties={
            'addresses':relation(Address)
        })

        sess = create_session(autocommit=False, autoflush=True)
        u = sess.query(User).get(8)
        newad = Address()
        newad.email_address = 'something new'
        u.addresses.append(newad)
        u.user_name = 'some new name'
        assert u.user_name == 'some new name'
        assert len(u.addresses) == 4
        assert newad in u.addresses
        sess.rollback()
        assert u.user_name == 'ed'
        assert len(u.addresses) == 3
        assert newad not in u.addresses
        
        # pending objects dont get expired
        assert newad.email_address == 'something new'
    
    def test_textual_execute(self):
        """test that Session.execute() converts to text()"""
        
        tables.data()
        sess = create_session(bind=testing.db)
        # use :bindparam style
        self.assertEquals(sess.execute("select * from users where user_id=:id", {'id':7}).fetchall(), [(7, u'jack')])

    @engines.close_open_connections
    def test_subtransaction_on_external(self):
        class User(object):pass
        mapper(User, users)
        conn = testing.db.connect()
        trans = conn.begin()
        sess = create_session(bind=conn, autocommit=False, autoflush=True)
        sess.begin(subtransactions=True)
        u = User()
        sess.save(u)
        sess.flush()
        sess.commit() # commit does nothing
        trans.rollback() # rolls back
        assert len(sess.query(User).all()) == 0
        sess.close()

    @testing.unsupported('sqlite', 'mssql', 'firebird', 'sybase', 'access',
                         'oracle', 'maxdb')
    @engines.close_open_connections
    def test_external_nested_transaction(self):
        class User(object):pass
        mapper(User, users)
        try:
            conn = testing.db.connect()
            trans = conn.begin()
            sess = create_session(bind=conn, autocommit=False, autoflush=True)
            u1 = User()
            sess.save(u1)
            sess.flush()

            sess.begin_nested()
            u2 = User()
            sess.save(u2)
            sess.flush()
            sess.rollback()

            trans.commit()
            assert len(sess.query(User).all()) == 1
        except:
            conn.close()
            raise

    @testing.requires.savepoints
    def test_heavy_nesting(self):
        session = create_session(bind=testing.db)

        session.begin()
        session.connection().execute("insert into users (user_name) values ('user1')")

        session.begin(subtransactions=True)

        session.begin_nested()

        session.connection().execute("insert into users (user_name) values ('user2')")
        assert session.connection().execute("select count(1) from users").scalar() == 2

        session.rollback()
        assert session.connection().execute("select count(1) from users").scalar() == 1
        session.connection().execute("insert into users (user_name) values ('user3')")

        session.commit()
        assert session.connection().execute("select count(1) from users").scalar() == 2


    @testing.requires.two_phase_transactions
    def test_twophase(self):
        # TODO: mock up a failure condition here
        # to ensure a rollback succeeds
        class User(object):pass
        class Address(object):pass
        mapper(User, users)
        mapper(Address, addresses)

        engine2 = create_engine(testing.db.url)
        sess = create_session(autocommit=True, autoflush=False, twophase=True)
        sess.bind_mapper(User, testing.db)
        sess.bind_mapper(Address, engine2)
        sess.begin()
        u1 = User()
        a1 = Address()
        sess.save(u1)
        sess.save(a1)
        sess.commit()
        sess.close()
        engine2.dispose()
        assert users.count().scalar() == 1
        assert addresses.count().scalar() == 1

    def test_subtransaction_on_noautocommit(self):
        class User(object):pass
        mapper(User, users)
        sess = create_session(autocommit=False, autoflush=True)
        sess.begin(subtransactions=True)
        u = User()
        sess.save(u)
        sess.flush()
        sess.commit() # commit does nothing
        sess.rollback() # rolls back
        assert len(sess.query(User).all()) == 0
        sess.close()

    @testing.requires.savepoints
    def test_nested_transaction(self):
        class User(object):pass
        mapper(User, users)
        sess = create_session()
        sess.begin()

        u = User()
        sess.save(u)
        sess.flush()

        sess.begin_nested()  # nested transaction

        u2 = User()
        sess.save(u2)
        sess.flush()

        sess.rollback()

        sess.commit()
        assert len(sess.query(User).all()) == 1
        sess.close()

    @testing.requires.savepoints
    def test_nested_autotrans(self):
        class User(object):pass
        mapper(User, users)
        sess = create_session(autocommit=False)
        u = User()
        sess.save(u)
        sess.flush()

        sess.begin_nested()  # nested transaction

        u2 = User()
        sess.save(u2)
        sess.flush()

        sess.rollback()

        sess.commit()
        assert len(sess.query(User).all()) == 1
        sess.close()

    @testing.requires.savepoints
    def test_nested_transaction_connection_add(self):
        class User(object): pass
        mapper(User, users)

        sess = create_session(autocommit=True)

        sess.begin()
        sess.begin_nested()

        u1 = User()
        sess.save(u1)
        sess.flush()

        sess.rollback()

        u2 = User()
        sess.save(u2)

        sess.commit()

        self.assertEquals(util.Set(sess.query(User).all()), util.Set([u2]))

        sess.begin()
        sess.begin_nested()

        u3 = User()
        sess.save(u3)
        sess.commit() # commit the nested transaction
        sess.rollback()

        self.assertEquals(util.Set(sess.query(User).all()), util.Set([u2]))

        sess.close()

    @testing.requires.savepoints
    def test_mixed_transaction_control(self):
        class User(object): pass
        mapper(User, users)

        sess = create_session(autocommit=True)

        sess.begin()
        sess.begin_nested()
        transaction = sess.begin(subtransactions=True)

        sess.save(User())

        transaction.commit()
        sess.commit()
        sess.commit()

        sess.close()

        self.assertEquals(len(sess.query(User).all()), 1)

        t1 = sess.begin()
        t2 = sess.begin_nested()

        sess.save(User())

        t2.commit()
        assert sess.transaction is t1

        sess.close()

    @testing.requires.savepoints
    def test_mixed_transaction_close(self):
        class User(object): pass
        mapper(User, users)

        sess = create_session(autocommit=False)

        sess.begin_nested()

        sess.save(User())
        sess.flush()

        sess.close()

        sess.save(User())
        sess.commit()

        sess.close()

        self.assertEquals(len(sess.query(User).all()), 1)

    def test_error_on_using_inactive_session(self):
        class User(object): pass
        mapper(User, users)

        sess = create_session(autocommit=True)

        sess.begin()
        sess.begin(subtransactions=True)

        sess.save(User())
        sess.flush()

        sess.rollback()
        self.assertRaisesMessage(sa_exc.InvalidRequestError, "inactive due to a rollback in a subtransaction", sess.begin, subtransactions=True)
        sess.close()

    @engines.close_open_connections
    def test_bound_connection(self):
        class User(object):pass
        mapper(User, users)
        c = testing.db.connect()
        sess = create_session(bind=c)
        sess.begin()
        transaction = sess.transaction
        u = User()
        sess.save(u)
        sess.flush()
        assert transaction._connection_for_bind(testing.db) is transaction._connection_for_bind(c) is c

        self.assertRaisesMessage(sa_exc.InvalidRequestError, "Session already has a Connection associated", transaction._connection_for_bind, testing.db.connect())

        transaction.rollback()
        assert len(sess.query(User).all()) == 0
        sess.close()

    def test_bound_connection_transactional(self):
        class User(object):pass
        mapper(User, users)
        c = testing.db.connect()

        sess = create_session(bind=c, autocommit=False)
        u = User()
        sess.save(u)
        sess.flush()
        sess.close()
        assert not c.in_transaction()
        assert c.scalar("select count(1) from users") == 0

        sess = create_session(bind=c, autocommit=False)
        u = User()
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
        u = User()
        sess.save(u)
        sess.flush()
        assert c.in_transaction()
        trans.commit()
        assert not c.in_transaction()
        assert c.scalar("select count(1) from users") == 1


    @engines.close_open_connections
    def test_save_update_delete(self):

        s = create_session()
        class User(object):
            pass
        mapper(User, users)

        user = User()

        self.assertRaisesMessage(sa_exc.InvalidRequestError, "is not persisted", s.update, user)
        self.assertRaisesMessage(sa_exc.InvalidRequestError, "is not persisted", s.delete, user)

        s.save(user)
        s.flush()
        user = s.query(User).one()
        s.expunge(user)
        assert user not in s

        # modify outside of session, assert changes remain/get saved
        user.user_name = "fred"
        s.update(user)
        assert user in s
        assert user in s.dirty
        s.flush()
        s.clear()
        assert s.query(User).count() == 1
        user = s.query(User).one()
        assert user.user_name == 'fred'

        # ensure its not dirty if no changes occur
        s.clear()
        assert user not in s
        s.update(user)
        assert user in s
        assert user not in s.dirty

        self.assertRaisesMessage(sa_exc.InvalidRequestError, "is already persistent", s.save, user)

        s2 = create_session()
        self.assertRaisesMessage(sa_exc.InvalidRequestError, "is already attached to session", s2.delete, user)

        u2 = s2.query(User).get(user.user_id)
        self.assertRaisesMessage(sa_exc.InvalidRequestError, "already persisted with a different identity", s.delete, u2)

        s.delete(user)
        s.flush()
        assert user not in s
        assert s.query(User).count() == 0

    def test_is_modified(self):
        s = create_session()
        class User(object):pass
        class Address(object):pass

        mapper(User, users, properties={'addresses':relation(Address)})
        mapper(Address, addresses)

        # save user
        u = User()
        u.user_name = 'fred'
        s.save(u)
        s.flush()
        s.clear()

        user = s.query(User).one()
        assert user not in s.dirty
        assert not s.is_modified(user)
        user.user_name = 'fred'
        assert user in s.dirty
        assert not s.is_modified(user)
        user.user_name = 'ed'
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


    def test_weak_ref(self):
        """test the weak-referencing identity map, which strongly-references modified items."""

        s = create_session()
        class User(fixtures.Base):pass
        mapper(User, users)

        s.save(User(user_name='ed'))
        s.flush()
        assert not s.dirty

        user = s.query(User).one()
        del user
        gc.collect()
        assert len(s.identity_map) == 0

        user = s.query(User).one()
        user.user_name = 'fred'
        del user
        gc.collect()
        assert len(s.identity_map) == 1
        assert len(s.dirty) == 1

        s.flush()
        gc.collect()
        assert not s.dirty
        assert not s.identity_map

        user = s.query(User).one()
        assert user.user_name == 'fred'
        assert s.identity_map

    def test_strong_ref(self):
        s = create_session(weak_identity_map=False)
        class User(object):pass
        mapper(User, users)

        # save user
        s.save(User())
        s.flush()
        user = s.query(User).one()
        user = None
        print s.identity_map
        import gc
        gc.collect()
        assert len(s.identity_map) == 1

    def test_prune(self):
        s = create_session(weak_identity_map=False)
        class User(object):pass
        mapper(User, users)

        for o in [User() for x in xrange(10)]:
            s.save(o)
        # o is still live after this loop...

        self.assert_(len(s.identity_map) == 0)
        self.assert_(s.prune() == 0)
        s.flush()
        import gc
        gc.collect()
        self.assert_(s.prune() == 9)
        self.assert_(len(s.identity_map) == 1)

        user_id = o.user_id
        del o
        self.assert_(s.prune() == 1)
        self.assert_(len(s.identity_map) == 0)

        u = s.query(User).get(user_id)
        self.assert_(s.prune() == 0)
        self.assert_(len(s.identity_map) == 1)
        u.user_name = 'squiznart'
        del u
        self.assert_(s.prune() == 0)
        self.assert_(len(s.identity_map) == 1)
        s.flush()
        self.assert_(s.prune() == 1)
        self.assert_(len(s.identity_map) == 0)

        s.save(User())
        self.assert_(s.prune() == 0)
        self.assert_(len(s.identity_map) == 0)
        s.flush()
        self.assert_(len(s.identity_map) == 1)
        self.assert_(s.prune() == 1)
        self.assert_(len(s.identity_map) == 0)

        u = s.query(User).get(user_id)
        s.delete(u)
        del u
        self.assert_(s.prune() == 0)
        self.assert_(len(s.identity_map) == 1)
        s.flush()
        self.assert_(s.prune() == 0)
        self.assert_(len(s.identity_map) == 0)

    def test_no_save_cascade(self):
        mapper(Address, addresses)
        mapper(User, users, properties=dict(
            addresses=relation(Address, cascade="none", backref="user")
        ))
        s = create_session()
        u = User()
        s.save(u)
        a = Address()
        u.addresses.append(a)
        assert u in s
        assert a not in s
        s.flush()
        print "\n".join([repr(x.__dict__) for x in s])
        s.clear()
        assert s.query(User).one().user_id == u.user_id
        assert s.query(Address).first() is None

        clear_mappers()

        tables.delete()
        mapper(Address, addresses)
        mapper(User, users, properties=dict(
            addresses=relation(Address, cascade="all", backref=backref("user", cascade="none"))
        ))

        s = create_session()
        u = User()
        a = Address()
        a.user = u
        s.save(a)
        assert u not in s
        assert a in s
        s.flush()
        s.clear()
        assert s.query(Address).one().address_id == a.address_id
        assert s.query(User).first() is None

    def _assert_key(self, got, expect):
        assert got == expect, "expected %r got %r" % (expect, got)

    def test_identity_key_1(self):
        mapper(User, users)
        mapper(User, users, entity_name="en")
        s = create_session()
        key = s.identity_key(User, 1)
        self._assert_key(key, (User, (1,), None))
        key = s.identity_key(User, 1, "en")
        self._assert_key(key, (User, (1,), "en"))
        key = s.identity_key(User, 1, entity_name="en")
        self._assert_key(key, (User, (1,), "en"))
        key = s.identity_key(User, ident=1, entity_name="en")
        self._assert_key(key, (User, (1,), "en"))

    def test_identity_key_2(self):
        mapper(User, users)
        s = create_session()
        u = User()
        s.save(u)
        s.flush()
        key = s.identity_key(instance=u)
        self._assert_key(key, (User, (u.user_id,), None))

    def test_identity_key_3(self):
        mapper(User, users)
        mapper(User, users, entity_name="en")
        s = create_session()
        row = {users.c.user_id: 1, users.c.user_name: "Frank"}
        key = s.identity_key(User, row=row)
        self._assert_key(key, (User, (1,), None))
        key = s.identity_key(User, row=row, entity_name="en")
        self._assert_key(key, (User, (1,), "en"))

    def test_extension(self):
        mapper(User, users)
        log = []
        class MyExt(SessionExtension):
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
        u = User()
        sess.save(u)
        sess.flush()
        assert log == ['before_flush', 'after_begin', 'after_flush', 'before_commit', 'after_commit', 'after_flush_postexec']

        log = []
        sess = create_session(autocommit=False, extension=MyExt())
        u = User()
        sess.save(u)
        sess.flush()
        assert log == ['before_flush', 'after_begin', 'after_flush', 'after_flush_postexec']

        log = []
        u.user_name = 'ed'
        sess.commit()
        assert log == ['before_commit', 'before_flush', 'after_flush', 'after_flush_postexec', 'after_commit']

        log = []
        sess.commit()
        assert log == ['before_commit', 'after_commit']
        
        log = []
        sess = create_session(autocommit=False, extension=MyExt(), bind=testing.db)
        conn = sess.connection()
        assert log == ['after_begin']

    def test_pickled_update(self):
        mapper(User, users)
        sess1 = create_session()
        sess2 = create_session()

        u1 = User()
        sess1.save(u1)

        self.assertRaisesMessage(sa_exc.InvalidRequestError, "already attached to session", sess2.save, u1)

        u2 = pickle.loads(pickle.dumps(u1))

        sess2.save(u2)

    def test_duplicate_update(self):
        mapper(User, users)
        Session = sessionmaker()
        sess = Session()

        u1 = User()
        sess.save(u1)
        sess.flush()
        assert u1.user_id is not None

        sess.expunge(u1)

        assert u1 not in sess
        assert Session.object_session(u1) is None

        u2 = sess.query(User).get(u1.user_id)
        assert u2 is not None and u2 is not u1
        assert u2 in sess

        self.assertRaises(Exception, lambda: sess.update(u1))

        sess.expunge(u2)
        assert u2 not in sess
        assert Session.object_session(u2) is None

        u1.user_name = "John"
        u2.user_name = "Doe"

        sess.update(u1)
        assert u1 in sess
        assert Session.object_session(u1) is sess

        sess.flush()

        sess.clear()

        u3 = sess.query(User).get(u1.user_id)
        assert u3 is not u1 and u3 is not u2 and u3.user_name == u1.user_name

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


class TLTransactionTest(TestBase):
    def setUpAll(self):
        global users, metadata, tlengine
        tlengine = create_engine(testing.db.url, strategy='threadlocal')
        metadata = MetaData()
        users = Table('query_users', metadata,
            Column('user_id', INT, Sequence('query_users_id_seq', optional=True), primary_key=True),
            Column('user_name', VARCHAR(20)),
            test_needs_acid=True,
        )
        users.create(tlengine)
    def tearDown(self):
        tlengine.execute(users.delete())

    def tearDownAll(self):
        users.drop(tlengine)
        tlengine.dispose()

    @testing.exclude('mysql', '<', (5, 0, 3))
    def testsessionnesting(self):
        class User(object):
            pass
        try:
            mapper(User, users)

            sess = create_session(bind=tlengine)
            tlengine.begin()
            u = User()
            sess.save(u)
            sess.flush()
            tlengine.commit()
        finally:
            clear_mappers()


if __name__ == "__main__":
    testenv.main()
