import testbase
from sqlalchemy import *
from sqlalchemy import exceptions
from sqlalchemy.orm import *
from sqlalchemy.orm.session import Session as SessionCls
from testlib import *
from testlib.tables import *
from testlib import fixtures, tables

class SessionTest(AssertMixin):
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
        """test that flush() doenst close a connection the session didnt open"""
        c = testbase.db.connect()
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
        c = testbase.db.connect()
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
    
    @engines.close_open_connections
    def test_binds_from_expression(self):
        """test that Session can extract Table objects from ClauseElements and match them to tables."""
        Session = sessionmaker(binds={users:testbase.db, addresses:testbase.db})
        sess = Session()
        sess.execute(users.insert(), params=dict(user_id=1, user_name='ed'))
        assert sess.execute(users.select()).fetchall() == [(1, 'ed')]
        
        mapper(Address, addresses)
        mapper(User, users, properties={
            'addresses':relation(Address, backref=backref("user", cascade="all"), cascade="all")
        })
        Session = sessionmaker(binds={User:testbase.db, Address:testbase.db})
        sess.execute(users.insert(), params=dict(user_id=2, user_name='fred'))
        assert sess.execute(users.select()).fetchall() == [(1, 'ed'), (2, 'fred')]
        sess.close()
        
    @testing.unsupported('sqlite', 'mssql') # TEMP: test causes mssql to hang
    @engines.close_open_connections
    def test_transaction(self):
        class User(object):pass
        mapper(User, users)
        conn1 = testbase.db.connect()
        conn2 = testbase.db.connect()
        
        sess = create_session(transactional=True, bind=conn1)
        u = User()
        sess.save(u)
        sess.flush()
        assert conn1.execute("select count(1) from users").scalar() == 1
        assert conn2.execute("select count(1) from users").scalar() == 0
        sess.commit()
        assert conn1.execute("select count(1) from users").scalar() == 1
        assert testbase.db.connect().execute("select count(1) from users").scalar() == 1
        sess.close()
        
    @testing.unsupported('sqlite', 'mssql') # TEMP: test causes mssql to hang
    @engines.close_open_connections
    def test_autoflush(self):
        class User(object):pass
        mapper(User, users)
        conn1 = testbase.db.connect()
        conn2 = testbase.db.connect()
        
        sess = create_session(bind=conn1, transactional=True, autoflush=True)
        u = User()
        u.user_name='ed'
        sess.save(u)
        u2 = sess.query(User).filter_by(user_name='ed').one()
        assert u2 is u
        assert conn1.execute("select count(1) from users").scalar() == 1
        assert conn2.execute("select count(1) from users").scalar() == 0
        sess.commit()
        assert conn1.execute("select count(1) from users").scalar() == 1
        assert testbase.db.connect().execute("select count(1) from users").scalar() == 1
        sess.close()
        
    @testing.unsupported('sqlite', 'mssql') # TEMP: test causes mssql to hang
    @engines.close_open_connections
    def test_autoflush_unbound(self):
        class User(object):pass
        mapper(User, users)

        try:
            sess = create_session(transactional=True, autoflush=True)
            u = User()
            u.user_name='ed'
            sess.save(u)
            u2 = sess.query(User).filter_by(user_name='ed').one()
            assert u2 is u
            assert sess.execute("select count(1) from users", mapper=User).scalar() == 1
            assert testbase.db.connect().execute("select count(1) from users").scalar() == 0
            sess.commit()
            assert sess.execute("select count(1) from users", mapper=User).scalar() == 1
            assert testbase.db.connect().execute("select count(1) from users").scalar() == 1
            sess.close()
        except:
            sess.rollback()
            raise
            
    @engines.close_open_connections
    def test_autoflush_2(self):
        class User(object):pass
        mapper(User, users)
        conn1 = testbase.db.connect()
        conn2 = testbase.db.connect()
        
        sess = create_session(bind=conn1, transactional=True, autoflush=True)
        u = User()
        u.user_name='ed'
        sess.save(u)
        sess.commit()
        assert conn1.execute("select count(1) from users").scalar() == 1
        assert testbase.db.connect().execute("select count(1) from users").scalar() == 1
        sess.commit()

    # TODO: not doing rollback of attributes right now.
    def dont_test_autoflush_rollback(self):
        tables.data()
        mapper(Address, addresses)
        mapper(User, users, properties={
            'addresses':relation(Address)
        })
        
        sess = create_session(transactional=True, autoflush=True)
        u = sess.query(User).get(8)
        newad = Address()
        newad.email_address == 'something new'
        u.addresses.append(newad)
        u.user_name = 'some new name'
        assert u.user_name == 'some new name'
        assert len(u.addresses) == 4
        assert newad in u.addresses
        sess.rollback()
        assert u.user_name == 'ed'
        assert len(u.addresses) == 3
        assert newad not in u.addresses
        
        
    @engines.close_open_connections
    def test_external_joined_transaction(self):
        class User(object):pass
        mapper(User, users)
        conn = testbase.db.connect()
        trans = conn.begin()
        sess = create_session(bind=conn, transactional=True, autoflush=True)
        sess.begin() 
        u = User()
        sess.save(u)
        sess.flush()
        sess.commit() # commit does nothing
        trans.rollback() # rolls back
        assert len(sess.query(User).select()) == 0
        sess.close()

    @testing.supported('postgres', 'mysql')
    @engines.close_open_connections
    def test_external_nested_transaction(self):
        class User(object):pass
        mapper(User, users)
        try:
            conn = testbase.db.connect()
            trans = conn.begin()
            sess = create_session(bind=conn, transactional=True, autoflush=True)
            u1 = User()
            sess.save(u1)
            sess.flush()
        
            sess.begin_nested()  
            u2 = User()
            sess.save(u2)
            sess.flush()
            sess.rollback()
        
            trans.commit() 
            assert len(sess.query(User).select()) == 1
        except:
            conn.close()
            raise
    
    @testing.supported('postgres', 'mysql')
    @testing.exclude('mysql', '<', (5, 0, 3))
    def test_twophase(self):
        # TODO: mock up a failure condition here
        # to ensure a rollback succeeds
        class User(object):pass
        class Address(object):pass
        mapper(User, users)
        mapper(Address, addresses)
        
        engine2 = create_engine(testbase.db.url)
        sess = create_session(transactional=False, autoflush=False, twophase=True)
        sess.bind_mapper(User, testbase.db)
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
        
    def test_joined_transaction(self):
        class User(object):pass
        mapper(User, users)
        sess = create_session(transactional=True, autoflush=True)
        sess.begin()  
        u = User()
        sess.save(u)
        sess.flush()
        sess.commit() # commit does nothing
        sess.rollback() # rolls back
        assert len(sess.query(User).select()) == 0
        sess.close()

    @testing.supported('postgres', 'mysql')
    @testing.exclude('mysql', '<', (5, 0, 3))
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
        assert len(sess.query(User).select()) == 1
        sess.close()

    @testing.supported('postgres', 'mysql')
    @testing.exclude('mysql', '<', (5, 0, 3))
    def test_nested_autotrans(self):
        class User(object):pass
        mapper(User, users)
        sess = create_session(transactional=True)
        u = User()
        sess.save(u)
        sess.flush()

        sess.begin_nested()  # nested transaction

        u2 = User()
        sess.save(u2)
        sess.flush()
        
        sess.rollback() 

        sess.commit()
        assert len(sess.query(User).select()) == 1
        sess.close()

    @engines.close_open_connections
    def test_bound_connection(self):
        class User(object):pass
        mapper(User, users)
        c = testbase.db.connect()
        sess = create_session(bind=c)
        sess.create_transaction()
        transaction = sess.transaction
        u = User()
        sess.save(u)
        sess.flush()
        assert transaction.get_or_add(testbase.db) is transaction.get_or_add(c) is c
        
        try:
            transaction.add(testbase.db.connect())
            assert False
        except exceptions.InvalidRequestError, e: 
            assert str(e) == "Session already has a Connection associated for the given Connection's Engine"

        try:
            transaction.get_or_add(testbase.db.connect())
            assert False
        except exceptions.InvalidRequestError, e: 
            assert str(e) == "Session already has a Connection associated for the given Connection's Engine"

        try:
            transaction.add(testbase.db)
            assert False
        except exceptions.InvalidRequestError, e: 
            assert str(e) == "Session already has a Connection associated for the given Engine"
            
        transaction.rollback()
        assert len(sess.query(User).select()) == 0
        sess.close()

    def test_bound_connection_transactional(self):
        class User(object):pass
        mapper(User, users)
        c = testbase.db.connect()

        sess = create_session(bind=c, transactional=True)
        u = User()
        sess.save(u)
        sess.flush()
        sess.close()
        assert not c.in_transaction()
        assert c.scalar("select count(1) from users") == 0

        sess = create_session(bind=c, transactional=True)
        u = User()
        sess.save(u)
        sess.flush()
        sess.commit()
        assert not c.in_transaction()
        assert c.scalar("select count(1) from users") == 1
        c.execute("delete from users")
        assert c.scalar("select count(1) from users") == 0
        
        c = testbase.db.connect()

        trans = c.begin()
        sess = create_session(bind=c, transactional=False)
        u = User()
        sess.save(u)
        sess.flush()
        assert c.in_transaction()
        trans.commit()
        assert not c.in_transaction()
        assert c.scalar("select count(1) from users") == 1
        
        
    @engines.close_open_connections
    def test_update(self):
        """test that the update() method functions and doesnet blow away changes"""
        tables.delete()
        s = create_session()
        class User(object):pass
        mapper(User, users)
        
        # save user
        s.save(User())
        s.flush()
        user = s.query(User).selectone()
        s.expunge(user)
        assert user not in s
        
        # modify outside of session, assert changes remain/get saved
        user.user_name = "fred"
        s.update(user)
        assert user in s
        assert user in s.dirty
        s.flush()
        s.clear()
        user = s.query(User).selectone()
        assert user.user_name == 'fred'
        
        # ensure its not dirty if no changes occur
        s.clear()
        assert user not in s
        s.update(user)
        assert user in s
        assert user not in s.dirty
    
    def test_strong_ref(self):
        """test that the session is strong-referencing"""
        tables.delete()
        s = create_session()
        class User(object):pass
        mapper(User, users)
        
        # save user
        s.save(User())
        s.flush()
        user = s.query(User).selectone()
        user = None
        print s.identity_map
        import gc
        gc.collect()
        assert len(s.identity_map) == 1

    def test_prune(self):
        tables.delete()
        s = create_session()
        class User(object):pass
        mapper(User, users)

        for o in [User() for x in xrange(10)]:
            s.save(o)
        # o is still live after this loop...

        self.assert_(len(s.identity_map) == 0)
        self.assert_(s.prune() == 0)
        s.flush()
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
        s.clear()
        assert s.query(User).selectone().user_id == u.user_id
        assert s.query(Address).selectfirst() is None
        
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
        assert s.query(Address).selectone().address_id == a.address_id
        assert s.query(User).selectfirst() is None

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

class ScopedSessionTest(ORMTest):

    def define_tables(self, metadata):
        global table, table2
        table = Table('sometable', metadata, 
            Column('id', Integer, primary_key=True),
            Column('data', String(30)))
        table2 = Table('someothertable', metadata, 
            Column('id', Integer, primary_key=True),
            Column('someid', None, ForeignKey('sometable.id'))
            )
    
    def test_basic(self):
        Session = scoped_session(sessionmaker())

        class SomeObject(fixtures.Base):pass
        class SomeOtherObject(fixtures.Base):pass

        mapper(SomeObject, table, properties={
            'options':relation(SomeOtherObject)
        })
        mapper(SomeOtherObject, table2)

        s = SomeObject(id=1, data="hello")
        sso = SomeOtherObject()
        s.options.append(sso)
        Session.save(s)
        Session.commit()
        Session.remove()
        
        assert SomeObject(id=1, data="hello", options=[SomeOtherObject(someid=1)]) == Session.query(SomeObject).one()
        
        
class ScopedMapperTest(PersistTest):
    def setUpAll(self):
        global metadata, table, table2
        metadata = MetaData(testbase.db)
        table = Table('sometable', metadata, 
            Column('id', Integer, primary_key=True),
            Column('data', String(30)))
        table2 = Table('someothertable', metadata, 
            Column('id', Integer, primary_key=True),
            Column('someid', None, ForeignKey('sometable.id'))
            )
        metadata.create_all()

    def setUp(self):
        global SomeObject, SomeOtherObject
        class SomeObject(object):pass
        class SomeOtherObject(object):pass
        
        global Session
        
        Session = scoped_session(create_session)
        Session.mapper(SomeObject, table, properties={
            'options':relation(SomeOtherObject)
        })
        Session.mapper(SomeOtherObject, table2)

        s = SomeObject()
        s.id = 1
        s.data = 'hello'
        sso = SomeOtherObject()
        s.options.append(sso)
        Session.flush()
        Session.clear()

    def tearDownAll(self):
        metadata.drop_all()
        
    def tearDown(self):
        for table in metadata.table_iterator(reverse=True):
            table.delete().execute()
        clear_mappers()

    def test_query(self):
        sso = SomeOtherObject.query().first()
        assert SomeObject.query.filter_by(id=1).one().options[0].id == sso.id

    def test_query_compiles(self):
        class Foo(object):
            pass
        Session.mapper(Foo, table2)
        assert hasattr(Foo, 'query')
        
    def test_validating_constructor(self):
        s2 = SomeObject(someid=12)
        s3 = SomeOtherObject(someid=123, bogus=345)

        class ValidatedOtherObject(object):pass
        Session.mapper(ValidatedOtherObject, table2, validate=True)

        v1 = ValidatedOtherObject(someid=12)
        try:
            v2 = ValidatedOtherObject(someid=12, bogus=345)
            assert False
        except exceptions.ArgumentError:
            pass

    def test_dont_clobber_methods(self):
        class MyClass(object):
            def expunge(self):
                return "an expunge !"

        Session.mapper(MyClass, table2)

        assert MyClass().expunge() == "an expunge !"

class ScopedMapperTest2(ORMTest):
    def define_tables(self, metadata):
        global table, table2
        table = Table('sometable', metadata, 
            Column('id', Integer, primary_key=True),
            Column('data', String(30)),
            Column('type', String(30))
            
            )
        table2 = Table('someothertable', metadata, 
            Column('id', Integer, primary_key=True),
            Column('someid', None, ForeignKey('sometable.id')),
            Column('somedata', String(30)),
            )
    
    def test_inheritance(self):
        def expunge_list(l):
            for x in l:
                Session.expunge(x)
            return l
            
        class BaseClass(fixtures.Base):
            pass
        class SubClass(BaseClass):
            pass
        
        Session = scoped_session(sessionmaker())
        Session.mapper(BaseClass, table, polymorphic_identity='base', polymorphic_on=table.c.type)
        Session.mapper(SubClass, table2, polymorphic_identity='sub', inherits=BaseClass)
        
        b = BaseClass(data='b1')
        s =  SubClass(data='s1', somedata='somedata')
        Session.commit()
        Session.clear()
        
        assert expunge_list([BaseClass(data='b1'), SubClass(data='s1', somedata='somedata')]) == BaseClass.query.all()
        assert expunge_list([SubClass(data='s1', somedata='somedata')]) == SubClass.query.all()
        
        

if __name__ == "__main__":    
    testbase.main()
