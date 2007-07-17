from testbase import AssertMixin
import testbase
import unittest, sys, datetime

import tables
from tables import *

db = testbase.db
from sqlalchemy import *
from sqlalchemy.orm import *


class SessionTest(AssertMixin):
    def setUpAll(self):
        tables.create()
    def tearDownAll(self):
        tables.drop()
    def tearDown(self):
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
    
    @testbase.unsupported('sqlite')    
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
    
    def test_autoflush(self):
        class User(object):pass
        mapper(User, users)
        conn1 = testbase.db.connect()
        conn2 = testbase.db.connect()
        
        sess = create_session(autoflush=True, bind=conn1)
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

    def test_autoflush_2(self):
        class User(object):pass
        mapper(User, users)
        conn1 = testbase.db.connect()
        conn2 = testbase.db.connect()
        
        sess = create_session(autoflush=True, bind=conn1)
        u = User()
        u.user_name='ed'
        sess.save(u)
        sess.commit()
        assert conn1.execute("select count(1) from users").scalar() == 1
        assert testbase.db.connect().execute("select count(1) from users").scalar() == 1
        
    def test_external_joined_transaction(self):
        class User(object):pass
        mapper(User, users)
        conn = testbase.db.connect()
        trans = conn.begin()
        sess = create_session(bind=conn)
        sess.begin() 
        u = User()
        sess.save(u)
        sess.flush()
        sess.commit() # commit does nothing
        trans.rollback() # rolls back
        assert len(sess.query(User).select()) == 0

    @testbase.supported('postgres', 'mysql')
    def test_external_nested_transaction(self):
        class User(object):pass
        mapper(User, users)
        try:
            conn = testbase.db.connect()
            trans = conn.begin()
            sess = create_session(bind=conn)
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
            
    def test_joined_transaction(self):
        class User(object):pass
        mapper(User, users)
        sess = create_session()
        sess.begin()
        sess.begin()  
        u = User()
        sess.save(u)
        sess.flush()
        sess.commit() # commit does nothing
        sess.rollback() # rolls back
        assert len(sess.query(User).select()) == 0

    @testbase.supported('postgres', 'mysql')
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

    @testbase.supported('postgres', 'mysql')
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
        
        
if __name__ == "__main__":    
    testbase.main()
