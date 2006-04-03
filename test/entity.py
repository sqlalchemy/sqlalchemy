from testbase import PersistTest, AssertMixin
import unittest
from sqlalchemy import *
import testbase

from tables import *
import tables

class EntityTest(AssertMixin):
    """tests mappers that are constructed based on "entity names", which allows the same class
    to have multiple primary mappers """
    def setUpAll(self):
        global user1, user2, address1, address2
        db = testbase.db
        user1 = Table('user1', db, 
            Column('user_id', Integer, Sequence('user1_id_seq'), primary_key=True),
            Column('name', String(60), nullable=False)
            ).create()
        user2 = Table('user2', db, 
            Column('user_id', Integer, Sequence('user2_id_seq'), primary_key=True),
            Column('name', String(60), nullable=False)
            ).create()
        address1 = Table('address1', db,
            Column('address_id', Integer, Sequence('address1_id_seq'), primary_key=True),
            Column('user_id', Integer, ForeignKey(user1.c.user_id), nullable=False),
            Column('email', String(100), nullable=False)
            ).create()
        address2 = Table('address2', db,
            Column('address_id', Integer, Sequence('address2_id_seq'), primary_key=True),
            Column('user_id', Integer, ForeignKey(user2.c.user_id), nullable=False),
            Column('email', String(100), nullable=False)
            ).create()
    def tearDownAll(self):
        address1.drop()
        address2.drop()
        user1.drop()
        user2.drop()
    def tearDown(self):
        address1.delete().execute()
        address2.delete().execute()
        user1.delete().execute()
        user2.delete().execute()
        objectstore.clear()
        clear_mappers()

    def testbasic(self):
        """tests a pair of one-to-many mapper structures, establishing that both
        parent and child objects honor the "entity_name" attribute attached to the object
        instances."""
        class User(object):pass
        class Address(object):pass
            
        a1mapper = mapper(Address, address1, entity_name='address1')
        a2mapper = mapper(Address, address2, entity_name='address2')    
        u1mapper = mapper(User, user1, entity_name='user1', properties ={
            'addresses':relation(a1mapper)
        })
        u2mapper =mapper(User, user2, entity_name='user2', properties={
            'addresses':relation(a2mapper)
        })
        
        u1 = User(_sa_entity_name='user1')
        u1.name = 'this is user 1'
        a1 = Address(_sa_entity_name='address1')
        a1.email='a1@foo.com'
        u1.addresses.append(a1)
        
        u2 = User(_sa_entity_name='user2')
        u2.name='this is user 2'
        a2 = Address(_sa_entity_name='address2')
        a2.email='a2@foo.com'
        u2.addresses.append(a2)
        
        objectstore.commit()
        assert user1.select().execute().fetchall() == [(u1.user_id, u1.name)]
        assert user2.select().execute().fetchall() == [(u2.user_id, u2.name)]
        assert address1.select().execute().fetchall() == [(u1.user_id, a1.user_id, 'a1@foo.com')]
        assert address2.select().execute().fetchall() == [(u2.user_id, a2.user_id, 'a2@foo.com')]

        objectstore.clear()
        u1list = u1mapper.select()
        u2list = u2mapper.select()
        assert len(u1list) == len(u2list) == 1
        assert u1list[0] is not u2list[0]
        assert len(u1list[0].addresses) == len(u2list[0].addresses) == 1

    def testpolymorphic(self):
        """tests that entity_name can be used to have two kinds of relations on the same class."""
        class User(object):pass
        class Address1(object):pass
        class Address2(object):pass
            
        a1mapper = mapper(Address1, address1)
        a2mapper = mapper(Address2, address2)    
        u1mapper = mapper(User, user1, entity_name='user1', properties ={
            'addresses':relation(a1mapper)
        })
        u2mapper =mapper(User, user2, entity_name='user2', properties={
            'addresses':relation(a2mapper)
        })

        u1 = User(_sa_entity_name='user1')
        u1.name = 'this is user 1'
        a1 = Address1()
        a1.email='a1@foo.com'
        u1.addresses.append(a1)

        u2 = User(_sa_entity_name='user2')
        u2.name='this is user 2'
        a2 = Address2()
        a2.email='a2@foo.com'
        u2.addresses.append(a2)

        objectstore.commit()
        assert user1.select().execute().fetchall() == [(u1.user_id, u1.name)]
        assert user2.select().execute().fetchall() == [(u2.user_id, u2.name)]
        assert address1.select().execute().fetchall() == [(u1.user_id, a1.user_id, 'a1@foo.com')]
        assert address2.select().execute().fetchall() == [(u2.user_id, a2.user_id, 'a2@foo.com')]

        objectstore.clear()
        u1list = u1mapper.select()
        u2list = u2mapper.select()
        assert len(u1list) == len(u2list) == 1
        assert u1list[0] is not u2list[0]
        assert len(u1list[0].addresses) == len(u2list[0].addresses) == 1
        assert isinstance(u1list[0].addresses[0], Address1)
        assert isinstance(u2list[0].addresses[0], Address2)
        
if __name__ == "__main__":    
    testbase.main()
