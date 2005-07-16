from testbase import PersistTest
import unittest, sys

import sqlalchemy.databases.sqlite as sqllite

db = sqllite.engine('querytest.db', echo = True)

from sqlalchemy.sql import *
from sqlalchemy.schema import *

import sqlalchemy.mapper as mapper

class User:
    def __repr__(self):
        return "User: " + repr(self.user_id) + " " + self.user_name + repr(getattr(self, 'addresses', None))

class Address:
    def __repr__(self):
        return "Address: " + repr(self.user_id) + " " + self.email_address

class MapperTest(PersistTest):
    
    def setUp(self):
        self.users = Table('users', db,
            Column('user_id', INT, primary_key = True),
            Column('user_name', VARCHAR(20)),
        )

        self.addresses = Table('email_addresses', db,
            Column('address_id', INT, primary_key = True),
            Column('user_id', INT),
            Column('email_address', VARCHAR(20)),
        )
        
        self.users.build()
        self.users.insert().execute(user_id = 7, user_name = 'jack')
        self.users.insert().execute(user_id = 8, user_name = 'ed')
        
        self.addresses.build()
        self.addresses.insert().execute(address_id = 1, user_id = 7, email_address = "jack@bean.com")
        self.addresses.insert().execute(address_id = 2, user_id = 8, email_address = "ed@wood.com")
        self.addresses.insert().execute(address_id = 3, user_id = 8, email_address = "ed@lala.com")
        
    def testmapper(self):
        m = mapper.Mapper(User, self.users)
        l = m.select()
        print repr(l)
        print repr(m.identitymap.map)

    def testeager(self):
        m = mapper.Mapper(User, self.users, properties = dict(
            addresses = mapper.EagerLoader(mapper.Mapper(Address, self.addresses), self.users.c.user_id==self.addresses.c.user_id)
        ))
        l = m.select()
        print repr(l)
        
    def tearDown(self):
        self.users.drop()
        self.addresses.drop()
        
        
if __name__ == "__main__":
    unittest.main()        
