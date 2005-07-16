from testbase import PersistTest
import unittest, sys

import sqlalchemy.databases.sqlite as sqllite

db = sqllite.engine('querytest.db', echo = False)

from sqlalchemy.sql import *
from sqlalchemy.schema import *

import sqlalchemy.mapper as mapper

class User:
    def __repr__(self):
        return "User: " + repr(self.user_id) + " " + self.user_name

class MapperTest(PersistTest):
    
    def setUp(self):
        self.users = Table('users', db,
            Column('user_id', INT, primary_key = True),
            Column('user_name', VARCHAR(20)),
        )
        
        self.users.build()
        self.users.insert().execute(user_id = 7, user_name = 'jack')
        self.users.insert().execute(user_id = 8, user_name = 'ed')
        
    def testmapper(self):
        m = mapper.Mapper(User, self.users)
        l = m.select()
        print repr(l)

    def tearDown(self):
        self.users.drop()
        
if __name__ == "__main__":
    unittest.main()        
