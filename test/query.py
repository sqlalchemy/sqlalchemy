from testbase import PersistTest
import testbase
import unittest, sys

import sqlalchemy.databases.sqlite as sqllite

db = sqllite.engine(':memory:', {}, echo = testbase.echo)

from sqlalchemy.sql import *
from sqlalchemy.schema import *

class QueryTest(PersistTest):
    
    def setUp(self):
        self.users = Table('query_users', db,
            Column('user_id', INT, primary_key = True),
            Column('user_name', VARCHAR(20)),
            redefine = True
        )
        self.users.create()
        
        
    def testinsert(self):
        c = db.connection()
        self.users.insert().execute(user_id = 7, user_name = 'jack')
        print repr(self.users.select().execute().fetchall())
        
    def testupdate(self):
        c = db.connection()

        self.users.insert().execute(user_id = 7, user_name = 'jack')
        print repr(self.users.select().execute().fetchall())

        self.users.update(self.users.c.user_id == 7).execute(user_name = 'fred')
        print repr(self.users.select().execute().fetchall())

    def testdelete(self):
        c = db.connection()

        self.users.insert().execute(user_id = 7, user_name = 'jack')
        self.users.insert().execute(user_id = 8, user_name = 'fred')
        print repr(self.users.select().execute().fetchall())

        self.users.delete(self.users.c.user_name == 'fred').execute()
        
        print repr(self.users.select().execute().fetchall())
        
    def testtransaction(self):
        def dostuff():
            self.users.insert().execute(user_id = 7, user_name = 'john')
            self.users.insert().execute(user_id = 8, user_name = 'jack')
        
        db.transaction(dostuff)
        print repr(self.users.select().execute().fetchall())    


    def tearDown(self):
        self.users.drop()
        
if __name__ == "__main__":
    unittest.main()        
