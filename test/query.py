from testbase import PersistTest
import testbase
import unittest, sys, datetime

import sqlalchemy.databases.sqlite as sqllite

db = testbase.db

from sqlalchemy import *

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

    def testselectdate(self):
           users_with_date = Table('query_users_with_date', db,
               Column('user_id', INT, primary_key = True),
               Column('user_name', VARCHAR(20)),
               Column('user_date', DateTime),
               redefine = True
           )
           users_with_date.create()

           c = db.connection()
           users_with_date.insert().execute(user_id = 7, user_name = 'jack', user_date=datetime.datetime(2005,11,10))
           users_with_date.insert().execute(user_id = 8, user_name = 'roy', user_date=datetime.datetime(2005,11,10, 11,52,35))
           users_with_date.insert().execute(user_id = 9, user_name = 'foo', user_date=datetime.datetime(2005,11,10, 11,52,35, 54839))
           users_with_date.insert().execute(user_id = 10, user_name = 'colber', user_date=None)
           print repr(users_with_date.select().execute().fetchall())
           users_with_date.drop()

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
