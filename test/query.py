from testbase import PersistTest
import testbase
import unittest, sys, datetime

import sqlalchemy.databases.sqlite as sqllite

db = testbase.db

from sqlalchemy import *

class QueryTest(PersistTest):
    
    def setUpAll(self):
        global users
        users = Table('query_users', db,
            Column('user_id', INT, primary_key = True),
            Column('user_name', VARCHAR(20)),
            redefine = True
        )
        users.create()
    
    def setUp(self):
        self.users = users
    def tearDown(self):
        self.users.delete().execute()
    
    def tearDownAll(self):
        global users
        users.drop()
        
    def testinsert(self):
        self.users.insert().execute(user_id = 7, user_name = 'jack')
        print repr(self.users.select().execute().fetchall())
        
    def testupdate(self):

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

    def testdefaults(self):
        x = {'x':0}
        def mydefault():
            x['x'] += 1
            return x['x']
            
        t = Table('default_test1', db, 
            Column('col1', Integer, primary_key=True, default=mydefault),
            Column('col2', String(20), default="imthedefault"),
            Column('col3', String(20), default=func.count(1)),
        )
        t.create()
        t.insert().execute()
        t.insert().execute()
        t.insert().execute()
        t.drop()
        
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

    def testselectlimit(self):
        self.users.insert().execute(user_id=1, user_name='john')
        self.users.insert().execute(user_id=2, user_name='jack')
        self.users.insert().execute(user_id=3, user_name='ed')
        self.users.insert().execute(user_id=4, user_name='wendy')
        self.users.insert().execute(user_id=5, user_name='laura')
        self.users.insert().execute(user_id=6, user_name='ralph')
        self.users.insert().execute(user_id=7, user_name='fido')
        r = self.users.select(limit=3).execute().fetchall()
        self.assert_(r == [(1, 'john'), (2, 'jack'), (3, 'ed')])
        r = self.users.select(limit=3, offset=2).execute().fetchall()
        self.assert_(r==[(3, 'ed'), (4, 'wendy'), (5, 'laura')])
        r = self.users.select(offset=5).execute().fetchall()
        self.assert_(r==[(6, 'ralph'), (7, 'fido')])
        
if __name__ == "__main__":
    testbase.main()        
