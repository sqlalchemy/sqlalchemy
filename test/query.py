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

    def testdefaults(self):
        x = {'x':0}
        def mydefault():
            x['x'] += 1
            return x['x']
            
        # select "count(1)" from the DB which returns different results
        # on different DBs
        f = select([func.count(1)], engine=db).execute().fetchone()[0]
        
        t = Table('default_test1', db, 
            Column('col1', Integer, primary_key=True, default=mydefault),
            Column('col2', String(20), default="imthedefault"),
            Column('col3', Integer, default=func.count(1)),
        )
        t.create()
        try:
            t.insert().execute()
            t.insert().execute()
            t.insert().execute()
        
            l = t.select().execute()
            self.assert_(l.fetchall() == [(1, 'imthedefault', f), (2, 'imthedefault', f), (3, 'imthedefault', f)])
        finally:
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
