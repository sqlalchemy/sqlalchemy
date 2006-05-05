from sqlalchemy import *

from testbase import PersistTest
import testbase
import unittest, re
import tables

class TransactionTest(PersistTest):
    def setUpAll(self):
        tables.create()
    def tearDownAll(self):
        tables.drop()
    def tearDown(self):
        tables.delete()
        
    def testbasic(self):
        testbase.db.begin()
        tables.users.insert().execute(user_name='jack')
        tables.users.insert().execute(user_name='fred')
        testbase.db.commit()
        l = tables.users.select().execute().fetchall()
        print l
        self.assert_(len(l) == 2)

    def testrollback(self):
        testbase.db.begin()
        tables.users.insert().execute(user_name='jack')
        tables.users.insert().execute(user_name='fred')
        testbase.db.rollback()
        l = tables.users.select().execute().fetchall()
        print l
        self.assert_(len(l) == 0)

    @testbase.unsupported('sqlite')
    def testnested(self):
        """tests nested sessions.  SQLite should raise an error."""
        testbase.db.begin()
        tables.users.insert().execute(user_name='jack')
        tables.users.insert().execute(user_name='fred')
        testbase.db.push_session()
        tables.users.insert().execute(user_name='ed')
        tables.users.insert().execute(user_name='wendy')
        testbase.db.pop_session()
        testbase.db.rollback()
        l = tables.users.select().execute().fetchall()
        print l
        self.assert_(len(l) == 2)

    def testtwo(self):
        testbase.db.begin()
        tables.users.insert().execute(user_name='jack')
        tables.users.insert().execute(user_name='fred')
        testbase.db.commit()
        testbase.db.begin()
        tables.users.insert().execute(user_name='ed')
        tables.users.insert().execute(user_name='wendy')
        testbase.db.commit()
        testbase.db.rollback()
        l = tables.users.select().execute().fetchall()
        print l
        self.assert_(len(l) == 4)

if __name__ == "__main__":
    testbase.main()        
