
import testbase
import unittest, sys, datetime
import tables
db = testbase.db
from sqlalchemy import *

class TransactionTest(testbase.PersistTest):
    def setUpAll(self):
        global users, metadata
        metadata = MetaData()
        users = Table('query_users', metadata,
            Column('user_id', INT, primary_key = True),
            Column('user_name', VARCHAR(20)),
        )
        users.create(testbase.db)
    
    def tearDown(self):
        testbase.db.connect().execute(users.delete())
    def tearDownAll(self):
        users.drop(testbase.db)
    
    @testbase.unsupported('mysql')
    def testrollback(self):
        """test a basic rollback"""
        connection = testbase.db.connect()
        transaction = connection.begin()
        connection.execute(users.insert(), user_id=1, user_name='user1')
        connection.execute(users.insert(), user_id=2, user_name='user2')
        connection.execute(users.insert(), user_id=3, user_name='user3')
        transaction.rollback()
        
        result = connection.execute("select * from query_users")
        assert len(result.fetchall()) == 0
        connection.close()

class AutoRollbackTest(testbase.PersistTest):
    def setUpAll(self):
        global metadata
        metadata = MetaData()
    
    def tearDownAll(self):
        metadata.drop_all(testbase.db)

    def testrollback_deadlock(self):
        """test that returning connections to the pool clears any object locks."""
        conn1 = testbase.db.connect()
        conn2 = testbase.db.connect()
        users = Table('deadlock_users', metadata,
            Column('user_id', INT, primary_key = True),
            Column('user_name', VARCHAR(20)),
        )
        users.create(conn1)
        conn1.execute("select * from deadlock_users")
        conn1.close()
        # without auto-rollback in the connection pool's return() logic, this deadlocks in Postgres, 
        # because conn1 is returned to the pool but still has a lock on "deadlock_users"
        # comment out the rollback in pool/ConnectionFairy._close() to see !
        users.drop(conn2)
        conn2.close()
        
if __name__ == "__main__":
    testbase.main()        
