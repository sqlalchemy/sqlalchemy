
import sqlalchemy.ansisql as ansisql
import sqlalchemy.databases.postgres as postgres
import sqlalchemy.databases.oracle as oracle

db = ansisql.engine()

from sqlalchemy.sql import *
from sqlalchemy.schema import *

from testbase import PersistTest
import testbase
import unittest, re


class EngineTest(PersistTest):
    def testsqlitetableops(self):
        import sqlalchemy.databases.sqlite as sqllite
        db = sqllite.engine(':memory:', {}, echo = testbase.echo)
        self.do_tableops(db)
        
    def do_tableops(self, db):
        # really trip it up with a circular reference
        users = Table('users', db,
            Column('user_id', INT, primary_key = True),
            Column('user_name', VARCHAR(20), nullable = False),
            Column('test1', CHAR(5), nullable = False),
            Column('test2', FLOAT(5,5), nullable = False),
            Column('test3', TEXT),
            Column('test4', DECIMAL, nullable = False),
            Column('test5', TIMESTAMP),
            Column('parent_user_id', INT, foreign_key = ForeignKey('users.user_id')),
            Column('test6', DATETIME, nullable = False),
            Column('test7', CLOB),
            Column('test8', BLOB),
            
        )

        addresses = Table('email_addresses', db,
            Column('address_id', INT, primary_key = True),
            Column('remote_user_id', INT, foreign_key = ForeignKey(users.c.user_id)),
            Column('email_address', VARCHAR(20)),
        )
        
#        users.c.parent_user_id.set_foreign_key(ForeignKey(users.c.user_id))

        users.create()
        addresses.create()

        # clear out table registry
        db.tables.clear()

        users = Table('users', db, autoload = True)
        addresses = Table('email_addresses', db, autoload = True)

        users.drop()
        addresses.drop()
        
        users.create()
        addresses.create()
        
if __name__ == "__main__":
    unittest.main()        
        
