
import sqlalchemy.ansisql as ansisql
import sqlalchemy.databases.postgres as postgres
import sqlalchemy.databases.oracle as oracle
import sqlalchemy.databases.sqlite as sqllite

from sqlalchemy import *

from testbase import PersistTest
import testbase
import unittest, re

class EngineTest(PersistTest):
    def testbasic(self):
        # really trip it up with a circular reference
        users = Table('engine_users', testbase.db,
            Column('user_id', INT, primary_key = True),
            Column('user_name', VARCHAR(20), nullable = False),
            Column('test1', CHAR(5), nullable = False),
            Column('test2', FLOAT(5), nullable = False),
            Column('test3', TEXT),
            Column('test4', DECIMAL, nullable = False),
            Column('test5', TIMESTAMP),
            Column('parent_user_id', Integer, ForeignKey('engine_users.user_id')),
            Column('test6', DateTime, nullable = False),
            Column('test7', String),
            Column('test8', Binary),
            Column('test9', Binary(100)),
            
        )

        addresses = Table('engine_email_addresses', testbase.db,
            Column('address_id', Integer, primary_key = True),
            Column('remote_user_id', Integer, ForeignKey(users.c.user_id)),
            Column('email_address', String(20)),
        )

        print repr(users)
        print repr(addresses)
        
#        users.c.parent_user_id.set_foreign_key(ForeignKey(users.c.user_id))

        users.create()
        addresses.create()

        # clear out table registry
        users.deregister()
        addresses.deregister()
        
        try:
            users = Table('engine_users', testbase.db, autoload = True)
            addresses = Table('engine_email_addresses', testbase.db, autoload = True)
        finally:
            addresses.drop()
            users.drop()

        users.create()
        addresses.create()
        try:
            # create a join from the two tables, this insures that
            # theres a foreign key set up
#            addresses.c.remote_user_id.append_item(ForeignKey('engine_users.user_id'))
            j = join(users, addresses)
            print str(j.onclause)
            self.assert_((users.c.user_id==addresses.c.remote_user_id).compare(j.onclause))
        finally:
            addresses.drop()
            users.drop()

    def testmultipk(self):
        table = Table(
            'engine_multi', testbase.db, 
            Column('multi_id', Integer, primary_key=True),
            Column('multi_rev', Integer, primary_key=True),
            Column('name', String(50), nullable=False),
            Column('value', String(100))
        )
        table.create()
        # clear out table registry
        table.deregister()

        try:
            table = Table('engine_multi', testbase.db, autoload=True)
        finally:
            table.drop()
        
        print repr(
            [table.c['multi_id'].primary_key,
            table.c['multi_rev'].primary_key
            ]
        )
        table.create()
        table.insert().execute({'multi_id':1,'multi_rev':1,'name':'row1', 'value':'value1'})
        table.insert().execute({'multi_id':2,'multi_rev':18,'name':'row2', 'value':'value2'})
        table.insert().execute({'multi_id':3,'multi_rev':3,'name':'row3', 'value':'value3'})
        table.select().execute().fetchall()
        table.drop()
        
        
if __name__ == "__main__":
    testbase.main()        
        
