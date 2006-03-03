
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
        
        use_function_defaults = testbase.db.engine.__module__.endswith('postgres') or testbase.db.engine.__module__.endswith('oracle')
        
        use_string_defaults = use_function_defaults or testbase.db.engine.__module__.endswith('sqlite')

        if use_function_defaults:
            defval = func.current_date()
            deftype = Date
        else:
            defval = "3"
            deftype = Integer

        if use_string_defaults:
            deftype2 = String
            defval2 = "im a default"
        else:
            deftype2 = Integer
            defval2 = "15"
            
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
            Column('test_passivedefault', deftype, PassiveDefault(defval)),
            Column('test_passivedefault2', Integer, PassiveDefault("5")),
            Column('test_passivedefault3', deftype2, PassiveDefault(defval2)),
            Column('test9', Binary(100)),
            mysql_engine='InnoDB'
        )

        addresses = Table('engine_email_addresses', testbase.db,
            Column('address_id', Integer, primary_key = True),
            Column('remote_user_id', Integer, ForeignKey(users.c.user_id)),
            Column('email_address', String(20)),
            mysql_engine='InnoDB'
        )

        
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
            # previously, we couldnt get foreign keys out of mysql.  seems like
            # we can now as long as we use InnoDB
#            if testbase.db.engine.__module__.endswith('mysql'):
 #               addresses.c.remote_user_id.append_item(ForeignKey('engine_users.user_id'))
            print users
            print addresses
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
    
    def testtoengine(self):
        db = ansisql.engine()
        
        table = Table('mytable', db,
            Column('myid', Integer, key = 'id'),
            Column('name', String, key = 'name', nullable=False),
            Column('description', String, key = 'description'),
        )
        
        print repr(table)
        
        pgdb = postgres.engine({})
        
        pgtable = table.toengine(pgdb)
        
        print repr(pgtable)
        assert pgtable.c.id.nullable 
        assert not pgtable.c.name.nullable 
        assert pgtable.c.description.nullable 
        
    def testoverride(self):
        table = Table(
            'override_test', testbase.db, 
            Column('col1', Integer, primary_key=True),
            Column('col2', String(20)),
            Column('col3', Numeric)
        )
        table.create()
        # clear out table registry
        table.deregister()

        try:
            table = Table(
                'override_test', testbase.db,
                Column('col2', Unicode()),
                Column('col4', String(30)), autoload=True)
        
            print repr(table)
            self.assert_(isinstance(table.c.col1.type, Integer))
            self.assert_(isinstance(table.c.col2.type, Unicode))
            self.assert_(isinstance(table.c.col4.type, String))
        finally:
            table.drop()
            
if __name__ == "__main__":
    testbase.main()        
        
