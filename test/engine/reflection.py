from testbase import PersistTest
import testbase

import sqlalchemy.ansisql as ansisql

from sqlalchemy import *
from sqlalchemy.exceptions import NoSuchTableError

import unittest, re, StringIO

class ReflectionTest(PersistTest):
    def testbasic(self):
        # really trip it up with a circular reference
        
        use_function_defaults = testbase.db.engine.name == 'postgres' or testbase.db.engine.name == 'oracle'
        
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
        
        meta = BoundMetaData(testbase.db)
        
        users = Table('engine_users', meta,
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

        addresses = Table('engine_email_addresses', meta,
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
            Column('val', String(100))
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
        table.insert().execute({'multi_id':1,'multi_rev':1,'name':'row1', 'val':'value1'})
        table.insert().execute({'multi_id':2,'multi_rev':18,'name':'row2', 'val':'value2'})
        table.insert().execute({'multi_id':3,'multi_rev':3,'name':'row3', 'val':'value3'})
        table.select().execute().fetchall()
        table.drop()

    def testcompositefk(self):
        meta = BoundMetaData(testbase.db)
        table = Table(
            'multi', meta, 
            Column('multi_id', Integer, primary_key=True),
            Column('multi_rev', Integer, primary_key=True),
            Column('name', String(50), nullable=False),
            Column('val', String(100)),
            mysql_engine='InnoDB'
        )
        table2 = Table('multi2', meta, 
            Column('id', Integer, primary_key=True),
            Column('foo', Integer),
            Column('bar', Integer),
            Column('data', String(50)),
            ForeignKeyConstraint(['foo', 'bar'], ['multi.multi_id', 'multi.multi_rev']),
            mysql_engine='InnoDB'
        )
        meta.create_all()
        meta.clear()
        
        try:
            table = Table('multi', meta, autoload=True)
            table2 = Table('multi2', meta, autoload=True)
            
            print table
            print table2
            j = join(table, table2)
            print str(j.onclause)
            self.assert_(and_(table.c.multi_id==table2.c.foo, table.c.multi_rev==table2.c.bar).compare(j.onclause))

        finally:
            meta.drop_all()

    def testcheckfirst(self):
        meta = BoundMetaData(testbase.db)
        
        table = Table('checkfirst', meta, 
            Column('col1', Integer, primary_key=True),
            Column('col2', String(40)))
        try:
            assert not table.exists()
            table.create()
            assert table.exists()
            table.create(checkfirst=True)
            table.drop()
            table.drop(checkfirst=True)
            assert not table.exists()
            table.create(checkfirst=True)
            table.drop()
        finally:
            meta.drop_all()
            
    def testtoengine(self):
        meta = MetaData('md1')
        meta2 = MetaData('md2')
        
        table = Table('mytable', meta,
            Column('myid', Integer, primary_key=True),
            Column('name', String, nullable=False),
            Column('description', String(30)),
        )
        
        table2 = Table('othertable', meta,
            Column('id', Integer, primary_key=True),
            Column('myid', Integer, ForeignKey('mytable.myid'))
            )
            
        
        table_c = table.tometadata(meta2)
        table2_c = table2.tometadata(meta2)

        assert table is not table_c
        assert table_c.c.myid.primary_key
        assert not table_c.c.name.nullable 
        assert table_c.c.description.nullable 
        assert table.primary_key is not table_c.primary_key
        assert [x.name for x in table.primary_key] == [x.name for x in table_c.primary_key]
        assert table2_c.c.myid.foreign_key.column is table_c.c.myid
        assert table2_c.c.myid.foreign_key.column is not table.c.myid
        
    # mysql throws its own exception for no such table, resulting in 
    # a sqlalchemy.SQLError instead of sqlalchemy.NoSuchTableError.
    # this could probably be fixed at some point.
    @testbase.unsupported('mysql')    
    def test_nonexistent(self):
        self.assertRaises(NoSuchTableError, Table,
                          'fake_table',
                          testbase.db, autoload=True)
        
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

class CreateDropTest(PersistTest):
    def setUpAll(self):
        global metadata
        metadata = MetaData()
        users = Table('users', metadata,
                      Column('user_id', Integer, Sequence('user_id_seq', optional=True), primary_key = True),
                      Column('user_name', String(40)),
                      )

        addresses = Table('email_addresses', metadata,
            Column('address_id', Integer, Sequence('address_id_seq', optional=True), primary_key = True),
            Column('user_id', Integer, ForeignKey(users.c.user_id)),
            Column('email_address', String(40)),
    
        )

        orders = Table('orders', metadata,
            Column('order_id', Integer, Sequence('order_id_seq', optional=True), primary_key = True),
            Column('user_id', Integer, ForeignKey(users.c.user_id)),
            Column('description', String(50)),
            Column('isopen', Integer),
    
        )

        orderitems = Table('items', metadata,
            Column('item_id', INT, Sequence('items_id_seq', optional=True), primary_key = True),
            Column('order_id', INT, ForeignKey("orders")),
            Column('item_name', VARCHAR(50)),
    
        )

    def test_sorter( self ):
        tables = metadata._sort_tables(metadata.tables.values())
        table_names = [t.name for t in tables]
        self.assert_( table_names == ['users', 'orders', 'items', 'email_addresses'] or table_names ==  ['users', 'email_addresses', 'orders', 'items'])


    def test_createdrop(self):
        metadata.create_all(engine=testbase.db)
        self.assertEqual( testbase.db.has_table('items'), True )
        self.assertEqual( testbase.db.has_table('email_addresses'), True )        
        metadata.create_all(engine=testbase.db)
        self.assertEqual( testbase.db.has_table('items'), True )        

        metadata.drop_all(engine=testbase.db)
        self.assertEqual( testbase.db.has_table('items'), False )
        self.assertEqual( testbase.db.has_table('email_addresses'), False )                
        metadata.drop_all(engine=testbase.db)
        self.assertEqual( testbase.db.has_table('items'), False )                

class SchemaTest(PersistTest):
    # this test should really be in the sql tests somewhere, not engine
    def testiteration(self):
        metadata = MetaData()
        table1 = Table('table1', metadata, 
            Column('col1', Integer, primary_key=True),
            schema='someschema')
        table2 = Table('table2', metadata, 
            Column('col1', Integer, primary_key=True),
            Column('col2', Integer, ForeignKey('someschema.table1.col1')),
            schema='someschema')
        # insure this doesnt crash
        print [t for t in metadata.table_iterator()]
        buf = StringIO.StringIO()
        def foo(s, p):
            buf.write(s)
        gen = testbase.db.dialect.schemagenerator(testbase.db.engine, foo)
        table1.accept_schema_visitor(gen)
        table2.accept_schema_visitor(gen)
        buf = buf.getvalue()
        assert buf.index("CREATE TABLE someschema.table1") > -1
        assert buf.index("CREATE TABLE someschema.table2") > -1
         
        
        
        
if __name__ == "__main__":
    testbase.main()        
        
