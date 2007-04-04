from testbase import PersistTest
import testbase

import sqlalchemy.ansisql as ansisql

from sqlalchemy import *
from sqlalchemy.exceptions import NoSuchTableError
import sqlalchemy.databases.mysql as mysql

import unittest, re, StringIO

class ReflectionTest(PersistTest):
    def testbasic(self):
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
            Column('test_numeric', Numeric(None, None)),
            mysql_engine='InnoDB'
        )

        addresses = Table('engine_email_addresses', meta,
            Column('address_id', Integer, primary_key = True),
            Column('remote_user_id', Integer, ForeignKey(users.c.user_id)),
            Column('email_address', String(20)),
            mysql_engine='InnoDB'
        )
        meta.drop_all()
        
        users.create()
        addresses.create()

        # clear out table registry
        meta.clear()

        try:
            addresses = Table('engine_email_addresses', meta, autoload = True)
            # reference the addresses foreign key col, which will require users to be 
            # reflected at some point
            users = Table('engine_users', meta, autoload = True)
            assert users.c.user_id in users.primary_key
            assert len(users.primary_key) == 1
        finally:
            addresses.drop()
            users.drop()
        
        # a hack to remove the defaults we got reflecting from postgres
        # SERIAL columns, since they reference sequences that were just dropped.
        # PG 8.1 doesnt want to create them if the underlying sequence doesnt exist
        users.c.user_id.default = None
        addresses.c.address_id.default = None
        
        users.create()
        addresses.create()
        try:
            # create a join from the two tables, this ensures that
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
            
    @testbase.supported('postgres')
    def testpgdates(self):
        m1 = BoundMetaData(testbase.db)
        t1 = Table('pgdate', m1, 
            Column('date1', DateTime(timezone=True)),
            Column('date2', DateTime(timezone=False))
            )
        m1.create_all()
        try:
            m2 = BoundMetaData(testbase.db)
            t2 = Table('pgdate', m2, autoload=True)
            assert t2.c.date1.type.timezone is True
            assert t2.c.date2.type.timezone is False
        finally:
            m1.drop_all()
            
            
    def testoverridecolumns(self):
        """test that you can override columns which contain foreign keys to other reflected tables"""
        meta = BoundMetaData(testbase.db)
        users = Table('users', meta, 
            Column('id', Integer, primary_key=True),
            Column('name', String(30)))
        addresses = Table('addresses', meta,
            Column('id', Integer, primary_key=True),
            Column('street', String(30)),
            Column('user_id', Integer))
            
        meta.create_all()            
        try:
            meta2 = BoundMetaData(testbase.db)
            a2 = Table('addresses', meta2, 
                Column('user_id', Integer, ForeignKey('users.id')),
                autoload=True)
            u2 = Table('users', meta2, autoload=True)
            
            assert len(a2.c.user_id.foreign_keys)>0
            assert list(a2.c.user_id.foreign_keys)[0].parent is a2.c.user_id
            assert u2.join(a2).onclause == u2.c.id==a2.c.user_id

            meta3 = BoundMetaData(testbase.db)
            u3 = Table('users', meta3, autoload=True)
            a3 = Table('addresses', meta3, 
                Column('user_id', Integer, ForeignKey('users.id')),
                autoload=True)
            
            assert u3.join(a3).onclause == u3.c.id==a3.c.user_id
            
        finally:
            meta.drop_all()

    def testoverridecolumns2(self):
        """test that you can override columns which contain foreign keys to other reflected tables,
        where the foreign key column is also a primary key column"""
        meta = BoundMetaData(testbase.db)
        users = Table('users', meta, 
            Column('id', Integer, primary_key=True),
            Column('name', String(30)))
        addresses = Table('addresses', meta,
            Column('id', Integer, primary_key=True),
            Column('street', String(30)))


        meta.create_all()            
        try:
            meta2 = BoundMetaData(testbase.db)
            a2 = Table('addresses', meta2, 
                Column('id', Integer, ForeignKey('users.id'), primary_key=True, ),
                autoload=True)
            u2 = Table('users', meta2, autoload=True)

            assert list(a2.primary_key) == [a2.c.id]
            assert list(u2.primary_key) == [u2.c.id]
            assert u2.join(a2).onclause == u2.c.id==a2.c.id

            # heres what was originally failing, because a2's primary key
            # had two "id" columns, one of which was not part of a2's "c" collection
            #class Address(object):pass
            #mapper(Address, a2)
            #add1 = Address()
            #sess = create_session()
            #sess.save(add1)
            #sess.flush()
            
            meta3 = BoundMetaData(testbase.db)
            u3 = Table('users', meta3, autoload=True)
            a3 = Table('addresses', meta3, 
                Column('id', Integer, ForeignKey('users.id'), primary_key=True),
                autoload=True)

            assert list(a3.primary_key) == [a3.c.id]
            assert list(u3.primary_key) == [u3.c.id]
            assert u3.join(a3).onclause == u3.c.id==a3.c.id

        finally:
            meta.drop_all()
            
    @testbase.supported('mysql')
    def testmysqltypes(self):
        meta1 = BoundMetaData(testbase.db)
        table = Table(
            'mysql_types', meta1,
            Column('id', Integer, primary_key=True),
            Column('num1', mysql.MSInteger(unsigned=True)),
            Column('text1', mysql.MSLongText),
            Column('text2', mysql.MSLongText()),
             Column('num2', mysql.MSBigInteger),
             Column('num3', mysql.MSBigInteger()),
             Column('num4', mysql.MSDouble),
             Column('num5', mysql.MSDouble()),
             Column('enum1', mysql.MSEnum('"black"', '"white"')),
            )
        try:
            table.create(checkfirst=True)
            meta2 = BoundMetaData(testbase.db)
            t2 = Table('mysql_types', meta2, autoload=True)
            assert isinstance(t2.c.num1.type, mysql.MSInteger)
            assert t2.c.num1.type.unsigned
            assert isinstance(t2.c.text1.type, mysql.MSLongText)
            assert isinstance(t2.c.text2.type, mysql.MSLongText)
            assert isinstance(t2.c.num2.type, mysql.MSBigInteger)
            assert isinstance(t2.c.num3.type, mysql.MSBigInteger)
            assert isinstance(t2.c.num4.type, mysql.MSDouble)
            assert isinstance(t2.c.num5.type, mysql.MSDouble)
            assert isinstance(t2.c.enum1.type, mysql.MSEnum)
            t2.drop()
            t2.create()
        finally:
            table.drop(checkfirst=True)
            
    @testbase.supported('postgres')
    def testredundantsequence(self):
        """test that sequences get checked for, before create"""
        meta1 = BoundMetaData(testbase.db)
        t = Table('mytable', meta1, 
            Column('col1', Integer, Sequence('fooseq')))
        try:
            testbase.db.execute("CREATE SEQUENCE fooseq")
            t.create()
        finally:
            t.drop()
    
    def test_pks_not_uniques(self):
        """test that primary key reflection not tripped up by unique indexes"""
        testbase.db.execute("""
        CREATE TABLE book (
            id INTEGER NOT NULL,
            title VARCHAR(100) NOT NULL,
            series INTEGER NULL,
            series_id INTEGER NULL,
            UNIQUE(series, series_id),
            PRIMARY KEY(id)
        )""")
        try:
            metadata = MetaData(engine=testbase.db)
            book = Table('book', metadata, autoload=True)
            assert book.c.id  in book.primary_key
            assert book.c.series not in book.primary_key
            assert len(book.primary_key) == 1
        finally:
            testbase.db.execute("drop table book")

    def test_composite_pks(self):
        """test reflection of a composite primary key"""
        testbase.db.execute("""
        CREATE TABLE book (
            id INTEGER NOT NULL,
            isbn VARCHAR(50) NOT NULL,
            title VARCHAR(100) NOT NULL,
            series INTEGER NULL,
            series_id INTEGER NULL,
            UNIQUE(series, series_id),
            PRIMARY KEY(id, isbn)
        )""")
        try:
            metadata = MetaData(engine=testbase.db)
            book = Table('book', metadata, autoload=True)
            assert book.c.id  in book.primary_key
            assert book.c.isbn  in book.primary_key
            assert book.c.series not in book.primary_key
            assert len(book.primary_key) == 2
        finally:
            testbase.db.execute("drop table book")
            
    @testbase.supported('sqlite')
    def test_goofy_sqlite(self):
        """test autoload of table where quotes were used with all the colnames.  quirky in sqlite."""
        testbase.db.execute("""CREATE TABLE "django_content_type" (
            "id" integer NOT NULL PRIMARY KEY,
            "django_stuff" text NULL
        )
        """)
        testbase.db.execute("""
        CREATE TABLE "django_admin_log" (
            "id" integer NOT NULL PRIMARY KEY,
            "action_time" datetime NOT NULL,
            "content_type_id" integer NULL REFERENCES "django_content_type" ("id"),
            "object_id" text NULL,
            "change_message" text NOT NULL
        )
        """)
        try:
            meta = BoundMetaData(testbase.db)
            table1 = Table("django_admin_log", meta, autoload=True)
            table2 = Table("django_content_type", meta, autoload=True)
            j = table1.join(table2)
            assert j.onclause == table1.c.content_type_id==table2.c.id
        finally:
            testbase.db.execute("drop table django_admin_log")
            testbase.db.execute("drop table django_content_type")

    def test_composite_fk(self):
        """test reflection of composite foreign keys"""
        meta = BoundMetaData(testbase.db)
        table = Table(
            'multi', meta, 
            Column('multi_id', Integer, primary_key=True),
            Column('multi_rev', Integer, primary_key=True),
            Column('multi_hoho', Integer, primary_key=True),
            Column('name', String(50), nullable=False),
            Column('val', String(100)),
            mysql_engine='InnoDB'
        )
        table2 = Table('multi2', meta, 
            Column('id', Integer, primary_key=True),
            Column('foo', Integer),
            Column('bar', Integer),
            Column('lala', Integer),
            Column('data', String(50)),
            ForeignKeyConstraint(['foo', 'bar', 'lala'], ['multi.multi_id', 'multi.multi_rev', 'multi.multi_hoho']),
            mysql_engine='InnoDB'
        )
        assert table.c.multi_hoho
        meta.create_all()
        meta.clear()
        
        try:
            table = Table('multi', meta, autoload=True)
            table2 = Table('multi2', meta, autoload=True)
            
            print table
            print table2
            j = join(table, table2)
            print str(j.onclause)
            self.assert_(and_(table.c.multi_id==table2.c.foo, table.c.multi_rev==table2.c.bar, table.c.multi_hoho==table2.c.lala).compare(j.onclause))

        finally:
            meta.drop_all()

            
    def testtometadata(self):
        meta = MetaData('md1')
        meta2 = MetaData('md2')
        
        table = Table('mytable', meta,
            Column('myid', Integer, primary_key=True),
            Column('name', String, nullable=False),
            Column('description', String(30), CheckConstraint("description='hi'")),
            UniqueConstraint('name')
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
        assert list(table2_c.c.myid.foreign_keys)[0].column is table_c.c.myid
        assert list(table2_c.c.myid.foreign_keys)[0].column is not table.c.myid
        for c in table_c.c.description.constraints:
            if isinstance(c, CheckConstraint):
                break
        else:
            assert False
        assert c.sqltext=="description='hi'"
        
        for c in table_c.constraints:
            if isinstance(c, UniqueConstraint):
                break
        else:
            assert False
        assert c.columns.contains_column(table_c.c.name)
        assert not c.columns.contains_column(table.c.name)
        
    # mysql throws its own exception for no such table, resulting in 
    # a sqlalchemy.SQLError instead of sqlalchemy.NoSuchTableError.
    # this could probably be fixed at some point.
    @testbase.unsupported('mysql')    
    def test_nonexistent(self):
        self.assertRaises(NoSuchTableError, Table,
                          'fake_table',
                          testbase.db, autoload=True)
        
    def testoverride(self):
        meta = BoundMetaData(testbase.db)
        table = Table(
            'override_test', meta, 
            Column('col1', Integer, primary_key=True),
            Column('col2', String(20)),
            Column('col3', Numeric)
        )
        table.create()
        # clear out table registry

        meta2 = BoundMetaData(testbase.db)
        try:
            table = Table(
                'override_test', meta2,
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
        global metadata, users
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
        tables = metadata.table_iterator(reverse=False)
        table_names = [t.name for t in tables]
        self.assert_( table_names == ['users', 'orders', 'items', 'email_addresses'] or table_names ==  ['users', 'email_addresses', 'orders', 'items'])

    def testcheckfirst(self):
        try:
            assert not users.exists(testbase.db)
            users.create(connectable=testbase.db)
            assert users.exists(testbase.db)
            users.create(connectable=testbase.db, checkfirst=True)
            users.drop(connectable=testbase.db)
            users.drop(connectable=testbase.db, checkfirst=True)
            assert not users.exists(connectable=testbase.db)
            users.create(connectable=testbase.db, checkfirst=True)
            users.drop(connectable=testbase.db)
        finally:
            metadata.drop_all(connectable=testbase.db)

    def test_createdrop(self):
        metadata.create_all(connectable=testbase.db)
        self.assertEqual( testbase.db.has_table('items'), True )
        self.assertEqual( testbase.db.has_table('email_addresses'), True )        
        metadata.create_all(connectable=testbase.db)
        self.assertEqual( testbase.db.has_table('items'), True )        

        metadata.drop_all(connectable=testbase.db)
        self.assertEqual( testbase.db.has_table('items'), False )
        self.assertEqual( testbase.db.has_table('email_addresses'), False )                
        metadata.drop_all(connectable=testbase.db)
        self.assertEqual( testbase.db.has_table('items'), False )                

class SchemaTest(PersistTest):
    # this test should really be in the sql tests somewhere, not engine
    @testbase.unsupported('sqlite')
    def testiteration(self):
        metadata = MetaData()
        table1 = Table('table1', metadata, 
            Column('col1', Integer, primary_key=True),
            schema='someschema')
        table2 = Table('table2', metadata, 
            Column('col1', Integer, primary_key=True),
            Column('col2', Integer, ForeignKey('someschema.table1.col1')),
            schema='someschema')
        # ensure this doesnt crash
        print [t for t in metadata.table_iterator()]
        buf = StringIO.StringIO()
        def foo(s, p=None):
            buf.write(s)
        gen = create_engine(testbase.db.name + "://", strategy="mock", executor=foo)
        gen = gen.dialect.schemagenerator(gen)
        gen.traverse(table1)
        gen.traverse(table2)
        buf = buf.getvalue()
        print buf
        assert buf.index("CREATE TABLE someschema.table1") > -1
        assert buf.index("CREATE TABLE someschema.table2") > -1
    
    @testbase.supported('postgres')
    def testpg(self):
        """note: this test requires that the 'test_schema' schema be separate and accessible by the test user"""
        
        meta1 = BoundMetaData(testbase.db)
        users = Table('users', meta1,
            Column('user_id', Integer, primary_key = True),
            Column('user_name', String(30), nullable = False),
            schema="test_schema"
            )

        addresses = Table('email_addresses', meta1,
            Column('address_id', Integer, primary_key = True),
            Column('remote_user_id', Integer, ForeignKey(users.c.user_id)),
            Column('email_address', String(20)),
            schema="test_schema"
        )
        meta1.create_all()
        try:
            meta2 = BoundMetaData(testbase.db)
            addresses = Table('email_addresses', meta2, autoload=True, schema="test_schema")
            users = Table('users', meta2, mustexist=True, schema="test_schema")

            print users
            print addresses
            j = join(users, addresses)
            print str(j.onclause)
            self.assert_((users.c.user_id==addresses.c.remote_user_id).compare(j.onclause))
        finally:
            meta1.drop_all()
        
if __name__ == "__main__":
    testbase.main()        
        
