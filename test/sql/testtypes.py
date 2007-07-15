from testbase import PersistTest, AssertMixin
import testbase
import pickleable
from sqlalchemy import *
import string,datetime, re, sys, os
import sqlalchemy.engine.url as url

import sqlalchemy.types
from sqlalchemy.databases import mssql, oracle, mysql

db = testbase.db

class MyType(types.TypeEngine):
    def get_col_spec(self):
        return "VARCHAR(100)"
    def convert_bind_param(self, value, engine):
        return "BIND_IN"+ value
    def convert_result_value(self, value, engine):
        return value + "BIND_OUT"
    def adapt(self, typeobj):
        return typeobj()

class MyDecoratedType(types.TypeDecorator):
    impl = String
    def convert_bind_param(self, value, dialect):
        return "BIND_IN"+ super(MyDecoratedType, self).convert_bind_param(value, dialect)
    def convert_result_value(self, value, dialect):
        return super(MyDecoratedType, self).convert_result_value(value, dialect) + "BIND_OUT"
    def copy(self):
        return MyDecoratedType()
        
class MyUnicodeType(types.TypeDecorator):
    impl = Unicode
    def convert_bind_param(self, value, dialect):
        return "UNI_BIND_IN"+ super(MyUnicodeType, self).convert_bind_param(value, dialect)
    def convert_result_value(self, value, dialect):
        return super(MyUnicodeType, self).convert_result_value(value, dialect) + "UNI_BIND_OUT"
    def copy(self):
        return MyUnicodeType(self.impl.length)

class AdaptTest(PersistTest):
    def testadapt(self):
        e1 = url.URL('postgres').get_dialect()()
        e2 = url.URL('mysql').get_dialect()()
        e3 = url.URL('sqlite').get_dialect()()
        
        type = String(40)
        
        t1 = type.dialect_impl(e1)
        t2 = type.dialect_impl(e2)
        t3 = type.dialect_impl(e3)
        assert t1 != t2
        assert t2 != t3
        assert t3 != t1
    
    def testmsnvarchar(self):
        dialect = mssql.MSSQLDialect()
        # run the test twice to insure the caching step works too
        for x in range(0, 1):
            col = Column('', Unicode(length=10))
            dialect_type = col.type.dialect_impl(dialect)
            assert isinstance(dialect_type, mssql.MSNVarchar)
            assert dialect_type.get_col_spec() == 'NVARCHAR(10)'

    def testoracletext(self):
        dialect = oracle.OracleDialect()
        col = Column('', MyDecoratedType)
        dialect_type = col.type.dialect_impl(dialect)
        assert isinstance(dialect_type.impl, oracle.OracleText), repr(dialect_type.impl)


    def testoracletimestamp(self):
        dialect = oracle.OracleDialect()
        t1 = oracle.OracleTimestamp
        t2 = oracle.OracleTimestamp()
        t3 = types.TIMESTAMP
        assert isinstance(dialect.type_descriptor(t1), oracle.OracleTimestamp)
        assert isinstance(dialect.type_descriptor(t2), oracle.OracleTimestamp)
        assert isinstance(dialect.type_descriptor(t3), oracle.OracleTimestamp)

    def testmysqlbinary(self):
        dialect = mysql.MySQLDialect()
        t1 = mysql.MSVarBinary
        t2 = mysql.MSVarBinary()
        assert isinstance(dialect.type_descriptor(t1), mysql.MSVarBinary)
        assert isinstance(dialect.type_descriptor(t2), mysql.MSVarBinary)
        
        
class OverrideTest(PersistTest):
    """tests user-defined types, including a full type as well as a TypeDecorator"""

    def testbasic(self):
        print users.c.goofy4.type
        print users.c.goofy4.type.dialect_impl(testbase.db.dialect)
        print users.c.goofy4.type.dialect_impl(testbase.db.dialect).get_col_spec()
        
    def testprocessing(self):

        global users
        users.insert().execute(user_id = 2, goofy = 'jack', goofy2='jack', goofy3='jack', goofy4='jack')
        users.insert().execute(user_id = 3, goofy = 'lala', goofy2='lala', goofy3='lala', goofy4='lala')
        users.insert().execute(user_id = 4, goofy = 'fred', goofy2='fred', goofy3='fred', goofy4='fred')
        
        l = users.select().execute().fetchall()
        print repr(l)
        self.assert_(l == [(2, 'BIND_INjackBIND_OUT', 'BIND_INjackBIND_OUT', 'BIND_INjackBIND_OUT', u'UNI_BIND_INjackUNI_BIND_OUT'), (3, 'BIND_INlalaBIND_OUT', 'BIND_INlalaBIND_OUT', 'BIND_INlalaBIND_OUT', u'UNI_BIND_INlalaUNI_BIND_OUT'), (4, 'BIND_INfredBIND_OUT', 'BIND_INfredBIND_OUT', 'BIND_INfredBIND_OUT', u'UNI_BIND_INfredUNI_BIND_OUT')])

    def setUpAll(self):
        global users
        users = Table('type_users', db, 
            Column('user_id', Integer, primary_key = True),
            # totall custom type
            Column('goofy', MyType, nullable = False),
            
            # decorated type with an argument, so its a String
            Column('goofy2', MyDecoratedType(50), nullable = False),
            
            # decorated type without an argument, it will adapt_args to TEXT
            Column('goofy3', MyDecoratedType, nullable = False),

            Column('goofy4', MyUnicodeType, nullable = False),

        )
        
        users.create()
    def tearDownAll(self):
        global users
        users.drop()


class ColumnsTest(AssertMixin):

    def testcolumns(self):
        expectedResults = { 'int_column': 'int_column INTEGER',
                            'smallint_column': 'smallint_column SMALLINT',
                            'varchar_column': 'varchar_column VARCHAR(20)',
                            'numeric_column': 'numeric_column NUMERIC(12, 3)',
                            'float_column': 'float_column NUMERIC(25, 2)'
                          }

        if not db.name=='sqlite' and not db.name=='oracle':
            expectedResults['float_column'] = 'float_column FLOAT(25)'
    
        print db.engine.__module__
        testTable = Table('testColumns', db,
            Column('int_column', Integer),
            Column('smallint_column', Smallinteger),
            Column('varchar_column', String(20)),
            Column('numeric_column', Numeric(12,3)),
            Column('float_column', Float(25)),
        )

        for aCol in testTable.c:
            self.assertEquals(expectedResults[aCol.name], db.dialect.schemagenerator(db, None, None).get_column_specification(aCol))
        
class UnicodeTest(AssertMixin):
    """tests the Unicode type.  also tests the TypeDecorator with instances in the types package."""
    def setUpAll(self):
        global unicode_table
        unicode_table = Table('unicode_table', db, 
            Column('id', Integer, Sequence('uni_id_seq', optional=True), primary_key=True),
            Column('unicode_varchar', Unicode(250)),
            Column('unicode_text', Unicode),
            Column('plain_varchar', String(250))
            )
        unicode_table.create()
    def tearDownAll(self):
        unicode_table.drop()

    def testbasic(self):
        assert unicode_table.c.unicode_varchar.type.length == 250
        rawdata = 'Alors vous imaginez ma surprise, au lever du jour, quand une dr\xc3\xb4le de petit voix m\xe2\x80\x99a r\xc3\xa9veill\xc3\xa9. Elle disait: \xc2\xab S\xe2\x80\x99il vous pla\xc3\xaet\xe2\x80\xa6 dessine-moi un mouton! \xc2\xbb\n'
        unicodedata = rawdata.decode('utf-8')
        unicode_table.insert().execute(unicode_varchar=unicodedata,
                                       unicode_text=unicodedata,
                                       plain_varchar=rawdata)
        x = unicode_table.select().execute().fetchone()
        self.echo(repr(x['unicode_varchar']))
        self.echo(repr(x['unicode_text']))
        self.echo(repr(x['plain_varchar']))
        self.assert_(isinstance(x['unicode_varchar'], unicode) and x['unicode_varchar'] == unicodedata)
        self.assert_(isinstance(x['unicode_text'], unicode) and x['unicode_text'] == unicodedata)
        if isinstance(x['plain_varchar'], unicode):
            # SQLLite and MSSQL return non-unicode data as unicode
            self.assert_(db.name in ('sqlite', 'mssql'))
            self.assert_(x['plain_varchar'] == unicodedata)
            self.echo("it's %s!" % db.name)
        else:
            self.assert_(not isinstance(x['plain_varchar'], unicode) and x['plain_varchar'] == rawdata)

    def testengineparam(self):
        """tests engine-wide unicode conversion"""
        prev_unicode = db.engine.dialect.convert_unicode
        try:
            db.engine.dialect.convert_unicode = True
            rawdata = 'Alors vous imaginez ma surprise, au lever du jour, quand une dr\xc3\xb4le de petit voix m\xe2\x80\x99a r\xc3\xa9veill\xc3\xa9. Elle disait: \xc2\xab S\xe2\x80\x99il vous pla\xc3\xaet\xe2\x80\xa6 dessine-moi un mouton! \xc2\xbb\n'
            unicodedata = rawdata.decode('utf-8')
            unicode_table.insert().execute(unicode_varchar=unicodedata,
                                           unicode_text=unicodedata,
                                           plain_varchar=rawdata)
            x = unicode_table.select().execute().fetchone()
            self.echo(repr(x['unicode_varchar']))
            self.echo(repr(x['unicode_text']))
            self.echo(repr(x['plain_varchar']))
            self.assert_(isinstance(x['unicode_varchar'], unicode) and x['unicode_varchar'] == unicodedata)
            self.assert_(isinstance(x['unicode_text'], unicode) and x['unicode_text'] == unicodedata)
            self.assert_(isinstance(x['plain_varchar'], unicode) and x['plain_varchar'] == unicodedata)
        finally:
            db.engine.dialect.convert_unicode = prev_unicode

    @testbase.unsupported('oracle')
    def testlength(self):
        """checks the database correctly understands the length of a unicode string"""
        teststr = u'aaa\x1234'
        self.assert_(db.func.length(teststr).scalar() == len(teststr))
  
class BinaryTest(AssertMixin):
    def setUpAll(self):
        global binary_table
        binary_table = Table('binary_table', db, 
        Column('primary_id', Integer, Sequence('binary_id_seq', optional=True), primary_key=True),
        Column('data', Binary),
        Column('data_slice', Binary(100)),
        Column('misc', String(30)),
        # construct PickleType with non-native pickle module, since cPickle uses relative module
        # loading and confuses this test's parent package 'sql' with the 'sqlalchemy.sql' package relative
	# to the 'types' module
        Column('pickled', PickleType)
        )
        binary_table.create()

    def tearDown(self):
        binary_table.delete().execute()

    def tearDownAll(self):
        binary_table.drop()

    def testbinary(self):
        testobj1 = pickleable.Foo('im foo 1')
        testobj2 = pickleable.Foo('im foo 2')

        stream1 =self.load_stream('binary_data_one.dat')
        stream2 =self.load_stream('binary_data_two.dat')
        binary_table.insert().execute(primary_id=1, misc='binary_data_one.dat',    data=stream1, data_slice=stream1[0:100], pickled=testobj1)
        binary_table.insert().execute(primary_id=2, misc='binary_data_two.dat', data=stream2, data_slice=stream2[0:99], pickled=testobj2)
        binary_table.insert().execute(primary_id=3, misc='binary_data_two.dat', data=None, data_slice=stream2[0:99], pickled=None)
        l = binary_table.select(order_by=binary_table.c.primary_id).execute().fetchall()
        print type(stream1), type(l[0]['data']), type(l[0]['data_slice'])
        print len(stream1), len(l[0]['data']), len(l[0]['data_slice'])
        self.assert_(list(stream1) == list(l[0]['data']))
        self.assert_(list(stream1[0:100]) == list(l[0]['data_slice']))
        self.assert_(list(stream2) == list(l[1]['data']))
        self.assert_(testobj1 == l[0]['pickled'])
        self.assert_(testobj2 == l[1]['pickled'])

    def load_stream(self, name, len=12579):
        f = os.path.join(os.path.dirname(testbase.__file__), name)
        # put a number less than the typical MySQL default BLOB size
        return file(f).read(len)
    
    @testbase.supported('oracle')
    def test_oracle_autobinary(self):
        stream1 =self.load_stream('binary_data_one.dat')
        stream2 =self.load_stream('binary_data_two.dat')
        binary_table.insert().execute(primary_id=1, misc='binary_data_one.dat',    data=stream1, data_slice=stream1[0:100])
        binary_table.insert().execute(primary_id=2, misc='binary_data_two.dat', data=stream2, data_slice=stream2[0:99])
        binary_table.insert().execute(primary_id=3, misc='binary_data_two.dat', data=None, data_slice=stream2[0:99], pickled=None)
        result = testbase.db.connect().execute("select primary_id, misc, data, data_slice from binary_table")
        l = result.fetchall()
        l[0]['data']
        self.assert_(list(stream1) == list(l[0]['data']))
        self.assert_(list(stream1[0:100]) == list(l[0]['data_slice']))
        self.assert_(list(stream2) == list(l[1]['data']))

    
class DateTest(AssertMixin):
    def setUpAll(self):
        global users_with_date, insert_data

        if db.engine.name == 'oracle':
            import sqlalchemy.databases.oracle as oracle
            insert_data =  [
                    [7, 'jack', datetime.datetime(2005, 11, 10, 0, 0), datetime.date(2005,11,10), datetime.datetime(2005, 11, 10, 0, 0, 0, 29384)],
                    [8, 'roy', datetime.datetime(2005, 11, 10, 11, 52, 35), datetime.date(2005,10,10), datetime.datetime(2006, 5, 10, 15, 32, 47, 6754)],
                    [9, 'foo', datetime.datetime(2006, 11, 10, 11, 52, 35), datetime.date(1970,4,1), datetime.datetime(2004, 9, 18, 4, 0, 52, 1043)],
                    [10, 'colber', None, None, None]
             ]

            fnames = ['user_id', 'user_name', 'user_datetime', 'user_date', 'user_time']

            collist = [Column('user_id', INT, primary_key = True), Column('user_name', VARCHAR(20)), Column('user_datetime', DateTime),
               Column('user_date', Date), Column('user_time', TIMESTAMP)]
        elif db.engine.name == 'mysql' or db.engine.name == 'mssql':
            # these dont really support the TIME type at all
            insert_data =  [
                 [7, 'jack', datetime.datetime(2005, 11, 10, 0, 0), datetime.datetime(2005, 11, 10, 0, 0, 0)],
                 [8, 'roy', datetime.datetime(2005, 11, 10, 11, 52, 35), datetime.datetime(2006, 5, 10, 15, 32, 47)],
                 [9, 'foo', datetime.datetime(2005, 11, 10, 11, 52, 35), datetime.datetime(2004, 9, 18, 4, 0, 52)],
                 [10, 'colber', None, None]
            ]

            fnames = ['user_id', 'user_name', 'user_datetime', 'user_date', 'user_time']

            collist = [Column('user_id', INT, primary_key = True), Column('user_name', VARCHAR(20)), Column('user_datetime', DateTime),
            Column('user_date', DateTime)]
        else:
            insert_data =  [
                    [7, 'jack', datetime.datetime(2005, 11, 10, 0, 0), datetime.date(2005,11,10), datetime.time(12,20,2)],
                    [8, 'roy', datetime.datetime(2005, 11, 10, 11, 52, 35), datetime.date(2005,10,10), datetime.time(0,0,0)],
                    [9, 'foo', datetime.datetime(2005, 11, 10, 11, 52, 35, 54839), datetime.date(1970,4,1), datetime.time(23,59,59,999)],
                    [10, 'colber', None, None, None]
            ]

            fnames = ['user_id', 'user_name', 'user_datetime', 'user_date', 'user_time']

            collist = [Column('user_id', INT, primary_key = True), Column('user_name', VARCHAR(20)), Column('user_datetime', DateTime(timezone=False)),
                           Column('user_date', Date), Column('user_time', Time)]
 
        users_with_date = Table('query_users_with_date', db, *collist)
        users_with_date.create()
        insert_dicts = [dict(zip(fnames, d)) for d in insert_data]

        for idict in insert_dicts:
            users_with_date.insert().execute(**idict) # insert the data

    def tearDownAll(self):
        users_with_date.drop()

    def testdate(self):
        global insert_data

        l = map(list, users_with_date.select().execute().fetchall())
        self.assert_(l == insert_data, 'DateTest mismatch: got:%s expected:%s' % (l, insert_data))


    def testtextdate(self):     
        x = db.text("select user_datetime from query_users_with_date", typemap={'user_datetime':DateTime}).execute().fetchall()
        
        print repr(x)
        self.assert_(isinstance(x[0][0], datetime.datetime))
        
        #x = db.text("select * from query_users_with_date where user_datetime=:date", bindparams=[bindparam('date', )]).execute(date=datetime.datetime(2005, 11, 10, 11, 52, 35)).fetchall()
        #print repr(x)

    def testdate2(self):
        t = Table('testdate', testbase.metadata, Column('id', Integer, Sequence('datetest_id_seq', optional=True), primary_key=True),
                Column('adate', Date), Column('adatetime', DateTime))
        t.create()
        try:
            d1 = datetime.date(2007, 10, 30)
            t.insert().execute(adate=d1, adatetime=d1)
            d2 = datetime.datetime(2007, 10, 30)
            t.insert().execute(adate=d2, adatetime=d2)

            x = t.select().execute().fetchall()[0]
            self.assert_(x.adate.__class__ == datetime.date)
            self.assert_(x.adatetime.__class__ == datetime.datetime)

        finally:
            t.drop()

class IntervalTest(AssertMixin):
    def setUpAll(self):
        global interval_table, metadata
        metadata = MetaData(testbase.db)
        interval_table = Table("intervaltable", metadata, 
            Column("id", Integer, Sequence('interval_id_seq', optional=True), primary_key=True),
            Column("interval", Interval),
            )
        metadata.create_all()
    
    def tearDown(self):
        interval_table.delete().execute()
            
    def tearDownAll(self):
        metadata.drop_all()
        
    def test_roundtrip(self):
        delta = datetime.datetime(2006, 10, 5) - datetime.datetime(2005, 8, 17)
        interval_table.insert().execute(interval=delta)
        assert interval_table.select().execute().fetchone()['interval'] == delta

    def test_null(self):
        interval_table.insert().execute(id=1, inverval=None)
        assert interval_table.select().execute().fetchone()['interval'] is None
        
class BooleanTest(AssertMixin):
    def setUpAll(self):
        global bool_table
        metadata = MetaData(testbase.db)
        bool_table = Table('booltest', metadata, 
            Column('id', Integer, primary_key=True),
            Column('value', Boolean))
        bool_table.create()
    def tearDownAll(self):
        bool_table.drop()
    def testbasic(self):
        bool_table.insert().execute(id=1, value=True)
        bool_table.insert().execute(id=2, value=False)
        bool_table.insert().execute(id=3, value=True)
        bool_table.insert().execute(id=4, value=True)
        bool_table.insert().execute(id=5, value=True)
        
        res = bool_table.select(bool_table.c.value==True).execute().fetchall()
        print res
        assert(res==[(1, True),(3, True),(4, True),(5, True)])
        
        res2 = bool_table.select(bool_table.c.value==False).execute().fetchall()
        print res2
        assert(res2==[(2, False)])

if __name__ == "__main__":
    testbase.main()
