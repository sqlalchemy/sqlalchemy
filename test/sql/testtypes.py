import testbase
import pickleable
import datetime, os
from sqlalchemy import *
from sqlalchemy import types
import sqlalchemy.engine.url as url
from sqlalchemy.databases import mssql, oracle, mysql
from testlib import *


class MyType(types.TypeEngine):
    def get_col_spec(self):
        return "VARCHAR(100)"
    def bind_processor(self, dialect):
        def process(value):
            return "BIND_IN"+ value
        return process
    def result_processor(self, dialect):
        def process(value):
            return value + "BIND_OUT"
        return process
    def adapt(self, typeobj):
        return typeobj()

class MyDecoratedType(types.TypeDecorator):
    impl = String
    def bind_processor(self, dialect):
        impl_processor = super(MyDecoratedType, self).bind_processor(dialect) or (lambda value:value)
        def process(value):
            return "BIND_IN"+ impl_processor(value)
        return process
    def result_processor(self, dialect):
        impl_processor = super(MyDecoratedType, self).result_processor(dialect) or (lambda value:value)
        def process(value):
            return impl_processor(value) + "BIND_OUT"
        return process
    def copy(self):
        return MyDecoratedType()
        
class MyUnicodeType(types.TypeDecorator):
    impl = Unicode
    
    def bind_processor(self, dialect):
        impl_processor = super(MyUnicodeType, self).bind_processor(dialect)
        def process(value):
            return "UNI_BIND_IN"+ impl_processor(value)
        return process
        
    def result_processor(self, dialect):
        impl_processor = super(MyUnicodeType, self).result_processor(dialect)
        def process(value):
            return impl_processor(value) + "UNI_BIND_OUT"
        return process

    def copy(self):
        return MyUnicodeType(self.impl.length)

class LegacyType(types.TypeEngine):
    def get_col_spec(self):
        return "VARCHAR(100)"
    def convert_bind_param(self, value, dialect):
        return "BIND_IN"+ value
    def convert_result_value(self, value, dialect):
        return value + "BIND_OUT"
    def adapt(self, typeobj):
        return typeobj()

class LegacyUnicodeType(types.TypeDecorator):
    impl = Unicode

    def convert_bind_param(self, value, dialect):
        return "UNI_BIND_IN" + super(LegacyUnicodeType, self).convert_bind_param(value, dialect)

    def convert_result_value(self, value, dialect):
        return super(LegacyUnicodeType, self).convert_result_value(value, dialect) + "UNI_BIND_OUT"

    def copy(self):
        return LegacyUnicodeType(self.impl.length)

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
        
        
class UserDefinedTest(PersistTest):
    """tests user-defined types."""

    def testbasic(self):
        print users.c.goofy4.type
        print users.c.goofy4.type.dialect_impl(testbase.db.dialect)
        print users.c.goofy4.type.dialect_impl(testbase.db.dialect).get_col_spec()
        
    def testprocessing(self):

        global users
        users.insert().execute(user_id = 2, goofy = 'jack', goofy2='jack', goofy3='jack', goofy4='jack', goofy5='jack', goofy6='jack')
        users.insert().execute(user_id = 3, goofy = 'lala', goofy2='lala', goofy3='lala', goofy4='lala', goofy5='lala', goofy6='lala')
        users.insert().execute(user_id = 4, goofy = 'fred', goofy2='fred', goofy3='fred', goofy4='fred', goofy5='fred', goofy6='fred')
        
        l = users.select().execute().fetchall()
        assert l == [
            (2, 'BIND_INjackBIND_OUT', 'BIND_INjackBIND_OUT', 'BIND_INjackBIND_OUT', u'UNI_BIND_INjackUNI_BIND_OUT', u'UNI_BIND_INjackUNI_BIND_OUT', 'BIND_INjackBIND_OUT'), 
            (3, 'BIND_INlalaBIND_OUT', 'BIND_INlalaBIND_OUT', 'BIND_INlalaBIND_OUT', u'UNI_BIND_INlalaUNI_BIND_OUT', u'UNI_BIND_INlalaUNI_BIND_OUT', 'BIND_INlalaBIND_OUT'),
            (4, 'BIND_INfredBIND_OUT', 'BIND_INfredBIND_OUT', 'BIND_INfredBIND_OUT', u'UNI_BIND_INfredUNI_BIND_OUT', u'UNI_BIND_INfredUNI_BIND_OUT', 'BIND_INfredBIND_OUT')
        ]

    def setUpAll(self):
        global users, metadata
        metadata = MetaData(testbase.db)
        users = Table('type_users', metadata,
            Column('user_id', Integer, primary_key = True),
            # totall custom type
            Column('goofy', MyType, nullable = False),
            
            # decorated type with an argument, so its a String
            Column('goofy2', MyDecoratedType(50), nullable = False),
            
            # decorated type without an argument, it will adapt_args to TEXT
            Column('goofy3', MyDecoratedType, nullable = False),

            Column('goofy4', MyUnicodeType, nullable = False),
            Column('goofy5', LegacyUnicodeType, nullable = False),
            Column('goofy6', LegacyType, nullable = False),

        )
        
        metadata.create_all()
        
    def tearDownAll(self):
        metadata.drop_all()

class ColumnsTest(AssertMixin):

    def testcolumns(self):
        expectedResults = { 'int_column': 'int_column INTEGER',
                            'smallint_column': 'smallint_column SMALLINT',
                            'varchar_column': 'varchar_column VARCHAR(20)',
                            'numeric_column': 'numeric_column NUMERIC(12, 3)',
                            'float_column': 'float_column NUMERIC(25, 2)'
                          }

        db = testbase.db
        if not db.name=='sqlite' and not db.name=='oracle':
            expectedResults['float_column'] = 'float_column FLOAT(25)'
    
        print db.engine.__module__
        testTable = Table('testColumns', MetaData(db),
            Column('int_column', Integer),
            Column('smallint_column', SmallInteger),
            Column('varchar_column', String(20)),
            Column('numeric_column', Numeric(12,3)),
            Column('float_column', Float(25)),
        )

        for aCol in testTable.c:
            self.assertEquals(expectedResults[aCol.name], db.dialect.schemagenerator(db.dialect, db, None, None).get_column_specification(aCol))
        
class UnicodeTest(AssertMixin):
    """tests the Unicode type.  also tests the TypeDecorator with instances in the types package."""
    def setUpAll(self):
        global unicode_table
        metadata = MetaData(testbase.db)
        unicode_table = Table('unicode_table', metadata, 
            Column('id', Integer, Sequence('uni_id_seq', optional=True), primary_key=True),
            Column('unicode_varchar', Unicode(250)),
            Column('unicode_text', Unicode),
            Column('plain_varchar', String(250))
            )
        unicode_table.create()
    def tearDownAll(self):
        unicode_table.drop()
    
    def tearDown(self):
        unicode_table.delete().execute()
        
    def testbasic(self):
        assert unicode_table.c.unicode_varchar.type.length == 250
        rawdata = 'Alors vous imaginez ma surprise, au lever du jour, quand une dr\xc3\xb4le de petit voix m\xe2\x80\x99a r\xc3\xa9veill\xc3\xa9. Elle disait: \xc2\xab S\xe2\x80\x99il vous pla\xc3\xaet\xe2\x80\xa6 dessine-moi un mouton! \xc2\xbb\n'
        unicodedata = rawdata.decode('utf-8')
        unicode_table.insert().execute(unicode_varchar=unicodedata,
                                       unicode_text=unicodedata,
                                       plain_varchar=rawdata)
        x = unicode_table.select().execute().fetchone()
        print repr(x['unicode_varchar'])
        print repr(x['unicode_text'])
        print repr(x['plain_varchar'])
        self.assert_(isinstance(x['unicode_varchar'], unicode) and x['unicode_varchar'] == unicodedata)
        self.assert_(isinstance(x['unicode_text'], unicode) and x['unicode_text'] == unicodedata)
        if isinstance(x['plain_varchar'], unicode):
            # SQLLite and MSSQL return non-unicode data as unicode
            self.assert_(testbase.db.name in ('sqlite', 'mssql'))
            self.assert_(x['plain_varchar'] == unicodedata)
            print "it's %s!" % testbase.db.name
        else:
            self.assert_(not isinstance(x['plain_varchar'], unicode) and x['plain_varchar'] == rawdata)

    def testblanks(self):
        unicode_table.insert().execute(unicode_varchar=u'')
        assert select([unicode_table.c.unicode_varchar]).scalar() == u''
        
    def testengineparam(self):
        """tests engine-wide unicode conversion"""
        prev_unicode = testbase.db.engine.dialect.convert_unicode
        try:
            testbase.db.engine.dialect.convert_unicode = True
            rawdata = 'Alors vous imaginez ma surprise, au lever du jour, quand une dr\xc3\xb4le de petit voix m\xe2\x80\x99a r\xc3\xa9veill\xc3\xa9. Elle disait: \xc2\xab S\xe2\x80\x99il vous pla\xc3\xaet\xe2\x80\xa6 dessine-moi un mouton! \xc2\xbb\n'
            unicodedata = rawdata.decode('utf-8')
            unicode_table.insert().execute(unicode_varchar=unicodedata,
                                           unicode_text=unicodedata,
                                           plain_varchar=rawdata)
            x = unicode_table.select().execute().fetchone()
            print repr(x['unicode_varchar'])
            print repr(x['unicode_text'])
            print repr(x['plain_varchar'])
            self.assert_(isinstance(x['unicode_varchar'], unicode) and x['unicode_varchar'] == unicodedata)
            self.assert_(isinstance(x['unicode_text'], unicode) and x['unicode_text'] == unicodedata)
            self.assert_(isinstance(x['plain_varchar'], unicode) and x['plain_varchar'] == unicodedata)
        finally:
            testbase.db.engine.dialect.convert_unicode = prev_unicode

    @testing.unsupported('oracle')
    def testlength(self):
        """checks the database correctly understands the length of a unicode string"""
        teststr = u'aaa\x1234'
        self.assert_(testbase.db.func.length(teststr).scalar() == len(teststr))
  
class BinaryTest(AssertMixin):
    def setUpAll(self):
        global binary_table
        binary_table = Table('binary_table', MetaData(testbase.db), 
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
        
        for stmt in (
            binary_table.select(order_by=binary_table.c.primary_id),
            text("select * from binary_table order by binary_table.primary_id", typemap={'pickled':PickleType}, bind=testbase.db)
        ):
            l = stmt.execute().fetchall()
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
    
    
class DateTest(AssertMixin):
    def setUpAll(self):
        global users_with_date, insert_data

        db = testbase.db
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
        elif db.engine.name == 'mysql':
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

            if db.engine.name == 'mssql':
                # MSSQL can't reliably fetch the millisecond part
                insert_data[2] = [9, 'foo', datetime.datetime(2005, 11, 10, 11, 52, 35), datetime.date(1970,4,1), datetime.time(23,59,59)]
            
            fnames = ['user_id', 'user_name', 'user_datetime', 'user_date', 'user_time']

            collist = [Column('user_id', INT, primary_key = True), Column('user_name', VARCHAR(20)), Column('user_datetime', DateTime(timezone=False)),
                           Column('user_date', Date), Column('user_time', Time)]
 
        users_with_date = Table('query_users_with_date',
                                MetaData(testbase.db), *collist)
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

    @testing.supported('sqlite')
    def test_sqlite_date(self):
        meta = MetaData(testbase.db)
        t = Table('testdate', meta,
                  Column('id', Integer, primary_key=True),
                Column('adate', Date), 
                Column('adatetime', DateTime))
        t.create(checkfirst=True)
        try:
            d1 = datetime.date(2007, 10, 30)
            d2 = datetime.datetime(2007, 10, 30)

            t.insert().execute(adate=str(d1), adatetime=str(d2))
            
            assert t.select().execute().fetchall()[0] == (1, datetime.date(2007, 10, 30), datetime.datetime(2007, 10, 30))
            
        finally:
            t.drop(checkfirst=True)
        
        
    def testtextdate(self):     
        x = testbase.db.text("select user_datetime from query_users_with_date", typemap={'user_datetime':DateTime}).execute().fetchall()
        
        print repr(x)
        self.assert_(isinstance(x[0][0], datetime.datetime))
        
        #x = db.text("select * from query_users_with_date where user_datetime=:date", bindparams=[bindparam('date', )]).execute(date=datetime.datetime(2005, 11, 10, 11, 52, 35)).fetchall()
        #print repr(x)

    def testdate2(self):
        meta = MetaData(testbase.db)
        t = Table('testdate', meta,
                  Column('id', Integer,
                         Sequence('datetest_id_seq', optional=True),
                         primary_key=True),
                Column('adate', Date), Column('adatetime', DateTime))
        t.create(checkfirst=True)
        try:
            d1 = datetime.date(2007, 10, 30)
            t.insert().execute(adate=d1, adatetime=d1)
            d2 = datetime.datetime(2007, 10, 30)
            t.insert().execute(adate=d2, adatetime=d2)

            x = t.select().execute().fetchall()[0]
            self.assert_(x.adate.__class__ == datetime.date)
            self.assert_(x.adatetime.__class__ == datetime.datetime)

        finally:
            t.drop(checkfirst=True)

class NumericTest(AssertMixin):
    def setUpAll(self):
        global numeric_table, metadata
        metadata = MetaData(testbase.db)
        numeric_table = Table('numeric_table', metadata,
            Column('id', Integer, Sequence('numeric_id_seq', optional=True), primary_key=True),
            Column('numericcol', Numeric(asdecimal=False)),
            Column('floatcol', Float),
            Column('ncasdec', Numeric),
            Column('fcasdec', Float(asdecimal=True))
        )
        metadata.create_all()
        
    def tearDownAll(self):
        metadata.drop_all()
        
    def tearDown(self):
        numeric_table.delete().execute()
        
    def test_decimal(self):
        from decimal import Decimal
        numeric_table.insert().execute(numericcol=3.5, floatcol=5.6, ncasdec=12.4, fcasdec=15.78)
        numeric_table.insert().execute(numericcol=Decimal("3.5"), floatcol=Decimal("5.6"), ncasdec=Decimal("12.4"), fcasdec=Decimal("15.78"))
        l = numeric_table.select().execute().fetchall()
        print l
        rounded = [
            (l[0][0], l[0][1], round(l[0][2], 5), l[0][3], l[0][4]),
            (l[1][0], l[1][1], round(l[1][2], 5), l[1][3], l[1][4]),
        ]
        assert rounded == [
            (1, 3.5, 5.6, Decimal("12.4"), Decimal("15.78")),
            (2, 3.5, 5.6, Decimal("12.4"), Decimal("15.78")),
        ]
        
            
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
