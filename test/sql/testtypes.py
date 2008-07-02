import testenv; testenv.configure_for_tests()
import datetime, os, pickleable, re
from sqlalchemy import *
from sqlalchemy import exceptions, types, util
from sqlalchemy.sql import operators
import sqlalchemy.engine.url as url
from sqlalchemy.databases import mssql, oracle, mysql, postgres, firebird
from testlib import *



class AdaptTest(TestBase):
    def testadapt(self):
        e1 = url.URL('postgres').get_dialect()()
        e2 = url.URL('mysql').get_dialect()()
        e3 = url.URL('sqlite').get_dialect()()
        e4 = url.URL('firebird').get_dialect()()

        type = String(40)

        t1 = type.dialect_impl(e1)
        t2 = type.dialect_impl(e2)
        t3 = type.dialect_impl(e3)
        t4 = type.dialect_impl(e4)

        impls = [t1, t2, t3, t4]
        for i,ta in enumerate(impls):
            for j,tb in enumerate(impls):
                if i == j:
                    assert ta == tb  # call me paranoid...  :)
                else:
                    assert ta != tb

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
        class MyDecoratedType(types.TypeDecorator):
            impl = String
            def copy(self):
                return MyDecoratedType()

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

    def teststringadapt(self):
        """test that String with no size becomes TEXT, *all* others stay as varchar/String"""

        oracle_dialect = oracle.OracleDialect()
        mysql_dialect = mysql.MySQLDialect()
        postgres_dialect = postgres.PGDialect()
        firebird_dialect = firebird.FBDialect()

        for dialect, start, test in [
            (oracle_dialect, String(), oracle.OracleText),
            (oracle_dialect, VARCHAR(), oracle.OracleString),
            (oracle_dialect, String(50), oracle.OracleString),
            (oracle_dialect, Unicode(), oracle.OracleText),
            (oracle_dialect, UnicodeText(), oracle.OracleText),
            (oracle_dialect, NCHAR(), oracle.OracleString),
            (oracle_dialect, oracle.OracleRaw(50), oracle.OracleRaw),
            (mysql_dialect, String(), mysql.MSText),
            (mysql_dialect, VARCHAR(), mysql.MSString),
            (mysql_dialect, String(50), mysql.MSString),
            (mysql_dialect, Unicode(), mysql.MSText),
            (mysql_dialect, UnicodeText(), mysql.MSText),
            (mysql_dialect, NCHAR(), mysql.MSNChar),
            (postgres_dialect, String(), postgres.PGText),
            (postgres_dialect, VARCHAR(), postgres.PGString),
            (postgres_dialect, String(50), postgres.PGString),
            (postgres_dialect, Unicode(), postgres.PGText),
            (postgres_dialect, UnicodeText(), postgres.PGText),
            (postgres_dialect, NCHAR(), postgres.PGString),
            (firebird_dialect, String(), firebird.FBText),
            (firebird_dialect, VARCHAR(), firebird.FBString),
            (firebird_dialect, String(50), firebird.FBString),
            (firebird_dialect, Unicode(), firebird.FBText),
            (firebird_dialect, UnicodeText(), firebird.FBText),
            (firebird_dialect, NCHAR(), firebird.FBString),
        ]:
            assert isinstance(start.dialect_impl(dialect), test), "wanted %r got %r" % (test, start.dialect_impl(dialect))



class UserDefinedTest(TestBase):
    """tests user-defined types."""

    def testbasic(self):
        print users.c.goofy4.type
        print users.c.goofy4.type.dialect_impl(testing.db.dialect)
        print users.c.goofy4.type.dialect_impl(testing.db.dialect).get_col_spec()

    def testprocessing(self):

        global users
        users.insert().execute(user_id = 2, goofy = 'jack', goofy2='jack', goofy3='jack', goofy4=u'jack', goofy5=u'jack', goofy6='jack', goofy7=u'jack', goofy8=12, goofy9=12)
        users.insert().execute(user_id = 3, goofy = 'lala', goofy2='lala', goofy3='lala', goofy4=u'lala', goofy5=u'lala', goofy6='lala', goofy7=u'lala', goofy8=15, goofy9=15)
        users.insert().execute(user_id = 4, goofy = 'fred', goofy2='fred', goofy3='fred', goofy4=u'fred', goofy5=u'fred', goofy6='fred', goofy7=u'fred', goofy8=9, goofy9=9)

        l = users.select().execute().fetchall()
        for assertstr, assertint, assertint2, row in zip(
            ["BIND_INjackBIND_OUT", "BIND_INlalaBIND_OUT", "BIND_INfredBIND_OUT"],
            [1200, 1500, 900],
            [1800, 2250, 1350],
            l

        ):
            for col in row[1:8]:
                self.assertEquals(col, assertstr)
            self.assertEquals(row[8], assertint)
            self.assertEquals(row[9], assertint2)
            for col in (row[4], row[5], row[7]):
                assert isinstance(col, unicode)

    def setUpAll(self):
        global users, metadata

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

        class MyNewUnicodeType(types.TypeDecorator):
            impl = Unicode

            def process_bind_param(self, value, dialect):
                return "BIND_IN" + value

            def process_result_value(self, value, dialect):
                return value + "BIND_OUT"

            def copy(self):
                return MyNewUnicodeType(self.impl.length)

        class MyNewIntType(types.TypeDecorator):
            impl = Integer

            def process_bind_param(self, value, dialect):
                return value * 10

            def process_result_value(self, value, dialect):
                return value * 10

            def copy(self):
                return MyNewIntType()

        class MyNewIntSubClass(MyNewIntType):
            def process_result_value(self, value, dialect):
                return value * 15

            def copy(self):
                return MyNewIntSubClass()

        class MyUnicodeType(types.TypeDecorator):
            impl = Unicode

            def bind_processor(self, dialect):
                impl_processor = super(MyUnicodeType, self).bind_processor(dialect) or (lambda value:value)

                def process(value):
                    return "BIND_IN"+ impl_processor(value)
                return process

            def result_processor(self, dialect):
                impl_processor = super(MyUnicodeType, self).result_processor(dialect) or (lambda value:value)
                def process(value):
                    return impl_processor(value) + "BIND_OUT"
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
                return "BIND_IN" + super(LegacyUnicodeType, self).convert_bind_param(value, dialect)

            def convert_result_value(self, value, dialect):
                return super(LegacyUnicodeType, self).convert_result_value(value, dialect) + "BIND_OUT"

            def copy(self):
                return LegacyUnicodeType(self.impl.length)

        metadata = MetaData(testing.db)
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
            Column('goofy7', MyNewUnicodeType, nullable = False),
            Column('goofy8', MyNewIntType, nullable = False),
            Column('goofy9', MyNewIntSubClass, nullable = False),

        )

        metadata.create_all()

    def tearDownAll(self):
        metadata.drop_all()

class ColumnsTest(TestBase, AssertsExecutionResults):

    def testcolumns(self):
        expectedResults = { 'int_column': 'int_column INTEGER',
                            'smallint_column': 'smallint_column SMALLINT',
                            'varchar_column': 'varchar_column VARCHAR(20)',
                            'numeric_column': 'numeric_column NUMERIC(12, 3)',
                            'float_column': 'float_column FLOAT(25)',
                          }

        db = testing.db
        if testing.against('oracle'):
            expectedResults['float_column'] = 'float_column NUMERIC(25, 2)'

        if testing.against('sqlite'):
            expectedResults['float_column'] = 'float_column FLOAT'
            
        if testing.against('maxdb'):
            expectedResults['numeric_column'] = (
                expectedResults['numeric_column'].replace('NUMERIC', 'FIXED'))

        print db.engine.__module__
        testTable = Table('testColumns', MetaData(db),
            Column('int_column', Integer),
            Column('smallint_column', SmallInteger),
            Column('varchar_column', String(20)),
            Column('numeric_column', Numeric(12,3)),
            Column('float_column', Float(25)),
        )

        for aCol in testTable.c:
            self.assertEquals(
                expectedResults[aCol.name],
                db.dialect.schemagenerator(db.dialect, db, None, None).\
                  get_column_specification(aCol))

class UnicodeTest(TestBase, AssertsExecutionResults):
    """tests the Unicode type.  also tests the TypeDecorator with instances in the types package."""
    def setUpAll(self):
        global unicode_table
        metadata = MetaData(testing.db)
        unicode_table = Table('unicode_table', metadata,
            Column('id', Integer, Sequence('uni_id_seq', optional=True), primary_key=True),
            Column('unicode_varchar', Unicode(250)),
            Column('unicode_text', UnicodeText),
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
        print 0, repr(unicodedata)
        print 1, repr(x['unicode_varchar'])
        print 2, repr(x['unicode_text'])
        print 3, repr(x['plain_varchar'])
        self.assert_(isinstance(x['unicode_varchar'], unicode) and x['unicode_varchar'] == unicodedata)
        self.assert_(isinstance(x['unicode_text'], unicode) and x['unicode_text'] == unicodedata)
        if isinstance(x['plain_varchar'], unicode):
            # SQLLite and MSSQL return non-unicode data as unicode
            self.assert_(testing.against('sqlite', 'mssql'))
            self.assert_(x['plain_varchar'] == unicodedata)
            print "it's %s!" % testing.db.name
        else:
            self.assert_(not isinstance(x['plain_varchar'], unicode) and x['plain_varchar'] == rawdata)

    def testassert(self):
        try:
            unicode_table.insert().execute(unicode_varchar='not unicode')
            assert False
        except exceptions.SAWarning, e:
            assert str(e) == "Unicode type received non-unicode bind param value 'not unicode'", str(e)

        unicode_engine = engines.utf8_engine(options={'convert_unicode':True,
                                                      'assert_unicode':True})
        try:
            try:
                unicode_engine.execute(unicode_table.insert(), plain_varchar='im not unicode')
                assert False
            except exceptions.InvalidRequestError, e:
                assert str(e) == "Unicode type received non-unicode bind param value 'im not unicode'"

            @testing.emits_warning('.*non-unicode bind')
            def warns():
                # test that data still goes in if warning is emitted....
                unicode_table.insert().execute(unicode_varchar='not unicode')
                assert (select([unicode_table.c.unicode_varchar]).execute().fetchall() == [('not unicode', )])
            warns()

        finally:
            unicode_engine.dispose()

    @testing.fails_on('oracle')
    def testblanks(self):
        unicode_table.insert().execute(unicode_varchar=u'')
        assert select([unicode_table.c.unicode_varchar]).scalar() == u''

    def testengineparam(self):
        """tests engine-wide unicode conversion"""
        prev_unicode = testing.db.engine.dialect.convert_unicode
        prev_assert = testing.db.engine.dialect.assert_unicode
        try:
            testing.db.engine.dialect.convert_unicode = True
            testing.db.engine.dialect.assert_unicode = False
            rawdata = 'Alors vous imaginez ma surprise, au lever du jour, quand une dr\xc3\xb4le de petit voix m\xe2\x80\x99a r\xc3\xa9veill\xc3\xa9. Elle disait: \xc2\xab S\xe2\x80\x99il vous pla\xc3\xaet\xe2\x80\xa6 dessine-moi un mouton! \xc2\xbb\n'
            unicodedata = rawdata.decode('utf-8')
            unicode_table.insert().execute(unicode_varchar=unicodedata,
                                           unicode_text=unicodedata,
                                           plain_varchar=rawdata)
            x = unicode_table.select().execute().fetchone()
            print 0, repr(unicodedata)
            print 1, repr(x['unicode_varchar'])
            print 2, repr(x['unicode_text'])
            print 3, repr(x['plain_varchar'])
            self.assert_(isinstance(x['unicode_varchar'], unicode) and x['unicode_varchar'] == unicodedata)
            self.assert_(isinstance(x['unicode_text'], unicode) and x['unicode_text'] == unicodedata)
            self.assert_(isinstance(x['plain_varchar'], unicode) and x['plain_varchar'] == unicodedata)
        finally:
            testing.db.engine.dialect.convert_unicode = prev_unicode
            testing.db.engine.dialect.convert_unicode = prev_assert

    @testing.unsupported('oracle')
    def testlength(self):
        """checks the database correctly understands the length of a unicode string"""
        teststr = u'aaa\x1234'
        self.assert_(testing.db.func.length(teststr).scalar() == len(teststr))

class BinaryTest(TestBase, AssertsExecutionResults):
    def setUpAll(self):
        global binary_table, MyPickleType

        class MyPickleType(types.TypeDecorator):
            impl = PickleType

            def process_bind_param(self, value, dialect):
                if value:
                    value.stuff = 'this is modified stuff'
                return value

            def process_result_value(self, value, dialect):
                if value:
                    value.stuff = 'this is the right stuff'
                return value

        binary_table = Table('binary_table', MetaData(testing.db),
        Column('primary_id', Integer, Sequence('binary_id_seq', optional=True), primary_key=True),
        Column('data', Binary),
        Column('data_slice', Binary(100)),
        Column('misc', String(30)),
        # construct PickleType with non-native pickle module, since cPickle uses relative module
        # loading and confuses this test's parent package 'sql' with the 'sqlalchemy.sql' package relative
        # to the 'types' module
        Column('pickled', PickleType),
        Column('mypickle', MyPickleType)
        )
        binary_table.create()

    def tearDown(self):
        binary_table.delete().execute()

    def tearDownAll(self):
        binary_table.drop()

    def testbinary(self):
        testobj1 = pickleable.Foo('im foo 1')
        testobj2 = pickleable.Foo('im foo 2')
        testobj3 = pickleable.Foo('im foo 3')

        stream1 =self.load_stream('binary_data_one.dat')
        stream2 =self.load_stream('binary_data_two.dat')
        binary_table.insert().execute(primary_id=1, misc='binary_data_one.dat',    data=stream1, data_slice=stream1[0:100], pickled=testobj1, mypickle=testobj3)
        binary_table.insert().execute(primary_id=2, misc='binary_data_two.dat', data=stream2, data_slice=stream2[0:99], pickled=testobj2)
        binary_table.insert().execute(primary_id=3, misc='binary_data_two.dat', data=None, data_slice=stream2[0:99], pickled=None)

        for stmt in (
            binary_table.select(order_by=binary_table.c.primary_id),
            text("select * from binary_table order by binary_table.primary_id", typemap={'pickled':PickleType, 'mypickle':MyPickleType}, bind=testing.db)
        ):
            l = stmt.execute().fetchall()
            print type(stream1), type(l[0]['data']), type(l[0]['data_slice'])
            print len(stream1), len(l[0]['data']), len(l[0]['data_slice'])
            self.assertEquals(list(stream1), list(l[0]['data']))
            self.assertEquals(list(stream1[0:100]), list(l[0]['data_slice']))
            self.assertEquals(list(stream2), list(l[1]['data']))
            self.assertEquals(testobj1, l[0]['pickled'])
            self.assertEquals(testobj2, l[1]['pickled'])
            self.assertEquals(testobj3.moredata, l[0]['mypickle'].moredata)
            self.assertEquals(l[0]['mypickle'].stuff, 'this is the right stuff')

    def load_stream(self, name, len=12579):
        f = os.path.join(os.path.dirname(testenv.__file__), name)
        # put a number less than the typical MySQL default BLOB size
        return file(f).read(len)

class ExpressionTest(TestBase, AssertsExecutionResults):
    def setUpAll(self):
        global test_table, meta

        class MyCustomType(types.TypeEngine):
            def get_col_spec(self):
                return "INT"
            def bind_processor(self, dialect):
                def process(value):
                    return value * 10
                return process
            def result_processor(self, dialect):
                def process(value):
                    return value / 10
                return process
            def adapt_operator(self, op):
                return {operators.add:operators.sub, operators.sub:operators.add}.get(op, op)

        meta = MetaData(testing.db)
        test_table = Table('test', meta,
            Column('id', Integer, primary_key=True),
            Column('data', String(30)),
            Column('atimestamp', Date),
            Column('avalue', MyCustomType))

        meta.create_all()

        test_table.insert().execute({'id':1, 'data':'somedata', 'atimestamp':datetime.date(2007, 10, 15), 'avalue':25})

    def tearDownAll(self):
        meta.drop_all()

    def test_control(self):
        assert testing.db.execute("select avalue from test").scalar() == 250

        assert test_table.select().execute().fetchall() == [(1, 'somedata', datetime.date(2007, 10, 15), 25)]

    def test_bind_adapt(self):
        expr = test_table.c.atimestamp == bindparam("thedate")
        assert expr.right.type.__class__ == test_table.c.atimestamp.type.__class__

        assert testing.db.execute(test_table.select().where(expr), {"thedate":datetime.date(2007, 10, 15)}).fetchall() == [(1, 'somedata', datetime.date(2007, 10, 15), 25)]

        expr = test_table.c.avalue == bindparam("somevalue")
        assert expr.right.type.__class__ == test_table.c.avalue.type.__class__
        assert testing.db.execute(test_table.select().where(expr), {"somevalue":25}).fetchall() == [(1, 'somedata', datetime.date(2007, 10, 15), 25)]


    def test_operator_adapt(self):
        """test type-based overloading of operators"""

        # test string concatenation
        expr = test_table.c.data + "somedata"
        assert testing.db.execute(select([expr])).scalar() == "somedatasomedata"

        expr = test_table.c.id + 15
        assert testing.db.execute(select([expr])).scalar() == 16

        # test custom operator conversion
        expr = test_table.c.avalue + 40
        assert expr.type.__class__ is test_table.c.avalue.type.__class__

        # + operator converted to -
        # value is calculated as: (250 - (40 * 10)) / 10 == -15
        assert testing.db.execute(select([expr.label('foo')])).scalar() == -15

        # this one relies upon anonymous labeling to assemble result
        # processing rules on the column.
        assert testing.db.execute(select([expr])).scalar() == -15

class DateTest(TestBase, AssertsExecutionResults):
    def setUpAll(self):
        global users_with_date, insert_data

        db = testing.db
        if testing.against('oracle'):
            import sqlalchemy.databases.oracle as oracle
            insert_data =  [
                    [7, 'jack',
                     datetime.datetime(2005, 11, 10, 0, 0),
                     datetime.date(2005,11,10),
                     datetime.datetime(2005, 11, 10, 0, 0, 0, 29384)],
                    [8, 'roy',
                     datetime.datetime(2005, 11, 10, 11, 52, 35),
                     datetime.date(2005,10,10),
                     datetime.datetime(2006, 5, 10, 15, 32, 47, 6754)],
                    [9, 'foo',
                     datetime.datetime(2006, 11, 10, 11, 52, 35),
                     datetime.date(1970,4,1),
                     datetime.datetime(2004, 9, 18, 4, 0, 52, 1043)],
                    [10, 'colber', None, None, None]
             ]
            fnames = ['user_id', 'user_name', 'user_datetime',
                      'user_date', 'user_time']

            collist = [Column('user_id', INT, primary_key=True),
                       Column('user_name', VARCHAR(20)),
                       Column('user_datetime', DateTime),
                       Column('user_date', Date),
                       Column('user_time', TIMESTAMP)]
        else:
            datetime_micro = 54839
            time_micro = 999

            # Missing or poor microsecond support:
            if testing.against('mssql', 'mysql', 'firebird'):
                datetime_micro, time_micro = 0, 0
            # No microseconds for TIME
            elif testing.against('maxdb'):
                time_micro = 0

            insert_data =  [
                [7, 'jack',
                 datetime.datetime(2005, 11, 10, 0, 0),
                 datetime.date(2005, 11, 10),
                 datetime.time(12, 20, 2)],
                [8, 'roy',
                 datetime.datetime(2005, 11, 10, 11, 52, 35),
                 datetime.date(2005, 10, 10),
                 datetime.time(0, 0, 0)],
                [9, 'foo',
                 datetime.datetime(2005, 11, 10, 11, 52, 35, datetime_micro),
                 datetime.date(1970, 4, 1),
                 datetime.time(23, 59, 59, time_micro)],
                [10, 'colber', None, None, None]
            ]
            fnames = ['user_id', 'user_name', 'user_datetime',
                      'user_date', 'user_time']

            collist = [Column('user_id', INT, primary_key=True),
                       Column('user_name', VARCHAR(20)),
                       Column('user_datetime', DateTime(timezone=False)),
                       Column('user_date', Date),
                       Column('user_time', Time)]

        users_with_date = Table('query_users_with_date',
                                MetaData(testing.db), *collist)
        users_with_date.create()
        insert_dicts = [dict(zip(fnames, d)) for d in insert_data]

        for idict in insert_dicts:
            users_with_date.insert().execute(**idict)

    def tearDownAll(self):
        users_with_date.drop()

    def testdate(self):
        global insert_data

        l = map(list, users_with_date.select().execute().fetchall())
        self.assert_(l == insert_data,
                     'DateTest mismatch: got:%s expected:%s' % (l, insert_data))

    def testtextdate(self):
        x = testing.db.text(
            "select user_datetime from query_users_with_date",
            typemap={'user_datetime':DateTime}).execute().fetchall()

        print repr(x)
        self.assert_(isinstance(x[0][0], datetime.datetime))

        x = testing.db.text(
            "select * from query_users_with_date where user_datetime=:somedate",
            bindparams=[bindparam('somedate', type_=types.DateTime)]).execute(
            somedate=datetime.datetime(2005, 11, 10, 11, 52, 35)).fetchall()
        print repr(x)

    def testdate2(self):
        meta = MetaData(testing.db)
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

            t.delete().execute()

            # test mismatched date/datetime
            t.insert().execute(adate=d2, adatetime=d2)
            self.assertEquals(select([t.c.adate, t.c.adatetime], t.c.adate==d1).execute().fetchall(), [(d1, d2)])
            self.assertEquals(select([t.c.adate, t.c.adatetime], t.c.adate==d1).execute().fetchall(), [(d1, d2)])

        finally:
            t.drop(checkfirst=True)

class StringTest(TestBase, AssertsExecutionResults):
    def test_nolen_string_deprecated(self):
        metadata = MetaData(testing.db)
        foo =Table('foo', metadata,
            Column('one', String))

        # no warning
        select([func.count("*")], bind=testing.db).execute()

        try:
            # warning during CREATE
            foo.create()
            assert False
        except exceptions.SADeprecationWarning, e:
            assert "Using String type with no length" in str(e)
            assert re.search(r'\bone\b', str(e))

        bar = Table('bar', metadata, Column('one', String(40)))

        try:
            # no warning
            bar.create()

            # no warning for non-lengthed string
            select([func.count("*")], from_obj=bar).execute()
        finally:
            bar.drop()

def _missing_decimal():
    """Python implementation supports decimals"""
    try:
        import decimal
        return False
    except ImportError:
        return True

class NumericTest(TestBase, AssertsExecutionResults):
    def setUpAll(self):
        global numeric_table, metadata
        metadata = MetaData(testing.db)
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

    @testing.fails_if(_missing_decimal)
    def test_decimal(self):
        from decimal import Decimal
        numeric_table.insert().execute(
            numericcol=3.5, floatcol=5.6, ncasdec=12.4, fcasdec=15.75)
        numeric_table.insert().execute(
            numericcol=Decimal("3.5"), floatcol=Decimal("5.6"),
            ncasdec=Decimal("12.4"), fcasdec=Decimal("15.75"))

        l = numeric_table.select().execute().fetchall()
        print l
        rounded = [
            (l[0][0], l[0][1], round(l[0][2], 5), l[0][3], l[0][4]),
            (l[1][0], l[1][1], round(l[1][2], 5), l[1][3], l[1][4]),
        ]
        assert rounded == [
            (1, 3.5, 5.6, Decimal("12.4"), Decimal("15.75")),
            (2, 3.5, 5.6, Decimal("12.4"), Decimal("15.75")),
        ]

    @testing.emits_warning('True Decimal types not available')
    def test_decimal_fallback(self):
        from sqlalchemy.util import Decimal  # could be Decimal or float

        numeric_table.insert().execute(ncasdec=12.4, fcasdec=15.75)
        numeric_table.insert().execute(ncasdec=Decimal("12.4"),
                                       fcasdec=Decimal("15.75"))

        for row in numeric_table.select().execute().fetchall():
            assert isinstance(row['ncasdec'], util.decimal_type)
            assert isinstance(row['fcasdec'], util.decimal_type)


class IntervalTest(TestBase, AssertsExecutionResults):
    def setUpAll(self):
        global interval_table, metadata
        metadata = MetaData(testing.db)
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

class BooleanTest(TestBase, AssertsExecutionResults):
    def setUpAll(self):
        global bool_table
        metadata = MetaData(testing.db)
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
    testenv.main()
