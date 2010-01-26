# coding: utf-8
from sqlalchemy.test.testing import eq_, assert_raises, assert_raises_message
import decimal
import datetime, os, re
from sqlalchemy import *
from sqlalchemy import exc, types, util, schema
from sqlalchemy.sql import operators
from sqlalchemy.test.testing import eq_
import sqlalchemy.engine.url as url
from sqlalchemy.databases import *
from sqlalchemy.test.schema import Table, Column
from sqlalchemy.test import *


class AdaptTest(TestBase):
    def test_uppercase_rendering(self):
        """Test that uppercase types from types.py always render as their type.
        
        As of SQLA 0.6, using an uppercase type means you want specifically that
        type.  If the database in use doesn't support that DDL, it (the DB backend) 
        should raise an error - it means you should be using a lowercased (genericized) type.
        
        """
        
        for dialect in [
                oracle.dialect(), 
                mysql.dialect(), 
                postgresql.dialect(), 
                sqlite.dialect(), 
                mssql.dialect()]: # TODO when dialects are complete:  engines.all_dialects():
            for type_, expected in (
                (FLOAT, "FLOAT"),
                (NUMERIC, "NUMERIC"),
                (DECIMAL, "DECIMAL"),
                (INTEGER, "INTEGER"),
                (SMALLINT, "SMALLINT"),
                (TIMESTAMP, "TIMESTAMP"),
                (DATETIME, "DATETIME"),
                (DATE, "DATE"),
                (TIME, "TIME"),
                (CLOB, "CLOB"),
                (VARCHAR(10), "VARCHAR(10)"),
                (NVARCHAR(10), ("NVARCHAR(10)", "NATIONAL VARCHAR(10)", "NVARCHAR2(10)")),
                (CHAR, "CHAR"),
                (NCHAR, ("NCHAR", "NATIONAL CHAR")),
                (BLOB, "BLOB"),
                (BOOLEAN, ("BOOLEAN", "BOOL"))
            ):
                if isinstance(expected, str):
                    expected = (expected, )
                for exp in expected:
                    compiled = types.to_instance(type_).compile(dialect=dialect)
                    if exp in compiled:
                        break
                else:
                    assert False, "%r matches none of %r for dialect %s" % (compiled, expected, dialect.name)
            
class TypeAffinityTest(TestBase):
    def test_type_affinity(self):
        for t1, t2, comp in [
            (Integer(), SmallInteger(), True),
            (Integer(), String(), False),
            (Integer(), Integer(), True),
            (Text(), String(), True),
            (Text(), Unicode(), True),
            (LargeBinary(), Integer(), False),
            (LargeBinary(), PickleType(), True),
            (PickleType(), LargeBinary(), True),
            (PickleType(), PickleType(), True),
        ]:
            eq_(t1._compare_type_affinity(t2), comp, "%s %s" % (t1, t2))
        
    
class UserDefinedTest(TestBase):
    """tests user-defined types."""

    def testprocessing(self):

        global users
        users.insert().execute(
            user_id=2, goofy='jack', goofy2='jack', goofy4=u'jack',
            goofy7=u'jack', goofy8=12, goofy9=12)
        users.insert().execute(
            user_id=3, goofy='lala', goofy2='lala', goofy4=u'lala',
            goofy7=u'lala', goofy8=15, goofy9=15)
        users.insert().execute(
            user_id=4, goofy='fred', goofy2='fred', goofy4=u'fred',
            goofy7=u'fred', goofy8=9, goofy9=9)

        l = users.select().order_by(users.c.user_id).execute().fetchall()
        for assertstr, assertint, assertint2, row in zip(
            ["BIND_INjackBIND_OUT", "BIND_INlalaBIND_OUT", "BIND_INfredBIND_OUT"],
            [1200, 1500, 900],
            [1800, 2250, 1350],
            l
        ):
            for col in row[1:5]:
                eq_(col, assertstr)
            eq_(row[5], assertint)
            eq_(row[6], assertint2)
            for col in row[3], row[4]:
                assert isinstance(col, unicode)

    @classmethod
    def setup_class(cls):
        global users, metadata

        class MyType(types.UserDefinedType):
            def get_col_spec(self):
                return "VARCHAR(100)"
            def bind_processor(self, dialect):
                def process(value):
                    return "BIND_IN"+ value
                return process
            def result_processor(self, dialect, coltype):
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
            def result_processor(self, dialect, coltype):
                impl_processor = super(MyDecoratedType, self).result_processor(dialect, coltype) or (lambda value:value)
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

            def result_processor(self, dialect, coltype):
                impl_processor = super(MyUnicodeType, self).result_processor(dialect, coltype) or (lambda value:value)
                def process(value):
                    return impl_processor(value) + "BIND_OUT"
                return process

            def copy(self):
                return MyUnicodeType(self.impl.length)

        metadata = MetaData(testing.db)
        users = Table('type_users', metadata,
            Column('user_id', Integer, primary_key = True),
            # totall custom type
            Column('goofy', MyType, nullable = False),

            # decorated type with an argument, so its a String
            Column('goofy2', MyDecoratedType(50), nullable = False),

            Column('goofy4', MyUnicodeType(50), nullable = False),
            Column('goofy7', MyNewUnicodeType(50), nullable = False),
            Column('goofy8', MyNewIntType, nullable = False),
            Column('goofy9', MyNewIntSubClass, nullable = False),
        )

        metadata.create_all()

    @classmethod
    def teardown_class(cls):
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
        if testing.against('oracle') or \
            testing.against('sqlite') or \
            testing.against('firebird'):
            expectedResults['float_column'] = 'float_column FLOAT'
            
        if testing.against('maxdb'):
            expectedResults['numeric_column'] = (
                expectedResults['numeric_column'].replace('NUMERIC', 'FIXED'))

        if testing.against('mssql'):
            for key, value in expectedResults.items():
                expectedResults[key] = '%s NULL' % value

        testTable = Table('testColumns', MetaData(db),
            Column('int_column', Integer),
            Column('smallint_column', SmallInteger),
            Column('varchar_column', String(20)),
            Column('numeric_column', Numeric(12,3)),
            Column('float_column', Float(25)),
        )

        for aCol in testTable.c:
            eq_(
                expectedResults[aCol.name],
                db.dialect.ddl_compiler(
                            db.dialect, schema.CreateTable(testTable)).
                            get_column_specification(aCol)
            )

class UnicodeTest(TestBase, AssertsExecutionResults):
    """tests the Unicode type.  also tests the TypeDecorator with instances in the types package."""

    @classmethod
    def setup_class(cls):
        global unicode_table, metadata
        metadata = MetaData(testing.db)
        unicode_table = Table('unicode_table', metadata,
            Column('id', Integer, Sequence('uni_id_seq', optional=True), primary_key=True),
            Column('unicode_varchar', Unicode(250)),
            Column('unicode_text', UnicodeText),
            )
        metadata.create_all()
        
    @classmethod
    def teardown_class(cls):
        metadata.drop_all()

    @engines.close_first
    def teardown(self):
        unicode_table.delete().execute()
    
    def test_native_unicode(self):
        """assert expected values for 'native unicode' mode"""
        
        assert testing.db.dialect.returns_unicode_strings == \
            ((testing.db.name, testing.db.driver) in \
            (
                ('postgresql','psycopg2'),
                ('postgresql','pg8000'),
                ('postgresql','zxjdbc'),  
                ('mysql','oursql'),
                ('mysql','zxjdbc'),
                ('sqlite','pysqlite'),
                ('oracle','zxjdbc'),
            )), \
            "name: %s driver %s returns_unicode_strings=%s" % \
                                        (testing.db.name, 
                                         testing.db.driver, 
                                         testing.db.dialect.returns_unicode_strings)
        
    def test_round_trip(self):
        unicodedata = u"Alors vous imaginez ma surprise, au lever du jour, quand une drôle de petit voix m’a réveillé. Elle disait: « S’il vous plaît… dessine-moi un mouton! »"
        
        unicode_table.insert().execute(unicode_varchar=unicodedata,unicode_text=unicodedata)
        
        x = unicode_table.select().execute().first()
        assert isinstance(x['unicode_varchar'], unicode)
        assert isinstance(x['unicode_text'], unicode)
        eq_(x['unicode_varchar'], unicodedata)
        eq_(x['unicode_text'], unicodedata)

    def test_round_trip_executemany(self):
        # cx_oracle was producing different behavior for cursor.executemany()
        # vs. cursor.execute()
        
        unicodedata = u"Alors vous imaginez ma surprise, au lever du jour, quand une drôle de petit voix m’a réveillé. Elle disait: « S’il vous plaît… dessine-moi un mouton! »"

        unicode_table.insert().execute(
                dict(unicode_varchar=unicodedata,unicode_text=unicodedata),
                dict(unicode_varchar=unicodedata,unicode_text=unicodedata)
        )

        x = unicode_table.select().execute().first()
        assert isinstance(x['unicode_varchar'], unicode)
        eq_(x['unicode_varchar'], unicodedata)
        assert isinstance(x['unicode_text'], unicode)
        eq_(x['unicode_text'], unicodedata)

    def test_union(self):
        """ensure compiler processing works for UNIONs"""

        unicodedata = u"Alors vous imaginez ma surprise, au lever du jour, quand une drôle de petit voix m’a réveillé. Elle disait: « S’il vous plaît… dessine-moi un mouton! »"

        unicode_table.insert().execute(unicode_varchar=unicodedata,unicode_text=unicodedata)
                                       
        x = union(select([unicode_table.c.unicode_varchar]), select([unicode_table.c.unicode_varchar])).execute().first()
        self.assert_(isinstance(x['unicode_varchar'], unicode) and x['unicode_varchar'] == unicodedata)

    @testing.fails_on('oracle', 'oracle converts empty strings to a blank space')
    def test_blank_strings(self):
        unicode_table.insert().execute(unicode_varchar=u'')
        assert select([unicode_table.c.unicode_varchar]).scalar() == u''

    def test_parameters(self):
        """test the dialect convert_unicode parameters."""

        unicodedata = u"Alors vous imaginez ma surprise, au lever du jour, quand une drôle de petit voix m’a réveillé. Elle disait: « S’il vous plaît… dessine-moi un mouton! »"

        u = Unicode(assert_unicode=True)
        uni = u.dialect_impl(testing.db.dialect).bind_processor(testing.db.dialect)
        # Py3K
        #assert_raises(exc.InvalidRequestError, uni, b'x')
        # Py2K
        assert_raises(exc.InvalidRequestError, uni, 'x')
        # end Py2K

        u = Unicode()
        uni = u.dialect_impl(testing.db.dialect).bind_processor(testing.db.dialect)
        # Py3K
        #assert_raises(exc.SAWarning, uni, b'x')
        # Py2K
        assert_raises(exc.SAWarning, uni, 'x')
        # end Py2K

        unicode_engine = engines.utf8_engine(options={'convert_unicode':True,'assert_unicode':True})
        unicode_engine.dialect.supports_unicode_binds = False
        
        s = String()
        uni = s.dialect_impl(unicode_engine.dialect).bind_processor(unicode_engine.dialect)
        # Py3K
        #assert_raises(exc.InvalidRequestError, uni, b'x')
        #assert isinstance(uni(unicodedata), bytes)
        # Py2K
        assert_raises(exc.InvalidRequestError, uni, 'x')
        assert isinstance(uni(unicodedata), str)
        # end Py2K
        
        assert uni(unicodedata) == unicodedata.encode('utf-8')

class EnumTest(TestBase):
    @classmethod
    def setup_class(cls):
        global enum_table, non_native_enum_table, metadata
        metadata = MetaData(testing.db)
        enum_table = Table('enum_table', metadata,
            Column("id", Integer, primary_key=True),
            Column('someenum', Enum('one','two','three', name='myenum'))
        )

        non_native_enum_table = Table('non_native_enum_table', metadata,
            Column("id", Integer, primary_key=True),
            Column('someenum', Enum('one','two','three', native_enum=False)),
        )

        metadata.create_all()
    
    def teardown(self):
        enum_table.delete().execute()
        non_native_enum_table.delete().execute()
        
    @classmethod
    def teardown_class(cls):
        metadata.drop_all()

    @testing.fails_on('postgresql+zxjdbc', 
                        'zxjdbc fails on ENUM: column "XXX" is of type XXX '
                        'but expression is of type character varying')
    @testing.fails_on('postgresql+pg8000', 
                        'zxjdbc fails on ENUM: column "XXX" is of type XXX '
                        'but expression is of type text')
    def test_round_trip(self):
        enum_table.insert().execute([
            {'id':1, 'someenum':'two'},
            {'id':2, 'someenum':'two'},
            {'id':3, 'someenum':'one'},
        ])
        
        eq_(
            enum_table.select().order_by(enum_table.c.id).execute().fetchall(), 
            [
                (1, 'two'),
                (2, 'two'),
                (3, 'one'),
            ]
        )

    def test_non_native_round_trip(self):
        non_native_enum_table.insert().execute([
            {'id':1, 'someenum':'two'},
            {'id':2, 'someenum':'two'},
            {'id':3, 'someenum':'one'},
        ])

        eq_(
            non_native_enum_table.select().
                    order_by(non_native_enum_table.c.id).execute().fetchall(), 
            [
                (1, 'two'),
                (2, 'two'),
                (3, 'one'),
            ]
        )

    @testing.fails_on('mysql+mysqldb', "MySQL seems to issue a 'data truncated' warning.")
    def test_constraint(self):
        assert_raises(exc.DBAPIError, 
            enum_table.insert().execute,
            {'id':4, 'someenum':'four'}
        )

    @testing.fails_on('mysql', "the CHECK constraint doesn't raise an exception for unknown reason")
    def test_non_native_constraint(self):
        assert_raises(exc.DBAPIError, 
            non_native_enum_table.insert().execute,
            {'id':4, 'someenum':'four'}
        )
        
class BinaryTest(TestBase, AssertsExecutionResults):
    __excluded_on__ = (
        ('mysql', '<', (4, 1, 1)),  # screwy varbinary types
        )

    @classmethod
    def setup_class(cls):
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
            Column('data', LargeBinary),
            Column('data_slice', LargeBinary(100)),
            Column('misc', String(30)),
            # construct PickleType with non-native pickle module, since cPickle uses relative module
            # loading and confuses this test's parent package 'sql' with the 'sqlalchemy.sql' package relative
            # to the 'types' module
            Column('pickled', PickleType),
            Column('mypickle', MyPickleType)
        )
        binary_table.create()

    @engines.close_first
    def teardown(self):
        binary_table.delete().execute()

    @classmethod
    def teardown_class(cls):
        binary_table.drop()

    def test_round_trip(self):
        testobj1 = pickleable.Foo('im foo 1')
        testobj2 = pickleable.Foo('im foo 2')
        testobj3 = pickleable.Foo('im foo 3')

        stream1 =self.load_stream('binary_data_one.dat')
        stream2 =self.load_stream('binary_data_two.dat')
        binary_table.insert().execute(
                            primary_id=1, 
                            misc='binary_data_one.dat', 
                            data=stream1, 
                            data_slice=stream1[0:100], 
                            pickled=testobj1, 
                            mypickle=testobj3)
        binary_table.insert().execute(
                            primary_id=2, 
                            misc='binary_data_two.dat', 
                            data=stream2, 
                            data_slice=stream2[0:99], 
                            pickled=testobj2)
        binary_table.insert().execute(
                            primary_id=3, 
                            misc='binary_data_two.dat', 
                            data=None, 
                            data_slice=stream2[0:99], 
                            pickled=None)

        for stmt in (
            binary_table.select(order_by=binary_table.c.primary_id),
            text(
                "select * from binary_table order by binary_table.primary_id", 
                typemap={'pickled':PickleType, 'mypickle':MyPickleType, 'data':LargeBinary, 'data_slice':LargeBinary}, 
                bind=testing.db)
        ):
            l = stmt.execute().fetchall()
            eq_(stream1, l[0]['data'])
            eq_(stream1[0:100], l[0]['data_slice'])
            eq_(stream2, l[1]['data'])
            eq_(testobj1, l[0]['pickled'])
            eq_(testobj2, l[1]['pickled'])
            eq_(testobj3.moredata, l[0]['mypickle'].moredata)
            eq_(l[0]['mypickle'].stuff, 'this is the right stuff')

    def load_stream(self, name):
        f = os.path.join(os.path.dirname(__file__), "..", name)
        return open(f, mode='rb').read()

class ExpressionTest(TestBase, AssertsExecutionResults):
    @classmethod
    def setup_class(cls):
        global test_table, meta

        class MyCustomType(types.UserDefinedType):
            def get_col_spec(self):
                return "INT"
            def bind_processor(self, dialect):
                def process(value):
                    return value * 10
                return process
            def result_processor(self, dialect, coltype):
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

    @classmethod
    def teardown_class(cls):
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

    @testing.fails_on('firebird', 'Data type unknown on the parameter')
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
        
    def test_distinct(self):
        s = select([distinct(test_table.c.avalue)])
        eq_(testing.db.execute(s).scalar(), 25)

        s = select([test_table.c.avalue.distinct()])
        eq_(testing.db.execute(s).scalar(), 25)

        assert distinct(test_table.c.data).type == test_table.c.data.type
        assert test_table.c.data.distinct().type == test_table.c.data.type

class DateTest(TestBase, AssertsExecutionResults):
    @classmethod
    def setup_class(cls):
        global users_with_date, insert_data

        db = testing.db
        if testing.against('oracle'):
            insert_data =  [
                    (7, 'jack',
                     datetime.datetime(2005, 11, 10, 0, 0),
                     datetime.date(2005,11,10),
                     datetime.datetime(2005, 11, 10, 0, 0, 0, 29384)),
                    (8, 'roy',
                     datetime.datetime(2005, 11, 10, 11, 52, 35),
                     datetime.date(2005,10,10),
                     datetime.datetime(2006, 5, 10, 15, 32, 47, 6754)),
                    (9, 'foo',
                     datetime.datetime(2006, 11, 10, 11, 52, 35),
                     datetime.date(1970,4,1),
                     datetime.datetime(2004, 9, 18, 4, 0, 52, 1043)),
                    (10, 'colber', None, None, None),
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
            if testing.against('mssql', 'mysql', 'firebird', '+zxjdbc'):
                datetime_micro, time_micro = 0, 0
            # No microseconds for TIME
            elif testing.against('maxdb'):
                time_micro = 0

            insert_data =  [
                (7, 'jack',
                 datetime.datetime(2005, 11, 10, 0, 0),
                 datetime.date(2005, 11, 10),
                 datetime.time(12, 20, 2)),
                (8, 'roy',
                 datetime.datetime(2005, 11, 10, 11, 52, 35),
                 datetime.date(2005, 10, 10),
                 datetime.time(0, 0, 0)),
                (9, 'foo',
                 datetime.datetime(2005, 11, 10, 11, 52, 35, datetime_micro),
                 datetime.date(1970, 4, 1),
                 datetime.time(23, 59, 59, time_micro)),
                (10, 'colber', None, None, None),
            ]
            
            
            fnames = ['user_id', 'user_name', 'user_datetime',
                      'user_date', 'user_time']

            collist = [Column('user_id', INT, primary_key=True),
                       Column('user_name', VARCHAR(20)),
                       Column('user_datetime', DateTime(timezone=False)),
                       Column('user_date', Date),
                       Column('user_time', Time)]

        if testing.against('sqlite', 'postgresql'):
            insert_data.append(
                (11, 'historic',
                datetime.datetime(1850, 11, 10, 11, 52, 35, datetime_micro),
                datetime.date(1727,4,1),
                None),
            )

        users_with_date = Table('query_users_with_date',
                                MetaData(testing.db), *collist)
        users_with_date.create()
        insert_dicts = [dict(zip(fnames, d)) for d in insert_data]

        for idict in insert_dicts:
            users_with_date.insert().execute(**idict)

    @classmethod
    def teardown_class(cls):
        users_with_date.drop()

    def testdate(self):
        global insert_data

        l = map(tuple,
                users_with_date.select().order_by(users_with_date.c.user_id).execute().fetchall())
        self.assert_(l == insert_data,
                     'DateTest mismatch: got:%s expected:%s' % (l, insert_data))

    def testtextdate(self):
        x = testing.db.text(
            "select user_datetime from query_users_with_date",
            typemap={'user_datetime':DateTime}).execute().fetchall()

        self.assert_(isinstance(x[0][0], datetime.datetime))

        x = testing.db.text(
            "select * from query_users_with_date where user_datetime=:somedate",
            bindparams=[bindparam('somedate', type_=types.DateTime)]).execute(
            somedate=datetime.datetime(2005, 11, 10, 11, 52, 35)).fetchall()

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
            eq_(x.adate.__class__, datetime.date)
            eq_(x.adatetime.__class__, datetime.datetime)

            t.delete().execute()

            # test mismatched date/datetime
            t.insert().execute(adate=d2, adatetime=d2)
            eq_(select([t.c.adate, t.c.adatetime], t.c.adate==d1).execute().fetchall(), [(d1, d2)])
            eq_(select([t.c.adate, t.c.adatetime], t.c.adate==d1).execute().fetchall(), [(d1, d2)])

        finally:
            t.drop(checkfirst=True)

class StringTest(TestBase, AssertsExecutionResults):

    @testing.requires.unbounded_varchar
    def test_nolength_string(self):
        metadata = MetaData(testing.db)
        foo = Table('foo', metadata, Column('one', String))

        foo.create()
        foo.drop()

def _missing_decimal():
    """Python implementation supports decimals"""
    try:
        import decimal
        return False
    except ImportError:
        return True

class NumericTest(TestBase, AssertsExecutionResults):
    @classmethod
    def setup_class(cls):
        global numeric_table, metadata
        metadata = MetaData(testing.db)
        numeric_table = Table('numeric_table', metadata,
            Column('id', Integer, Sequence('numeric_id_seq', optional=True), primary_key=True),
            Column('numericcol', Numeric(precision=10, scale=2, asdecimal=False)),
            Column('floatcol', Float(precision=10, )),
            Column('ncasdec', Numeric(precision=10, scale=2)),
            Column('fcasdec', Float(precision=10, asdecimal=True))
        )
        metadata.create_all()

    @classmethod
    def teardown_class(cls):
        metadata.drop_all()

    @engines.close_first
    def teardown(self):
        numeric_table.delete().execute()

    @testing.fails_if(_missing_decimal)
    def test_decimal(self):
        from decimal import Decimal
        numeric_table.insert().execute(
            numericcol=3.5, floatcol=5.6, ncasdec=12.4, fcasdec=15.75)
            
        numeric_table.insert().execute(
            numericcol=Decimal("3.5"), floatcol=Decimal("5.6"),
            ncasdec=Decimal("12.4"), fcasdec=Decimal("15.75"))

        l = numeric_table.select().order_by(numeric_table.c.id).execute().fetchall()
        rounded = [
            (l[0][0], l[0][1], round(l[0][2], 5), l[0][3], l[0][4]),
            (l[1][0], l[1][1], round(l[1][2], 5), l[1][3], l[1][4]),
        ]
        testing.eq_(rounded, [
            (1, 3.5, 5.6, Decimal("12.4"), Decimal("15.75")),
            (2, 3.5, 5.6, Decimal("12.4"), Decimal("15.75")),
        ])

    def test_decimal_fallback(self):
        from decimal import Decimal

        numeric_table.insert().execute(ncasdec=12.4, fcasdec=15.75)
        numeric_table.insert().execute(ncasdec=Decimal("12.4"),
                                       fcasdec=Decimal("15.75"))

        for row in numeric_table.select().execute().fetchall():
            assert isinstance(row['ncasdec'], decimal.Decimal)
            assert isinstance(row['fcasdec'], decimal.Decimal)

            
class IntervalTest(TestBase, AssertsExecutionResults):
    @classmethod
    def setup_class(cls):
        global interval_table, metadata
        metadata = MetaData(testing.db)
        interval_table = Table("intervaltable", metadata,
            Column("id", Integer, primary_key=True, test_needs_autoincrement=True),
            Column("native_interval", Interval()),
            Column("native_interval_args", Interval(day_precision=3, second_precision=6)),
            Column("non_native_interval", Interval(native=False)),
            )
        metadata.create_all()

    @engines.close_first
    def teardown(self):
        interval_table.delete().execute()

    @classmethod
    def teardown_class(cls):
        metadata.drop_all()

    @testing.fails_on("+pg8000", "Not yet known how to pass values of the INTERVAL type")
    @testing.fails_on("postgresql+zxjdbc", "Not yet known how to pass values of the INTERVAL type")
    def test_roundtrip(self):
        small_delta = datetime.timedelta(days=15, seconds=5874)
        delta = datetime.timedelta(414)
        interval_table.insert().execute(
                                native_interval=small_delta, 
                                native_interval_args=delta, 
                                non_native_interval=delta
                                )
        row = interval_table.select().execute().first()
        eq_(row['native_interval'], small_delta)
        eq_(row['native_interval_args'], delta)
        eq_(row['non_native_interval'], delta)

    def test_null(self):
        interval_table.insert().execute(id=1, native_inverval=None, non_native_interval=None)
        row = interval_table.select().execute().first()
        eq_(row['native_interval'], None)
        eq_(row['native_interval_args'], None)
        eq_(row['non_native_interval'], None)

class BooleanTest(TestBase, AssertsExecutionResults):
    @classmethod
    def setup_class(cls):
        global bool_table
        metadata = MetaData(testing.db)
        bool_table = Table('booltest', metadata,
            Column('id', Integer, primary_key=True),
            Column('value', Boolean),
            Column('unconstrained_value', Boolean(create_constraint=False)),
            )
        bool_table.create()
        
    @classmethod
    def teardown_class(cls):
        bool_table.drop()
    
    def teardown(self):
        bool_table.delete().execute()
        
    def test_boolean(self):
        bool_table.insert().execute(id=1, value=True)
        bool_table.insert().execute(id=2, value=False)
        bool_table.insert().execute(id=3, value=True)
        bool_table.insert().execute(id=4, value=True)
        bool_table.insert().execute(id=5, value=True)
        bool_table.insert().execute(id=6, value=None)

        res = select([bool_table.c.id, bool_table.c.value]).where(
            bool_table.c.value == True
            ).order_by(bool_table.c.id).execute().fetchall()
        eq_(res, [(1, True), (3, True), (4, True), (5, True)])

        res2 = select([bool_table.c.id, bool_table.c.value]).where(
                    bool_table.c.value == False).execute().fetchall()
        eq_(res2, [(2, False)])

        res3 = select([bool_table.c.id, bool_table.c.value]).\
                order_by(bool_table.c.id).\
                execute().fetchall()
        eq_(res3, [(1, True), (2, False), 
                    (3, True), (4, True), 
                    (5, True), (6, None)])
        
        # ensure we're getting True/False, not just ints
        assert res3[0][1] is True
        assert res3[1][1] is False
    
    @testing.fails_on('mysql', 
            "The CHECK clause is parsed but ignored by all storage engines.")
    @testing.skip_if(lambda: testing.db.dialect.supports_native_boolean)
    def test_constraint(self):
        assert_raises((exc.IntegrityError, exc.ProgrammingError),
                        testing.db.execute, 
                        "insert into booltest (id, value) values(1, 5)")

    @testing.skip_if(lambda: testing.db.dialect.supports_native_boolean)
    def test_unconstrained(self):
        testing.db.execute(
            "insert into booltest (id, unconstrained_value) values (1, 5)")
    
        
class PickleTest(TestBase):
    def test_eq_comparison(self):
        p1 = PickleType()
        
        for obj in (
            {'1':'2'},
            pickleable.Bar(5, 6),
            pickleable.OldSchool(10, 11)
        ):
            assert p1.compare_values(p1.copy_value(obj), obj)

        assert_raises(NotImplementedError, p1.compare_values, pickleable.BrokenComparable('foo'),pickleable.BrokenComparable('foo'))
        
    def test_nonmutable_comparison(self):
        p1 = PickleType()

        for obj in (
            {'1':'2'},
            pickleable.Bar(5, 6),
            pickleable.OldSchool(10, 11)
        ):
            assert p1.compare_values(p1.copy_value(obj), obj)
    
class CallableTest(TestBase):
    @classmethod
    def setup_class(cls):
        global meta
        meta = MetaData(testing.db)

    @classmethod
    def teardown_class(cls):
        meta.drop_all()

    def test_callable_as_arg(self):
        ucode = util.partial(Unicode, assert_unicode=None)

        thing_table = Table('thing', meta,
            Column('name', ucode(20))
        )
        assert isinstance(thing_table.c.name.type, Unicode)
        thing_table.create()

    def test_callable_as_kwarg(self):
        ucode = util.partial(Unicode, assert_unicode=None)

        thang_table = Table('thang', meta,
            Column('name', type_=ucode(20), primary_key=True)
        )
        assert isinstance(thang_table.c.name.type, Unicode)
        thang_table.create()

