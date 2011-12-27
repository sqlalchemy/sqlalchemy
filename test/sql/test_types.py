# coding: utf-8
from test.lib.testing import eq_, assert_raises, assert_raises_message
import decimal
import datetime, os, re
from sqlalchemy import *
from sqlalchemy import exc, types, util, schema, dialects
for name in dialects.__all__:
    __import__("sqlalchemy.dialects.%s" % name)
from sqlalchemy.sql import operators, column, table
from test.lib.testing import eq_
import sqlalchemy.engine.url as url
from sqlalchemy.engine import default
from test.lib.schema import Table, Column
from test.lib import *
from test.lib.util import picklers
from sqlalchemy.util.compat import decimal
from test.lib.util import round_decimal
from test.lib import fixtures

class AdaptTest(fixtures.TestBase):
    def _all_dialect_modules(self):
        return [
            getattr(dialects, d)
            for d in dialects.__all__
            if not d.startswith('_')
        ]

    def _all_dialects(self):
        return [d.base.dialect() for d in 
                self._all_dialect_modules()]

    def _all_types(self):
        def types_for_mod(mod):
            for key in dir(mod):
                typ = getattr(mod, key)
                if not isinstance(typ, type) or not issubclass(typ, types.TypeEngine):
                    continue
                yield typ

        for typ in types_for_mod(types):
            yield typ
        for dialect in self._all_dialect_modules():
            for typ in types_for_mod(dialect):
                yield typ

    def test_uppercase_rendering(self):
        """Test that uppercase types from types.py always render as their
        type.

        As of SQLA 0.6, using an uppercase type means you want specifically
        that type. If the database in use doesn't support that DDL, it (the DB
        backend) should raise an error - it means you should be using a
        lowercased (genericized) type.

        """

        for dialect in self._all_dialects():
            for type_, expected in (
                (REAL, "REAL"),
                (FLOAT, "FLOAT"),
                (NUMERIC, "NUMERIC"),
                (DECIMAL, "DECIMAL"),
                (INTEGER, "INTEGER"),
                (SMALLINT, "SMALLINT"),
                (TIMESTAMP, ("TIMESTAMP", "TIMESTAMP WITHOUT TIME ZONE")),
                (DATETIME, "DATETIME"),
                (DATE, "DATE"),
                (TIME, ("TIME", "TIME WITHOUT TIME ZONE")),
                (CLOB, "CLOB"),
                (VARCHAR(10), ("VARCHAR(10)","VARCHAR(10 CHAR)")),
                (NVARCHAR(10), ("NVARCHAR(10)", "NATIONAL VARCHAR(10)",
                                    "NVARCHAR2(10)")),
                (CHAR, "CHAR"),
                (NCHAR, ("NCHAR", "NATIONAL CHAR")),
                (BLOB, ("BLOB", "BLOB SUB_TYPE 0")),
                (BOOLEAN, ("BOOLEAN", "BOOL", "INTEGER"))
            ):
                if isinstance(expected, str):
                    expected = (expected, )

                try:
                    compiled = types.to_instance(type_).\
                            compile(dialect=dialect)
                except NotImplementedError:
                    continue

                assert compiled in expected, \
                    "%r matches none of %r for dialect %s" % \
                    (compiled, expected, dialect.name)

                assert str(types.to_instance(type_)) in expected, \
                    "default str() of type %r not expected, %r" % \
                    (type_, expected)

    @testing.uses_deprecated()
    def test_adapt_method(self):
        """ensure all types have a working adapt() method,
        which creates a distinct copy.

        The distinct copy ensures that when we cache
        the adapted() form of a type against the original
        in a weak key dictionary, a cycle is not formed.

        This test doesn't test type-specific arguments of
        adapt() beyond their defaults.

        """

        for typ in self._all_types():
            if typ in (types.TypeDecorator, types.TypeEngine, types.Variant):
                continue
            elif typ is dialects.postgresql.ARRAY:
                t1 = typ(String)
            else:
                t1 = typ()
            for cls in [typ] + typ.__subclasses__():
                if not issubclass(typ, types.Enum) and \
                    issubclass(cls, types.Enum):
                    continue
                t2 = t1.adapt(cls)
                assert t1 is not t2
                for k in t1.__dict__:
                    if k == 'impl':
                        continue
                    # assert each value was copied, or that
                    # the adapted type has a more specific
                    # value than the original (i.e. SQL Server
                    # applies precision=24 for REAL)
                    assert \
                        getattr(t2, k) == t1.__dict__[k] or \
                        t1.__dict__[k] is None

    def test_python_type(self):
        eq_(types.Integer().python_type, int)
        eq_(types.Numeric().python_type, decimal.Decimal)
        eq_(types.Numeric(asdecimal=False).python_type, float)
        # Py3K
        #eq_(types.LargeBinary().python_type, bytes)
        # Py2K
        eq_(types.LargeBinary().python_type, str)
        # end Py2K
        eq_(types.Float().python_type, float)
        eq_(types.Interval().python_type, datetime.timedelta)
        eq_(types.Date().python_type, datetime.date)
        eq_(types.DateTime().python_type, datetime.datetime)
        # Py3K
        #eq_(types.String().python_type, unicode)
        # Py2K
        eq_(types.String().python_type, str)
        # end Py2K
        eq_(types.Unicode().python_type, unicode)
        eq_(types.String(convert_unicode=True).python_type, unicode)

        assert_raises(
            NotImplementedError,
            lambda: types.TypeEngine().python_type
        )

    @testing.uses_deprecated()
    def test_repr(self):
        for typ in self._all_types():
            if typ in (types.TypeDecorator, types.TypeEngine, types.Variant):
                continue
            elif typ is dialects.postgresql.ARRAY:
                t1 = typ(String)
            else:
                t1 = typ()
            repr(t1)

    def test_plain_init_deprecation_warning(self):
        for typ in (Integer, Date, SmallInteger):
            assert_raises_message(
                exc.SADeprecationWarning,
                "Passing arguments to type object "
                "constructor %s is deprecated" % typ,
                typ, 11
            )

class TypeAffinityTest(fixtures.TestBase):
    def test_type_affinity(self):
        for type_, affin in [
            (String(), String),
            (VARCHAR(), String),
            (Date(), Date),
            (LargeBinary(), types._Binary)
        ]:
            eq_(type_._type_affinity, affin)

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

    def test_decorator_doesnt_cache(self):
        from sqlalchemy.dialects import postgresql

        class MyType(TypeDecorator):
            impl = CHAR

            def load_dialect_impl(self, dialect):
                if dialect.name == 'postgresql':
                    return dialect.type_descriptor(postgresql.UUID())
                else:
                    return dialect.type_descriptor(CHAR(32))

        t1 = MyType()
        d = postgresql.dialect()
        assert t1._type_affinity is String
        assert t1.dialect_impl(d)._type_affinity is postgresql.UUID

class PickleMetadataTest(fixtures.TestBase):
    def testmeta(self):
        for loads, dumps in picklers():
            column_types = [
                Column('Boo', Boolean()),
                Column('Str', String()),
                Column('Tex', Text()),
                Column('Uni', Unicode()),
                Column('Int', Integer()),
                Column('Sma', SmallInteger()),
                Column('Big', BigInteger()),
                Column('Num', Numeric()),
                Column('Flo', Float()),
                Column('Dat', DateTime()),
                Column('Dat', Date()),
                Column('Tim', Time()),
                Column('Lar', LargeBinary()),
                Column('Pic', PickleType()),
                Column('Int', Interval()),
                Column('Enu', Enum('x','y','z', name="somename")),
            ]
            for column_type in column_types:
                #print column_type
                meta = MetaData()
                Table('foo', meta, column_type)
                ct = loads(dumps(column_type))
                mt = loads(dumps(meta))


class UserDefinedTest(fixtures.TablesTest, AssertsCompiledSQL):
    """tests user-defined types."""

    def test_processing(self):
        users = self.tables.users
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
            for col in list(row)[1:5]:
                eq_(col, assertstr)
            eq_(row[5], assertint)
            eq_(row[6], assertint2)
            for col in row[3], row[4]:
                assert isinstance(col, unicode)

    def test_typedecorator_impl(self):
        for impl_, exp, kw in [
            (Float, "FLOAT", {}),
            (Float, "FLOAT(2)", {'precision':2}),
            (Float(2), "FLOAT(2)", {'precision':4}),
            (Numeric(19, 2), "NUMERIC(19, 2)", {}),
        ]:
            for dialect_ in (dialects.postgresql, dialects.mssql, dialects.mysql):
                dialect_ = dialect_.dialect()

                raw_impl = types.to_instance(impl_, **kw)

                class MyType(types.TypeDecorator):
                    impl = impl_

                dec_type = MyType(**kw)

                eq_(dec_type.impl.__class__, raw_impl.__class__)

                raw_dialect_impl = raw_impl.dialect_impl(dialect_)
                dec_dialect_impl = dec_type.dialect_impl(dialect_)
                eq_(dec_dialect_impl.__class__, MyType)
                eq_(raw_dialect_impl.__class__ , dec_dialect_impl.impl.__class__)

                self.assert_compile(
                    MyType(**kw),
                    exp,
                    dialect=dialect_
                )

    def test_user_defined_typedec_impl(self):
        class MyType(types.TypeDecorator):
            impl = Float

            def load_dialect_impl(self, dialect):
                if dialect.name == 'sqlite':
                    return String(50)
                else:
                    return super(MyType, self).load_dialect_impl(dialect)

        sl = dialects.sqlite.dialect()
        pg = dialects.postgresql.dialect()
        t = MyType()
        self.assert_compile(t, "VARCHAR(50)", dialect=sl)
        self.assert_compile(t, "FLOAT", dialect=pg)
        eq_(
            t.dialect_impl(dialect=sl).impl.__class__, 
            String().dialect_impl(dialect=sl).__class__
        )
        eq_(
                t.dialect_impl(dialect=pg).impl.__class__, 
                Float().dialect_impl(pg).__class__
        )

    def test_user_defined_typedec_impl_bind(self):
        class TypeOne(types.TypeEngine):
            def bind_processor(self, dialect):
                def go(value):
                    return value + " ONE"
                return go

        class TypeTwo(types.TypeEngine):
            def bind_processor(self, dialect):
                def go(value):
                    return value + " TWO"
                return go

        class MyType(types.TypeDecorator):
            impl = TypeOne

            def load_dialect_impl(self, dialect):
                if dialect.name == 'sqlite':
                    return TypeOne()
                else:
                    return TypeTwo()

            def process_bind_param(self, value, dialect):
                return "MYTYPE " + value
        sl = dialects.sqlite.dialect()
        pg = dialects.postgresql.dialect()
        t = MyType()
        eq_(
            t._cached_bind_processor(sl)('foo'),
            "MYTYPE foo ONE"
        )
        eq_(
            t._cached_bind_processor(pg)('foo'),
            "MYTYPE foo TWO"
        )

    def test_user_defined_dialect_specific_args(self):
        class MyType(types.UserDefinedType):
            def __init__(self, foo='foo', **kwargs):
                super(MyType, self).__init__()
                self.foo = foo
                self.dialect_specific_args = kwargs
            def adapt(self, cls):
                return cls(foo=self.foo, **self.dialect_specific_args)
        t = MyType(bar='bar')
        a = t.dialect_impl(testing.db.dialect)
        eq_(a.foo, 'foo')
        eq_(a.dialect_specific_args['bar'], 'bar')

    @testing.provide_metadata
    def test_type_coerce(self):
        """test ad-hoc usage of custom types with type_coerce()."""

        metadata = self.metadata
        class MyType(types.TypeDecorator):
            impl = String

            def process_bind_param(self, value, dialect):
                return value[0:-8]

            def process_result_value(self, value, dialect):
                return value + "BIND_OUT"

        t = Table('t', metadata, Column('data', String(50)))
        metadata.create_all()

        t.insert().values(data=type_coerce('d1BIND_OUT',MyType)).execute()

        eq_(
            select([type_coerce(t.c.data, MyType)]).execute().fetchall(),
            [('d1BIND_OUT', )]
        )

        eq_(
            select([t.c.data, type_coerce(t.c.data, MyType)]).execute().fetchall(),
            [('d1', 'd1BIND_OUT')]
        )

        eq_(
            select([t.c.data, type_coerce(t.c.data, MyType)]).\
                        where(type_coerce(t.c.data, MyType) == 'd1BIND_OUT').\
                        execute().fetchall(),
            [('d1', 'd1BIND_OUT')]
        )

        eq_(
            select([t.c.data, type_coerce(t.c.data, MyType)]).\
                        where(t.c.data == type_coerce('d1BIND_OUT', MyType)).\
                        execute().fetchall(),
            [('d1', 'd1BIND_OUT')]
        )

        eq_(
            select([t.c.data, type_coerce(t.c.data, MyType)]).\
                        where(t.c.data == type_coerce(None, MyType)).\
                        execute().fetchall(),
            []
        )

        eq_(
            select([t.c.data, type_coerce(t.c.data, MyType)]).\
                        where(type_coerce(t.c.data, MyType) == None).\
                        execute().fetchall(),
            []
        )

    @classmethod
    def define_tables(cls, metadata):
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
                impl_processor = super(MyDecoratedType, self).bind_processor(dialect)\
                                        or (lambda value:value)
                def process(value):
                    return "BIND_IN"+ impl_processor(value)
                return process
            def result_processor(self, dialect, coltype):
                impl_processor = super(MyDecoratedType, self).result_processor(dialect, coltype)\
                                        or (lambda value:value)
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
                impl_processor = super(MyUnicodeType, self).bind_processor(dialect)\
                                        or (lambda value:value)

                def process(value):
                    return "BIND_IN"+ impl_processor(value)
                return process

            def result_processor(self, dialect, coltype):
                impl_processor = super(MyUnicodeType, self).result_processor(dialect, coltype)\
                                        or (lambda value:value)
                def process(value):
                    return impl_processor(value) + "BIND_OUT"
                return process

            def copy(self):
                return MyUnicodeType(self.impl.length)

        Table('users', metadata,
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

class VariantTest(fixtures.TestBase, AssertsCompiledSQL):
    def setup(self):
        class UTypeOne(types.UserDefinedType):
            def get_col_spec(self):
                return "UTYPEONE"
            def bind_processor(self, dialect):
                def process(value):
                    return value + "UONE"
                return process

        class UTypeTwo(types.UserDefinedType):
            def get_col_spec(self):
                return "UTYPETWO"
            def bind_processor(self, dialect):
                def process(value):
                    return value + "UTWO"
                return process

        class UTypeThree(types.UserDefinedType):
            def get_col_spec(self):
                return "UTYPETHREE"

        self.UTypeOne = UTypeOne
        self.UTypeTwo = UTypeTwo
        self.UTypeThree = UTypeThree
        self.variant = self.UTypeOne().with_variant(
                            self.UTypeTwo(), 'postgresql')
        self.composite = self.variant.with_variant(
                            self.UTypeThree(), 'mysql')

    def test_illegal_dupe(self):
        v = self.UTypeOne().with_variant(
            self.UTypeTwo(), 'postgresql'
        )
        assert_raises_message(
            exc.ArgumentError,
            "Dialect 'postgresql' is already present "
            "in the mapping for this Variant",
            lambda: v.with_variant(self.UTypeThree(), 'postgresql')
        )
    def test_compile(self):
        self.assert_compile(
            self.variant,
            "UTYPEONE",
            use_default_dialect=True
        )
        self.assert_compile(
            self.variant,
            "UTYPEONE",
            dialect=dialects.mysql.dialect()
        )
        self.assert_compile(
            self.variant,
            "UTYPETWO",
            dialect=dialects.postgresql.dialect()
        )

    def test_compile_composite(self):
        self.assert_compile(
            self.composite,
            "UTYPEONE",
            use_default_dialect=True
        )
        self.assert_compile(
            self.composite,
            "UTYPETHREE",
            dialect=dialects.mysql.dialect()
        )
        self.assert_compile(
            self.composite,
            "UTYPETWO",
            dialect=dialects.postgresql.dialect()
        )

    def test_bind_process(self):
        eq_(
            self.variant._cached_bind_processor(
                    dialects.mysql.dialect())('foo'),
            'fooUONE'
        )
        eq_(
            self.variant._cached_bind_processor(
                    default.DefaultDialect())('foo'),
            'fooUONE'
        )
        eq_(
            self.variant._cached_bind_processor(
                    dialects.postgresql.dialect())('foo'),
            'fooUTWO'
        )

    def test_bind_process_composite(self):
        assert self.composite._cached_bind_processor(
                    dialects.mysql.dialect()) is None
        eq_(
            self.composite._cached_bind_processor(
                    default.DefaultDialect())('foo'),
            'fooUONE'
        )
        eq_(
            self.composite._cached_bind_processor(
                    dialects.postgresql.dialect())('foo'),
            'fooUTWO'
        )

class UnicodeTest(fixtures.TestBase, AssertsExecutionResults):
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

        if \
	     (testing.against('mssql+pyodbc') and not testing.db.dialect.freetds):
            assert testing.db.dialect.returns_unicode_strings == 'conditional'
            return

        if testing.against('mssql+pymssql'):
            assert testing.db.dialect.returns_unicode_strings == ('charset' in testing.db.url.query)
            return

        assert testing.db.dialect.returns_unicode_strings == \
            ((testing.db.name, testing.db.driver) in \
            (
                ('postgresql','psycopg2'),
                ('postgresql','pypostgresql'),
                ('postgresql','pg8000'),
                ('postgresql','zxjdbc'),
                ('mysql','oursql'),
                ('mysql','zxjdbc'),
                ('mysql','mysqlconnector'),
                ('mysql','pymysql'),
                ('sqlite','pysqlite'),
                ('oracle','zxjdbc'),
                ('oracle','cx_oracle'),
            )), \
            "name: %s driver %s returns_unicode_strings=%s" % \
                                        (testing.db.name, 
                                         testing.db.driver, 
                                         testing.db.dialect.returns_unicode_strings)

    def test_round_trip(self):
        unicodedata = u"Alors vous imaginez ma surprise, au lever du jour, "\
                    u"quand une drôle de petite voix m’a réveillé. Elle "\
                    u"disait: « S’il vous plaît… dessine-moi un mouton! »"

        unicode_table.insert().execute(unicode_varchar=unicodedata,unicode_text=unicodedata)

        x = unicode_table.select().execute().first()
        assert isinstance(x['unicode_varchar'], unicode)
        assert isinstance(x['unicode_text'], unicode)
        eq_(x['unicode_varchar'], unicodedata)
        eq_(x['unicode_text'], unicodedata)

    def test_round_trip_executemany(self):
        # cx_oracle was producing different behavior for cursor.executemany()
        # vs. cursor.execute()

        unicodedata = u"Alors vous imaginez ma surprise, au lever du jour, quand "\
                        u"une drôle de petite voix m’a réveillé. "\
                        u"Elle disait: « S’il vous plaît… dessine-moi un mouton! »"

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

        unicodedata = u"Alors vous imaginez ma surprise, au lever du jour, quand "\
                        u"une drôle de petite voix m’a réveillé. "\
                        u"Elle disait: « S’il vous plaît… dessine-moi un mouton! »"

        unicode_table.insert().execute(unicode_varchar=unicodedata,unicode_text=unicodedata)

        x = union(
                    select([unicode_table.c.unicode_varchar]),
                    select([unicode_table.c.unicode_varchar])
                ).execute().first()

        assert isinstance(x['unicode_varchar'], unicode)
        eq_(x['unicode_varchar'], unicodedata)

    @testing.fails_on('oracle', 'oracle converts empty strings to a blank space')
    def test_blank_strings(self):
        unicode_table.insert().execute(unicode_varchar=u'')
        assert select([unicode_table.c.unicode_varchar]).scalar() == u''

    def test_unicode_warnings(self):
        """test the warnings raised when SQLA must coerce unicode binds,
        *and* is using the Unicode type.

        """

        unicodedata = u"Alors vous imaginez ma surprise, au lever du jour, quand "\
                        u"une drôle de petite voix m’a réveillé. "\
                        u"Elle disait: « S’il vous plaît… dessine-moi un mouton! »"

        # using Unicode explicly - warning should be emitted
        u = Unicode()
        uni = u.dialect_impl(testing.db.dialect).bind_processor(testing.db.dialect)
        if testing.db.dialect.supports_unicode_binds:
            # Py3K
            #assert_raises(exc.SAWarning, uni, b'x')
            #assert isinstance(uni(unicodedata), str)
            # Py2K
            assert_raises(exc.SAWarning, uni, 'x')
            assert isinstance(uni(unicodedata), unicode)
            # end Py2K

            eq_(uni(unicodedata), unicodedata)
        else:
            # Py3K
            #assert_raises(exc.SAWarning, uni, b'x')
            #assert isinstance(uni(unicodedata), bytes)
            # Py2K
            assert_raises(exc.SAWarning, uni, 'x')
            assert isinstance(uni(unicodedata), str)
            # end Py2K

            eq_(uni(unicodedata), unicodedata.encode('utf-8'))

        # using convert unicode at engine level - 
        # this should not be raising a warning
        unicode_engine = engines.utf8_engine(options={'convert_unicode':True,})
        unicode_engine.dialect.supports_unicode_binds = False

        s = String()
        uni = s.dialect_impl(unicode_engine.dialect).bind_processor(unicode_engine.dialect)
        # this is not the unicode type - no warning
        # Py3K
        #uni(b'x')
        #assert isinstance(uni(unicodedata), bytes)
        # Py2K
        uni('x')
        assert isinstance(uni(unicodedata), str)
        # end Py2K

        eq_(uni(unicodedata), unicodedata.encode('utf-8'))

    # Py3K
    #@testing.fails_if(
    #                    lambda: testing.db_spec("postgresql+pg8000")(testing.db),
    #                    "pg8000 appropriately does not accept 'bytes' for a VARCHAR column."
    #                    )
    def test_ignoring_unicode_error(self):
        """checks String(unicode_error='ignore') is passed to underlying codec."""

        unicodedata = u"Alors vous imaginez ma surprise, au lever du jour, quand "\
                        u"une drôle de petite voix m’a réveillé. "\
                        u"Elle disait: « S’il vous plaît… dessine-moi un mouton! »"

        asciidata = unicodedata.encode('ascii', 'ignore')

        m = MetaData()
        table = Table('unicode_err_table', m,
            Column('sort', Integer),
            Column('plain_varchar_no_coding_error', \
                    String(248, convert_unicode='force', unicode_error='ignore'))
            )

        m2 = MetaData()
        utf8_table = Table('unicode_err_table', m2,
            Column('sort', Integer),
            Column('plain_varchar_no_coding_error', \
                    String(248, convert_unicode=True))
            )

        engine = engines.testing_engine(options={'encoding':'ascii'})
        m.create_all(engine)
        try:
            # insert a row that should be ascii and 
            # coerce from unicode with ignore on the bind side
            engine.execute(
                table.insert(),
                sort=1,
                plain_varchar_no_coding_error=unicodedata
            )

            # switch to utf-8
            engine.dialect.encoding = 'utf-8'
            from binascii import hexlify

            # the row that we put in was stored as hexlified ascii
            row = engine.execute(utf8_table.select()).first()
            x = row['plain_varchar_no_coding_error']
            connect_opts = engine.dialect.create_connect_args(testing.db.url)[1]
            if isinstance(x, unicode):
                x = x.encode('utf-8')
            a = hexlify(x)
            b = hexlify(asciidata)
            eq_(a, b)

            # insert another row which will be stored with
            # utf-8 only chars
            engine.execute(
                utf8_table.insert(),
                sort=2,
                plain_varchar_no_coding_error=unicodedata
            )

            # switch back to ascii
            engine.dialect.encoding = 'ascii'

            # one row will be ascii with ignores,
            # the other will be either ascii with the ignores
            # or just the straight unicode+ utf8 value if the 
            # dialect just returns unicode
            result = engine.execute(table.select().order_by(table.c.sort))
            ascii_row = result.fetchone()
            utf8_row = result.fetchone()
            result.close()

            x = ascii_row['plain_varchar_no_coding_error']
            # on python3 "x" comes back as string (i.e. unicode),
            # hexlify requires bytes
            a = hexlify(x.encode('utf-8'))
            b = hexlify(asciidata)
            eq_(a, b)

            x = utf8_row['plain_varchar_no_coding_error']
            if testing.against('mssql+pyodbc') and not testing.db.dialect.freetds:
                # TODO: no clue what this is
                eq_(
                      x,
                      u'Alors vous imaginez ma surprise, au lever du jour, quand une '
                      u'drle de petite voix ma rveill. Elle disait:  Sil vous plat '
                      u'dessine-moi un mouton! '
                )
            elif engine.dialect.returns_unicode_strings:
                eq_(x, unicodedata)
            else:
                a = hexlify(x)
                eq_(a, b)

        finally:
            m.drop_all(engine)


class EnumTest(fixtures.TestBase):
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

    def test_adapt(self):
        from sqlalchemy.dialects.postgresql import ENUM
        e1 = Enum('one','two','three', native_enum=False)
        eq_(e1.adapt(ENUM).native_enum, False)
        e1 = Enum('one','two','three', native_enum=True)
        eq_(e1.adapt(ENUM).native_enum, True)
        e1 = Enum('one','two','three', name='foo', schema='bar')
        eq_(e1.adapt(ENUM).name, 'foo')
        eq_(e1.adapt(ENUM).schema, 'bar')

    @testing.crashes('mysql', 
                    'Inconsistent behavior across various OS/drivers'
                )
    def test_constraint(self):
        assert_raises(exc.DBAPIError, 
            enum_table.insert().execute,
            {'id':4, 'someenum':'four'}
        )

    @testing.fails_on('mysql', 
                    "the CHECK constraint doesn't raise an exception for unknown reason")
    def test_non_native_constraint(self):
        assert_raises(exc.DBAPIError, 
            non_native_enum_table.insert().execute,
            {'id':4, 'someenum':'four'}
        )

    def test_mock_engine_no_prob(self):
        """ensure no 'checkfirst' queries are run when enums
        are created with checkfirst=False"""

        e = engines.mock_engine()
        t = Table('t1', MetaData(),
            Column('x', Enum("x", "y", name="pge"))
        )
        t.create(e, checkfirst=False)
        # basically looking for the start of 
        # the constraint, or the ENUM def itself,
        # depending on backend.
        assert "('x'," in e.print_sql()

class BinaryTest(fixtures.TestBase, AssertsExecutionResults):
    __excluded_on__ = (
        ('mysql', '<', (4, 1, 1)),  # screwy varbinary types
        )

    @classmethod
    def setup_class(cls):
        global binary_table, MyPickleType, metadata

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

        metadata = MetaData(testing.db)
        binary_table = Table('binary_table', metadata,
            Column('primary_id', Integer, primary_key=True, test_needs_autoincrement=True),
            Column('data', LargeBinary),
            Column('data_slice', LargeBinary(100)),
            Column('misc', String(30)),
            Column('pickled', PickleType),
            Column('mypickle', MyPickleType)
        )
        metadata.create_all()

    @engines.close_first
    def teardown(self):
        binary_table.delete().execute()

    @classmethod
    def teardown_class(cls):
        metadata.drop_all()

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
                typemap={'pickled':PickleType, 
                        'mypickle':MyPickleType, 
                        'data':LargeBinary, 'data_slice':LargeBinary}, 
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

    @testing.fails_on('oracle+cx_oracle', 'oracle fairly grumpy about binary '
                                        'data, not really known how to make this work')
    def test_comparison(self):
        """test that type coercion occurs on comparison for binary"""

        expr = binary_table.c.data == 'foo'
        assert isinstance(expr.right.type, LargeBinary)

        data = os.urandom(32)
        binary_table.insert().execute(data=data)
        eq_(binary_table.select().where(binary_table.c.data==data).alias().count().scalar(), 1)


    def load_stream(self, name):
        f = os.path.join(os.path.dirname(__file__), "..", name)
        return open(f, mode='rb').read()

class ExpressionTest(fixtures.TestBase, AssertsExecutionResults, AssertsCompiledSQL):
    __dialect__ = 'default'

    @classmethod
    def setup_class(cls):
        global test_table, meta, MyCustomType, MyTypeDec

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

        class MyTypeDec(types.TypeDecorator):
            impl = String

            def process_bind_param(self, value, dialect):
                return "BIND_IN" + str(value)

            def process_result_value(self, value, dialect):
                return value + "BIND_OUT"

        meta = MetaData(testing.db)
        test_table = Table('test', meta,
            Column('id', Integer, primary_key=True),
            Column('data', String(30)),
            Column('atimestamp', Date),
            Column('avalue', MyCustomType),
            Column('bvalue', MyTypeDec(50)),
            )

        meta.create_all()

        test_table.insert().execute({
                                'id':1, 
                                'data':'somedata', 
                                'atimestamp':datetime.date(2007, 10, 15), 
                                'avalue':25, 'bvalue':'foo'})

    @classmethod
    def teardown_class(cls):
        meta.drop_all()

    def test_control(self):
        assert testing.db.execute("select avalue from test").scalar() == 250

        eq_(
            test_table.select().execute().fetchall(),
            [(1, 'somedata', datetime.date(2007, 10, 15), 25,
             'BIND_INfooBIND_OUT')]
        )

    def test_bind_adapt(self):
        # test an untyped bind gets the left side's type
        expr = test_table.c.atimestamp == bindparam("thedate")
        eq_(expr.right.type._type_affinity, Date)

        eq_(
            testing.db.execute(
                    select([test_table.c.id, test_table.c.data, test_table.c.atimestamp])
                    .where(expr), 
                    {"thedate":datetime.date(2007, 10, 15)}).fetchall(),
            [(1, 'somedata', datetime.date(2007, 10, 15))]
        )

        expr = test_table.c.avalue == bindparam("somevalue")
        eq_(expr.right.type._type_affinity, MyCustomType)

        eq_(
            testing.db.execute(test_table.select().where(expr),
             {'somevalue': 25}).fetchall(),
            [(1, 'somedata', datetime.date(2007, 10, 15), 25,
             'BIND_INfooBIND_OUT')]
        )

        expr = test_table.c.bvalue == bindparam("somevalue")
        eq_(expr.right.type._type_affinity, String)

        eq_(
            testing.db.execute(test_table.select().where(expr), 
                {"somevalue":"foo"}).fetchall(),
            [(1, 'somedata', 
                datetime.date(2007, 10, 15), 25, 'BIND_INfooBIND_OUT')]
        )

    def test_literal_adapt(self):
        # literals get typed based on the types dictionary, unless
        # compatible with the left side type

        expr = column('foo', String) == 5
        eq_(expr.right.type._type_affinity, Integer)

        expr = column('foo', String) == "asdf"
        eq_(expr.right.type._type_affinity, String)

        expr = column('foo', CHAR) == 5
        eq_(expr.right.type._type_affinity, Integer)

        expr = column('foo', CHAR) == "asdf"
        eq_(expr.right.type.__class__, CHAR)


    @testing.fails_on('firebird', 'Data type unknown on the parameter')
    @testing.fails_on('mssql', 'int is unsigned ?  not clear')
    def test_operator_adapt(self):
        """test type-based overloading of operators"""

        # test string concatenation
        expr = test_table.c.data + "somedata"
        eq_(testing.db.execute(select([expr])).scalar(), "somedatasomedata")

        expr = test_table.c.id + 15
        eq_(testing.db.execute(select([expr])).scalar(), 16)

        # test custom operator conversion
        expr = test_table.c.avalue + 40
        assert expr.type.__class__ is test_table.c.avalue.type.__class__

        # value here is calculated as (250 - 40) / 10 = 21
        # because "40" is an integer, not an "avalue"
        eq_(testing.db.execute(select([expr.label('foo')])).scalar(), 21)

        expr = test_table.c.avalue + literal(40, type_=MyCustomType)

        # + operator converted to -
        # value is calculated as: (250 - (40 * 10)) / 10 == -15
        eq_(testing.db.execute(select([expr.label('foo')])).scalar(), -15)

        # this one relies upon anonymous labeling to assemble result
        # processing rules on the column.
        eq_(testing.db.execute(select([expr])).scalar(), -15)

    def test_typedec_operator_adapt(self):
        expr = test_table.c.bvalue + "hi"

        assert expr.type.__class__ is MyTypeDec
        assert expr.right.type.__class__ is MyTypeDec

        eq_(
            testing.db.execute(select([expr.label('foo')])).scalar(),
            "BIND_INfooBIND_INhiBIND_OUT"
        )

    def test_typedec_righthand_coercion(self):
        class MyTypeDec(types.TypeDecorator):
            impl = String

            def process_bind_param(self, value, dialect):
                return "BIND_IN" + str(value)

            def process_result_value(self, value, dialect):
                return value + "BIND_OUT"

        tab = table('test', column('bvalue', MyTypeDec))
        expr = tab.c.bvalue + 6

        self.assert_compile(
            expr,
            "test.bvalue || :bvalue_1",
            use_default_dialect=True
        )

        assert expr.type.__class__ is MyTypeDec
        eq_(
            testing.db.execute(select([expr.label('foo')])).scalar(),
            "BIND_INfooBIND_IN6BIND_OUT"
        )


    def test_bind_typing(self):
        from sqlalchemy.sql import column

        class MyFoobarType(types.UserDefinedType):
            pass

        class Foo(object):
            pass

        # unknown type + integer, right hand bind
        # is an Integer
        expr = column("foo", MyFoobarType) + 5
        assert expr.right.type._type_affinity is types.Integer

        # untyped bind - it gets assigned MyFoobarType
        expr = column("foo", MyFoobarType) + bindparam("foo")
        assert expr.right.type._type_affinity is MyFoobarType

        expr = column("foo", MyFoobarType) + bindparam("foo", type_=Integer)
        assert expr.right.type._type_affinity is types.Integer

        # unknown type + unknown, right hand bind
        # coerces to the left
        expr = column("foo", MyFoobarType) + Foo()
        assert expr.right.type._type_affinity is MyFoobarType

        # including for non-commutative ops
        expr = column("foo", MyFoobarType) - Foo()
        assert expr.right.type._type_affinity is MyFoobarType

        expr = column("foo", MyFoobarType) - datetime.date(2010, 8, 25)
        assert expr.right.type._type_affinity is types.Date

    def test_date_coercion(self):
        from sqlalchemy.sql import column

        expr = column('bar', types.NULLTYPE) - column('foo', types.TIMESTAMP)
        eq_(expr.type._type_affinity, types.NullType)

        expr = func.sysdate() - column('foo', types.TIMESTAMP)
        eq_(expr.type._type_affinity, types.Interval)

        expr = func.current_date() - column('foo', types.TIMESTAMP)
        eq_(expr.type._type_affinity, types.Interval)

    def test_numerics_coercion(self):
        from sqlalchemy.sql import column
        import operator

        for op in (
            operator.add,
            operator.mul,
            operator.truediv,
            operator.sub
        ):
            for other in (Numeric(10, 2), Integer):
                expr = op(
                        column('bar', types.Numeric(10, 2)),
                        column('foo', other)
                       )
                assert isinstance(expr.type, types.Numeric)
                expr = op(
                        column('foo', other),
                        column('bar', types.Numeric(10, 2))
                       )
                assert isinstance(expr.type, types.Numeric)

    def test_null_comparison(self):
        eq_(
            str(column('a', types.NullType()) + column('b', types.NullType())), 
            "a + b"
        )

    def test_expression_typing(self):
        expr = column('bar', Integer) - 3

        eq_(expr.type._type_affinity, Integer)

        expr = bindparam('bar') + bindparam('foo')
        eq_(expr.type, types.NULLTYPE)

    def test_distinct(self):
        s = select([distinct(test_table.c.avalue)])
        eq_(testing.db.execute(s).scalar(), 25)

        s = select([test_table.c.avalue.distinct()])
        eq_(testing.db.execute(s).scalar(), 25)

        assert distinct(test_table.c.data).type == test_table.c.data.type
        assert test_table.c.data.distinct().type == test_table.c.data.type

class CompileTest(fixtures.TestBase, AssertsCompiledSQL):
    def test_default_compile(self):
        """test that the base dialect of the type object is used
        for default compilation.

        """

        for type_, expected in (
            (String(), "VARCHAR"),
            (Integer(), "INTEGER"),
            (dialects.postgresql.INET(), "INET"),
            (dialects.postgresql.FLOAT(), "FLOAT"),
            (dialects.mysql.REAL(precision=8, scale=2), "REAL(8, 2)"),
            (dialects.postgresql.REAL(), "REAL"),
            (INTEGER(), "INTEGER"),
            (dialects.mysql.INTEGER(display_width=5), "INTEGER(5)")
        ):
            self.assert_compile(type_, expected,
                                allow_dialect_select=True)

class DateTest(fixtures.TestBase, AssertsExecutionResults):
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
        x = testing.db.execute(text(
            "select user_datetime from query_users_with_date",
            typemap={'user_datetime':DateTime})).fetchall()

        self.assert_(isinstance(x[0][0], datetime.datetime))

        x = testing.db.execute(text(
            "select * from query_users_with_date where user_datetime=:somedate",
            bindparams=[bindparam('somedate', type_=types.DateTime)]),
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

class StringTest(fixtures.TestBase):

    @testing.requires.unbounded_varchar
    def test_nolength_string(self):
        metadata = MetaData(testing.db)
        foo = Table('foo', metadata, Column('one', String))

        foo.create()
        foo.drop()

class NumericTest(fixtures.TestBase):
    def setup(self):
        global metadata
        metadata = MetaData(testing.db)

    def teardown(self):
        metadata.drop_all()

    @testing.emits_warning(r".*does \*not\* support Decimal objects natively")
    def _do_test(self, type_, input_, output, filter_ = None):
        t = Table('t', metadata, Column('x', type_))
        t.create()
        t.insert().execute([{'x':x} for x in input_])

        result = set([row[0] for row in t.select().execute()])
        output = set(output)
        if filter_:
            result = set(filter_(x) for x in result)
            output = set(filter_(x) for x in output)
        #print result
        #print output
        eq_(result, output)

    def test_numeric_as_decimal(self):
        self._do_test(
            Numeric(precision=8, scale=4),
            [15.7563, decimal.Decimal("15.7563"), None],
            [decimal.Decimal("15.7563"), None], 
        )

    def test_numeric_as_float(self):
        if testing.against("oracle+cx_oracle"):
            filter_ = lambda n:n is not None and round(n, 5) or None
        else:
            filter_ = None

        self._do_test(
            Numeric(precision=8, scale=4, asdecimal=False),
            [15.7563, decimal.Decimal("15.7563"), None],
            [15.7563, None],
            filter_ = filter_
        )

    def test_float_as_decimal(self):
        self._do_test(
            Float(precision=8, asdecimal=True),
            [15.7563, decimal.Decimal("15.7563"), None],
            [decimal.Decimal("15.7563"), None], 
            filter_ = lambda n:n is not None and round(n, 5) or None
        )

    def test_float_as_float(self):
        self._do_test(
            Float(precision=8),
            [15.7563, decimal.Decimal("15.7563")],
            [15.7563],
            filter_ = lambda n:n is not None and round(n, 5) or None
        )

    @testing.fails_on('mssql+pymssql', 'FIXME: improve pymssql dec handling')
    def test_precision_decimal(self):
        numbers = set([
            decimal.Decimal("54.234246451650"),
            decimal.Decimal("0.004354"), 
            decimal.Decimal("900.0"), 
        ])

        self._do_test(
            Numeric(precision=18, scale=12),
            numbers,
            numbers,
        )

    @testing.fails_on('mssql+pymssql', 'FIXME: improve pymssql dec handling')
    def test_enotation_decimal(self):
        """test exceedingly small decimals.

        Decimal reports values with E notation when the exponent 
        is greater than 6.

        """

        numbers = set([
            decimal.Decimal('1E-2'),
            decimal.Decimal('1E-3'),
            decimal.Decimal('1E-4'),
            decimal.Decimal('1E-5'),
            decimal.Decimal('1E-6'),
            decimal.Decimal('1E-7'),
            decimal.Decimal('1E-8'),
            decimal.Decimal("0.01000005940696"),
            decimal.Decimal("0.00000005940696"),
            decimal.Decimal("0.00000000000696"),
            decimal.Decimal("0.70000000000696"),
            decimal.Decimal("696E-12"),
        ])
        self._do_test(
            Numeric(precision=18, scale=14),
            numbers,
            numbers
        )

    @testing.fails_on("sybase+pyodbc", 
                        "Don't know how do get these values through FreeTDS + Sybase")
    @testing.fails_on("firebird", "Precision must be from 1 to 18")
    def test_enotation_decimal_large(self):
        """test exceedingly large decimals.

        """

        numbers = set([
            decimal.Decimal('4E+8'),
            decimal.Decimal("5748E+15"),
            decimal.Decimal('1.521E+15'),
            decimal.Decimal('00000000000000.1E+12'),
        ])
        self._do_test(
            Numeric(precision=25, scale=2),
            numbers,
            numbers
        )

    @testing.fails_on('sqlite', 'TODO')
    @testing.fails_on("firebird", "Precision must be from 1 to 18")
    @testing.fails_on("sybase+pysybase", "TODO")
    @testing.fails_on('mssql+pymssql', 'FIXME: improve pymssql dec handling')
    def test_many_significant_digits(self):
        numbers = set([
            decimal.Decimal("31943874831932418390.01"),
            decimal.Decimal("319438950232418390.273596"),
            decimal.Decimal("87673.594069654243"),
        ])
        self._do_test(
            Numeric(precision=38, scale=12),
            numbers,
            numbers
        )

class NumericRawSQLTest(fixtures.TestBase):
    """Test what DBAPIs and dialects return without any typing
    information supplied at the SQLA level.

    """
    def _fixture(self, metadata, type, data):
        t = Table('t', metadata,
            Column("val", type)
        )
        metadata.create_all()
        t.insert().execute(val=data)

    @testing.fails_on('sqlite', "Doesn't provide Decimal results natively")
    @testing.provide_metadata
    def test_decimal_fp(self):
        metadata = self.metadata
        t = self._fixture(metadata, Numeric(10, 5), decimal.Decimal("45.5"))
        val = testing.db.execute("select val from t").scalar()
        assert isinstance(val, decimal.Decimal)
        eq_(val, decimal.Decimal("45.5"))

    @testing.fails_on('sqlite', "Doesn't provide Decimal results natively")
    @testing.provide_metadata
    def test_decimal_int(self):
        metadata = self.metadata
        t = self._fixture(metadata, Numeric(10, 5), decimal.Decimal("45"))
        val = testing.db.execute("select val from t").scalar()
        assert isinstance(val, decimal.Decimal)
        eq_(val, decimal.Decimal("45"))

    @testing.provide_metadata
    def test_ints(self):
        metadata = self.metadata
        t = self._fixture(metadata, Integer, 45)
        val = testing.db.execute("select val from t").scalar()
        assert isinstance(val, (int, long))
        eq_(val, 45)

    @testing.provide_metadata
    def test_float(self):
        metadata = self.metadata
        t = self._fixture(metadata, Float, 46.583)
        val = testing.db.execute("select val from t").scalar()
        assert isinstance(val, float)

        # some DBAPIs have unusual float handling
        if testing.against('oracle+cx_oracle', 'mysql+oursql'):
            eq_(round_decimal(val, 3), 46.583)
        else:
            eq_(val, 46.583)




class IntervalTest(fixtures.TestBase, AssertsExecutionResults):
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

    def test_non_native_adapt(self):
        interval = Interval(native=False)
        adapted = interval.dialect_impl(testing.db.dialect)
        assert type(adapted) is Interval
        assert adapted.native is False
        eq_(str(adapted), "DATETIME")

    @testing.fails_on("+pg8000", "Not yet known how to pass values of the INTERVAL type")
    @testing.fails_on("postgresql+zxjdbc", "Not yet known how to pass values of the INTERVAL type")
    @testing.fails_on("oracle+zxjdbc", "Not yet known how to pass values of the INTERVAL type")
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

    @testing.fails_on("oracle+zxjdbc", "Not yet known how to pass values of the INTERVAL type")
    def test_null(self):
        interval_table.insert().execute(id=1, native_inverval=None, non_native_interval=None)
        row = interval_table.select().execute().first()
        eq_(row['native_interval'], None)
        eq_(row['native_interval_args'], None)
        eq_(row['non_native_interval'], None)


class BooleanTest(fixtures.TestBase, AssertsExecutionResults):
    @classmethod
    def setup_class(cls):
        global bool_table
        metadata = MetaData(testing.db)
        bool_table = Table('booltest', metadata,
            Column('id', Integer, primary_key=True, autoincrement=False),
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
    @testing.fails_on('mssql', 
            "FIXME: MS-SQL 2005 doesn't honor CHECK ?!?")
    @testing.skip_if(lambda: testing.db.dialect.supports_native_boolean)
    def test_constraint(self):
        assert_raises((exc.IntegrityError, exc.ProgrammingError),
                        testing.db.execute, 
                        "insert into booltest (id, value) values(1, 5)")

    @testing.skip_if(lambda: testing.db.dialect.supports_native_boolean)
    def test_unconstrained(self):
        testing.db.execute(
            "insert into booltest (id, unconstrained_value) values (1, 5)")

class PickleTest(fixtures.TestBase):
    def test_eq_comparison(self):
        p1 = PickleType()

        for obj in (
            {'1':'2'},
            pickleable.Bar(5, 6),
            pickleable.OldSchool(10, 11)
        ):
            assert p1.compare_values(p1.copy_value(obj), obj)

        assert_raises(NotImplementedError, 
                        p1.compare_values,
                        pickleable.BrokenComparable('foo'),
                        pickleable.BrokenComparable('foo'))

    def test_nonmutable_comparison(self):
        p1 = PickleType()

        for obj in (
            {'1':'2'},
            pickleable.Bar(5, 6),
            pickleable.OldSchool(10, 11)
        ):
            assert p1.compare_values(p1.copy_value(obj), obj)

class CallableTest(fixtures.TestBase):
    @classmethod
    def setup_class(cls):
        global meta
        meta = MetaData(testing.db)

    @classmethod
    def teardown_class(cls):
        meta.drop_all()

    def test_callable_as_arg(self):
        ucode = util.partial(Unicode)

        thing_table = Table('thing', meta,
            Column('name', ucode(20))
        )
        assert isinstance(thing_table.c.name.type, Unicode)
        thing_table.create()

    def test_callable_as_kwarg(self):
        ucode = util.partial(Unicode)

        thang_table = Table('thang', meta,
            Column('name', type_=ucode(20), primary_key=True)
        )
        assert isinstance(thang_table.c.name.type, Unicode)
        thang_table.create()

