# -*- encoding: utf-8
from sqlalchemy.testing import eq_, engines, pickleable, assert_raises_message
import datetime
import os
from sqlalchemy import Table, Column, MetaData, Float, \
    Integer, String, Boolean, TIMESTAMP, Sequence, Numeric, select, \
    Date, Time, DateTime, DefaultClause, PickleType, text, Text, \
    UnicodeText, LargeBinary
from sqlalchemy import types, schema
from sqlalchemy.databases import mssql
from sqlalchemy.dialects.mssql.base import TIME, _MSDate
from sqlalchemy.dialects.mssql.base import MS_2005_VERSION, MS_2008_VERSION
from sqlalchemy.testing import fixtures, \
    AssertsExecutionResults, ComparesTables
from sqlalchemy import testing
from sqlalchemy.testing import emits_warning_on
import decimal
from sqlalchemy.util import b


class TimeTypeTest(fixtures.TestBase):

    def test_result_processor_no_microseconds(self):
        expected = datetime.time(12, 34, 56)
        self._assert_result_processor(expected, '12:34:56')

    def test_result_processor_too_many_microseconds(self):
        # microsecond must be in 0..999999, should truncate (6 vs 7 digits)
        expected = datetime.time(12, 34, 56, 123456)
        self._assert_result_processor(expected, '12:34:56.1234567')

    def _assert_result_processor(self, expected, value):
        mssql_time_type = TIME()
        result_processor = mssql_time_type.result_processor(None, None)
        eq_(expected, result_processor(value))

    def test_result_processor_invalid(self):
        mssql_time_type = TIME()
        result_processor = mssql_time_type.result_processor(None, None)
        assert_raises_message(
            ValueError,
            "could not parse 'abc' as a time value",
            result_processor, 'abc'
        )


class MSDateTypeTest(fixtures.TestBase):

    def test_result_processor(self):
        expected = datetime.date(2000, 1, 2)
        self._assert_result_processor(expected, '2000-01-02')

    def _assert_result_processor(self, expected, value):
        mssql_date_type = _MSDate()
        result_processor = mssql_date_type.result_processor(None, None)
        eq_(expected, result_processor(value))

    def test_result_processor_invalid(self):
        mssql_date_type = _MSDate()
        result_processor = mssql_date_type.result_processor(None, None)
        assert_raises_message(
            ValueError,
            "could not parse 'abc' as a date value",
            result_processor, 'abc'
        )

    def test_extract(self):
        from sqlalchemy import extract
        fivedaysago = datetime.datetime.now() \
            - datetime.timedelta(days=5)
        for field, exp in ('year', fivedaysago.year), \
                ('month', fivedaysago.month), ('day', fivedaysago.day):
            r = testing.db.execute(
                select([
                    extract(field, fivedaysago)])
            ).scalar()
            eq_(r, exp)


class TypeDDLTest(fixtures.TestBase):

    def test_boolean(self):
        "Exercise type specification for boolean type."

        columns = [
            # column type, args, kwargs, expected ddl
            (Boolean, [], {},
             'BIT'),
        ]

        metadata = MetaData()
        table_args = ['test_mssql_boolean', metadata]
        for index, spec in enumerate(columns):
            type_, args, kw, res = spec
            table_args.append(
                Column('c%s' % index, type_(*args, **kw), nullable=None))

        boolean_table = Table(*table_args)
        dialect = mssql.dialect()
        gen = dialect.ddl_compiler(dialect, schema.CreateTable(boolean_table))

        for col in boolean_table.c:
            index = int(col.name[1:])
            testing.eq_(
                gen.get_column_specification(col),
                "%s %s" % (col.name, columns[index][3]))
            self.assert_(repr(col))

    def test_numeric(self):
        "Exercise type specification and options for numeric types."

        columns = [
            # column type, args, kwargs, expected ddl
            (types.NUMERIC, [], {},
             'NUMERIC'),
            (types.NUMERIC, [None], {},
             'NUMERIC'),
            (types.NUMERIC, [12, 4], {},
             'NUMERIC(12, 4)'),

            (types.Float, [], {},
             'FLOAT'),
            (types.Float, [None], {},
             'FLOAT'),
            (types.Float, [12], {},
             'FLOAT(12)'),
            (mssql.MSReal, [], {},
             'REAL'),

            (types.Integer, [], {},
             'INTEGER'),
            (types.BigInteger, [], {},
             'BIGINT'),
            (mssql.MSTinyInteger, [], {},
             'TINYINT'),
            (types.SmallInteger, [], {},
             'SMALLINT'),
        ]

        metadata = MetaData()
        table_args = ['test_mssql_numeric', metadata]
        for index, spec in enumerate(columns):
            type_, args, kw, res = spec
            table_args.append(
                Column('c%s' % index, type_(*args, **kw), nullable=None))

        numeric_table = Table(*table_args)
        dialect = mssql.dialect()
        gen = dialect.ddl_compiler(dialect, schema.CreateTable(numeric_table))

        for col in numeric_table.c:
            index = int(col.name[1:])
            testing.eq_(
                gen.get_column_specification(col),
                "%s %s" % (col.name, columns[index][3]))
            self.assert_(repr(col))

    def test_char(self):
        """Exercise COLLATE-ish options on string types."""

        columns = [
            (mssql.MSChar, [], {},
             'CHAR'),
            (mssql.MSChar, [1], {},
             'CHAR(1)'),
            (mssql.MSChar, [1], {'collation': 'Latin1_General_CI_AS'},
             'CHAR(1) COLLATE Latin1_General_CI_AS'),

            (mssql.MSNChar, [], {},
             'NCHAR'),
            (mssql.MSNChar, [1], {},
             'NCHAR(1)'),
            (mssql.MSNChar, [1], {'collation': 'Latin1_General_CI_AS'},
             'NCHAR(1) COLLATE Latin1_General_CI_AS'),

            (mssql.MSString, [], {},
             'VARCHAR(max)'),
            (mssql.MSString, [1], {},
             'VARCHAR(1)'),
            (mssql.MSString, [1], {'collation': 'Latin1_General_CI_AS'},
             'VARCHAR(1) COLLATE Latin1_General_CI_AS'),

            (mssql.MSNVarchar, [], {},
             'NVARCHAR(max)'),
            (mssql.MSNVarchar, [1], {},
             'NVARCHAR(1)'),
            (mssql.MSNVarchar, [1], {'collation': 'Latin1_General_CI_AS'},
             'NVARCHAR(1) COLLATE Latin1_General_CI_AS'),

            (mssql.MSText, [], {},
             'TEXT'),
            (mssql.MSText, [], {'collation': 'Latin1_General_CI_AS'},
             'TEXT COLLATE Latin1_General_CI_AS'),

            (mssql.MSNText, [], {},
             'NTEXT'),
            (mssql.MSNText, [], {'collation': 'Latin1_General_CI_AS'},
             'NTEXT COLLATE Latin1_General_CI_AS'),
        ]

        metadata = MetaData()
        table_args = ['test_mssql_charset', metadata]
        for index, spec in enumerate(columns):
            type_, args, kw, res = spec
            table_args.append(
                Column('c%s' % index, type_(*args, **kw), nullable=None))

        charset_table = Table(*table_args)
        dialect = mssql.dialect()
        gen = dialect.ddl_compiler(dialect, schema.CreateTable(charset_table))

        for col in charset_table.c:
            index = int(col.name[1:])
            testing.eq_(
                gen.get_column_specification(col),
                "%s %s" % (col.name, columns[index][3]))
            self.assert_(repr(col))

    def test_dates(self):
        "Exercise type specification for date types."

        columns = [
            # column type, args, kwargs, expected ddl
            (mssql.MSDateTime, [], {},
             'DATETIME', None),

            (types.DATE, [], {},
             'DATE', None),
            (types.Date, [], {},
             'DATE', None),
            (types.Date, [], {},
             'DATETIME', MS_2005_VERSION),
            (mssql.MSDate, [], {},
             'DATE', None),
            (mssql.MSDate, [], {},
             'DATETIME', MS_2005_VERSION),

            (types.TIME, [], {},
             'TIME', None),
            (types.Time, [], {},
             'TIME', None),
            (mssql.MSTime, [], {},
             'TIME', None),
            (mssql.MSTime, [1], {},
             'TIME(1)', None),
            (types.Time, [], {},
             'DATETIME', MS_2005_VERSION),
            (mssql.MSTime, [], {},
             'TIME', None),

            (mssql.MSSmallDateTime, [], {},
             'SMALLDATETIME', None),

            (mssql.MSDateTimeOffset, [], {},
             'DATETIMEOFFSET', None),
            (mssql.MSDateTimeOffset, [1], {},
             'DATETIMEOFFSET(1)', None),

            (mssql.MSDateTime2, [], {},
             'DATETIME2', None),
            (mssql.MSDateTime2, [0], {},
             'DATETIME2(0)', None),
            (mssql.MSDateTime2, [1], {},
             'DATETIME2(1)', None),

            (mssql.MSTime, [0], {},
             'TIME(0)', None),

            (mssql.MSDateTimeOffset, [0], {},
             'DATETIMEOFFSET(0)', None),

        ]

        metadata = MetaData()
        table_args = ['test_mssql_dates', metadata]
        for index, spec in enumerate(columns):
            type_, args, kw, res, server_version = spec
            table_args.append(
                Column('c%s' % index, type_(*args, **kw), nullable=None))

        date_table = Table(*table_args)
        dialect = mssql.dialect()
        dialect.server_version_info = MS_2008_VERSION
        ms_2005_dialect = mssql.dialect()
        ms_2005_dialect.server_version_info = MS_2005_VERSION
        gen = dialect.ddl_compiler(dialect, schema.CreateTable(date_table))
        gen2005 = ms_2005_dialect.ddl_compiler(
            ms_2005_dialect, schema.CreateTable(date_table))

        for col in date_table.c:
            index = int(col.name[1:])
            server_version = columns[index][4]
            if not server_version:
                testing.eq_(
                    gen.get_column_specification(col),
                    "%s %s" % (col.name, columns[index][3]))
            else:
                testing.eq_(
                    gen2005.get_column_specification(col),
                    "%s %s" % (col.name, columns[index][3]))

            self.assert_(repr(col))

    def test_large_type_deprecation(self):
        d1 = mssql.dialect(deprecate_large_types=True)
        d2 = mssql.dialect(deprecate_large_types=False)
        d3 = mssql.dialect()
        d3.server_version_info = (11, 0)
        d3._setup_version_attributes()
        d4 = mssql.dialect()
        d4.server_version_info = (10, 0)
        d4._setup_version_attributes()

        for dialect in (d1, d3):
            eq_(
                str(Text().compile(dialect=dialect)),
                "VARCHAR(max)"
            )
            eq_(
                str(UnicodeText().compile(dialect=dialect)),
                "NVARCHAR(max)"
            )
            eq_(
                str(LargeBinary().compile(dialect=dialect)),
                "VARBINARY(max)"
            )

        for dialect in (d2, d4):
            eq_(
                str(Text().compile(dialect=dialect)),
                "TEXT"
            )
            eq_(
                str(UnicodeText().compile(dialect=dialect)),
                "NTEXT"
            )
            eq_(
                str(LargeBinary().compile(dialect=dialect)),
                "IMAGE"
            )

    def test_timestamp(self):
        """Exercise TIMESTAMP column."""

        dialect = mssql.dialect()

        metadata = MetaData()
        spec, expected = (TIMESTAMP, 'TIMESTAMP')
        t = Table(
            'mssql_ts', metadata,
            Column('id', Integer, primary_key=True),
            Column('t', spec, nullable=None))
        gen = dialect.ddl_compiler(dialect, schema.CreateTable(t))
        testing.eq_(gen.get_column_specification(t.c.t), "t %s" % expected)
        self.assert_(repr(t.c.t))

    def test_money(self):
        """Exercise type specification for money types."""

        columns = [(mssql.MSMoney, [], {}, 'MONEY'),
                   (mssql.MSSmallMoney, [], {}, 'SMALLMONEY')]
        metadata = MetaData()
        table_args = ['test_mssql_money', metadata]
        for index, spec in enumerate(columns):
            type_, args, kw, res = spec
            table_args.append(Column('c%s' % index, type_(*args, **kw),
                              nullable=None))
        money_table = Table(*table_args)
        dialect = mssql.dialect()
        gen = dialect.ddl_compiler(dialect,
                                   schema.CreateTable(money_table))
        for col in money_table.c:
            index = int(col.name[1:])
            testing.eq_(gen.get_column_specification(col), '%s %s'
                        % (col.name, columns[index][3]))
            self.assert_(repr(col))

    def test_binary(self):
        "Exercise type specification for binary types."

        columns = [
            # column type, args, kwargs, expected ddl
            (mssql.MSBinary, [], {},
             'BINARY'),
            (mssql.MSBinary, [10], {},
             'BINARY(10)'),

            (types.BINARY, [], {},
             'BINARY'),
            (types.BINARY, [10], {},
             'BINARY(10)'),

            (mssql.MSVarBinary, [], {},
             'VARBINARY(max)'),
            (mssql.MSVarBinary, [10], {},
             'VARBINARY(10)'),

            (types.VARBINARY, [10], {},
             'VARBINARY(10)'),
            (types.VARBINARY, [], {},
             'VARBINARY(max)'),

            (mssql.MSImage, [], {},
             'IMAGE'),

            (mssql.IMAGE, [], {},
             'IMAGE'),

            (types.LargeBinary, [], {},
             'IMAGE'),
        ]

        metadata = MetaData()
        table_args = ['test_mssql_binary', metadata]
        for index, spec in enumerate(columns):
            type_, args, kw, res = spec
            table_args.append(Column('c%s' % index, type_(*args, **kw),
                              nullable=None))
        binary_table = Table(*table_args)
        dialect = mssql.dialect()
        gen = dialect.ddl_compiler(dialect,
                                   schema.CreateTable(binary_table))
        for col in binary_table.c:
            index = int(col.name[1:])
            testing.eq_(gen.get_column_specification(col), '%s %s'
                        % (col.name, columns[index][3]))
            self.assert_(repr(col))

metadata = None


class TypeRoundTripTest(
        fixtures.TestBase, AssertsExecutionResults, ComparesTables):
    __only_on__ = 'mssql'

    @classmethod
    def setup_class(cls):
        global metadata
        metadata = MetaData(testing.db)

    def teardown(self):
        metadata.drop_all()

    @testing.fails_on_everything_except(
        'mssql+pyodbc',
        'this is some pyodbc-specific feature')
    def test_decimal_notation(self):
        numeric_table = Table(
            'numeric_table', metadata,
            Column(
                'id', Integer,
                Sequence('numeric_id_seq', optional=True), primary_key=True),
            Column(
                'numericcol',
                Numeric(precision=38, scale=20, asdecimal=True)))
        metadata.create_all()
        test_items = [decimal.Decimal(d) for d in (
            '1500000.00000000000000000000',
            '-1500000.00000000000000000000',
            '1500000',
            '0.0000000000000000002',
            '0.2',
            '-0.0000000000000000002',
            '-2E-2',
            '156666.458923543',
            '-156666.458923543',
            '1',
            '-1',
            '-1234',
            '1234',
            '2E-12',
            '4E8',
            '3E-6',
            '3E-7',
            '4.1',
            '1E-1',
            '1E-2',
            '1E-3',
            '1E-4',
            '1E-5',
            '1E-6',
            '1E-7',
            '1E-1',
            '1E-8',
            '0.2732E2',
            '-0.2432E2',
            '4.35656E2',
            '-02452E-2',
            '45125E-2',
            '1234.58965E-2',
            '1.521E+15',
            '-1E-25',
            '1E-25',
            '1254E-25',
            '-1203E-25',
            '0',
            '-0.00',
            '-0',
            '4585E12',
            '000000000000000000012',
            '000000000000.32E12',
            '00000000000000.1E+12',
            '000000000000.2E-32',
        )]

        for value in test_items:
            numeric_table.insert().execute(numericcol=value)

        for value in select([numeric_table.c.numericcol]).execute():
            assert value[0] in test_items, "%r not in test_items" % value[0]

    def test_float(self):
        float_table = Table(
            'float_table', metadata,
            Column(
                'id', Integer,
                Sequence('numeric_id_seq', optional=True), primary_key=True),
            Column('floatcol', Float()))

        metadata.create_all()
        try:
            test_items = [float(d) for d in (
                '1500000.00000000000000000000',
                '-1500000.00000000000000000000',
                '1500000',
                '0.0000000000000000002',
                '0.2',
                '-0.0000000000000000002',
                '156666.458923543',
                '-156666.458923543',
                '1',
                '-1',
                '1234',
                '2E-12',
                '4E8',
                '3E-6',
                '3E-7',
                '4.1',
                '1E-1',
                '1E-2',
                '1E-3',
                '1E-4',
                '1E-5',
                '1E-6',
                '1E-7',
                '1E-8',
            )]
            for value in test_items:
                float_table.insert().execute(floatcol=value)
        except Exception as e:
            raise e

    # todo this should suppress warnings, but it does not
    @emits_warning_on('mssql+mxodbc', r'.*does not have any indexes.*')
    def test_dates(self):
        "Exercise type specification for date types."

        columns = [
            # column type, args, kwargs, expected ddl
            (mssql.MSDateTime, [], {},
             'DATETIME', []),

            (types.DATE, [], {},
             'DATE', ['>=', (10,)]),
            (types.Date, [], {},
             'DATE', ['>=', (10,)]),
            (types.Date, [], {},
             'DATETIME', ['<', (10,)], mssql.MSDateTime),
            (mssql.MSDate, [], {},
             'DATE', ['>=', (10,)]),
            (mssql.MSDate, [], {},
             'DATETIME', ['<', (10,)], mssql.MSDateTime),

            (types.TIME, [], {},
             'TIME', ['>=', (10,)]),
            (types.Time, [], {},
             'TIME', ['>=', (10,)]),
            (mssql.MSTime, [], {},
             'TIME', ['>=', (10,)]),
            (mssql.MSTime, [1], {},
             'TIME(1)', ['>=', (10,)]),
            (types.Time, [], {},
             'DATETIME', ['<', (10,)], mssql.MSDateTime),
            (mssql.MSTime, [], {},
             'TIME', ['>=', (10,)]),

            (mssql.MSSmallDateTime, [], {},
             'SMALLDATETIME', []),

            (mssql.MSDateTimeOffset, [], {},
             'DATETIMEOFFSET', ['>=', (10,)]),
            (mssql.MSDateTimeOffset, [1], {},
             'DATETIMEOFFSET(1)', ['>=', (10,)]),

            (mssql.MSDateTime2, [], {},
             'DATETIME2', ['>=', (10,)]),
            (mssql.MSDateTime2, [0], {},
             'DATETIME2(0)', ['>=', (10,)]),
            (mssql.MSDateTime2, [1], {},
             'DATETIME2(1)', ['>=', (10,)]),

        ]

        table_args = ['test_mssql_dates', metadata]
        for index, spec in enumerate(columns):
            type_, args, kw, res, requires = spec[0:5]
            if requires and \
                    testing._is_excluded('mssql', *requires) or not requires:
                c = Column('c%s' % index, type_(*args, **kw), nullable=None)
                testing.db.dialect.type_descriptor(c.type)
                table_args.append(c)
        dates_table = Table(*table_args)
        gen = testing.db.dialect.ddl_compiler(
            testing.db.dialect,
            schema.CreateTable(dates_table))
        for col in dates_table.c:
            index = int(col.name[1:])
            testing.eq_(gen.get_column_specification(col), '%s %s'
                        % (col.name, columns[index][3]))
            self.assert_(repr(col))
        dates_table.create(checkfirst=True)
        reflected_dates = Table('test_mssql_dates',
                                MetaData(testing.db), autoload=True)
        for col in reflected_dates.c:
            self.assert_types_base(col, dates_table.c[col.key])

    def test_date_roundtrip(self):
        t = Table(
            'test_dates', metadata,
            Column('id', Integer,
                   Sequence('datetest_id_seq', optional=True),
                   primary_key=True),
            Column('adate', Date),
            Column('atime', Time),
            Column('adatetime', DateTime))
        metadata.create_all()
        d1 = datetime.date(2007, 10, 30)
        t1 = datetime.time(11, 2, 32)
        d2 = datetime.datetime(2007, 10, 30, 11, 2, 32)
        t.insert().execute(adate=d1, adatetime=d2, atime=t1)
        t.insert().execute(adate=d2, adatetime=d2, atime=d2)

        x = t.select().execute().fetchall()[0]
        self.assert_(x.adate.__class__ == datetime.date)
        self.assert_(x.atime.__class__ == datetime.time)
        self.assert_(x.adatetime.__class__ == datetime.datetime)

        t.delete().execute()

        t.insert().execute(adate=d1, adatetime=d2, atime=t1)

        eq_(select([t.c.adate, t.c.atime, t.c.adatetime], t.c.adate
            == d1).execute().fetchall(), [(d1, t1, d2)])

    @emits_warning_on('mssql+mxodbc', r'.*does not have any indexes.*')
    @testing.provide_metadata
    def _test_binary_reflection(self, deprecate_large_types):
        "Exercise type specification for binary types."

        columns = [
            # column type, args, kwargs, expected ddl from reflected
            (mssql.MSBinary, [], {},
             'BINARY(1)'),
            (mssql.MSBinary, [10], {},
             'BINARY(10)'),

            (types.BINARY, [], {},
             'BINARY(1)'),
            (types.BINARY, [10], {},
             'BINARY(10)'),

            (mssql.MSVarBinary, [], {},
             'VARBINARY(max)'),
            (mssql.MSVarBinary, [10], {},
             'VARBINARY(10)'),

            (types.VARBINARY, [10], {},
             'VARBINARY(10)'),
            (types.VARBINARY, [], {},
             'VARBINARY(max)'),

            (mssql.MSImage, [], {},
             'IMAGE'),

            (mssql.IMAGE, [], {},
             'IMAGE'),

            (types.LargeBinary, [], {},
             'IMAGE' if not deprecate_large_types else 'VARBINARY(max)'),
        ]

        metadata = self.metadata
        metadata.bind = engines.testing_engine(
            options={"deprecate_large_types": deprecate_large_types})
        table_args = ['test_mssql_binary', metadata]
        for index, spec in enumerate(columns):
            type_, args, kw, res = spec
            table_args.append(Column('c%s' % index, type_(*args, **kw),
                              nullable=None))
        binary_table = Table(*table_args)
        metadata.create_all()
        reflected_binary = Table('test_mssql_binary',
                                 MetaData(testing.db), autoload=True)
        for col, spec in zip(reflected_binary.c, columns):
            eq_(
                str(col.type), spec[3],
                "column %s %s != %s" % (col.key, str(col.type), spec[3])
            )
            c1 = testing.db.dialect.type_descriptor(col.type).__class__
            c2 = \
                testing.db.dialect.type_descriptor(
                    binary_table.c[col.name].type).__class__
            assert issubclass(c1, c2), \
                'column %s: %r is not a subclass of %r' \
                % (col.key, c1, c2)
            if binary_table.c[col.name].type.length:
                testing.eq_(col.type.length,
                            binary_table.c[col.name].type.length)

    def test_binary_reflection_legacy_large_types(self):
        self._test_binary_reflection(False)

    @testing.only_on('mssql >= 11')
    def test_binary_reflection_sql2012_large_types(self):
        self._test_binary_reflection(True)

    def test_autoincrement(self):
        Table(
            'ai_1', metadata,
            Column('int_y', Integer, primary_key=True),
            Column(
                'int_n', Integer, DefaultClause('0'),
                primary_key=True, autoincrement=False))
        Table(
            'ai_2', metadata,
            Column('int_y', Integer, primary_key=True),
            Column('int_n', Integer, DefaultClause('0'),
                   primary_key=True, autoincrement=False))
        Table(
            'ai_3', metadata,
            Column('int_n', Integer, DefaultClause('0'),
                   primary_key=True, autoincrement=False),
            Column('int_y', Integer, primary_key=True))

        Table(
            'ai_4', metadata,
            Column('int_n', Integer, DefaultClause('0'),
                   primary_key=True, autoincrement=False),
            Column('int_n2', Integer, DefaultClause('0'),
                   primary_key=True, autoincrement=False))
        Table(
            'ai_5', metadata,
            Column('int_y', Integer, primary_key=True),
            Column('int_n', Integer, DefaultClause('0'),
                   primary_key=True, autoincrement=False))
        Table(
            'ai_6', metadata,
            Column('o1', String(1), DefaultClause('x'),
                   primary_key=True),
            Column('int_y', Integer, primary_key=True))
        Table(
            'ai_7', metadata,
            Column('o1', String(1), DefaultClause('x'),
                   primary_key=True),
            Column('o2', String(1), DefaultClause('x'),
                   primary_key=True),
            Column('int_y', Integer, primary_key=True))
        Table(
            'ai_8', metadata,
            Column('o1', String(1), DefaultClause('x'),
                   primary_key=True),
            Column('o2', String(1), DefaultClause('x'),
                   primary_key=True))
        metadata.create_all()

        table_names = ['ai_1', 'ai_2', 'ai_3', 'ai_4',
                       'ai_5', 'ai_6', 'ai_7', 'ai_8']
        mr = MetaData(testing.db)

        for name in table_names:
            tbl = Table(name, mr, autoload=True)
            tbl = metadata.tables[name]
            for c in tbl.c:
                if c.name.startswith('int_y'):
                    assert c.autoincrement, name
                    assert tbl._autoincrement_column is c, name
                elif c.name.startswith('int_n'):
                    assert not c.autoincrement, name
                    assert tbl._autoincrement_column is not c, name

            # mxodbc can't handle scope_identity() with DEFAULT VALUES

            if testing.db.driver == 'mxodbc':
                eng = \
                    [engines.testing_engine(options={
                        'implicit_returning': True})]
            else:
                eng = \
                    [engines.testing_engine(options={
                        'implicit_returning': False}),
                     engines.testing_engine(options={
                         'implicit_returning': True})]

            for counter, engine in enumerate(eng):
                engine.execute(tbl.insert())
                if 'int_y' in tbl.c:
                    assert engine.scalar(select([tbl.c.int_y])) \
                        == counter + 1
                    assert list(
                        engine.execute(tbl.select()).first()).\
                        count(counter + 1) == 1
                else:
                    assert 1 \
                        not in list(engine.execute(tbl.select()).first())
                engine.execute(tbl.delete())


class MonkeyPatchedBinaryTest(fixtures.TestBase):
    __only_on__ = 'mssql+pymssql'

    def test_unicode(self):
        module = __import__('pymssql')
        result = module.Binary('foo')
        eq_(result, 'foo')

    def test_bytes(self):
        module = __import__('pymssql')
        input = b('\x80\x03]q\x00X\x03\x00\x00\x00oneq\x01a.')
        expected_result = input
        result = module.Binary(input)
        eq_(result, expected_result)

binary_table = None
MyPickleType = None


class BinaryTest(fixtures.TestBase, AssertsExecutionResults):

    """Test the Binary and VarBinary types"""

    __only_on__ = 'mssql'

    @classmethod
    def setup_class(cls):
        global MyPickleType

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

    def teardown(self):
        self.binary_table.drop(testing.db)

    def _fixture(self, engine):
        self.binary_table = binary_table = Table(
            'binary_table',
            MetaData(),
            Column('primary_id', Integer, Sequence('binary_id_seq',
                   optional=True), primary_key=True),
            Column('data', mssql.MSVarBinary(8000)),
            Column('data_image', mssql.MSImage),
            Column('data_slice', types.BINARY(100)),
            Column('misc', String(30)),
            Column('pickled', PickleType),
            Column('mypickle', MyPickleType),
        )
        binary_table.create(engine)
        return binary_table

    def test_binary_legacy_types(self):
        self._test_binary(False)

    @testing.only_on('mssql >= 11')
    def test_binary_updated_types(self):
        self._test_binary(True)

    def test_binary_none_legacy_types(self):
        self._test_binary_none(False)

    @testing.only_on('mssql >= 11')
    def test_binary_none_updated_types(self):
        self._test_binary_none(True)

    def _test_binary(self, deprecate_large_types):
        testobj1 = pickleable.Foo('im foo 1')
        testobj2 = pickleable.Foo('im foo 2')
        testobj3 = pickleable.Foo('im foo 3')
        stream1 = self._load_stream('binary_data_one.dat')
        stream2 = self._load_stream('binary_data_two.dat')
        engine = engines.testing_engine(
            options={"deprecate_large_types": deprecate_large_types})

        binary_table = self._fixture(engine)

        with engine.connect() as conn:
            conn.execute(
                binary_table.insert(),
                primary_id=1,
                misc='binary_data_one.dat',
                data=stream1,
                data_image=stream1,
                data_slice=stream1[0:100],
                pickled=testobj1,
                mypickle=testobj3,
            )
            conn.execute(
                binary_table.insert(),
                primary_id=2,
                misc='binary_data_two.dat',
                data=stream2,
                data_image=stream2,
                data_slice=stream2[0:99],
                pickled=testobj2,
            )

        for stmt in \
            binary_table.select(order_by=binary_table.c.primary_id), \
                text(
                    'select * from binary_table order by '
                    'binary_table.primary_id',
                    typemap=dict(
                        data=mssql.MSVarBinary(8000),
                        data_image=mssql.MSImage,
                        data_slice=types.BINARY(100), pickled=PickleType,
                        mypickle=MyPickleType),
                    bind=testing.db):
            with engine.connect() as conn:
                l = conn.execute(stmt).fetchall()
            eq_(list(stream1), list(l[0]['data']))
            paddedstream = list(stream1[0:100])
            paddedstream.extend(['\x00'] * (100 - len(paddedstream)))
            eq_(paddedstream, list(l[0]['data_slice']))
            eq_(list(stream2), list(l[1]['data']))
            eq_(list(stream2), list(l[1]['data_image']))
            eq_(testobj1, l[0]['pickled'])
            eq_(testobj2, l[1]['pickled'])
            eq_(testobj3.moredata, l[0]['mypickle'].moredata)
            eq_(l[0]['mypickle'].stuff, 'this is the right stuff')

    def _test_binary_none(self, deprecate_large_types):
        engine = engines.testing_engine(
            options={"deprecate_large_types": deprecate_large_types})

        binary_table = self._fixture(engine)

        stream2 = self._load_stream('binary_data_two.dat')

        with engine.connect() as conn:
            conn.execute(
                binary_table.insert(),
                primary_id=3,
                misc='binary_data_two.dat', data_image=None,
                data_slice=stream2[0:99], pickled=None)
            for stmt in \
                binary_table.select(), \
                    text(
                        'select * from binary_table',
                        typemap=dict(
                            data=mssql.MSVarBinary(8000),
                            data_image=mssql.MSImage,
                            data_slice=types.BINARY(100),
                            pickled=PickleType,
                            mypickle=MyPickleType),
                        bind=testing.db):
                row = conn.execute(stmt).first()
                eq_(
                    row['pickled'], None
                )
                eq_(
                    row['data_image'], None
                )

                # the type we used here is 100 bytes
                # so we will get 100 bytes zero-padded
                paddedstream = list(stream2[0:99])
                paddedstream.extend(['\x00'] * (100 - len(paddedstream)))
                eq_(
                    list(row['data_slice']), paddedstream
                )

    def _load_stream(self, name, len=3000):
        fp = open(
            os.path.join(os.path.dirname(__file__), "..", "..", name), 'rb')
        stream = fp.read(len)
        fp.close()
        return stream
