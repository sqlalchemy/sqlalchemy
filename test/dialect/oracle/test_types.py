# coding: utf-8


from sqlalchemy.testing import eq_
from sqlalchemy import types as sqltypes, exc, schema
from sqlalchemy.sql import table, column
from sqlalchemy.testing import (fixtures,
                                AssertsExecutionResults,
                                AssertsCompiledSQL)
from sqlalchemy import testing
from sqlalchemy import Integer, Text, LargeBinary, Unicode, UniqueConstraint,\
    Index, MetaData, select, inspect, ForeignKey, String, func, \
    TypeDecorator, bindparam, Numeric, TIMESTAMP, CHAR, text, \
    literal_column, VARCHAR, create_engine, Date, NVARCHAR, \
    ForeignKeyConstraint, Sequence, Float, DateTime, cast, UnicodeText, \
    union, except_, type_coerce, or_, outerjoin, DATE, NCHAR, outparam, \
    PrimaryKeyConstraint, FLOAT
from sqlalchemy.util import u, b
from sqlalchemy import util
from sqlalchemy.testing import assert_raises, assert_raises_message
from sqlalchemy.testing.engines import testing_engine
from sqlalchemy.dialects.oracle import cx_oracle, base as oracle
from sqlalchemy.engine import default
import decimal
from sqlalchemy.engine import url
from sqlalchemy.testing.schema import Table, Column
import datetime
import os
from sqlalchemy import sql
from sqlalchemy.testing.mock import Mock


class DialectTypesTest(fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = oracle.OracleDialect()

    def test_no_clobs_for_string_params(self):
        """test that simple string params get a DBAPI type of
        VARCHAR, not CLOB. This is to prevent setinputsizes
        from setting up cx_oracle.CLOBs on
        string-based bind params [ticket:793]."""

        class FakeDBAPI(object):
            def __getattr__(self, attr):
                return attr

        dialect = oracle.OracleDialect()
        dbapi = FakeDBAPI()

        b = bindparam("foo", "hello world!")
        eq_(
            b.type.dialect_impl(dialect).get_dbapi_type(dbapi),
            'STRING'
        )

        b = bindparam("foo", "hello world!")
        eq_(
            b.type.dialect_impl(dialect).get_dbapi_type(dbapi),
            'STRING'
        )

    def test_long(self):
        self.assert_compile(oracle.LONG(), "LONG")

    def test_type_adapt(self):
        dialect = cx_oracle.dialect()

        for start, test in [
            (Date(), cx_oracle._OracleDate),
            (oracle.OracleRaw(), cx_oracle._OracleRaw),
            (String(), String),
            (VARCHAR(), cx_oracle._OracleString),
            (DATE(), cx_oracle._OracleDate),
            (oracle.DATE(), oracle.DATE),
            (String(50), cx_oracle._OracleString),
            (Unicode(), cx_oracle._OracleNVarChar),
            (Text(), cx_oracle._OracleText),
            (UnicodeText(), cx_oracle._OracleUnicodeText),
            (NCHAR(), cx_oracle._OracleNVarChar),
            (oracle.RAW(50), cx_oracle._OracleRaw),
        ]:
            assert isinstance(start.dialect_impl(dialect), test), \
                    "wanted %r got %r" % (test, start.dialect_impl(dialect))

    def test_raw_compile(self):
        self.assert_compile(oracle.RAW(), "RAW")
        self.assert_compile(oracle.RAW(35), "RAW(35)")

    def test_char_length(self):
        self.assert_compile(VARCHAR(50), "VARCHAR(50 CHAR)")

        oracle8dialect = oracle.dialect()
        oracle8dialect.server_version_info = (8, 0)
        self.assert_compile(VARCHAR(50), "VARCHAR(50)", dialect=oracle8dialect)

        self.assert_compile(NVARCHAR(50), "NVARCHAR2(50)")
        self.assert_compile(CHAR(50), "CHAR(50)")

    def test_varchar_types(self):
        dialect = oracle.dialect()
        for typ, exp in [
            (String(50), "VARCHAR2(50 CHAR)"),
            (Unicode(50), "NVARCHAR2(50)"),
            (NVARCHAR(50), "NVARCHAR2(50)"),
            (VARCHAR(50), "VARCHAR(50 CHAR)"),
            (oracle.NVARCHAR2(50), "NVARCHAR2(50)"),
            (oracle.VARCHAR2(50), "VARCHAR2(50 CHAR)"),
            (String(), "VARCHAR2"),
            (Unicode(), "NVARCHAR2"),
            (NVARCHAR(), "NVARCHAR2"),
            (VARCHAR(), "VARCHAR"),
            (oracle.NVARCHAR2(), "NVARCHAR2"),
            (oracle.VARCHAR2(), "VARCHAR2"),
        ]:
            self.assert_compile(typ, exp, dialect=dialect)

    def test_interval(self):
        for type_, expected in [(oracle.INTERVAL(),
                                'INTERVAL DAY TO SECOND'),
                                (oracle.INTERVAL(day_precision=3),
                                'INTERVAL DAY(3) TO SECOND'),
                                (oracle.INTERVAL(second_precision=5),
                                'INTERVAL DAY TO SECOND(5)'),
                                (oracle.INTERVAL(day_precision=2,
                                 second_precision=5),
                                'INTERVAL DAY(2) TO SECOND(5)')]:
            self.assert_compile(type_, expected)


class TypesTest(fixtures.TestBase):
    __only_on__ = 'oracle'
    __dialect__ = oracle.OracleDialect()
    __backend__ = True

    @testing.fails_on('+zxjdbc', 'zxjdbc lacks the FIXED_CHAR dbapi type')
    def test_fixed_char(self):
        m = MetaData(testing.db)
        t = Table('t1', m,
                  Column('id', Integer, primary_key=True),
                  Column('data', CHAR(30), nullable=False))

        t.create()
        try:
            t.insert().execute(
                dict(id=1, data="value 1"),
                dict(id=2, data="value 2"),
                dict(id=3, data="value 3")
            )

            eq_(
                t.select().where(t.c.data == 'value 2').execute().fetchall(),
                [(2, 'value 2                       ')]
            )

            m2 = MetaData(testing.db)
            t2 = Table('t1', m2, autoload=True)
            assert type(t2.c.data.type) is CHAR
            eq_(
                t2.select().where(t2.c.data == 'value 2').execute().fetchall(),
                [(2, 'value 2                       ')]
            )

        finally:
            t.drop()

    @testing.requires.returning
    @testing.provide_metadata
    def test_int_not_float(self):
        m = self.metadata
        t1 = Table('t1', m, Column('foo', Integer))
        t1.create()
        r = t1.insert().values(foo=5).returning(t1.c.foo).execute()
        x = r.scalar()
        assert x == 5
        assert isinstance(x, int)

        x = t1.select().scalar()
        assert x == 5
        assert isinstance(x, int)

    @testing.requires.returning
    @testing.provide_metadata
    def test_int_not_float_no_coerce_decimal(self):
        engine = testing_engine(options=dict(coerce_to_decimal=False))

        m = self.metadata
        t1 = Table('t1', m, Column('foo', Integer))
        t1.create()
        r = engine.execute(t1.insert().values(foo=5).returning(t1.c.foo))
        x = r.scalar()
        assert x == 5
        assert isinstance(x, int)

        x = t1.select().scalar()
        assert x == 5
        assert isinstance(x, int)

    @testing.provide_metadata
    def test_rowid(self):
        metadata = self.metadata
        t = Table('t1', metadata, Column('x', Integer))
        t.create()
        t.insert().execute(x=5)
        s1 = select([t])
        s2 = select([column('rowid')]).select_from(s1)
        rowid = s2.scalar()

        # the ROWID type is not really needed here,
        # as cx_oracle just treats it as a string,
        # but we want to make sure the ROWID works...
        rowid_col = column('rowid', oracle.ROWID)
        s3 = select([t.c.x, rowid_col]) \
            .where(rowid_col == cast(rowid, oracle.ROWID))
        eq_(s3.select().execute().fetchall(), [(5, rowid)])

    @testing.fails_on('+zxjdbc',
                      'Not yet known how to pass values of the '
                      'INTERVAL type')
    @testing.provide_metadata
    def test_interval(self):
        metadata = self.metadata
        interval_table = Table('intervaltable', metadata, Column('id',
                               Integer, primary_key=True,
                               test_needs_autoincrement=True),
                               Column('day_interval',
                               oracle.INTERVAL(day_precision=3)))
        metadata.create_all()
        interval_table.insert().\
            execute(day_interval=datetime.timedelta(days=35, seconds=5743))
        row = interval_table.select().execute().first()
        eq_(row['day_interval'], datetime.timedelta(days=35,
            seconds=5743))

    @testing.provide_metadata
    def test_numerics(self):
        m = self.metadata
        t1 = Table('t1', m,
                   Column('intcol', Integer),
                   Column('numericcol', Numeric(precision=9, scale=2)),
                   Column('floatcol1', Float()),
                   Column('floatcol2', FLOAT()),
                   Column('doubleprec', oracle.DOUBLE_PRECISION),
                   Column('numbercol1', oracle.NUMBER(9)),
                   Column('numbercol2', oracle.NUMBER(9, 3)),
                   Column('numbercol3', oracle.NUMBER))
        t1.create()
        t1.insert().execute(
            intcol=1,
            numericcol=5.2,
            floatcol1=6.5,
            floatcol2=8.5,
            doubleprec=9.5,
            numbercol1=12,
            numbercol2=14.85,
            numbercol3=15.76
            )

        m2 = MetaData(testing.db)
        t2 = Table('t1', m2, autoload=True)

        for row in (
            t1.select().execute().first(),
            t2.select().execute().first()
        ):
            for i, (val, type_) in enumerate((
                (1, int),
                (decimal.Decimal("5.2"), decimal.Decimal),
                (6.5, float),
                (8.5, float),
                (9.5, float),
                (12, int),
                (decimal.Decimal("14.85"), decimal.Decimal),
                (15.76, float),
            )):
                eq_(row[i], val)
                assert isinstance(row[i], type_), '%r is not %r' \
                    % (row[i], type_)

    @testing.provide_metadata
    def test_numeric_infinity_float(self):
        m = self.metadata
        t1 = Table('t1', m,
                   Column("intcol", Integer),
                   Column("numericcol", oracle.BINARY_DOUBLE(asdecimal=False)))
        t1.create()
        t1.insert().execute([
            dict(
                intcol=1,
                numericcol=float("inf")
            ),
            dict(
                intcol=2,
                numericcol=float("-inf")
            ),
        ])

        eq_(
            select([t1.c.numericcol]).
            order_by(t1.c.intcol).execute().fetchall(),
            [(float('inf'), ), (float('-inf'), )]
        )

        eq_(
            testing.db.execute(
                "select numericcol from t1 order by intcol").fetchall(),
            [(float('inf'), ), (float('-inf'), )]
        )

    @testing.provide_metadata
    def test_numeric_infinity_decimal(self):
        m = self.metadata
        t1 = Table('t1', m,
                   Column("intcol", Integer),
                   Column("numericcol", oracle.BINARY_DOUBLE(asdecimal=True)))
        t1.create()
        t1.insert().execute([
            dict(
                intcol=1,
                numericcol=decimal.Decimal("Infinity")
            ),
            dict(
                intcol=2,
                numericcol=decimal.Decimal("-Infinity")
            ),
        ])

        eq_(
            select([t1.c.numericcol]).
            order_by(t1.c.intcol).execute().fetchall(),
            [(decimal.Decimal("Infinity"), ), (decimal.Decimal("-Infinity"), )]
        )

        eq_(
            testing.db.execute(
                "select numericcol from t1 order by intcol").fetchall(),
            [(decimal.Decimal("Infinity"), ), (decimal.Decimal("-Infinity"), )]
        )

    @testing.provide_metadata
    def test_numerics_broken_inspection(self):
        """Numeric scenarios where Oracle type info is 'broken',
        returning us precision, scale of the form (0, 0) or (0, -127).
        We convert to Decimal and let int()/float() processors take over.

        """

        metadata = self.metadata

        # this test requires cx_oracle 5

        foo = Table('foo', metadata,
                    Column('idata', Integer),
                    Column('ndata', Numeric(20, 2)),
                    Column('ndata2', Numeric(20, 2)),
                    Column('nidata', Numeric(5, 0)),
                    Column('fdata', Float()))
        foo.create()

        foo.insert().execute({
            'idata': 5,
            'ndata': decimal.Decimal("45.6"),
            'ndata2': decimal.Decimal("45.0"),
            'nidata': decimal.Decimal('53'),
            'fdata': 45.68392
        })

        stmt = "SELECT idata, ndata, ndata2, nidata, fdata FROM foo"

        row = testing.db.execute(stmt).fetchall()[0]
        eq_(
            [type(x) for x in row],
            [int, decimal.Decimal, decimal.Decimal, int, float]
        )
        eq_(
            row,
            (5, decimal.Decimal('45.6'), decimal.Decimal('45'),
                53, 45.683920000000001)
        )

        # with a nested subquery,
        # both Numeric values that don't have decimal places, regardless
        # of their originating type, come back as ints with no useful
        # typing information beyond "numeric".  So native handler
        # must convert to int.
        # this means our Decimal converters need to run no matter what.
        # totally sucks.

        stmt = """
        SELECT
            (SELECT (SELECT idata FROM foo) FROM DUAL) AS idata,
            (SELECT CAST((SELECT ndata FROM foo) AS NUMERIC(20, 2)) FROM DUAL)
            AS ndata,
            (SELECT CAST((SELECT ndata2 FROM foo) AS NUMERIC(20, 2)) FROM DUAL)
            AS ndata2,
            (SELECT CAST((SELECT nidata FROM foo) AS NUMERIC(5, 0)) FROM DUAL)
            AS nidata,
            (SELECT CAST((SELECT fdata FROM foo) AS FLOAT) FROM DUAL) AS fdata
        FROM dual
        """
        row = testing.db.execute(stmt).fetchall()[0]
        eq_(
            [type(x) for x in row],
            [int, decimal.Decimal, int, int, decimal.Decimal]
        )
        eq_(
            row,
            (5, decimal.Decimal('45.6'), 45, 53, decimal.Decimal('45.68392'))
        )

        row = testing.db.execute(text(stmt,
                                      typemap={
                                          'idata': Integer(),
                                          'ndata': Numeric(20, 2),
                                          'ndata2': Numeric(20, 2),
                                          'nidata': Numeric(5, 0),
                                          'fdata': Float()})).fetchall()[0]
        eq_(
            [type(x) for x in row],
            [int, decimal.Decimal, decimal.Decimal, decimal.Decimal, float]
        )
        eq_(
            row,
            (5, decimal.Decimal('45.6'), decimal.Decimal('45'),
                decimal.Decimal('53'), 45.683920000000001)
        )

        stmt = """
        SELECT
                anon_1.idata AS anon_1_idata,
                anon_1.ndata AS anon_1_ndata,
                anon_1.ndata2 AS anon_1_ndata2,
                anon_1.nidata AS anon_1_nidata,
                anon_1.fdata AS anon_1_fdata
        FROM (SELECT idata, ndata, ndata2, nidata, fdata
        FROM (
            SELECT
                (SELECT (SELECT idata FROM foo) FROM DUAL) AS idata,
                (SELECT CAST((SELECT ndata FROM foo) AS NUMERIC(20, 2))
                FROM DUAL) AS ndata,
                (SELECT CAST((SELECT ndata2 FROM foo) AS NUMERIC(20, 2))
                FROM DUAL) AS ndata2,
                (SELECT CAST((SELECT nidata FROM foo) AS NUMERIC(5, 0))
                FROM DUAL) AS nidata,
                (SELECT CAST((SELECT fdata FROM foo) AS FLOAT) FROM DUAL)
                AS fdata
            FROM dual
        )
        WHERE ROWNUM >= 0) anon_1
        """
        row = testing.db.execute(stmt).fetchall()[0]
        eq_(
            [type(x) for x in row],
            [int, decimal.Decimal, int, int, decimal.Decimal]
        )
        eq_(
            row,
            (5, decimal.Decimal('45.6'), 45, 53, decimal.Decimal('45.68392'))
        )

        row = testing.db.execute(text(stmt,
                                      typemap={
                                          'anon_1_idata': Integer(),
                                          'anon_1_ndata': Numeric(20, 2),
                                          'anon_1_ndata2': Numeric(20, 2),
                                          'anon_1_nidata': Numeric(5, 0),
                                          'anon_1_fdata': Float()
                                      })).fetchall()[0]
        eq_(
            [type(x) for x in row],
            [int, decimal.Decimal, decimal.Decimal, decimal.Decimal, float]
        )
        eq_(
            row,
            (5, decimal.Decimal('45.6'), decimal.Decimal('45'),
                decimal.Decimal('53'), 45.683920000000001)
        )

        row = testing.db.execute(text(
            stmt,
            typemap={
                'anon_1_idata': Integer(),
                'anon_1_ndata': Numeric(20, 2, asdecimal=False),
                'anon_1_ndata2': Numeric(20, 2, asdecimal=False),
                'anon_1_nidata': Numeric(5, 0, asdecimal=False),
                'anon_1_fdata': Float(asdecimal=True)
            })).fetchall()[0]
        eq_(
            [type(x) for x in row],
            [int, float, float, float, decimal.Decimal]
        )
        eq_(
            row,
            (5, 45.6, 45, 53, decimal.Decimal('45.68392'))
        )

    def test_numeric_no_coerce_decimal_mode(self):
        engine = testing_engine(options=dict(coerce_to_decimal=False))

        # raw SQL no longer coerces to decimal
        value = engine.scalar("SELECT 5.66 FROM DUAL")
        assert isinstance(value, float)

        # explicit typing still *does* coerce to decimal
        # (change in 1.2)
        value = engine.scalar(
            text("SELECT 5.66 AS foo FROM DUAL").
            columns(foo=Numeric(4, 2, asdecimal=True)))
        assert isinstance(value, decimal.Decimal)

        # default behavior is raw SQL coerces to decimal
        value = testing.db.scalar("SELECT 5.66 FROM DUAL")
        assert isinstance(value, decimal.Decimal)

    @testing.only_on("oracle+cx_oracle", "cx_oracle-specific feature")
    @testing.fails_if(
        testing.requires.python3,
        "cx_oracle always returns unicode on py3k")
    def test_coerce_to_unicode(self):
        engine = testing_engine(options=dict(coerce_to_unicode=True))
        value = engine.scalar("SELECT 'hello' FROM DUAL")
        assert isinstance(value, util.text_type)

        value = testing.db.scalar("SELECT 'hello' FROM DUAL")
        assert isinstance(value, util.binary_type)

    @testing.provide_metadata
    def test_reflect_dates(self):
        metadata = self.metadata
        Table(
            "date_types", metadata,
            Column('d1', sqltypes.DATE),
            Column('d2', oracle.DATE),
            Column('d3', TIMESTAMP),
            Column('d4', TIMESTAMP(timezone=True)),
            Column('d5', oracle.INTERVAL(second_precision=5)),
        )
        metadata.create_all()
        m = MetaData(testing.db)
        t1 = Table(
            "date_types", m,
            autoload=True)
        assert isinstance(t1.c.d1.type, oracle.DATE)
        assert isinstance(t1.c.d1.type, DateTime)
        assert isinstance(t1.c.d2.type, oracle.DATE)
        assert isinstance(t1.c.d2.type, DateTime)
        assert isinstance(t1.c.d3.type, TIMESTAMP)
        assert not t1.c.d3.type.timezone
        assert isinstance(t1.c.d4.type, TIMESTAMP)
        assert t1.c.d4.type.timezone
        assert isinstance(t1.c.d5.type, oracle.INTERVAL)

    def _dont_test_reflect_all_types_schema(self):
        types_table = Table('all_types', MetaData(testing.db),
                            Column('owner', String(30), primary_key=True),
                            Column('type_name', String(30), primary_key=True),
                            autoload=True, oracle_resolve_synonyms=True)
        for row in types_table.select().execute().fetchall():
            [row[k] for k in row.keys()]

    @testing.provide_metadata
    def test_raw_roundtrip(self):
        metadata = self.metadata
        raw_table = Table('raw', metadata,
                          Column('id', Integer, primary_key=True),
                          Column('data', oracle.RAW(35)))
        metadata.create_all()
        testing.db.execute(raw_table.insert(), id=1, data=b("ABCDEF"))
        eq_(
            testing.db.execute(raw_table.select()).first(),
            (1, b("ABCDEF"))
        )

    @testing.provide_metadata
    def test_reflect_nvarchar(self):
        metadata = self.metadata
        Table('tnv', metadata, Column('data', sqltypes.NVARCHAR(255)))
        metadata.create_all()
        m2 = MetaData(testing.db)
        t2 = Table('tnv', m2, autoload=True)
        assert isinstance(t2.c.data.type, sqltypes.NVARCHAR)

        if testing.against('oracle+cx_oracle'):
            # nvarchar returns unicode natively.  cx_oracle
            # _OracleNVarChar type should be at play here.
            assert isinstance(
                t2.c.data.type.dialect_impl(testing.db.dialect),
                cx_oracle._OracleNVarChar)

        data = u('m’a réveillé.')
        t2.insert().execute(data=data)
        res = t2.select().execute().first()['data']
        eq_(res, data)
        assert isinstance(res, util.text_type)

    @testing.provide_metadata
    def test_char_length(self):
        metadata = self.metadata
        t1 = Table('t1', metadata,
                   Column("c1", VARCHAR(50)),
                   Column("c2", NVARCHAR(250)),
                   Column("c3", CHAR(200)))
        t1.create()
        m2 = MetaData(testing.db)
        t2 = Table('t1', m2, autoload=True)
        eq_(t2.c.c1.type.length, 50)
        eq_(t2.c.c2.type.length, 250)
        eq_(t2.c.c3.type.length, 200)

    @testing.provide_metadata
    def test_long_type(self):
        metadata = self.metadata

        t = Table('t', metadata, Column('data', oracle.LONG))
        metadata.create_all(testing.db)
        testing.db.execute(t.insert(), data='xyz')
        eq_(
            testing.db.scalar(select([t.c.data])),
            "xyz"
        )

    def test_longstring(self):
        metadata = MetaData(testing.db)
        testing.db.execute("""
        CREATE TABLE Z_TEST
        (
          ID        NUMERIC(22) PRIMARY KEY,
          ADD_USER  VARCHAR2(20)  NOT NULL
        )
        """)
        try:
            t = Table("z_test", metadata, autoload=True)
            t.insert().execute(id=1.0, add_user='foobar')
            assert t.select().execute().fetchall() == [(1, 'foobar')]
        finally:
            testing.db.execute("DROP TABLE Z_TEST")


class LOBFetchTest(fixtures.TablesTest):
    __only_on__ = 'oracle'
    __backend__ = True

    run_inserts = 'once'
    run_deletes = None

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "z_test",
            metadata,
            Column('id', Integer, primary_key=True),
            Column('data', Text),
            Column('bindata', LargeBinary))

        Table(
            'binary_table', metadata,
            Column('id', Integer, primary_key=True),
            Column('data', LargeBinary)
        )

    @classmethod
    def insert_data(cls):
        cls.data = data = [
            dict(
                id=i, data='this is text %d' % i,
                bindata=b('this is binary %d' % i)
            ) for i in range(1, 20)
        ]

        testing.db.execute(cls.tables.z_test.insert(), data)

        binary_table = cls.tables.binary_table
        fname = os.path.join(
            os.path.dirname(__file__), "..", "..",
            'binary_data_one.dat')
        with open(fname, "rb") as file_:
            cls.stream = stream = file_.read(12000)

        for i in range(1, 11):
            binary_table.insert().execute(id=i, data=stream)

    def test_lobs_without_convert(self):
        engine = testing_engine(options=dict(auto_convert_lobs=False))
        t = self.tables.z_test
        row = engine.execute(t.select().where(t.c.id == 1)).first()
        eq_(row['data'].read(), 'this is text 1')
        eq_(row['bindata'].read(), b('this is binary 1'))

    def test_lobs_with_convert(self):
        t = self.tables.z_test
        row = testing.db.execute(t.select().where(t.c.id == 1)).first()
        eq_(row['data'], 'this is text 1')
        eq_(row['bindata'], b('this is binary 1'))

    def test_lobs_with_convert_raw(self):
        row = testing.db.execute("select data, bindata from z_test").first()
        eq_(row['data'], 'this is text 1')
        eq_(row['bindata'], b('this is binary 1'))

    def test_lobs_without_convert_many_rows(self):
        engine = testing_engine(
            options=dict(auto_convert_lobs=False, arraysize=1))
        result = engine.execute(
            "select id, data, bindata from z_test order by id")
        results = result.fetchall()

        def go():
            eq_(
                [
                    dict(
                        id=row["id"],
                        data=row["data"].read(),
                        bindata=row["bindata"].read()
                    ) for row in results
                ],
                self.data)
        # this comes from cx_Oracle because these are raw
        # cx_Oracle.Variable objects
        if testing.requires.oracle5x.enabled:
            assert_raises_message(
                testing.db.dialect.dbapi.ProgrammingError,
                "LOB variable no longer valid after subsequent fetch",
                go
            )
        else:
            go()

    def test_lobs_with_convert_many_rows(self):
        # even with low arraysize, lobs are fine in autoconvert
        engine = testing_engine(
            options=dict(auto_convert_lobs=True, arraysize=1))
        result = engine.execute(
            "select id, data, bindata from z_test order by id")
        results = result.fetchall()

        eq_(
            [
                dict(
                    id=row["id"],
                    data=row["data"],
                    bindata=row["bindata"]
                ) for row in results
            ],
            self.data)

    def test_large_stream(self):
        binary_table = self.tables.binary_table
        result = binary_table.select().order_by(binary_table.c.id).\
            execute().fetchall()
        eq_(result, [(i, self.stream) for i in range(1, 11)])

    def test_large_stream_single_arraysize(self):
        binary_table = self.tables.binary_table
        eng = testing_engine(options={'arraysize': 1})
        result = eng.execute(binary_table.select().
                             order_by(binary_table.c.id)).fetchall()
        eq_(result, [(i, self.stream) for i in range(1, 11)])


class EuroNumericTest(fixtures.TestBase):
    """
    test the numeric output_type_handler when using non-US locale for NLS_LANG.
    """

    __only_on__ = 'oracle+cx_oracle'
    __backend__ = True

    def setup(self):
        connect = testing.db.pool._creator

        def _creator():
            conn = connect()
            cursor = conn.cursor()
            cursor.execute("ALTER SESSION SET NLS_TERRITORY='GERMANY'")
            cursor.close()
            return conn

        self.engine = testing_engine(options={"creator": _creator})

    def teardown(self):
        self.engine.dispose()

    def test_were_getting_a_comma(self):
        connection = self.engine.pool._creator()
        cursor = connection.cursor()
        try:
            cx_Oracle = self.engine.dialect.dbapi

            def output_type_handler(cursor, name, defaultType,
                                    size, precision, scale):
                return cursor.var(cx_Oracle.STRING, 255,
                                  arraysize=cursor.arraysize)
            cursor.outputtypehandler = output_type_handler
            cursor.execute("SELECT 1.1 FROM DUAL")
            row = cursor.fetchone()
            eq_(row[0], "1,1")
        finally:
            cursor.close()
            connection.close()

    def test_output_type_handler(self):
        with self.engine.connect() as conn:
            for stmt, exp, kw in [
                ("SELECT 0.1 FROM DUAL", decimal.Decimal("0.1"), {}),
                ("SELECT CAST(15 AS INTEGER) FROM DUAL", 15, {}),
                ("SELECT CAST(15 AS NUMERIC(3, 1)) FROM DUAL",
                 decimal.Decimal("15"), {}),
                ("SELECT CAST(0.1 AS NUMERIC(5, 2)) FROM DUAL",
                 decimal.Decimal("0.1"), {}),
                ("SELECT :num FROM DUAL", decimal.Decimal("2.5"),
                 {'num': decimal.Decimal("2.5")}),

                (
                    text(
                        "SELECT CAST(28.532 AS NUMERIC(5, 3)) "
                        "AS val FROM DUAL").columns(
                        val=Numeric(5, 3, asdecimal=True)),
                    decimal.Decimal("28.532"), {}
                )
            ]:
                test_exp = conn.scalar(stmt, **kw)
                eq_(
                    test_exp,
                    exp
                )
                assert type(test_exp) is type(exp)


