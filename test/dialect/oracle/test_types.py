# coding: utf-8


import datetime
import decimal
import os
import random

from sqlalchemy import bindparam
from sqlalchemy import cast
from sqlalchemy import CHAR
from sqlalchemy import DATE
from sqlalchemy import Date
from sqlalchemy import DateTime
from sqlalchemy import event
from sqlalchemy import FLOAT
from sqlalchemy import Float
from sqlalchemy import Integer
from sqlalchemy import LargeBinary
from sqlalchemy import MetaData
from sqlalchemy import NCHAR
from sqlalchemy import Numeric
from sqlalchemy import NVARCHAR
from sqlalchemy import select
from sqlalchemy import SmallInteger
from sqlalchemy import String
from sqlalchemy import testing
from sqlalchemy import Text
from sqlalchemy import text
from sqlalchemy import TIMESTAMP
from sqlalchemy import TypeDecorator
from sqlalchemy import types as sqltypes
from sqlalchemy import Unicode
from sqlalchemy import UnicodeText
from sqlalchemy import util
from sqlalchemy import VARCHAR
from sqlalchemy.dialects.oracle import base as oracle
from sqlalchemy.dialects.oracle import cx_oracle
from sqlalchemy.sql import column
from sqlalchemy.sql.sqltypes import NullType
from sqlalchemy.testing import assert_raises_message
from sqlalchemy.testing import AssertsCompiledSQL
from sqlalchemy.testing import eq_
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import is_
from sqlalchemy.testing import mock
from sqlalchemy.testing.engines import testing_engine
from sqlalchemy.testing.schema import Column
from sqlalchemy.testing.schema import Table
from sqlalchemy.util import b
from sqlalchemy.util import py2k
from sqlalchemy.util import u


def exec_sql(conn, sql, *args, **kwargs):
    return conn.exec_driver_sql(sql, *args, **kwargs)


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
        eq_(b.type.dialect_impl(dialect).get_dbapi_type(dbapi), "STRING")

        b = bindparam("foo", "hello world!")
        eq_(b.type.dialect_impl(dialect).get_dbapi_type(dbapi), "STRING")

    def test_long(self):
        self.assert_compile(oracle.LONG(), "LONG")

    @testing.combinations(
        (Date(), cx_oracle._OracleDate),
        (oracle.OracleRaw(), cx_oracle._OracleRaw),
        (String(), String),
        (VARCHAR(), cx_oracle._OracleString),
        (DATE(), cx_oracle._OracleDate),
        (oracle.DATE(), oracle.DATE),
        (String(50), cx_oracle._OracleString),
        (Unicode(), cx_oracle._OracleUnicodeStringCHAR),
        (Text(), cx_oracle._OracleText),
        (UnicodeText(), cx_oracle._OracleUnicodeTextCLOB),
        (CHAR(), cx_oracle._OracleChar),
        (NCHAR(), cx_oracle._OracleNChar),
        (NVARCHAR(), cx_oracle._OracleUnicodeStringNCHAR),
        (oracle.RAW(50), cx_oracle._OracleRaw),
    )
    def test_type_adapt(self, start, test):
        dialect = cx_oracle.dialect()

        assert isinstance(
            start.dialect_impl(dialect), test
        ), "wanted %r got %r" % (test, start.dialect_impl(dialect))

    @testing.combinations(
        (String(), String),
        (VARCHAR(), cx_oracle._OracleString),
        (String(50), cx_oracle._OracleString),
        (Unicode(), cx_oracle._OracleUnicodeStringNCHAR),
        (Text(), cx_oracle._OracleText),
        (UnicodeText(), cx_oracle._OracleUnicodeTextNCLOB),
        (NCHAR(), cx_oracle._OracleNChar),
        (NVARCHAR(), cx_oracle._OracleUnicodeStringNCHAR),
    )
    def test_type_adapt_nchar(self, start, test):
        dialect = cx_oracle.dialect(use_nchar_for_unicode=True)

        assert isinstance(
            start.dialect_impl(dialect), test
        ), "wanted %r got %r" % (test, start.dialect_impl(dialect))

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

    @testing.combinations(
        (String(50), "VARCHAR2(50 CHAR)"),
        (Unicode(50), "VARCHAR2(50 CHAR)"),
        (NVARCHAR(50), "NVARCHAR2(50)"),
        (VARCHAR(50), "VARCHAR(50 CHAR)"),
        (oracle.NVARCHAR2(50), "NVARCHAR2(50)"),
        (oracle.VARCHAR2(50), "VARCHAR2(50 CHAR)"),
        (String(), "VARCHAR2"),
        (Unicode(), "VARCHAR2"),
        (NVARCHAR(), "NVARCHAR2"),
        (VARCHAR(), "VARCHAR"),
        (oracle.NVARCHAR2(), "NVARCHAR2"),
        (oracle.VARCHAR2(), "VARCHAR2"),
    )
    def test_varchar_types(self, typ, exp):
        dialect = oracle.dialect()
        self.assert_compile(typ, exp, dialect=dialect)

    @testing.combinations(
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
    )
    def test_varchar_use_nchar_types(self, typ, exp):
        dialect = oracle.dialect(use_nchar_for_unicode=True)
        self.assert_compile(typ, exp, dialect=dialect)

    @testing.combinations(
        (oracle.INTERVAL(), "INTERVAL DAY TO SECOND"),
        (oracle.INTERVAL(day_precision=3), "INTERVAL DAY(3) TO SECOND"),
        (oracle.INTERVAL(second_precision=5), "INTERVAL DAY TO SECOND(5)"),
        (
            oracle.INTERVAL(day_precision=2, second_precision=5),
            "INTERVAL DAY(2) TO SECOND(5)",
        ),
    )
    def test_interval(self, type_, expected):
        self.assert_compile(type_, expected)


class TypesTest(fixtures.TestBase):
    __only_on__ = "oracle"
    __dialect__ = oracle.OracleDialect()
    __backend__ = True

    @testing.combinations((CHAR,), (NCHAR,), argnames="char_type")
    def test_fixed_char(self, metadata, connection, char_type):
        m = metadata
        t = Table(
            "t1",
            m,
            Column("id", Integer, primary_key=True),
            Column("data", char_type(30), nullable=False),
        )

        if py2k and char_type is NCHAR:
            v1, v2, v3 = u"value 1", u"value 2", u"value 3"
        else:
            v1, v2, v3 = "value 1", "value 2", "value 3"

        t.create(connection)
        connection.execute(
            t.insert(),
            [
                dict(id=1, data=v1),
                dict(id=2, data=v2),
                dict(id=3, data=v3),
            ],
        )

        eq_(
            connection.execute(t.select().where(t.c.data == v2)).fetchall(),
            [(2, "value 2                       ")],
        )

        m2 = MetaData()
        t2 = Table("t1", m2, autoload_with=connection)
        is_(type(t2.c.data.type), char_type)
        eq_(
            connection.execute(t2.select().where(t2.c.data == v2)).fetchall(),
            [(2, "value 2                       ")],
        )

    @testing.requires.returning
    def test_int_not_float(self, metadata, connection):
        m = metadata
        t1 = Table("t1", m, Column("foo", Integer))
        t1.create(connection)
        r = connection.execute(t1.insert().values(foo=5).returning(t1.c.foo))
        x = r.scalar()
        assert x == 5
        assert isinstance(x, int)

        x = connection.scalar(t1.select())
        assert x == 5
        assert isinstance(x, int)

    @testing.requires.returning
    def test_int_not_float_no_coerce_decimal(self, metadata):
        engine = testing_engine(options=dict(coerce_to_decimal=False))

        m = metadata
        t1 = Table("t1", m, Column("foo", Integer))
        with engine.begin() as conn:
            t1.create(conn)
            r = conn.execute(t1.insert().values(foo=5).returning(t1.c.foo))
            x = r.scalar()
            assert x == 5
            assert isinstance(x, int)

            x = conn.execute(t1.select()).scalar()
            assert x == 5
            assert isinstance(x, int)

    def test_rowid(self, metadata, connection):
        t = Table("t1", metadata, Column("x", Integer))

        t.create(connection)
        connection.execute(t.insert(), {"x": 5})
        s1 = select(t).subquery()
        s2 = select(column("rowid")).select_from(s1)
        rowid = connection.scalar(s2)

        # the ROWID type is not really needed here,
        # as cx_oracle just treats it as a string,
        # but we want to make sure the ROWID works...
        rowid_col = column("rowid", oracle.ROWID)
        s3 = select(t.c.x, rowid_col).where(
            rowid_col == cast(rowid, oracle.ROWID)
        )
        eq_(connection.execute(s3).fetchall(), [(5, rowid)])

    def test_interval(self, metadata, connection):
        interval_table = Table(
            "intervaltable",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("day_interval", oracle.INTERVAL(day_precision=3)),
        )
        metadata.create_all(connection)
        connection.execute(
            interval_table.insert(),
            dict(day_interval=datetime.timedelta(days=35, seconds=5743)),
        )
        row = connection.execute(interval_table.select()).first()
        eq_(row["day_interval"], datetime.timedelta(days=35, seconds=5743))

    def test_numerics(self, metadata, connection):
        m = metadata
        t1 = Table(
            "t1",
            m,
            Column("intcol", Integer),
            Column("numericcol", Numeric(precision=9, scale=2)),
            Column("floatcol1", Float()),
            Column("floatcol2", FLOAT()),
            Column("doubleprec", oracle.DOUBLE_PRECISION),
            Column("numbercol1", oracle.NUMBER(9)),
            Column("numbercol2", oracle.NUMBER(9, 3)),
            Column("numbercol3", oracle.NUMBER),
        )
        t1.create(connection)
        connection.execute(
            t1.insert(),
            dict(
                intcol=1,
                numericcol=5.2,
                floatcol1=6.5,
                floatcol2=8.5,
                doubleprec=9.5,
                numbercol1=12,
                numbercol2=14.85,
                numbercol3=15.76,
            ),
        )

        m2 = MetaData()
        t2 = Table("t1", m2, autoload_with=connection)

        for row in (
            connection.execute(t1.select()).first(),
            connection.execute(t2.select()).first(),
        ):
            for i, (val, type_) in enumerate(
                (
                    (1, int),
                    (decimal.Decimal("5.2"), decimal.Decimal),
                    (6.5, float),
                    (8.5, float),
                    (9.5, float),
                    (12, int),
                    (decimal.Decimal("14.85"), decimal.Decimal),
                    (15.76, float),
                )
            ):
                eq_(row[i], val)
                assert isinstance(row[i], type_), "%r is not %r" % (
                    row[i],
                    type_,
                )

    def test_numeric_infinity_float(self, metadata, connection):
        m = metadata
        t1 = Table(
            "t1",
            m,
            Column("intcol", Integer),
            Column("numericcol", oracle.BINARY_DOUBLE(asdecimal=False)),
        )
        t1.create(connection)
        connection.execute(
            t1.insert(),
            [
                dict(intcol=1, numericcol=float("inf")),
                dict(intcol=2, numericcol=float("-inf")),
            ],
        )

        eq_(
            connection.execute(
                select(t1.c.numericcol).order_by(t1.c.intcol)
            ).fetchall(),
            [(float("inf"),), (float("-inf"),)],
        )

        eq_(
            exec_sql(
                connection, "select numericcol from t1 order by intcol"
            ).fetchall(),
            [(float("inf"),), (float("-inf"),)],
        )

    def test_numeric_infinity_decimal(self, metadata, connection):
        m = metadata
        t1 = Table(
            "t1",
            m,
            Column("intcol", Integer),
            Column("numericcol", oracle.BINARY_DOUBLE(asdecimal=True)),
        )
        t1.create(connection)
        connection.execute(
            t1.insert(),
            [
                dict(intcol=1, numericcol=decimal.Decimal("Infinity")),
                dict(intcol=2, numericcol=decimal.Decimal("-Infinity")),
            ],
        )

        eq_(
            connection.execute(
                select(t1.c.numericcol).order_by(t1.c.intcol)
            ).fetchall(),
            [(decimal.Decimal("Infinity"),), (decimal.Decimal("-Infinity"),)],
        )

        eq_(
            exec_sql(
                connection, "select numericcol from t1 order by intcol"
            ).fetchall(),
            [(decimal.Decimal("Infinity"),), (decimal.Decimal("-Infinity"),)],
        )

    def test_numeric_nan_float(self, metadata, connection):
        m = metadata
        t1 = Table(
            "t1",
            m,
            Column("intcol", Integer),
            Column("numericcol", oracle.BINARY_DOUBLE(asdecimal=False)),
        )
        t1.create(connection)
        connection.execute(
            t1.insert(),
            [
                dict(intcol=1, numericcol=float("nan")),
                dict(intcol=2, numericcol=float("-nan")),
            ],
        )

        eq_(
            [
                tuple(str(col) for col in row)
                for row in connection.execute(
                    select(t1.c.numericcol).order_by(t1.c.intcol)
                )
            ],
            [("nan",), ("nan",)],
        )

        eq_(
            [
                tuple(str(col) for col in row)
                for row in exec_sql(
                    connection, "select numericcol from t1 order by intcol"
                )
            ],
            [("nan",), ("nan",)],
        )

    # needs https://github.com/oracle/python-cx_Oracle/
    # issues/184#issuecomment-391399292
    def _dont_test_numeric_nan_decimal(self, metadata, connection):
        m = metadata
        t1 = Table(
            "t1",
            m,
            Column("intcol", Integer),
            Column("numericcol", oracle.BINARY_DOUBLE(asdecimal=True)),
        )
        t1.create()
        t1.insert().execute(
            [
                dict(intcol=1, numericcol=decimal.Decimal("NaN")),
                dict(intcol=2, numericcol=decimal.Decimal("-NaN")),
            ]
        )

        eq_(
            select(t1.c.numericcol).order_by(t1.c.intcol).execute().fetchall(),
            [(decimal.Decimal("NaN"),), (decimal.Decimal("NaN"),)],
        )

        eq_(
            exec_sql(
                connection, "select numericcol from t1 order by intcol"
            ).fetchall(),
            [(decimal.Decimal("NaN"),), (decimal.Decimal("NaN"),)],
        )

    def test_numerics_broken_inspection(self, metadata, connection):
        """Numeric scenarios where Oracle type info is 'broken',
        returning us precision, scale of the form (0, 0) or (0, -127).
        We convert to Decimal and let int()/float() processors take over.

        """

        # this test requires cx_oracle 5

        foo = Table(
            "foo",
            metadata,
            Column("idata", Integer),
            Column("ndata", Numeric(20, 2)),
            Column("ndata2", Numeric(20, 2)),
            Column("nidata", Numeric(5, 0)),
            Column("fdata", Float()),
        )
        foo.create(connection)

        connection.execute(
            foo.insert(),
            {
                "idata": 5,
                "ndata": decimal.Decimal("45.6"),
                "ndata2": decimal.Decimal("45.0"),
                "nidata": decimal.Decimal("53"),
                "fdata": 45.68392,
            },
        )

        stmt = "SELECT idata, ndata, ndata2, nidata, fdata FROM foo"

        row = exec_sql(connection, stmt).fetchall()[0]
        eq_(
            [type(x) for x in row],
            [int, decimal.Decimal, decimal.Decimal, int, float],
        )
        eq_(
            row,
            (
                5,
                decimal.Decimal("45.6"),
                decimal.Decimal("45"),
                53,
                45.683920000000001,
            ),
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
        row = exec_sql(connection, stmt).fetchall()[0]
        eq_(
            [type(x) for x in row],
            [int, decimal.Decimal, int, int, decimal.Decimal],
        )
        eq_(
            row,
            (5, decimal.Decimal("45.6"), 45, 53, decimal.Decimal("45.68392")),
        )

        row = connection.execute(
            text(stmt).columns(
                idata=Integer(),
                ndata=Numeric(20, 2),
                ndata2=Numeric(20, 2),
                nidata=Numeric(5, 0),
                fdata=Float(),
            )
        ).fetchall()[0]
        eq_(
            [type(x) for x in row],
            [int, decimal.Decimal, decimal.Decimal, decimal.Decimal, float],
        )
        eq_(
            row,
            (
                5,
                decimal.Decimal("45.6"),
                decimal.Decimal("45"),
                decimal.Decimal("53"),
                45.683920000000001,
            ),
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
        row = exec_sql(connection, stmt).fetchall()[0]
        eq_(
            [type(x) for x in row],
            [int, decimal.Decimal, int, int, decimal.Decimal],
        )
        eq_(
            row,
            (5, decimal.Decimal("45.6"), 45, 53, decimal.Decimal("45.68392")),
        )

        row = connection.execute(
            text(stmt).columns(
                anon_1_idata=Integer(),
                anon_1_ndata=Numeric(20, 2),
                anon_1_ndata2=Numeric(20, 2),
                anon_1_nidata=Numeric(5, 0),
                anon_1_fdata=Float(),
            )
        ).fetchall()[0]
        eq_(
            [type(x) for x in row],
            [int, decimal.Decimal, decimal.Decimal, decimal.Decimal, float],
        )
        eq_(
            row,
            (
                5,
                decimal.Decimal("45.6"),
                decimal.Decimal("45"),
                decimal.Decimal("53"),
                45.683920000000001,
            ),
        )

        row = connection.execute(
            text(stmt).columns(
                anon_1_idata=Integer(),
                anon_1_ndata=Numeric(20, 2, asdecimal=False),
                anon_1_ndata2=Numeric(20, 2, asdecimal=False),
                anon_1_nidata=Numeric(5, 0, asdecimal=False),
                anon_1_fdata=Float(asdecimal=True),
            )
        ).fetchall()[0]
        eq_(
            [type(x) for x in row], [int, float, float, float, decimal.Decimal]
        )
        eq_(row, (5, 45.6, 45, 53, decimal.Decimal("45.68392")))

    def test_numeric_no_coerce_decimal_mode(self):
        engine = testing_engine(options=dict(coerce_to_decimal=False))
        with engine.connect() as conn:
            # raw SQL no longer coerces to decimal
            value = exec_sql(conn, "SELECT 5.66 FROM DUAL").scalar()
            assert isinstance(value, float)

            # explicit typing still *does* coerce to decimal
            # (change in 1.2)
            value = conn.scalar(
                text("SELECT 5.66 AS foo FROM DUAL").columns(
                    foo=Numeric(4, 2, asdecimal=True)
                )
            )
            assert isinstance(value, decimal.Decimal)

    def test_numeric_coerce_decimal_mode(self, connection):
        # default behavior is raw SQL coerces to decimal
        value = exec_sql(connection, "SELECT 5.66 FROM DUAL").scalar()
        assert isinstance(value, decimal.Decimal)

    @testing.combinations(
        (
            "Max 32-bit Number",
            "SELECT CAST(2147483647 AS NUMBER(19,0)) FROM dual",
        ),
        (
            "Min 32-bit Number",
            "SELECT CAST(-2147483648 AS NUMBER(19,0)) FROM dual",
        ),
        (
            "32-bit Integer Overflow",
            "SELECT CAST(2147483648 AS NUMBER(19,0)) FROM dual",
        ),
        (
            "32-bit Integer Underflow",
            "SELECT CAST(-2147483649 AS NUMBER(19,0)) FROM dual",
        ),
        (
            "Max Number with Precision 19",
            "SELECT CAST(9999999999999999999 AS NUMBER(19,0)) FROM dual",
        ),
        (
            "Min Number with Precision 19",
            "SELECT CAST(-9999999999999999999 AS NUMBER(19,0)) FROM dual",
        ),
    )
    @testing.only_on("oracle+cx_oracle", "cx_oracle-specific feature")
    def test_raw_numerics(self, title, stmt):
        with testing.db.connect() as conn:
            # get a brand new connection that definitely is not
            # in the pool to avoid any outputtypehandlers
            cx_oracle_raw = testing.db.pool._creator()
            cursor = cx_oracle_raw.cursor()
            cursor.execute(stmt)
            cx_oracle_result = cursor.fetchone()[0]
            cursor.close()

            sqla_result = conn.exec_driver_sql(stmt).scalar()

            eq_(sqla_result, cx_oracle_result)

    @testing.only_on("oracle+cx_oracle", "cx_oracle-specific feature")
    @testing.fails_if(
        testing.requires.python3, "cx_oracle always returns unicode on py3k"
    )
    def test_coerce_to_unicode(self, connection):
        engine = testing_engine(options=dict(coerce_to_unicode=False))
        with engine.connect() as conn_no_coerce:
            value = exec_sql(
                conn_no_coerce, "SELECT 'hello' FROM DUAL"
            ).scalar()
            assert isinstance(value, util.binary_type)

        value = exec_sql(connection, "SELECT 'hello' FROM DUAL").scalar()
        assert isinstance(value, util.text_type)

    def test_reflect_dates(self, metadata, connection):
        Table(
            "date_types",
            metadata,
            Column("d1", sqltypes.DATE),
            Column("d2", oracle.DATE),
            Column("d3", TIMESTAMP),
            Column("d4", TIMESTAMP(timezone=True)),
            Column("d5", oracle.INTERVAL(second_precision=5)),
        )
        metadata.create_all(connection)
        m = MetaData()
        t1 = Table("date_types", m, autoload_with=connection)
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
        types_table = Table(
            "all_types",
            MetaData(),
            Column("owner", String(30), primary_key=True),
            Column("type_name", String(30), primary_key=True),
            autoload_with=testing.db,
            oracle_resolve_synonyms=True,
        )
        for row in types_table.select().execute().fetchall():
            [row[k] for k in row.keys()]

    def test_raw_roundtrip(self, metadata, connection):
        raw_table = Table(
            "raw",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("data", oracle.RAW(35)),
        )
        metadata.create_all(connection)
        connection.execute(raw_table.insert(), dict(id=1, data=b("ABCDEF")))
        eq_(connection.execute(raw_table.select()).first(), (1, b("ABCDEF")))

    def test_reflect_nvarchar(self, metadata, connection):
        Table(
            "tnv",
            metadata,
            Column("nv_data", sqltypes.NVARCHAR(255)),
            Column("c_data", sqltypes.NCHAR(20)),
        )
        metadata.create_all(connection)
        m2 = MetaData()
        t2 = Table("tnv", m2, autoload_with=connection)
        assert isinstance(t2.c.nv_data.type, sqltypes.NVARCHAR)
        assert isinstance(t2.c.c_data.type, sqltypes.NCHAR)

        if testing.against("oracle+cx_oracle"):
            assert isinstance(
                t2.c.nv_data.type.dialect_impl(connection.dialect),
                cx_oracle._OracleUnicodeStringNCHAR,
            )

            assert isinstance(
                t2.c.c_data.type.dialect_impl(connection.dialect),
                cx_oracle._OracleNChar,
            )

        data = u("m’a réveillé.")
        connection.execute(t2.insert(), dict(nv_data=data, c_data=data))
        nv_data, c_data = connection.execute(t2.select()).first()
        eq_(nv_data, data)
        eq_(c_data, data + (" " * 7))  # char is space padded
        assert isinstance(nv_data, util.text_type)
        assert isinstance(c_data, util.text_type)

    def test_reflect_unicode_no_nvarchar(self, metadata, connection):
        Table("tnv", metadata, Column("data", sqltypes.Unicode(255)))
        metadata.create_all(connection)
        m2 = MetaData()
        t2 = Table("tnv", m2, autoload_with=connection)
        assert isinstance(t2.c.data.type, sqltypes.VARCHAR)

        if testing.against("oracle+cx_oracle"):
            assert isinstance(
                t2.c.data.type.dialect_impl(connection.dialect),
                cx_oracle._OracleString,
            )

        data = u("m’a réveillé.")
        connection.execute(t2.insert(), {"data": data})
        res = connection.execute(t2.select()).first().data
        eq_(res, data)
        assert isinstance(res, util.text_type)

    def test_char_length(self, metadata, connection):
        t1 = Table(
            "t1",
            metadata,
            Column("c1", VARCHAR(50)),
            Column("c2", NVARCHAR(250)),
            Column("c3", CHAR(200)),
            Column("c4", NCHAR(180)),
        )
        t1.create(connection)
        m2 = MetaData()
        t2 = Table("t1", m2, autoload_with=connection)
        eq_(t2.c.c1.type.length, 50)
        eq_(t2.c.c2.type.length, 250)
        eq_(t2.c.c3.type.length, 200)
        eq_(t2.c.c4.type.length, 180)

    def test_long_type(self, metadata, connection):

        t = Table("t", metadata, Column("data", oracle.LONG))
        metadata.create_all(connection)
        connection.execute(t.insert(), dict(data="xyz"))
        eq_(connection.scalar(select(t.c.data)), "xyz")

    def test_longstring(self, metadata, connection):
        exec_sql(
            connection,
            """
        CREATE TABLE Z_TEST
        (
          ID        NUMERIC(22) PRIMARY KEY,
          ADD_USER  VARCHAR2(20)  NOT NULL
        )
        """,
        )
        try:
            t = Table("z_test", metadata, autoload_with=connection)
            connection.execute(t.insert(), dict(id=1.0, add_user="foobar"))
            assert connection.execute(t.select()).fetchall() == [(1, "foobar")]
        finally:
            exec_sql(connection, "DROP TABLE Z_TEST")


class LOBFetchTest(fixtures.TablesTest):
    __only_on__ = "oracle"
    __backend__ = True

    run_inserts = "once"
    run_deletes = None

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "z_test",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("data", Text),
            Column("bindata", LargeBinary),
        )

        Table(
            "binary_table",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("data", LargeBinary),
        )

    @classmethod
    def insert_data(cls, connection):
        cls.data = data = [
            dict(
                id=i,
                data="this is text %d" % i,
                bindata=b("this is binary %d" % i),
            )
            for i in range(1, 20)
        ]

        connection.execute(cls.tables.z_test.insert(), data)

        binary_table = cls.tables.binary_table
        fname = os.path.join(
            os.path.dirname(__file__), "..", "..", "binary_data_one.dat"
        )
        with open(fname, "rb") as file_:
            cls.stream = stream = file_.read(12000)

        for i in range(1, 11):
            connection.execute(binary_table.insert(), dict(id=i, data=stream))

    def test_lobs_without_convert(self):
        engine = testing_engine(options=dict(auto_convert_lobs=False))
        t = self.tables.z_test
        with engine.begin() as conn:
            row = conn.execute(t.select().where(t.c.id == 1)).first()
            eq_(row["data"].read(), "this is text 1")
            eq_(row["bindata"].read(), b("this is binary 1"))

    def test_lobs_with_convert(self, connection):
        t = self.tables.z_test
        row = connection.execute(t.select().where(t.c.id == 1)).first()
        eq_(row["data"], "this is text 1")
        eq_(row["bindata"], b("this is binary 1"))

    def test_lobs_with_convert_raw(self, connection):
        row = exec_sql(connection, "select data, bindata from z_test").first()
        eq_(row["data"], "this is text 1")
        eq_(row["bindata"], b("this is binary 1"))

    def test_lobs_without_convert_many_rows(self):
        engine = testing_engine(
            options=dict(auto_convert_lobs=False, arraysize=1)
        )
        result = exec_sql(
            engine.connect(),
            "select id, data, bindata from z_test order by id",
        )
        results = result.fetchall()

        def go():
            eq_(
                [
                    dict(
                        id=row["id"],
                        data=row["data"].read(),
                        bindata=row["bindata"].read(),
                    )
                    for row in results
                ],
                self.data,
            )

        # this comes from cx_Oracle because these are raw
        # cx_Oracle.Variable objects
        if testing.requires.oracle5x.enabled:
            assert_raises_message(
                testing.db.dialect.dbapi.ProgrammingError,
                "LOB variable no longer valid after subsequent fetch",
                go,
            )
        else:
            go()

    def test_lobs_with_convert_many_rows(self):
        # even with low arraysize, lobs are fine in autoconvert
        engine = testing_engine(
            options=dict(auto_convert_lobs=True, arraysize=1)
        )
        with engine.connect() as conn:
            result = exec_sql(
                conn,
                "select id, data, bindata from z_test order by id",
            )
            results = result.fetchall()

            eq_(
                [
                    dict(
                        id=row["id"], data=row["data"], bindata=row["bindata"]
                    )
                    for row in results
                ],
                self.data,
            )

    @testing.combinations(
        (UnicodeText(),), (Text(),), (LargeBinary(),), argnames="datatype"
    )
    @testing.combinations((10,), (100,), (250,), argnames="datasize")
    @testing.combinations(
        ("x,y,z"), ("y"), ("y,x,z"), ("x,z,y"), argnames="retcols"
    )
    def test_insert_returning_w_lobs(
        self, datatype, datasize, retcols, metadata, connection
    ):
        long_text = Table(
            "long_text",
            metadata,
            Column("x", Integer),
            Column("y", datatype),
            Column("z", Integer),
        )
        long_text.create(connection)

        if isinstance(datatype, UnicodeText):
            word_seed = u"ab🐍’«cdefg"
        else:
            word_seed = "abcdef"

        some_text = u" ".join(
            "".join(random.choice(word_seed) for j in range(150))
            for i in range(datasize)
        )
        if isinstance(datatype, LargeBinary):
            some_text = some_text.encode("ascii")

        data = {"x": 5, "y": some_text, "z": 10}
        return_columns = [long_text.c[col] for col in retcols.split(",")]
        expected = tuple(data[col] for col in retcols.split(","))
        result = connection.execute(
            long_text.insert().returning(*return_columns),
            data,
        )

        eq_(result.fetchall(), [expected])

    def test_insert_returning_w_unicode(self, metadata, connection):
        long_text = Table(
            "long_text",
            metadata,
            Column("x", Integer),
            Column("y", Unicode(255)),
        )
        long_text.create(connection)

        word_seed = u"ab🐍’«cdefg"

        some_text = u" ".join(
            "".join(random.choice(word_seed) for j in range(10))
            for i in range(15)
        )

        data = {"x": 5, "y": some_text}
        result = connection.execute(
            long_text.insert().returning(long_text.c.y),
            data,
        )

        eq_(result.fetchall(), [(some_text,)])

    def test_large_stream(self, connection):
        binary_table = self.tables.binary_table
        result = connection.execute(
            binary_table.select().order_by(binary_table.c.id)
        ).fetchall()
        eq_(result, [(i, self.stream) for i in range(1, 11)])

    def test_large_stream_single_arraysize(self):
        binary_table = self.tables.binary_table
        eng = testing_engine(options={"arraysize": 1})
        with eng.connect() as conn:
            result = conn.execute(
                binary_table.select().order_by(binary_table.c.id)
            ).fetchall()
            eq_(result, [(i, self.stream) for i in range(1, 11)])


class EuroNumericTest(fixtures.TestBase):
    """
    test the numeric output_type_handler when using non-US locale for NLS_LANG.
    """

    __only_on__ = "oracle+cx_oracle"
    __backend__ = True

    def setup_test(self):
        connect = testing.db.pool._creator

        def _creator():
            conn = connect()
            cursor = conn.cursor()
            cursor.execute("ALTER SESSION SET NLS_TERRITORY='GERMANY'")
            cursor.close()
            return conn

        self.engine = testing_engine(options={"creator": _creator})

    def teardown_test(self):
        self.engine.dispose()

    def test_were_getting_a_comma(self):
        connection = self.engine.pool._creator()
        cursor = connection.cursor()
        try:
            cx_Oracle = self.engine.dialect.dbapi

            def output_type_handler(
                cursor, name, defaultType, size, precision, scale
            ):
                return cursor.var(
                    cx_Oracle.STRING, 255, arraysize=cursor.arraysize
                )

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
                (
                    "SELECT CAST(15 AS NUMERIC(3, 1)) FROM DUAL",
                    decimal.Decimal("15"),
                    {},
                ),
                (
                    "SELECT CAST(0.1 AS NUMERIC(5, 2)) FROM DUAL",
                    decimal.Decimal("0.1"),
                    {},
                ),
                (
                    "SELECT :num FROM DUAL",
                    decimal.Decimal("2.5"),
                    {"num": decimal.Decimal("2.5")},
                ),
                (
                    text(
                        "SELECT CAST(28.532 AS NUMERIC(5, 3)) "
                        "AS val FROM DUAL"
                    ).columns(val=Numeric(5, 3, asdecimal=True)),
                    decimal.Decimal("28.532"),
                    {},
                ),
            ]:
                if isinstance(stmt, util.string_types):
                    test_exp = conn.exec_driver_sql(stmt, kw).scalar()
                else:
                    test_exp = conn.scalar(stmt, **kw)
                eq_(test_exp, exp)
                assert type(test_exp) is type(exp)


class SetInputSizesTest(fixtures.TestBase):
    __only_on__ = "oracle+cx_oracle"
    __backend__ = True

    @testing.combinations(
        (SmallInteger, 25, int, False),
        (Integer, 25, int, False),
        (Numeric(10, 8), decimal.Decimal("25.34534"), None, False),
        (Float(15), 25.34534, None, False),
        (oracle.BINARY_DOUBLE, 25.34534, "NATIVE_FLOAT", False),
        (oracle.BINARY_FLOAT, 25.34534, "NATIVE_FLOAT", False),
        (oracle.DOUBLE_PRECISION, 25.34534, None, False),
        (Unicode(30), u("test"), "NCHAR", True),
        (UnicodeText(), u("test"), "NCLOB", True),
        (Unicode(30), u("test"), None, False),
        (UnicodeText(), u("test"), "CLOB", False),
        (String(30), "test", None, False),
        (CHAR(30), "test", "FIXED_CHAR", False),
        (NCHAR(30), u("test"), "FIXED_NCHAR", False),
        (oracle.LONG(), "test", None, False),
        argnames="datatype, value, sis_value_text, set_nchar_flag",
    )
    def test_setinputsizes(
        self, metadata, datatype, value, sis_value_text, set_nchar_flag
    ):
        if isinstance(sis_value_text, str):
            sis_value = getattr(testing.db.dialect.dbapi, sis_value_text)
        else:
            sis_value = sis_value_text

        class TestTypeDec(TypeDecorator):
            impl = NullType()

            def load_dialect_impl(self, dialect):
                if dialect.name == "oracle":
                    return dialect.type_descriptor(datatype)
                else:
                    return self.impl

        m = metadata
        # Oracle can have only one column of type LONG so we make three
        # tables rather than one table w/ three columns
        t1 = Table("t1", m, Column("foo", datatype))
        t2 = Table(
            "t2", m, Column("foo", NullType().with_variant(datatype, "oracle"))
        )
        t3 = Table("t3", m, Column("foo", TestTypeDec()))

        class CursorWrapper(object):
            # cx_oracle cursor can't be modified so we have to
            # invent a whole wrapping scheme

            def __init__(self, connection_fairy):
                self.cursor = connection_fairy.connection.cursor()
                self.mock = mock.Mock()
                connection_fairy.info["mock"] = self.mock

            def setinputsizes(self, *arg, **kw):
                self.mock.setinputsizes(*arg, **kw)
                self.cursor.setinputsizes(*arg, **kw)

            def __getattr__(self, key):
                return getattr(self.cursor, key)

        if set_nchar_flag:
            engine = testing_engine(options={"use_nchar_for_unicode": True})
        else:
            engine = testing.db

        with engine.connect() as conn:
            conn.begin()

            m.create_all(conn, checkfirst=False)

            connection_fairy = conn.connection
            for tab in [t1, t2, t3]:
                with mock.patch.object(
                    connection_fairy,
                    "cursor",
                    lambda: CursorWrapper(connection_fairy),
                ):
                    conn.execute(tab.insert(), {"foo": value})

                if sis_value:
                    eq_(
                        conn.info["mock"].mock_calls,
                        [mock.call.setinputsizes(foo=sis_value)],
                    )
                else:
                    eq_(
                        conn.info["mock"].mock_calls,
                        [mock.call.setinputsizes()],
                    )

    def test_event_no_native_float(self, metadata):
        def _remove_type(inputsizes, cursor, statement, parameters, context):
            for param, dbapitype in list(inputsizes.items()):
                if dbapitype is testing.db.dialect.dbapi.NATIVE_FLOAT:
                    del inputsizes[param]

        event.listen(testing.db, "do_setinputsizes", _remove_type)
        try:
            self.test_setinputsizes(
                metadata, oracle.BINARY_FLOAT, 25.34534, None, False
            )
        finally:
            event.remove(testing.db, "do_setinputsizes", _remove_type)
