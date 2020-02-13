# -*- encoding: utf-8
import codecs
import datetime
import decimal
import os

import sqlalchemy as sa
from sqlalchemy import Boolean
from sqlalchemy import Column
from sqlalchemy import column
from sqlalchemy import Date
from sqlalchemy import DateTime
from sqlalchemy import DefaultClause
from sqlalchemy import Float
from sqlalchemy import inspect
from sqlalchemy import Integer
from sqlalchemy import LargeBinary
from sqlalchemy import literal
from sqlalchemy import MetaData
from sqlalchemy import Numeric
from sqlalchemy import PickleType
from sqlalchemy import schema
from sqlalchemy import select
from sqlalchemy import Sequence
from sqlalchemy import String
from sqlalchemy import Table
from sqlalchemy import testing
from sqlalchemy import Text
from sqlalchemy import text
from sqlalchemy import Time
from sqlalchemy import types
from sqlalchemy import Unicode
from sqlalchemy import UnicodeText
from sqlalchemy import util
from sqlalchemy.databases import mssql
from sqlalchemy.dialects.mssql import ROWVERSION
from sqlalchemy.dialects.mssql import TIMESTAMP
from sqlalchemy.dialects.mssql.base import _MSDate
from sqlalchemy.dialects.mssql.base import DATETIMEOFFSET
from sqlalchemy.dialects.mssql.base import MS_2005_VERSION
from sqlalchemy.dialects.mssql.base import MS_2008_VERSION
from sqlalchemy.dialects.mssql.base import TIME
from sqlalchemy.sql import sqltypes
from sqlalchemy.testing import assert_raises
from sqlalchemy.testing import assert_raises_message
from sqlalchemy.testing import AssertsCompiledSQL
from sqlalchemy.testing import AssertsExecutionResults
from sqlalchemy.testing import ComparesTables
from sqlalchemy.testing import emits_warning_on
from sqlalchemy.testing import engines
from sqlalchemy.testing import eq_
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import is_
from sqlalchemy.testing import is_not_
from sqlalchemy.testing import pickleable
from sqlalchemy.util import b


class TimeTypeTest(fixtures.TestBase):
    def test_result_processor_no_microseconds(self):
        expected = datetime.time(12, 34, 56)
        self._assert_result_processor(expected, "12:34:56")

    def test_result_processor_too_many_microseconds(self):
        # microsecond must be in 0..999999, should truncate (6 vs 7 digits)
        expected = datetime.time(12, 34, 56, 123456)
        self._assert_result_processor(expected, "12:34:56.1234567")

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
            result_processor,
            "abc",
        )


class MSDateTypeTest(fixtures.TestBase):
    __only_on__ = "mssql"
    __backend__ = True

    def test_result_processor(self):
        expected = datetime.date(2000, 1, 2)
        self._assert_result_processor(expected, "2000-01-02")

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
            result_processor,
            "abc",
        )

    def test_extract(self):
        from sqlalchemy import extract

        fivedaysago = datetime.datetime.now() - datetime.timedelta(days=5)
        for field, exp in (
            ("year", fivedaysago.year),
            ("month", fivedaysago.month),
            ("day", fivedaysago.day),
        ):
            r = testing.db.execute(
                select([extract(field, fivedaysago)])
            ).scalar()
            eq_(r, exp)


class RowVersionTest(fixtures.TablesTest):
    __only_on__ = "mssql"
    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "rv_t",
            metadata,
            Column("data", String(50)),
            Column("rv", ROWVERSION),
        )

        Table(
            "ts_t",
            metadata,
            Column("data", String(50)),
            Column("rv", TIMESTAMP),
        )

    def test_rowversion_reflection(self):
        # ROWVERSION is only a synonym for TIMESTAMP
        insp = inspect(testing.db)
        assert isinstance(insp.get_columns("rv_t")[1]["type"], TIMESTAMP)

    def test_timestamp_reflection(self):
        insp = inspect(testing.db)
        assert isinstance(insp.get_columns("ts_t")[1]["type"], TIMESTAMP)

    def test_class_hierarchy(self):
        """TIMESTAMP and ROWVERSION aren't datetime types, theyre binary."""

        assert issubclass(TIMESTAMP, sqltypes._Binary)
        assert issubclass(ROWVERSION, sqltypes._Binary)

    def test_round_trip_ts(self):
        self._test_round_trip("ts_t", TIMESTAMP, False)

    def test_round_trip_rv(self):
        self._test_round_trip("rv_t", ROWVERSION, False)

    def test_round_trip_ts_int(self):
        self._test_round_trip("ts_t", TIMESTAMP, True)

    def test_round_trip_rv_int(self):
        self._test_round_trip("rv_t", ROWVERSION, True)

    def _test_round_trip(self, tab, cls, convert_int):
        t = Table(
            tab,
            MetaData(),
            Column("data", String(50)),
            Column("rv", cls(convert_int=convert_int)),
        )

        with testing.db.connect() as conn:
            conn.execute(t.insert().values(data="foo"))
            last_ts_1 = conn.scalar("SELECT @@DBTS")

            if convert_int:
                last_ts_1 = int(codecs.encode(last_ts_1, "hex"), 16)

            eq_(conn.scalar(select([t.c.rv])), last_ts_1)

            conn.execute(
                t.update().values(data="bar").where(t.c.data == "foo")
            )
            last_ts_2 = conn.scalar("SELECT @@DBTS")
            if convert_int:
                last_ts_2 = int(codecs.encode(last_ts_2, "hex"), 16)

            eq_(conn.scalar(select([t.c.rv])), last_ts_2)

    def test_cant_insert_rowvalue(self):
        self._test_cant_insert(self.tables.rv_t)

    def test_cant_insert_timestamp(self):
        self._test_cant_insert(self.tables.ts_t)

    def _test_cant_insert(self, tab):
        with testing.db.connect() as conn:
            assert_raises_message(
                sa.exc.DBAPIError,
                r".*Cannot insert an explicit value into a timestamp column.",
                conn.execute,
                tab.insert().values(data="ins", rv=b"000"),
            )


class TypeDDLTest(fixtures.TestBase):
    def test_boolean(self):
        "Exercise type specification for boolean type."

        columns = [
            # column type, args, kwargs, expected ddl
            (Boolean, [], {}, "BIT")
        ]

        metadata = MetaData()
        table_args = ["test_mssql_boolean", metadata]
        for index, spec in enumerate(columns):
            type_, args, kw, res = spec
            table_args.append(
                Column("c%s" % index, type_(*args, **kw), nullable=None)
            )

        boolean_table = Table(*table_args)
        dialect = mssql.dialect()
        gen = dialect.ddl_compiler(dialect, schema.CreateTable(boolean_table))

        for col in boolean_table.c:
            index = int(col.name[1:])
            testing.eq_(
                gen.get_column_specification(col),
                "%s %s" % (col.name, columns[index][3]),
            )
            self.assert_(repr(col))

    def test_numeric(self):
        "Exercise type specification and options for numeric types."

        columns = [
            # column type, args, kwargs, expected ddl
            (types.NUMERIC, [], {}, "NUMERIC"),
            (types.NUMERIC, [None], {}, "NUMERIC"),
            (types.NUMERIC, [12, 4], {}, "NUMERIC(12, 4)"),
            (types.Float, [], {}, "FLOAT"),
            (types.Float, [None], {}, "FLOAT"),
            (types.Float, [12], {}, "FLOAT(12)"),
            (mssql.MSReal, [], {}, "REAL"),
            (types.Integer, [], {}, "INTEGER"),
            (types.BigInteger, [], {}, "BIGINT"),
            (mssql.MSTinyInteger, [], {}, "TINYINT"),
            (types.SmallInteger, [], {}, "SMALLINT"),
        ]

        metadata = MetaData()
        table_args = ["test_mssql_numeric", metadata]
        for index, spec in enumerate(columns):
            type_, args, kw, res = spec
            table_args.append(
                Column("c%s" % index, type_(*args, **kw), nullable=None)
            )

        numeric_table = Table(*table_args)
        dialect = mssql.dialect()
        gen = dialect.ddl_compiler(dialect, schema.CreateTable(numeric_table))

        for col in numeric_table.c:
            index = int(col.name[1:])
            testing.eq_(
                gen.get_column_specification(col),
                "%s %s" % (col.name, columns[index][3]),
            )
            self.assert_(repr(col))

    def test_char(self):
        """Exercise COLLATE-ish options on string types."""

        columns = [
            (mssql.MSChar, [], {}, "CHAR"),
            (mssql.MSChar, [1], {}, "CHAR(1)"),
            (
                mssql.MSChar,
                [1],
                {"collation": "Latin1_General_CI_AS"},
                "CHAR(1) COLLATE Latin1_General_CI_AS",
            ),
            (mssql.MSNChar, [], {}, "NCHAR"),
            (mssql.MSNChar, [1], {}, "NCHAR(1)"),
            (
                mssql.MSNChar,
                [1],
                {"collation": "Latin1_General_CI_AS"},
                "NCHAR(1) COLLATE Latin1_General_CI_AS",
            ),
            (mssql.MSString, [], {}, "VARCHAR(max)"),
            (mssql.MSString, [1], {}, "VARCHAR(1)"),
            (
                mssql.MSString,
                [1],
                {"collation": "Latin1_General_CI_AS"},
                "VARCHAR(1) COLLATE Latin1_General_CI_AS",
            ),
            (mssql.MSNVarchar, [], {}, "NVARCHAR(max)"),
            (mssql.MSNVarchar, [1], {}, "NVARCHAR(1)"),
            (
                mssql.MSNVarchar,
                [1],
                {"collation": "Latin1_General_CI_AS"},
                "NVARCHAR(1) COLLATE Latin1_General_CI_AS",
            ),
            (mssql.MSText, [], {}, "TEXT"),
            (
                mssql.MSText,
                [],
                {"collation": "Latin1_General_CI_AS"},
                "TEXT COLLATE Latin1_General_CI_AS",
            ),
            (mssql.MSNText, [], {}, "NTEXT"),
            (
                mssql.MSNText,
                [],
                {"collation": "Latin1_General_CI_AS"},
                "NTEXT COLLATE Latin1_General_CI_AS",
            ),
        ]

        metadata = MetaData()
        table_args = ["test_mssql_charset", metadata]
        for index, spec in enumerate(columns):
            type_, args, kw, res = spec
            table_args.append(
                Column("c%s" % index, type_(*args, **kw), nullable=None)
            )

        charset_table = Table(*table_args)
        dialect = mssql.dialect()
        gen = dialect.ddl_compiler(dialect, schema.CreateTable(charset_table))

        for col in charset_table.c:
            index = int(col.name[1:])
            testing.eq_(
                gen.get_column_specification(col),
                "%s %s" % (col.name, columns[index][3]),
            )
            self.assert_(repr(col))

    def test_dates(self):
        "Exercise type specification for date types."

        columns = [
            # column type, args, kwargs, expected ddl
            (mssql.MSDateTime, [], {}, "DATETIME", None),
            (types.DATE, [], {}, "DATE", None),
            (types.Date, [], {}, "DATE", None),
            (types.Date, [], {}, "DATETIME", MS_2005_VERSION),
            (mssql.MSDate, [], {}, "DATE", None),
            (mssql.MSDate, [], {}, "DATETIME", MS_2005_VERSION),
            (types.TIME, [], {}, "TIME", None),
            (types.Time, [], {}, "TIME", None),
            (mssql.MSTime, [], {}, "TIME", None),
            (mssql.MSTime, [1], {}, "TIME(1)", None),
            (types.Time, [], {}, "DATETIME", MS_2005_VERSION),
            (mssql.MSTime, [], {}, "TIME", None),
            (mssql.MSSmallDateTime, [], {}, "SMALLDATETIME", None),
            (mssql.MSDateTimeOffset, [], {}, "DATETIMEOFFSET", None),
            (mssql.MSDateTimeOffset, [1], {}, "DATETIMEOFFSET(1)", None),
            (mssql.MSDateTime2, [], {}, "DATETIME2", None),
            (mssql.MSDateTime2, [0], {}, "DATETIME2(0)", None),
            (mssql.MSDateTime2, [1], {}, "DATETIME2(1)", None),
            (mssql.MSTime, [0], {}, "TIME(0)", None),
            (mssql.MSDateTimeOffset, [0], {}, "DATETIMEOFFSET(0)", None),
        ]

        metadata = MetaData()
        table_args = ["test_mssql_dates", metadata]
        for index, spec in enumerate(columns):
            type_, args, kw, res, server_version = spec
            table_args.append(
                Column("c%s" % index, type_(*args, **kw), nullable=None)
            )

        date_table = Table(*table_args)
        dialect = mssql.dialect()
        dialect.server_version_info = MS_2008_VERSION
        ms_2005_dialect = mssql.dialect()
        ms_2005_dialect.server_version_info = MS_2005_VERSION
        gen = dialect.ddl_compiler(dialect, schema.CreateTable(date_table))
        gen2005 = ms_2005_dialect.ddl_compiler(
            ms_2005_dialect, schema.CreateTable(date_table)
        )

        for col in date_table.c:
            index = int(col.name[1:])
            server_version = columns[index][4]
            if not server_version:
                testing.eq_(
                    gen.get_column_specification(col),
                    "%s %s" % (col.name, columns[index][3]),
                )
            else:
                testing.eq_(
                    gen2005.get_column_specification(col),
                    "%s %s" % (col.name, columns[index][3]),
                )

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
            eq_(str(Text().compile(dialect=dialect)), "VARCHAR(max)")
            eq_(str(UnicodeText().compile(dialect=dialect)), "NVARCHAR(max)")
            eq_(str(LargeBinary().compile(dialect=dialect)), "VARBINARY(max)")

        for dialect in (d2, d4):
            eq_(str(Text().compile(dialect=dialect)), "TEXT")
            eq_(str(UnicodeText().compile(dialect=dialect)), "NTEXT")
            eq_(str(LargeBinary().compile(dialect=dialect)), "IMAGE")

    def test_money(self):
        """Exercise type specification for money types."""

        columns = [
            (mssql.MSMoney, [], {}, "MONEY"),
            (mssql.MSSmallMoney, [], {}, "SMALLMONEY"),
        ]
        metadata = MetaData()
        table_args = ["test_mssql_money", metadata]
        for index, spec in enumerate(columns):
            type_, args, kw, res = spec
            table_args.append(
                Column("c%s" % index, type_(*args, **kw), nullable=None)
            )
        money_table = Table(*table_args)
        dialect = mssql.dialect()
        gen = dialect.ddl_compiler(dialect, schema.CreateTable(money_table))
        for col in money_table.c:
            index = int(col.name[1:])
            testing.eq_(
                gen.get_column_specification(col),
                "%s %s" % (col.name, columns[index][3]),
            )
            self.assert_(repr(col))

    def test_binary(self):
        "Exercise type specification for binary types."

        columns = [
            # column type, args, kwargs, expected ddl
            (mssql.MSBinary, [], {}, "BINARY"),
            (mssql.MSBinary, [10], {}, "BINARY(10)"),
            (types.BINARY, [], {}, "BINARY"),
            (types.BINARY, [10], {}, "BINARY(10)"),
            (mssql.MSVarBinary, [], {}, "VARBINARY(max)"),
            (mssql.MSVarBinary, [10], {}, "VARBINARY(10)"),
            (types.VARBINARY, [10], {}, "VARBINARY(10)"),
            (types.VARBINARY, [], {}, "VARBINARY(max)"),
            (mssql.MSImage, [], {}, "IMAGE"),
            (mssql.IMAGE, [], {}, "IMAGE"),
            (types.LargeBinary, [], {}, "IMAGE"),
        ]

        metadata = MetaData()
        table_args = ["test_mssql_binary", metadata]
        for index, spec in enumerate(columns):
            type_, args, kw, res = spec
            table_args.append(
                Column("c%s" % index, type_(*args, **kw), nullable=None)
            )
        binary_table = Table(*table_args)
        dialect = mssql.dialect()
        gen = dialect.ddl_compiler(dialect, schema.CreateTable(binary_table))
        for col in binary_table.c:
            index = int(col.name[1:])
            testing.eq_(
                gen.get_column_specification(col),
                "%s %s" % (col.name, columns[index][3]),
            )
            self.assert_(repr(col))


metadata = None


class TypeRoundTripTest(
    fixtures.TestBase, AssertsExecutionResults, ComparesTables
):
    __only_on__ = "mssql"

    __backend__ = True

    @classmethod
    def setup_class(cls):
        global metadata
        metadata = MetaData(testing.db)

    def teardown(self):
        metadata.drop_all()

    def test_decimal_notation(self):
        numeric_table = Table(
            "numeric_table",
            metadata,
            Column(
                "id",
                Integer,
                Sequence("numeric_id_seq", optional=True),
                primary_key=True,
            ),
            Column(
                "numericcol", Numeric(precision=38, scale=20, asdecimal=True)
            ),
        )
        metadata.create_all()
        test_items = [
            decimal.Decimal(d)
            for d in (
                "1500000.00000000000000000000",
                "-1500000.00000000000000000000",
                "1500000",
                "0.0000000000000000002",
                "0.2",
                "-0.0000000000000000002",
                "-2E-2",
                "156666.458923543",
                "-156666.458923543",
                "1",
                "-1",
                "-1234",
                "1234",
                "2E-12",
                "4E8",
                "3E-6",
                "3E-7",
                "4.1",
                "1E-1",
                "1E-2",
                "1E-3",
                "1E-4",
                "1E-5",
                "1E-6",
                "1E-7",
                "1E-1",
                "1E-8",
                "0.2732E2",
                "-0.2432E2",
                "4.35656E2",
                "-02452E-2",
                "45125E-2",
                "1234.58965E-2",
                "1.521E+15",
                # previously, these were at -1E-25, which were inserted
                # cleanly however we only got back 20 digits of accuracy.
                # pyodbc as of 4.0.22 now disallows the silent truncation.
                "-1E-20",
                "1E-20",
                "1254E-20",
                "-1203E-20",
                "0",
                "-0.00",
                "-0",
                "4585E12",
                "000000000000000000012",
                "000000000000.32E12",
                "00000000000000.1E+12",
                # these are no longer accepted by pyodbc 4.0.22 but it seems
                # they were not actually round-tripping correctly before that
                # in any case
                # '-1E-25',
                # '1E-25',
                # '1254E-25',
                # '-1203E-25',
                # '000000000000.2E-32',
            )
        ]

        with testing.db.connect() as conn:
            for value in test_items:
                result = conn.execute(
                    numeric_table.insert(), dict(numericcol=value)
                )
                primary_key = result.inserted_primary_key
                returned = conn.scalar(
                    select([numeric_table.c.numericcol]).where(
                        numeric_table.c.id == primary_key[0]
                    )
                )
                eq_(value, returned)

    def test_float(self):
        float_table = Table(
            "float_table",
            metadata,
            Column(
                "id",
                Integer,
                Sequence("numeric_id_seq", optional=True),
                primary_key=True,
            ),
            Column("floatcol", Float()),
        )

        metadata.create_all()
        try:
            test_items = [
                float(d)
                for d in (
                    "1500000.00000000000000000000",
                    "-1500000.00000000000000000000",
                    "1500000",
                    "0.0000000000000000002",
                    "0.2",
                    "-0.0000000000000000002",
                    "156666.458923543",
                    "-156666.458923543",
                    "1",
                    "-1",
                    "1234",
                    "2E-12",
                    "4E8",
                    "3E-6",
                    "3E-7",
                    "4.1",
                    "1E-1",
                    "1E-2",
                    "1E-3",
                    "1E-4",
                    "1E-5",
                    "1E-6",
                    "1E-7",
                    "1E-8",
                )
            ]
            for value in test_items:
                float_table.insert().execute(floatcol=value)
        except Exception as e:
            raise e

    # todo this should suppress warnings, but it does not
    @emits_warning_on("mssql+mxodbc", r".*does not have any indexes.*")
    def test_dates(self):
        "Exercise type specification for date types."

        columns = [
            # column type, args, kwargs, expected ddl
            (mssql.MSDateTime, [], {}, "DATETIME", []),
            (types.DATE, [], {}, "DATE", [">=", (10,)]),
            (types.Date, [], {}, "DATE", [">=", (10,)]),
            (types.Date, [], {}, "DATETIME", ["<", (10,)], mssql.MSDateTime),
            (mssql.MSDate, [], {}, "DATE", [">=", (10,)]),
            (mssql.MSDate, [], {}, "DATETIME", ["<", (10,)], mssql.MSDateTime),
            (types.TIME, [], {}, "TIME", [">=", (10,)]),
            (types.Time, [], {}, "TIME", [">=", (10,)]),
            (mssql.MSTime, [], {}, "TIME", [">=", (10,)]),
            (mssql.MSTime, [1], {}, "TIME(1)", [">=", (10,)]),
            (types.Time, [], {}, "DATETIME", ["<", (10,)], mssql.MSDateTime),
            (mssql.MSTime, [], {}, "TIME", [">=", (10,)]),
            (mssql.MSSmallDateTime, [], {}, "SMALLDATETIME", []),
            (mssql.MSDateTimeOffset, [], {}, "DATETIMEOFFSET", [">=", (10,)]),
            (
                mssql.MSDateTimeOffset,
                [1],
                {},
                "DATETIMEOFFSET(1)",
                [">=", (10,)],
            ),
            (mssql.MSDateTime2, [], {}, "DATETIME2", [">=", (10,)]),
            (mssql.MSDateTime2, [0], {}, "DATETIME2(0)", [">=", (10,)]),
            (mssql.MSDateTime2, [1], {}, "DATETIME2(1)", [">=", (10,)]),
        ]

        table_args = ["test_mssql_dates", metadata]
        for index, spec in enumerate(columns):
            type_, args, kw, res, requires = spec[0:5]
            if (
                requires
                and testing._is_excluded("mssql", *requires)
                or not requires
            ):
                c = Column("c%s" % index, type_(*args, **kw), nullable=None)
                testing.db.dialect.type_descriptor(c.type)
                table_args.append(c)
        dates_table = Table(*table_args)
        gen = testing.db.dialect.ddl_compiler(
            testing.db.dialect, schema.CreateTable(dates_table)
        )
        for col in dates_table.c:
            index = int(col.name[1:])
            testing.eq_(
                gen.get_column_specification(col),
                "%s %s" % (col.name, columns[index][3]),
            )
            self.assert_(repr(col))
        dates_table.create(checkfirst=True)
        reflected_dates = Table(
            "test_mssql_dates", MetaData(testing.db), autoload=True
        )
        for col in reflected_dates.c:
            self.assert_types_base(col, dates_table.c[col.key])

    @testing.metadata_fixture()
    def date_fixture(self, metadata):
        t = Table(
            "test_dates",
            metadata,
            Column("adate", Date),
            Column("atime1", Time),
            Column("atime2", Time),
            Column("adatetime", DateTime),
            Column("adatetimeoffset", DATETIMEOFFSET),
        )

        d1 = datetime.date(2007, 10, 30)
        t1 = datetime.time(11, 2, 32)
        d2 = datetime.datetime(2007, 10, 30, 11, 2, 32)
        return t, (d1, t1, d2)

    def test_date_roundtrips(self, date_fixture):
        t, (d1, t1, d2) = date_fixture
        with testing.db.begin() as conn:
            conn.execute(
                t.insert(), adate=d1, adatetime=d2, atime1=t1, atime2=d2
            )

            row = conn.execute(t.select()).first()
            eq_(
                (row.adate, row.adatetime, row.atime1, row.atime2),
                (d1, d2, t1, d2.time()),
            )

    @testing.metadata_fixture()
    def datetimeoffset_fixture(self, metadata):
        t = Table(
            "test_dates", metadata, Column("adatetimeoffset", DATETIMEOFFSET),
        )

        return t

    @testing.combinations(
        ("dto_param_none", lambda: None, None, False),
        (
            "dto_param_datetime_aware_positive",
            lambda: datetime.datetime(
                2007,
                10,
                30,
                11,
                2,
                32,
                123456,
                util.timezone(datetime.timedelta(hours=1)),
            ),
            1,
            False,
        ),
        (
            "dto_param_datetime_aware_negative",
            lambda: datetime.datetime(
                2007,
                10,
                30,
                11,
                2,
                32,
                123456,
                util.timezone(datetime.timedelta(hours=-5)),
            ),
            -5,
            False,
        ),
        (
            "dto_param_datetime_aware_seconds_frac_fail",
            lambda: datetime.datetime(
                2007,
                10,
                30,
                11,
                2,
                32,
                123456,
                util.timezone(datetime.timedelta(seconds=4000)),
            ),
            None,
            True,
            testing.requires.python37,
        ),
        (
            "dto_param_datetime_naive",
            lambda: datetime.datetime(2007, 10, 30, 11, 2, 32, 123456),
            0,
            False,
        ),
        (
            "dto_param_string_one",
            lambda: "2007-10-30 11:02:32.123456 +01:00",
            1,
            False,
        ),
        # wow
        (
            "dto_param_string_two",
            lambda: "October 30, 2007 11:02:32.123456",
            0,
            False,
        ),
        ("dto_param_string_invalid", lambda: "this is not a date", 0, True),
        id_="iaaa",
        argnames="dto_param_value, expected_offset_hours, should_fail",
    )
    def test_datetime_offset(
        self,
        datetimeoffset_fixture,
        dto_param_value,
        expected_offset_hours,
        should_fail,
    ):
        t = datetimeoffset_fixture
        dto_param_value = dto_param_value()

        with testing.db.begin() as conn:
            if should_fail:
                assert_raises(
                    sa.exc.DBAPIError,
                    conn.execute,
                    t.insert(),
                    adatetimeoffset=dto_param_value,
                )
                return

            conn.execute(
                t.insert(), adatetimeoffset=dto_param_value,
            )

            row = conn.execute(t.select()).first()

            if dto_param_value is None:
                is_(row.adatetimeoffset, None)
            else:
                eq_(
                    row.adatetimeoffset,
                    datetime.datetime(
                        2007,
                        10,
                        30,
                        11,
                        2,
                        32,
                        123456,
                        util.timezone(
                            datetime.timedelta(hours=expected_offset_hours)
                        ),
                    ),
                )

    @emits_warning_on("mssql+mxodbc", r".*does not have any indexes.*")
    @testing.provide_metadata
    def _test_binary_reflection(self, deprecate_large_types):
        "Exercise type specification for binary types."

        columns = [
            # column type, args, kwargs, expected ddl from reflected
            (mssql.MSBinary, [], {}, "BINARY(1)"),
            (mssql.MSBinary, [10], {}, "BINARY(10)"),
            (types.BINARY, [], {}, "BINARY(1)"),
            (types.BINARY, [10], {}, "BINARY(10)"),
            (mssql.MSVarBinary, [], {}, "VARBINARY(max)"),
            (mssql.MSVarBinary, [10], {}, "VARBINARY(10)"),
            (types.VARBINARY, [10], {}, "VARBINARY(10)"),
            (types.VARBINARY, [], {}, "VARBINARY(max)"),
            (mssql.MSImage, [], {}, "IMAGE"),
            (mssql.IMAGE, [], {}, "IMAGE"),
            (
                types.LargeBinary,
                [],
                {},
                "IMAGE" if not deprecate_large_types else "VARBINARY(max)",
            ),
        ]

        metadata = self.metadata
        metadata.bind = engines.testing_engine(
            options={"deprecate_large_types": deprecate_large_types}
        )
        table_args = ["test_mssql_binary", metadata]
        for index, spec in enumerate(columns):
            type_, args, kw, res = spec
            table_args.append(
                Column("c%s" % index, type_(*args, **kw), nullable=None)
            )
        binary_table = Table(*table_args)
        metadata.create_all()
        reflected_binary = Table(
            "test_mssql_binary", MetaData(testing.db), autoload=True
        )
        for col, spec in zip(reflected_binary.c, columns):
            eq_(
                str(col.type),
                spec[3],
                "column %s %s != %s" % (col.key, str(col.type), spec[3]),
            )
            c1 = testing.db.dialect.type_descriptor(col.type).__class__
            c2 = testing.db.dialect.type_descriptor(
                binary_table.c[col.name].type
            ).__class__
            assert issubclass(
                c1, c2
            ), "column %s: %r is not a subclass of %r" % (col.key, c1, c2)
            if binary_table.c[col.name].type.length:
                testing.eq_(
                    col.type.length, binary_table.c[col.name].type.length
                )

    def test_binary_reflection_legacy_large_types(self):
        self._test_binary_reflection(False)

    @testing.only_on("mssql >= 11")
    def test_binary_reflection_sql2012_large_types(self):
        self._test_binary_reflection(True)

    def test_autoincrement(self):
        Table(
            "ai_1",
            metadata,
            Column("int_y", Integer, primary_key=True, autoincrement=True),
            Column("int_n", Integer, DefaultClause("0"), primary_key=True),
        )
        Table(
            "ai_2",
            metadata,
            Column("int_y", Integer, primary_key=True, autoincrement=True),
            Column("int_n", Integer, DefaultClause("0"), primary_key=True),
        )
        Table(
            "ai_3",
            metadata,
            Column("int_n", Integer, DefaultClause("0"), primary_key=True),
            Column("int_y", Integer, primary_key=True, autoincrement=True),
        )

        Table(
            "ai_4",
            metadata,
            Column("int_n", Integer, DefaultClause("0"), primary_key=True),
            Column("int_n2", Integer, DefaultClause("0"), primary_key=True),
        )
        Table(
            "ai_5",
            metadata,
            Column("int_y", Integer, primary_key=True, autoincrement=True),
            Column("int_n", Integer, DefaultClause("0"), primary_key=True),
        )
        Table(
            "ai_6",
            metadata,
            Column("o1", String(1), DefaultClause("x"), primary_key=True),
            Column("int_y", Integer, primary_key=True, autoincrement=True),
        )
        Table(
            "ai_7",
            metadata,
            Column("o1", String(1), DefaultClause("x"), primary_key=True),
            Column("o2", String(1), DefaultClause("x"), primary_key=True),
            Column("int_y", Integer, autoincrement=True, primary_key=True),
        )
        Table(
            "ai_8",
            metadata,
            Column("o1", String(1), DefaultClause("x"), primary_key=True),
            Column("o2", String(1), DefaultClause("x"), primary_key=True),
        )
        metadata.create_all()

        table_names = [
            "ai_1",
            "ai_2",
            "ai_3",
            "ai_4",
            "ai_5",
            "ai_6",
            "ai_7",
            "ai_8",
        ]
        mr = MetaData(testing.db)

        for name in table_names:
            tbl = Table(name, mr, autoload=True)
            tbl = metadata.tables[name]

            # test that the flag itself reflects appropriately
            for col in tbl.c:
                if "int_y" in col.name:
                    is_(col.autoincrement, True)
                    is_(tbl._autoincrement_column, col)
                else:
                    eq_(col.autoincrement, "auto")
                    is_not_(tbl._autoincrement_column, col)

            # mxodbc can't handle scope_identity() with DEFAULT VALUES

            if testing.db.driver == "mxodbc":
                eng = [
                    engines.testing_engine(
                        options={"implicit_returning": True}
                    )
                ]
            else:
                eng = [
                    engines.testing_engine(
                        options={"implicit_returning": False}
                    ),
                    engines.testing_engine(
                        options={"implicit_returning": True}
                    ),
                ]

            for counter, engine in enumerate(eng):
                with engine.begin() as conn:
                    conn.execute(tbl.insert())
                    if "int_y" in tbl.c:
                        eq_(
                            conn.execute(select([tbl.c.int_y])).scalar(),
                            counter + 1,
                        )
                        assert (
                            list(conn.execute(tbl.select()).first()).count(
                                counter + 1
                            )
                            == 1
                        )
                    else:
                        assert 1 not in list(
                            conn.execute(tbl.select()).first()
                        )
                    conn.execute(tbl.delete())


class StringTest(fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = mssql.dialect()

    def test_unicode_literal_binds(self):
        self.assert_compile(
            column("x", Unicode()) == "foo", "x = N'foo'", literal_binds=True
        )

    def test_unicode_text_literal_binds(self):
        self.assert_compile(
            column("x", UnicodeText()) == "foo",
            "x = N'foo'",
            literal_binds=True,
        )

    def test_string_text_literal_binds(self):
        self.assert_compile(
            column("x", String()) == "foo", "x = 'foo'", literal_binds=True
        )

    def test_string_text_literal_binds_explicit_unicode_right(self):
        self.assert_compile(
            column("x", String()) == util.u("foo"),
            "x = 'foo'",
            literal_binds=True,
        )

    def test_string_text_explicit_literal_binds(self):
        # the literal experssion here coerces the right side to
        # Unicode on Python 3 for plain string, test with unicode
        # string just to confirm literal is doing this
        self.assert_compile(
            column("x", String()) == literal(util.u("foo")),
            "x = N'foo'",
            literal_binds=True,
        )

    def test_text_text_literal_binds(self):
        self.assert_compile(
            column("x", Text()) == "foo", "x = 'foo'", literal_binds=True
        )


class BinaryTest(fixtures.TestBase):
    __only_on__ = "mssql"
    __requires__ = ("non_broken_binary",)
    __backend__ = True

    def test_character_binary(self):
        self._test_round_trip(mssql.MSVarBinary(800), b("some normal data"))

    @testing.provide_metadata
    def _test_round_trip(
        self, type_, data, deprecate_large_types=True, expected=None
    ):
        if (
            testing.db.dialect.deprecate_large_types
            is not deprecate_large_types
        ):
            engine = engines.testing_engine(
                options={"deprecate_large_types": deprecate_large_types}
            )
        else:
            engine = testing.db

        binary_table = Table(
            "binary_table",
            self.metadata,
            Column("id", Integer, primary_key=True),
            Column("data", type_),
        )
        binary_table.create(engine)

        if expected is None:
            expected = data

        with engine.connect() as conn:
            conn.execute(binary_table.insert(), data=data)

            eq_(conn.scalar(select([binary_table.c.data])), expected)

            eq_(
                conn.scalar(
                    text("select data from binary_table").columns(
                        binary_table.c.data
                    )
                ),
                expected,
            )

            conn.execute(binary_table.delete())

            conn.execute(binary_table.insert(), data=None)
            eq_(conn.scalar(select([binary_table.c.data])), None)

            eq_(
                conn.scalar(
                    text("select data from binary_table").columns(
                        binary_table.c.data
                    )
                ),
                None,
            )

    def test_plain_pickle(self):
        self._test_round_trip(PickleType, pickleable.Foo("im foo 1"))

    def test_custom_pickle(self):
        class MyPickleType(types.TypeDecorator):
            impl = PickleType

            def process_bind_param(self, value, dialect):
                if value:
                    value.stuff = "BIND" + value.stuff
                return value

            def process_result_value(self, value, dialect):
                if value:
                    value.stuff = value.stuff + "RESULT"
                return value

        data = pickleable.Foo("im foo 1")
        expected = pickleable.Foo("im foo 1")
        expected.stuff = "BINDim stuffRESULT"

        self._test_round_trip(MyPickleType, data, expected=expected)

    def test_image(self):
        stream1 = self._load_stream("binary_data_one.dat")
        self._test_round_trip(mssql.MSImage, stream1)

    def test_large_binary(self):
        stream1 = self._load_stream("binary_data_one.dat")
        self._test_round_trip(sqltypes.LargeBinary, stream1)

    def test_large_legacy_types(self):
        stream1 = self._load_stream("binary_data_one.dat")
        self._test_round_trip(
            sqltypes.LargeBinary, stream1, deprecate_large_types=False
        )

    def test_mssql_varbinary_max(self):
        stream1 = self._load_stream("binary_data_one.dat")
        self._test_round_trip(mssql.VARBINARY("max"), stream1)

    def test_mssql_legacy_varbinary_max(self):
        stream1 = self._load_stream("binary_data_one.dat")
        self._test_round_trip(
            mssql.VARBINARY("max"), stream1, deprecate_large_types=False
        )

    def test_binary_slice(self):
        self._test_var_slice(types.BINARY)

    def test_binary_slice_zeropadding(self):
        self._test_var_slice_zeropadding(types.BINARY, True)

    def test_varbinary_slice(self):
        self._test_var_slice(types.VARBINARY)

    def test_varbinary_slice_zeropadding(self):
        self._test_var_slice_zeropadding(types.VARBINARY, False)

    def test_mssql_varbinary_slice(self):
        self._test_var_slice(mssql.VARBINARY)

    def test_mssql_varbinary_slice_zeropadding(self):
        self._test_var_slice_zeropadding(mssql.VARBINARY, False)

    def _test_var_slice(self, type_):
        stream1 = self._load_stream("binary_data_one.dat")

        data = stream1[0:100]

        self._test_round_trip(type_(100), data)

    def _test_var_slice_zeropadding(
        self, type_, pad, deprecate_large_types=True
    ):
        stream2 = self._load_stream("binary_data_two.dat")

        data = stream2[0:99]

        # the type we used here is 100 bytes
        # so we will get 100 bytes zero-padded

        if pad:
            paddedstream = stream2[0:99] + b"\x00"
        else:
            paddedstream = stream2[0:99]

        self._test_round_trip(type_(100), data, expected=paddedstream)

    def _load_stream(self, name, len_=3000):
        fp = open(
            os.path.join(os.path.dirname(__file__), "..", "..", name), "rb"
        )
        stream = fp.read(len_)
        fp.close()
        return stream
