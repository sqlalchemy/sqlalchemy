# coding: utf-8
from collections import OrderedDict
import datetime
import decimal

from sqlalchemy import BOOLEAN
from sqlalchemy import Boolean
from sqlalchemy import Column
from sqlalchemy import DefaultClause
from sqlalchemy import Enum
from sqlalchemy import exc
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import schema
from sqlalchemy import select
from sqlalchemy import sql
from sqlalchemy import String
from sqlalchemy import Table
from sqlalchemy import testing
from sqlalchemy import TIMESTAMP
from sqlalchemy import TypeDecorator
from sqlalchemy import types as sqltypes
from sqlalchemy import UnicodeText
from sqlalchemy import util
from sqlalchemy.dialects.mysql import base as mysql
from sqlalchemy.testing import assert_raises
from sqlalchemy.testing import assert_raises_message
from sqlalchemy.testing import AssertsCompiledSQL
from sqlalchemy.testing import AssertsExecutionResults
from sqlalchemy.testing import eq_
from sqlalchemy.testing import eq_regex
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import is_
from sqlalchemy.util import u


class TypeCompileTest(fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = mysql.dialect()

    @testing.combinations(
        # column type, args, kwargs, expected ddl
        # e.g. Column(Integer(10, unsigned=True)) ==
        # 'INTEGER(10) UNSIGNED'
        (mysql.MSNumeric, [], {}, "NUMERIC"),
        (mysql.MSNumeric, [None], {}, "NUMERIC"),
        (mysql.MSNumeric, [12], {}, "NUMERIC(12)"),
        (
            mysql.MSNumeric,
            [12, 4],
            {"unsigned": True},
            "NUMERIC(12, 4) UNSIGNED",
        ),
        (
            mysql.MSNumeric,
            [12, 4],
            {"zerofill": True},
            "NUMERIC(12, 4) ZEROFILL",
        ),
        (
            mysql.MSNumeric,
            [12, 4],
            {"zerofill": True, "unsigned": True},
            "NUMERIC(12, 4) UNSIGNED ZEROFILL",
        ),
        (mysql.MSDecimal, [], {}, "DECIMAL"),
        (mysql.MSDecimal, [None], {}, "DECIMAL"),
        (mysql.MSDecimal, [12], {}, "DECIMAL(12)"),
        (mysql.MSDecimal, [12, None], {}, "DECIMAL(12)"),
        (
            mysql.MSDecimal,
            [12, 4],
            {"unsigned": True},
            "DECIMAL(12, 4) UNSIGNED",
        ),
        (
            mysql.MSDecimal,
            [12, 4],
            {"zerofill": True},
            "DECIMAL(12, 4) ZEROFILL",
        ),
        (
            mysql.MSDecimal,
            [12, 4],
            {"zerofill": True, "unsigned": True},
            "DECIMAL(12, 4) UNSIGNED ZEROFILL",
        ),
        (mysql.MSDouble, [None, None], {}, "DOUBLE"),
        (
            mysql.MSDouble,
            [12, 4],
            {"unsigned": True},
            "DOUBLE(12, 4) UNSIGNED",
        ),
        (
            mysql.MSDouble,
            [12, 4],
            {"zerofill": True},
            "DOUBLE(12, 4) ZEROFILL",
        ),
        (
            mysql.MSDouble,
            [12, 4],
            {"zerofill": True, "unsigned": True},
            "DOUBLE(12, 4) UNSIGNED ZEROFILL",
        ),
        (mysql.MSReal, [None, None], {}, "REAL"),
        (mysql.MSReal, [12, 4], {"unsigned": True}, "REAL(12, 4) UNSIGNED"),
        (mysql.MSReal, [12, 4], {"zerofill": True}, "REAL(12, 4) ZEROFILL"),
        (
            mysql.MSReal,
            [12, 4],
            {"zerofill": True, "unsigned": True},
            "REAL(12, 4) UNSIGNED ZEROFILL",
        ),
        (mysql.MSFloat, [], {}, "FLOAT"),
        (mysql.MSFloat, [None], {}, "FLOAT"),
        (mysql.MSFloat, [12], {}, "FLOAT(12)"),
        (mysql.MSFloat, [12, 4], {}, "FLOAT(12, 4)"),
        (mysql.MSFloat, [12, 4], {"unsigned": True}, "FLOAT(12, 4) UNSIGNED"),
        (mysql.MSFloat, [12, 4], {"zerofill": True}, "FLOAT(12, 4) ZEROFILL"),
        (
            mysql.MSFloat,
            [12, 4],
            {"zerofill": True, "unsigned": True},
            "FLOAT(12, 4) UNSIGNED ZEROFILL",
        ),
        (mysql.MSInteger, [], {}, "INTEGER"),
        (mysql.MSInteger, [4], {}, "INTEGER(4)"),
        (mysql.MSInteger, [4], {"unsigned": True}, "INTEGER(4) UNSIGNED"),
        (mysql.MSInteger, [4], {"zerofill": True}, "INTEGER(4) ZEROFILL"),
        (
            mysql.MSInteger,
            [4],
            {"zerofill": True, "unsigned": True},
            "INTEGER(4) UNSIGNED ZEROFILL",
        ),
        (mysql.MSBigInteger, [], {}, "BIGINT"),
        (mysql.MSBigInteger, [4], {}, "BIGINT(4)"),
        (mysql.MSBigInteger, [4], {"unsigned": True}, "BIGINT(4) UNSIGNED"),
        (mysql.MSBigInteger, [4], {"zerofill": True}, "BIGINT(4) ZEROFILL"),
        (
            mysql.MSBigInteger,
            [4],
            {"zerofill": True, "unsigned": True},
            "BIGINT(4) UNSIGNED ZEROFILL",
        ),
        (mysql.MSMediumInteger, [], {}, "MEDIUMINT"),
        (mysql.MSMediumInteger, [4], {}, "MEDIUMINT(4)"),
        (
            mysql.MSMediumInteger,
            [4],
            {"unsigned": True},
            "MEDIUMINT(4) UNSIGNED",
        ),
        (
            mysql.MSMediumInteger,
            [4],
            {"zerofill": True},
            "MEDIUMINT(4) ZEROFILL",
        ),
        (
            mysql.MSMediumInteger,
            [4],
            {"zerofill": True, "unsigned": True},
            "MEDIUMINT(4) UNSIGNED ZEROFILL",
        ),
        (mysql.MSTinyInteger, [], {}, "TINYINT"),
        (mysql.MSTinyInteger, [1], {}, "TINYINT(1)"),
        (mysql.MSTinyInteger, [1], {"unsigned": True}, "TINYINT(1) UNSIGNED"),
        (mysql.MSTinyInteger, [1], {"zerofill": True}, "TINYINT(1) ZEROFILL"),
        (
            mysql.MSTinyInteger,
            [1],
            {"zerofill": True, "unsigned": True},
            "TINYINT(1) UNSIGNED ZEROFILL",
        ),
        (mysql.MSSmallInteger, [], {}, "SMALLINT"),
        (mysql.MSSmallInteger, [4], {}, "SMALLINT(4)"),
        (
            mysql.MSSmallInteger,
            [4],
            {"unsigned": True},
            "SMALLINT(4) UNSIGNED",
        ),
        (
            mysql.MSSmallInteger,
            [4],
            {"zerofill": True},
            "SMALLINT(4) ZEROFILL",
        ),
        (
            mysql.MSSmallInteger,
            [4],
            {"zerofill": True, "unsigned": True},
            "SMALLINT(4) UNSIGNED ZEROFILL",
        ),
    )
    def test_numeric(self, type_, args, kw, res):
        "Exercise type specification and options for numeric types."

        type_inst = type_(*args, **kw)
        self.assert_compile(type_inst, res)
        # test that repr() copies out all arguments
        self.assert_compile(eval("mysql.%r" % type_inst), res)

    @testing.combinations(
        (mysql.MSChar, [1], {}, "CHAR(1)"),
        (mysql.NCHAR, [1], {}, "NATIONAL CHAR(1)"),
        (mysql.MSChar, [1], {"binary": True}, "CHAR(1) BINARY"),
        (mysql.MSChar, [1], {"ascii": True}, "CHAR(1) ASCII"),
        (mysql.MSChar, [1], {"unicode": True}, "CHAR(1) UNICODE"),
        (
            mysql.MSChar,
            [1],
            {"ascii": True, "binary": True},
            "CHAR(1) ASCII BINARY",
        ),
        (
            mysql.MSChar,
            [1],
            {"unicode": True, "binary": True},
            "CHAR(1) UNICODE BINARY",
        ),
        (mysql.MSChar, [1], {"charset": "utf8"}, "CHAR(1) CHARACTER SET utf8"),
        (
            mysql.MSChar,
            [1],
            {"charset": "utf8", "binary": True},
            "CHAR(1) CHARACTER SET utf8 BINARY",
        ),
        (
            mysql.MSChar,
            [1],
            {"charset": "utf8", "unicode": True},
            "CHAR(1) CHARACTER SET utf8",
        ),
        (
            mysql.MSChar,
            [1],
            {"charset": "utf8", "ascii": True},
            "CHAR(1) CHARACTER SET utf8",
        ),
        (
            mysql.MSChar,
            [1],
            {"collation": "utf8_bin"},
            "CHAR(1) COLLATE utf8_bin",
        ),
        (
            mysql.MSChar,
            [1],
            {"charset": "utf8", "collation": "utf8_bin"},
            "CHAR(1) CHARACTER SET utf8 COLLATE utf8_bin",
        ),
        (
            mysql.MSChar,
            [1],
            {"charset": "utf8", "binary": True},
            "CHAR(1) CHARACTER SET utf8 BINARY",
        ),
        (
            mysql.MSChar,
            [1],
            {"charset": "utf8", "collation": "utf8_bin", "binary": True},
            "CHAR(1) CHARACTER SET utf8 COLLATE utf8_bin",
        ),
        (mysql.MSChar, [1], {"national": True}, "NATIONAL CHAR(1)"),
        (
            mysql.MSChar,
            [1],
            {"national": True, "charset": "utf8"},
            "NATIONAL CHAR(1)",
        ),
        (
            mysql.MSChar,
            [1],
            {"national": True, "charset": "utf8", "binary": True},
            "NATIONAL CHAR(1) BINARY",
        ),
        (
            mysql.MSChar,
            [1],
            {"national": True, "binary": True, "unicode": True},
            "NATIONAL CHAR(1) BINARY",
        ),
        (
            mysql.MSChar,
            [1],
            {"national": True, "collation": "utf8_bin"},
            "NATIONAL CHAR(1) COLLATE utf8_bin",
        ),
        (
            mysql.MSString,
            [1],
            {"charset": "utf8", "collation": "utf8_bin"},
            "VARCHAR(1) CHARACTER SET utf8 COLLATE utf8_bin",
        ),
        (
            mysql.MSString,
            [1],
            {"national": True, "collation": "utf8_bin"},
            "NATIONAL VARCHAR(1) COLLATE utf8_bin",
        ),
        (
            mysql.MSTinyText,
            [],
            {"charset": "utf8", "collation": "utf8_bin"},
            "TINYTEXT CHARACTER SET utf8 COLLATE utf8_bin",
        ),
        (
            mysql.MSMediumText,
            [],
            {"charset": "utf8", "binary": True},
            "MEDIUMTEXT CHARACTER SET utf8 BINARY",
        ),
        (mysql.MSLongText, [], {"ascii": True}, "LONGTEXT ASCII"),
        (
            mysql.ENUM,
            ["foo", "bar"],
            {"unicode": True},
            """ENUM('foo','bar') UNICODE""",
        ),
        (String, [20], {"collation": "utf8"}, "VARCHAR(20) COLLATE utf8"),
    )
    @testing.exclude("mysql", "<", (4, 1, 1), "no charset support")
    def test_charset(self, type_, args, kw, res):
        """Exercise CHARACTER SET and COLLATE-ish options on string types."""

        type_inst = type_(*args, **kw)
        self.assert_compile(type_inst, res)

    @testing.combinations(
        (mysql.MSBit(), "BIT"),
        (mysql.MSBit(1), "BIT(1)"),
        (mysql.MSBit(63), "BIT(63)"),
    )
    def test_bit_50(self, type_, expected):
        """Exercise BIT types on 5.0+ (not valid for all engine types)"""

        self.assert_compile(type_, expected)

    @testing.combinations(
        (BOOLEAN(), "BOOL"),
        (Boolean(), "BOOL"),
        (mysql.TINYINT(1), "TINYINT(1)"),
        (mysql.TINYINT(1, unsigned=True), "TINYINT(1) UNSIGNED"),
    )
    def test_boolean_compile(self, type_, expected):
        self.assert_compile(type_, expected)

    def test_timestamp_fsp(self):
        self.assert_compile(mysql.TIMESTAMP(fsp=5), "TIMESTAMP(5)")

    @testing.combinations(
        ([TIMESTAMP], {}, "TIMESTAMP NULL"),
        ([mysql.MSTimeStamp], {}, "TIMESTAMP NULL"),
        (
            [
                mysql.MSTimeStamp(),
                DefaultClause(sql.text("CURRENT_TIMESTAMP")),
            ],
            {},
            "TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP",
        ),
        (
            [mysql.MSTimeStamp, DefaultClause(sql.text("CURRENT_TIMESTAMP"))],
            {"nullable": False},
            "TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP",
        ),
        (
            [
                mysql.MSTimeStamp,
                DefaultClause(sql.text("'1999-09-09 09:09:09'")),
            ],
            {"nullable": False},
            "TIMESTAMP NOT NULL DEFAULT '1999-09-09 09:09:09'",
        ),
        (
            [
                mysql.MSTimeStamp(),
                DefaultClause(sql.text("'1999-09-09 09:09:09'")),
            ],
            {},
            "TIMESTAMP NULL DEFAULT '1999-09-09 09:09:09'",
        ),
        (
            [
                mysql.MSTimeStamp(),
                DefaultClause(
                    sql.text(
                        "'1999-09-09 09:09:09' " "ON UPDATE CURRENT_TIMESTAMP"
                    )
                ),
            ],
            {},
            "TIMESTAMP NULL DEFAULT '1999-09-09 09:09:09' "
            "ON UPDATE CURRENT_TIMESTAMP",
        ),
        (
            [
                mysql.MSTimeStamp,
                DefaultClause(
                    sql.text(
                        "'1999-09-09 09:09:09' " "ON UPDATE CURRENT_TIMESTAMP"
                    )
                ),
            ],
            {"nullable": False},
            "TIMESTAMP NOT NULL DEFAULT '1999-09-09 09:09:09' "
            "ON UPDATE CURRENT_TIMESTAMP",
        ),
        (
            [
                mysql.MSTimeStamp(),
                DefaultClause(
                    sql.text(
                        "CURRENT_TIMESTAMP " "ON UPDATE CURRENT_TIMESTAMP"
                    )
                ),
            ],
            {},
            "TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP "
            "ON UPDATE CURRENT_TIMESTAMP",
        ),
        (
            [
                mysql.MSTimeStamp,
                DefaultClause(
                    sql.text(
                        "CURRENT_TIMESTAMP " "ON UPDATE CURRENT_TIMESTAMP"
                    )
                ),
            ],
            {"nullable": False},
            "TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP "
            "ON UPDATE CURRENT_TIMESTAMP",
        ),
    )
    def test_timestamp_defaults(self, spec, kw, expected):
        """Exercise funky TIMESTAMP default syntax when used in columns."""

        c = Column("t", *spec, **kw)
        Table("t", MetaData(), c)
        self.assert_compile(schema.CreateColumn(c), "t %s" % expected)

    def test_datetime_generic(self):
        self.assert_compile(mysql.DATETIME(), "DATETIME")

    def test_datetime_fsp(self):
        self.assert_compile(mysql.DATETIME(fsp=4), "DATETIME(4)")

    def test_time_generic(self):
        """"Exercise TIME."""

        self.assert_compile(mysql.TIME(), "TIME")

    def test_time_fsp(self):
        self.assert_compile(mysql.TIME(fsp=5), "TIME(5)")

    def test_time_result_processor(self):
        eq_(
            mysql.TIME().result_processor(None, None)(
                datetime.timedelta(seconds=35, minutes=517, microseconds=450)
            ),
            datetime.time(8, 37, 35, 450),
        )


class TypeRoundTripTest(fixtures.TestBase, AssertsExecutionResults):

    __dialect__ = mysql.dialect()
    __only_on__ = "mysql", "mariadb"
    __backend__ = True

    # fixed in mysql-connector as of 2.0.1,
    # see http://bugs.mysql.com/bug.php?id=73266
    def test_precision_float_roundtrip(self, metadata, connection):
        t = Table(
            "t",
            metadata,
            Column(
                "scale_value",
                mysql.DOUBLE(precision=15, scale=12, asdecimal=True),
            ),
            Column(
                "unscale_value",
                mysql.DOUBLE(decimal_return_scale=12, asdecimal=True),
            ),
        )
        t.create(connection)
        connection.execute(
            t.insert(),
            dict(
                scale_value=45.768392065789,
                unscale_value=45.768392065789,
            ),
        )
        result = connection.scalar(select(t.c.scale_value))
        eq_(result, decimal.Decimal("45.768392065789"))

        result = connection.scalar(select(t.c.unscale_value))
        eq_(result, decimal.Decimal("45.768392065789"))

    @testing.only_if("mysql")
    def test_charset_collate_table(self, metadata, connection):
        t = Table(
            "foo",
            metadata,
            Column("id", Integer),
            Column("data", UnicodeText),
            mysql_default_charset="utf8",
            mysql_collate="utf8_bin",
        )
        t.create(connection)
        t2 = Table("foo", MetaData(), autoload_with=connection)
        eq_(t2.kwargs["mysql_collate"], "utf8_bin")
        eq_(t2.kwargs["mysql_default charset"], "utf8")

        # test [ticket:2906]
        # in order to test the condition here, need to use
        # MySQLdb 1.2.3 and also need to pass either use_unicode=1
        # or charset=utf8 to the URL.
        connection.execute(t.insert(), dict(id=1, data=u("some text")))
        assert isinstance(connection.scalar(select(t.c.data)), util.text_type)

    @testing.metadata_fixture(ddl="class")
    def bit_table(self, metadata):
        bit_table = Table(
            "mysql_bits",
            metadata,
            Column("b1", mysql.MSBit),
            Column("b2", mysql.MSBit()),
            Column("b3", mysql.MSBit(), nullable=False),
            Column("b4", mysql.MSBit(1)),
            Column("b5", mysql.MSBit(8)),
            Column("b6", mysql.MSBit(32)),
            Column("b7", mysql.MSBit(63)),
            Column("b8", mysql.MSBit(64)),
        )
        return bit_table

    i, j, k, l = 255, 2 ** 32 - 1, 2 ** 63 - 1, 2 ** 64 - 1

    @testing.combinations(
        (([0] * 8), None),
        ([None, None, 0, None, None, None, None, None], None),
        (([1] * 8), None),
        ([sql.text("b'1'")] * 8, [1] * 8),
        ([0, 0, 0, 0, i, i, i, i], None),
        ([0, 0, 0, 0, 0, j, j, j], None),
        ([0, 0, 0, 0, 0, 0, k, k], None),
        ([0, 0, 0, 0, 0, 0, 0, l], None),
        argnames="store, expected",
    )
    def test_bit_50_roundtrip(self, connection, bit_table, store, expected):

        reflected = Table("mysql_bits", MetaData(), autoload_with=connection)

        expected = expected or store
        connection.execute(reflected.insert().values(store))
        row = connection.execute(reflected.select()).first()
        eq_(list(row), expected)

    @testing.combinations(
        (([0] * 8), None),
        ([None, None, 0, None, None, None, None, None], None),
        (([1] * 8), None),
        ([sql.text("b'1'")] * 8, [1] * 8),
        ([0, 0, 0, 0, i, i, i, i], None),
        ([0, 0, 0, 0, 0, j, j, j], None),
        ([0, 0, 0, 0, 0, 0, k, k], None),
        ([0, 0, 0, 0, 0, 0, 0, l], None),
        argnames="store, expected",
    )
    def test_bit_50_roundtrip_reflected(
        self, connection, bit_table, store, expected
    ):
        bit_table = Table("mysql_bits", MetaData(), autoload_with=connection)

        expected = expected or store
        connection.execute(bit_table.insert().values(store))
        row = connection.execute(bit_table.select()).first()
        eq_(list(row), expected)

    @testing.metadata_fixture(ddl="class")
    def boolean_table(self, metadata):
        bool_table = Table(
            "mysql_bool",
            metadata,
            Column("b1", BOOLEAN),
            Column("b2", Boolean),
            Column("b3", mysql.MSTinyInteger(1)),
            Column("b4", mysql.MSTinyInteger(1, unsigned=True)),
            Column("b5", mysql.MSTinyInteger),
        )
        return bool_table

    @testing.combinations(
        ([None, None, None, None, None], None),
        ([True, True, 1, 1, 1], None),
        ([False, False, 0, 0, 0], None),
        ([True, True, True, True, True], [True, True, 1, 1, 1]),
        ([False, False, 0, 0, 0], [False, False, 0, 0, 0]),
        argnames="store, expected",
    )
    def test_boolean_roundtrip(
        self, connection, boolean_table, store, expected
    ):
        table = boolean_table

        expected = expected or store
        connection.execute(table.insert().values(store))
        row = connection.execute(table.select()).first()
        eq_(list(row), expected)
        for i, val in enumerate(expected):
            if isinstance(val, bool):
                self.assert_(val is row[i])

    @testing.combinations(
        ([None, None, None, None, None], None),
        ([True, True, 1, 1, 1], [True, True, True, True, 1]),
        ([False, False, 0, 0, 0], [False, False, False, False, 0]),
        ([True, True, True, True, True], [True, True, True, True, 1]),
        ([False, False, 0, 0, 0], [False, False, False, False, 0]),
        argnames="store, expected",
    )
    def test_boolean_roundtrip_reflected(
        self, connection, boolean_table, store, expected
    ):
        table = Table("mysql_bool", MetaData(), autoload_with=connection)
        eq_(colspec(table.c.b3), "b3 TINYINT(1)")
        eq_regex(colspec(table.c.b4), r"b4 TINYINT(?:\(1\))? UNSIGNED")

        table = Table(
            "mysql_bool",
            MetaData(),
            Column("b1", BOOLEAN),
            Column("b2", Boolean),
            Column("b3", BOOLEAN),
            Column("b4", BOOLEAN),
            autoload_with=connection,
        )
        eq_(colspec(table.c.b3), "b3 BOOL")
        eq_(colspec(table.c.b4), "b4 BOOL")

        expected = expected or store
        connection.execute(table.insert().values(store))
        row = connection.execute(table.select()).first()
        eq_(list(row), expected)
        for i, val in enumerate(expected):
            if isinstance(val, bool):
                self.assert_(val is row[i])

    class MyTime(TypeDecorator):
        impl = TIMESTAMP

    @testing.combinations(
        (TIMESTAMP,),
        (MyTime(),),
        (String().with_variant(TIMESTAMP, "mysql"),),
        argnames="type_",
    )
    @testing.requires.mysql_zero_date
    def test_timestamp_nullable(self, metadata, connection, type_):
        ts_table = Table(
            "mysql_timestamp",
            metadata,
            Column("t1", type_),
            Column("t2", type_, nullable=False),
            mysql_engine="InnoDB",
        )
        metadata.create_all(connection)

        # TIMESTAMP without NULL inserts current time when passed
        # NULL.  when not passed, generates 0000-00-00 quite
        # annoyingly.
        # the flag http://dev.mysql.com/doc/refman/5.6/en/\
        # server-system-variables.html#sysvar_explicit_defaults_for_timestamp
        # changes this for 5.6 if set.

        # normalize dates for the amount of time the operation took
        def normalize(dt):
            if dt is None:
                return None
            elif now <= dt <= new_now:
                return now
            else:
                return dt

        now = connection.exec_driver_sql("select now()").scalar()
        connection.execute(ts_table.insert(), {"t1": now, "t2": None})
        connection.execute(ts_table.insert(), {"t1": None, "t2": None})
        connection.execute(ts_table.insert(), {"t2": None})

        new_now = connection.exec_driver_sql("select now()").scalar()

        eq_(
            [
                tuple([normalize(dt) for dt in row])
                for row in connection.execute(ts_table.select())
            ],
            [(now, now), (None, now), (None, now)],
        )

    def test_time_roundtrip(self, metadata, connection):
        t = Table("mysql_time", metadata, Column("t1", mysql.TIME()))

        t.create(connection)

        connection.execute(t.insert().values(t1=datetime.time(8, 37, 35)))
        eq_(
            connection.execute(select(t.c.t1)).scalar(),
            datetime.time(8, 37, 35),
        )

    def test_year(self, metadata, connection):
        """Exercise YEAR."""

        year_table = Table(
            "mysql_year",
            metadata,
            Column("y1", mysql.MSYear),
            Column("y2", mysql.MSYear),
            Column("y3", mysql.MSYear),
            Column("y5", mysql.MSYear(4)),
        )

        for col in year_table.c:
            self.assert_(repr(col))
        year_table.create(connection)
        reflected = Table("mysql_year", MetaData(), autoload_with=connection)

        for table in year_table, reflected:
            connection.execute(
                table.insert().values(["1950", "50", None, 1950])
            )
            row = connection.execute(table.select()).first()
            eq_(list(row), [1950, 2050, None, 1950])
            self.assert_(colspec(table.c.y1).startswith("y1 YEAR"))
            eq_regex(colspec(table.c.y5), r"y5 YEAR(?:\(4\))?")


class JSONTest(fixtures.TestBase):
    __requires__ = ("json_type",)
    __only_on__ = "mysql", "mariadb"
    __backend__ = True

    @testing.requires.reflects_json_type
    def test_reflection(self, metadata, connection):

        Table("mysql_json", metadata, Column("foo", mysql.JSON))
        metadata.create_all(connection)

        reflected = Table("mysql_json", MetaData(), autoload_with=connection)
        is_(reflected.c.foo.type._type_affinity, sqltypes.JSON)
        assert isinstance(reflected.c.foo.type, mysql.JSON)

    def test_rudimental_round_trip(self, metadata, connection):
        # note that test_suite has many more JSON round trip tests
        # using the backend-agnostic JSON type

        mysql_json = Table("mysql_json", metadata, Column("foo", mysql.JSON))
        metadata.create_all(connection)

        value = {"json": {"foo": "bar"}, "recs": ["one", "two"]}

        connection.execute(mysql_json.insert(), dict(foo=value))

        eq_(connection.scalar(select(mysql_json.c.foo)), value)


class EnumSetTest(
    fixtures.TestBase, AssertsExecutionResults, AssertsCompiledSQL
):

    __only_on__ = "mysql", "mariadb"
    __dialect__ = mysql.dialect()
    __backend__ = True

    class SomeEnum(object):
        # Implements PEP 435 in the minimal fashion needed by SQLAlchemy
        __members__ = OrderedDict()

        def __init__(self, name, value):
            self.name = name
            self.value = value
            self.__members__[name] = self
            setattr(self.__class__, name, self)

    one = SomeEnum("one", 1)
    two = SomeEnum("two", 2)
    three = SomeEnum("three", 3)
    a_member = SomeEnum("AMember", "a")
    b_member = SomeEnum("BMember", "b")

    @staticmethod
    def get_enum_string_values(some_enum):
        return [str(v.value) for v in some_enum.__members__.values()]

    def test_enum(self, metadata, connection):
        """Exercise the ENUM type."""

        e1 = mysql.ENUM("a", "b")
        e2 = mysql.ENUM("a", "b")
        e3 = mysql.ENUM("a", "b", strict=True)
        e4 = mysql.ENUM("a", "b", strict=True)

        enum_table = Table(
            "mysql_enum",
            metadata,
            Column("e1", e1),
            Column("e2", e2, nullable=False),
            Column(
                "e2generic",
                Enum("a", "b", validate_strings=True),
                nullable=False,
            ),
            Column("e3", e3),
            Column("e4", e4, nullable=False),
            Column("e5", mysql.ENUM("a", "b")),
            Column("e5generic", Enum("a", "b")),
            Column("e6", mysql.ENUM("'a'", "b")),
            Column(
                "e7",
                mysql.ENUM(
                    EnumSetTest.SomeEnum,
                    values_callable=EnumSetTest.get_enum_string_values,
                ),
            ),
            Column("e8", mysql.ENUM(EnumSetTest.SomeEnum)),
        )

        eq_(colspec(enum_table.c.e1), "e1 ENUM('a','b')")
        eq_(colspec(enum_table.c.e2), "e2 ENUM('a','b') NOT NULL")
        eq_(
            colspec(enum_table.c.e2generic), "e2generic ENUM('a','b') NOT NULL"
        )
        eq_(colspec(enum_table.c.e3), "e3 ENUM('a','b')")
        eq_(colspec(enum_table.c.e4), "e4 ENUM('a','b') NOT NULL")
        eq_(colspec(enum_table.c.e5), "e5 ENUM('a','b')")
        eq_(colspec(enum_table.c.e5generic), "e5generic ENUM('a','b')")
        eq_(colspec(enum_table.c.e6), "e6 ENUM('''a''','b')")
        eq_(colspec(enum_table.c.e7), "e7 ENUM('1','2','3','a','b')")
        eq_(
            colspec(enum_table.c.e8),
            "e8 ENUM('one','two','three','AMember','BMember')",
        )
        enum_table.create(connection)

        assert_raises(
            exc.DBAPIError,
            connection.execute,
            enum_table.insert(),
            dict(
                e1=None,
                e2=None,
                e3=None,
                e4=None,
            ),
        )

        assert enum_table.c.e2generic.type.validate_strings

        assert_raises(
            exc.StatementError,
            connection.execute,
            enum_table.insert(),
            dict(
                e1="c",
                e2="c",
                e2generic="c",
                e3="c",
                e4="c",
                e5="c",
                e5generic="c",
                e6="c",
                e7="c",
                e8="c",
            ),
        )

        connection.execute(enum_table.insert())
        connection.execute(
            enum_table.insert(),
            dict(
                e1="a",
                e2="a",
                e2generic="a",
                e3="a",
                e4="a",
                e5="a",
                e5generic="a",
                e6="'a'",
                e7="a",
                e8="AMember",
            ),
        )
        connection.execute(
            enum_table.insert(),
            dict(
                e1="b",
                e2="b",
                e2generic="b",
                e3="b",
                e4="b",
                e5="b",
                e5generic="b",
                e6="b",
                e7="b",
                e8="BMember",
            ),
        )

        res = connection.execute(enum_table.select()).fetchall()

        expected = [
            (None, "a", "a", None, "a", None, None, None, None, None),
            (
                "a",
                "a",
                "a",
                "a",
                "a",
                "a",
                "a",
                "'a'",
                EnumSetTest.SomeEnum.AMember,
                EnumSetTest.SomeEnum.AMember,
            ),
            (
                "b",
                "b",
                "b",
                "b",
                "b",
                "b",
                "b",
                "b",
                EnumSetTest.SomeEnum.BMember,
                EnumSetTest.SomeEnum.BMember,
            ),
        ]

        eq_(res, expected)

    def _set_fixture_one(self, metadata):
        e1 = mysql.SET("a", "b")
        e2 = mysql.SET("a", "b")
        e4 = mysql.SET("'a'", "b")
        e5 = mysql.SET("a", "b")

        set_table = Table(
            "mysql_set",
            metadata,
            Column("e1", e1),
            Column("e2", e2, nullable=False),
            Column("e3", mysql.SET("a", "b")),
            Column("e4", e4),
            Column("e5", e5),
        )
        return set_table

    def test_set_colspec(self, metadata):
        set_table = self._set_fixture_one(metadata)
        eq_(colspec(set_table.c.e1), "e1 SET('a','b')")
        eq_(colspec(set_table.c.e2), "e2 SET('a','b') NOT NULL")
        eq_(colspec(set_table.c.e3), "e3 SET('a','b')")
        eq_(colspec(set_table.c.e4), "e4 SET('''a''','b')")
        eq_(colspec(set_table.c.e5), "e5 SET('a','b')")

    def test_no_null(self, metadata, connection):
        set_table = self._set_fixture_one(metadata)
        set_table.create(connection)
        assert_raises(
            exc.DBAPIError,
            connection.execute,
            set_table.insert(),
            dict(e1=None, e2=None, e3=None, e4=None),
        )

    @testing.requires.mysql_non_strict
    def test_empty_set_no_empty_string(self, metadata, connection):
        t = Table(
            "t",
            metadata,
            Column("id", Integer),
            Column("data", mysql.SET("a", "b")),
        )
        t.create(connection)
        connection.execute(
            t.insert(),
            [
                {"id": 1, "data": set()},
                {"id": 2, "data": set([""])},
                {"id": 3, "data": set(["a", ""])},
                {"id": 4, "data": set(["b"])},
            ],
        )
        eq_(
            connection.execute(t.select().order_by(t.c.id)).fetchall(),
            [(1, set()), (2, set()), (3, set(["a"])), (4, set(["b"]))],
        )

    def test_bitwise_required_for_empty(self):
        assert_raises_message(
            exc.ArgumentError,
            "Can't use the blank value '' in a SET without setting "
            "retrieve_as_bitwise=True",
            mysql.SET,
            "a",
            "b",
            "",
        )

    def test_empty_set_empty_string(self, metadata, connection):
        t = Table(
            "t",
            metadata,
            Column("id", Integer),
            Column("data", mysql.SET("a", "b", "", retrieve_as_bitwise=True)),
        )
        t.create(connection)
        connection.execute(
            t.insert(),
            [
                {"id": 1, "data": set()},
                {"id": 2, "data": set([""])},
                {"id": 3, "data": set(["a", ""])},
                {"id": 4, "data": set(["b"])},
            ],
        )
        eq_(
            connection.execute(t.select().order_by(t.c.id)).fetchall(),
            [
                (1, set()),
                (2, set([""])),
                (3, set(["a", ""])),
                (4, set(["b"])),
            ],
        )

    def test_string_roundtrip(self, metadata, connection):
        set_table = self._set_fixture_one(metadata)
        set_table.create(connection)
        connection.execute(
            set_table.insert(),
            dict(e1="a", e2="a", e3="a", e4="'a'", e5="a,b"),
        )
        connection.execute(
            set_table.insert(),
            dict(e1="b", e2="b", e3="b", e4="b", e5="a,b"),
        )

        expected = [
            (
                set(["a"]),
                set(["a"]),
                set(["a"]),
                set(["'a'"]),
                set(["a", "b"]),
            ),
            (
                set(["b"]),
                set(["b"]),
                set(["b"]),
                set(["b"]),
                set(["a", "b"]),
            ),
        ]
        res = connection.execute(set_table.select()).fetchall()

        eq_(res, expected)

    def test_unicode_roundtrip(self, metadata, connection):
        set_table = Table(
            "t",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("data", mysql.SET(u("réveillé"), u("drôle"), u("S’il"))),
        )

        set_table.create(connection)
        connection.execute(
            set_table.insert(), {"data": set([u("réveillé"), u("drôle")])}
        )

        row = connection.execute(set_table.select()).first()

        eq_(row, (1, set([u("réveillé"), u("drôle")])))

    def test_int_roundtrip(self, metadata, connection):
        set_table = self._set_fixture_one(metadata)
        set_table.create(connection)
        connection.execute(
            set_table.insert(), dict(e1=1, e2=2, e3=3, e4=3, e5=0)
        )
        res = connection.execute(set_table.select()).first()
        eq_(
            res,
            (
                set(["a"]),
                set(["b"]),
                set(["a", "b"]),
                set(["'a'", "b"]),
                set([]),
            ),
        )

    def test_set_roundtrip_plus_reflection(self, metadata, connection):
        set_table = Table(
            "mysql_set",
            metadata,
            Column("s1", mysql.SET("dq", "sq")),
            Column("s2", mysql.SET("a")),
            Column("s3", mysql.SET("5", "7", "9")),
        )

        eq_(colspec(set_table.c.s1), "s1 SET('dq','sq')")
        eq_(colspec(set_table.c.s2), "s2 SET('a')")
        eq_(colspec(set_table.c.s3), "s3 SET('5','7','9')")
        set_table.create(connection)
        reflected = Table("mysql_set", MetaData(), autoload_with=connection)
        for table in set_table, reflected:

            def roundtrip(store, expected=None):
                expected = expected or store
                connection.execute(table.insert().values(store))
                row = connection.execute(table.select()).first()
                eq_(row, tuple(expected))
                connection.execute(table.delete())

            roundtrip([None, None, None], [None] * 3)
            roundtrip(["", "", ""], [set([])] * 3)
            roundtrip([set(["dq"]), set(["a"]), set(["5"])])
            roundtrip(["dq", "a", "5"], [set(["dq"]), set(["a"]), set(["5"])])
            roundtrip([1, 1, 1], [set(["dq"]), set(["a"]), set(["5"])])
            roundtrip([set(["dq", "sq"]), None, set(["9", "5", "7"])])
        connection.execute(
            set_table.insert(),
            [
                {"s3": set(["5"])},
                {"s3": set(["5", "7"])},
                {"s3": set(["5", "7", "9"])},
                {"s3": set(["7", "9"])},
            ],
        )

        rows = connection.execute(
            select(set_table.c.s3).where(
                set_table.c.s3.in_([set(["5"]), ["5", "7"]])
            )
        ).fetchall()

        eq_(list(rows), [({"5"},), ({"7", "5"},)])

    def test_unicode_enum(self, metadata, connection):
        t1 = Table(
            "table",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("value", Enum(u("réveillé"), u("drôle"), u("S’il"))),
            Column("value2", mysql.ENUM(u("réveillé"), u("drôle"), u("S’il"))),
        )
        metadata.create_all(connection)

        connection.execute(
            t1.insert(),
            [
                dict(value=u("drôle"), value2=u("drôle")),
                dict(value=u("réveillé"), value2=u("réveillé")),
                dict(value=u("S’il"), value2=u("S’il")),
            ],
        )
        eq_(
            connection.execute(t1.select().order_by(t1.c.id)).fetchall(),
            [
                (1, u("drôle"), u("drôle")),
                (2, u("réveillé"), u("réveillé")),
                (3, u("S’il"), u("S’il")),
            ],
        )

        # test reflection of the enum labels

        t2 = Table("table", MetaData(), autoload_with=connection)

        # TODO: what's wrong with the last element ?  is there
        #       latin-1 stuff forcing its way in ?

        eq_(
            t2.c.value.type.enums[0:2], [u("réveillé"), u("drôle")]
        )  # u'S’il') # eh ?

        eq_(
            t2.c.value2.type.enums[0:2], [u("réveillé"), u("drôle")]
        )  # u'S’il') # eh ?

    def test_enum_compile(self):
        e1 = Enum("x", "y", "z", name="somename")
        t1 = Table("sometable", MetaData(), Column("somecolumn", e1))
        self.assert_compile(
            schema.CreateTable(t1),
            "CREATE TABLE sometable (somecolumn " "ENUM('x','y','z'))",
        )
        t1 = Table(
            "sometable",
            MetaData(),
            Column(
                "somecolumn",
                Enum("x", "y", "z", native_enum=False, create_constraint=True),
            ),
        )
        self.assert_compile(
            schema.CreateTable(t1),
            "CREATE TABLE sometable (somecolumn "
            "VARCHAR(1), CHECK (somecolumn IN ('x', "
            "'y', 'z')))",
        )

    def test_enum_parse(self, metadata, connection):

        enum_table = Table(
            "mysql_enum",
            metadata,
            Column("e1", mysql.ENUM("a")),
            Column("e2", mysql.ENUM("")),
            Column("e3", mysql.ENUM("a")),
            Column("e4", mysql.ENUM("")),
            Column("e5", mysql.ENUM("a", "")),
            Column("e6", mysql.ENUM("", "a")),
            Column("e7", mysql.ENUM("", "'a'", "b'b", "'")),
        )

        for col in enum_table.c:
            self.assert_(repr(col))

        enum_table.create(connection)
        reflected = Table("mysql_enum", MetaData(), autoload_with=connection)
        for t in enum_table, reflected:
            eq_(t.c.e1.type.enums, ["a"])
            eq_(t.c.e2.type.enums, [""])
            eq_(t.c.e3.type.enums, ["a"])
            eq_(t.c.e4.type.enums, [""])
            eq_(t.c.e5.type.enums, ["a", ""])
            eq_(t.c.e6.type.enums, ["", "a"])
            eq_(t.c.e7.type.enums, ["", "'a'", "b'b", "'"])

    def test_set_parse(self, metadata, connection):
        set_table = Table(
            "mysql_set",
            metadata,
            Column("e1", mysql.SET("a")),
            Column("e2", mysql.SET("", retrieve_as_bitwise=True)),
            Column("e3", mysql.SET("a")),
            Column("e4", mysql.SET("", retrieve_as_bitwise=True)),
            Column("e5", mysql.SET("a", "", retrieve_as_bitwise=True)),
            Column("e6", mysql.SET("", "a", retrieve_as_bitwise=True)),
            Column(
                "e7",
                mysql.SET(
                    "",
                    "'a'",
                    "b'b",
                    "'",
                    retrieve_as_bitwise=True,
                ),
            ),
        )

        for col in set_table.c:
            self.assert_(repr(col))

        set_table.create(connection)

        # don't want any warnings on reflection
        reflected = Table("mysql_set", MetaData(), autoload_with=connection)
        for t in set_table, reflected:
            eq_(t.c.e1.type.values, ("a",))
            eq_(t.c.e2.type.values, ("",))
            eq_(t.c.e3.type.values, ("a",))
            eq_(t.c.e4.type.values, ("",))
            eq_(t.c.e5.type.values, ("a", ""))
            eq_(t.c.e6.type.values, ("", "a"))
            eq_(t.c.e7.type.values, ("", "'a'", "b'b", "'"))

    @testing.requires.mysql_non_strict
    def test_broken_enum_returns_blanks(self, metadata, connection):
        t = Table(
            "enum_missing",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("e1", sqltypes.Enum("one", "two", "three")),
            Column("e2", mysql.ENUM("one", "two", "three")),
        )
        t.create(connection)

        connection.execute(
            t.insert(),
            [
                {"e1": "nonexistent", "e2": "nonexistent"},
                {"e1": "", "e2": ""},
                {"e1": "two", "e2": "two"},
                {"e1": None, "e2": None},
            ],
        )

        eq_(
            connection.execute(
                select(t.c.e1, t.c.e2).order_by(t.c.id)
            ).fetchall(),
            [("", ""), ("", ""), ("two", "two"), (None, None)],
        )


def colspec(c):
    return testing.db.dialect.ddl_compiler(
        testing.db.dialect, None
    ).get_column_specification(c)
