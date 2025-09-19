"""SQLite-specific tests."""

import datetime
import json
import os
import random

from sqlalchemy import and_
from sqlalchemy import bindparam
from sqlalchemy import CheckConstraint
from sqlalchemy import Column
from sqlalchemy import column
from sqlalchemy import Computed
from sqlalchemy import create_engine
from sqlalchemy import DDL
from sqlalchemy import DefaultClause
from sqlalchemy import event
from sqlalchemy import exc
from sqlalchemy import extract
from sqlalchemy import ForeignKey
from sqlalchemy import func
from sqlalchemy import Index
from sqlalchemy import inspect
from sqlalchemy import literal
from sqlalchemy import MetaData
from sqlalchemy import pool
from sqlalchemy import PrimaryKeyConstraint
from sqlalchemy import schema
from sqlalchemy import select
from sqlalchemy import sql
from sqlalchemy import Table
from sqlalchemy import table
from sqlalchemy import testing
from sqlalchemy import text
from sqlalchemy import tuple_
from sqlalchemy import types as sqltypes
from sqlalchemy import UniqueConstraint
from sqlalchemy.dialects.sqlite import base as sqlite
from sqlalchemy.dialects.sqlite import insert
from sqlalchemy.dialects.sqlite import pysqlite as pysqlite_dialect
from sqlalchemy.engine.url import make_url
from sqlalchemy.schema import CreateTable
from sqlalchemy.schema import FetchedValue
from sqlalchemy.sql.elements import quoted_name
from sqlalchemy.testing import assert_raises
from sqlalchemy.testing import assert_raises_message
from sqlalchemy.testing import AssertsCompiledSQL
from sqlalchemy.testing import AssertsExecutionResults
from sqlalchemy.testing import combinations
from sqlalchemy.testing import engines
from sqlalchemy.testing import eq_
from sqlalchemy.testing import eq_ignore_whitespace
from sqlalchemy.testing import expect_raises
from sqlalchemy.testing import expect_warnings
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import is_
from sqlalchemy.testing import is_true
from sqlalchemy.testing import mock
from sqlalchemy.types import Boolean
from sqlalchemy.types import Date
from sqlalchemy.types import DateTime
from sqlalchemy.types import Integer
from sqlalchemy.types import String
from sqlalchemy.types import Time


def exec_sql(engine, sql, *args, **kwargs):
    # TODO: convert all tests to not use this
    with engine.begin() as conn:
        conn.exec_driver_sql(sql, *args, **kwargs)


class TestTypes(fixtures.TestBase, AssertsExecutionResults):
    __only_on__ = "sqlite"

    __backend__ = True

    def test_boolean(self, connection, metadata):
        """Test that the boolean only treats 1 as True"""

        t = Table(
            "bool_table",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("boo", Boolean(create_constraint=False)),
        )
        metadata.create_all(connection)
        for stmt in [
            "INSERT INTO bool_table (id, boo) VALUES (1, 'false');",
            "INSERT INTO bool_table (id, boo) VALUES (2, 'true');",
            "INSERT INTO bool_table (id, boo) VALUES (3, '1');",
            "INSERT INTO bool_table (id, boo) VALUES (4, '0');",
            "INSERT INTO bool_table (id, boo) VALUES (5, 1);",
            "INSERT INTO bool_table (id, boo) VALUES (6, 0);",
        ]:
            connection.exec_driver_sql(stmt)

        eq_(
            connection.execute(
                t.select().where(t.c.boo).order_by(t.c.id)
            ).fetchall(),
            [(3, True), (5, True)],
        )

    def test_string_dates_passed_raise(self, connection):
        assert_raises(
            exc.StatementError,
            connection.execute,
            select(1).where(bindparam("date", type_=Date)),
            dict(date=str(datetime.date(2007, 10, 30))),
        )

    def test_cant_parse_datetime_message(self, connection):
        for typ, disp in [
            (Time, "time"),
            (DateTime, "datetime"),
            (Date, "date"),
        ]:
            assert_raises_message(
                ValueError,
                "Invalid isoformat string:",
                lambda: connection.execute(
                    text("select 'ASDF' as value").columns(value=typ)
                ).scalar(),
            )

    @testing.provide_metadata
    def test_native_datetime(self):
        dbapi = testing.db.dialect.dbapi
        connect_args = {
            "detect_types": dbapi.PARSE_DECLTYPES | dbapi.PARSE_COLNAMES
        }
        engine = engines.testing_engine(
            options={"connect_args": connect_args, "native_datetime": True}
        )
        t = Table(
            "datetest",
            self.metadata,
            Column("id", Integer, primary_key=True),
            Column("d1", Date),
            Column("d2", sqltypes.TIMESTAMP),
        )
        t.create(engine)
        with engine.begin() as conn:
            conn.execute(
                t.insert(),
                {
                    "d1": datetime.date(2010, 5, 10),
                    "d2": datetime.datetime(2010, 5, 10, 12, 15, 25),
                },
            )
            row = conn.execute(t.select()).first()
            eq_(
                row,
                (
                    1,
                    datetime.date(2010, 5, 10),
                    datetime.datetime(2010, 5, 10, 12, 15, 25),
                ),
            )
            r = conn.execute(func.current_date()).scalar()
            assert isinstance(r, str)

    @testing.provide_metadata
    def test_custom_datetime(self, connection):
        sqlite_date = sqlite.DATETIME(
            # 2004-05-21T00:00:00
            storage_format="%(year)04d-%(month)02d-%(day)02d"
            "T%(hour)02d:%(minute)02d:%(second)02d",
            regexp=r"^(\d+)-(\d+)-(\d+)T(\d+):(\d+):(\d+)$",
        )
        t = Table("t", self.metadata, Column("d", sqlite_date))
        self.metadata.create_all(connection)
        connection.execute(
            t.insert().values(d=datetime.datetime(2010, 10, 15, 12, 37, 0))
        )
        connection.exec_driver_sql(
            "insert into t (d) values ('2004-05-21T00:00:00')"
        )
        eq_(
            connection.exec_driver_sql(
                "select * from t order by d"
            ).fetchall(),
            [("2004-05-21T00:00:00",), ("2010-10-15T12:37:00",)],
        )
        eq_(
            connection.execute(select(t.c.d).order_by(t.c.d)).fetchall(),
            [
                (datetime.datetime(2004, 5, 21, 0, 0),),
                (datetime.datetime(2010, 10, 15, 12, 37),),
            ],
        )

    @testing.provide_metadata
    def test_custom_datetime_text_affinity(self, connection):
        sqlite_date = sqlite.DATETIME(
            storage_format="%(year)04d%(month)02d%(day)02d"
            "%(hour)02d%(minute)02d%(second)02d",
            regexp=r"^(\d{4})(\d{2})(\d{2})(\d{2})(\d{2})(\d{2})$",
        )
        t = Table("t", self.metadata, Column("d", sqlite_date))
        self.metadata.create_all(connection)
        connection.execute(
            t.insert().values(d=datetime.datetime(2010, 10, 15, 12, 37, 0))
        )
        connection.exec_driver_sql(
            "insert into t (d) values ('20040521000000')"
        )
        eq_(
            connection.exec_driver_sql(
                "select * from t order by d"
            ).fetchall(),
            [("20040521000000",), ("20101015123700",)],
        )
        eq_(
            connection.execute(select(t.c.d).order_by(t.c.d)).fetchall(),
            [
                (datetime.datetime(2004, 5, 21, 0, 0),),
                (datetime.datetime(2010, 10, 15, 12, 37),),
            ],
        )

    @testing.provide_metadata
    def test_custom_date_text_affinity(self, connection):
        sqlite_date = sqlite.DATE(
            storage_format="%(year)04d%(month)02d%(day)02d",
            regexp=r"(\d{4})(\d{2})(\d{2})",
        )
        t = Table("t", self.metadata, Column("d", sqlite_date))
        self.metadata.create_all(connection)
        connection.execute(t.insert().values(d=datetime.date(2010, 10, 15)))
        connection.exec_driver_sql("insert into t (d) values ('20040521')")
        eq_(
            connection.exec_driver_sql(
                "select * from t order by d"
            ).fetchall(),
            [("20040521",), ("20101015",)],
        )
        eq_(
            connection.execute(select(t.c.d).order_by(t.c.d)).fetchall(),
            [(datetime.date(2004, 5, 21),), (datetime.date(2010, 10, 15),)],
        )

    @testing.provide_metadata
    def test_custom_date(self, connection):
        sqlite_date = sqlite.DATE(
            # 2004-05-21T00:00:00
            storage_format="%(year)04d|%(month)02d|%(day)02d",
            regexp=r"(\d+)\|(\d+)\|(\d+)",
        )
        t = Table("t", self.metadata, Column("d", sqlite_date))
        self.metadata.create_all(connection)
        connection.execute(t.insert().values(d=datetime.date(2010, 10, 15)))

        connection.exec_driver_sql("insert into t (d) values ('2004|05|21')")

        eq_(
            connection.exec_driver_sql(
                "select * from t order by d"
            ).fetchall(),
            [("2004|05|21",), ("2010|10|15",)],
        )
        eq_(
            connection.execute(select(t.c.d).order_by(t.c.d)).fetchall(),
            [(datetime.date(2004, 5, 21),), (datetime.date(2010, 10, 15),)],
        )

    def test_no_convert_unicode(self):
        """test no utf-8 encoding occurs"""

        dialect = sqlite.dialect()
        for t in (
            String(),
            sqltypes.CHAR(),
            sqltypes.Unicode(),
            sqltypes.UnicodeText(),
            String(),
            sqltypes.CHAR(),
            sqltypes.Unicode(),
            sqltypes.UnicodeText(),
        ):
            bindproc = t.dialect_impl(dialect).bind_processor(dialect)
            assert not bindproc or isinstance(bindproc("some string"), str)


class JSONTest(fixtures.TestBase):
    __requires__ = ("json_type",)
    __only_on__ = "sqlite"
    __backend__ = True

    @testing.requires.reflects_json_type
    def test_reflection(self, connection, metadata):
        Table("json_test", metadata, Column("foo", sqlite.JSON))
        metadata.create_all(connection)

        reflected = Table("json_test", MetaData(), autoload_with=connection)
        is_(reflected.c.foo.type._type_affinity, sqltypes.JSON)
        assert isinstance(reflected.c.foo.type, sqlite.JSON)

    def test_rudimentary_roundtrip(self, metadata, connection):
        sqlite_json = Table("json_test", metadata, Column("foo", sqlite.JSON))

        metadata.create_all(connection)

        value = {"json": {"foo": "bar"}, "recs": ["one", "two"]}

        connection.execute(sqlite_json.insert(), dict(foo=value))

        eq_(connection.scalar(select(sqlite_json.c.foo)), value)

    def test_extract_subobject(self, connection, metadata):
        sqlite_json = Table("json_test", metadata, Column("foo", sqlite.JSON))

        metadata.create_all(connection)

        value = {"json": {"foo": "bar"}}

        connection.execute(sqlite_json.insert(), dict(foo=value))

        eq_(
            connection.scalar(select(sqlite_json.c.foo["json"])), value["json"]
        )

    def test_serializer_args(self, metadata):
        sqlite_json = Table("json_test", metadata, Column("foo", sqlite.JSON))
        data_element = {"foo": "bar"}

        js = mock.Mock(side_effect=json.dumps)
        jd = mock.Mock(side_effect=json.loads)

        engine = engines.testing_engine(
            options=dict(json_serializer=js, json_deserializer=jd)
        )
        metadata.create_all(engine)

        with engine.begin() as conn:
            conn.execute(sqlite_json.insert(), {"foo": data_element})

            row = conn.execute(select(sqlite_json.c.foo)).first()

            eq_(row, (data_element,))
            eq_(js.mock_calls, [mock.call(data_element)])
            eq_(jd.mock_calls, [mock.call(json.dumps(data_element))])


class DateTimeTest(fixtures.TestBase, AssertsCompiledSQL):
    def test_time_microseconds(self):
        dt = datetime.datetime(2008, 6, 27, 12, 0, 0, 125)
        eq_(str(dt), "2008-06-27 12:00:00.000125")
        sldt = sqlite.DATETIME()
        bp = sldt.bind_processor(None)
        eq_(bp(dt), "2008-06-27 12:00:00.000125")
        rp = sldt.result_processor(None, None)
        eq_(rp(bp(dt)), dt)

    def test_truncate_microseconds(self):
        dt = datetime.datetime(2008, 6, 27, 12, 0, 0, 125)
        dt_out = datetime.datetime(2008, 6, 27, 12, 0, 0)
        eq_(str(dt), "2008-06-27 12:00:00.000125")
        sldt = sqlite.DATETIME(truncate_microseconds=True)
        bp = sldt.bind_processor(None)
        eq_(bp(dt), "2008-06-27 12:00:00")
        rp = sldt.result_processor(None, None)
        eq_(rp(bp(dt)), dt_out)

    def test_custom_format_compact(self):
        dt = datetime.datetime(2008, 6, 27, 12, 0, 0, 125)
        eq_(str(dt), "2008-06-27 12:00:00.000125")
        sldt = sqlite.DATETIME(
            storage_format=(
                "%(year)04d%(month)02d%(day)02d"
                "%(hour)02d%(minute)02d%(second)02d%(microsecond)06d"
            ),
            regexp=r"(\d{4})(\d{2})(\d{2})(\d{2})(\d{2})(\d{2})(\d{6})",
        )
        bp = sldt.bind_processor(None)
        eq_(bp(dt), "20080627120000000125")
        rp = sldt.result_processor(None, None)
        eq_(rp(bp(dt)), dt)


class DateTest(fixtures.TestBase, AssertsCompiledSQL):
    def test_default(self):
        dt = datetime.date(2008, 6, 27)
        eq_(str(dt), "2008-06-27")
        sldt = sqlite.DATE()
        bp = sldt.bind_processor(None)
        eq_(bp(dt), "2008-06-27")
        rp = sldt.result_processor(None, None)
        eq_(rp(bp(dt)), dt)

    def test_custom_format(self):
        dt = datetime.date(2008, 6, 27)
        eq_(str(dt), "2008-06-27")
        sldt = sqlite.DATE(
            storage_format="%(month)02d/%(day)02d/%(year)04d",
            regexp=r"(?P<month>\d+)/(?P<day>\d+)/(?P<year>\d+)",
        )
        bp = sldt.bind_processor(None)
        eq_(bp(dt), "06/27/2008")
        rp = sldt.result_processor(None, None)
        eq_(rp(bp(dt)), dt)


class TimeTest(fixtures.TestBase, AssertsCompiledSQL):
    def test_default(self):
        dt = datetime.date(2008, 6, 27)
        eq_(str(dt), "2008-06-27")
        sldt = sqlite.DATE()
        bp = sldt.bind_processor(None)
        eq_(bp(dt), "2008-06-27")
        rp = sldt.result_processor(None, None)
        eq_(rp(bp(dt)), dt)

    def test_truncate_microseconds(self):
        dt = datetime.time(12, 0, 0, 125)
        dt_out = datetime.time(12, 0, 0)
        eq_(str(dt), "12:00:00.000125")
        sldt = sqlite.TIME(truncate_microseconds=True)
        bp = sldt.bind_processor(None)
        eq_(bp(dt), "12:00:00")
        rp = sldt.result_processor(None, None)
        eq_(rp(bp(dt)), dt_out)

    def test_custom_format(self):
        dt = datetime.date(2008, 6, 27)
        eq_(str(dt), "2008-06-27")
        sldt = sqlite.DATE(
            storage_format="%(year)04d%(month)02d%(day)02d",
            regexp=r"(\d{4})(\d{2})(\d{2})",
        )
        bp = sldt.bind_processor(None)
        eq_(bp(dt), "20080627")
        rp = sldt.result_processor(None, None)
        eq_(rp(bp(dt)), dt)


class DefaultsTest(fixtures.TestBase, AssertsCompiledSQL):
    __only_on__ = "sqlite"
    __backend__ = True

    def test_default_reflection(self, connection, metadata):
        specs = [
            (String(3), '"foo"'),
            (sqltypes.NUMERIC(10, 2), "100.50"),
            (Integer, "5"),
            (Boolean, "False"),
        ]
        columns = [
            Column("c%i" % (i + 1), t[0], server_default=text(t[1]))
            for (i, t) in enumerate(specs)
        ]
        Table("t_defaults", metadata, *columns)
        metadata.create_all(connection)
        m2 = MetaData()
        rt = Table("t_defaults", m2, autoload_with=connection)
        expected = [c[1] for c in specs]
        for i, reflected in enumerate(rt.c):
            eq_(str(reflected.server_default.arg), expected[i])

    @testing.exclude(
        "sqlite",
        "<",
        (3, 3, 8),
        "sqlite3 changesets 3353 and 3440 modified "
        "behavior of default displayed in pragma "
        "table_info()",
    )
    def test_default_reflection_2(self):
        db = testing.db
        m = MetaData()
        expected = ["'my_default'", "0"]
        table = """CREATE TABLE r_defaults (
            data VARCHAR(40) DEFAULT 'my_default',
            val INTEGER NOT NULL DEFAULT 0
            )"""
        try:
            exec_sql(db, table)
            rt = Table("r_defaults", m, autoload_with=db)
            for i, reflected in enumerate(rt.c):
                eq_(str(reflected.server_default.arg), expected[i])
        finally:
            exec_sql(db, "DROP TABLE r_defaults")

    def test_default_reflection_3(self):
        db = testing.db
        table = """CREATE TABLE r_defaults (
            data VARCHAR(40) DEFAULT 'my_default',
            val INTEGER NOT NULL DEFAULT 0
            )"""
        try:
            exec_sql(db, table)
            m1 = MetaData()
            t1 = Table("r_defaults", m1, autoload_with=db)
            exec_sql(db, "DROP TABLE r_defaults")
            t1.create(db)
            m2 = MetaData()
            t2 = Table("r_defaults", m2, autoload_with=db)
            self.assert_compile(
                CreateTable(t2),
                "CREATE TABLE r_defaults (data VARCHAR(40) "
                "DEFAULT 'my_default', val INTEGER DEFAULT 0 "
                "NOT NULL)",
            )
        finally:
            exec_sql(db, "DROP TABLE r_defaults")

    @testing.provide_metadata
    def test_boolean_default(self):
        t = Table(
            "t",
            self.metadata,
            Column("x", Boolean, server_default=sql.false()),
        )
        t.create(testing.db)
        with testing.db.begin() as conn:
            conn.execute(t.insert())
            conn.execute(t.insert().values(x=True))
            eq_(
                conn.execute(t.select().order_by(t.c.x)).fetchall(),
                [(False,), (True,)],
            )

    @testing.provide_metadata
    def test_function_default(self):
        t = Table(
            "t",
            self.metadata,
            Column("id", Integer, primary_key=True),
            Column("x", String(), server_default=func.lower("UPPERCASE")),
        )
        t.create(testing.db)
        with testing.db.begin() as conn:
            conn.execute(t.insert())
            conn.execute(t.insert().values(x="foobar"))
            eq_(
                conn.execute(select(t.c.x).order_by(t.c.id)).fetchall(),
                [("uppercase",), ("foobar",)],
            )

    @testing.provide_metadata
    def test_expression_with_function_default(self):
        t = Table(
            "t",
            self.metadata,
            Column("id", Integer, primary_key=True),
            Column("x", Integer(), server_default=func.abs(-5) + 17),
        )
        t.create(testing.db)
        with testing.db.begin() as conn:
            conn.execute(t.insert())
            conn.execute(t.insert().values(x=35))
            eq_(
                conn.execute(select(t.c.x).order_by(t.c.id)).fetchall(),
                [(22,), (35,)],
            )

    def test_old_style_default(self):
        """test non-quoted integer value on older sqlite pragma"""

        dialect = sqlite.dialect()
        info = dialect._get_column_info(
            "foo", "INTEGER", False, 3, False, False, False, None
        )
        eq_(info["default"], "3")


class DialectTest(
    fixtures.TestBase, AssertsExecutionResults, AssertsCompiledSQL
):
    __only_on__ = "sqlite"
    __backend__ = True

    def test_3_7_16_warning(self):
        with expect_warnings(
            r"SQLite version \(3, 2, 8\) is older than 3.7.16, and "
            "will not support right nested joins"
        ):
            sqlite.dialect(
                dbapi=mock.Mock(
                    version_info=(2, 6, 0), sqlite_version_info=(3, 2, 8)
                )
            )

    @testing.only_on("sqlite+pysqlcipher")
    def test_pysqlcipher_connects(self):
        """test #6586"""
        str_url = str(testing.db.url)
        e = create_engine(str_url)

        with e.connect() as conn:
            eq_(conn.scalar(text("select 1")), 1)

    @testing.provide_metadata
    def test_extra_reserved_words(self, connection):
        """Tests reserved words in identifiers.

        'true', 'false', and 'column' are undocumented reserved words
        when used as column identifiers (as of 3.5.1).  Covering them
        here to ensure they remain in place if the dialect's
        reserved_words set is updated in the future."""

        t = Table(
            "reserved",
            self.metadata,
            Column("safe", Integer),
            Column("true", Integer),
            Column("false", Integer),
            Column("column", Integer),
            Column("exists", Integer),
        )
        self.metadata.create_all(connection)
        connection.execute(t.insert(), dict(safe=1))
        result = connection.execute(t.select())
        eq_(list(result), [(1, None, None, None, None)])

    @testing.provide_metadata
    def test_quoted_identifiers_functional_one(self):
        """Tests autoload of tables created with quoted column names."""

        metadata = self.metadata
        exec_sql(
            testing.db,
            """CREATE TABLE "django_content_type" (
            "id" integer NOT NULL PRIMARY KEY,
            "django_stuff" text NULL
        )
        """,
        )
        exec_sql(
            testing.db,
            """
        CREATE TABLE "django_admin_log" (
            "id" integer NOT NULL PRIMARY KEY,
            "action_time" datetime NOT NULL,
            "content_type_id" integer NULL
                    REFERENCES "django_content_type" ("id"),
            "object_id" text NULL,
            "change_message" text NOT NULL
        )
        """,
        )
        table1 = Table("django_admin_log", metadata, autoload_with=testing.db)
        table2 = Table(
            "django_content_type", metadata, autoload_with=testing.db
        )
        j = table1.join(table2)
        assert j.onclause.compare(table1.c.content_type_id == table2.c.id)

    @testing.provide_metadata
    def test_quoted_identifiers_functional_two(self):
        """test the edgiest of edge cases, quoted table/col names
        that start and end with quotes.

        SQLite claims to have fixed this in
        https://www.sqlite.org/src/info/600482d161, however
        it still fails if the FK points to a table name that actually
        has quotes as part of its name.

        """

        metadata = self.metadata
        exec_sql(
            testing.db,
            r'''CREATE TABLE """a""" (
            """id""" integer NOT NULL PRIMARY KEY
        )
        ''',
        )

        # unfortunately, still can't do this; sqlite quadruples
        # up the quotes on the table name here for pragma foreign_key_list
        # exec_sql(testing.db,r'''
        # CREATE TABLE """b""" (
        #    """id""" integer NOT NULL PRIMARY KEY,
        #    """aid""" integer NULL
        #           REFERENCES """a""" ("""id""")
        # )
        # ''')

        table1 = Table(r'"a"', metadata, autoload_with=testing.db)
        assert '"id"' in table1.c

    @testing.provide_metadata
    def test_description_encoding(self, connection):
        t = Table(
            "x",
            self.metadata,
            Column("méil", Integer, primary_key=True),
            Column("\u6e2c\u8a66", Integer),
        )
        self.metadata.create_all(testing.db)

        result = connection.execute(t.select())
        assert "méil" in result.keys()
        assert "\u6e2c\u8a66" in result.keys()

    def test_pool_class(self):
        e = create_engine("sqlite+pysqlite://")
        assert e.pool.__class__ is pool.SingletonThreadPool

        e = create_engine("sqlite+pysqlite:///:memory:")
        assert e.pool.__class__ is pool.SingletonThreadPool

        e = create_engine(
            "sqlite+pysqlite:///file:foo.db?mode=memory&uri=true"
        )
        assert e.pool.__class__ is pool.SingletonThreadPool

        e = create_engine("sqlite+pysqlite:///foo.db")
        # changed as of 2.0 #7490
        assert e.pool.__class__ is pool.QueuePool

    @combinations(
        (
            "sqlite:///foo.db",  # file path is absolute
            ([os.path.abspath("foo.db")], {"check_same_thread": False}),
        ),
        (
            "sqlite:////abs/path/to/foo.db",
            (
                [os.path.abspath("/abs/path/to/foo.db")],
                {"check_same_thread": False},
            ),
        ),
        ("sqlite://", ([":memory:"], {"check_same_thread": True})),
        (
            "sqlite:///?check_same_thread=true",
            ([":memory:"], {"check_same_thread": True}),
        ),
        (
            "sqlite:///file:path/to/database?"
            "check_same_thread=true&timeout=10&mode=ro&nolock=1&uri=true",
            (
                ["file:path/to/database?mode=ro&nolock=1"],
                {"check_same_thread": True, "timeout": 10.0, "uri": True},
            ),
        ),
        (
            "sqlite:///file:path/to/database?mode=ro&uri=true",
            (
                ["file:path/to/database?mode=ro"],
                {"uri": True, "check_same_thread": False},
            ),
        ),
        (
            "sqlite:///file:path/to/database?uri=true",
            (
                ["file:path/to/database"],
                {"uri": True, "check_same_thread": False},
            ),
        ),
    )
    def test_connect_args(self, url, expected):
        """test create_connect_args scenarios including support for uri=True"""

        d = pysqlite_dialect.dialect()
        url = make_url(url)
        eq_(d.create_connect_args(url), expected)

    @testing.combinations(
        ("no_persisted", "", "ignore"),
        ("persisted_none", "", None),
        ("persisted_true", " STORED", True),
        ("persisted_false", " VIRTUAL", False),
        id_="iaa",
    )
    def test_column_computed(self, text, persisted):
        m = MetaData()
        kwargs = {"persisted": persisted} if persisted != "ignore" else {}
        t = Table(
            "t",
            m,
            Column("x", Integer),
            Column("y", Integer, Computed("x + 2", **kwargs)),
        )
        self.assert_compile(
            schema.CreateTable(t),
            "CREATE TABLE t (x INTEGER,"
            " y INTEGER GENERATED ALWAYS AS (x + 2)%s)" % text,
        )

    @testing.combinations(
        (func.localtimestamp(),),
        (func.now(),),
        (func.char_length("test"),),
        (func.aggregate_strings("abc", ","),),
        argnames="fn",
    )
    def test_builtin_functions_roundtrip(self, fn, connection):
        connection.execute(select(fn))


class AttachedDBTest(fixtures.TablesTest):
    __only_on__ = "sqlite"
    __backend__ = True

    run_create_tables = "each"

    @classmethod
    def define_tables(self, metadata):
        meta = metadata

        Table("created", meta, Column("foo", Integer), Column("bar", String))
        Table("local_only", meta, Column("q", Integer), Column("p", Integer))

        Table(
            "created",
            meta,
            Column("id", Integer),
            Column("name", String),
            schema="test_schema",
        )

        Table(
            "another_created",
            meta,
            Column("bat", Integer),
            Column("hoho", String),
            schema="test_schema",
        )

    def test_no_tables(self, connection):
        tt = self.tables("test_schema.created", "test_schema.another_created")
        for t in tt:
            t.drop(connection)
        insp = inspect(connection)
        eq_(insp.get_table_names("test_schema"), [])

    def test_column_names(self, connection):
        insp = inspect(connection)
        eq_(
            [
                d["name"]
                for d in insp.get_columns("created", schema="test_schema")
            ],
            ["id", "name"],
        )
        eq_(
            [d["name"] for d in insp.get_columns("created", schema=None)],
            ["foo", "bar"],
        )

        with expect_raises(exc.NoSuchTableError):
            insp.get_columns("nonexistent", schema="test_schema")

        with expect_raises(exc.NoSuchTableError):
            insp.get_columns("another_created", schema=None)

        with expect_raises(exc.NoSuchTableError):
            insp.get_columns("local_only", schema="test_schema")

        eq_([d["name"] for d in insp.get_columns("local_only")], ["q", "p"])

    def test_table_names_present(self, connection):
        insp = inspect(connection)
        eq_(
            set(insp.get_table_names("test_schema")),
            {"created", "another_created"},
        )

    def test_table_names_system(self, connection):
        insp = inspect(connection)
        eq_(
            set(insp.get_table_names("test_schema")),
            {"created", "another_created"},
        )

    def test_schema_names(self, connection):
        insp = inspect(connection)
        eq_(insp.get_schema_names(), ["main", "test_schema"])

        # implicitly creates a "temp" schema
        connection.exec_driver_sql("select * from sqlite_temp_master")

        # we're not including it
        insp = inspect(connection)
        eq_(insp.get_schema_names(), ["main", "test_schema"])

    def test_reflect_system_table(self, connection):
        meta = MetaData()
        alt_master = Table(
            "sqlite_master",
            meta,
            autoload_with=connection,
            schema="test_schema",
        )
        assert len(alt_master.c) > 0

    def test_reflect_user_table(self, connection):
        m2 = MetaData()
        c2 = Table("created", m2, autoload_with=connection)
        eq_(len(c2.c), 2)

    def test_crud(self, connection):
        (ct,) = self.tables("test_schema.created")
        connection.execute(ct.insert(), {"id": 1, "name": "foo"})
        eq_(connection.execute(ct.select()).fetchall(), [(1, "foo")])

        connection.execute(ct.update(), {"id": 2, "name": "bar"})
        eq_(connection.execute(ct.select()).fetchall(), [(2, "bar")])
        connection.execute(ct.delete())
        eq_(connection.execute(ct.select()).fetchall(), [])

    def test_col_targeting(self, connection):
        (ct,) = self.tables("test_schema.created")

        connection.execute(ct.insert(), {"id": 1, "name": "foo"})
        row = connection.execute(ct.select()).first()
        eq_(row._mapping["id"], 1)
        eq_(row._mapping["name"], "foo")

    def test_col_targeting_union(self, connection):
        (ct,) = self.tables("test_schema.created")
        connection.execute(ct.insert(), {"id": 1, "name": "foo"})
        row = connection.execute(ct.select().union(ct.select())).first()
        eq_(row._mapping["id"], 1)
        eq_(row._mapping["name"], "foo")


class SQLTest(fixtures.TestBase, AssertsCompiledSQL):
    """Tests SQLite-dialect specific compilation."""

    __dialect__ = sqlite.dialect()

    def test_extract(self):
        t = sql.table("t", sql.column("col1"))
        mapping = {
            "month": "%m",
            "day": "%d",
            "year": "%Y",
            "second": "%S",
            "hour": "%H",
            "doy": "%j",
            "minute": "%M",
            "epoch": "%s",
            "dow": "%w",
            "week": "%W",
        }
        for field, subst in mapping.items():
            self.assert_compile(
                select(extract(field, t.c.col1)),
                "SELECT CAST(STRFTIME('%s', t.col1) AS "
                "INTEGER) AS anon_1 FROM t" % subst,
            )

    def test_plain_stringify_returning(self):
        t = Table(
            "t",
            MetaData(),
            Column("myid", Integer, primary_key=True),
            Column("name", String, server_default="some str"),
            Column("description", String, default=func.lower("hi")),
        )
        stmt = t.insert().values().return_defaults()
        eq_ignore_whitespace(
            str(stmt.compile(dialect=sqlite.SQLiteDialect())),
            "INSERT INTO t (description) VALUES (lower(?)) "
            "RETURNING myid, name, description",
        )

    def test_true_false(self):
        self.assert_compile(sql.false(), "0")
        self.assert_compile(sql.true(), "1")

    def test_is_distinct_from(self):
        self.assert_compile(
            sql.column("x").is_distinct_from(None), "x IS NOT NULL"
        )

        self.assert_compile(
            sql.column("x").is_not_distinct_from(False), "x IS 0"
        )

    def test_localtime(self):
        self.assert_compile(
            func.localtimestamp(), "DATETIME(CURRENT_TIMESTAMP, 'localtime')"
        )

    def test_constraints_with_schemas(self):
        metadata = MetaData()
        Table(
            "t1",
            metadata,
            Column("id", Integer, primary_key=True),
            schema="master",
        )
        t2 = Table(
            "t2",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("t1_id", Integer, ForeignKey("master.t1.id")),
            schema="master",
        )
        t3 = Table(
            "t3",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("t1_id", Integer, ForeignKey("master.t1.id")),
            schema="alternate",
        )
        t4 = Table(
            "t4",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("t1_id", Integer, ForeignKey("master.t1.id")),
        )

        # schema->schema, generate REFERENCES with no schema name
        self.assert_compile(
            schema.CreateTable(t2),
            "CREATE TABLE master.t2 ("
            "id INTEGER NOT NULL, "
            "t1_id INTEGER, "
            "PRIMARY KEY (id), "
            "FOREIGN KEY(t1_id) REFERENCES t1 (id)"
            ")",
        )

        # schema->different schema, don't generate REFERENCES
        self.assert_compile(
            schema.CreateTable(t3),
            "CREATE TABLE alternate.t3 ("
            "id INTEGER NOT NULL, "
            "t1_id INTEGER, "
            "PRIMARY KEY (id)"
            ")",
        )

        # same for local schema
        self.assert_compile(
            schema.CreateTable(t4),
            "CREATE TABLE t4 ("
            "id INTEGER NOT NULL, "
            "t1_id INTEGER, "
            "PRIMARY KEY (id)"
            ")",
        )

    @testing.combinations(
        (
            Boolean(create_constraint=True),
            sql.false(),
            "BOOLEAN DEFAULT 0, CHECK (x IN (0, 1))",
        ),
        (
            String(),
            func.sqlite_version(),
            "VARCHAR DEFAULT (sqlite_version())",
        ),
        (Integer(), func.abs(-5) + 17, "INTEGER DEFAULT (abs(-5) + 17)"),
        (
            # test #12425
            String(),
            func.now(),
            "VARCHAR DEFAULT CURRENT_TIMESTAMP",
        ),
        (
            # test #12425
            String(),
            func.datetime(func.now(), "localtime"),
            "VARCHAR DEFAULT (datetime(CURRENT_TIMESTAMP, 'localtime'))",
        ),
        (
            # test #12425
            String(),
            text("datetime(CURRENT_TIMESTAMP, 'localtime')"),
            "VARCHAR DEFAULT (datetime(CURRENT_TIMESTAMP, 'localtime'))",
        ),
        (
            # default with leading spaces that should not be
            # parenthesized
            String,
            text("  'some default'"),
            "VARCHAR DEFAULT   'some default'",
        ),
        (String, text("'some default'"), "VARCHAR DEFAULT 'some default'"),
        argnames="datatype,default,expected",
    )
    def test_column_defaults_ddl(self, datatype, default, expected):
        t = Table(
            "t",
            MetaData(),
            Column(
                "x",
                datatype,
                server_default=default,
            ),
        )

        self.assert_compile(
            CreateTable(t),
            f"CREATE TABLE t (x {expected})",
        )

    def test_create_partial_index(self):
        m = MetaData()
        tbl = Table("testtbl", m, Column("data", Integer))
        idx = Index(
            "test_idx1",
            tbl.c.data,
            sqlite_where=and_(tbl.c.data > 5, tbl.c.data < 10),
        )

        # test quoting and all that

        idx2 = Index(
            "test_idx2",
            tbl.c.data,
            sqlite_where=and_(tbl.c.data > "a", tbl.c.data < "b's"),
        )
        self.assert_compile(
            schema.CreateIndex(idx),
            "CREATE INDEX test_idx1 ON testtbl (data) "
            "WHERE data > 5 AND data < 10",
            dialect=sqlite.dialect(),
        )
        self.assert_compile(
            schema.CreateIndex(idx2),
            "CREATE INDEX test_idx2 ON testtbl (data) "
            "WHERE data > 'a' AND data < 'b''s'",
            dialect=sqlite.dialect(),
        )

    def test_no_autoinc_on_composite_pk(self):
        m = MetaData()
        t = Table(
            "t",
            m,
            Column("x", Integer, primary_key=True, autoincrement=True),
            Column("y", Integer, primary_key=True),
        )
        assert_raises_message(
            exc.CompileError,
            "SQLite does not support autoincrement for composite",
            CreateTable(t).compile,
            dialect=sqlite.dialect(),
        )

    def test_in_tuple(self):
        compiled = (
            tuple_(column("q"), column("p"))
            .in_([(1, 2), (3, 4)])
            .compile(dialect=sqlite.dialect())
        )
        eq_(str(compiled), "(q, p) IN (__[POSTCOMPILE_param_1])")
        eq_(
            compiled._literal_execute_expanding_parameter(
                "param_1",
                compiled.binds["param_1"],
                compiled.binds["param_1"].value,
            ),
            (
                [
                    ("param_1_1_1", 1),
                    ("param_1_1_2", 2),
                    ("param_1_2_1", 3),
                    ("param_1_2_2", 4),
                ],
                "VALUES (?, ?), (?, ?)",
            ),
        )

    def test_create_table_without_rowid(self):
        m = MetaData()
        tbl = Table(
            "atable", m, Column("id", Integer), sqlite_with_rowid=False
        )
        self.assert_compile(
            schema.CreateTable(tbl),
            "CREATE TABLE atable (id INTEGER) WITHOUT ROWID",
        )

    def test_create_table_strict(self):
        m = MetaData()
        table = Table("atable", m, Column("id", Integer), sqlite_strict=True)
        self.assert_compile(
            schema.CreateTable(table),
            "CREATE TABLE atable (id INTEGER) STRICT",
        )

    def test_create_table_without_rowid_strict(self):
        m = MetaData()
        table = Table(
            "atable",
            m,
            Column("id", Integer),
            sqlite_with_rowid=False,
            sqlite_strict=True,
        )
        self.assert_compile(
            schema.CreateTable(table),
            "CREATE TABLE atable (id INTEGER) WITHOUT ROWID, STRICT",
        )


class OnConflictDDLTest(fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = sqlite.dialect()

    def test_on_conflict_clause_column_not_null(self):
        c = Column(
            "test", Integer, nullable=False, sqlite_on_conflict_not_null="FAIL"
        )

        self.assert_compile(
            schema.CreateColumn(c),
            "test INTEGER NOT NULL ON CONFLICT FAIL",
            dialect=sqlite.dialect(),
        )

    def test_on_conflict_clause_column_many_clause(self):
        meta = MetaData()
        t = Table(
            "n",
            meta,
            Column(
                "test",
                Integer,
                nullable=False,
                primary_key=True,
                sqlite_on_conflict_not_null="FAIL",
                sqlite_on_conflict_primary_key="IGNORE",
            ),
        )

        self.assert_compile(
            CreateTable(t),
            "CREATE TABLE n ("
            "test INTEGER NOT NULL ON CONFLICT FAIL, "
            "PRIMARY KEY (test) ON CONFLICT IGNORE)",
            dialect=sqlite.dialect(),
        )

    def test_on_conflict_clause_unique_constraint_from_column(self):
        meta = MetaData()
        t = Table(
            "n",
            meta,
            Column(
                "x", String(30), unique=True, sqlite_on_conflict_unique="FAIL"
            ),
        )

        self.assert_compile(
            CreateTable(t),
            "CREATE TABLE n (x VARCHAR(30), UNIQUE (x) ON CONFLICT FAIL)",
            dialect=sqlite.dialect(),
        )

    def test_on_conflict_clause_unique_constraint(self):
        meta = MetaData()
        t = Table(
            "n",
            meta,
            Column("id", Integer),
            Column("x", String(30)),
            UniqueConstraint("id", "x", sqlite_on_conflict="FAIL"),
        )

        self.assert_compile(
            CreateTable(t),
            "CREATE TABLE n (id INTEGER, x VARCHAR(30), "
            "UNIQUE (id, x) ON CONFLICT FAIL)",
            dialect=sqlite.dialect(),
        )

    def test_on_conflict_clause_primary_key(self):
        meta = MetaData()
        t = Table(
            "n",
            meta,
            Column(
                "id",
                Integer,
                primary_key=True,
                sqlite_on_conflict_primary_key="FAIL",
            ),
            sqlite_autoincrement=True,
        )

        self.assert_compile(
            CreateTable(t),
            "CREATE TABLE n (id INTEGER NOT NULL "
            "PRIMARY KEY ON CONFLICT FAIL AUTOINCREMENT)",
            dialect=sqlite.dialect(),
        )

    def test_on_conflict_clause_primary_key_constraint_from_column(self):
        meta = MetaData()
        t = Table(
            "n",
            meta,
            Column(
                "x",
                String(30),
                sqlite_on_conflict_primary_key="FAIL",
                primary_key=True,
            ),
        )

        self.assert_compile(
            CreateTable(t),
            "CREATE TABLE n (x VARCHAR(30) NOT NULL, "
            "PRIMARY KEY (x) ON CONFLICT FAIL)",
            dialect=sqlite.dialect(),
        )

    def test_on_conflict_clause_check_constraint(self):
        meta = MetaData()
        t = Table(
            "n",
            meta,
            Column("id", Integer),
            Column("x", Integer),
            CheckConstraint("id > x", sqlite_on_conflict="FAIL"),
        )

        self.assert_compile(
            CreateTable(t),
            "CREATE TABLE n (id INTEGER, x INTEGER, "
            "CHECK (id > x) ON CONFLICT FAIL)",
            dialect=sqlite.dialect(),
        )

    def test_on_conflict_clause_check_constraint_from_column(self):
        meta = MetaData()
        t = Table(
            "n",
            meta,
            Column(
                "x",
                Integer,
                CheckConstraint("x > 1", sqlite_on_conflict="FAIL"),
            ),
        )

        assert_raises_message(
            exc.CompileError,
            "SQLite does not support on conflict "
            "clause for column check constraint",
            CreateTable(t).compile,
            dialect=sqlite.dialect(),
        )

    def test_on_conflict_clause_primary_key_constraint(self):
        meta = MetaData()
        t = Table(
            "n",
            meta,
            Column("id", Integer),
            Column("x", String(30)),
            PrimaryKeyConstraint("id", "x", sqlite_on_conflict="FAIL"),
        )

        self.assert_compile(
            CreateTable(t),
            "CREATE TABLE n ("
            "id INTEGER NOT NULL, "
            "x VARCHAR(30) NOT NULL, "
            "PRIMARY KEY (id, x) ON CONFLICT FAIL)",
            dialect=sqlite.dialect(),
        )


class InsertTest(fixtures.TestBase, AssertsExecutionResults):
    """Tests inserts and autoincrement."""

    __only_on__ = "sqlite"
    __backend__ = True

    # empty insert was added as of sqlite 3.3.8.

    def _test_empty_insert(self, connection, table, expect=1):
        try:
            table.create(connection)
            for wanted in expect, expect * 2:
                connection.execute(table.insert())
                rows = connection.execute(table.select()).fetchall()
                eq_(len(rows), wanted)
        finally:
            table.drop(connection)

    def test_empty_insert_pk1(self, connection):
        self._test_empty_insert(
            connection,
            Table(
                "a",
                MetaData(),
                Column("id", Integer, primary_key=True),
            ),
        )

    def test_empty_insert_pk2(self, connection):
        # now warns due to [ticket:3216]

        with expect_warnings(
            "Column 'b.x' is marked as a member of the "
            "primary key for table 'b'",
            "Column 'b.y' is marked as a member of the "
            "primary key for table 'b'",
        ):
            assert_raises(
                exc.IntegrityError,
                self._test_empty_insert,
                connection,
                Table(
                    "b",
                    MetaData(),
                    Column("x", Integer, primary_key=True),
                    Column("y", Integer, primary_key=True),
                ),
            )

    def test_empty_insert_pk2_fv(self, connection):
        assert_raises(
            exc.DBAPIError,
            self._test_empty_insert,
            connection,
            Table(
                "b",
                MetaData(),
                Column(
                    "x",
                    Integer,
                    primary_key=True,
                    server_default=FetchedValue(),
                ),
                Column(
                    "y",
                    Integer,
                    primary_key=True,
                    server_default=FetchedValue(),
                ),
            ),
        )

    def test_empty_insert_pk3(self, connection):
        # now warns due to [ticket:3216]
        with expect_warnings(
            "Column 'c.x' is marked as a member of the primary key for table"
        ):
            assert_raises(
                exc.IntegrityError,
                self._test_empty_insert,
                connection,
                Table(
                    "c",
                    MetaData(),
                    Column("x", Integer, primary_key=True),
                    Column(
                        "y", Integer, DefaultClause("123"), primary_key=True
                    ),
                ),
            )

    def test_empty_insert_pk3_fv(self, connection):
        assert_raises(
            exc.DBAPIError,
            self._test_empty_insert,
            connection,
            Table(
                "c",
                MetaData(),
                Column(
                    "x",
                    Integer,
                    primary_key=True,
                    server_default=FetchedValue(),
                ),
                Column("y", Integer, DefaultClause("123"), primary_key=True),
            ),
        )

    def test_empty_insert_pk4(self, connection):
        self._test_empty_insert(
            connection,
            Table(
                "d",
                MetaData(),
                Column("x", Integer, primary_key=True),
                Column("y", Integer, DefaultClause("123")),
            ),
        )

    def test_empty_insert_nopk1(self, connection):
        self._test_empty_insert(
            connection, Table("e", MetaData(), Column("id", Integer))
        )

    def test_empty_insert_nopk2(self, connection):
        self._test_empty_insert(
            connection,
            Table(
                "f",
                MetaData(),
                Column("x", Integer),
                Column("y", Integer),
            ),
        )

    @testing.provide_metadata
    def test_inserts_with_spaces(self, connection):
        tbl = Table(
            "tbl",
            self.metadata,
            Column("with space", Integer),
            Column("without", Integer),
        )
        tbl.create(connection)
        connection.execute(tbl.insert(), {"without": 123})
        eq_(connection.execute(tbl.select()).fetchall(), [(None, 123)])
        connection.execute(tbl.insert(), {"with space": 456})
        eq_(
            connection.execute(tbl.select()).fetchall(),
            [(None, 123), (456, None)],
        )


def full_text_search_missing():
    """Test if full text search is not implemented and return False if
    it is and True otherwise."""

    try:
        exec_sql(testing.db, "CREATE VIRTUAL TABLE t using FTS3;")
        exec_sql(testing.db, "DROP TABLE t;")
        return False
    except Exception:
        return True


metadata = cattable = matchtable = None


class MatchTest(fixtures.TestBase, AssertsCompiledSQL):
    __only_on__ = "sqlite"
    __skip_if__ = (full_text_search_missing,)
    __backend__ = True

    @classmethod
    def setup_test_class(cls):
        global metadata, cattable, matchtable
        metadata = MetaData()
        exec_sql(
            testing.db,
            """
        CREATE VIRTUAL TABLE cattable using FTS3 (
            id INTEGER NOT NULL,
            description VARCHAR(50),
            PRIMARY KEY (id)
        )
        """,
        )
        cattable = Table("cattable", metadata, autoload_with=testing.db)
        exec_sql(
            testing.db,
            """
        CREATE VIRTUAL TABLE matchtable using FTS3 (
            id INTEGER NOT NULL,
            title VARCHAR(200),
            category_id INTEGER NOT NULL,
            PRIMARY KEY (id)
        )
        """,
        )
        matchtable = Table("matchtable", metadata, autoload_with=testing.db)
        with testing.db.begin() as conn:
            metadata.create_all(conn)

            conn.execute(
                cattable.insert(),
                [
                    {"id": 1, "description": "Python"},
                    {"id": 2, "description": "Ruby"},
                ],
            )
            conn.execute(
                matchtable.insert(),
                [
                    {
                        "id": 1,
                        "title": "Agile Web Development with Rails",
                        "category_id": 2,
                    },
                    {"id": 2, "title": "Dive Into Python", "category_id": 1},
                    {
                        "id": 3,
                        "title": "Programming Matz's Ruby",
                        "category_id": 2,
                    },
                    {
                        "id": 4,
                        "title": "The Definitive Guide to Django",
                        "category_id": 1,
                    },
                    {
                        "id": 5,
                        "title": "Python in a Nutshell",
                        "category_id": 1,
                    },
                ],
            )

    @classmethod
    def teardown_test_class(cls):
        metadata.drop_all(testing.db)

    def test_expression(self):
        self.assert_compile(
            matchtable.c.title.match("somstr"),
            "matchtable.title MATCH ?",
            dialect=sqlite.dialect(),
        )

    def test_simple_match(self, connection):
        results = connection.execute(
            matchtable.select()
            .where(matchtable.c.title.match("python"))
            .order_by(matchtable.c.id)
        ).fetchall()
        eq_([2, 5], [r.id for r in results])

    def test_simple_prefix_match(self, connection):
        results = connection.execute(
            matchtable.select().where(matchtable.c.title.match("nut*"))
        ).fetchall()
        eq_([5], [r.id for r in results])

    def test_or_match(self, connection):
        results2 = connection.execute(
            matchtable.select()
            .where(matchtable.c.title.match("nutshell OR ruby"))
            .order_by(matchtable.c.id)
        ).fetchall()
        eq_([3, 5], [r.id for r in results2])

    def test_and_match(self, connection):
        results2 = connection.execute(
            matchtable.select().where(
                matchtable.c.title.match("python nutshell")
            )
        ).fetchall()
        eq_([5], [r.id for r in results2])

    def test_match_across_joins(self, connection):
        results = connection.execute(
            matchtable.select()
            .where(
                and_(
                    cattable.c.id == matchtable.c.category_id,
                    cattable.c.description.match("Ruby"),
                )
            )
            .order_by(matchtable.c.id)
        ).fetchall()
        eq_([1, 3], [r.id for r in results])


class AutoIncrementTest(fixtures.TestBase, AssertsCompiledSQL):
    def test_sqlite_autoincrement(self):
        table = Table(
            "autoinctable",
            MetaData(),
            Column("id", Integer, primary_key=True),
            Column("x", Integer, default=None),
            sqlite_autoincrement=True,
        )
        self.assert_compile(
            schema.CreateTable(table),
            "CREATE TABLE autoinctable (id INTEGER NOT "
            "NULL PRIMARY KEY AUTOINCREMENT, x INTEGER)",
            dialect=sqlite.dialect(),
        )

    def test_sqlite_autoincrement_constraint(self):
        table = Table(
            "autoinctable",
            MetaData(),
            Column("id", Integer, primary_key=True),
            Column("x", Integer, default=None),
            UniqueConstraint("x"),
            sqlite_autoincrement=True,
        )
        self.assert_compile(
            schema.CreateTable(table),
            "CREATE TABLE autoinctable (id INTEGER NOT "
            "NULL PRIMARY KEY AUTOINCREMENT, x "
            "INTEGER, UNIQUE (x))",
            dialect=sqlite.dialect(),
        )

    def test_sqlite_no_autoincrement(self):
        table = Table(
            "noautoinctable",
            MetaData(),
            Column("id", Integer, primary_key=True),
            Column("x", Integer, default=None),
        )
        self.assert_compile(
            schema.CreateTable(table),
            "CREATE TABLE noautoinctable (id INTEGER "
            "NOT NULL, x INTEGER, PRIMARY KEY (id))",
            dialect=sqlite.dialect(),
        )

    def test_sqlite_autoincrement_int_affinity(self):
        class MyInteger(sqltypes.TypeDecorator):
            impl = Integer
            cache_ok = True

        table = Table(
            "autoinctable",
            MetaData(),
            Column("id", MyInteger, primary_key=True),
            sqlite_autoincrement=True,
        )
        self.assert_compile(
            schema.CreateTable(table),
            "CREATE TABLE autoinctable (id INTEGER NOT "
            "NULL PRIMARY KEY AUTOINCREMENT)",
            dialect=sqlite.dialect(),
        )


class ReflectHeadlessFKsTest(fixtures.TestBase):
    __only_on__ = "sqlite"
    __backend__ = True

    def setup_test(self):
        exec_sql(testing.db, "CREATE TABLE a (id INTEGER PRIMARY KEY)")
        # this syntax actually works on other DBs perhaps we'd want to add
        # tests to test_reflection
        exec_sql(
            testing.db, "CREATE TABLE b (id INTEGER PRIMARY KEY REFERENCES a)"
        )

    def teardown_test(self):
        exec_sql(testing.db, "drop table b")
        exec_sql(testing.db, "drop table a")

    def test_reflect_tables_fk_no_colref(self):
        meta = MetaData()
        a = Table("a", meta, autoload_with=testing.db)
        b = Table("b", meta, autoload_with=testing.db)

        assert b.c.id.references(a.c.id)


class KeywordInDatabaseNameTest(fixtures.TestBase):
    __only_on__ = "sqlite"
    __backend__ = True

    @testing.fixture
    def db_fixture(self, connection):
        connection.exec_driver_sql(
            'ATTACH %r AS "default"' % connection.engine.url.database
        )
        connection.exec_driver_sql(
            'CREATE TABLE "default".a (id INTEGER PRIMARY KEY)'
        )
        try:
            yield
        finally:
            connection.exec_driver_sql('drop table "default".a')
            connection.exec_driver_sql('DETACH DATABASE "default"')

    def test_reflect(self, connection, db_fixture):
        meta = MetaData(schema="default")
        meta.reflect(connection)
        assert "default.a" in meta.tables


class ConstraintReflectionTest(fixtures.TestBase):
    __only_on__ = "sqlite"
    __backend__ = True

    @classmethod
    def setup_test_class(cls):
        with testing.db.begin() as conn:
            conn.exec_driver_sql("CREATE TABLE a1 (id INTEGER PRIMARY KEY)")
            conn.exec_driver_sql("CREATE TABLE a2 (id INTEGER PRIMARY KEY)")
            conn.exec_driver_sql(
                "CREATE TABLE b (id INTEGER PRIMARY KEY, "
                "FOREIGN KEY(id) REFERENCES a1(id),"
                "FOREIGN KEY(id) REFERENCES a2(id)"
                ")"
            )
            conn.exec_driver_sql(
                "CREATE TABLE c (id INTEGER, "
                "CONSTRAINT bar PRIMARY KEY(id),"
                "CONSTRAINT foo1 FOREIGN KEY(id) REFERENCES a1(id),"
                "CONSTRAINT foo2 FOREIGN KEY(id) REFERENCES a2(id)"
                ")"
            )
            conn.exec_driver_sql(
                # the lower casing + inline is intentional here
                "CREATE TABLE d (id INTEGER, x INTEGER unique)"
            )
            conn.exec_driver_sql(
                # the lower casing + inline is intentional here
                "CREATE TABLE d1 "
                '(id INTEGER, "some ( STUPID n,ame" INTEGER unique)'
            )
            conn.exec_driver_sql(
                # the lower casing + inline is intentional here
                'CREATE TABLE d2 ( "some STUPID n,ame" INTEGER unique)'
            )
            conn.exec_driver_sql(
                # the lower casing + inline is intentional here
                'CREATE TABLE d3 ( "some STUPID n,ame" INTEGER NULL unique)'
            )

            conn.exec_driver_sql(
                # lower casing + inline is intentional
                "CREATE TABLE e (id INTEGER, x INTEGER references a2(id))"
            )
            conn.exec_driver_sql(
                'CREATE TABLE e1 (id INTEGER, "some ( STUPID n,ame" INTEGER '
                'references a2   ("some ( STUPID n,ame"))'
            )
            conn.exec_driver_sql(
                "CREATE TABLE e2 (id INTEGER, "
                '"some ( STUPID n,ame" INTEGER NOT NULL  '
                'references a2   ("some ( STUPID n,ame"))'
            )

            conn.exec_driver_sql(
                "CREATE TABLE f (x INTEGER, CONSTRAINT foo_fx UNIQUE(x))"
            )
            conn.exec_driver_sql(
                # intentional broken casing
                "CREATE TABLE h (x INTEGER, COnstraINT foo_hx unIQUE(x))"
            )
            conn.exec_driver_sql(
                "CREATE TABLE i (x INTEGER, y INTEGER, PRIMARY KEY(x, y))"
            )
            conn.exec_driver_sql(
                "CREATE TABLE j (id INTEGER, q INTEGER, p INTEGER, "
                "PRIMARY KEY(id), FOreiGN KEY(q,p) REFERENCes  i(x,y))"
            )
            conn.exec_driver_sql(
                "CREATE TABLE k (id INTEGER, q INTEGER, p INTEGER, "
                "PRIMARY KEY(id), "
                "conSTRAINT my_fk FOreiGN KEY (  q  , p  )   "
                "REFERENCes   i    (  x ,   y ))"
            )

            meta = MetaData()
            Table("l", meta, Column("bar", String, index=True), schema="main")

            Table(
                "m",
                meta,
                Column("id", Integer, primary_key=True),
                Column("x", String(30)),
                UniqueConstraint("x"),
            )

            Table(
                "p",
                meta,
                Column("id", Integer),
                PrimaryKeyConstraint("id", name="pk_name"),
            )

            Table("q", meta, Column("id", Integer), PrimaryKeyConstraint("id"))

            # intentional new line
            Table(
                "r",
                meta,
                Column("id", Integer),
                Column("value", Integer),
                Column("prefix", String),
                CheckConstraint("id > 0"),
                UniqueConstraint("prefix", name="prefix_named"),
                # Constraint definition with newline and tab characters
                CheckConstraint(
                    """((value > 0) AND \n\t(value < 100) AND \n\t
                      (value != 50))""",
                    name="ck_r_value_multiline",
                ),
                UniqueConstraint("value"),
                # Constraint name with special chars and 'check' in the name
                CheckConstraint("value IS NOT NULL", name="^check-r* #\n\t"),
                PrimaryKeyConstraint("id", name="pk_name"),
                # Constraint definition with special characters.
                CheckConstraint("prefix NOT GLOB '*[^-. /#,]*'"),
            )

            meta.create_all(conn)

            # will contain an "autoindex"
            conn.exec_driver_sql(
                "create table o (foo varchar(20) primary key)"
            )
            conn.exec_driver_sql(
                "CREATE TABLE onud_test (id INTEGER PRIMARY KEY, "
                "c1 INTEGER, c2 INTEGER, c3 INTEGER, c4 INTEGER, "
                "CONSTRAINT fk1 FOREIGN KEY (c1) REFERENCES a1(id) "
                "ON DELETE SET NULL, "
                "CONSTRAINT fk2 FOREIGN KEY (c2) REFERENCES a1(id) "
                "ON UPDATE CASCADE, "
                "CONSTRAINT fk3 FOREIGN KEY (c3) REFERENCES a2(id) "
                "ON DELETE CASCADE ON UPDATE SET NULL,"
                "CONSTRAINT fk4 FOREIGN KEY (c4) REFERENCES a2(id) "
                "ON UPDATE NO ACTION)"
            )

            conn.exec_driver_sql(
                "CREATE TABLE deferrable_test (id INTEGER PRIMARY KEY, "
                "c1 INTEGER, c2 INTEGER, c3 INTEGER, c4 INTEGER, "
                "CONSTRAINT fk1 FOREIGN KEY (c1) REFERENCES a1(id) "
                "DEFERRABLE,"
                "CONSTRAINT fk2 FOREIGN KEY (c2) REFERENCES a1(id) "
                "NOT DEFERRABLE,"
                "CONSTRAINT fk3 FOREIGN KEY (c3) REFERENCES a2(id) "
                "ON UPDATE CASCADE "
                "DEFERRABLE INITIALLY DEFERRED,"
                "CONSTRAINT fk4 FOREIGN KEY (c4) REFERENCES a2(id) "
                "NOT DEFERRABLE INITIALLY IMMEDIATE)"
            )

            conn.exec_driver_sql(
                "CREATE TABLE cp ("
                "id INTEGER NOT NULL,\n"
                "q INTEGER, \n"
                "p INTEGER, \n"
                "CONSTRAINT cq CHECK (p = 1 OR (p > 2 AND p < 5)),\n"
                "PRIMARY KEY (id)\n"
                ")"
            )

            conn.exec_driver_sql(
                "CREATE TABLE cp_inline (\n"
                "id INTEGER NOT NULL,\n"
                "q INTEGER CHECK (q > 1 AND q < 6), \n"
                "p INTEGER CONSTRAINT cq CHECK (p = 1 OR (p > 2 AND p < 5)),\n"
                "PRIMARY KEY (id)\n"
                ")"
            )

            conn.exec_driver_sql(
                "CREATE TABLE implicit_referred (pk integer primary key)"
            )
            # single col foreign key with no referred column given,
            # must assume primary key of referred table
            conn.exec_driver_sql(
                "CREATE TABLE implicit_referrer "
                "(id integer REFERENCES implicit_referred)"
            )

            conn.exec_driver_sql(
                "CREATE TABLE implicit_referred_comp "
                "(pk1 integer, pk2 integer, primary key (pk1, pk2))"
            )
            # composite foreign key with no referred columns given,
            # must assume primary key of referred table
            conn.exec_driver_sql(
                "CREATE TABLE implicit_referrer_comp "
                "(id1 integer, id2 integer, foreign key(id1, id2) "
                "REFERENCES implicit_referred_comp)"
            )

            # worst case - FK that refers to nonexistent table so we can't
            # get pks.  requires FK pragma is turned off
            conn.exec_driver_sql(
                "CREATE TABLE implicit_referrer_comp_fake "
                "(id1 integer, id2 integer, foreign key(id1, id2) "
                "REFERENCES fake_table)"
            )

    @classmethod
    def teardown_test_class(cls):
        with testing.db.begin() as conn:
            for name in [
                "implicit_referrer_comp_fake",
                "implicit_referrer",
                "implicit_referred",
                "implicit_referrer_comp",
                "implicit_referred_comp",
                "m",
                "main.l",
                "k",
                "j",
                "i",
                "h",
                "f",
                "e",
                "e1",
                "d",
                "d1",
                "d2",
                "c",
                "b",
                "a1",
                "a2",
                "r",
            ]:
                conn.exec_driver_sql("drop table %s" % name)

    @testing.fixture
    def temp_table_fixture(self, connection):
        connection.exec_driver_sql(
            "CREATE TEMPORARY TABLE g "
            "(x INTEGER, CONSTRAINT foo_gx UNIQUE(x))"
        )

        n = Table(
            "n",
            MetaData(),
            Column("id", Integer, primary_key=True),
            Column("x", String(30)),
            UniqueConstraint("x"),
            prefixes=["TEMPORARY"],
        )

        n.create(connection)
        try:
            yield
        finally:
            connection.exec_driver_sql("DROP TABLE g")
            n.drop(connection)

    def test_legacy_quoted_identifiers_unit(self):
        dialect = sqlite.dialect()
        dialect._broken_fk_pragma_quotes = True

        for row in [
            (0, None, "target", "tid", "id", None),
            (0, None, '"target"', "tid", "id", None),
            (0, None, "[target]", "tid", "id", None),
            (0, None, "'target'", "tid", "id", None),
            (0, None, "`target`", "tid", "id", None),
        ]:

            def _get_table_pragma(*arg, **kw):
                return [row]

            def _get_table_sql(*arg, **kw):
                return (
                    "CREATE TABLE foo "
                    "(tid INTEGER, "
                    "FOREIGN KEY(tid) REFERENCES %s (id))" % row[2]
                )

            with mock.patch.object(
                dialect, "_get_table_pragma", _get_table_pragma
            ):
                with mock.patch.object(
                    dialect, "_get_table_sql", _get_table_sql
                ):
                    fkeys = dialect.get_foreign_keys(None, "foo")
                    eq_(
                        fkeys,
                        [
                            {
                                "referred_table": "target",
                                "referred_columns": ["id"],
                                "referred_schema": None,
                                "name": None,
                                "constrained_columns": ["tid"],
                                "options": {},
                            }
                        ],
                    )

    def test_foreign_key_name_is_none(self):
        # and not "0"
        inspector = inspect(testing.db)
        fks = inspector.get_foreign_keys("b")
        eq_(
            fks,
            [
                {
                    "referred_table": "a1",
                    "referred_columns": ["id"],
                    "referred_schema": None,
                    "name": None,
                    "constrained_columns": ["id"],
                    "options": {},
                },
                {
                    "referred_table": "a2",
                    "referred_columns": ["id"],
                    "referred_schema": None,
                    "name": None,
                    "constrained_columns": ["id"],
                    "options": {},
                },
            ],
        )

    def test_foreign_key_name_is_not_none(self):
        inspector = inspect(testing.db)
        fks = inspector.get_foreign_keys("c")
        eq_(
            fks,
            [
                {
                    "referred_table": "a1",
                    "referred_columns": ["id"],
                    "referred_schema": None,
                    "name": "foo1",
                    "constrained_columns": ["id"],
                    "options": {},
                },
                {
                    "referred_table": "a2",
                    "referred_columns": ["id"],
                    "referred_schema": None,
                    "name": "foo2",
                    "constrained_columns": ["id"],
                    "options": {},
                },
            ],
        )

    def test_foreign_key_implicit_parent(self):
        inspector = inspect(testing.db)
        fks = inspector.get_foreign_keys("implicit_referrer")
        eq_(
            fks,
            [
                {
                    "name": None,
                    "constrained_columns": ["id"],
                    "referred_schema": None,
                    "referred_table": "implicit_referred",
                    "referred_columns": ["pk"],
                    "options": {},
                }
            ],
        )

    def test_foreign_key_composite_implicit_parent(self):
        inspector = inspect(testing.db)
        fks = inspector.get_foreign_keys("implicit_referrer_comp")
        eq_(
            fks,
            [
                {
                    "name": None,
                    "constrained_columns": ["id1", "id2"],
                    "referred_schema": None,
                    "referred_table": "implicit_referred_comp",
                    "referred_columns": ["pk1", "pk2"],
                    "options": {},
                }
            ],
        )

    def test_foreign_key_implicit_missing_parent(self):
        # test when the FK refers to a non-existent table and column names
        # aren't given.   only sqlite allows this case to exist
        inspector = inspect(testing.db)
        fks = inspector.get_foreign_keys("implicit_referrer_comp_fake")
        # the referred table doesn't exist but the operation does not fail
        eq_(
            fks,
            [
                {
                    "name": None,
                    "constrained_columns": ["id1", "id2"],
                    "referred_schema": None,
                    "referred_table": "fake_table",
                    "referred_columns": [],
                    "options": {},
                }
            ],
        )

    def test_foreign_key_implicit_missing_parent_reflection(self):
        # full Table reflection fails however, which is not a new behavior
        m = MetaData()
        assert_raises_message(
            exc.NoSuchTableError,
            "fake_table",
            Table,
            "implicit_referrer_comp_fake",
            m,
            autoload_with=testing.db,
        )

    def test_unnamed_inline_foreign_key(self):
        inspector = inspect(testing.db)
        fks = inspector.get_foreign_keys("e")
        eq_(
            fks,
            [
                {
                    "referred_table": "a2",
                    "referred_columns": ["id"],
                    "referred_schema": None,
                    "name": None,
                    "constrained_columns": ["x"],
                    "options": {},
                }
            ],
        )

    def test_unnamed_inline_foreign_key_quoted(self):
        inspector = inspect(testing.db)
        fks = inspector.get_foreign_keys("e1")
        eq_(
            fks,
            [
                {
                    "referred_table": "a2",
                    "referred_columns": ["some ( STUPID n,ame"],
                    "referred_schema": None,
                    "options": {},
                    "name": None,
                    "constrained_columns": ["some ( STUPID n,ame"],
                }
            ],
        )
        fks = inspector.get_foreign_keys("e2")
        eq_(
            fks,
            [
                {
                    "referred_table": "a2",
                    "referred_columns": ["some ( STUPID n,ame"],
                    "referred_schema": None,
                    "options": {},
                    "name": None,
                    "constrained_columns": ["some ( STUPID n,ame"],
                }
            ],
        )

    def test_foreign_key_composite_broken_casing(self):
        inspector = inspect(testing.db)
        fks = inspector.get_foreign_keys("j")
        eq_(
            fks,
            [
                {
                    "referred_table": "i",
                    "referred_columns": ["x", "y"],
                    "referred_schema": None,
                    "name": None,
                    "constrained_columns": ["q", "p"],
                    "options": {},
                }
            ],
        )
        fks = inspector.get_foreign_keys("k")
        eq_(
            fks,
            [
                {
                    "referred_table": "i",
                    "referred_columns": ["x", "y"],
                    "referred_schema": None,
                    "name": "my_fk",
                    "constrained_columns": ["q", "p"],
                    "options": {},
                }
            ],
        )

    def test_foreign_key_ondelete_onupdate(self):
        inspector = inspect(testing.db)
        fks = inspector.get_foreign_keys("onud_test")
        eq_(
            fks,
            [
                {
                    "referred_table": "a1",
                    "referred_columns": ["id"],
                    "referred_schema": None,
                    "name": "fk1",
                    "constrained_columns": ["c1"],
                    "options": {"ondelete": "SET NULL"},
                },
                {
                    "referred_table": "a1",
                    "referred_columns": ["id"],
                    "referred_schema": None,
                    "name": "fk2",
                    "constrained_columns": ["c2"],
                    "options": {"onupdate": "CASCADE"},
                },
                {
                    "referred_table": "a2",
                    "referred_columns": ["id"],
                    "referred_schema": None,
                    "name": "fk3",
                    "constrained_columns": ["c3"],
                    "options": {"ondelete": "CASCADE", "onupdate": "SET NULL"},
                },
                {
                    "referred_table": "a2",
                    "referred_columns": ["id"],
                    "referred_schema": None,
                    "name": "fk4",
                    "constrained_columns": ["c4"],
                    "options": {},
                },
            ],
        )

    def test_foreign_key_deferrable_initially(self):
        inspector = inspect(testing.db)
        fks = inspector.get_foreign_keys("deferrable_test")
        eq_(
            fks,
            [
                {
                    "referred_table": "a1",
                    "referred_columns": ["id"],
                    "referred_schema": None,
                    "name": "fk1",
                    "constrained_columns": ["c1"],
                    "options": {"deferrable": True},
                },
                {
                    "referred_table": "a1",
                    "referred_columns": ["id"],
                    "referred_schema": None,
                    "name": "fk2",
                    "constrained_columns": ["c2"],
                    "options": {"deferrable": False},
                },
                {
                    "referred_table": "a2",
                    "referred_columns": ["id"],
                    "referred_schema": None,
                    "name": "fk3",
                    "constrained_columns": ["c3"],
                    "options": {
                        "deferrable": True,
                        "initially": "DEFERRED",
                        "onupdate": "CASCADE",
                    },
                },
                {
                    "referred_table": "a2",
                    "referred_columns": ["id"],
                    "referred_schema": None,
                    "name": "fk4",
                    "constrained_columns": ["c4"],
                    "options": {"deferrable": False, "initially": "IMMEDIATE"},
                },
            ],
        )

    def test_foreign_key_options_unnamed_inline(self):
        with testing.db.begin() as conn:
            conn.exec_driver_sql(
                "create table foo (id integer, "
                "foreign key (id) references bar (id) on update cascade)"
            )

            insp = inspect(conn)
            eq_(
                insp.get_foreign_keys("foo"),
                [
                    {
                        "name": None,
                        "referred_columns": ["id"],
                        "referred_table": "bar",
                        "constrained_columns": ["id"],
                        "referred_schema": None,
                        "options": {"onupdate": "CASCADE"},
                    }
                ],
            )

    def test_dont_reflect_autoindex(self):
        inspector = inspect(testing.db)
        eq_(inspector.get_indexes("o"), [])
        eq_(
            inspector.get_indexes("o", include_auto_indexes=True),
            [
                {
                    "unique": 1,
                    "name": "sqlite_autoindex_o_1",
                    "column_names": ["foo"],
                    "dialect_options": {},
                }
            ],
        )

    def test_create_index_with_schema(self):
        """Test creation of index with explicit schema"""

        inspector = inspect(testing.db)
        eq_(
            inspector.get_indexes("l", schema="main"),
            [
                {
                    "unique": 0,
                    "name": "ix_main_l_bar",
                    "column_names": ["bar"],
                    "dialect_options": {},
                }
            ],
        )

    @testing.requires.sqlite_partial_indexes
    def test_reflect_partial_indexes(self, connection):
        connection.exec_driver_sql(
            "create table foo_with_partial_index (x integer, y integer)"
        )
        connection.exec_driver_sql(
            "create unique index ix_partial on "
            "foo_with_partial_index (x) where y > 10"
        )
        connection.exec_driver_sql(
            "create unique index ix_no_partial on "
            "foo_with_partial_index (x)"
        )
        connection.exec_driver_sql(
            "create unique index ix_partial2 on "
            "foo_with_partial_index (x, y) where "
            "y = 10 or abs(x) < 5"
        )

        inspector = inspect(connection)
        indexes = inspector.get_indexes("foo_with_partial_index")
        eq_(
            indexes,
            [
                {
                    "unique": 1,
                    "name": "ix_no_partial",
                    "column_names": ["x"],
                    "dialect_options": {},
                },
                {
                    "unique": 1,
                    "name": "ix_partial",
                    "column_names": ["x"],
                    "dialect_options": {"sqlite_where": mock.ANY},
                },
                {
                    "unique": 1,
                    "name": "ix_partial2",
                    "column_names": ["x", "y"],
                    "dialect_options": {"sqlite_where": mock.ANY},
                },
            ],
        )
        eq_(indexes[1]["dialect_options"]["sqlite_where"].text, "y > 10")
        eq_(
            indexes[2]["dialect_options"]["sqlite_where"].text,
            "y = 10 or abs(x) < 5",
        )

    def test_unique_constraint_named(self):
        inspector = inspect(testing.db)
        eq_(
            inspector.get_unique_constraints("f"),
            [{"column_names": ["x"], "name": "foo_fx"}],
        )

    def test_unique_constraint_named_broken_casing(self):
        inspector = inspect(testing.db)
        eq_(
            inspector.get_unique_constraints("h"),
            [{"column_names": ["x"], "name": "foo_hx"}],
        )

    def test_unique_constraint_named_broken_temp(
        self, connection, temp_table_fixture
    ):
        inspector = inspect(connection)
        eq_(
            inspector.get_unique_constraints("g"),
            [{"column_names": ["x"], "name": "foo_gx"}],
        )

    def test_unique_constraint_unnamed_inline(self):
        inspector = inspect(testing.db)
        eq_(
            inspector.get_unique_constraints("d"),
            [{"column_names": ["x"], "name": None}],
        )

    def test_unique_constraint_unnamed_inline_quoted(self):
        inspector = inspect(testing.db)
        eq_(
            inspector.get_unique_constraints("d1"),
            [{"column_names": ["some ( STUPID n,ame"], "name": None}],
        )
        eq_(
            inspector.get_unique_constraints("d2"),
            [{"column_names": ["some STUPID n,ame"], "name": None}],
        )
        eq_(
            inspector.get_unique_constraints("d3"),
            [{"column_names": ["some STUPID n,ame"], "name": None}],
        )

    def test_unique_constraint_unnamed_normal(self):
        inspector = inspect(testing.db)
        eq_(
            inspector.get_unique_constraints("m"),
            [{"column_names": ["x"], "name": None}],
        )

    def test_unique_constraint_unnamed_normal_temporary(
        self, connection, temp_table_fixture
    ):
        inspector = inspect(connection)
        eq_(
            inspector.get_unique_constraints("n"),
            [{"column_names": ["x"], "name": None}],
        )

    def test_unique_constraint_mixed_into_ck(self, connection):
        """test #11832"""

        inspector = inspect(connection)
        eq_(
            inspector.get_unique_constraints("r"),
            [
                {"name": "prefix_named", "column_names": ["prefix"]},
                {"name": None, "column_names": ["value"]},
            ],
        )

    def test_primary_key_constraint_mixed_into_ck(self, connection):
        """test #11832"""

        inspector = inspect(connection)
        eq_(
            inspector.get_pk_constraint("r"),
            {"constrained_columns": ["id"], "name": "pk_name"},
        )

    def test_primary_key_constraint_named(self):
        inspector = inspect(testing.db)
        eq_(
            inspector.get_pk_constraint("p"),
            {"constrained_columns": ["id"], "name": "pk_name"},
        )

    def test_primary_key_constraint_unnamed(self):
        inspector = inspect(testing.db)
        eq_(
            inspector.get_pk_constraint("q"),
            {"constrained_columns": ["id"], "name": None},
        )

    def test_primary_key_constraint_no_pk(self):
        inspector = inspect(testing.db)
        eq_(
            inspector.get_pk_constraint("d"),
            {"constrained_columns": [], "name": None},
        )

    def test_check_constraint_plain(self):
        inspector = inspect(testing.db)
        eq_(
            inspector.get_check_constraints("cp"),
            [
                {"sqltext": "p = 1 OR (p > 2 AND p < 5)", "name": "cq"},
            ],
        )

    def test_check_constraint_inline_plain(self):
        inspector = inspect(testing.db)
        eq_(
            inspector.get_check_constraints("cp_inline"),
            [
                {"sqltext": "p = 1 OR (p > 2 AND p < 5)", "name": "cq"},
                {"sqltext": "q > 1 AND q < 6", "name": None},
            ],
        )

    @testing.fails("need to come up with new regex and/or DDL parsing")
    def test_check_constraint_multiline(self):
        """test for #11677"""

        inspector = inspect(testing.db)
        eq_(
            inspector.get_check_constraints("r"),
            [
                {"sqltext": "value IS NOT NULL", "name": "^check-r* #\n\t"},
                # Triple-quote multi-line definition should have added a
                # newline and whitespace:
                {
                    "sqltext": "((value > 0) AND \n\t(value < 100) AND \n\t\n"
                    "                      (value != 50))",
                    "name": "ck_r_value_multiline",
                },
                {"sqltext": "id > 0", "name": None},
                {"sqltext": "prefix NOT GLOB '*[^-. /#,]*'", "name": None},
            ],
        )

    @testing.combinations(
        ("plain_name", "plain_name"),
        ("name with spaces", "name with spaces"),
        ("plainname", "plainname"),
        ("[Code]", "[Code]"),
        (quoted_name("[Code]", quote=False), "Code"),
        argnames="colname,expected",
    )
    @testing.combinations(
        "uq",
        "uq_inline",
        "uq_inline_tab_before",  # tab before column params
        "uq_inline_tab_within",  # tab within column params
        "pk",
        "ix",
        argnames="constraint_type",
    )
    def test_constraint_cols(
        self, colname, expected, constraint_type, connection, metadata
    ):
        if constraint_type.startswith("uq_inline"):
            inline_create_sql = {
                "uq_inline": "CREATE TABLE t (%s INTEGER UNIQUE)",
                "uq_inline_tab_before": "CREATE TABLE t (%s\tINTEGER UNIQUE)",
                "uq_inline_tab_within": "CREATE TABLE t (%s INTEGER\tUNIQUE)",
            }

            t = Table("t", metadata, Column(colname, Integer))
            connection.exec_driver_sql(
                inline_create_sql[constraint_type]
                % connection.dialect.identifier_preparer.quote(colname)
            )
        else:
            t = Table("t", metadata, Column(colname, Integer))
            if constraint_type == "uq":
                constraint = UniqueConstraint(t.c[colname])
            elif constraint_type == "pk":
                constraint = PrimaryKeyConstraint(t.c[colname])
            elif constraint_type == "ix":
                constraint = Index("some_index", t.c[colname])
            else:
                assert False

            t.append_constraint(constraint)

            t.create(connection)

        if constraint_type in (
            "uq",
            "uq_inline",
            "uq_inline_tab_before",
            "uq_inline_tab_within",
        ):
            const = inspect(connection).get_unique_constraints("t")[0]
            eq_(const["column_names"], [expected])
        elif constraint_type == "pk":
            const = inspect(connection).get_pk_constraint("t")
            eq_(const["constrained_columns"], [expected])
        elif constraint_type == "ix":
            const = inspect(connection).get_indexes("t")[0]
            eq_(const["column_names"], [expected])
        else:
            assert False


class SavepointTest(fixtures.TablesTest):
    """test that savepoints work when we use the correct event setup"""

    __only_on__ = "sqlite"
    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "users",
            metadata,
            Column("user_id", Integer, primary_key=True),
            Column("user_name", String),
        )

    @classmethod
    def setup_bind(cls):
        engine = engines.testing_engine(options={"scope": "class"})

        @event.listens_for(engine, "connect")
        def do_connect(dbapi_connection, connection_record):
            # disable pysqlite's emitting of the BEGIN statement entirely.
            # also stops it from emitting COMMIT before any DDL.
            dbapi_connection.isolation_level = None

        @event.listens_for(engine, "begin")
        def do_begin(conn):
            # emit our own BEGIN
            conn.exec_driver_sql("BEGIN")

        return engine

    def test_nested_subtransaction_rollback(self):
        users = self.tables.users
        connection = self.bind.connect()
        transaction = connection.begin()
        connection.execute(users.insert(), dict(user_id=1, user_name="user1"))
        trans2 = connection.begin_nested()
        connection.execute(users.insert(), dict(user_id=2, user_name="user2"))
        trans2.rollback()
        connection.execute(users.insert(), dict(user_id=3, user_name="user3"))
        transaction.commit()
        eq_(
            connection.execute(
                select(users.c.user_id).order_by(users.c.user_id)
            ).fetchall(),
            [(1,), (3,)],
        )
        connection.close()

    def test_nested_subtransaction_commit(self):
        users = self.tables.users
        connection = self.bind.connect()
        transaction = connection.begin()
        connection.execute(users.insert(), dict(user_id=1, user_name="user1"))
        trans2 = connection.begin_nested()
        connection.execute(users.insert(), dict(user_id=2, user_name="user2"))
        trans2.commit()
        connection.execute(users.insert(), dict(user_id=3, user_name="user3"))
        transaction.commit()
        eq_(
            connection.execute(
                select(users.c.user_id).order_by(users.c.user_id)
            ).fetchall(),
            [(1,), (2,), (3,)],
        )
        connection.close()


class TypeReflectionTest(fixtures.TestBase):
    __only_on__ = "sqlite"
    __backend__ = True

    def _fixed_lookup_fixture(self):
        return [
            (sqltypes.String(), sqltypes.VARCHAR()),
            (sqltypes.String(1), sqltypes.VARCHAR(1)),
            (sqltypes.String(3), sqltypes.VARCHAR(3)),
            (sqltypes.Text(), sqltypes.TEXT()),
            (sqltypes.Unicode(), sqltypes.VARCHAR()),
            (sqltypes.Unicode(1), sqltypes.VARCHAR(1)),
            (sqltypes.UnicodeText(), sqltypes.TEXT()),
            (sqltypes.CHAR(3), sqltypes.CHAR(3)),
            (sqltypes.NUMERIC, sqltypes.NUMERIC()),
            (sqltypes.NUMERIC(10, 2), sqltypes.NUMERIC(10, 2)),
            (sqltypes.Numeric, sqltypes.NUMERIC()),
            (sqltypes.Numeric(10, 2), sqltypes.NUMERIC(10, 2)),
            (sqltypes.DECIMAL, sqltypes.DECIMAL()),
            (sqltypes.DECIMAL(10, 2), sqltypes.DECIMAL(10, 2)),
            (sqltypes.INTEGER, sqltypes.INTEGER()),
            (sqltypes.BIGINT, sqltypes.BIGINT()),
            (sqltypes.Float, sqltypes.FLOAT()),
            (sqltypes.TIMESTAMP, sqltypes.TIMESTAMP()),
            (sqltypes.DATETIME, sqltypes.DATETIME()),
            (sqltypes.DateTime, sqltypes.DATETIME()),
            (sqltypes.DateTime(), sqltypes.DATETIME()),
            (sqltypes.DATE, sqltypes.DATE()),
            (sqltypes.Date, sqltypes.DATE()),
            (sqltypes.TIME, sqltypes.TIME()),
            (sqltypes.Time, sqltypes.TIME()),
            (sqltypes.BOOLEAN, sqltypes.BOOLEAN()),
            (sqltypes.Boolean, sqltypes.BOOLEAN()),
            (
                sqlite.DATE(storage_format="%(year)04d%(month)02d%(day)02d"),
                sqltypes.DATE(),
            ),
            (
                sqlite.TIME(
                    storage_format="%(hour)02d%(minute)02d%(second)02d"
                ),
                sqltypes.TIME(),
            ),
            (
                sqlite.DATETIME(
                    storage_format="%(year)04d%(month)02d%(day)02d"
                    "%(hour)02d%(minute)02d%(second)02d"
                ),
                sqltypes.DATETIME(),
            ),
        ]

    def _unsupported_args_fixture(self):
        return [
            ("INTEGER(5)", sqltypes.INTEGER()),
            ("DATETIME(6, 12)", sqltypes.DATETIME()),
        ]

    def _type_affinity_fixture(self):
        return [
            ("LONGTEXT", sqltypes.TEXT()),
            ("TINYINT", sqltypes.INTEGER()),
            ("MEDIUMINT", sqltypes.INTEGER()),
            ("INT2", sqltypes.INTEGER()),
            ("UNSIGNED BIG INT", sqltypes.INTEGER()),
            ("INT8", sqltypes.INTEGER()),
            ("CHARACTER(20)", sqltypes.TEXT()),
            ("CLOB", sqltypes.TEXT()),
            ("CLOBBER", sqltypes.TEXT()),
            ("VARYING CHARACTER(70)", sqltypes.TEXT()),
            ("NATIVE CHARACTER(70)", sqltypes.TEXT()),
            ("BLOB", sqltypes.BLOB()),
            ("BLOBBER", sqltypes.NullType()),
            ("DOUBLE PRECISION", sqltypes.REAL()),
            ("FLOATY", sqltypes.REAL()),
            ("SOMETHING UNKNOWN", sqltypes.NUMERIC()),
        ]

    def _fixture_as_string(self, fixture):
        for from_, to_ in fixture:
            if isinstance(from_, sqltypes.TypeEngine):
                from_ = str(from_.compile())
            elif isinstance(from_, type):
                from_ = str(from_().compile())
            yield from_, to_

    def _test_lookup_direct(self, fixture, warnings=False):
        dialect = sqlite.dialect()
        for from_, to_ in self._fixture_as_string(fixture):
            if warnings:

                def go():
                    return dialect._resolve_type_affinity(from_)

                final_type = testing.assert_warnings(
                    go, ["Could not instantiate"], regex=True
                )
            else:
                final_type = dialect._resolve_type_affinity(from_)
            expected_type = type(to_)
            is_(type(final_type), expected_type)

    def _test_round_trip(self, fixture, warnings=False):
        from sqlalchemy import inspect

        for from_, to_ in self._fixture_as_string(fixture):
            with testing.db.begin() as conn:
                inspector = inspect(conn)
                conn.exec_driver_sql("CREATE TABLE foo (data %s)" % from_)
                try:
                    if warnings:

                        def go():
                            return inspector.get_columns("foo")[0]

                        col_info = testing.assert_warnings(
                            go, ["Could not instantiate"], regex=True
                        )
                    else:
                        col_info = inspector.get_columns("foo")[0]
                    expected_type = type(to_)
                    is_(type(col_info["type"]), expected_type)

                    # test args
                    for attr in ("scale", "precision", "length"):
                        if getattr(to_, attr, None) is not None:
                            eq_(
                                getattr(col_info["type"], attr),
                                getattr(to_, attr, None),
                            )
                finally:
                    conn.exec_driver_sql("DROP TABLE foo")

    def test_lookup_direct_lookup(self):
        self._test_lookup_direct(self._fixed_lookup_fixture())

    def test_lookup_direct_unsupported_args(self):
        self._test_lookup_direct(
            self._unsupported_args_fixture(), warnings=True
        )

    def test_lookup_direct_type_affinity(self):
        self._test_lookup_direct(self._type_affinity_fixture())

    def test_round_trip_direct_lookup(self):
        self._test_round_trip(self._fixed_lookup_fixture())

    def test_round_trip_direct_unsupported_args(self):
        self._test_round_trip(self._unsupported_args_fixture(), warnings=True)

    def test_round_trip_direct_type_affinity(self):
        self._test_round_trip(self._type_affinity_fixture())


class RegexpTest(fixtures.TestBase, testing.AssertsCompiledSQL):
    __dialect__ = "sqlite"

    def setup_test(self):
        self.table = table(
            "mytable", column("myid", String), column("name", String)
        )

    @testing.only_on("sqlite >= 3.9")
    def test_determinsitic_parameter(self):
        """for #9379, make sure that "deterministic=True" is used when we are
        on python 3.8 with modern SQLite version.

        For the case where we are not on py3.8 or not on modern sqlite version,
        the rest of the test suite confirms that connection still passes.

        """
        e = create_engine("sqlite://")

        @event.listens_for(e, "do_connect", retval=True)
        def _mock_connect(dialect, conn_rec, cargs, cparams):
            conn = e.dialect.loaded_dbapi.connect(":memory:")
            return mock.Mock(wraps=conn)

        c = e.connect()
        eq_(
            c.connection.driver_connection.create_function.mock_calls,
            [
                mock.call("regexp", 2, mock.ANY, deterministic=True),
                mock.call("floor", 1, mock.ANY, deterministic=True),
            ],
        )

    def test_regexp_match(self):
        self.assert_compile(
            self.table.c.myid.regexp_match("pattern"),
            "mytable.myid REGEXP ?",
            checkpositional=("pattern",),
        )

    def test_regexp_match_column(self):
        self.assert_compile(
            self.table.c.myid.regexp_match(self.table.c.name),
            "mytable.myid REGEXP mytable.name",
            checkparams={},
        )

    def test_regexp_match_str(self):
        self.assert_compile(
            literal("string").regexp_match(self.table.c.name),
            "? REGEXP mytable.name",
            checkpositional=("string",),
        )

    def test_regexp_match_flags(self):
        self.assert_compile(
            self.table.c.myid.regexp_match("pattern", flags="ig"),
            "mytable.myid REGEXP ?",
            checkpositional=("pattern",),
        )

    def test_not_regexp_match(self):
        self.assert_compile(
            ~self.table.c.myid.regexp_match("pattern"),
            "mytable.myid NOT REGEXP ?",
            checkpositional=("pattern",),
        )

    def test_not_regexp_match_flags(self):
        self.assert_compile(
            ~self.table.c.myid.regexp_match("pattern", flags="ig"),
            "mytable.myid NOT REGEXP ?",
            checkpositional=("pattern",),
        )

    def test_not_regexp_match_column(self):
        self.assert_compile(
            ~self.table.c.myid.regexp_match(self.table.c.name),
            "mytable.myid NOT REGEXP mytable.name",
            checkparams={},
        )

    def test_not_regexp_match_str(self):
        self.assert_compile(
            ~literal("string").regexp_match(self.table.c.name),
            "? NOT REGEXP mytable.name",
            checkpositional=("string",),
        )

    def test_regexp_replace(self):
        assert_raises_message(
            exc.CompileError,
            "sqlite dialect does not support regular expression replacements",
            self.table.c.myid.regexp_replace("pattern", "rep").compile,
            dialect=sqlite.dialect(),
        )


class OnConflictCompileTest(
    AssertsCompiledSQL, fixtures.CacheKeySuite, fixtures.TestBase
):
    __dialect__ = "sqlite"

    @testing.combinations(
        (
            lambda users, stmt: stmt.on_conflict_do_nothing(
                index_elements=["id"], index_where=text("name = 'hi'")
            ),
            "ON CONFLICT (id) WHERE name = 'hi' DO NOTHING",
        ),
        (
            lambda users, stmt: stmt.on_conflict_do_nothing(
                index_elements=["id"], index_where="name = 'hi'"
            ),
            exc.ArgumentError,
        ),
        (
            lambda users, stmt: stmt.on_conflict_do_nothing(
                index_elements=[users.c.id], index_where=users.c.name == "hi"
            ),
            "ON CONFLICT (id) WHERE name = __[POSTCOMPILE_name_1] DO NOTHING",
        ),
        (
            lambda users, stmt: stmt.on_conflict_do_update(
                index_elements=[users.c.id],
                set_={users.c.name: "there"},
                where=users.c.name == "hi",
            ),
            "ON CONFLICT (id) DO UPDATE SET name = ? " "WHERE users.name = ?",
        ),
        (
            lambda users, stmt: stmt.on_conflict_do_update(
                index_elements=[users.c.id],
                set_={users.c.name: "there"},
                where=text("name = 'hi'"),
            ),
            "ON CONFLICT (id) DO UPDATE SET name = ? " "WHERE name = 'hi'",
        ),
        (
            lambda users, stmt: stmt.on_conflict_do_update(
                index_elements=[users.c.id],
                set_={users.c.name: "there"},
                where="name = 'hi'",
            ),
            exc.ArgumentError,
        ),
        argnames="case,expected",
    )
    def test_assorted_arg_coercion(self, users, case, expected):
        stmt = insert(users)

        if isinstance(expected, type) and issubclass(expected, Exception):
            with expect_raises(expected):
                testing.resolve_lambda(case, stmt=stmt, users=users),
        else:
            self.assert_compile(
                testing.resolve_lambda(case, stmt=stmt, users=users),
                f"INSERT INTO users (id, name) VALUES (?, ?) {expected}",
            )

    @fixtures.CacheKeySuite.run_suite_tests
    def test_insert_on_conflict_cache_key(self):
        table = Table(
            "foos",
            MetaData(),
            Column("id", Integer, primary_key=True),
            Column("bar", String(10)),
            Column("baz", String(10)),
        )
        Index("foo_idx", table.c.id)

        def stmt0():
            # note a multivalues INSERT is not cacheable; use just one
            # set of values
            return insert(table).values(
                {"id": 1, "bar": "ab"},
            )

        def stmt1():
            stmt = stmt0()
            return stmt.on_conflict_do_nothing()

        def stmt2():
            stmt = stmt0()
            return stmt.on_conflict_do_nothing(index_elements=["id"])

        def stmt21():
            stmt = stmt0()
            return stmt.on_conflict_do_nothing(index_elements=[table.c.id])

        def stmt22():
            stmt = stmt0()
            return stmt.on_conflict_do_nothing(
                index_elements=["id", table.c.bar]
            )

        def stmt23():
            stmt = stmt0()
            return stmt.on_conflict_do_nothing(index_elements=["id", "bar"])

        def stmt24():
            stmt = insert(table).values(
                {"id": 1, "bar": "ab", "baz": "xy"},
            )
            return stmt.on_conflict_do_nothing(index_elements=["id", "bar"])

        def stmt3():
            stmt = stmt0()
            return stmt.on_conflict_do_update(
                index_elements=["id"],
                set_={
                    "bar": random.choice(["a", "b", "c"]),
                    "baz": random.choice(["d", "e", "f"]),
                },
            )

        def stmt31():
            stmt = stmt0()
            return stmt.on_conflict_do_update(
                index_elements=["id"],
                set_={
                    "baz": random.choice(["d", "e", "f"]),
                },
            )

        return lambda: [
            stmt0(),
            stmt1(),
            stmt2(),
            stmt21(),
            stmt22(),
            stmt23(),
            stmt24(),
            stmt3(),
            stmt31(),
        ]

    @testing.combinations("control", "excluded", "dict", argnames="scenario")
    def test_set_excluded(self, scenario, users, users_w_key):
        """test #8014, sending all of .excluded to set"""

        if scenario == "control":

            stmt = insert(users)
            self.assert_compile(
                stmt.on_conflict_do_update(set_=stmt.excluded),
                "INSERT INTO users (id, name) VALUES (?, ?) ON CONFLICT  "
                "DO UPDATE SET id = excluded.id, name = excluded.name",
            )
        else:

            stmt = insert(users_w_key)

            if scenario == "excluded":
                self.assert_compile(
                    stmt.on_conflict_do_update(set_=stmt.excluded),
                    "INSERT INTO users_w_key (id, name) VALUES (?, ?) "
                    "ON CONFLICT  "
                    "DO UPDATE SET id = excluded.id, name = excluded.name",
                )
            else:
                self.assert_compile(
                    stmt.on_conflict_do_update(
                        set_={
                            "id": stmt.excluded.id,
                            "name_keyed": stmt.excluded.name_keyed,
                        }
                    ),
                    "INSERT INTO users_w_key (id, name) VALUES (?, ?) "
                    "ON CONFLICT  "
                    "DO UPDATE SET id = excluded.id, name = excluded.name",
                )

    def test_dont_consume_set_collection(self, users):
        stmt = insert(users).values(
            [
                {
                    "name": "spongebob",
                },
                {
                    "name": "sandy",
                },
            ]
        )
        stmt = stmt.on_conflict_do_update(
            index_elements=[users.c.name], set_=dict(name=stmt.excluded.name)
        )
        self.assert_compile(
            stmt,
            "INSERT INTO users (name) VALUES (?), (?) "
            "ON CONFLICT (name) DO UPDATE SET name = excluded.name",
        )
        stmt = stmt.returning(users)
        self.assert_compile(
            stmt,
            "INSERT INTO users (name) VALUES (?), (?) "
            "ON CONFLICT (name) DO UPDATE SET name = excluded.name "
            "RETURNING id, name",
        )

    def test_on_conflict_do_update_exotic_targets_six(self, users_xtra):
        users = users_xtra

        unique_partial_index = schema.Index(
            "idx_unique_partial_name",
            users_xtra.c.name,
            users_xtra.c.lets_index_this,
            unique=True,
            sqlite_where=users_xtra.c.lets_index_this == "unique_name",
        )

        i = insert(users)
        i = i.on_conflict_do_update(
            index_elements=unique_partial_index.columns,
            index_where=unique_partial_index.dialect_options["sqlite"][
                "where"
            ],
            set_=dict(
                name=i.excluded.name, login_email=i.excluded.login_email
            ),
        )

        # this test illustrates that the index_where clause can't use
        # bound parameters, where we see below a literal_execute parameter is
        # used (will be sent as literal to the DBAPI).  SQLite otherwise
        # fails here with "(sqlite3.OperationalError) ON CONFLICT clause does
        # not match any PRIMARY KEY or UNIQUE constraint" if sent as a real
        # bind parameter.
        self.assert_compile(
            i,
            "INSERT INTO users_xtra (id, name, login_email, lets_index_this) "
            "VALUES (?, ?, ?, ?) ON CONFLICT (name, lets_index_this) "
            "WHERE lets_index_this = __[POSTCOMPILE_lets_index_this_1] "
            "DO UPDATE "
            "SET name = excluded.name, login_email = excluded.login_email",
        )

    @testing.fixture
    def users(self):
        metadata = MetaData()
        return Table(
            "users",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("name", String(50)),
        )

    @testing.fixture
    def users_w_key(self):
        metadata = MetaData()
        return Table(
            "users_w_key",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("name", String(50), key="name_keyed"),
        )

    @testing.fixture
    def users_xtra(self):
        metadata = MetaData()
        return Table(
            "users_xtra",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("name", String(50)),
            Column("login_email", String(50)),
            Column("lets_index_this", String(50)),
        )


class OnConflictTest(fixtures.TablesTest):
    __only_on__ = ("sqlite >= 3.24.0",)
    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "users",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("name", String(50)),
        )

        Table(
            "users_w_key",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("name", String(50), key="name_keyed"),
        )

        class SpecialType(sqltypes.TypeDecorator):
            impl = String
            cache_ok = True

            def process_bind_param(self, value, dialect):
                return value + " processed"

        Table(
            "bind_targets",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("data", SpecialType()),
        )

        users_xtra = Table(
            "users_xtra",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("name", String(50)),
            Column("login_email", String(50)),
            Column("lets_index_this", String(50)),
        )
        cls.unique_partial_index = schema.Index(
            "idx_unique_partial_name",
            users_xtra.c.name,
            users_xtra.c.lets_index_this,
            unique=True,
            sqlite_where=users_xtra.c.lets_index_this == "unique_name",
        )

        cls.unique_constraint = schema.UniqueConstraint(
            users_xtra.c.login_email, name="uq_login_email"
        )
        cls.bogus_index = schema.Index(
            "idx_special_ops",
            users_xtra.c.lets_index_this,
            sqlite_where=users_xtra.c.lets_index_this > "m",
        )

    def test_bad_args(self):
        with expect_raises(ValueError):
            insert(self.tables.users).on_conflict_do_update()

    def test_on_conflict_do_no_call_twice(self):
        users = self.tables.users

        for stmt in (
            insert(users).on_conflict_do_nothing(),
            insert(users).on_conflict_do_update(
                index_elements=[users.c.id], set_=dict(name="foo")
            ),
        ):
            for meth in (
                stmt.on_conflict_do_nothing,
                stmt.on_conflict_do_update,
            ):
                with testing.expect_raises_message(
                    exc.InvalidRequestError,
                    "This Insert construct already has an "
                    "ON CONFLICT clause established",
                ):
                    meth()

    def test_on_conflict_do_nothing(self, connection):
        users = self.tables.users

        conn = connection
        result = conn.execute(
            insert(users).on_conflict_do_nothing(),
            dict(id=1, name="name1"),
        )
        eq_(result.inserted_primary_key, (1,))

        result = conn.execute(
            insert(users).on_conflict_do_nothing(),
            dict(id=1, name="name2"),
        )
        eq_(result.inserted_primary_key, (1,))

        eq_(
            conn.execute(users.select().where(users.c.id == 1)).fetchall(),
            [(1, "name1")],
        )

    def test_on_conflict_do_nothing_connectionless(self, connection):
        users = self.tables.users_xtra

        result = connection.execute(
            insert(users).on_conflict_do_nothing(
                index_elements=["login_email"]
            ),
            dict(name="name1", login_email="email1"),
        )
        eq_(result.inserted_primary_key, (1,))

        result = connection.execute(
            insert(users).on_conflict_do_nothing(
                index_elements=["login_email"]
            ),
            dict(name="name2", login_email="email1"),
        )
        eq_(result.inserted_primary_key, (1,))

        eq_(
            connection.execute(
                users.select().where(users.c.id == 1)
            ).fetchall(),
            [(1, "name1", "email1", None)],
        )

    @testing.provide_metadata
    def test_on_conflict_do_nothing_target(self, connection):
        users = self.tables.users

        conn = connection

        result = conn.execute(
            insert(users).on_conflict_do_nothing(
                index_elements=users.primary_key.columns
            ),
            dict(id=1, name="name1"),
        )
        eq_(result.inserted_primary_key, (1,))

        result = conn.execute(
            insert(users).on_conflict_do_nothing(
                index_elements=users.primary_key.columns
            ),
            dict(id=1, name="name2"),
        )
        eq_(result.inserted_primary_key, (1,))

        eq_(
            conn.execute(users.select().where(users.c.id == 1)).fetchall(),
            [(1, "name1")],
        )

    @testing.combinations(
        ("with_dict", True),
        ("issue_5939", False),
        id_="ia",
        argnames="with_dict",
    )
    def test_on_conflict_do_update_one(self, connection, with_dict):
        users = self.tables.users

        conn = connection
        conn.execute(users.insert(), dict(id=1, name="name1"))

        i = insert(users)
        i = i.on_conflict_do_update(
            index_elements=[users.c.id],
            set_=dict(name=i.excluded.name) if with_dict else i.excluded,
        )
        result = conn.execute(i, dict(id=1, name="name1"))

        eq_(result.inserted_primary_key, (1,))

        eq_(
            conn.execute(users.select().where(users.c.id == 1)).fetchall(),
            [(1, "name1")],
        )

    def test_on_conflict_do_update_two(self, connection):
        users = self.tables.users

        conn = connection
        conn.execute(users.insert(), dict(id=1, name="name1"))

        i = insert(users)
        i = i.on_conflict_do_update(
            index_elements=[users.c.id],
            set_=dict(id=i.excluded.id, name=i.excluded.name),
        )

        result = conn.execute(i, dict(id=1, name="name2"))
        eq_(result.inserted_primary_key, (1,))

        eq_(
            conn.execute(users.select().where(users.c.id == 1)).fetchall(),
            [(1, "name2")],
        )

    def test_on_conflict_do_update_three(self, connection):
        users = self.tables.users

        conn = connection
        conn.execute(users.insert(), dict(id=1, name="name1"))

        i = insert(users)
        i = i.on_conflict_do_update(
            index_elements=users.primary_key.columns,
            set_=dict(name=i.excluded.name),
        )
        result = conn.execute(i, dict(id=1, name="name3"))
        eq_(result.inserted_primary_key, (1,))

        eq_(
            conn.execute(users.select().where(users.c.id == 1)).fetchall(),
            [(1, "name3")],
        )

    def test_on_conflict_do_update_four(self, connection):
        users = self.tables.users

        conn = connection
        conn.execute(users.insert(), dict(id=1, name="name1"))

        i = insert(users)
        i = i.on_conflict_do_update(
            index_elements=users.primary_key.columns,
            set_=dict(id=i.excluded.id, name=i.excluded.name),
        ).values(id=1, name="name4")

        result = conn.execute(i)
        eq_(result.inserted_primary_key, (1,))

        eq_(
            conn.execute(users.select().where(users.c.id == 1)).fetchall(),
            [(1, "name4")],
        )

    def test_on_conflict_do_update_five(self, connection):
        users = self.tables.users

        conn = connection
        conn.execute(users.insert(), dict(id=1, name="name1"))

        i = insert(users)
        i = i.on_conflict_do_update(
            index_elements=users.primary_key.columns,
            set_=dict(id=10, name="I'm a name"),
        ).values(id=1, name="name4")

        result = conn.execute(i)
        eq_(result.inserted_primary_key, (1,))

        eq_(
            conn.execute(users.select().where(users.c.id == 10)).fetchall(),
            [(10, "I'm a name")],
        )

    def test_on_conflict_do_update_column_keys(self, connection):
        users = self.tables.users

        conn = connection
        conn.execute(users.insert(), dict(id=1, name="name1"))

        i = insert(users)
        i = i.on_conflict_do_update(
            index_elements=users.primary_key.columns,
            set_={users.c.id: 10, users.c.name: "I'm a name"},
        ).values(id=1, name="name4")

        result = conn.execute(i)
        eq_(result.inserted_primary_key, (1,))

        eq_(
            conn.execute(users.select().where(users.c.id == 10)).fetchall(),
            [(10, "I'm a name")],
        )

    def test_on_conflict_do_update_clauseelem_keys(self, connection):
        users = self.tables.users

        class MyElem:
            def __init__(self, expr):
                self.expr = expr

            def __clause_element__(self):
                return self.expr

        conn = connection
        conn.execute(users.insert(), dict(id=1, name="name1"))

        i = insert(users)
        i = i.on_conflict_do_update(
            index_elements=users.primary_key.columns,
            set_={MyElem(users.c.id): 10, MyElem(users.c.name): "I'm a name"},
        ).values({MyElem(users.c.id): 1, MyElem(users.c.name): "name4"})

        result = conn.execute(i)
        eq_(result.inserted_primary_key, (1,))

        eq_(
            conn.execute(users.select().where(users.c.id == 10)).fetchall(),
            [(10, "I'm a name")],
        )

    def test_on_conflict_do_update_multivalues(self, connection):
        users = self.tables.users

        conn = connection

        conn.execute(users.insert(), dict(id=1, name="name1"))
        conn.execute(users.insert(), dict(id=2, name="name2"))

        i = insert(users)
        i = i.on_conflict_do_update(
            index_elements=users.primary_key.columns,
            set_=dict(name="updated"),
            where=(i.excluded.name != "name12"),
        ).values(
            [
                dict(id=1, name="name11"),
                dict(id=2, name="name12"),
                dict(id=3, name="name13"),
                dict(id=4, name="name14"),
            ]
        )

        result = conn.execute(i)
        eq_(result.inserted_primary_key, (None,))

        eq_(
            conn.execute(users.select().order_by(users.c.id)).fetchall(),
            [(1, "updated"), (2, "name2"), (3, "name13"), (4, "name14")],
        )

    def _exotic_targets_fixture(self, conn):
        users = self.tables.users_xtra

        conn.execute(
            insert(users),
            dict(
                id=1,
                name="name1",
                login_email="name1@gmail.com",
                lets_index_this="not",
            ),
        )
        conn.execute(
            users.insert(),
            dict(
                id=2,
                name="name2",
                login_email="name2@gmail.com",
                lets_index_this="not",
            ),
        )

        eq_(
            conn.execute(users.select().where(users.c.id == 1)).fetchall(),
            [(1, "name1", "name1@gmail.com", "not")],
        )

    def test_on_conflict_do_update_exotic_targets_two(self, connection):
        users = self.tables.users_xtra

        conn = connection
        self._exotic_targets_fixture(conn)
        # try primary key constraint: cause an upsert on unique id column
        i = insert(users)
        i = i.on_conflict_do_update(
            index_elements=users.primary_key.columns,
            set_=dict(
                name=i.excluded.name, login_email=i.excluded.login_email
            ),
        )
        result = conn.execute(
            i,
            dict(
                id=1,
                name="name2",
                login_email="name1@gmail.com",
                lets_index_this="not",
            ),
        )
        eq_(result.inserted_primary_key, (1,))

        eq_(
            conn.execute(users.select().where(users.c.id == 1)).fetchall(),
            [(1, "name2", "name1@gmail.com", "not")],
        )

    def test_on_conflict_do_update_exotic_targets_three(self, connection):
        users = self.tables.users_xtra

        conn = connection
        self._exotic_targets_fixture(conn)
        # try unique constraint: cause an upsert on target
        # login_email, not id
        i = insert(users)
        i = i.on_conflict_do_update(
            index_elements=["login_email"],
            set_=dict(
                id=i.excluded.id,
                name=i.excluded.name,
                login_email=i.excluded.login_email,
            ),
        )
        # note: lets_index_this value totally ignored in SET clause.
        result = conn.execute(
            i,
            dict(
                id=42,
                name="nameunique",
                login_email="name2@gmail.com",
                lets_index_this="unique",
            ),
        )
        eq_(result.inserted_primary_key, (42,))

        eq_(
            conn.execute(
                users.select().where(users.c.login_email == "name2@gmail.com")
            ).fetchall(),
            [(42, "nameunique", "name2@gmail.com", "not")],
        )

    def test_on_conflict_do_update_exotic_targets_four(self, connection):
        users = self.tables.users_xtra

        conn = connection
        self._exotic_targets_fixture(conn)
        # try unique constraint by name: cause an
        # upsert on target login_email, not id
        i = insert(users)
        i = i.on_conflict_do_update(
            index_elements=["login_email"],
            set_=dict(
                id=i.excluded.id,
                name=i.excluded.name,
                login_email=i.excluded.login_email,
            ),
        )
        # note: lets_index_this value totally ignored in SET clause.

        result = conn.execute(
            i,
            dict(
                id=43,
                name="nameunique2",
                login_email="name2@gmail.com",
                lets_index_this="unique",
            ),
        )
        eq_(result.inserted_primary_key, (43,))

        eq_(
            conn.execute(
                users.select().where(users.c.login_email == "name2@gmail.com")
            ).fetchall(),
            [(43, "nameunique2", "name2@gmail.com", "not")],
        )

    def test_on_conflict_do_update_exotic_targets_four_no_pk(self, connection):
        users = self.tables.users_xtra

        conn = connection
        self._exotic_targets_fixture(conn)
        # try unique constraint by name: cause an
        # upsert on target login_email, not id
        i = insert(users)
        i = i.on_conflict_do_update(
            index_elements=[users.c.login_email],
            set_=dict(
                id=i.excluded.id,
                name=i.excluded.name,
                login_email=i.excluded.login_email,
            ),
        )

        conn.execute(i, dict(name="name3", login_email="name1@gmail.com"))

        eq_(
            conn.execute(users.select().where(users.c.id == 1)).fetchall(),
            [],
        )

        eq_(
            conn.execute(users.select().order_by(users.c.id)).fetchall(),
            [
                (2, "name2", "name2@gmail.com", "not"),
                (3, "name3", "name1@gmail.com", "not"),
            ],
        )

    def test_on_conflict_do_update_exotic_targets_five(self, connection):
        users = self.tables.users_xtra

        conn = connection
        self._exotic_targets_fixture(conn)
        # try bogus index
        i = insert(users)

        i = i.on_conflict_do_update(
            index_elements=self.bogus_index.columns,
            index_where=self.bogus_index.dialect_options["sqlite"]["where"],
            set_=dict(
                name=i.excluded.name, login_email=i.excluded.login_email
            ),
        )

        assert_raises(
            exc.OperationalError,
            conn.execute,
            i,
            dict(
                id=1,
                name="namebogus",
                login_email="bogus@gmail.com",
                lets_index_this="bogus",
            ),
        )

    def test_on_conflict_do_update_exotic_targets_six(self, connection):
        users = self.tables.users_xtra

        conn = connection
        conn.execute(
            insert(users),
            dict(
                id=1,
                name="name1",
                login_email="mail1@gmail.com",
                lets_index_this="unique_name",
            ),
        )
        i = insert(users)
        i = i.on_conflict_do_update(
            index_elements=self.unique_partial_index.columns,
            index_where=self.unique_partial_index.dialect_options["sqlite"][
                "where"
            ],
            set_=dict(
                name=i.excluded.name, login_email=i.excluded.login_email
            ),
        )

        conn.execute(
            i,
            [
                dict(
                    name="name1",
                    login_email="mail2@gmail.com",
                    lets_index_this="unique_name",
                )
            ],
        )

        eq_(
            conn.execute(users.select()).fetchall(),
            [(1, "name1", "mail2@gmail.com", "unique_name")],
        )

    def test_on_conflict_do_update_no_row_actually_affected(self, connection):
        users = self.tables.users_xtra

        conn = connection
        self._exotic_targets_fixture(conn)
        i = insert(users)
        i = i.on_conflict_do_update(
            index_elements=[users.c.login_email],
            set_=dict(name="new_name"),
            where=(i.excluded.name == "other_name"),
        )
        result = conn.execute(
            i, dict(name="name2", login_email="name1@gmail.com")
        )

        # The last inserted primary key should be 2 here
        # it is taking the result from the exotic fixture
        eq_(result.inserted_primary_key, (2,))

        eq_(
            conn.execute(users.select()).fetchall(),
            [
                (1, "name1", "name1@gmail.com", "not"),
                (2, "name2", "name2@gmail.com", "not"),
            ],
        )

    def test_on_conflict_do_update_special_types_in_set(self, connection):
        bind_targets = self.tables.bind_targets

        conn = connection
        i = insert(bind_targets)
        conn.execute(i, {"id": 1, "data": "initial data"})

        eq_(
            conn.scalar(sql.select(bind_targets.c.data)),
            "initial data processed",
        )

        i = insert(bind_targets)
        i = i.on_conflict_do_update(
            index_elements=[bind_targets.c.id],
            set_=dict(data="new updated data"),
        )
        conn.execute(i, {"id": 1, "data": "new inserted data"})

        eq_(
            conn.scalar(sql.select(bind_targets.c.data)),
            "new updated data processed",
        )


class ReflectInternalSchemaTables(fixtures.TablesTest):
    __only_on__ = "sqlite"
    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "sqliteatable",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("other", String(42)),
            sqlite_autoincrement=True,
        )
        view = "CREATE VIEW sqliteview AS SELECT * FROM sqliteatable"
        event.listen(metadata, "after_create", DDL(view))
        event.listen(metadata, "before_drop", DDL("DROP VIEW sqliteview"))

    def test_get_table_names(self, connection):
        insp = inspect(connection)

        res = insp.get_table_names(sqlite_include_internal=True)
        eq_(res, ["sqlite_sequence", "sqliteatable"])
        res = insp.get_table_names()
        eq_(res, ["sqliteatable"])

        meta = MetaData()
        meta.reflect(connection)
        eq_(len(meta.tables), 1)
        eq_(set(meta.tables), {"sqliteatable"})

    def test_get_view_names(self, connection):
        insp = inspect(connection)

        res = insp.get_view_names(sqlite_include_internal=True)
        eq_(res, ["sqliteview"])
        res = insp.get_view_names()
        eq_(res, ["sqliteview"])

    def test_get_temp_table_names(self, connection, metadata):
        Table(
            "sqlitetemptable",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("other", String(42)),
            sqlite_autoincrement=True,
            prefixes=["TEMPORARY"],
        ).create(connection)
        insp = inspect(connection)

        res = insp.get_temp_table_names(sqlite_include_internal=True)
        eq_(res, ["sqlite_sequence", "sqlitetemptable"])
        res = insp.get_temp_table_names()
        eq_(res, ["sqlitetemptable"])

    def test_get_temp_view_names(self, connection):
        view = (
            "CREATE TEMPORARY VIEW sqlitetempview AS "
            "SELECT * FROM sqliteatable"
        )
        connection.exec_driver_sql(view)
        insp = inspect(connection)
        try:
            res = insp.get_temp_view_names(sqlite_include_internal=True)
            eq_(res, ["sqlitetempview"])
            res = insp.get_temp_view_names()
            eq_(res, ["sqlitetempview"])
        finally:
            connection.exec_driver_sql("DROP VIEW sqlitetempview")


class ComputedReflectionTest(fixtures.TestBase):
    __only_on__ = "sqlite"
    __backend__ = True

    @testing.combinations(
        (
            """CREATE TABLE test1 (
                s VARCHAR,
                x VARCHAR GENERATED ALWAYS AS (s || 'x')
            );""",
            "test1",
            {"x": {"text": "s || 'x'", "stored": False}},
        ),
        (
            """CREATE TABLE test2 (
                s VARCHAR,
                x VARCHAR GENERATED ALWAYS AS (s || 'x'),
                y VARCHAR GENERATED ALWAYS AS (s || 'y')
            );""",
            "test2",
            {
                "x": {"text": "s || 'x'", "stored": False},
                "y": {"text": "s || 'y'", "stored": False},
            },
        ),
        (
            """CREATE TABLE test3 (
                s VARCHAR,
                x INTEGER GENERATED ALWAYS AS (INSTR(s, ","))
            );""",
            "test3",
            {"x": {"text": 'INSTR(s, ",")', "stored": False}},
        ),
        (
            """CREATE TABLE test4 (
                s VARCHAR,
                x INTEGER GENERATED ALWAYS AS (INSTR(s, ",")),
                y INTEGER GENERATED ALWAYS AS (INSTR(x, ",")));""",
            "test4",
            {
                "x": {"text": 'INSTR(s, ",")', "stored": False},
                "y": {"text": 'INSTR(x, ",")', "stored": False},
            },
        ),
        (
            """CREATE TABLE test5 (
                s VARCHAR,
                x VARCHAR GENERATED ALWAYS AS (s || 'x') STORED
            );""",
            "test5",
            {"x": {"text": "s || 'x'", "stored": True}},
        ),
        (
            """CREATE TABLE test6 (
                s VARCHAR,
                x VARCHAR GENERATED ALWAYS AS (s || 'x') STORED,
                y VARCHAR GENERATED ALWAYS AS (s || 'y') STORED
            );""",
            "test6",
            {
                "x": {"text": "s || 'x'", "stored": True},
                "y": {"text": "s || 'y'", "stored": True},
            },
        ),
        (
            """CREATE TABLE test7 (
                s VARCHAR,
                x INTEGER GENERATED ALWAYS AS (INSTR(s, ",")) STORED
            );""",
            "test7",
            {"x": {"text": 'INSTR(s, ",")', "stored": True}},
        ),
        (
            """CREATE TABLE test8 (
                s VARCHAR,
                x INTEGER GENERATED ALWAYS AS (INSTR(s, ",")) STORED,
                y INTEGER GENERATED ALWAYS AS (INSTR(x, ",")) STORED
            );""",
            "test8",
            {
                "x": {"text": 'INSTR(s, ",")', "stored": True},
                "y": {"text": 'INSTR(x, ",")', "stored": True},
            },
        ),
        (
            """CREATE TABLE test9 (
                id INTEGER PRIMARY KEY,
                s VARCHAR,
                x VARCHAR GENERATED ALWAYS AS (s || 'x')
            ) WITHOUT ROWID;""",
            "test9",
            {"x": {"text": "s || 'x'", "stored": False}},
        ),
        (
            """CREATE TABLE test_strict1 (
                s TEXT,
                x TEXT GENERATED ALWAYS AS (s || 'x')
            ) STRICT;""",
            "test_strict1",
            {"x": {"text": "s || 'x'", "stored": False}},
            testing.only_on("sqlite>=3.37.0"),
        ),
        (
            """CREATE TABLE test_strict2 (
                id INTEGER PRIMARY KEY,
                s TEXT,
                x TEXT GENERATED ALWAYS AS (s || 'x')
            ) STRICT, WITHOUT ROWID;""",
            "test_strict2",
            {"x": {"text": "s || 'x'", "stored": False}},
            testing.only_on("sqlite>=3.37.0"),
        ),
        (
            """CREATE TABLE test_strict3 (
                id INTEGER PRIMARY KEY,
                s TEXT,
                x TEXT GENERATED ALWAYS AS (s || 'x')
            ) WITHOUT ROWID, STRICT;""",
            "test_strict3",
            {"x": {"text": "s || 'x'", "stored": False}},
            testing.only_on("sqlite>=3.37.0"),
        ),
        argnames="table_ddl,table_name,spec",
        id_="asa",
    )
    @testing.requires.computed_columns
    def test_reflection(
        self, metadata, connection, table_ddl, table_name, spec
    ):
        connection.exec_driver_sql(table_ddl)

        tbl = Table(table_name, metadata, autoload_with=connection)
        seen = set(spec).intersection(tbl.c.keys())

        for col in tbl.c:
            if col.name not in seen:
                is_(col.computed, None)
            else:
                info = spec[col.name]
                msg = f"{tbl.name}-{col.name}"
                is_true(bool(col.computed))
                eq_(col.computed.sqltext.text, info["text"], msg)
                eq_(col.computed.persisted, info["stored"], msg)
