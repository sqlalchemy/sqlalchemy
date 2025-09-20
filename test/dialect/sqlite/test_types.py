"""SQLite-specific tests."""

import datetime
import json

from sqlalchemy import bindparam
from sqlalchemy import Column
from sqlalchemy import exc
from sqlalchemy import func
from sqlalchemy import MetaData
from sqlalchemy import select
from sqlalchemy import Table
from sqlalchemy import testing
from sqlalchemy import text
from sqlalchemy import types as sqltypes
from sqlalchemy.dialects.sqlite import base as sqlite
from sqlalchemy.testing import assert_raises
from sqlalchemy.testing import assert_raises_message
from sqlalchemy.testing import AssertsCompiledSQL
from sqlalchemy.testing import AssertsExecutionResults
from sqlalchemy.testing import engines
from sqlalchemy.testing import eq_
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import is_
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

    def test_deprecated_serializer_args(self, metadata):
        sqlite_json = Table("json_test", metadata, Column("foo", sqlite.JSON))
        data_element = {"foo": "bar"}

        js = mock.Mock(side_effect=json.dumps)
        jd = mock.Mock(side_effect=json.loads)

        with testing.expect_deprecated(
            "The _json_deserializer argument to the SQLite "
            "dialect has been renamed",
            "The _json_serializer argument to the SQLite "
            "dialect has been renamed",
        ):
            engine = engines.testing_engine(
                options=dict(_json_serializer=js, _json_deserializer=jd)
            )
        metadata.create_all(engine)

        with engine.begin() as conn:
            conn.execute(sqlite_json.insert(), {"foo": data_element})

            row = conn.execute(select(sqlite_json.c.foo)).first()

            eq_(row, (data_element,))
            eq_(js.mock_calls, [mock.call(data_element)])
            eq_(jd.mock_calls, [mock.call(json.dumps(data_element))])

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
