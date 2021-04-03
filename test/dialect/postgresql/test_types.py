# coding: utf-8
import datetime
import decimal
import re
import uuid

import sqlalchemy as sa
from sqlalchemy import any_
from sqlalchemy import ARRAY
from sqlalchemy import cast
from sqlalchemy import Column
from sqlalchemy import column
from sqlalchemy import DateTime
from sqlalchemy import Enum
from sqlalchemy import exc
from sqlalchemy import Float
from sqlalchemy import func
from sqlalchemy import inspect
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import null
from sqlalchemy import Numeric
from sqlalchemy import REAL
from sqlalchemy import select
from sqlalchemy import String
from sqlalchemy import Table
from sqlalchemy import testing
from sqlalchemy import Text
from sqlalchemy import text
from sqlalchemy import type_coerce
from sqlalchemy import TypeDecorator
from sqlalchemy import types
from sqlalchemy import Unicode
from sqlalchemy import util
from sqlalchemy.dialects import postgresql
from sqlalchemy.dialects.postgresql import array
from sqlalchemy.dialects.postgresql import DATERANGE
from sqlalchemy.dialects.postgresql import HSTORE
from sqlalchemy.dialects.postgresql import hstore
from sqlalchemy.dialects.postgresql import INT4RANGE
from sqlalchemy.dialects.postgresql import INT8RANGE
from sqlalchemy.dialects.postgresql import JSON
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import NUMRANGE
from sqlalchemy.dialects.postgresql import TSRANGE
from sqlalchemy.dialects.postgresql import TSTZRANGE
from sqlalchemy.exc import CompileError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session
from sqlalchemy.sql import operators
from sqlalchemy.sql import sqltypes
from sqlalchemy.sql.type_api import Variant
from sqlalchemy.testing import fixtures
from sqlalchemy.testing.assertions import assert_raises
from sqlalchemy.testing.assertions import assert_raises_message
from sqlalchemy.testing.assertions import AssertsCompiledSQL
from sqlalchemy.testing.assertions import AssertsExecutionResults
from sqlalchemy.testing.assertions import ComparesTables
from sqlalchemy.testing.assertions import eq_
from sqlalchemy.testing.assertions import is_
from sqlalchemy.testing.assertsql import RegexSQL
from sqlalchemy.testing.schema import pep435_enum
from sqlalchemy.testing.suite import test_types as suite
from sqlalchemy.testing.util import round_decimal


class FloatCoercionTest(fixtures.TablesTest, AssertsExecutionResults):
    __only_on__ = "postgresql"
    __dialect__ = postgresql.dialect()
    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "data_table",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("data", Integer),
        )

    @classmethod
    def insert_data(cls, connection):
        data_table = cls.tables.data_table

        connection.execute(
            data_table.insert().values(
                [
                    {"data": 3},
                    {"data": 5},
                    {"data": 7},
                    {"data": 2},
                    {"data": 15},
                    {"data": 12},
                    {"data": 6},
                    {"data": 478},
                    {"data": 52},
                    {"data": 9},
                ]
            )
        )

    def test_float_coercion(self, connection):
        data_table = self.tables.data_table

        for type_, result in [
            (Numeric, decimal.Decimal("140.381230939")),
            (Float, 140.381230939),
            (Float(asdecimal=True), decimal.Decimal("140.381230939")),
            (Numeric(asdecimal=False), 140.381230939),
        ]:
            ret = connection.execute(
                select(func.stddev_pop(data_table.c.data, type_=type_))
            ).scalar()

            eq_(round_decimal(ret, 9), result)

            ret = connection.execute(
                select(cast(func.stddev_pop(data_table.c.data), type_))
            ).scalar()
            eq_(round_decimal(ret, 9), result)

    def test_arrays_pg(self, connection, metadata):
        t1 = Table(
            "t",
            metadata,
            Column("x", postgresql.ARRAY(Float)),
            Column("y", postgresql.ARRAY(REAL)),
            Column("z", postgresql.ARRAY(postgresql.DOUBLE_PRECISION)),
            Column("q", postgresql.ARRAY(Numeric)),
        )
        metadata.create_all(connection)
        connection.execute(
            t1.insert(), dict(x=[5], y=[5], z=[6], q=[decimal.Decimal("6.4")])
        )
        row = connection.execute(t1.select()).first()
        eq_(row, ([5], [5], [6], [decimal.Decimal("6.4")]))

    def test_arrays_base(self, connection, metadata):
        t1 = Table(
            "t",
            metadata,
            Column("x", sqltypes.ARRAY(Float)),
            Column("y", sqltypes.ARRAY(REAL)),
            Column("z", sqltypes.ARRAY(postgresql.DOUBLE_PRECISION)),
            Column("q", sqltypes.ARRAY(Numeric)),
        )
        metadata.create_all(connection)
        connection.execute(
            t1.insert(), dict(x=[5], y=[5], z=[6], q=[decimal.Decimal("6.4")])
        )
        row = connection.execute(t1.select()).first()
        eq_(row, ([5], [5], [6], [decimal.Decimal("6.4")]))


class EnumTest(fixtures.TestBase, AssertsExecutionResults):
    __backend__ = True

    __only_on__ = "postgresql > 8.3"

    def test_create_table(self, metadata, connection):
        metadata = self.metadata
        t1 = Table(
            "table",
            metadata,
            Column("id", Integer, primary_key=True),
            Column(
                "value", Enum("one", "two", "three", name="onetwothreetype")
            ),
        )
        t1.create(connection)
        t1.create(connection, checkfirst=True)  # check the create
        connection.execute(t1.insert(), dict(value="two"))
        connection.execute(t1.insert(), dict(value="three"))
        connection.execute(t1.insert(), dict(value="three"))
        eq_(
            connection.execute(t1.select().order_by(t1.c.id)).fetchall(),
            [(1, "two"), (2, "three"), (3, "three")],
        )

    @testing.combinations(None, "foo", argnames="symbol_name")
    def test_create_table_schema_translate_map(self, connection, symbol_name):
        # note we can't use the fixture here because it will not drop
        # from the correct schema
        metadata = MetaData()

        t1 = Table(
            "table",
            metadata,
            Column("id", Integer, primary_key=True),
            Column(
                "value",
                Enum(
                    "one",
                    "two",
                    "three",
                    name="schema_enum",
                    schema=symbol_name,
                ),
            ),
            schema=symbol_name,
        )
        conn = connection.execution_options(
            schema_translate_map={symbol_name: testing.config.test_schema}
        )
        t1.create(conn)
        assert "schema_enum" in [
            e["name"]
            for e in inspect(conn).get_enums(schema=testing.config.test_schema)
        ]
        t1.create(conn, checkfirst=True)

        conn.execute(t1.insert(), dict(value="two"))
        conn.execute(t1.insert(), dict(value="three"))
        conn.execute(t1.insert(), dict(value="three"))
        eq_(
            conn.execute(t1.select().order_by(t1.c.id)).fetchall(),
            [(1, "two"), (2, "three"), (3, "three")],
        )

        t1.drop(conn)
        assert "schema_enum" not in [
            e["name"]
            for e in inspect(conn).get_enums(schema=testing.config.test_schema)
        ]
        t1.drop(conn, checkfirst=True)

    def test_name_required(self, metadata, connection):
        etype = Enum("four", "five", "six", metadata=metadata)
        assert_raises(exc.CompileError, etype.create, connection)
        assert_raises(
            exc.CompileError, etype.compile, dialect=connection.dialect
        )

    def test_unicode_labels(self, connection, metadata):
        t1 = Table(
            "table",
            metadata,
            Column("id", Integer, primary_key=True),
            Column(
                "value",
                Enum(
                    util.u("réveillé"),
                    util.u("drôle"),
                    util.u("S’il"),
                    name="onetwothreetype",
                ),
            ),
        )
        metadata.create_all(connection)
        connection.execute(t1.insert(), dict(value=util.u("drôle")))
        connection.execute(t1.insert(), dict(value=util.u("réveillé")))
        connection.execute(t1.insert(), dict(value=util.u("S’il")))
        eq_(
            connection.execute(t1.select().order_by(t1.c.id)).fetchall(),
            [
                (1, util.u("drôle")),
                (2, util.u("réveillé")),
                (3, util.u("S’il")),
            ],
        )
        m2 = MetaData()
        t2 = Table("table", m2, autoload_with=connection)
        eq_(
            t2.c.value.type.enums,
            [util.u("réveillé"), util.u("drôle"), util.u("S’il")],
        )

    def test_non_native_enum(self, metadata, connection):
        metadata = self.metadata
        t1 = Table(
            "foo",
            metadata,
            Column(
                "bar",
                Enum(
                    "one",
                    "two",
                    "three",
                    name="myenum",
                    create_constraint=True,
                    native_enum=False,
                ),
            ),
        )

        def go():
            t1.create(connection)

        self.assert_sql(
            connection,
            go,
            [
                (
                    "CREATE TABLE foo (\tbar "
                    "VARCHAR(5), \tCONSTRAINT myenum CHECK "
                    "(bar IN ('one', 'two', 'three')))",
                    {},
                )
            ],
        )
        connection.execute(t1.insert(), {"bar": "two"})
        eq_(connection.scalar(select(t1.c.bar)), "two")

    def test_non_native_enum_w_unicode(self, metadata, connection):
        metadata = self.metadata
        t1 = Table(
            "foo",
            metadata,
            Column(
                "bar",
                Enum(
                    "B",
                    util.u("Ü"),
                    name="myenum",
                    create_constraint=True,
                    native_enum=False,
                ),
            ),
        )

        def go():
            t1.create(connection)

        self.assert_sql(
            connection,
            go,
            [
                (
                    util.u(
                        "CREATE TABLE foo (\tbar "
                        "VARCHAR(1), \tCONSTRAINT myenum CHECK "
                        "(bar IN ('B', 'Ü')))"
                    ),
                    {},
                )
            ],
        )

        connection.execute(t1.insert(), {"bar": util.u("Ü")})
        eq_(connection.scalar(select(t1.c.bar)), util.u("Ü"))

    def test_disable_create(self, metadata, connection):
        metadata = self.metadata

        e1 = postgresql.ENUM(
            "one", "two", "three", name="myenum", create_type=False
        )

        t1 = Table("e1", metadata, Column("c1", e1))
        # table can be created separately
        # without conflict
        e1.create(bind=connection)
        t1.create(connection)
        t1.drop(connection)
        e1.drop(bind=connection)

    def test_dont_keep_checking(self, metadata, connection):
        metadata = self.metadata

        e1 = postgresql.ENUM("one", "two", "three", name="myenum")

        Table("t", metadata, Column("a", e1), Column("b", e1), Column("c", e1))

        with self.sql_execution_asserter(connection) as asserter:
            metadata.create_all(connection)

        asserter.assert_(
            # check for table
            RegexSQL(
                "select relname from pg_class c join pg_namespace.*",
                dialect="postgresql",
            ),
            # check for enum, just once
            RegexSQL(r".*SELECT EXISTS ", dialect="postgresql"),
            RegexSQL("CREATE TYPE myenum AS ENUM .*", dialect="postgresql"),
            RegexSQL(r"CREATE TABLE t .*", dialect="postgresql"),
        )

        with self.sql_execution_asserter(connection) as asserter:
            metadata.drop_all(connection)

        asserter.assert_(
            RegexSQL(
                "select relname from pg_class c join pg_namespace.*",
                dialect="postgresql",
            ),
            RegexSQL("DROP TABLE t", dialect="postgresql"),
            RegexSQL(r".*SELECT EXISTS ", dialect="postgresql"),
            RegexSQL("DROP TYPE myenum", dialect="postgresql"),
        )

    def test_generate_multiple(self, metadata, connection):
        """Test that the same enum twice only generates once
        for the create_all() call, without using checkfirst.

        A 'memo' collection held by the DDL runner
        now handles this.

        """
        e1 = Enum("one", "two", "three", name="myenum")
        Table("e1", metadata, Column("c1", e1))

        Table("e2", metadata, Column("c1", e1))

        metadata.create_all(connection, checkfirst=False)
        metadata.drop_all(connection, checkfirst=False)
        assert "myenum" not in [
            e["name"] for e in inspect(connection).get_enums()
        ]

    def test_generate_alone_on_metadata(self, connection, metadata):
        """Test that the same enum twice only generates once
        for the create_all() call, without using checkfirst.

        A 'memo' collection held by the DDL runner
        now handles this.

        """

        Enum("one", "two", "three", name="myenum", metadata=metadata)

        metadata.create_all(connection, checkfirst=False)
        assert "myenum" in [e["name"] for e in inspect(connection).get_enums()]
        metadata.drop_all(connection, checkfirst=False)
        assert "myenum" not in [
            e["name"] for e in inspect(connection).get_enums()
        ]

    def test_generate_multiple_on_metadata(self, connection, metadata):

        e1 = Enum("one", "two", "three", name="myenum", metadata=metadata)

        t1 = Table("e1", metadata, Column("c1", e1))

        t2 = Table("e2", metadata, Column("c1", e1))

        metadata.create_all(connection, checkfirst=False)
        assert "myenum" in [e["name"] for e in inspect(connection).get_enums()]
        metadata.drop_all(connection, checkfirst=False)
        assert "myenum" not in [
            e["name"] for e in inspect(connection).get_enums()
        ]

        e1.create(connection)  # creates ENUM
        t1.create(connection)  # does not create ENUM
        t2.create(connection)  # does not create ENUM

    def test_generate_multiple_schemaname_on_metadata(
        self, metadata, connection
    ):

        Enum("one", "two", "three", name="myenum", metadata=metadata)
        Enum(
            "one",
            "two",
            "three",
            name="myenum",
            metadata=metadata,
            schema="test_schema",
        )

        metadata.create_all(connection, checkfirst=False)
        assert "myenum" in [e["name"] for e in inspect(connection).get_enums()]
        assert "myenum" in [
            e["name"]
            for e in inspect(connection).get_enums(schema="test_schema")
        ]
        metadata.drop_all(connection, checkfirst=False)
        assert "myenum" not in [
            e["name"] for e in inspect(connection).get_enums()
        ]
        assert "myenum" not in [
            e["name"]
            for e in inspect(connection).get_enums(schema="test_schema")
        ]

    def test_drops_on_table(self, connection, metadata):

        e1 = Enum("one", "two", "three", name="myenum")
        table = Table("e1", metadata, Column("c1", e1))

        table.create(connection)
        table.drop(connection)
        assert "myenum" not in [
            e["name"] for e in inspect(connection).get_enums()
        ]
        table.create(connection)
        assert "myenum" in [e["name"] for e in inspect(connection).get_enums()]
        table.drop(connection)
        assert "myenum" not in [
            e["name"] for e in inspect(connection).get_enums()
        ]

    def test_create_drop_schema_translate_map(self, connection):

        conn = connection.execution_options(
            schema_translate_map={None: testing.config.test_schema}
        )

        e1 = Enum("one", "two", "three", name="myenum")

        assert "myenum" not in [
            e["name"]
            for e in inspect(connection).get_enums(testing.config.test_schema)
        ]

        e1.create(conn, checkfirst=True)
        e1.create(conn, checkfirst=True)

        assert "myenum" in [
            e["name"]
            for e in inspect(connection).get_enums(testing.config.test_schema)
        ]

        s1 = conn.begin_nested()
        assert_raises(exc.ProgrammingError, e1.create, conn, checkfirst=False)
        s1.rollback()

        e1.drop(conn, checkfirst=True)
        e1.drop(conn, checkfirst=True)

        assert "myenum" not in [
            e["name"]
            for e in inspect(connection).get_enums(testing.config.test_schema)
        ]

        assert_raises(exc.ProgrammingError, e1.drop, conn, checkfirst=False)

    def test_remain_on_table_metadata_wide(self, metadata, future_connection):
        connection = future_connection

        e1 = Enum("one", "two", "three", name="myenum", metadata=metadata)
        table = Table("e1", metadata, Column("c1", e1))

        # need checkfirst here, otherwise enum will not be created
        assert_raises_message(
            sa.exc.ProgrammingError,
            '.*type "myenum" does not exist',
            table.create,
            connection,
        )
        connection.rollback()

        table.create(connection, checkfirst=True)
        table.drop(connection)
        table.create(connection, checkfirst=True)
        table.drop(connection)
        assert "myenum" in [e["name"] for e in inspect(connection).get_enums()]
        metadata.drop_all(connection)
        assert "myenum" not in [
            e["name"] for e in inspect(connection).get_enums()
        ]

    def test_non_native_dialect(self, metadata, testing_engine):
        engine = testing_engine()
        engine.connect()
        engine.dialect.supports_native_enum = False
        t1 = Table(
            "foo",
            metadata,
            Column(
                "bar",
                Enum(
                    "one",
                    "two",
                    "three",
                    name="myenum",
                    create_constraint=True,
                ),
            ),
        )

        def go():
            t1.create(engine)

        self.assert_sql(
            engine,
            go,
            [
                (
                    "CREATE TABLE foo (bar "
                    "VARCHAR(5), CONSTRAINT myenum CHECK "
                    "(bar IN ('one', 'two', 'three')))",
                    {},
                )
            ],
        )

    def test_standalone_enum(self, connection, metadata):
        etype = Enum(
            "four", "five", "six", name="fourfivesixtype", metadata=metadata
        )
        etype.create(connection)
        try:
            assert connection.dialect.has_type(connection, "fourfivesixtype")
        finally:
            etype.drop(connection)
            assert not connection.dialect.has_type(
                connection, "fourfivesixtype"
            )
        metadata.create_all(connection)
        try:
            assert connection.dialect.has_type(connection, "fourfivesixtype")
        finally:
            metadata.drop_all(connection)
            assert not connection.dialect.has_type(
                connection, "fourfivesixtype"
            )

    def test_no_support(self, testing_engine):
        def server_version_info(self):
            return (8, 2)

        e = testing_engine()
        dialect = e.dialect
        dialect._get_server_version_info = server_version_info

        assert dialect.supports_native_enum
        e.connect()
        assert not dialect.supports_native_enum

        # initialize is called again on new pool
        e.dispose()
        e.connect()
        assert not dialect.supports_native_enum

    def test_reflection(self, metadata, connection):
        etype = Enum(
            "four", "five", "six", name="fourfivesixtype", metadata=metadata
        )
        Table(
            "table",
            metadata,
            Column("id", Integer, primary_key=True),
            Column(
                "value", Enum("one", "two", "three", name="onetwothreetype")
            ),
            Column("value2", etype),
        )
        metadata.create_all(connection)
        m2 = MetaData()
        t2 = Table("table", m2, autoload_with=connection)
        eq_(t2.c.value.type.enums, ["one", "two", "three"])
        eq_(t2.c.value.type.name, "onetwothreetype")
        eq_(t2.c.value2.type.enums, ["four", "five", "six"])
        eq_(t2.c.value2.type.name, "fourfivesixtype")

    def test_schema_reflection(self, metadata, connection):
        etype = Enum(
            "four",
            "five",
            "six",
            name="fourfivesixtype",
            schema="test_schema",
            metadata=metadata,
        )
        Table(
            "table",
            metadata,
            Column("id", Integer, primary_key=True),
            Column(
                "value",
                Enum(
                    "one",
                    "two",
                    "three",
                    name="onetwothreetype",
                    schema="test_schema",
                ),
            ),
            Column("value2", etype),
        )
        metadata.create_all(connection)
        m2 = MetaData()
        t2 = Table("table", m2, autoload_with=connection)
        eq_(t2.c.value.type.enums, ["one", "two", "three"])
        eq_(t2.c.value.type.name, "onetwothreetype")
        eq_(t2.c.value2.type.enums, ["four", "five", "six"])
        eq_(t2.c.value2.type.name, "fourfivesixtype")
        eq_(t2.c.value2.type.schema, "test_schema")

    def test_custom_subclass(self, metadata, connection):
        class MyEnum(TypeDecorator):
            impl = Enum("oneHI", "twoHI", "threeHI", name="myenum")

            def process_bind_param(self, value, dialect):
                if value is not None:
                    value += "HI"
                return value

            def process_result_value(self, value, dialect):
                if value is not None:
                    value += "THERE"
                return value

        t1 = Table("table1", self.metadata, Column("data", MyEnum()))
        self.metadata.create_all(connection)

        connection.execute(t1.insert(), {"data": "two"})
        eq_(connection.scalar(select(t1.c.data)), "twoHITHERE")

    def test_generic_w_pg_variant(self, metadata, connection):
        some_table = Table(
            "some_table",
            self.metadata,
            Column(
                "data",
                Enum(
                    "one",
                    "two",
                    "three",
                    native_enum=True  # make sure this is True because
                    # it should *not* take effect due to
                    # the variant
                ).with_variant(
                    postgresql.ENUM("four", "five", "six", name="my_enum"),
                    "postgresql",
                ),
            ),
        )

        assert "my_enum" not in [
            e["name"] for e in inspect(connection).get_enums()
        ]

        self.metadata.create_all(connection)

        assert "my_enum" in [
            e["name"] for e in inspect(connection).get_enums()
        ]

        connection.execute(some_table.insert(), {"data": "five"})

        self.metadata.drop_all(connection)

        assert "my_enum" not in [
            e["name"] for e in inspect(connection).get_enums()
        ]

    def test_generic_w_some_other_variant(self, metadata, connection):
        some_table = Table(
            "some_table",
            self.metadata,
            Column(
                "data",
                Enum(
                    "one", "two", "three", name="my_enum", native_enum=True
                ).with_variant(Enum("four", "five", "six"), "mysql"),
            ),
        )

        assert "my_enum" not in [
            e["name"] for e in inspect(connection).get_enums()
        ]

        self.metadata.create_all(connection)

        assert "my_enum" in [
            e["name"] for e in inspect(connection).get_enums()
        ]

        connection.execute(some_table.insert(), {"data": "two"})

        self.metadata.drop_all(connection)

        assert "my_enum" not in [
            e["name"] for e in inspect(connection).get_enums()
        ]


class OIDTest(fixtures.TestBase):
    __only_on__ = "postgresql"
    __backend__ = True

    def test_reflection(self, connection, metadata):
        Table(
            "table",
            metadata,
            Column("x", Integer),
            Column("y", postgresql.OID),
        )
        metadata.create_all(connection)
        m2 = MetaData()
        t2 = Table(
            "table",
            m2,
            autoload_with=connection,
        )
        assert isinstance(t2.c.y.type, postgresql.OID)


class RegClassTest(fixtures.TestBase):
    __only_on__ = "postgresql"
    __backend__ = True

    @testing.fixture()
    def scalar(self, connection):
        def go(expression):
            return connection.scalar(select(expression))

        return go

    def test_cast_name(self, scalar):
        eq_(scalar(cast("pg_class", postgresql.REGCLASS)), "pg_class")

    def test_cast_path(self, scalar):
        eq_(
            scalar(cast("pg_catalog.pg_class", postgresql.REGCLASS)),
            "pg_class",
        )

    def test_cast_oid(self, scalar):
        regclass = cast("pg_class", postgresql.REGCLASS)
        oid = scalar(cast(regclass, postgresql.OID))
        assert isinstance(oid, int)
        eq_(
            scalar(
                cast(type_coerce(oid, postgresql.OID), postgresql.REGCLASS)
            ),
            "pg_class",
        )

    def test_cast_whereclause(self, connection):
        pga = Table(
            "pg_attribute",
            MetaData(),
            Column("attrelid", postgresql.OID),
            Column("attname", String(64)),
        )
        oid = connection.scalar(
            select(pga.c.attrelid).where(
                pga.c.attrelid == cast("pg_class", postgresql.REGCLASS)
            )
        )
        assert isinstance(oid, int)


class NumericInterpretationTest(fixtures.TestBase):
    __only_on__ = "postgresql"
    __backend__ = True

    def test_numeric_codes(self):
        from sqlalchemy.dialects.postgresql import (
            pg8000,
            pygresql,
            psycopg2,
            psycopg2cffi,
            base,
        )

        dialects = (
            pg8000.dialect(),
            pygresql.dialect(),
            psycopg2.dialect(),
            psycopg2cffi.dialect(),
        )
        for dialect in dialects:
            typ = Numeric().dialect_impl(dialect)
            for code in (
                base._INT_TYPES + base._FLOAT_TYPES + base._DECIMAL_TYPES
            ):
                proc = typ.result_processor(dialect, code)
                val = 23.7
                if proc is not None:
                    val = proc(val)
                assert val in (23.7, decimal.Decimal("23.7"))

    def test_numeric_default(self, connection, metadata):
        # pg8000 appears to fail when the value is 0,
        # returns an int instead of decimal.
        t = Table(
            "t",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("nd", Numeric(asdecimal=True), default=1),
            Column("nf", Numeric(asdecimal=False), default=1),
            Column("fd", Float(asdecimal=True), default=1),
            Column("ff", Float(asdecimal=False), default=1),
        )
        metadata.create_all(connection)
        connection.execute(t.insert())

        row = connection.execute(t.select()).first()
        assert isinstance(row[1], decimal.Decimal)
        assert isinstance(row[2], float)
        assert isinstance(row[3], decimal.Decimal)
        assert isinstance(row[4], float)
        eq_(row, (1, decimal.Decimal("1"), 1, decimal.Decimal("1"), 1))


class PythonTypeTest(fixtures.TestBase):
    def test_interval(self):
        is_(postgresql.INTERVAL().python_type, datetime.timedelta)


class TimezoneTest(fixtures.TablesTest):
    __backend__ = True

    """Test timezone-aware datetimes.

    psycopg will return a datetime with a tzinfo attached to it, if
    postgresql returns it.  python then will not let you compare a
    datetime with a tzinfo to a datetime that doesn't have one.  this
    test illustrates two ways to have datetime types with and without
    timezone info. """

    __only_on__ = "postgresql"

    @classmethod
    def define_tables(cls, metadata):
        # current_timestamp() in postgresql is assumed to return
        # TIMESTAMP WITH TIMEZONE

        Table(
            "tztable",
            metadata,
            Column("id", Integer, primary_key=True),
            Column(
                "date",
                DateTime(timezone=True),
                onupdate=func.current_timestamp(),
            ),
            Column("name", String(20)),
        )
        Table(
            "notztable",
            metadata,
            Column("id", Integer, primary_key=True),
            Column(
                "date",
                DateTime(timezone=False),
                onupdate=cast(
                    func.current_timestamp(), DateTime(timezone=False)
                ),
            ),
            Column("name", String(20)),
        )

    def test_with_timezone(self, connection):
        tztable, notztable = self.tables("tztable", "notztable")
        # get a date with a tzinfo

        somedate = connection.scalar(func.current_timestamp().select())
        assert somedate.tzinfo
        connection.execute(
            tztable.insert(), dict(id=1, name="row1", date=somedate)
        )
        row = connection.execute(
            select(tztable.c.date).where(tztable.c.id == 1)
        ).first()
        eq_(row[0], somedate)
        eq_(
            somedate.tzinfo.utcoffset(somedate),
            row[0].tzinfo.utcoffset(row[0]),
        )
        result = connection.execute(
            tztable.update(tztable.c.id == 1).returning(tztable.c.date),
            dict(
                name="newname",
            ),
        )
        row = result.first()
        assert row[0] >= somedate

    def test_without_timezone(self, connection):

        # get a date without a tzinfo
        tztable, notztable = self.tables("tztable", "notztable")

        somedate = datetime.datetime(2005, 10, 20, 11, 52, 0)
        assert not somedate.tzinfo
        connection.execute(
            notztable.insert(), dict(id=1, name="row1", date=somedate)
        )
        row = connection.execute(
            select(notztable.c.date).where(notztable.c.id == 1)
        ).first()
        eq_(row[0], somedate)
        eq_(row[0].tzinfo, None)
        result = connection.execute(
            notztable.update(notztable.c.id == 1).returning(notztable.c.date),
            dict(
                name="newname",
            ),
        )
        row = result.first()
        assert row[0] >= somedate


class TimePrecisionCompileTest(fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = postgresql.dialect()

    @testing.combinations(
        (postgresql.TIME(), "TIME WITHOUT TIME ZONE"),
        (postgresql.TIME(precision=5), "TIME(5) WITHOUT TIME ZONE"),
        (
            postgresql.TIME(timezone=True, precision=5),
            "TIME(5) WITH TIME ZONE",
        ),
        (postgresql.TIMESTAMP(), "TIMESTAMP WITHOUT TIME ZONE"),
        (postgresql.TIMESTAMP(precision=5), "TIMESTAMP(5) WITHOUT TIME ZONE"),
        (
            postgresql.TIMESTAMP(timezone=True, precision=5),
            "TIMESTAMP(5) WITH TIME ZONE",
        ),
        (postgresql.TIME(precision=0), "TIME(0) WITHOUT TIME ZONE"),
        (postgresql.TIMESTAMP(precision=0), "TIMESTAMP(0) WITHOUT TIME ZONE"),
    )
    def test_compile(self, type_, expected):
        self.assert_compile(type_, expected)


class TimePrecisionTest(fixtures.TestBase):

    __only_on__ = "postgresql"
    __backend__ = True

    def test_reflection(self, metadata, connection):
        t1 = Table(
            "t1",
            metadata,
            Column("c1", postgresql.TIME()),
            Column("c2", postgresql.TIME(precision=5)),
            Column("c3", postgresql.TIME(timezone=True, precision=5)),
            Column("c4", postgresql.TIMESTAMP()),
            Column("c5", postgresql.TIMESTAMP(precision=5)),
            Column("c6", postgresql.TIMESTAMP(timezone=True, precision=5)),
        )
        t1.create(connection)
        m2 = MetaData()
        t2 = Table("t1", m2, autoload_with=connection)
        eq_(t2.c.c1.type.precision, None)
        eq_(t2.c.c2.type.precision, 5)
        eq_(t2.c.c3.type.precision, 5)
        eq_(t2.c.c4.type.precision, None)
        eq_(t2.c.c5.type.precision, 5)
        eq_(t2.c.c6.type.precision, 5)
        eq_(t2.c.c1.type.timezone, False)
        eq_(t2.c.c2.type.timezone, False)
        eq_(t2.c.c3.type.timezone, True)
        eq_(t2.c.c4.type.timezone, False)
        eq_(t2.c.c5.type.timezone, False)
        eq_(t2.c.c6.type.timezone, True)


class ArrayTest(AssertsCompiledSQL, fixtures.TestBase):
    __dialect__ = "postgresql"

    def test_array_literal(self):
        obj = postgresql.array([1, 2]) + postgresql.array([3, 4, 5])

        self.assert_compile(
            obj,
            "ARRAY[%(param_1)s, %(param_2)s] || "
            "ARRAY[%(param_3)s, %(param_4)s, %(param_5)s]",
            params={
                "param_1": 1,
                "param_2": 2,
                "param_3": 3,
                "param_4": 4,
                "param_5": 5,
            },
        )
        self.assert_compile(
            obj[1],
            "(ARRAY[%(param_1)s, %(param_2)s] || ARRAY[%(param_3)s, "
            "%(param_4)s, %(param_5)s])[%(param_6)s]",
            params={
                "param_1": 1,
                "param_2": 2,
                "param_3": 3,
                "param_4": 4,
                "param_5": 5,
            },
        )

    def test_array_literal_getitem_multidim(self):
        obj = postgresql.array(
            [postgresql.array([1, 2]), postgresql.array([3, 4])]
        )

        self.assert_compile(
            obj,
            "ARRAY[ARRAY[%(param_1)s, %(param_2)s], "
            "ARRAY[%(param_3)s, %(param_4)s]]",
        )
        self.assert_compile(
            obj[1],
            "(ARRAY[ARRAY[%(param_1)s, %(param_2)s], "
            "ARRAY[%(param_3)s, %(param_4)s]])[%(param_5)s]",
        )
        self.assert_compile(
            obj[1][0],
            "(ARRAY[ARRAY[%(param_1)s, %(param_2)s], "
            "ARRAY[%(param_3)s, %(param_4)s]])[%(param_5)s][%(param_6)s]",
        )

    def test_array_type_render_str(self):
        self.assert_compile(postgresql.ARRAY(Unicode(30)), "VARCHAR(30)[]")

    def test_array_type_render_str_collate(self):
        self.assert_compile(
            postgresql.ARRAY(Unicode(30, collation="en_US")),
            'VARCHAR(30)[] COLLATE "en_US"',
        )

    def test_array_type_render_str_multidim(self):
        self.assert_compile(
            postgresql.ARRAY(Unicode(30), dimensions=2), "VARCHAR(30)[][]"
        )

        self.assert_compile(
            postgresql.ARRAY(Unicode(30), dimensions=3), "VARCHAR(30)[][][]"
        )

    def test_array_type_render_str_collate_multidim(self):
        self.assert_compile(
            postgresql.ARRAY(Unicode(30, collation="en_US"), dimensions=2),
            'VARCHAR(30)[][] COLLATE "en_US"',
        )

        self.assert_compile(
            postgresql.ARRAY(Unicode(30, collation="en_US"), dimensions=3),
            'VARCHAR(30)[][][] COLLATE "en_US"',
        )

    def test_array_int_index(self):
        col = column("x", postgresql.ARRAY(Integer))
        self.assert_compile(
            select(col[3]),
            "SELECT x[%(x_1)s] AS anon_1",
            checkparams={"x_1": 3},
        )

    def test_array_any(self):
        col = column("x", postgresql.ARRAY(Integer))
        self.assert_compile(
            select(col.any(7, operator=operators.lt)),
            "SELECT %(param_1)s < ANY (x) AS anon_1",
            checkparams={"param_1": 7},
        )

    def test_array_all(self):
        col = column("x", postgresql.ARRAY(Integer))
        self.assert_compile(
            select(col.all(7, operator=operators.lt)),
            "SELECT %(param_1)s < ALL (x) AS anon_1",
            checkparams={"param_1": 7},
        )

    def test_array_contains(self):
        col = column("x", postgresql.ARRAY(Integer))
        self.assert_compile(
            select(col.contains(array([4, 5, 6]))),
            "SELECT x @> ARRAY[%(param_1)s, %(param_2)s, %(param_3)s] "
            "AS anon_1",
            checkparams={"param_1": 4, "param_3": 6, "param_2": 5},
        )

    def test_contains_override_raises(self):
        col = column("x", postgresql.ARRAY(Integer))

        assert_raises_message(
            NotImplementedError,
            "Operator 'contains' is not supported on this expression",
            lambda: "foo" in col,
        )

    def test_array_contained_by(self):
        col = column("x", postgresql.ARRAY(Integer))
        self.assert_compile(
            select(col.contained_by(array([4, 5, 6]))),
            "SELECT x <@ ARRAY[%(param_1)s, %(param_2)s, %(param_3)s] "
            "AS anon_1",
            checkparams={"param_1": 4, "param_3": 6, "param_2": 5},
        )

    def test_array_overlap(self):
        col = column("x", postgresql.ARRAY(Integer))
        self.assert_compile(
            select(col.overlap(array([4, 5, 6]))),
            "SELECT x && ARRAY[%(param_1)s, %(param_2)s, %(param_3)s] "
            "AS anon_1",
            checkparams={"param_1": 4, "param_3": 6, "param_2": 5},
        )

    def test_array_slice_index(self):
        col = column("x", postgresql.ARRAY(Integer))
        self.assert_compile(
            select(col[5:10]),
            "SELECT x[%(x_1)s:%(x_2)s] AS anon_1",
            checkparams={"x_2": 10, "x_1": 5},
        )

    def test_array_dim_index(self):
        col = column("x", postgresql.ARRAY(Integer, dimensions=2))
        self.assert_compile(
            select(col[3][5]),
            "SELECT x[%(x_1)s][%(param_1)s] AS anon_1",
            checkparams={"x_1": 3, "param_1": 5},
        )

    def test_array_concat(self):
        col = column("x", postgresql.ARRAY(Integer))
        literal = array([4, 5])

        self.assert_compile(
            select(col + literal),
            "SELECT x || ARRAY[%(param_1)s, %(param_2)s] AS anon_1",
            checkparams={"param_1": 4, "param_2": 5},
        )

    def test_array_index_map_dimensions(self):
        col = column("x", postgresql.ARRAY(Integer, dimensions=3))
        is_(col[5].type._type_affinity, ARRAY)
        assert isinstance(col[5].type, postgresql.ARRAY)
        eq_(col[5].type.dimensions, 2)
        is_(col[5][6].type._type_affinity, ARRAY)
        assert isinstance(col[5][6].type, postgresql.ARRAY)
        eq_(col[5][6].type.dimensions, 1)
        is_(col[5][6][7].type._type_affinity, Integer)

    def test_array_getitem_single_type(self):
        m = MetaData()
        arrtable = Table(
            "arrtable",
            m,
            Column("intarr", postgresql.ARRAY(Integer)),
            Column("strarr", postgresql.ARRAY(String)),
        )
        is_(arrtable.c.intarr[1].type._type_affinity, Integer)
        is_(arrtable.c.strarr[1].type._type_affinity, String)

    def test_array_getitem_slice_type(self):
        m = MetaData()
        arrtable = Table(
            "arrtable",
            m,
            Column("intarr", postgresql.ARRAY(Integer)),
            Column("strarr", postgresql.ARRAY(String)),
        )

        # type affinity is Array...
        is_(arrtable.c.intarr[1:3].type._type_affinity, ARRAY)
        is_(arrtable.c.strarr[1:3].type._type_affinity, ARRAY)

        # but the slice returns the actual type
        assert isinstance(arrtable.c.intarr[1:3].type, postgresql.ARRAY)
        assert isinstance(arrtable.c.strarr[1:3].type, postgresql.ARRAY)

    def test_array_functions_plus_getitem(self):
        """test parenthesizing of functions plus indexing, which seems
        to be required by PostgreSQL.

        """
        stmt = select(
            func.array_cat(
                array([1, 2, 3]),
                array([4, 5, 6]),
                type_=postgresql.ARRAY(Integer),
            )[2:5]
        )
        self.assert_compile(
            stmt,
            "SELECT (array_cat(ARRAY[%(param_1)s, %(param_2)s, %(param_3)s], "
            "ARRAY[%(param_4)s, %(param_5)s, %(param_6)s]))"
            "[%(param_7)s:%(param_8)s] AS anon_1",
        )

        self.assert_compile(
            func.array_cat(
                array([1, 2, 3]),
                array([4, 5, 6]),
                type_=postgresql.ARRAY(Integer),
            )[3],
            "(array_cat(ARRAY[%(param_1)s, %(param_2)s, %(param_3)s], "
            "ARRAY[%(param_4)s, %(param_5)s, %(param_6)s]))[%(array_cat_1)s]",
        )

    def test_array_agg_generic(self):
        expr = func.array_agg(column("q", Integer))
        is_(expr.type.__class__, types.ARRAY)
        is_(expr.type.item_type.__class__, Integer)

    @testing.combinations(
        ("original", False, False),
        ("just_enum", True, False),
        ("just_order_by", False, True),
        ("issue_5989", True, True),
        id_="iaa",
        argnames="with_enum, using_aggregate_order_by",
    )
    def test_array_agg_specific(self, with_enum, using_aggregate_order_by):
        from sqlalchemy.dialects.postgresql import aggregate_order_by
        from sqlalchemy.dialects.postgresql import array_agg
        from sqlalchemy.dialects.postgresql import ENUM

        element_type = ENUM if with_enum else Integer
        expr = (
            array_agg(
                aggregate_order_by(
                    column("q", element_type), column("idx", Integer)
                )
            )
            if using_aggregate_order_by
            else array_agg(column("q", element_type))
        )
        is_(expr.type.__class__, postgresql.ARRAY)
        is_(expr.type.item_type.__class__, element_type)


AnEnum = pep435_enum("AnEnum")
AnEnum("Foo", 1)
AnEnum("Bar", 2)
AnEnum("Baz", 3)


class ArrayRoundTripTest(object):

    __only_on__ = "postgresql"
    __backend__ = True
    __unsupported_on__ = ("postgresql+pg8000",)

    ARRAY = postgresql.ARRAY

    @classmethod
    def define_tables(cls, metadata):
        class ProcValue(TypeDecorator):
            impl = cls.ARRAY(Integer, dimensions=2)

            def process_bind_param(self, value, dialect):
                if value is None:
                    return None
                return [[x + 5 for x in v] for v in value]

            def process_result_value(self, value, dialect):
                if value is None:
                    return None
                return [[x - 7 for x in v] for v in value]

        Table(
            "arrtable",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("intarr", cls.ARRAY(Integer)),
            Column("strarr", cls.ARRAY(Unicode())),
            Column("dimarr", ProcValue),
        )

        Table(
            "dim_arrtable",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("intarr", cls.ARRAY(Integer, dimensions=1)),
            Column("strarr", cls.ARRAY(Unicode(), dimensions=1)),
            Column("dimarr", ProcValue),
        )

    def _fixture_456(self, table, connection):
        connection.execute(table.insert(), dict(intarr=[4, 5, 6]))

    def test_reflect_array_column(self, connection):
        metadata2 = MetaData()
        tbl = Table("arrtable", metadata2, autoload_with=connection)
        assert isinstance(tbl.c.intarr.type, self.ARRAY)
        assert isinstance(tbl.c.strarr.type, self.ARRAY)
        assert isinstance(tbl.c.intarr.type.item_type, Integer)
        assert isinstance(tbl.c.strarr.type.item_type, String)

    def test_array_str_collation(self, metadata, connection):
        t = Table(
            "t",
            metadata,
            Column("data", sqltypes.ARRAY(String(50, collation="en_US"))),
        )

        t.create(connection)

    def test_array_agg(self, metadata, connection):
        values_table = Table("values", metadata, Column("value", Integer))
        metadata.create_all(connection)
        connection.execute(
            values_table.insert(), [{"value": i} for i in range(1, 10)]
        )

        stmt = select(func.array_agg(values_table.c.value))
        eq_(connection.execute(stmt).scalar(), list(range(1, 10)))

        stmt = select(func.array_agg(values_table.c.value)[3])
        eq_(connection.execute(stmt).scalar(), 3)

        stmt = select(func.array_agg(values_table.c.value)[2:4])
        eq_(connection.execute(stmt).scalar(), [2, 3, 4])

    def test_array_index_slice_exprs(self, connection):
        """test a variety of expressions that sometimes need parenthesizing"""

        stmt = select(array([1, 2, 3, 4])[2:3])
        eq_(connection.execute(stmt).scalar(), [2, 3])

        stmt = select(array([1, 2, 3, 4])[2])
        eq_(connection.execute(stmt).scalar(), 2)

        stmt = select((array([1, 2]) + array([3, 4]))[2:3])
        eq_(connection.execute(stmt).scalar(), [2, 3])

        stmt = select(array([1, 2]) + array([3, 4])[2:3])
        eq_(connection.execute(stmt).scalar(), [1, 2, 4])

        stmt = select(array([1, 2])[2:3] + array([3, 4]))
        eq_(connection.execute(stmt).scalar(), [2, 3, 4])

        stmt = select(
            func.array_cat(
                array([1, 2, 3]),
                array([4, 5, 6]),
                type_=self.ARRAY(Integer),
            )[2:5]
        )
        eq_(connection.execute(stmt).scalar(), [2, 3, 4, 5])

    def test_any_all_exprs_array(self, connection):
        stmt = select(
            3
            == any_(
                func.array_cat(
                    array([1, 2, 3]),
                    array([4, 5, 6]),
                    type_=self.ARRAY(Integer),
                )
            )
        )
        eq_(connection.execute(stmt).scalar(), True)

    def test_insert_array(self, connection):
        arrtable = self.tables.arrtable
        connection.execute(
            arrtable.insert(),
            dict(
                intarr=[1, 2, 3],
                strarr=[util.u("abc"), util.u("def")],
            ),
        )
        results = connection.execute(arrtable.select()).fetchall()
        eq_(len(results), 1)
        eq_(results[0].intarr, [1, 2, 3])
        eq_(results[0].strarr, [util.u("abc"), util.u("def")])

    def test_insert_array_w_null(self, connection):
        arrtable = self.tables.arrtable
        connection.execute(
            arrtable.insert(),
            dict(
                intarr=[1, None, 3],
                strarr=[util.u("abc"), None],
            ),
        )
        results = connection.execute(arrtable.select()).fetchall()
        eq_(len(results), 1)
        eq_(results[0].intarr, [1, None, 3])
        eq_(results[0].strarr, [util.u("abc"), None])

    def test_array_where(self, connection):
        arrtable = self.tables.arrtable
        connection.execute(
            arrtable.insert(),
            dict(
                intarr=[1, 2, 3],
                strarr=[util.u("abc"), util.u("def")],
            ),
        )
        connection.execute(
            arrtable.insert(), dict(intarr=[4, 5, 6], strarr=util.u("ABC"))
        )
        results = connection.execute(
            arrtable.select().where(arrtable.c.intarr == [1, 2, 3])
        ).fetchall()
        eq_(len(results), 1)
        eq_(results[0].intarr, [1, 2, 3])

    def test_array_concat(self, connection):
        arrtable = self.tables.arrtable
        connection.execute(
            arrtable.insert(),
            dict(intarr=[1, 2, 3], strarr=[util.u("abc"), util.u("def")]),
        )
        results = connection.execute(
            select(arrtable.c.intarr + [4, 5, 6])
        ).fetchall()
        eq_(len(results), 1)
        eq_(results[0][0], [1, 2, 3, 4, 5, 6])

    def test_array_comparison(self, connection):
        arrtable = self.tables.arrtable
        connection.execute(
            arrtable.insert(),
            dict(
                id=5, intarr=[1, 2, 3], strarr=[util.u("abc"), util.u("def")]
            ),
        )
        results = connection.execute(
            select(arrtable.c.id).where(arrtable.c.intarr < [4, 5, 6])
        ).fetchall()
        eq_(len(results), 1)
        eq_(results[0][0], 5)

    def test_array_subtype_resultprocessor(self, connection):
        arrtable = self.tables.arrtable
        connection.execute(
            arrtable.insert(),
            dict(
                intarr=[4, 5, 6],
                strarr=[[util.ue("m\xe4\xe4")], [util.ue("m\xf6\xf6")]],
            ),
        )
        connection.execute(
            arrtable.insert(),
            dict(
                intarr=[1, 2, 3],
                strarr=[util.ue("m\xe4\xe4"), util.ue("m\xf6\xf6")],
            ),
        )
        results = connection.execute(
            arrtable.select(order_by=[arrtable.c.intarr])
        ).fetchall()
        eq_(len(results), 2)
        eq_(results[0].strarr, [util.ue("m\xe4\xe4"), util.ue("m\xf6\xf6")])
        eq_(
            results[1].strarr,
            [[util.ue("m\xe4\xe4")], [util.ue("m\xf6\xf6")]],
        )

    def test_array_literal_roundtrip(self, connection):
        eq_(
            connection.scalar(
                select(postgresql.array([1, 2]) + postgresql.array([3, 4, 5]))
            ),
            [1, 2, 3, 4, 5],
        )

        eq_(
            connection.scalar(
                select(
                    (postgresql.array([1, 2]) + postgresql.array([3, 4, 5]))[3]
                )
            ),
            3,
        )

        eq_(
            connection.scalar(
                select(
                    (postgresql.array([1, 2]) + postgresql.array([3, 4, 5]))[
                        2:4
                    ]
                )
            ),
            [2, 3, 4],
        )

    def test_array_literal_multidimensional_roundtrip(self, connection):
        eq_(
            connection.scalar(
                select(
                    postgresql.array(
                        [postgresql.array([1, 2]), postgresql.array([3, 4])]
                    )
                )
            ),
            [[1, 2], [3, 4]],
        )

        eq_(
            connection.scalar(
                select(
                    postgresql.array(
                        [postgresql.array([1, 2]), postgresql.array([3, 4])]
                    )[2][1]
                )
            ),
            3,
        )

    def test_array_literal_compare(self, connection):
        eq_(
            connection.scalar(select(postgresql.array([1, 2]) < [3, 4, 5])),
            True,
        )

    def test_array_getitem_single_exec(self, connection):
        arrtable = self.tables.arrtable
        self._fixture_456(arrtable, connection)
        eq_(connection.scalar(select(arrtable.c.intarr[2])), 5)
        connection.execute(arrtable.update().values({arrtable.c.intarr[2]: 7}))
        eq_(connection.scalar(select(arrtable.c.intarr[2])), 7)

    def test_array_getitem_slice_exec(self, connection):
        arrtable = self.tables.arrtable
        connection.execute(
            arrtable.insert(),
            dict(
                intarr=[4, 5, 6],
                strarr=[util.u("abc"), util.u("def")],
            ),
        )
        eq_(connection.scalar(select(arrtable.c.intarr[2:3])), [5, 6])
        connection.execute(
            arrtable.update().values({arrtable.c.intarr[2:3]: [7, 8]})
        )
        eq_(connection.scalar(select(arrtable.c.intarr[2:3])), [7, 8])

    def test_multi_dim_roundtrip(self, connection):
        arrtable = self.tables.arrtable
        connection.execute(
            arrtable.insert(), dict(dimarr=[[1, 2, 3], [4, 5, 6]])
        )
        eq_(
            connection.scalar(select(arrtable.c.dimarr)),
            [[-1, 0, 1], [2, 3, 4]],
        )

    def test_array_any_exec(self, connection):
        arrtable = self.tables.arrtable
        connection.execute(arrtable.insert(), dict(intarr=[4, 5, 6]))
        eq_(
            connection.scalar(
                select(arrtable.c.intarr).where(
                    postgresql.Any(5, arrtable.c.intarr)
                )
            ),
            [4, 5, 6],
        )

    def test_array_all_exec(self, connection):
        arrtable = self.tables.arrtable
        connection.execute(arrtable.insert(), dict(intarr=[4, 5, 6]))
        eq_(
            connection.scalar(
                select(arrtable.c.intarr).where(
                    arrtable.c.intarr.all(4, operator=operators.le)
                )
            ),
            [4, 5, 6],
        )

    def test_tuple_flag(self, connection, metadata):

        t1 = Table(
            "t1",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("data", self.ARRAY(String(5), as_tuple=True)),
            Column(
                "data2", self.ARRAY(Numeric(asdecimal=False), as_tuple=True)
            ),
        )
        metadata.create_all(connection)
        connection.execute(
            t1.insert(), dict(id=1, data=["1", "2", "3"], data2=[5.4, 5.6])
        )
        connection.execute(
            t1.insert(), dict(id=2, data=["4", "5", "6"], data2=[1.0])
        )
        connection.execute(
            t1.insert(),
            dict(
                id=3,
                data=[["4", "5"], ["6", "7"]],
                data2=[[5.4, 5.6], [1.0, 1.1]],
            ),
        )

        r = connection.execute(t1.select().order_by(t1.c.id)).fetchall()
        eq_(
            r,
            [
                (1, ("1", "2", "3"), (5.4, 5.6)),
                (2, ("4", "5", "6"), (1.0,)),
                (3, (("4", "5"), ("6", "7")), ((5.4, 5.6), (1.0, 1.1))),
            ],
        )
        # hashable
        eq_(
            set(row[1] for row in r),
            set([("1", "2", "3"), ("4", "5", "6"), (("4", "5"), ("6", "7"))]),
        )

    def test_array_plus_native_enum_create(self, metadata, connection):
        t = Table(
            "t",
            metadata,
            Column(
                "data_1",
                self.ARRAY(postgresql.ENUM("a", "b", "c", name="my_enum_1")),
            ),
            Column(
                "data_2",
                self.ARRAY(types.Enum("a", "b", "c", name="my_enum_2")),
            ),
            Column(
                "data_3",
                self.ARRAY(
                    types.Enum("a", "b", "c", name="my_enum_3")
                ).with_variant(String(), "other"),
            ),
        )

        t.create(connection)
        eq_(
            set(e["name"] for e in inspect(connection).get_enums()),
            set(["my_enum_1", "my_enum_2", "my_enum_3"]),
        )
        t.drop(connection)
        eq_(inspect(connection).get_enums(), [])

    def _type_combinations(exclude_json=False):
        def str_values(x):
            return ["one", "two: %s" % x, "three", "four", "five"]

        def unicode_values(x):
            return [
                util.u("réveillé"),
                util.u("drôle"),
                util.u("S’il %s" % x),
                util.u("🐍 %s" % x),
                util.u("« S’il vous"),
            ]

        def json_values(x):
            return [
                1,
                {"a": x},
                {"b": [1, 2, 3]},
                ["d", "e", "f"],
                {"struct": True, "none": None},
            ]

        def binary_values(x):
            return [v.encode("utf-8") for v in unicode_values(x)]

        def enum_values(x):
            return [
                AnEnum.Foo,
                AnEnum.Baz,
                AnEnum.get(x),
                AnEnum.Baz,
                AnEnum.Foo,
            ]

        class inet_str(str):
            def __eq__(self, other):
                return str(self) == str(other)

            def __ne__(self, other):
                return str(self) != str(other)

        class money_str(str):
            def __eq__(self, other):
                comp = re.sub(r"[^\d\.]", "", other)
                return float(self) == float(comp)

            def __ne__(self, other):
                return not self.__eq__(other)

        elements = [
            (sqltypes.Integer, lambda x: [1, x, 3, 4, 5]),
            (sqltypes.Text, str_values),
            (sqltypes.String, str_values),
            (sqltypes.Unicode, unicode_values),
            (postgresql.JSONB, json_values),
            (sqltypes.Boolean, lambda x: [False] + [True] * x),
            (
                sqltypes.LargeBinary,
                binary_values,
            ),
            (
                postgresql.BYTEA,
                binary_values,
            ),
            (
                postgresql.INET,
                lambda x: [
                    inet_str("1.1.1.1"),
                    inet_str("{0}.{0}.{0}.{0}".format(x)),
                    inet_str("192.168.1.1"),
                    inet_str("10.1.2.25"),
                    inet_str("192.168.22.5"),
                ],
            ),
            (
                postgresql.CIDR,
                lambda x: [
                    inet_str("10.0.0.0/8"),
                    inet_str("%s.0.0.0/8" % x),
                    inet_str("192.168.1.0/24"),
                    inet_str("192.168.0.0/16"),
                    inet_str("192.168.1.25/32"),
                ],
            ),
            (
                sqltypes.Date,
                lambda x: [
                    datetime.date(2020, 5, x),
                    datetime.date(2020, 7, 12),
                    datetime.date(2018, 12, 15),
                    datetime.date(2009, 1, 5),
                    datetime.date(2021, 3, 18),
                ],
            ),
            (
                sqltypes.DateTime,
                lambda x: [
                    datetime.datetime(2020, 5, x, 2, 15, 0),
                    datetime.datetime(2020, 7, 12, 15, 30, x),
                    datetime.datetime(2018, 12, 15, 3, x, 25),
                    datetime.datetime(2009, 1, 5, 12, 45, x),
                    datetime.datetime(2021, 3, 18, 17, 1, 0),
                ],
            ),
            (
                sqltypes.Numeric,
                lambda x: [
                    decimal.Decimal("45.10"),
                    decimal.Decimal(x),
                    decimal.Decimal(".03242"),
                    decimal.Decimal("532.3532"),
                    decimal.Decimal("95503.23"),
                ],
            ),
            (
                postgresql.MONEY,
                lambda x: [
                    money_str("2"),
                    money_str("%s" % (5 + x)),
                    money_str("50.25"),
                    money_str("18.99"),
                    money_str("15.%s" % x),
                ],
                testing.skip_if(
                    "postgresql+psycopg2", "this is a psycopg2 bug"
                ),
            ),
            (
                postgresql.HSTORE,
                lambda x: [
                    {"a": "1"},
                    {"b": "%s" % x},
                    {"c": "3"},
                    {"c": "c2"},
                    {"d": "e"},
                ],
                testing.requires.hstore,
            ),
            (postgresql.ENUM(AnEnum), enum_values),
            (sqltypes.Enum(AnEnum, native_enum=True), enum_values),
            (sqltypes.Enum(AnEnum, native_enum=False), enum_values),
        ]

        if not exclude_json:
            elements.extend(
                [
                    (sqltypes.JSON, json_values),
                    (postgresql.JSON, json_values),
                ]
            )

        return testing.combinations_list(
            elements, argnames="type_,gen", id_="na"
        )

    @classmethod
    def _cls_type_combinations(cls, **kw):
        return ArrayRoundTripTest.__dict__["_type_combinations"](**kw)

    @testing.fixture(params=[True, False])
    def type_specific_fixture(self, request, metadata, connection, type_):
        use_variant = request.param
        meta = MetaData()

        if use_variant:
            typ = self.ARRAY(type_).with_variant(String(), "other")
        else:
            typ = self.ARRAY(type_)

        table = Table(
            "foo",
            meta,
            Column("id", Integer),
            Column("bar", typ),
        )

        meta.create_all(connection)

        def go(gen):
            connection.execute(
                table.insert(),
                [{"id": 1, "bar": gen(1)}, {"id": 2, "bar": gen(2)}],
            )
            return table

        return go

    @_type_combinations()
    def test_type_specific_value_select(
        self, type_specific_fixture, connection, type_, gen
    ):
        table = type_specific_fixture(gen)

        rows = connection.execute(
            select(table.c.bar).order_by(table.c.id)
        ).all()

        eq_(rows, [(gen(1),), (gen(2),)])

    @_type_combinations()
    def test_type_specific_value_update(
        self, type_specific_fixture, connection, type_, gen
    ):
        table = type_specific_fixture(gen)

        new_gen = gen(3)
        connection.execute(
            table.update().where(table.c.id == 2).values(bar=new_gen)
        )

        eq_(
            new_gen,
            connection.scalar(select(table.c.bar).where(table.c.id == 2)),
        )

    @_type_combinations()
    def test_type_specific_slice_update(
        self, type_specific_fixture, connection, type_, gen
    ):
        table = type_specific_fixture(gen)

        new_gen = gen(3)

        if isinstance(table.c.bar.type, Variant):
            # this is not likely to occur to users but we need to just
            # exercise this as far as we can
            expr = type_coerce(table.c.bar, ARRAY(type_))[1:3]
        else:
            expr = table.c.bar[1:3]
        connection.execute(
            table.update().where(table.c.id == 2).values({expr: new_gen[1:4]})
        )

        rows = connection.execute(
            select(table.c.bar).order_by(table.c.id)
        ).all()

        sliced_gen = gen(2)
        sliced_gen[0:3] = new_gen[1:4]

        eq_(rows, [(gen(1),), (sliced_gen,)])

    @_type_combinations(exclude_json=True)
    def test_type_specific_value_delete(
        self, type_specific_fixture, connection, type_, gen
    ):
        table = type_specific_fixture(gen)

        new_gen = gen(2)

        connection.execute(table.delete().where(table.c.bar == new_gen))

        eq_(connection.scalar(select(func.count(table.c.id))), 1)


class CoreArrayRoundTripTest(
    ArrayRoundTripTest, fixtures.TablesTest, AssertsExecutionResults
):

    ARRAY = sqltypes.ARRAY


class PGArrayRoundTripTest(
    ArrayRoundTripTest, fixtures.TablesTest, AssertsExecutionResults
):
    ARRAY = postgresql.ARRAY

    @ArrayRoundTripTest._cls_type_combinations(exclude_json=True)
    def test_type_specific_contains(
        self, type_specific_fixture, connection, type_, gen
    ):
        table = type_specific_fixture(gen)

        connection.execute(
            table.insert(),
            [{"id": 1, "bar": gen(1)}, {"id": 2, "bar": gen(2)}],
        )

        id_, value = connection.execute(
            select(table).where(table.c.bar.contains(gen(1)))
        ).first()
        eq_(id_, 1)
        eq_(value, gen(1))

    @testing.combinations(
        (set,), (list,), (lambda elem: (x for x in elem),), argnames="struct"
    )
    def test_undim_array_contains_typed_exec(self, struct, connection):
        arrtable = self.tables.arrtable
        self._fixture_456(arrtable, connection)
        eq_(
            connection.scalar(
                select(arrtable.c.intarr).where(
                    arrtable.c.intarr.contains(struct([4, 5]))
                )
            ),
            [4, 5, 6],
        )

    @testing.combinations(
        (set,), (list,), (lambda elem: (x for x in elem),), argnames="struct"
    )
    def test_dim_array_contains_typed_exec(self, struct, connection):
        dim_arrtable = self.tables.dim_arrtable
        self._fixture_456(dim_arrtable, connection)
        eq_(
            connection.scalar(
                select(dim_arrtable.c.intarr).where(
                    dim_arrtable.c.intarr.contains(struct([4, 5]))
                )
            ),
            [4, 5, 6],
        )

    def test_array_contained_by_exec(self, connection):
        arrtable = self.tables.arrtable
        connection.execute(arrtable.insert(), dict(intarr=[6, 5, 4]))
        eq_(
            connection.scalar(
                select(arrtable.c.intarr.contained_by([4, 5, 6, 7]))
            ),
            True,
        )

    def test_undim_array_empty(self, connection):
        arrtable = self.tables.arrtable
        self._fixture_456(arrtable, connection)
        eq_(
            connection.scalar(
                select(arrtable.c.intarr).where(arrtable.c.intarr.contains([]))
            ),
            [4, 5, 6],
        )

    def test_array_overlap_exec(self, connection):
        arrtable = self.tables.arrtable
        connection.execute(arrtable.insert(), dict(intarr=[4, 5, 6]))
        eq_(
            connection.scalar(
                select(arrtable.c.intarr).where(
                    arrtable.c.intarr.overlap([7, 6])
                )
            ),
            [4, 5, 6],
        )


class _ArrayOfEnum(TypeDecorator):
    # previous workaround for array of enum
    impl = postgresql.ARRAY

    def bind_expression(self, bindvalue):
        return sa.cast(bindvalue, self)

    def result_processor(self, dialect, coltype):
        super_rp = super(_ArrayOfEnum, self).result_processor(dialect, coltype)

        def handle_raw_string(value):
            inner = re.match(r"^{(.*)}$", value).group(1)
            return inner.split(",") if inner else []

        def process(value):
            if value is None:
                return None
            return super_rp(handle_raw_string(value))

        return process


class ArrayEnum(fixtures.TestBase):
    __backend__ = True
    __only_on__ = "postgresql"
    __unsupported_on__ = ("postgresql+pg8000",)

    @testing.combinations(
        sqltypes.ARRAY, postgresql.ARRAY, argnames="array_cls"
    )
    @testing.combinations(sqltypes.Enum, postgresql.ENUM, argnames="enum_cls")
    def test_raises_non_native_enums(
        self, metadata, connection, array_cls, enum_cls
    ):
        Table(
            "my_table",
            self.metadata,
            Column(
                "my_col",
                array_cls(
                    enum_cls(
                        "foo",
                        "bar",
                        "baz",
                        name="my_enum",
                        create_constraint=True,
                        native_enum=False,
                    )
                ),
            ),
        )

        testing.assert_raises_message(
            CompileError,
            "PostgreSQL dialect cannot produce the CHECK constraint "
            "for ARRAY of non-native ENUM; please specify "
            "create_constraint=False on this Enum datatype.",
            self.metadata.create_all,
            connection,
        )

    @testing.combinations(sqltypes.Enum, postgresql.ENUM, argnames="enum_cls")
    @testing.combinations(
        sqltypes.ARRAY,
        postgresql.ARRAY,
        (_ArrayOfEnum, testing.only_on("postgresql+psycopg2")),
        argnames="array_cls",
    )
    def test_array_of_enums(self, array_cls, enum_cls, metadata, connection):
        tbl = Table(
            "enum_table",
            self.metadata,
            Column("id", Integer, primary_key=True),
            Column(
                "enum_col",
                array_cls(enum_cls("foo", "bar", "baz", name="an_enum")),
            ),
        )

        if util.py3k:
            from enum import Enum

            class MyEnum(Enum):
                a = "aaa"
                b = "bbb"
                c = "ccc"

            tbl.append_column(
                Column(
                    "pyenum_col",
                    array_cls(enum_cls(MyEnum)),
                ),
            )

        self.metadata.create_all(connection)

        connection.execute(
            tbl.insert(), [{"enum_col": ["foo"]}, {"enum_col": ["foo", "bar"]}]
        )

        sel = select(tbl.c.enum_col).order_by(tbl.c.id)
        eq_(
            connection.execute(sel).fetchall(), [(["foo"],), (["foo", "bar"],)]
        )

        if util.py3k:
            connection.execute(tbl.insert(), {"pyenum_col": [MyEnum.a]})
            sel = select(tbl.c.pyenum_col).order_by(tbl.c.id.desc())
            eq_(connection.scalar(sel), [MyEnum.a])

        self.metadata.drop_all(connection)


class ArrayJSON(fixtures.TestBase):
    __backend__ = True
    __only_on__ = "postgresql"
    __unsupported_on__ = ("postgresql+pg8000",)

    @testing.combinations(
        sqltypes.ARRAY, postgresql.ARRAY, argnames="array_cls"
    )
    @testing.combinations(
        sqltypes.JSON, postgresql.JSON, postgresql.JSONB, argnames="json_cls"
    )
    def test_array_of_json(self, array_cls, json_cls, metadata, connection):
        tbl = Table(
            "json_table",
            self.metadata,
            Column("id", Integer, primary_key=True),
            Column(
                "json_col",
                array_cls(json_cls),
            ),
        )

        self.metadata.create_all(connection)

        connection.execute(
            tbl.insert(),
            [
                {"id": 1, "json_col": ["foo"]},
                {"id": 2, "json_col": [{"foo": "bar"}, [1]]},
                {"id": 3, "json_col": [None]},
                {"id": 4, "json_col": [42]},
                {"id": 5, "json_col": [True]},
                {"id": 6, "json_col": None},
            ],
        )

        sel = select(tbl.c.json_col).order_by(tbl.c.id)
        eq_(
            connection.execute(sel).fetchall(),
            [
                (["foo"],),
                ([{"foo": "bar"}, [1]],),
                ([None],),
                ([42],),
                ([True],),
                (None,),
            ],
        )

        eq_(
            connection.exec_driver_sql(
                """select json_col::text = array['"foo"']::json[]::text"""
                " from json_table where id = 1"
            ).scalar(),
            True,
        )
        eq_(
            connection.exec_driver_sql(
                "select json_col::text = "
                """array['{"foo": "bar"}', '[1]']::json[]::text"""
                " from json_table where id = 2"
            ).scalar(),
            True,
        )
        eq_(
            connection.exec_driver_sql(
                """select json_col::text = array['null']::json[]::text"""
                " from json_table where id = 3"
            ).scalar(),
            True,
        )
        eq_(
            connection.exec_driver_sql(
                """select json_col::text = array['42']::json[]::text"""
                " from json_table where id = 4"
            ).scalar(),
            True,
        )
        eq_(
            connection.exec_driver_sql(
                """select json_col::text = array['true']::json[]::text"""
                " from json_table where id = 5"
            ).scalar(),
            True,
        )
        eq_(
            connection.exec_driver_sql(
                "select json_col is null from json_table where id = 6"
            ).scalar(),
            True,
        )


class HashableFlagORMTest(fixtures.TestBase):
    """test the various 'collection' types that they flip the 'hashable' flag
    appropriately.  [ticket:3499]"""

    __only_on__ = "postgresql"

    @testing.combinations(
        (
            "ARRAY",
            postgresql.ARRAY(Text()),
            [["a", "b", "c"], ["d", "e", "f"]],
        ),
        (
            "JSON",
            postgresql.JSON(),
            [
                {"a": "1", "b": "2", "c": "3"},
                {
                    "d": "4",
                    "e": {"e1": "5", "e2": "6"},
                    "f": {"f1": [9, 10, 11]},
                },
            ],
        ),
        (
            "HSTORE",
            postgresql.HSTORE(),
            [{"a": "1", "b": "2", "c": "3"}, {"d": "4", "e": "5", "f": "6"}],
            testing.requires.hstore,
        ),
        (
            "JSONB",
            postgresql.JSONB(),
            [
                {"a": "1", "b": "2", "c": "3"},
                {
                    "d": "4",
                    "e": {"e1": "5", "e2": "6"},
                    "f": {"f1": [9, 10, 11]},
                },
            ],
            testing.requires.postgresql_jsonb,
        ),
        argnames="type_,data",
        id_="iaa",
    )
    def test_hashable_flag(self, metadata, connection, type_, data):
        Base = declarative_base(metadata=metadata)

        class A(Base):
            __tablename__ = "a1"
            id = Column(Integer, primary_key=True)
            data = Column(type_)

        Base.metadata.create_all(connection)
        s = Session(connection)
        s.add_all([A(data=elem) for elem in data])
        s.commit()

        eq_(
            [
                (obj.A.id, obj.data)
                for obj in s.query(A, A.data).order_by(A.id)
            ],
            list(enumerate(data, 1)),
        )


class TimestampTest(fixtures.TestBase, AssertsExecutionResults):
    __only_on__ = "postgresql"
    __backend__ = True

    def test_timestamp(self, connection):
        s = select(text("timestamp '2007-12-25'"))
        result = connection.execute(s).first()
        eq_(result[0], datetime.datetime(2007, 12, 25, 0, 0))

    def test_interval_arithmetic(self, connection):
        # basically testing that we get timedelta back for an INTERVAL
        # result.  more of a driver assertion.
        s = select(text("timestamp '2007-12-25' - timestamp '2007-11-15'"))
        result = connection.execute(s).first()
        eq_(result[0], datetime.timedelta(40))

    def test_interval_coercion(self):
        expr = column("bar", postgresql.INTERVAL) + column("foo", types.Date)
        eq_(expr.type._type_affinity, types.DateTime)

        expr = column("bar", postgresql.INTERVAL) * column(
            "foo", types.Numeric
        )
        eq_(expr.type._type_affinity, types.Interval)
        assert isinstance(expr.type, postgresql.INTERVAL)


class SpecialTypesCompileTest(fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = "postgresql"

    @testing.combinations(
        (postgresql.BIT(), "BIT(1)"),
        (postgresql.BIT(5), "BIT(5)"),
        (postgresql.BIT(varying=True), "BIT VARYING"),
        (postgresql.BIT(5, varying=True), "BIT VARYING(5)"),
    )
    def test_bit_compile(self, type_, expected):
        self.assert_compile(type_, expected)


class SpecialTypesTest(fixtures.TablesTest, ComparesTables):

    """test DDL and reflection of PG-specific types """

    __only_on__ = ("postgresql >= 8.3.0",)
    __backend__ = True

    @testing.metadata_fixture()
    def special_types_table(self, metadata):

        # create these types so that we can issue
        # special SQL92 INTERVAL syntax
        class y2m(types.UserDefinedType, postgresql.INTERVAL):
            def get_col_spec(self):
                return "INTERVAL YEAR TO MONTH"

        class d2s(types.UserDefinedType, postgresql.INTERVAL):
            def get_col_spec(self):
                return "INTERVAL DAY TO SECOND"

        table = Table(
            "sometable",
            metadata,
            Column("id", postgresql.UUID, primary_key=True),
            Column("flag", postgresql.BIT),
            Column("bitstring", postgresql.BIT(4)),
            Column("addr", postgresql.INET),
            Column("addr2", postgresql.MACADDR),
            Column("price", postgresql.MONEY),
            Column("addr3", postgresql.CIDR),
            Column("doubleprec", postgresql.DOUBLE_PRECISION),
            Column("plain_interval", postgresql.INTERVAL),
            Column("year_interval", y2m()),
            Column("month_interval", d2s()),
            Column("precision_interval", postgresql.INTERVAL(precision=3)),
            Column("tsvector_document", postgresql.TSVECTOR),
        )

        return table

    def test_reflection(self, special_types_table, connection):
        # cheat so that the "strict type check"
        # works
        special_types_table.c.year_interval.type = postgresql.INTERVAL()
        special_types_table.c.month_interval.type = postgresql.INTERVAL()

        m = MetaData()
        t = Table("sometable", m, autoload_with=connection)

        self.assert_tables_equal(special_types_table, t, strict_types=True)
        assert t.c.plain_interval.type.precision is None
        assert t.c.precision_interval.type.precision == 3
        assert t.c.bitstring.type.length == 4

    def test_tsvector_round_trip(self, connection, metadata):
        t = Table("t1", metadata, Column("data", postgresql.TSVECTOR))
        t.create(connection)
        connection.execute(t.insert(), dict(data="a fat cat sat"))
        eq_(connection.scalar(select(t.c.data)), "'a' 'cat' 'fat' 'sat'")

        connection.execute(
            t.update(), dict(data="'a' 'cat' 'fat' 'mat' 'sat'")
        )

        eq_(
            connection.scalar(select(t.c.data)),
            "'a' 'cat' 'fat' 'mat' 'sat'",
        )

    def test_bit_reflection(self, metadata, connection):
        t1 = Table(
            "t1",
            metadata,
            Column("bit1", postgresql.BIT()),
            Column("bit5", postgresql.BIT(5)),
            Column("bitvarying", postgresql.BIT(varying=True)),
            Column("bitvarying5", postgresql.BIT(5, varying=True)),
        )
        t1.create(connection)
        m2 = MetaData()
        t2 = Table("t1", m2, autoload_with=connection)
        eq_(t2.c.bit1.type.length, 1)
        eq_(t2.c.bit1.type.varying, False)
        eq_(t2.c.bit5.type.length, 5)
        eq_(t2.c.bit5.type.varying, False)
        eq_(t2.c.bitvarying.type.length, None)
        eq_(t2.c.bitvarying.type.varying, True)
        eq_(t2.c.bitvarying5.type.length, 5)
        eq_(t2.c.bitvarying5.type.varying, True)


class UUIDTest(fixtures.TestBase):

    """Test the bind/return values of the UUID type."""

    __only_on__ = "postgresql >= 8.3"
    __backend__ = True

    @testing.combinations(
        (
            "not_as_uuid",
            postgresql.UUID(as_uuid=False),
            str(uuid.uuid4()),
            str(uuid.uuid4()),
        ),
        ("as_uuid", postgresql.UUID(as_uuid=True), uuid.uuid4(), uuid.uuid4()),
        id_="iaaa",
        argnames="datatype, value1, value2",
    )
    def test_round_trip(self, datatype, value1, value2, connection):
        utable = Table("utable", MetaData(), Column("data", datatype))
        utable.create(connection)
        connection.execute(utable.insert(), {"data": value1})
        connection.execute(utable.insert(), {"data": value2})
        r = connection.execute(
            select(utable.c.data).where(utable.c.data != value1)
        )
        eq_(r.fetchone()[0], value2)
        eq_(r.fetchone(), None)

    @testing.combinations(
        (
            "as_uuid",
            postgresql.ARRAY(postgresql.UUID(as_uuid=True)),
            [uuid.uuid4(), uuid.uuid4()],
            [uuid.uuid4(), uuid.uuid4()],
        ),
        (
            "not_as_uuid",
            postgresql.ARRAY(postgresql.UUID(as_uuid=False)),
            [str(uuid.uuid4()), str(uuid.uuid4())],
            [str(uuid.uuid4()), str(uuid.uuid4())],
        ),
        id_="iaaa",
        argnames="datatype, value1, value2",
    )
    # passes pg8000 as of 1.19.1
    def test_uuid_array(self, datatype, value1, value2, connection):
        self.test_round_trip(datatype, value1, value2, connection)


class HStoreTest(AssertsCompiledSQL, fixtures.TestBase):
    __dialect__ = "postgresql"

    def setup_test(self):
        metadata = MetaData()
        self.test_table = Table(
            "test_table",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("hash", HSTORE),
        )
        self.hashcol = self.test_table.c.hash

    def _test_where(self, whereclause, expected):
        stmt = select(self.test_table).where(whereclause)
        self.assert_compile(
            stmt,
            "SELECT test_table.id, test_table.hash FROM test_table "
            "WHERE %s" % expected,
        )

    def test_bind_serialize_default(self):

        dialect = postgresql.dialect(use_native_hstore=False)
        proc = self.test_table.c.hash.type._cached_bind_processor(dialect)
        eq_(
            proc(util.OrderedDict([("key1", "value1"), ("key2", "value2")])),
            '"key1"=>"value1", "key2"=>"value2"',
        )

    def test_bind_serialize_with_slashes_and_quotes(self):
        dialect = postgresql.dialect(use_native_hstore=False)
        proc = self.test_table.c.hash.type._cached_bind_processor(dialect)
        eq_(proc({'\\"a': '\\"1'}), '"\\\\\\"a"=>"\\\\\\"1"')

    def test_parse_error(self):
        dialect = postgresql.dialect(use_native_hstore=False)
        proc = self.test_table.c.hash.type._cached_result_processor(
            dialect, None
        )
        assert_raises_message(
            ValueError,
            r"""After u?'\[\.\.\.\], "key1"=>"value1", ', could not parse """
            r"""residual at position 36: u?'crapcrapcrap, "key3"\[\.\.\.\]""",
            proc,
            '"key2"=>"value2", "key1"=>"value1", '
            'crapcrapcrap, "key3"=>"value3"',
        )

    def test_result_deserialize_default(self):
        dialect = postgresql.dialect(use_native_hstore=False)
        proc = self.test_table.c.hash.type._cached_result_processor(
            dialect, None
        )
        eq_(
            proc('"key2"=>"value2", "key1"=>"value1"'),
            {"key1": "value1", "key2": "value2"},
        )

    def test_result_deserialize_with_slashes_and_quotes(self):
        dialect = postgresql.dialect(use_native_hstore=False)
        proc = self.test_table.c.hash.type._cached_result_processor(
            dialect, None
        )
        eq_(proc('"\\\\\\"a"=>"\\\\\\"1"'), {'\\"a': '\\"1'})

    def test_bind_serialize_psycopg2(self):
        from sqlalchemy.dialects.postgresql import psycopg2

        dialect = psycopg2.PGDialect_psycopg2()
        dialect._has_native_hstore = True
        proc = self.test_table.c.hash.type._cached_bind_processor(dialect)
        is_(proc, None)

        dialect = psycopg2.PGDialect_psycopg2()
        dialect._has_native_hstore = False
        proc = self.test_table.c.hash.type._cached_bind_processor(dialect)
        eq_(
            proc(util.OrderedDict([("key1", "value1"), ("key2", "value2")])),
            '"key1"=>"value1", "key2"=>"value2"',
        )

    def test_result_deserialize_psycopg2(self):
        from sqlalchemy.dialects.postgresql import psycopg2

        dialect = psycopg2.PGDialect_psycopg2()
        dialect._has_native_hstore = True
        proc = self.test_table.c.hash.type._cached_result_processor(
            dialect, None
        )
        is_(proc, None)

        dialect = psycopg2.PGDialect_psycopg2()
        dialect._has_native_hstore = False
        proc = self.test_table.c.hash.type._cached_result_processor(
            dialect, None
        )
        eq_(
            proc('"key2"=>"value2", "key1"=>"value1"'),
            {"key1": "value1", "key2": "value2"},
        )

    def test_ret_type_text(self):
        col = column("x", HSTORE())

        is_(col["foo"].type.__class__, Text)

    def test_ret_type_custom(self):
        class MyType(types.UserDefinedType):
            pass

        col = column("x", HSTORE(text_type=MyType))

        is_(col["foo"].type.__class__, MyType)

    def test_where_has_key(self):
        self._test_where(
            # hide from 2to3
            getattr(self.hashcol, "has_key")("foo"),
            "test_table.hash ? %(hash_1)s",
        )

    def test_where_has_all(self):
        self._test_where(
            self.hashcol.has_all(postgresql.array(["1", "2"])),
            "test_table.hash ?& ARRAY[%(param_1)s, %(param_2)s]",
        )

    def test_where_has_any(self):
        self._test_where(
            self.hashcol.has_any(postgresql.array(["1", "2"])),
            "test_table.hash ?| ARRAY[%(param_1)s, %(param_2)s]",
        )

    def test_where_defined(self):
        self._test_where(
            self.hashcol.defined("foo"),
            "defined(test_table.hash, %(defined_1)s)",
        )

    def test_where_contains(self):
        self._test_where(
            self.hashcol.contains({"foo": "1"}),
            "test_table.hash @> %(hash_1)s",
        )

    def test_where_contained_by(self):
        self._test_where(
            self.hashcol.contained_by({"foo": "1", "bar": None}),
            "test_table.hash <@ %(hash_1)s",
        )

    def test_where_getitem(self):
        self._test_where(
            self.hashcol["bar"] == None,  # noqa
            "(test_table.hash -> %(hash_1)s) IS NULL",
        )

    @testing.combinations(
        (
            lambda self: self.hashcol["foo"],
            "test_table.hash -> %(hash_1)s AS anon_1",
            True,
        ),
        (
            lambda self: self.hashcol.delete("foo"),
            "delete(test_table.hash, %(delete_2)s) AS delete_1",
            True,
        ),
        (
            lambda self: self.hashcol.delete(postgresql.array(["foo", "bar"])),
            (
                "delete(test_table.hash, ARRAY[%(param_1)s, %(param_2)s]) "
                "AS delete_1"
            ),
            True,
        ),
        (
            lambda self: self.hashcol.delete(hstore("1", "2")),
            (
                "delete(test_table.hash, hstore(%(hstore_1)s, %(hstore_2)s)) "
                "AS delete_1"
            ),
            True,
        ),
        (
            lambda self: self.hashcol.slice(postgresql.array(["1", "2"])),
            (
                "slice(test_table.hash, ARRAY[%(param_1)s, %(param_2)s]) "
                "AS slice_1"
            ),
            True,
        ),
        (
            lambda self: hstore("foo", "3")["foo"],
            "hstore(%(hstore_1)s, %(hstore_2)s) -> %(hstore_3)s AS anon_1",
            False,
        ),
        (
            lambda self: hstore(
                postgresql.array(["1", "2"]), postgresql.array(["3", None])
            )["1"],
            (
                "hstore(ARRAY[%(param_1)s, %(param_2)s], "
                "ARRAY[%(param_3)s, NULL]) -> %(hstore_1)s AS anon_1"
            ),
            False,
        ),
        (
            lambda self: hstore(postgresql.array(["1", "2", "3", None]))["3"],
            (
                "hstore(ARRAY[%(param_1)s, %(param_2)s, %(param_3)s, NULL]) "
                "-> %(hstore_1)s AS anon_1"
            ),
            False,
        ),
        (
            lambda self: self.hashcol.concat(
                hstore(cast(self.test_table.c.id, Text), "3")
            ),
            (
                "test_table.hash || hstore(CAST(test_table.id AS TEXT), "
                "%(hstore_1)s) AS anon_1"
            ),
            True,
        ),
        (
            lambda self: hstore("foo", "bar") + self.hashcol,
            "hstore(%(hstore_1)s, %(hstore_2)s) || test_table.hash AS anon_1",
            True,
        ),
        (
            lambda self: (self.hashcol + self.hashcol)["foo"],
            "(test_table.hash || test_table.hash) -> %(param_1)s AS anon_1",
            True,
        ),
        (
            lambda self: self.hashcol["foo"] != None,  # noqa
            "(test_table.hash -> %(hash_1)s) IS NOT NULL AS anon_1",
            True,
        ),
        (
            # hide from 2to3
            lambda self: getattr(self.hashcol, "keys")(),
            "akeys(test_table.hash) AS akeys_1",
            True,
        ),
        (
            lambda self: self.hashcol.vals(),
            "avals(test_table.hash) AS avals_1",
            True,
        ),
        (
            lambda self: self.hashcol.array(),
            "hstore_to_array(test_table.hash) AS hstore_to_array_1",
            True,
        ),
        (
            lambda self: self.hashcol.matrix(),
            "hstore_to_matrix(test_table.hash) AS hstore_to_matrix_1",
            True,
        ),
    )
    def test_cols(self, colclause_fn, expected, from_):
        colclause = colclause_fn(self)
        stmt = select(colclause)
        self.assert_compile(
            stmt,
            ("SELECT %s" + (" FROM test_table" if from_ else "")) % expected,
        )


class HStoreRoundTripTest(fixtures.TablesTest):
    __requires__ = ("hstore",)
    __dialect__ = "postgresql"
    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "data_table",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("name", String(30), nullable=False),
            Column("data", HSTORE),
        )

    def _fixture_data(self, connection):
        data_table = self.tables.data_table
        connection.execute(
            data_table.insert(),
            [
                {"name": "r1", "data": {"k1": "r1v1", "k2": "r1v2"}},
                {"name": "r2", "data": {"k1": "r2v1", "k2": "r2v2"}},
                {"name": "r3", "data": {"k1": "r3v1", "k2": "r3v2"}},
                {"name": "r4", "data": {"k1": "r4v1", "k2": "r4v2"}},
                {"name": "r5", "data": {"k1": "r5v1", "k2": "r5v2"}},
            ],
        )

    def _assert_data(self, compare, conn):
        data = conn.execute(
            select(self.tables.data_table.c.data).order_by(
                self.tables.data_table.c.name
            )
        ).fetchall()
        eq_([d for d, in data], compare)

    def _test_insert(self, connection):
        connection.execute(
            self.tables.data_table.insert(),
            {"name": "r1", "data": {"k1": "r1v1", "k2": "r1v2"}},
        )
        self._assert_data([{"k1": "r1v1", "k2": "r1v2"}], connection)

    @testing.fixture
    def non_native_hstore_connection(self, testing_engine):
        local_engine = testing.requires.psycopg2_native_hstore.enabled

        if local_engine:
            engine = testing_engine(options=dict(use_native_hstore=False))
        else:
            engine = testing.db

        conn = engine.connect()
        trans = conn.begin()
        yield conn
        try:
            trans.rollback()
        finally:
            conn.close()

    def test_reflect(self, connection):
        insp = inspect(connection)
        cols = insp.get_columns("data_table")
        assert isinstance(cols[2]["type"], HSTORE)

    def test_literal_round_trip(self, connection):
        # in particular, this tests that the array index
        # operator against the function is handled by PG; with some
        # array functions it requires outer parenthezisation on the left and
        # we may not be doing that here
        expr = hstore(
            postgresql.array(["1", "2"]), postgresql.array(["3", None])
        )["1"]
        eq_(connection.scalar(select(expr)), "3")

    @testing.requires.psycopg2_native_hstore
    def test_insert_native(self, connection):
        self._test_insert(connection)

    def test_insert_python(self, non_native_hstore_connection):
        self._test_insert(non_native_hstore_connection)

    @testing.requires.psycopg2_native_hstore
    def test_criterion_native(self, connection):
        self._fixture_data(connection)
        self._test_criterion(connection)

    def test_criterion_python(self, non_native_hstore_connection):
        self._fixture_data(non_native_hstore_connection)
        self._test_criterion(non_native_hstore_connection)

    def _test_criterion(self, connection):
        data_table = self.tables.data_table
        result = connection.execute(
            select(data_table.c.data).where(data_table.c.data["k1"] == "r3v1")
        ).first()
        eq_(result, ({"k1": "r3v1", "k2": "r3v2"},))

    def _test_fixed_round_trip(self, connection):
        s = select(
            hstore(
                array(["key1", "key2", "key3"]),
                array(["value1", "value2", "value3"]),
            )
        )
        eq_(
            connection.scalar(s),
            {"key1": "value1", "key2": "value2", "key3": "value3"},
        )

    def test_fixed_round_trip_python(self, non_native_hstore_connection):
        self._test_fixed_round_trip(non_native_hstore_connection)

    @testing.requires.psycopg2_native_hstore
    def test_fixed_round_trip_native(self, connection):
        self._test_fixed_round_trip(connection)

    def _test_unicode_round_trip(self, connection):
        s = select(
            hstore(
                array([util.u("réveillé"), util.u("drôle"), util.u("S’il")]),
                array([util.u("réveillé"), util.u("drôle"), util.u("S’il")]),
            )
        )
        eq_(
            connection.scalar(s),
            {
                util.u("réveillé"): util.u("réveillé"),
                util.u("drôle"): util.u("drôle"),
                util.u("S’il"): util.u("S’il"),
            },
        )

    @testing.requires.psycopg2_native_hstore
    def test_unicode_round_trip_python(self, non_native_hstore_connection):
        self._test_unicode_round_trip(non_native_hstore_connection)

    @testing.requires.psycopg2_native_hstore
    def test_unicode_round_trip_native(self, connection):
        self._test_unicode_round_trip(connection)

    def test_escaped_quotes_round_trip_python(
        self, non_native_hstore_connection
    ):
        self._test_escaped_quotes_round_trip(non_native_hstore_connection)

    @testing.requires.psycopg2_native_hstore
    def test_escaped_quotes_round_trip_native(self, connection):
        self._test_escaped_quotes_round_trip(connection)

    def _test_escaped_quotes_round_trip(self, connection):
        connection.execute(
            self.tables.data_table.insert(),
            {"name": "r1", "data": {r"key \"foo\"": r'value \"bar"\ xyz'}},
        )
        self._assert_data([{r"key \"foo\"": r'value \"bar"\ xyz'}], connection)

    def test_orm_round_trip(self, connection):
        from sqlalchemy import orm

        class Data(object):
            def __init__(self, name, data):
                self.name = name
                self.data = data

        orm.mapper(Data, self.tables.data_table)

        with orm.Session(connection) as s:
            d = Data(
                name="r1",
                data={"key1": "value1", "key2": "value2", "key3": "value3"},
            )
            s.add(d)
            eq_(s.query(Data.data, Data).all(), [(d.data, d)])


class _RangeTypeCompilation(AssertsCompiledSQL, fixtures.TestBase):
    __dialect__ = "postgresql"

    # operator tests

    @classmethod
    def setup_test_class(cls):
        table = Table(
            "data_table",
            MetaData(),
            Column("range", cls._col_type, primary_key=True),
        )
        cls.col = table.c.range

    def _test_clause(self, colclause, expected, type_):
        self.assert_compile(colclause, expected)
        is_(colclause.type._type_affinity, type_._type_affinity)

    def test_where_equal(self):
        self._test_clause(
            self.col == self._data_str(),
            "data_table.range = %(range_1)s",
            sqltypes.BOOLEANTYPE,
        )

    def test_where_not_equal(self):
        self._test_clause(
            self.col != self._data_str(),
            "data_table.range <> %(range_1)s",
            sqltypes.BOOLEANTYPE,
        )

    def test_where_is_null(self):
        self._test_clause(
            self.col == None, "data_table.range IS NULL", sqltypes.BOOLEANTYPE
        )

    def test_where_is_not_null(self):
        self._test_clause(
            self.col != None,
            "data_table.range IS NOT NULL",
            sqltypes.BOOLEANTYPE,
        )

    def test_where_less_than(self):
        self._test_clause(
            self.col < self._data_str(),
            "data_table.range < %(range_1)s",
            sqltypes.BOOLEANTYPE,
        )

    def test_where_greater_than(self):
        self._test_clause(
            self.col > self._data_str(),
            "data_table.range > %(range_1)s",
            sqltypes.BOOLEANTYPE,
        )

    def test_where_less_than_or_equal(self):
        self._test_clause(
            self.col <= self._data_str(),
            "data_table.range <= %(range_1)s",
            sqltypes.BOOLEANTYPE,
        )

    def test_where_greater_than_or_equal(self):
        self._test_clause(
            self.col >= self._data_str(),
            "data_table.range >= %(range_1)s",
            sqltypes.BOOLEANTYPE,
        )

    def test_contains(self):
        self._test_clause(
            self.col.contains(self._data_str()),
            "data_table.range @> %(range_1)s",
            sqltypes.BOOLEANTYPE,
        )

    def test_contained_by(self):
        self._test_clause(
            self.col.contained_by(self._data_str()),
            "data_table.range <@ %(range_1)s",
            sqltypes.BOOLEANTYPE,
        )

    def test_overlaps(self):
        self._test_clause(
            self.col.overlaps(self._data_str()),
            "data_table.range && %(range_1)s",
            sqltypes.BOOLEANTYPE,
        )

    def test_strictly_left_of(self):
        self._test_clause(
            self.col << self._data_str(),
            "data_table.range << %(range_1)s",
            sqltypes.BOOLEANTYPE,
        )
        self._test_clause(
            self.col.strictly_left_of(self._data_str()),
            "data_table.range << %(range_1)s",
            sqltypes.BOOLEANTYPE,
        )

    def test_strictly_right_of(self):
        self._test_clause(
            self.col >> self._data_str(),
            "data_table.range >> %(range_1)s",
            sqltypes.BOOLEANTYPE,
        )
        self._test_clause(
            self.col.strictly_right_of(self._data_str()),
            "data_table.range >> %(range_1)s",
            sqltypes.BOOLEANTYPE,
        )

    def test_not_extend_right_of(self):
        self._test_clause(
            self.col.not_extend_right_of(self._data_str()),
            "data_table.range &< %(range_1)s",
            sqltypes.BOOLEANTYPE,
        )

    def test_not_extend_left_of(self):
        self._test_clause(
            self.col.not_extend_left_of(self._data_str()),
            "data_table.range &> %(range_1)s",
            sqltypes.BOOLEANTYPE,
        )

    def test_adjacent_to(self):
        self._test_clause(
            self.col.adjacent_to(self._data_str()),
            "data_table.range -|- %(range_1)s",
            sqltypes.BOOLEANTYPE,
        )

    def test_union(self):
        self._test_clause(
            self.col + self.col,
            "data_table.range + data_table.range",
            self.col.type,
        )

    def test_intersection(self):
        self._test_clause(
            self.col * self.col,
            "data_table.range * data_table.range",
            self.col.type,
        )

    def test_different(self):
        self._test_clause(
            self.col - self.col,
            "data_table.range - data_table.range",
            self.col.type,
        )


class _RangeTypeRoundTrip(fixtures.TablesTest):
    __requires__ = "range_types", "psycopg2_compatibility"
    __backend__ = True

    def extras(self):
        # done this way so we don't get ImportErrors with
        # older psycopg2 versions.
        if testing.against("postgresql+psycopg2cffi"):
            from psycopg2cffi import extras
        else:
            from psycopg2 import extras
        return extras

    @classmethod
    def define_tables(cls, metadata):
        # no reason ranges shouldn't be primary keys,
        # so lets just use them as such
        table = Table(
            "data_table",
            metadata,
            Column("range", cls._col_type, primary_key=True),
        )
        cls.col = table.c.range

    def test_actual_type(self):
        eq_(str(self._col_type()), self._col_str)

    def test_reflect(self, connection):
        from sqlalchemy import inspect

        insp = inspect(connection)
        cols = insp.get_columns("data_table")
        assert isinstance(cols[0]["type"], self._col_type)

    def _assert_data(self, conn):
        data = conn.execute(select(self.tables.data_table.c.range)).fetchall()
        eq_(data, [(self._data_obj(),)])

    def test_insert_obj(self, connection):
        connection.execute(
            self.tables.data_table.insert(), {"range": self._data_obj()}
        )
        self._assert_data(connection)

    def test_insert_text(self, connection):
        connection.execute(
            self.tables.data_table.insert(), {"range": self._data_str()}
        )
        self._assert_data(connection)

    def test_union_result(self, connection):
        # insert
        connection.execute(
            self.tables.data_table.insert(), {"range": self._data_str()}
        )
        # select
        range_ = self.tables.data_table.c.range
        data = connection.execute(select(range_ + range_)).fetchall()
        eq_(data, [(self._data_obj(),)])

    def test_intersection_result(self, connection):
        # insert
        connection.execute(
            self.tables.data_table.insert(), {"range": self._data_str()}
        )
        # select
        range_ = self.tables.data_table.c.range
        data = connection.execute(select(range_ * range_)).fetchall()
        eq_(data, [(self._data_obj(),)])

    def test_difference_result(self, connection):
        # insert
        connection.execute(
            self.tables.data_table.insert(), {"range": self._data_str()}
        )
        # select
        range_ = self.tables.data_table.c.range
        data = connection.execute(select(range_ - range_)).fetchall()
        eq_(data, [(self._data_obj().__class__(empty=True),)])


class _Int4RangeTests(object):

    _col_type = INT4RANGE
    _col_str = "INT4RANGE"

    def _data_str(self):
        return "[1,2)"

    def _data_obj(self):
        return self.extras().NumericRange(1, 2)


class _Int8RangeTests(object):

    _col_type = INT8RANGE
    _col_str = "INT8RANGE"

    def _data_str(self):
        return "[9223372036854775806,9223372036854775807)"

    def _data_obj(self):
        return self.extras().NumericRange(
            9223372036854775806, 9223372036854775807
        )


class _NumRangeTests(object):

    _col_type = NUMRANGE
    _col_str = "NUMRANGE"

    def _data_str(self):
        return "[1.0,2.0)"

    def _data_obj(self):
        return self.extras().NumericRange(
            decimal.Decimal("1.0"), decimal.Decimal("2.0")
        )


class _DateRangeTests(object):

    _col_type = DATERANGE
    _col_str = "DATERANGE"

    def _data_str(self):
        return "[2013-03-23,2013-03-24)"

    def _data_obj(self):
        return self.extras().DateRange(
            datetime.date(2013, 3, 23), datetime.date(2013, 3, 24)
        )


class _DateTimeRangeTests(object):

    _col_type = TSRANGE
    _col_str = "TSRANGE"

    def _data_str(self):
        return "[2013-03-23 14:30,2013-03-23 23:30)"

    def _data_obj(self):
        return self.extras().DateTimeRange(
            datetime.datetime(2013, 3, 23, 14, 30),
            datetime.datetime(2013, 3, 23, 23, 30),
        )


class _DateTimeTZRangeTests(object):

    _col_type = TSTZRANGE
    _col_str = "TSTZRANGE"

    # make sure we use one, steady timestamp with timezone pair
    # for all parts of all these tests
    _tstzs = None

    def tstzs(self):
        if self._tstzs is None:
            with testing.db.connect() as connection:
                lower = connection.scalar(func.current_timestamp().select())
                upper = lower + datetime.timedelta(1)
                self._tstzs = (lower, upper)
        return self._tstzs

    def _data_str(self):
        return "[%s,%s)" % self.tstzs()

    def _data_obj(self):
        return self.extras().DateTimeTZRange(*self.tstzs())


class Int4RangeCompilationTest(_Int4RangeTests, _RangeTypeCompilation):
    pass


class Int4RangeRoundTripTest(_Int4RangeTests, _RangeTypeRoundTrip):
    pass


class Int8RangeCompilationTest(_Int8RangeTests, _RangeTypeCompilation):
    pass


class Int8RangeRoundTripTest(_Int8RangeTests, _RangeTypeRoundTrip):
    pass


class NumRangeCompilationTest(_NumRangeTests, _RangeTypeCompilation):
    pass


class NumRangeRoundTripTest(_NumRangeTests, _RangeTypeRoundTrip):
    pass


class DateRangeCompilationTest(_DateRangeTests, _RangeTypeCompilation):
    pass


class DateRangeRoundTripTest(_DateRangeTests, _RangeTypeRoundTrip):
    pass


class DateTimeRangeCompilationTest(_DateTimeRangeTests, _RangeTypeCompilation):
    pass


class DateTimeRangeRoundTripTest(_DateTimeRangeTests, _RangeTypeRoundTrip):
    pass


class DateTimeTZRangeCompilationTest(
    _DateTimeTZRangeTests, _RangeTypeCompilation
):
    pass


class DateTimeTZRangeRoundTripTest(_DateTimeTZRangeTests, _RangeTypeRoundTrip):
    pass


class JSONTest(AssertsCompiledSQL, fixtures.TestBase):
    __dialect__ = "postgresql"

    def setup_test(self):
        metadata = MetaData()
        self.test_table = Table(
            "test_table",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("test_column", JSON),
        )
        self.jsoncol = self.test_table.c.test_column

    @testing.combinations(
        (
            lambda self: self.jsoncol["bar"] == None,  # noqa
            "(test_table.test_column -> %(test_column_1)s) IS NULL",
        ),
        (
            lambda self: self.jsoncol[("foo", 1)] == None,  # noqa
            "(test_table.test_column #> %(test_column_1)s) IS NULL",
        ),
        (
            lambda self: self.jsoncol["bar"].astext == None,  # noqa
            "(test_table.test_column ->> %(test_column_1)s) IS NULL",
        ),
        (
            lambda self: self.jsoncol["bar"].astext.cast(Integer) == 5,
            "CAST((test_table.test_column ->> %(test_column_1)s) AS INTEGER) "
            "= %(param_1)s",
        ),
        (
            lambda self: self.jsoncol["bar"].cast(Integer) == 5,
            "CAST((test_table.test_column -> %(test_column_1)s) AS INTEGER) "
            "= %(param_1)s",
        ),
        (
            lambda self: self.jsoncol[("foo", 1)].astext == None,  # noqa
            "(test_table.test_column #>> %(test_column_1)s) IS NULL",
        ),
    )
    def test_where(self, whereclause_fn, expected):
        whereclause = whereclause_fn(self)
        stmt = select(self.test_table).where(whereclause)
        self.assert_compile(
            stmt,
            "SELECT test_table.id, test_table.test_column FROM test_table "
            "WHERE %s" % expected,
        )

    def test_path_typing(self):
        col = column("x", JSON())
        is_(col["q"].type._type_affinity, types.JSON)
        is_(col[("q",)].type._type_affinity, types.JSON)
        is_(col["q"]["p"].type._type_affinity, types.JSON)
        is_(col[("q", "p")].type._type_affinity, types.JSON)

    def test_custom_astext_type(self):
        class MyType(types.UserDefinedType):
            pass

        col = column("x", JSON(astext_type=MyType))

        is_(col["q"].astext.type.__class__, MyType)

        is_(col[("q", "p")].astext.type.__class__, MyType)

        is_(col["q"]["p"].astext.type.__class__, MyType)

    @testing.combinations(
        (
            lambda self: self.jsoncol["foo"],
            "test_table.test_column -> %(test_column_1)s AS anon_1",
            True,
        )
    )
    def test_cols(self, colclause_fn, expected, from_):
        colclause = colclause_fn(self)
        stmt = select(colclause)
        self.assert_compile(
            stmt,
            ("SELECT %s" + (" FROM test_table" if from_ else "")) % expected,
        )


class JSONRoundTripTest(fixtures.TablesTest):
    __only_on__ = ("postgresql >= 9.3",)
    __backend__ = True

    data_type = JSON

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "data_table",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("name", String(30), nullable=False),
            Column("data", cls.data_type),
            Column("nulldata", cls.data_type(none_as_null=True)),
        )

    def _fixture_data(self, connection):
        data_table = self.tables.data_table

        data = [
            {"name": "r1", "data": {"k1": "r1v1", "k2": "r1v2"}},
            {"name": "r2", "data": {"k1": "r2v1", "k2": "r2v2"}},
            {"name": "r3", "data": {"k1": "r3v1", "k2": "r3v2"}},
            {"name": "r4", "data": {"k1": "r4v1", "k2": "r4v2"}},
            {"name": "r5", "data": {"k1": "r5v1", "k2": "r5v2", "k3": 5}},
            {"name": "r6", "data": {"k1": {"r6v1": {"subr": [1, 2, 3]}}}},
        ]
        connection.execute(data_table.insert(), data)
        return data

    def _assert_data(self, compare, conn, column="data"):
        col = self.tables.data_table.c[column]
        data = conn.execute(
            select(col).order_by(self.tables.data_table.c.name)
        ).fetchall()
        eq_([d for d, in data], compare)

    def _assert_column_is_NULL(self, conn, column="data"):
        col = self.tables.data_table.c[column]
        data = conn.execute(select(col).where(col.is_(null()))).fetchall()
        eq_([d for d, in data], [None])

    def _assert_column_is_JSON_NULL(self, conn, column="data"):
        col = self.tables.data_table.c[column]
        data = conn.execute(
            select(col).where(cast(col, String) == "null")
        ).fetchall()
        eq_([d for d, in data], [None])

    def test_reflect(self, connection):
        insp = inspect(connection)
        cols = insp.get_columns("data_table")
        assert isinstance(cols[2]["type"], self.data_type)

    def test_insert(self, connection):
        connection.execute(
            self.tables.data_table.insert(),
            {"name": "r1", "data": {"k1": "r1v1", "k2": "r1v2"}},
        )
        self._assert_data([{"k1": "r1v1", "k2": "r1v2"}], connection)

    def test_insert_nulls(self, connection):
        connection.execute(
            self.tables.data_table.insert(), {"name": "r1", "data": null()}
        )
        self._assert_data([None], connection)

    def test_insert_none_as_null(self, connection):
        connection.execute(
            self.tables.data_table.insert(),
            {"name": "r1", "nulldata": None},
        )
        self._assert_column_is_NULL(connection, column="nulldata")

    def test_insert_nulljson_into_none_as_null(self, connection):
        connection.execute(
            self.tables.data_table.insert(),
            {"name": "r1", "nulldata": JSON.NULL},
        )
        self._assert_column_is_JSON_NULL(connection, column="nulldata")

    def test_custom_serialize_deserialize(self, testing_engine):
        import json

        def loads(value):
            value = json.loads(value)
            value["x"] = value["x"] + "_loads"
            return value

        def dumps(value):
            value = dict(value)
            value["x"] = "dumps_y"
            return json.dumps(value)

        engine = testing_engine(
            options=dict(json_serializer=dumps, json_deserializer=loads)
        )

        s = select(cast({"key": "value", "x": "q"}, self.data_type))
        with engine.begin() as conn:
            eq_(conn.scalar(s), {"key": "value", "x": "dumps_y_loads"})

    def test_criterion(self, connection):
        self._fixture_data(connection)
        data_table = self.tables.data_table

        result = connection.execute(
            select(data_table.c.data).where(
                data_table.c.data["k1"].astext == "r3v1"
            )
        ).first()
        eq_(result, ({"k1": "r3v1", "k2": "r3v2"},))

        result = connection.execute(
            select(data_table.c.data).where(
                data_table.c.data["k1"].astext.cast(String) == "r3v1"
            )
        ).first()
        eq_(result, ({"k1": "r3v1", "k2": "r3v2"},))

    def test_path_query(self, connection):
        self._fixture_data(connection)
        data_table = self.tables.data_table

        result = connection.execute(
            select(data_table.c.name).where(
                data_table.c.data[("k1", "r6v1", "subr")].astext == "[1, 2, 3]"
            )
        )
        eq_(result.scalar(), "r6")

    @testing.fails_on(
        "postgresql < 9.4", "Improvement in PostgreSQL behavior?"
    )
    def test_multi_index_query(self, connection):
        self._fixture_data(connection)
        data_table = self.tables.data_table

        result = connection.execute(
            select(data_table.c.name).where(
                data_table.c.data["k1"]["r6v1"]["subr"].astext == "[1, 2, 3]"
            )
        )
        eq_(result.scalar(), "r6")

    def test_query_returned_as_text(self, connection):
        self._fixture_data(connection)
        data_table = self.tables.data_table
        result = connection.execute(
            select(data_table.c.data["k1"].astext)
        ).first()
        if connection.dialect.returns_unicode_strings:
            assert isinstance(result[0], util.text_type)
        else:
            assert isinstance(result[0], util.string_types)

    def test_query_returned_as_int(self, connection):
        self._fixture_data(connection)
        data_table = self.tables.data_table
        result = connection.execute(
            select(data_table.c.data["k3"].astext.cast(Integer)).where(
                data_table.c.name == "r5"
            )
        ).first()
        assert isinstance(result[0], int)

    def test_fixed_round_trip(self, connection):
        s = select(
            cast(
                {"key": "value", "key2": {"k1": "v1", "k2": "v2"}},
                self.data_type,
            )
        )
        eq_(
            connection.scalar(s),
            {"key": "value", "key2": {"k1": "v1", "k2": "v2"}},
        )

    def test_unicode_round_trip(self, connection):
        s = select(
            cast(
                {
                    util.u("réveillé"): util.u("réveillé"),
                    "data": {"k1": util.u("drôle")},
                },
                self.data_type,
            )
        )
        eq_(
            connection.scalar(s),
            {
                util.u("réveillé"): util.u("réveillé"),
                "data": {"k1": util.u("drôle")},
            },
        )

    def test_eval_none_flag_orm(self, connection):
        Base = declarative_base()

        class Data(Base):
            __table__ = self.tables.data_table

        with Session(connection) as s:
            d1 = Data(name="d1", data=None, nulldata=None)
            s.add(d1)
            s.commit()

            s.bulk_insert_mappings(
                Data, [{"name": "d2", "data": None, "nulldata": None}]
            )
            eq_(
                s.query(
                    cast(self.tables.data_table.c.data, String),
                    cast(self.tables.data_table.c.nulldata, String),
                )
                .filter(self.tables.data_table.c.name == "d1")
                .first(),
                ("null", None),
            )
            eq_(
                s.query(
                    cast(self.tables.data_table.c.data, String),
                    cast(self.tables.data_table.c.nulldata, String),
                )
                .filter(self.tables.data_table.c.name == "d2")
                .first(),
                ("null", None),
            )

    def test_literal(self, connection):
        exp = self._fixture_data(connection)
        result = connection.exec_driver_sql(
            "select data from data_table order by name"
        )
        res = list(result)
        eq_(len(res), len(exp))
        for row, expected in zip(res, exp):
            eq_(row[0], expected["data"])


class JSONBTest(JSONTest):
    def setup_test(self):
        metadata = MetaData()
        self.test_table = Table(
            "test_table",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("test_column", JSONB),
        )
        self.jsoncol = self.test_table.c.test_column

    @testing.combinations(
        (
            # hide from 2to3
            lambda self: getattr(self.jsoncol, "has_key")("data"),
            "test_table.test_column ? %(test_column_1)s",
        ),
        (
            lambda self: self.jsoncol.has_all(
                {"name": "r1", "data": {"k1": "r1v1", "k2": "r1v2"}}
            ),
            "test_table.test_column ?& %(test_column_1)s",
        ),
        (
            lambda self: self.jsoncol.has_any(
                postgresql.array(["name", "data"])
            ),
            "test_table.test_column ?| ARRAY[%(param_1)s, %(param_2)s]",
        ),
        (
            lambda self: self.jsoncol.contains({"k1": "r1v1"}),
            "test_table.test_column @> %(test_column_1)s",
        ),
        (
            lambda self: self.jsoncol.contained_by({"foo": "1", "bar": None}),
            "test_table.test_column <@ %(test_column_1)s",
        ),
    )
    def test_where(self, whereclause_fn, expected):
        super(JSONBTest, self).test_where(whereclause_fn, expected)


class JSONBRoundTripTest(JSONRoundTripTest):
    __requires__ = ("postgresql_jsonb",)

    data_type = JSONB

    @testing.requires.postgresql_utf8_server_encoding
    def test_unicode_round_trip(self, connection):
        super(JSONBRoundTripTest, self).test_unicode_round_trip(connection)


class JSONBSuiteTest(suite.JSONTest):
    __requires__ = ("postgresql_jsonb",)

    datatype = JSONB


class JSONBCastSuiteTest(suite.JSONLegacyStringCastIndexTest):
    __requires__ = ("postgresql_jsonb",)

    datatype = JSONB
