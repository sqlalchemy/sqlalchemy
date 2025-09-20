import datetime
import decimal
from enum import Enum as _PY_Enum
import functools
from ipaddress import IPv4Address
from ipaddress import IPv4Network
from ipaddress import IPv6Address
from ipaddress import IPv6Network
import re
import uuid

import sqlalchemy as sa
from sqlalchemy import all_
from sqlalchemy import any_
from sqlalchemy import ARRAY
from sqlalchemy import cast
from sqlalchemy import Column
from sqlalchemy import column
from sqlalchemy import DateTime
from sqlalchemy import Double
from sqlalchemy import Enum
from sqlalchemy import exc
from sqlalchemy import Float
from sqlalchemy import func
from sqlalchemy import insert
from sqlalchemy import inspect
from sqlalchemy import Integer
from sqlalchemy import literal
from sqlalchemy import literal_column
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
from sqlalchemy.dialects.postgresql import aggregate_order_by
from sqlalchemy.dialects.postgresql import array
from sqlalchemy.dialects.postgresql import array_agg
from sqlalchemy.dialects.postgresql import asyncpg
from sqlalchemy.dialects.postgresql import base
from sqlalchemy.dialects.postgresql import BIT
from sqlalchemy.dialects.postgresql import BitString
from sqlalchemy.dialects.postgresql import BYTEA
from sqlalchemy.dialects.postgresql import CITEXT
from sqlalchemy.dialects.postgresql import DATEMULTIRANGE
from sqlalchemy.dialects.postgresql import DATERANGE
from sqlalchemy.dialects.postgresql import DOMAIN
from sqlalchemy.dialects.postgresql import ENUM
from sqlalchemy.dialects.postgresql import HSTORE
from sqlalchemy.dialects.postgresql import hstore
from sqlalchemy.dialects.postgresql import INT4MULTIRANGE
from sqlalchemy.dialects.postgresql import INT4RANGE
from sqlalchemy.dialects.postgresql import INT8MULTIRANGE
from sqlalchemy.dialects.postgresql import INT8RANGE
from sqlalchemy.dialects.postgresql import JSON
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import NamedType
from sqlalchemy.dialects.postgresql import NUMMULTIRANGE
from sqlalchemy.dialects.postgresql import NUMRANGE
from sqlalchemy.dialects.postgresql import pg8000
from sqlalchemy.dialects.postgresql import psycopg
from sqlalchemy.dialects.postgresql import psycopg2
from sqlalchemy.dialects.postgresql import psycopg2cffi
from sqlalchemy.dialects.postgresql import Range
from sqlalchemy.dialects.postgresql import TSMULTIRANGE
from sqlalchemy.dialects.postgresql import TSRANGE
from sqlalchemy.dialects.postgresql import TSTZMULTIRANGE
from sqlalchemy.dialects.postgresql import TSTZRANGE
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.dialects.postgresql.ranges import MultiRange
from sqlalchemy.exc import CompileError
from sqlalchemy.exc import DBAPIError
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import Session
from sqlalchemy.sql import bindparam
from sqlalchemy.sql import operators
from sqlalchemy.sql import sqltypes
from sqlalchemy.testing import expect_raises
from sqlalchemy.testing import expect_raises_message
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import is_false
from sqlalchemy.testing import is_true
from sqlalchemy.testing.assertions import assert_raises
from sqlalchemy.testing.assertions import assert_raises_message
from sqlalchemy.testing.assertions import AssertsCompiledSQL
from sqlalchemy.testing.assertions import AssertsExecutionResults
from sqlalchemy.testing.assertions import ComparesTables
from sqlalchemy.testing.assertions import eq_
from sqlalchemy.testing.assertions import is_
from sqlalchemy.testing.assertions import ne_
from sqlalchemy.testing.assertsql import RegexSQL
from sqlalchemy.testing.schema import pep435_enum
from sqlalchemy.testing.suite import test_types as suite
from sqlalchemy.testing.util import round_decimal
from sqlalchemy.types import UserDefinedType
from ...engine.test_ddlevents import DDLEventWCreateHarness


def _array_any_deprecation():
    return testing.expect_deprecated(
        r"The ARRAY.Comparator.any\(\) and "
        r"ARRAY.Comparator.all\(\) methods "
        r"for arrays are deprecated for removal, along with the "
        r"PG-specific Any\(\) "
        r"and All\(\) functions. See any_\(\) and all_\(\) functions for "
        "modern use. "
    )


class MiscTypesTest(AssertsCompiledSQL, fixtures.TestBase):
    __dialect__ = postgresql.dialect()

    @testing.combinations(
        ("asyncpg", "x LIKE $1::VARCHAR"),
        ("psycopg", "x LIKE %(x_1)s::VARCHAR"),
        ("psycopg2", "x LIKE %(x_1)s"),
        ("pg8000", "x LIKE %s::VARCHAR"),
    )
    def test_string_coercion_no_len(self, driver, expected):
        """test #9511.

        comparing to string does not include length in the cast for those
        dialects that require a cast.

        """

        self.assert_compile(
            column("x", String(2)).like("%a%"),
            expected,
            dialect=f"postgresql+{driver}",
        )

    @testing.combinations(
        ("sa", sqltypes.Float(), "FLOAT"),  # ideally it should render real
        ("sa", sqltypes.Double(), "DOUBLE PRECISION"),
        ("sa", sqltypes.FLOAT(), "FLOAT"),
        ("sa", sqltypes.REAL(), "REAL"),
        ("sa", sqltypes.DOUBLE(), "DOUBLE"),
        ("sa", sqltypes.DOUBLE_PRECISION(), "DOUBLE PRECISION"),
        ("pg", postgresql.FLOAT(), "FLOAT"),
        ("pg", postgresql.DOUBLE_PRECISION(), "DOUBLE PRECISION"),
        ("pg", postgresql.REAL(), "REAL"),
        id_="ira",
    )
    def test_float_type_compile(self, type_, sql_text):
        self.assert_compile(type_, sql_text)


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
            Column("w", postgresql.ARRAY(Double)),
            Column("q", postgresql.ARRAY(Numeric)),
        )
        metadata.create_all(connection)
        connection.execute(
            t1.insert(),
            dict(x=[5], y=[5], z=[6], w=[7], q=[decimal.Decimal("6.4")]),
        )
        row = connection.execute(t1.select()).first()
        eq_(row, ([5], [5], [6], [7], [decimal.Decimal("6.4")]))

    def test_arrays_base(self, connection, metadata):
        t1 = Table(
            "t",
            metadata,
            Column("x", sqltypes.ARRAY(Float)),
            Column("y", sqltypes.ARRAY(REAL)),
            Column("z", sqltypes.ARRAY(postgresql.DOUBLE_PRECISION)),
            Column("w", sqltypes.ARRAY(Double)),
            Column("q", sqltypes.ARRAY(Numeric)),
        )
        metadata.create_all(connection)
        connection.execute(
            t1.insert(),
            dict(x=[5], y=[5], z=[6], w=[7], q=[decimal.Decimal("6.4")]),
        )
        row = connection.execute(t1.select()).first()
        eq_(row, ([5], [5], [6], [7], [decimal.Decimal("6.4")]))


class NamedTypeTest(
    AssertsCompiledSQL, fixtures.TestBase, AssertsExecutionResults
):
    __backend__ = True

    __only_on__ = "postgresql > 8.3"

    def test_native_enum_warnings(self):
        """test #6106"""

        with testing.expect_warnings(
            "the native_enum flag does not apply to the "
            "sqlalchemy.dialects.postgresql.ENUM datatype;"
        ):
            e1 = postgresql.ENUM(
                "a", "b", "c", name="pgenum", native_enum=False
            )

        e2 = postgresql.ENUM("a", "b", "c", name="pgenum", native_enum=True)
        e3 = postgresql.ENUM("a", "b", "c", name="pgenum")

        is_(e1.native_enum, True)
        is_(e2.native_enum, True)
        is_(e3.native_enum, True)

    @testing.combinations(
        ("name", "foobar", "name"),
        ("validate_strings", True, "validate_strings"),
        ("omit_aliases", False, "_omit_aliases"),
        ("create_type", False, "create_type"),
        ("create_type", True, "create_type"),
        ("schema", "someschema", "schema"),
        ("inherit_schema", False, "inherit_schema"),
        ("metadata", MetaData(), "metadata"),
        ("values_callable", lambda x: None, "values_callable"),
    )
    def test_enum_copy_args(self, argname, value, attrname):
        kw = {argname: value}
        e1 = ENUM("a", "b", "c", **kw)

        e1_copy = e1.copy()

        eq_(getattr(e1_copy, attrname), value)

    def test_enum_create_table(self, metadata, connection):
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

    def test_domain_create_table(self, metadata, connection):
        metadata = self.metadata
        Email = DOMAIN(
            name="email",
            data_type=Text,
            check=r"VALUE ~ '[^@]+@[^@]+\.[^@]+'",
        )
        PosInt = DOMAIN(
            name="pos_int",
            data_type=Integer,
            not_null=True,
            check=r"VALUE > 0",
        )
        t1 = Table(
            "table",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("email", Email),
            Column("number", PosInt),
        )
        t1.create(connection)
        t1.create(connection, checkfirst=True)  # check the create
        connection.execute(
            t1.insert(), {"email": "test@example.com", "number": 42}
        )
        connection.execute(t1.insert(), {"email": "a@b.c", "number": 1})
        connection.execute(
            t1.insert(), {"email": "example@gmail.co.uk", "number": 99}
        )
        eq_(
            connection.execute(t1.select().order_by(t1.c.id)).fetchall(),
            [
                (1, "test@example.com", 42),
                (2, "a@b.c", 1),
                (3, "example@gmail.co.uk", 99),
            ],
        )

    @testing.combinations(
        (ENUM("one", "two", "three", name="mytype"), "get_enums"),
        (
            DOMAIN(
                name="mytype",
                data_type=Text,
                check=r"VALUE ~ '[^@]+@[^@]+\.[^@]+'",
            ),
            "get_domains",
        ),
        argnames="datatype, method",
    )
    def test_drops_on_table(
        self, connection, metadata, datatype: "NamedType", method
    ):
        table = Table("e1", metadata, Column("e1", datatype))

        table.create(connection)
        table.drop(connection)

        assert "mytype" not in [
            e["name"] for e in getattr(inspect(connection), method)()
        ]
        table.create(connection)
        assert "mytype" in [
            e["name"] for e in getattr(inspect(connection), method)()
        ]
        table.drop(connection)
        assert "mytype" not in [
            e["name"] for e in getattr(inspect(connection), method)()
        ]

    @testing.combinations(
        (
            lambda symbol_name: ENUM(
                "one", "two", "three", name="schema_mytype", schema=symbol_name
            ),
            ["two", "three", "three"],
            "get_enums",
        ),
        (
            lambda symbol_name: DOMAIN(
                name="schema_mytype",
                data_type=Text,
                check=r"VALUE ~ '[^@]+@[^@]+\.[^@]+'",
                schema=symbol_name,
            ),
            ["test@example.com", "a@b.c", "example@gmail.co.uk"],
            "get_domains",
        ),
        argnames="datatype,data,method",
    )
    @testing.combinations(None, "foo", argnames="symbol_name")
    def test_create_table_schema_translate_map(
        self, connection, symbol_name, datatype, data, method
    ):
        # note we can't use the fixture here because it will not drop
        # from the correct schema
        metadata = MetaData()

        dt = datatype(symbol_name)

        t1 = Table(
            "table",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("value", dt),
            schema=symbol_name,
        )

        execution_opts = {
            "schema_translate_map": {symbol_name: testing.config.test_schema}
        }

        if symbol_name is None:
            # we are adding/ removing None from the schema_translate_map across
            # runs, so we can't use caching else compiler will raise if it sees
            # an inconsistency here
            execution_opts["compiled_cache"] = None  # type: ignore

        conn = connection.execution_options(**execution_opts)
        t1.create(conn)
        assert "schema_mytype" in [
            e["name"]
            for e in getattr(inspect(conn), method)(
                schema=testing.config.test_schema
            )
        ]
        t1.create(conn, checkfirst=True)

        conn.execute(
            t1.insert(),
            dict(value=data[0]),
        )
        conn.execute(t1.insert(), dict(value=data[1]))
        conn.execute(t1.insert(), dict(value=data[2]))
        eq_(
            conn.execute(t1.select().order_by(t1.c.id)).fetchall(),
            [
                (1, data[0]),
                (2, data[1]),
                (3, data[2]),
            ],
        )

        t1.drop(conn)

        assert "schema_mytype" not in [
            e["name"]
            for e in getattr(inspect(conn), method)(
                schema=testing.config.test_schema
            )
        ]
        t1.drop(conn, checkfirst=True)

    @testing.combinations(
        ("inherit_schema_false",),
        ("inherit_schema_not_provided",),
        ("metadata_schema_only",),
        ("inherit_table_schema",),
        ("override_metadata_schema",),
        argnames="test_case",
    )
    @testing.combinations("enum", "domain", argnames="datatype")
    @testing.requires.schemas
    def test_schema_inheritance(
        self, test_case, metadata, connection, datatype
    ):
        """test #6373"""

        metadata.schema = testing.config.test_schema
        default_schema = testing.config.db.dialect.default_schema_name

        def make_type(**kw):
            if datatype == "enum":
                return Enum("four", "five", "six", name="mytype", **kw)
            elif datatype == "domain":
                return DOMAIN(
                    name="mytype",
                    data_type=Text,
                    check=r"VALUE ~ '[^@]+@[^@]+\.[^@]+'",
                    **kw,
                )
            else:
                assert False

        if test_case == "metadata_schema_only":
            enum = make_type(metadata=metadata)
            assert_schema = testing.config.test_schema
        elif test_case == "override_metadata_schema":
            enum = make_type(
                metadata=metadata,
                schema=testing.config.test_schema_2,
            )
            assert_schema = testing.config.test_schema_2
        elif test_case == "inherit_table_schema":
            enum = make_type(metadata=metadata, inherit_schema=True)
            assert_schema = testing.config.test_schema_2
        elif test_case == "inherit_schema_not_provided":
            enum = make_type()
            assert_schema = testing.config.test_schema_2
        elif test_case == "inherit_schema_false":
            enum = make_type(inherit_schema=False)
            assert_schema = default_schema
        else:
            assert False

        Table(
            "t",
            metadata,
            Column("data", enum),
            schema=testing.config.test_schema_2,
        )

        metadata.create_all(connection)

        if datatype == "enum":
            eq_(
                inspect(connection).get_enums(schema=assert_schema),
                [
                    {
                        "labels": ["four", "five", "six"],
                        "name": "mytype",
                        "schema": assert_schema,
                        "visible": assert_schema == default_schema,
                    }
                ],
            )
        elif datatype == "domain":
            eq_(
                inspect(connection).get_domains(schema=assert_schema),
                [
                    {
                        "name": "mytype",
                        "type": "text",
                        "nullable": True,
                        "default": None,
                        "schema": assert_schema,
                        "visible": assert_schema == default_schema,
                        "constraints": [
                            {
                                "name": "mytype_check",
                                "check": r"VALUE ~ '[^@]+@[^@]+\.[^@]+'::text",
                            }
                        ],
                        "collation": "default",
                    }
                ],
            )
        else:
            assert False

    @testing.variation("name", ["noname", "nonename", "explicit_name"])
    @testing.variation("enum_type", ["pg", "plain"])
    def test_native_enum_string_from_pep435(self, name, enum_type):
        """test #9611"""

        class MyEnum(_PY_Enum):
            one = "one"
            two = "two"

        if enum_type.plain:
            cls = Enum
        elif enum_type.pg:
            cls = ENUM
        else:
            enum_type.fail()

        if name.noname:
            e1 = cls(MyEnum)
            eq_(e1.name, "myenum")
        elif name.nonename:
            e1 = cls(MyEnum, name=None)
            eq_(e1.name, None)
        elif name.explicit_name:
            e1 = cls(MyEnum, name="abc")
            eq_(e1.name, "abc")

    @testing.variation("backend_type", ["native", "non_native", "pg_native"])
    @testing.variation("enum_type", ["pep435", "str"])
    def test_compare_to_string_round_trip(
        self, connection, backend_type, enum_type, metadata
    ):
        """test #9621"""

        if enum_type.pep435:

            class MyEnum(_PY_Enum):
                one = "one"
                two = "two"

            if backend_type.pg_native:
                typ = ENUM(MyEnum, name="myenum2")
            else:
                typ = Enum(
                    MyEnum,
                    native_enum=bool(backend_type.native),
                    name="myenum2",
                )
            data = [{"someenum": MyEnum.one}, {"someenum": MyEnum.two}]
            expected = MyEnum.two
        elif enum_type.str:
            if backend_type.pg_native:
                typ = ENUM("one", "two", name="myenum2")
            else:
                typ = Enum(
                    "one",
                    "two",
                    native_enum=bool(backend_type.native),
                    name="myenum2",
                )
            data = [{"someenum": "one"}, {"someenum": "two"}]
            expected = "two"
        else:
            enum_type.fail()

        enum_table = Table(
            "et2",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("someenum", typ),
        )
        metadata.create_all(connection)

        connection.execute(insert(enum_table), data)
        expr = select(enum_table.c.someenum).where(
            enum_table.c.someenum == "two"
        )

        row = connection.execute(expr).one()
        eq_(row, (expected,))

    @testing.combinations(
        (Enum("one", "two", "three")),
        (ENUM("one", "two", "three", name=None)),
        (
            DOMAIN(
                name=None,
                data_type=Text,
                check=r"VALUE ~ '[^@]+@[^@]+\.[^@]+'",
            ),
        ),
        argnames="datatype",
    )
    def test_name_required(self, metadata, connection, datatype):
        assert_raises(exc.CompileError, datatype.create, connection)
        assert_raises(
            exc.CompileError, datatype.compile, dialect=connection.dialect
        )

    def test_enum_doesnt_construct_ENUM(self):
        """in 2.0 we made ENUM name required.   check that Enum adapt to
        ENUM doesnt call this constructor."""

        e1 = Enum("x", "y")
        eq_(e1.name, None)
        e2 = e1.adapt(ENUM)
        eq_(e2.name, None)

        # no name
        assert_raises(
            exc.CompileError, e2.compile, dialect=postgresql.dialect()
        )

    def test_py_enum_name_is_used(self):
        class MyEnum(_PY_Enum):
            x = "1"
            y = "2"

        e1 = Enum(MyEnum)
        eq_(e1.name, "myenum")
        e2 = e1.adapt(ENUM)

        # note that by making "name" required, we are now not supporting this:
        # e2 = ENUM(MyEnum)
        # they'd need ENUM(MyEnum, name="myenum")
        # I might be OK with that.   Use of pg.ENUM directly is not as
        # common and it suggests more explicitness on the part of the
        # programmer in any case

        eq_(e2.name, "myenum")

        self.assert_compile(e2, "myenum")

    def test_enum_unicode_labels(self, connection, metadata):
        t1 = Table(
            "table",
            metadata,
            Column("id", Integer, primary_key=True),
            Column(
                "value",
                Enum(
                    "réveillé",
                    "drôle",
                    "S’il",
                    name="onetwothreetype",
                ),
            ),
        )
        metadata.create_all(connection)
        connection.execute(t1.insert(), dict(value="drôle"))
        connection.execute(t1.insert(), dict(value="réveillé"))
        connection.execute(t1.insert(), dict(value="S’il"))
        eq_(
            connection.execute(t1.select().order_by(t1.c.id)).fetchall(),
            [
                (1, "drôle"),
                (2, "réveillé"),
                (3, "S’il"),
            ],
        )
        m2 = MetaData()
        t2 = Table("table", m2, autoload_with=connection)
        eq_(
            t2.c.value.type.enums,
            ["réveillé", "drôle", "S’il"],
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
                    "Ü",
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
                    "VARCHAR(1), \tCONSTRAINT myenum CHECK "
                    "(bar IN ('B', 'Ü')))",
                    {},
                )
            ],
        )

        connection.execute(t1.insert(), {"bar": "Ü"})
        eq_(connection.scalar(select(t1.c.bar)), "Ü")

    @testing.combinations(
        (ENUM("one", "two", "three", name="mytype", create_type=False),),
        (
            DOMAIN(
                name="mytype",
                data_type=Text,
                check=r"VALUE ~ '[^@]+@[^@]+\.[^@]+'",
                create_type=False,
            ),
        ),
        argnames="datatype",
    )
    def test_disable_create(self, metadata, connection, datatype):
        metadata = self.metadata

        t1 = Table("e1", metadata, Column("c1", datatype))
        # table can be created separately
        # without conflict
        datatype.create(bind=connection)
        t1.create(connection)
        t1.drop(connection)
        datatype.drop(bind=connection)

    def test_enum_dont_keep_checking(self, metadata, connection):
        metadata = self.metadata

        e1 = postgresql.ENUM("one", "two", "three", name="myenum")

        Table("t", metadata, Column("a", e1), Column("b", e1), Column("c", e1))

        with self.sql_execution_asserter(connection) as asserter:
            metadata.create_all(connection)

        asserter.assert_(
            # check for table
            RegexSQL(
                "SELECT pg_catalog.pg_class.relname FROM pg_catalog."
                "pg_class JOIN pg_catalog.pg_namespace.*",
                dialect="postgresql",
            ),
            # check for enum, just once
            RegexSQL(
                r"SELECT pg_catalog.pg_type.typname .* WHERE "
                "pg_catalog.pg_type.typname = ",
                dialect="postgresql",
            ),
            RegexSQL("CREATE TYPE myenum AS ENUM .*", dialect="postgresql"),
            RegexSQL(r"CREATE TABLE t .*", dialect="postgresql"),
        )

        with self.sql_execution_asserter(connection) as asserter:
            metadata.drop_all(connection)

        asserter.assert_(
            RegexSQL(
                "SELECT pg_catalog.pg_class.relname FROM pg_catalog."
                "pg_class JOIN pg_catalog.pg_namespace.*",
                dialect="postgresql",
            ),
            RegexSQL("DROP TABLE t", dialect="postgresql"),
            RegexSQL(
                r"SELECT pg_catalog.pg_type.typname .* WHERE "
                "pg_catalog.pg_type.typname = ",
                dialect="postgresql",
            ),
            RegexSQL("DROP TYPE myenum", dialect="postgresql"),
        )

    @testing.combinations(
        (
            Enum(
                "one",
                "two",
                "three",
                name="mytype",
            ),
            "get_enums",
        ),
        (
            ENUM(
                "one",
                "two",
                "three",
                name="mytype",
            ),
            "get_enums",
        ),
        (
            DOMAIN(
                name="mytype",
                data_type=Text,
                check=r"VALUE ~ '[^@]+@[^@]+\.[^@]+'",
            ),
            "get_domains",
        ),
        argnames="datatype, method",
    )
    def test_generate_multiple(self, metadata, connection, datatype, method):
        """Test that the same enum twice only generates once
        for the create_all() call, without using checkfirst.

        A 'memo' collection held by the DDL runner
        now handles this.

        """
        Table("e1", metadata, Column("c1", datatype))

        Table("e2", metadata, Column("c1", datatype))

        metadata.create_all(connection, checkfirst=False)

        assert "mytype" in [
            e["name"] for e in getattr(inspect(connection), method)()
        ]

        metadata.drop_all(connection, checkfirst=False)

        assert "mytype" not in [
            e["name"] for e in getattr(inspect(connection), method)()
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

    def test_enum_type_reflection(self, metadata, connection):
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
            cache_ok = True

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

    @testing.combinations(
        (
            Enum(
                "one",
                "two",
                "three",
                native_enum=True,  # make sure this is True because
                # it should *not* take effect due to
                # the variant
            ).with_variant(
                postgresql.ENUM("four", "five", "six", name="my_enum"),
                "postgresql",
            )
        ),
        (
            String(50).with_variant(
                postgresql.ENUM("four", "five", "six", name="my_enum"),
                "postgresql",
            )
        ),
        argnames="datatype",
    )
    def test_generic_w_pg_variant(self, metadata, connection, datatype):
        some_table = Table(
            "some_table",
            self.metadata,
            Column("data", datatype),
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


class DomainTest(
    AssertsCompiledSQL, fixtures.TestBase, AssertsExecutionResults
):
    __backend__ = True
    __only_on__ = "postgresql > 8.3"

    @testing.requires.postgresql_working_nullable_domains
    def test_domain_type_reflection(self, metadata, connection):
        positive_int = DOMAIN(
            "positive_int", Integer(), check="value > 0", not_null=True
        )
        my_str = DOMAIN("my_string", Text(), collation="C", default="~~")
        Table(
            "table",
            metadata,
            Column("value", positive_int),
            Column("str", my_str),
        )

        metadata.create_all(connection)
        m2 = MetaData()
        t2 = Table("table", m2, autoload_with=connection)

        vt = t2.c.value.type
        is_true(isinstance(vt, DOMAIN))
        is_true(isinstance(vt.data_type, Integer))
        eq_(vt.name, "positive_int")
        eq_(str(vt.check), "VALUE > 0")
        is_(vt.default, None)
        is_(vt.collation, None)
        is_true(vt.constraint_name is not None)
        is_true(vt.not_null)
        is_false(vt.create_type)

        st = t2.c.str.type
        is_true(isinstance(st, DOMAIN))
        is_true(isinstance(st.data_type, Text))
        eq_(st.name, "my_string")
        is_(st.check, None)
        is_true("~~" in st.default)
        eq_(st.collation, "C")
        is_(st.constraint_name, None)
        is_false(st.not_null)
        is_false(st.create_type)

    def test_domain_create_table(self, metadata, connection):
        metadata = self.metadata
        Email = DOMAIN(
            name="email",
            data_type=Text,
            check=r"VALUE ~ '[^@]+@[^@]+\.[^@]+'",
        )
        PosInt = DOMAIN(
            name="pos_int",
            data_type=Integer,
            not_null=True,
            check=r"VALUE > 0",
        )
        t1 = Table(
            "table",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("email", Email),
            Column("number", PosInt),
        )
        t1.create(connection)
        t1.create(connection, checkfirst=True)  # check the create
        connection.execute(
            t1.insert(), {"email": "test@example.com", "number": 42}
        )
        connection.execute(t1.insert(), {"email": "a@b.c", "number": 1})
        connection.execute(
            t1.insert(), {"email": "example@gmail.co.uk", "number": 99}
        )
        eq_(
            connection.execute(t1.select().order_by(t1.c.id)).fetchall(),
            [
                (1, "test@example.com", 42),
                (2, "a@b.c", 1),
                (3, "example@gmail.co.uk", 99),
            ],
        )

    @testing.combinations(
        tuple(
            [
                DOMAIN(
                    name="mytype",
                    data_type=Text,
                    check=r"VALUE ~ '[^@]+@[^@]+\.[^@]+'",
                    create_type=True,
                ),
            ]
        ),
        tuple(
            [
                DOMAIN(
                    name="mytype",
                    data_type=Text,
                    check=r"VALUE ~ '[^@]+@[^@]+\.[^@]+'",
                    create_type=False,
                ),
            ]
        ),
        argnames="domain",
    )
    def test_create_drop_domain_with_table(self, connection, metadata, domain):
        table = Table("e1", metadata, Column("e1", domain))

        def _domain_names():
            return {d["name"] for d in inspect(connection).get_domains()}

        assert "mytype" not in _domain_names()

        if domain.create_type:
            table.create(connection)
            assert "mytype" in _domain_names()
        else:
            with expect_raises(exc.ProgrammingError):
                table.create(connection)
            connection.rollback()

            domain.create(connection)
            assert "mytype" in _domain_names()
            table.create(connection)

        table.drop(connection)
        if domain.create_type:
            assert "mytype" not in _domain_names()

    @testing.combinations(
        (Integer, "value > 0", 4),
        (String, "value != ''", "hello world"),
        (
            UUID,
            "value != '{00000000-0000-0000-0000-000000000000}'",
            uuid.uuid4(),
        ),
        (
            DateTime,
            "value >= '2020-01-01T00:00:00'",
            datetime.datetime.fromisoformat("2021-01-01T00:00:00.000"),
        ),
        argnames="domain_datatype, domain_check, value",
    )
    def test_domain_roundtrip(
        self, metadata, connection, domain_datatype, domain_check, value
    ):
        table = Table(
            "domain_roundtrip_test",
            metadata,
            Column("id", Integer, primary_key=True),
            Column(
                "value",
                DOMAIN("valuedomain", domain_datatype, check=domain_check),
            ),
        )
        table.create(connection)

        connection.execute(table.insert(), {"value": value})

        results = connection.execute(
            table.select().order_by(table.c.id)
        ).fetchall()
        eq_(results, [(1, value)])

    @testing.combinations(
        (DOMAIN("pos_int", Integer, check="VALUE > 0", not_null=True), 4, -4),
        (
            DOMAIN("email", String, check=r"VALUE ~ '[^@]+@[^@]+\.[^@]+'"),
            "e@xample.com",
            "fred",
        ),
        argnames="domain,pass_value,fail_value",
    )
    def test_check_constraint(
        self, metadata, connection, domain, pass_value, fail_value
    ):
        table = Table("table", metadata, Column("value", domain))
        table.create(connection)

        connection.execute(table.insert(), {"value": pass_value})

        # psycopg/psycopg2 raise IntegrityError, while pg8000 raises
        # ProgrammingError
        with expect_raises(exc.DatabaseError):
            connection.execute(table.insert(), {"value": fail_value})

    @testing.combinations(
        (DOMAIN("nullable_domain", Integer, not_null=True), 1),
        (DOMAIN("non_nullable_domain", Integer, not_null=False), 1),
        argnames="domain,pass_value",
    )
    def test_domain_nullable(self, metadata, connection, domain, pass_value):
        table = Table("table", metadata, Column("value", domain))
        table.create(connection)
        connection.execute(table.insert(), {"value": pass_value})

        if domain.not_null:
            # psycopg/psycopg2 raise IntegrityError, while pg8000 raises
            # ProgrammingError
            with expect_raises(exc.DatabaseError):
                connection.execute(table.insert(), {"value": None})
        else:
            connection.execute(table.insert(), {"value": None})


class DomainDDLEventTest(DDLEventWCreateHarness, fixtures.TestBase):
    __backend__ = True

    __only_on__ = "postgresql > 8.3"

    creates_implicitly_with_table = False
    drops_implicitly_with_table = False
    requires_table_to_exist = False

    @testing.fixture
    def produce_subject(self):
        return DOMAIN(
            name="email",
            data_type=Text,
            check=r"VALUE ~ '[^@]+@[^@]+\.[^@]+'",
        )

    @testing.fixture
    def produce_table_integrated_subject(self, metadata, produce_subject):
        return Table(
            "table",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("email", produce_subject),
        )


class EnumDDLEventTest(DDLEventWCreateHarness, fixtures.TestBase):
    __backend__ = True

    __only_on__ = "postgresql > 8.3"

    creates_implicitly_with_table = False
    drops_implicitly_with_table = False
    requires_table_to_exist = False

    @testing.fixture
    def produce_subject(self):
        return Enum(
            "x",
            "y",
            "z",
            name="status",
        )

    @testing.fixture
    def produce_event_target(self, produce_subject, connection):
        return produce_subject.dialect_impl(connection.dialect)

    @testing.fixture
    def produce_table_integrated_subject(self, metadata, produce_subject):
        return Table(
            "table",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("status", produce_subject),
        )


class NativeEnumDDLEventTest(EnumDDLEventTest):
    @testing.fixture
    def produce_event_target(self, produce_subject, connection):
        return produce_subject

    @testing.fixture
    def produce_subject(self):
        return ENUM(
            "x",
            "y",
            "z",
            name="status",
        )


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
        dialects = (
            pg8000.dialect(),
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
            tztable.update()
            .where(tztable.c.id == 1)
            .returning(tztable.c.date),
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
            notztable.update()
            .where(notztable.c.id == 1)
            .returning(notztable.c.date),
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

        eq_(t1.c.c1.type.__class__, postgresql.TIME)
        eq_(t1.c.c4.type.__class__, postgresql.TIMESTAMP)

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

    def test_array_in_enum_psycopg2_cast(self):
        expr = column(
            "x",
            postgresql.ARRAY(
                postgresql.ENUM("one", "two", "three", name="myenum")
            ),
        ).in_([["one", "two"], ["three", "four"]])

        self.assert_compile(
            expr,
            "x IN (__[POSTCOMPILE_x_1])",
            dialect=postgresql.psycopg2.dialect(),
        )

        self.assert_compile(
            expr,
            "x IN (%(x_1_1)s::myenum[], %(x_1_2)s::myenum[])",
            dialect=postgresql.psycopg2.dialect(),
            render_postcompile=True,
        )

    def test_array_literal_render_no_inner_render(self):
        class MyType(UserDefinedType):
            cache_ok = True

            def get_col_spec(self, **kw):
                return "MYTYPE"

        with expect_raises_message(
            exc.CompileError,
            r"No literal value renderer is available for literal "
            r"value \"\[1, 2, 3\]\" with datatype ARRAY",
        ):
            self.assert_compile(
                select(literal([1, 2, 3], ARRAY(MyType()))),
                "nothing",
                literal_binds=True,
            )

    def test_array_in_str_psycopg2_cast(self):
        expr = column("x", postgresql.ARRAY(String(15))).in_(
            [["one", "two"], ["three", "four"]]
        )

        self.assert_compile(
            expr,
            "x IN (__[POSTCOMPILE_x_1])",
            dialect=postgresql.psycopg2.dialect(),
        )

        self.assert_compile(
            expr,
            "x IN (%(x_1_1)s::VARCHAR(15)[], %(x_1_2)s::VARCHAR(15)[])",
            dialect=postgresql.psycopg2.dialect(),
            render_postcompile=True,
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

    def test_array_deprecated_any(self):
        col = column("x", postgresql.ARRAY(Integer))

        with _array_any_deprecation():
            self.assert_compile(
                select(col.any(7, operator=operators.lt)),
                "SELECT %(x_1)s < ANY (x) AS anon_1",
                checkparams={"x_1": 7},
            )

    def test_array_deprecated_all(self):
        col = column("x", postgresql.ARRAY(Integer))

        with _array_any_deprecation():
            self.assert_compile(
                select(col.all(7, operator=operators.lt)),
                "SELECT %(x_1)s < ALL (x) AS anon_1",
                checkparams={"x_1": 7},
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

    def test_array_overlap_any(self):
        col = column("x", postgresql.ARRAY(Integer))
        self.assert_compile(
            select(col.overlap(any_(array([4, 5, 6])))),
            "SELECT x && ANY (ARRAY[%(param_1)s, %(param_2)s, %(param_3)s]) "
            "AS anon_1",
            checkparams={"param_1": 4, "param_3": 6, "param_2": 5},
        )

    def test_array_contains_any(self):
        col = column("x", postgresql.ARRAY(Integer))
        self.assert_compile(
            select(col.contains(any_(array([4, 5, 6])))),
            "SELECT x @> ANY (ARRAY[%(param_1)s, %(param_2)s, %(param_3)s]) "
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
        ("original", False),
        ("just_enum", True),
        ("just_order_by", False),
        ("issue_5989", True),
        id_="ia",
        argnames="with_enum",
    )
    @testing.variation("order_by_type", ["none", "legacy", "core"])
    def test_array_agg_specific(self, with_enum, order_by_type):
        element = ENUM(name="pgenum") if with_enum else Integer()
        element_type = type(element)

        if order_by_type.none:
            expr = array_agg(column("q", element))
        elif order_by_type.legacy:
            expr = array_agg(
                aggregate_order_by(
                    column("q", element), column("idx", Integer)
                )
            )
        elif order_by_type.core:
            expr = array_agg(column("q", element)).aggregate_order_by(
                column("idx", Integer)
            )

        is_(expr.type.__class__, postgresql.ARRAY)
        is_(expr.type.item_type.__class__, element_type)


AnEnum = pep435_enum("AnEnum")
AnEnum("Foo", 1)
AnEnum("Bar", 2)
AnEnum("Baz", 3)


class ArrayRoundTripTest:
    __only_on__ = "postgresql"
    __backend__ = True

    ARRAY = postgresql.ARRAY

    @classmethod
    def define_tables(cls, metadata):
        class ProcValue(TypeDecorator):
            impl = cls.ARRAY(Integer, dimensions=2)
            cache_ok = True

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
            Column("data", self.ARRAY(String(50, collation="en_US"))),
        )

        t.create(connection)

    @testing.fixture
    def array_in_fixture(self, connection):
        arrtable = self.tables.arrtable

        connection.execute(
            arrtable.insert(),
            [
                {
                    "id": 1,
                    "intarr": [1, 2, 3],
                    "strarr": ["one", "two", "three"],
                },
                {
                    "id": 2,
                    "intarr": [4, 5, 6],
                    "strarr": ["four", "five", "six"],
                },
                {"id": 3, "intarr": [1, 5], "strarr": ["one", "five"]},
                {"id": 4, "intarr": [], "strarr": []},
            ],
        )

    def test_array_in_int(self, array_in_fixture, connection):
        """test #7177"""

        arrtable = self.tables.arrtable

        stmt = (
            select(arrtable.c.intarr)
            .where(arrtable.c.intarr.in_([[1, 5], [4, 5, 6], [9, 10]]))
            .order_by(arrtable.c.id)
        )

        eq_(
            connection.execute(stmt).all(),
            [
                ([4, 5, 6],),
                ([1, 5],),
            ],
        )

    def test_array_in_str(self, array_in_fixture, connection):
        """test #7177"""

        arrtable = self.tables.arrtable

        stmt = (
            select(arrtable.c.strarr)
            .where(
                arrtable.c.strarr.in_(
                    [
                        ["one", "five"],
                        ["four", "five", "six"],
                        ["nine", "ten"],
                    ]
                )
            )
            .order_by(arrtable.c.id)
        )

        eq_(
            connection.execute(stmt).all(),
            [
                (["four", "five", "six"],),
                (["one", "five"],),
            ],
        )

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

    def test_array_agg_json(self, metadata, connection):
        table = Table(
            "values", metadata, Column("id", Integer), Column("bar", JSON)
        )
        metadata.create_all(connection)
        connection.execute(
            table.insert(),
            [{"id": 1, "bar": [{"buz": 1}]}, {"id": 2, "bar": None}],
        )

        arg = aggregate_order_by(table.c.bar, table.c.id)
        stmt = select(sa.func.array_agg(arg))
        eq_(connection.execute(stmt).scalar(), [[{"buz": 1}], None])

        arg = aggregate_order_by(table.c.bar, table.c.id.desc())
        stmt = select(sa.func.array_agg(arg))
        eq_(connection.execute(stmt).scalar(), [None, [{"buz": 1}]])

    @testing.combinations(ARRAY, postgresql.ARRAY, argnames="cls")
    def test_array_none(self, connection, metadata, cls):
        table = Table(
            "values", metadata, Column("id", Integer), Column("bar", cls(JSON))
        )
        metadata.create_all(connection)
        connection.execute(
            table.insert().values(
                [
                    {
                        "id": 1,
                        "bar": sa.text("""array['[{"x": 1}]'::json, null]"""),
                    },
                    {"id": 2, "bar": None},
                ]
            )
        )

        stmt = select(table.c.bar).order_by(table.c.id)
        eq_(connection.scalars(stmt).all(), [[[{"x": 1}], None], None])

        stmt = select(table.c.bar).order_by(table.c.id.desc())
        eq_(connection.scalars(stmt).all(), [None, [[{"x": 1}], None]])

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
                strarr=["abc", "def"],
            ),
        )
        results = connection.execute(arrtable.select()).fetchall()
        eq_(len(results), 1)
        eq_(results[0].intarr, [1, 2, 3])
        eq_(results[0].strarr, ["abc", "def"])

    def test_insert_array_w_null(self, connection):
        arrtable = self.tables.arrtable
        connection.execute(
            arrtable.insert(),
            dict(
                intarr=[1, None, 3],
                strarr=["abc", None],
            ),
        )
        results = connection.execute(arrtable.select()).fetchall()
        eq_(len(results), 1)
        eq_(results[0].intarr, [1, None, 3])
        eq_(results[0].strarr, ["abc", None])

    def test_array_where(self, connection):
        arrtable = self.tables.arrtable
        connection.execute(
            arrtable.insert(),
            dict(
                intarr=[1, 2, 3],
                strarr=["abc", "def"],
            ),
        )
        connection.execute(
            arrtable.insert(), dict(intarr=[4, 5, 6], strarr="ABC")
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
            dict(intarr=[1, 2, 3], strarr=["abc", "def"]),
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
            dict(id=5, intarr=[1, 2, 3], strarr=["abc", "def"]),
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
                strarr=[["m\xe4\xe4"], ["m\xf6\xf6"]],
            ),
        )
        connection.execute(
            arrtable.insert(),
            dict(
                intarr=[1, 2, 3],
                strarr=["m\xe4\xe4", "m\xf6\xf6"],
            ),
        )
        results = connection.execute(
            arrtable.select().order_by(arrtable.c.intarr)
        ).fetchall()
        eq_(len(results), 2)
        eq_(results[0].strarr, ["m\xe4\xe4", "m\xf6\xf6"])
        eq_(
            results[1].strarr,
            [["m\xe4\xe4"], ["m\xf6\xf6"]],
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
                strarr=["abc", "def"],
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
                select(arrtable.c.intarr).where(5 == any_(arrtable.c.intarr))
            ),
            [4, 5, 6],
        )

    def test_array_all_exec(self, connection):
        arrtable = self.tables.arrtable
        connection.execute(arrtable.insert(), dict(intarr=[4, 5, 6]))

        eq_(
            connection.scalar(
                select(arrtable.c.intarr).where(4 <= all_(arrtable.c.intarr))
            ),
            [4, 5, 6],
        )

    def test_array_any_deprecated_exec(self, connection):
        arrtable = self.tables.arrtable
        connection.execute(arrtable.insert(), dict(intarr=[4, 5, 6]))

        with _array_any_deprecation():
            eq_(
                connection.scalar(
                    select(arrtable.c.intarr).where(
                        postgresql.Any(5, arrtable.c.intarr)
                    )
                ),
                [4, 5, 6],
            )

    def test_array_all_deprecated_exec(self, connection):
        arrtable = self.tables.arrtable
        connection.execute(arrtable.insert(), dict(intarr=[4, 5, 6]))

        with _array_any_deprecation():
            eq_(
                connection.scalar(
                    select(arrtable.c.intarr).where(
                        postgresql.All(
                            4, arrtable.c.intarr, operator=operators.le
                        )
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
            {row[1] for row in r},
            {("1", "2", "3"), ("4", "5", "6"), (("4", "5"), ("6", "7"))},
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
            {e["name"] for e in inspect(connection).get_enums()},
            {"my_enum_1", "my_enum_2", "my_enum_3"},
        )
        t.drop(connection)
        eq_(inspect(connection).get_enums(), [])

    def _type_combinations(
        exclude_json=False,
        exclude_empty_lists=False,
        exclude_arrays_with_none=False,
    ):
        def str_values(x):
            return ["one", "two: %s" % x, "three", "four", "five"]

        def unicode_values(x):
            return [
                "réveillé",
                "drôle",
                "S’il %s" % x,
                "🐍 %s" % x,
                "« S’il vous",
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

        def empty_list(x):
            return []

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

        simple_enum = ["one", "two", "three", "four", "five", "six"]
        difficult_enum = [
            "Value",
            "With space",
            "With,comma",
            "NULL",
            'With"quote',
            "With\\escape",
            """Various!@#$%^*()"'\\][{};:.<>|_+~chars""",
        ]

        def make_enum(cls_, members, native):
            return cls_(*members, name="difficult_enum", native_enum=native)

        def make_enum_values(members, x, *, include_none=False):
            arr = [v for i, v in enumerate(members) if i != x - 1]
            if include_none:
                arr.insert(2, None)
            return arr

        elements = [
            (sqltypes.Integer, lambda x: [1, x, 3, 4, 5]),
            (sqltypes.Text, str_values),
            (sqltypes.String, str_values),
            (sqltypes.Unicode, unicode_values),
            (postgresql.JSONB, json_values),
            (sqltypes.Boolean, lambda x: [False] + [True] * x),
            (sqltypes.LargeBinary, binary_values),
            (postgresql.BYTEA, binary_values),
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
            (postgresql.ENUM(AnEnum, name="pgenum"), enum_values),
            (sqltypes.Enum(AnEnum, native_enum=True), enum_values),
            (sqltypes.Enum(AnEnum, native_enum=False), enum_values),
            (
                postgresql.ENUM(AnEnum, name="pgenum", native_enum=True),
                enum_values,
            ),
            (
                make_enum(sqltypes.Enum, difficult_enum, native=True),
                functools.partial(make_enum_values, difficult_enum),
            ),
            (
                make_enum(sqltypes.Enum, difficult_enum, native=False),
                functools.partial(make_enum_values, difficult_enum),
            ),
            (
                make_enum(postgresql.ENUM, difficult_enum, native=True),
                functools.partial(make_enum_values, difficult_enum),
            ),
        ]

        if not exclude_arrays_with_none:
            elements.extend(
                [
                    (
                        # unquoted ENUM values including NULL in the data
                        make_enum(sqltypes.Enum, simple_enum, native=True),
                        functools.partial(
                            make_enum_values, simple_enum, include_none=True
                        ),
                    ),
                    (
                        # unquoted ENUM values including NULL in the data
                        make_enum(sqltypes.Enum, simple_enum, native=False),
                        functools.partial(
                            make_enum_values, simple_enum, include_none=True
                        ),
                    ),
                    (
                        # unquoted ENUM values including NULL in the data
                        make_enum(postgresql.ENUM, simple_enum, native=True),
                        functools.partial(
                            make_enum_values, simple_enum, include_none=True
                        ),
                    ),
                    (
                        # quoted ENUM values, including both
                        # quoted "NULL" and real NULL in the data
                        make_enum(sqltypes.Enum, difficult_enum, native=True),
                        functools.partial(
                            make_enum_values, difficult_enum, include_none=True
                        ),
                    ),
                    (
                        # quoted ENUM values, including both
                        # quoted "NULL" and real NULL in the data
                        make_enum(sqltypes.Enum, difficult_enum, native=False),
                        functools.partial(
                            make_enum_values, difficult_enum, include_none=True
                        ),
                    ),
                    (
                        # quoted ENUM values, including both
                        # quoted "NULL" and real NULL in the data
                        make_enum(
                            postgresql.ENUM, difficult_enum, native=True
                        ),
                        functools.partial(
                            make_enum_values, difficult_enum, include_none=True
                        ),
                    ),
                ]
            )
        if not exclude_empty_lists:
            elements.extend(
                [
                    (postgresql.ENUM(AnEnum, name="pgenum"), empty_list),
                    (sqltypes.Enum(AnEnum, native_enum=True), empty_list),
                    (sqltypes.Enum(AnEnum, native_enum=False), empty_list),
                    (
                        postgresql.ENUM(
                            AnEnum, name="pgenum", native_enum=True
                        ),
                        empty_list,
                    ),
                ]
            )
        if not exclude_json:
            elements.extend(
                [
                    (sqltypes.JSON, json_values),
                    (postgresql.JSON, json_values),
                ]
            )

        _pg8000_skip_types = {
            postgresql.HSTORE,  # return not parsed returned as string
        }
        for i in range(len(elements)):
            elem = elements[i]
            if (
                elem[0] in _pg8000_skip_types
                or type(elem[0]) in _pg8000_skip_types
            ):
                elem += (
                    testing.skip_if(
                        "postgresql+pg8000", "type not supported by pg8000"
                    ),
                )
                elements[i] = elem

        return testing.combinations_list(
            elements, argnames="type_,generate_data", id_="na"
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

        def go(generate_data):
            connection.execute(
                table.insert(),
                [
                    {"id": 1, "bar": generate_data(1)},
                    {"id": 2, "bar": generate_data(2)},
                ],
            )
            return table

        return go

    @_type_combinations()
    def test_type_specific_value_select(
        self, type_specific_fixture, connection, type_, generate_data
    ):
        table = type_specific_fixture(generate_data)

        rows = connection.execute(
            select(table.c.bar).order_by(table.c.id)
        ).all()

        eq_(rows, [(generate_data(1),), (generate_data(2),)])

    @_type_combinations()
    def test_type_specific_value_update(
        self, type_specific_fixture, connection, type_, generate_data
    ):
        table = type_specific_fixture(generate_data)

        new_gen = generate_data(3)
        connection.execute(
            table.update().where(table.c.id == 2).values(bar=new_gen)
        )

        eq_(
            new_gen,
            connection.scalar(select(table.c.bar).where(table.c.id == 2)),
        )

    @_type_combinations(exclude_empty_lists=True)
    def test_type_specific_slice_update(
        self, type_specific_fixture, connection, type_, generate_data
    ):
        table = type_specific_fixture(generate_data)

        new_gen = generate_data(3)

        if not table.c.bar.type._variant_mapping:
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

        sliced_gen = generate_data(2)
        sliced_gen[0:3] = new_gen[1:4]

        eq_(rows, [(generate_data(1),), (sliced_gen,)])

    @_type_combinations(exclude_json=True, exclude_empty_lists=True)
    def test_type_specific_value_delete(
        self, type_specific_fixture, connection, type_, generate_data
    ):
        table = type_specific_fixture(generate_data)

        new_gen = generate_data(2)

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

    @ArrayRoundTripTest._cls_type_combinations(
        exclude_json=True, exclude_arrays_with_none=True
    )
    def test_type_specific_contains(
        self, type_specific_fixture, connection, type_, generate_data
    ):
        table = type_specific_fixture(generate_data)

        connection.execute(
            table.insert(),
            [
                {"id": 1, "bar": generate_data(1)},
                {"id": 2, "bar": generate_data(2)},
            ],
        )

        id_, value = connection.execute(
            select(table).where(table.c.bar.contains(generate_data(1)))
        ).first()
        eq_(id_, 1)
        eq_(value, generate_data(1))

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
    cache_ok = True

    # note expanding logic is checking _is_array here so that has to
    # translate through the TypeDecorator

    def bind_expression(self, bindvalue):
        return sa.cast(bindvalue, self)

    def result_processor(self, dialect, coltype):
        super_rp = super().result_processor(dialect, coltype)

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
    def test_raises_non_native_enums(self, metadata, connection, array_cls):
        enum_cls = sqltypes.Enum

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

    @testing.fixture
    def array_of_enum_fixture(self, metadata, connection):
        def go(array_cls, enum_cls):
            class MyEnum(_PY_Enum):
                a = "aaa"
                b = "bbb"
                c = "ccc"

            tbl = Table(
                "enum_table",
                metadata,
                Column("id", Integer, primary_key=True),
                Column(
                    "enum_col",
                    array_cls(enum_cls("foo", "bar", "baz", name="an_enum")),
                ),
                Column(
                    "pyenum_col",
                    array_cls(enum_cls(MyEnum, name="pgenum")),
                ),
            )

            metadata.create_all(connection)
            connection.execute(
                tbl.insert(),
                [
                    {"enum_col": ["foo"], "pyenum_col": [MyEnum.a, MyEnum.b]},
                    {"enum_col": ["foo", "bar"], "pyenum_col": [MyEnum.b]},
                ],
            )
            return tbl, MyEnum

        yield go

    def _enum_combinations(fn):
        return testing.combinations(
            sqltypes.Enum, postgresql.ENUM, argnames="enum_cls"
        )(
            testing.combinations(
                sqltypes.ARRAY,
                postgresql.ARRAY,
                (_ArrayOfEnum, testing.requires.any_psycopg_compatibility),
                argnames="array_cls",
            )(fn)
        )

    @_enum_combinations
    @testing.combinations("all", "any", argnames="fn")
    def test_any_all_roundtrip(
        self, array_of_enum_fixture, connection, array_cls, enum_cls, fn
    ):
        """test for #12874. originally from the legacy use case in #6515"""

        tbl, MyEnum = array_of_enum_fixture(array_cls, enum_cls)

        if fn == "all":
            expr = MyEnum.b == all_(tbl.c.pyenum_col)
            result = [([MyEnum.b],)]
        elif fn == "any":
            expr = MyEnum.b == any_(tbl.c.pyenum_col)
            result = [([MyEnum.a, MyEnum.b],), ([MyEnum.b],)]
        else:
            assert False
        sel = select(tbl.c.pyenum_col).where(expr).order_by(tbl.c.id)
        eq_(connection.execute(sel).fetchall(), result)

    @_enum_combinations
    @testing.combinations("all", "any", argnames="fn")
    def test_any_all_deprecated_roundtrip(
        self, array_of_enum_fixture, connection, array_cls, enum_cls, fn
    ):
        """test #6515"""

        tbl, MyEnum = array_of_enum_fixture(array_cls, enum_cls)

        with _array_any_deprecation():
            if fn == "all":
                expr = tbl.c.pyenum_col.all(MyEnum.b)
                result = [([MyEnum.b],)]
            elif fn == "any":
                expr = tbl.c.pyenum_col.any(MyEnum.b)
                result = [([MyEnum.a, MyEnum.b],), ([MyEnum.b],)]
            else:
                assert False
        sel = select(tbl.c.pyenum_col).where(expr).order_by(tbl.c.id)
        eq_(connection.execute(sel).fetchall(), result)

    @_enum_combinations
    def test_array_of_enums_roundtrip(
        self, array_of_enum_fixture, connection, array_cls, enum_cls
    ):
        tbl, MyEnum = array_of_enum_fixture(array_cls, enum_cls)

        # test select back
        sel = select(tbl.c.enum_col).order_by(tbl.c.id)
        eq_(
            connection.execute(sel).fetchall(), [(["foo"],), (["foo", "bar"],)]
        )

    @_enum_combinations
    def test_array_of_enums_expanding_in(
        self, array_of_enum_fixture, connection, array_cls, enum_cls
    ):
        tbl, MyEnum = array_of_enum_fixture(array_cls, enum_cls)

        # test select with WHERE using expanding IN against arrays
        # #7177
        sel = (
            select(tbl.c.enum_col)
            .where(tbl.c.enum_col.in_([["foo", "bar"], ["bar", "foo"]]))
            .order_by(tbl.c.id)
        )
        eq_(connection.execute(sel).fetchall(), [(["foo", "bar"],)])

    @_enum_combinations
    def test_array_of_enums_native_roundtrip(
        self, array_of_enum_fixture, connection, array_cls, enum_cls
    ):
        tbl, MyEnum = array_of_enum_fixture(array_cls, enum_cls)

        connection.execute(tbl.insert(), {"pyenum_col": [MyEnum.a]})
        sel = select(tbl.c.pyenum_col).order_by(tbl.c.id.desc())
        eq_(connection.scalar(sel), [MyEnum.a])


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


class TimestampTest(
    fixtures.TestBase, AssertsCompiledSQL, AssertsExecutionResults
):
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

        expr = operators.null_op(
            column("bar", postgresql.INTERVAL), column("foo", types.Numeric)
        )
        eq_(expr.type._type_affinity, types.Interval)
        assert isinstance(expr.type, postgresql.INTERVAL)

    def test_interval_coercion_literal(self):
        expr = column("bar", postgresql.INTERVAL) == datetime.timedelta(days=1)
        eq_(expr.right.type._type_affinity, types.Interval)

    def test_interval_literal_processor(self, connection):
        stmt = text("select :parameter - :parameter2")
        result = connection.execute(
            stmt.bindparams(
                bindparam(
                    "parameter",
                    datetime.timedelta(days=1, minutes=3, seconds=4),
                    literal_execute=True,
                ),
                bindparam(
                    "parameter2",
                    datetime.timedelta(days=0, minutes=1, seconds=4),
                    literal_execute=True,
                ),
            )
        ).one()
        eq_(result[0], datetime.timedelta(days=1, seconds=120))

    @testing.combinations(
        (
            text("select :parameter").bindparams(
                parameter=datetime.timedelta(days=2)
            ),
            ("select make_interval(secs=>172800.0)"),
        ),
        (
            text("select :parameter").bindparams(
                parameter=datetime.timedelta(days=730, seconds=2323213392),
            ),
            ("select make_interval(secs=>2386285392.0)"),
        ),
    )
    def test_interval_literal_processor_compiled(self, type_, expected):
        self.assert_compile(type_, expected, literal_binds=True)


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

    @testing.combinations(
        (psycopg.dialect(),),
        (psycopg2.dialect(),),
        (asyncpg.dialect(),),
        (pg8000.dialect(),),
        argnames="dialect",
        id_="n",
    )
    def test_network_address_cast(self, metadata, dialect):
        t = Table(
            "addresses",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("addr", postgresql.INET),
            Column("addr2", postgresql.MACADDR),
            Column("addr3", postgresql.CIDR),
            Column("addr4", postgresql.MACADDR8),
        )
        stmt = select(t.c.id).where(
            t.c.addr == "127.0.0.1",
            t.c.addr2 == "08:00:2b:01:02:03",
            t.c.addr3 == "192.168.100.128/25",
            t.c.addr4 == "08:00:2b:01:02:03:04:05",
        )
        param, param2, param3, param4 = {
            "format": ("%s", "%s", "%s", "%s"),
            "numeric_dollar": ("$1", "$2", "$3", "$4"),
            "pyformat": (
                "%(addr_1)s",
                "%(addr2_1)s",
                "%(addr3_1)s",
                "%(addr4_1)s",
            ),
        }[dialect.paramstyle]
        expected = (
            "SELECT addresses.id FROM addresses "
            f"WHERE addresses.addr = {param} "
            f"AND addresses.addr2 = {param2} "
            f"AND addresses.addr3 = {param3} "
            f"AND addresses.addr4 = {param4}"
        )
        self.assert_compile(stmt, expected, dialect=dialect)


class SpecialTypesTest(fixtures.TablesTest, ComparesTables):
    """test DDL and reflection of PG-specific types"""

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
            Column("bitstring_varying", postgresql.BIT(varying=True)),
            Column("bitstring_varying_6", postgresql.BIT(6, varying=True)),
            Column("bitstring_4", postgresql.BIT(4)),
            Column("addr", postgresql.INET),
            Column("addr2", postgresql.MACADDR),
            Column("addr4", postgresql.MACADDR8),
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

        assert t.c.flag.type.varying is False
        assert t.c.flag.type.length == 1

        assert t.c.bitstring_varying.type.varying is True
        assert t.c.bitstring_varying.type.length is None

        assert t.c.bitstring_varying_6.type.varying is True
        assert t.c.bitstring_varying_6.type.length == 6

        assert t.c.bitstring_4.type.varying is False
        assert t.c.bitstring_4.type.length == 4

    @testing.combinations(
        (postgresql.INET, "127.0.0.1"),
        (postgresql.CIDR, "192.168.100.128/25"),
        (postgresql.MACADDR, "08:00:2b:01:02:03"),
        (
            postgresql.MACADDR8,
            "08:00:2b:01:02:03:04:05",
            testing.skip_if("postgresql < 10"),
        ),
        argnames="column_type, value",
        id_="na",
    )
    def test_network_address_round_trip(
        self, connection, metadata, column_type, value
    ):
        t = Table(
            "addresses",
            metadata,
            Column("name", String),
            Column("value", column_type),
        )
        t.create(connection)
        connection.execute(t.insert(), {"name": "test", "value": value})
        eq_(
            connection.scalar(select(t.c.name).where(t.c.value == value)),
            "test",
        )

    @testing.combinations(
        (postgresql.BIT(varying=True), BitString("")),
        (postgresql.BIT(varying=True), BitString("1101010101")),
        (postgresql.BIT(6, varying=True), BitString("")),
        (postgresql.BIT(6, varying=True), BitString("010101")),
        (postgresql.BIT(1), BitString("0")),
        (postgresql.BIT(4), BitString("0010")),
        (postgresql.BIT(4), "0010"),
        argnames="column_type, value",
    )
    def test_bitstring_round_trip(
        self, connection, metadata, column_type, value
    ):
        t = Table(
            "bits",
            metadata,
            Column("name", String),
            Column("value", column_type),
        )
        t.create(connection)

        connection.execute(t.insert(), {"name": "test", "value": value})
        eq_(
            connection.scalar(select(t.c.name).where(t.c.value == value)),
            "test",
        )

        result_value = connection.scalar(
            select(t.c.value).where(t.c.name == "test")
        )
        assert isinstance(result_value, BitString)
        eq_(result_value, value)
        eq_(result_value, str(value))
        eq_(str(result_value), str(value))

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
    """Test postgresql-specific UUID cases.

    See also generic UUID tests in testing/suite/test_types

    """

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

    @testing.combinations(
        (
            "not_as_uuid",
            postgresql.UUID(as_uuid=False),
            str(uuid.uuid4()),
        ),
        (
            "as_uuid",
            postgresql.UUID(as_uuid=True),
            uuid.uuid4(),
        ),
        id_="iaa",
        argnames="datatype, value1",
    )
    def test_uuid_literal(self, datatype, value1, connection):
        v1 = connection.execute(
            select(
                bindparam(
                    "key",
                    value=value1,
                    literal_execute=True,
                    type_=datatype,
                )
            ),
        )
        eq_(v1.fetchone()[0], value1)

    def test_python_type(self):
        eq_(postgresql.UUID(as_uuid=True).python_type, uuid.UUID)
        eq_(postgresql.UUID(as_uuid=False).python_type, str)


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
            self.hashcol.has_key("foo"),
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

    def test_where_has_key_any(self):
        self._test_where(
            self.hashcol.has_key(any_(array(["foo"]))),
            "test_table.hash ? ANY (ARRAY[%(param_1)s])",
        )

    def test_where_has_all_any(self):
        self._test_where(
            self.hashcol.has_all(any_(postgresql.array(["1", "2"]))),
            "test_table.hash ?& ANY (ARRAY[%(param_1)s, %(param_2)s])",
        )

    def test_where_has_any_any(self):
        self._test_where(
            self.hashcol.has_any(any_(postgresql.array(["1", "2"]))),
            "test_table.hash ?| ANY (ARRAY[%(param_1)s, %(param_2)s])",
        )

    def test_where_contains_any(self):
        self._test_where(
            self.hashcol.contains(any_(array(["foo"]))),
            "test_table.hash @> ANY (ARRAY[%(param_1)s])",
        )

    def test_where_contained_by_any(self):
        self._test_where(
            self.hashcol.contained_by(any_(array(["foo"]))),
            "test_table.hash <@ ANY (ARRAY[%(param_1)s])",
        )

    def test_where_getitem(self):
        self._test_where(
            self.hashcol["bar"] == None,  # noqa
            "(test_table.hash -> %(hash_1)s) IS NULL",
        )

    def test_where_getitem_any(self):
        self._test_where(
            self.hashcol["bar"] == any_(array(["foo"])),  # noqa
            "(test_table.hash -> %(hash_1)s) = ANY (ARRAY[%(param_1)s])",
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
        local_engine = testing.requires.native_hstore.enabled

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

    @testing.requires.native_hstore
    def test_insert_native(self, connection):
        self._test_insert(connection)

    def test_insert_python(self, non_native_hstore_connection):
        self._test_insert(non_native_hstore_connection)

    @testing.requires.native_hstore
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

    @testing.requires.native_hstore
    def test_fixed_round_trip_native(self, connection):
        self._test_fixed_round_trip(connection)

    def _test_unicode_round_trip(self, connection):
        s = select(
            hstore(
                array(["réveillé", "drôle", "S’il"]),
                array(["réveillé", "drôle", "S’il"]),
            )
        )
        eq_(
            connection.scalar(s),
            {
                "réveillé": "réveillé",
                "drôle": "drôle",
                "S’il": "S’il",
            },
        )

    @testing.requires.native_hstore
    def test_unicode_round_trip_python(self, non_native_hstore_connection):
        self._test_unicode_round_trip(non_native_hstore_connection)

    @testing.requires.native_hstore
    def test_unicode_round_trip_native(self, connection):
        self._test_unicode_round_trip(connection)

    def test_escaped_quotes_round_trip_python(
        self, non_native_hstore_connection
    ):
        self._test_escaped_quotes_round_trip(non_native_hstore_connection)

    @testing.requires.native_hstore
    def test_escaped_quotes_round_trip_native(self, connection):
        self._test_escaped_quotes_round_trip(connection)

    def _test_escaped_quotes_round_trip(self, connection):
        connection.execute(
            self.tables.data_table.insert(),
            {"name": "r1", "data": {r"key \"foo\"": r'value \"bar"\ xyz'}},
        )
        self._assert_data([{r"key \"foo\"": r'value \"bar"\ xyz'}], connection)

    def test_orm_round_trip(self, registry):
        class Data:
            def __init__(self, name, data):
                self.name = name
                self.data = data

        registry.map_imperatively(Data, self.tables.data_table)

        with fixtures.fixture_session() as s:
            d = Data(
                name="r1",
                data={"key1": "value1", "key2": "value2", "key3": "value3"},
            )
            s.add(d)
            eq_(s.query(Data.data, Data).all(), [(d.data, d)])


class BitTests(fixtures.TestBase):
    __backend__ = True
    __only_on__ = "postgresql"

    def test_concatenation(self, connection):
        coltype = BIT(varying=True)

        q = select(
            literal(BitString("1111"), coltype).concat(BitString("0000"))
        )
        r = connection.execute(q).first()
        eq_(r[0], BitString("11110000"))

    def test_invert_operator(self, connection):
        coltype = BIT(4)

        q = select(literal(BitString("0010"), coltype).bitwise_not())
        r = connection.execute(q).first()

        eq_(r[0], BitString("1101"))

    def test_and_operator(self, connection):
        coltype = BIT(6)

        q1 = select(
            literal(BitString("001010"), coltype)
            & literal(BitString("010111"), coltype)
        )
        r1 = connection.execute(q1).first()

        eq_(r1[0], BitString("000010"))

        q2 = select(
            literal(BitString("010101"), coltype) & BitString("001011")
        )
        r2 = connection.execute(q2).first()
        eq_(r2[0], BitString("000001"))

    def test_or_operator(self, connection):
        coltype = BIT(6)

        q1 = select(
            literal(BitString("001010"), coltype)
            | literal(BitString("010111"), coltype)
        )
        r1 = connection.execute(q1).first()

        eq_(r1[0], BitString("011111"))

        q2 = select(
            literal(BitString("010101"), coltype) | BitString("001011")
        )
        r2 = connection.execute(q2).first()
        eq_(r2[0], BitString("011111"))

    def test_xor_operator(self, connection):
        coltype = BIT(6)

        q1 = select(
            literal(BitString("001010"), coltype).bitwise_xor(
                literal(BitString("010111"), coltype)
            )
        )
        r1 = connection.execute(q1).first()
        eq_(r1[0], BitString("011101"))

        q2 = select(
            literal(BitString("010101"), coltype).bitwise_xor(
                BitString("001011")
            )
        )
        r2 = connection.execute(q2).first()
        eq_(r2[0], BitString("011110"))

    def test_lshift_operator(self, connection):
        coltype = BIT(6)

        q = select(
            literal(BitString("001010"), coltype),
            literal(BitString("001010"), coltype) << 1,
        )

        r = connection.execute(q).first()
        eq_(tuple(r), (BitString("001010"), BitString("010100")))

    def test_rshift_operator(self, connection):
        coltype = BIT(6)

        q = select(
            literal(BitString("001010"), coltype),
            literal(BitString("001010"), coltype) >> 1,
        )

        r = connection.execute(q).first()
        eq_(tuple(r), (BitString("001010"), BitString("000101")))


class RangeMiscTests(fixtures.TestBase):
    @testing.combinations(
        (Range(2, 7), INT4RANGE),
        (Range(-10, 7), INT4RANGE),
        (Range(None, -7), INT4RANGE),
        (Range(33, None), INT4RANGE),
        (Range(-2147483648, 2147483647), INT4RANGE),
        (Range(-2147483648 - 1, 2147483647), INT8RANGE),
        (Range(-2147483648, 2147483647 + 1), INT8RANGE),
        (Range(-2147483648 - 1, None), INT8RANGE),
        (Range(None, 2147483647 + 1), INT8RANGE),
    )
    def test_resolve_for_literal(self, obj, type_):
        """This tests that the int4 / int8 version is selected correctly by
        _resolve_for_literal."""
        lit = literal(obj)
        eq_(type(lit.type), type_)

    @testing.combinations(
        (Range(2, 7), INT4MULTIRANGE),
        (Range(-10, 7), INT4MULTIRANGE),
        (Range(None, -7), INT4MULTIRANGE),
        (Range(33, None), INT4MULTIRANGE),
        (Range(-2147483648, 2147483647), INT4MULTIRANGE),
        (Range(-2147483648 - 1, 2147483647), INT8MULTIRANGE),
        (Range(-2147483648, 2147483647 + 1), INT8MULTIRANGE),
        (Range(-2147483648 - 1, None), INT8MULTIRANGE),
        (Range(None, 2147483647 + 1), INT8MULTIRANGE),
    )
    def test_resolve_for_literal_multi(self, obj, type_):
        """This tests that the int4 / int8 version is selected correctly by
        _resolve_for_literal."""
        list_ = MultiRange([Range(-1, 1), obj, Range(7, 100)])
        lit = literal(list_)
        eq_(type(lit.type), type_)

    def test_multirange_sequence(self):
        plain = [Range(-1, 1), Range(42, 43), Range(7, 100)]
        mr = MultiRange(plain)
        is_true(issubclass(MultiRange, list))
        is_true(isinstance(mr, list))
        eq_(mr, plain)
        eq_(str(mr), str(plain))
        eq_(repr(mr), repr(plain))
        ne_(mr, plain[1:])


class _RangeTests:
    _col_type = None
    "The concrete range class these tests are for."

    _col_str = None
    "The corresponding PG type name."

    _epsilon = None
    """A small value used to generate range variants"""

    def _data_str(self):
        """return string form of a sample range"""
        raise NotImplementedError()

    def _data_obj(self):
        """return Range form of the same range"""
        raise NotImplementedError()


class _RangeTypeCompilation(
    AssertsCompiledSQL, _RangeTests, fixtures.TestBase
):
    __dialect__ = "postgresql"

    @property
    def _col_str_arr(self):
        return self._col_str

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

    _comparisons = [
        (lambda col, other: col == other, "="),
        (lambda col, other: col != other, "!="),
        (lambda col, other: col > other, ">"),
        (lambda col, other: col < other, "<"),
        (lambda col, other: col >= other, ">="),
        (lambda col, other: col <= other, "<="),
        (lambda col, other: col.contains(other), "@>"),
        (lambda col, other: col.contained_by(other), "<@"),
        (lambda col, other: col.overlaps(other), "&&"),
        (lambda col, other: col << other, "<<"),
        (lambda col, other: col.strictly_left_of(other), "<<"),
        (lambda col, other: col >> other, ">>"),
        (lambda col, other: col.strictly_right_of(other), ">>"),
        (lambda col, other: col.not_extend_left_of(other), "&>"),
        (lambda col, other: col.not_extend_right_of(other), "&<"),
        (lambda col, other: col.adjacent_to(other), "-|-"),
    ]

    _operations = [
        (lambda col, other: col + other, "+"),
        (lambda col, other: col.union(other), "+"),
        (lambda col, other: col - other, "-"),
        (lambda col, other: col.difference(other), "-"),
        (lambda col, other: col * other, "*"),
        (lambda col, other: col.intersection(other), "*"),
    ]

    _all_fns = _comparisons + _operations

    _not_compare_op = ("+", "-", "*")

    @testing.combinations(*_all_fns, id_="as")
    def test_data_str(self, fn, op):
        self._test_clause(
            fn(self.col, self._data_str()),
            f"data_table.range {op} %(range_1)s",
            (
                self.col.type
                if op in self._not_compare_op
                else sqltypes.BOOLEANTYPE
            ),
        )

    @testing.combinations(*_all_fns, id_="as")
    def test_data_obj(self, fn, op):
        self._test_clause(
            fn(self.col, self._data_obj()),
            f"data_table.range {op} %(range_1)s::{self._col_str}",
            (
                self.col.type
                if op in self._not_compare_op
                else sqltypes.BOOLEANTYPE
            ),
        )

    @testing.combinations(*_comparisons, id_="as")
    def test_data_str_any(self, fn, op):
        self._test_clause(
            fn(self.col, any_(array([self._data_str()]))),
            f"data_table.range {op} ANY (ARRAY[%(param_1)s])",
            (
                self.col.type
                if op in self._not_compare_op
                else sqltypes.BOOLEANTYPE
            ),
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


class _RangeComparisonFixtures(_RangeTests):
    def _step_value_up(self, value):
        """given a value, return a step up

        this is a value that given the lower end of the sample range,
        would be less than the upper value of the range

        """
        raise NotImplementedError()

    def _step_value_down(self, value):
        """given a value, return a step down

        this is a value that given the upper end of the sample range,
        would be greater than the lower value of the range

        """
        raise NotImplementedError()

    def _value_values(self):
        """Return a series of values related to the base range

        le = left equal
        ll = lower than left
        re = right equal
        rh = higher than right
        il = inside lower
        ih = inside higher

        """
        spec = self._data_obj()

        le, re_ = spec.lower, spec.upper

        ll = self._step_value_down(le)
        il = self._step_value_up(le)
        rh = self._step_value_up(re_)
        ih = self._step_value_down(re_)

        return {"le": le, "re_": re_, "ll": ll, "il": il, "rh": rh, "ih": ih}

    @testing.fixture(
        params=[
            lambda **kw: Range(empty=True),
            lambda **kw: Range(bounds="[)"),
            lambda le, **kw: Range(upper=le, bounds="[)"),
            lambda le, re_, **kw: Range(lower=le, upper=re_, bounds="[)"),
            lambda le, re_, **kw: Range(lower=le, upper=re_, bounds="[)"),
            lambda le, re_, **kw: Range(lower=le, upper=re_, bounds="[]"),
            lambda le, re_, **kw: Range(lower=le, upper=re_, bounds="(]"),
            lambda le, re_, **kw: Range(lower=le, upper=re_, bounds="()"),
            lambda ll, le, **kw: Range(lower=ll, upper=le, bounds="[)"),
            lambda il, ih, **kw: Range(lower=il, upper=ih, bounds="[)"),
            lambda ll, le, **kw: Range(lower=ll, upper=le, bounds="(]"),
            lambda ll, rh, **kw: Range(lower=ll, upper=rh, bounds="[)"),
        ]
    )
    def contains_range_obj_combinations(self, request):
        """ranges that are used for range contains() contained_by() tests"""
        data = self._value_values()

        range_ = request.param(**data)
        yield range_

    @testing.fixture(
        params=[
            lambda l, r: Range(empty=True),
            lambda l, r: Range(bounds="()"),
            lambda l, r: Range(upper=r, bounds="(]"),
            lambda l, r: Range(lower=l, bounds="[)"),
            lambda l, r: Range(lower=l, upper=r, bounds="[)"),
            lambda l, r: Range(lower=l, upper=r, bounds="[]"),
            lambda l, r: Range(lower=l, upper=r, bounds="(]"),
            lambda l, r: Range(lower=l, upper=r, bounds="()"),
        ]
    )
    def bounds_obj_combinations(self, request):
        """sample ranges used for value and range contains()/contained_by()
        tests"""

        obj = self._data_obj()
        l, r = obj.lower, obj.upper

        template = request.param
        value = template(l=l, r=r)
        yield value

    @testing.fixture(params=["ll", "le", "il", "ih", "re_", "rh"])
    def value_combinations(self, request):
        """sample values used for value contains() tests"""
        data = self._value_values()
        return data[request.param]

    def test_basic_py_sanity(self):
        values = self._value_values()

        range_ = self._data_obj()

        is_true(range_.contains(Range(lower=values["il"], upper=values["ih"])))

        is_true(
            range_.contained_by(Range(lower=values["ll"], upper=values["rh"]))
        )

        is_true(range_.contains(values["il"]))
        is_true(values["il"] in range_)

        is_false(
            range_.contains(Range(lower=values["ll"], upper=values["ih"]))
        )

        is_false(range_.contains(values["rh"]))
        is_false(values["rh"] in range_)

        is_true(range_ == range_)
        is_false(range_ != range_)
        is_false(range_ == None)

    def test_compatibility_accessors(self):
        range_ = self._data_obj()

        is_true(range_.lower_inc)
        is_false(range_.upper_inc)
        is_false(Range(lower=range_.lower, bounds="()").lower_inc)
        is_true(Range(upper=range_.upper, bounds="(]").upper_inc)

        is_false(range_.lower_inf)
        is_false(range_.upper_inf)
        is_false(Range(empty=True).lower_inf)
        is_false(Range(empty=True).upper_inf)
        is_true(Range().lower_inf)
        is_true(Range().upper_inf)

        is_false(range_.isempty)
        is_true(Range(empty=True).isempty)

    def test_contains_value(
        self, connection, bounds_obj_combinations, value_combinations
    ):
        range_ = bounds_obj_combinations
        range_typ = self._col_str

        strvalue = range_._stringify()

        v = value_combinations
        RANGE = self._col_type

        q = select(
            literal_column(f"'{strvalue}'::{range_typ}", RANGE).label("r1"),
            cast(range_, RANGE).label("r2"),
        )
        literal_range, cast_range = connection.execute(q).first()
        eq_(literal_range, cast_range)

        q = select(
            cast(range_, RANGE),
            cast(range_, RANGE).contains(v),
        )
        r, expected = connection.execute(q).first()
        eq_(r.contains(v), expected)
        eq_(v in r, expected)

    _common_ranges_to_test = (
        lambda r, e: Range(empty=True),
        lambda r, e: Range(None, None, bounds="()"),
        lambda r, e: Range(r.lower, None, bounds="[)"),
        lambda r, e: Range(None, r.upper, bounds="(]"),
        lambda r, e: r,
        lambda r, e: Range(r.lower, r.upper, bounds="[]"),
        lambda r, e: Range(r.lower, r.upper, bounds="(]"),
        lambda r, e: Range(r.lower, r.upper, bounds="()"),
    )

    @testing.combinations(
        *_common_ranges_to_test,
        argnames="r1t",
    )
    @testing.combinations(
        *_common_ranges_to_test,
        lambda r, e: Range(r.lower + e, r.upper + e, bounds="[)"),
        lambda r, e: Range(r.lower - e, r.upper - e, bounds="(]"),
        lambda r, e: Range(r.lower + e, r.upper - e, bounds="[]"),
        lambda r, e: Range(r.lower + e, r.upper - e, bounds="(]"),
        lambda r, e: Range(r.lower + e, r.upper, bounds="(]"),
        lambda r, e: Range(r.lower + e, r.upper, bounds="[]"),
        lambda r, e: Range(r.lower + e, r.upper + e, bounds="[)"),
        lambda r, e: Range(r.lower - e, r.upper - e, bounds="[]"),
        lambda r, e: Range(r.lower - 2 * e, r.lower - e, bounds="(]"),
        lambda r, e: Range(r.lower - 4 * e, r.lower, bounds="[)"),
        lambda r, e: Range(r.upper + 4 * e, r.upper + 6 * e, bounds="()"),
        argnames="r2t",
    )
    def test_contains_range(self, connection, r1t, r2t):
        r1 = r1t(self._data_obj(), self._epsilon)
        r2 = r2t(self._data_obj(), self._epsilon)

        RANGE = self._col_type
        range_typ = self._col_str

        q = select(
            cast(r1, RANGE).contains(r2),
            cast(r1, RANGE).contained_by(r2),
        )

        validate_q = select(
            literal_column(f"'{r1}'::{range_typ} @> '{r2}'::{range_typ}"),
            literal_column(f"'{r1}'::{range_typ} <@ '{r2}'::{range_typ}"),
        )

        row = connection.execute(q).first()
        validate_row = connection.execute(validate_q).first()
        eq_(row, validate_row)

        pg_contains, pg_contained = row
        py_contains = r1.contains(r2)
        eq_(
            py_contains,
            pg_contains,
            f"{r1}.contains({r2}): got {py_contains},"
            f" expected {pg_contains}",
        )
        r2_in_r1 = r2 in r1
        eq_(
            r2_in_r1,
            pg_contains,
            f"{r2} in {r1}: got {r2_in_r1}, expected {pg_contains}",
        )
        py_contained = r1.contained_by(r2)
        eq_(
            py_contained,
            pg_contained,
            f"{r1}.contained_by({r2}): got {py_contained},"
            f" expected {pg_contained}",
        )
        eq_(
            r2.contains(r1),
            pg_contained,
            f"{r2}.contains({r1}: got {r2.contains(r1)},"
            f" expected {pg_contained})",
        )
        r1_in_r2 = r1 in r2
        eq_(
            r1_in_r2,
            pg_contained,
            f"{r1} in {r2}: got {r1_in_r2}, expected {pg_contained}",
        )

    @testing.combinations(
        *_common_ranges_to_test,
        argnames="r1t",
    )
    @testing.combinations(
        *_common_ranges_to_test,
        lambda r, e: Range(r.lower - 2 * e, r.lower - e, bounds="(]"),
        lambda r, e: Range(r.upper + e, r.upper + 2 * e, bounds="[)"),
        argnames="r2t",
    )
    def test_overlaps(self, connection, r1t, r2t):
        r1 = r1t(self._data_obj(), self._epsilon)
        r2 = r2t(self._data_obj(), self._epsilon)

        RANGE = self._col_type
        range_typ = self._col_str

        q = select(
            cast(r1, RANGE).overlaps(r2),
        )
        validate_q = select(
            literal_column(f"'{r1}'::{range_typ} && '{r2}'::{range_typ}"),
        )
        row = connection.execute(q).first()
        validate_row = connection.execute(validate_q).first()
        eq_(row, validate_row)

        pg_res = row[0]
        py_res = r1.overlaps(r2)
        eq_(
            py_res,
            pg_res,
            f"{r1}.overlaps({r2}): got {py_res}, expected {pg_res}",
        )

    @testing.combinations(
        *_common_ranges_to_test,
        argnames="r1t",
    )
    @testing.combinations(
        *_common_ranges_to_test,
        lambda r, e: Range(r.upper, r.upper + 2 * e, bounds="[]"),
        lambda r, e: Range(r.upper, r.upper + 2 * e, bounds="(]"),
        lambda r, e: Range(r.lower - 2 * e, r.lower, bounds="[]"),
        lambda r, e: Range(r.lower - 2 * e, r.lower, bounds="[)"),
        argnames="r2t",
    )
    def test_strictly_left_or_right_of(self, connection, r1t, r2t):
        r1 = r1t(self._data_obj(), self._epsilon)
        r2 = r2t(self._data_obj(), self._epsilon)

        RANGE = self._col_type
        range_typ = self._col_str

        q = select(
            cast(r1, RANGE).strictly_left_of(r2),
            cast(r1, RANGE).strictly_right_of(r2),
        )
        validate_q = select(
            literal_column(f"'{r1}'::{range_typ} << '{r2}'::{range_typ}"),
            literal_column(f"'{r1}'::{range_typ} >> '{r2}'::{range_typ}"),
        )

        row = connection.execute(q).first()
        validate_row = connection.execute(validate_q).first()
        eq_(row, validate_row)

        pg_left, pg_right = row
        py_left = r1.strictly_left_of(r2)
        eq_(
            py_left,
            pg_left,
            f"{r1}.strictly_left_of({r2}): got {py_left}, expected {pg_left}",
        )
        py_left = r1 << r2
        eq_(
            py_left,
            pg_left,
            f"{r1} << {r2}: got {py_left}, expected {pg_left}",
        )
        py_right = r1.strictly_right_of(r2)
        eq_(
            py_right,
            pg_right,
            f"{r1}.strictly_right_of({r2}): got {py_left},"
            f" expected {pg_right}",
        )
        py_right = r1 >> r2
        eq_(
            py_right,
            pg_right,
            f"{r1} >> {r2}: got {py_left}, expected {pg_right}",
        )

    @testing.combinations(
        *_common_ranges_to_test,
        argnames="r1t",
    )
    @testing.combinations(
        *_common_ranges_to_test,
        lambda r, e: Range(r.upper, r.upper + 2 * e, bounds="[]"),
        lambda r, e: Range(r.upper, r.upper + 2 * e, bounds="(]"),
        lambda r, e: Range(r.lower - 2 * e, r.lower, bounds="[]"),
        lambda r, e: Range(r.lower - 2 * e, r.lower, bounds="[)"),
        argnames="r2t",
    )
    def test_not_extend_left_or_right_of(self, connection, r1t, r2t):
        r1 = r1t(self._data_obj(), self._epsilon)
        r2 = r2t(self._data_obj(), self._epsilon)

        RANGE = self._col_type
        range_typ = self._col_str

        q = select(
            cast(r1, RANGE).not_extend_left_of(r2),
            cast(r1, RANGE).not_extend_right_of(r2),
        )
        validate_q = select(
            literal_column(f"'{r1}'::{range_typ} &> '{r2}'::{range_typ}"),
            literal_column(f"'{r1}'::{range_typ} &< '{r2}'::{range_typ}"),
        )
        row = connection.execute(q).first()
        validate_row = connection.execute(validate_q).first()
        eq_(row, validate_row)

        pg_left, pg_right = row
        py_left = r1.not_extend_left_of(r2)
        eq_(
            py_left,
            pg_left,
            f"{r1}.not_extend_left_of({r2}): got {py_left},"
            f" expected {pg_left}",
        )
        py_right = r1.not_extend_right_of(r2)
        eq_(
            py_right,
            pg_right,
            f"{r1}.not_extend_right_of({r2}): got {py_right},"
            f" expected {pg_right}",
        )

    @testing.combinations(
        *_common_ranges_to_test,
        lambda r, e: Range(r.lower - e, r.lower + e, bounds="[)"),
        lambda r, e: Range(r.lower - e, r.lower - e, bounds="[]"),
        argnames="r1t",
    )
    @testing.combinations(
        *_common_ranges_to_test,
        lambda r, e: Range(r.lower - e, r.lower + e, bounds="[)"),
        lambda r, e: Range(r.lower - e, r.lower - e, bounds="[]"),
        lambda r, e: Range(r.lower + e, r.upper - e, bounds="(]"),
        lambda r, e: Range(r.lower + e, r.upper - e, bounds="[]"),
        lambda r, e: Range(r.lower + e, r.upper, bounds="(]"),
        lambda r, e: Range(r.lower + e, r.upper, bounds="[]"),
        lambda r, e: Range(r.lower + e, r.upper + e, bounds="[)"),
        lambda r, e: Range(r.lower - e, r.lower - e, bounds="[]"),
        lambda r, e: Range(r.lower - 2 * e, r.lower - e, bounds="(]"),
        lambda r, e: Range(r.lower - 4 * e, r.lower, bounds="[)"),
        lambda r, e: Range(r.upper + 4 * e, r.upper + 6 * e, bounds="()"),
        argnames="r2t",
    )
    def test_adjacent(self, connection, r1t, r2t):
        r1 = r1t(self._data_obj(), self._epsilon)
        r2 = r2t(self._data_obj(), self._epsilon)

        RANGE = self._col_type
        range_typ = self._col_str

        q = select(
            cast(r1, RANGE).adjacent_to(r2),
        )
        validate_q = select(
            literal_column(f"'{r1}'::{range_typ} -|- '{r2}'::{range_typ}"),
        )

        row = connection.execute(q).first()
        validate_row = connection.execute(validate_q).first()
        eq_(row, validate_row)

        pg_res = row[0]
        py_res = r1.adjacent_to(r2)
        eq_(
            py_res,
            pg_res,
            f"{r1}.adjacent_to({r2}): got {py_res}, expected {pg_res}",
        )

    @testing.combinations(
        *_common_ranges_to_test,
        argnames="r1t",
    )
    @testing.combinations(
        *_common_ranges_to_test,
        lambda r, e: Range(r.lower, r.lower + e, bounds="[]"),
        lambda r, e: Range(r.upper + 4 * e, r.upper + 6 * e, bounds="()"),
        argnames="r2t",
    )
    def test_union(self, connection, r1t, r2t):
        r1 = r1t(self._data_obj(), self._epsilon)
        r2 = r2t(self._data_obj(), self._epsilon)

        RANGE = self._col_type
        range_typ = self._col_str

        q = select(
            cast(r1, RANGE).union(r2),
        )
        validate_q = select(
            literal_column(f"'{r1}'::{range_typ}+'{r2}'::{range_typ}", RANGE),
        )

        try:
            pg_res = connection.execute(q).scalar()
        except DBAPIError:
            connection.rollback()
            with expect_raises(DBAPIError):
                connection.execute(validate_q).scalar()
            with expect_raises(ValueError):
                r1.union(r2)
        else:
            validate_union = connection.execute(validate_q).scalar()
            eq_(pg_res, validate_union)
            py_res = r1.union(r2)
            eq_(
                py_res,
                pg_res,
                f"{r1}.union({r2}): got {py_res}, expected {pg_res}",
            )

    @testing.combinations(
        *_common_ranges_to_test,
        lambda r, e: Range(r.lower, r.lower, bounds="[]"),
        lambda r, e: Range(r.lower - e, r.upper - e, bounds="[]"),
        lambda r, e: Range(r.lower - e, r.upper + e, bounds="[)"),
        lambda r, e: Range(r.lower - e, r.upper + e, bounds="[]"),
        argnames="r1t",
    )
    @testing.combinations(
        *_common_ranges_to_test,
        lambda r, e: Range(r.lower, r.lower, bounds="[]"),
        lambda r, e: Range(r.lower, r.upper - e, bounds="(]"),
        lambda r, e: Range(r.lower, r.lower + e, bounds="[)"),
        lambda r, e: Range(r.lower - e, r.lower, bounds="(]"),
        lambda r, e: Range(r.lower - e, r.lower + e, bounds="()"),
        lambda r, e: Range(r.lower, r.upper, bounds="[]"),
        lambda r, e: Range(r.lower, r.upper, bounds="()"),
        argnames="r2t",
    )
    def test_difference(self, connection, r1t, r2t):
        r1 = r1t(self._data_obj(), self._epsilon)
        r2 = r2t(self._data_obj(), self._epsilon)

        RANGE = self._col_type
        range_typ = self._col_str

        q = select(
            cast(r1, RANGE).difference(r2),
        )
        validate_q = select(
            literal_column(f"'{r1}'::{range_typ}-'{r2}'::{range_typ}", RANGE),
        )

        try:
            pg_res = connection.execute(q).scalar()
        except DBAPIError:
            connection.rollback()
            with expect_raises(DBAPIError):
                connection.execute(validate_q).scalar()
            with expect_raises(ValueError):
                r1.difference(r2)
        else:
            validate_difference = connection.execute(validate_q).scalar()
            eq_(pg_res, validate_difference)
            py_res = r1.difference(r2)
            eq_(
                py_res,
                pg_res,
                f"{r1}.difference({r2}): got {py_res}, expected {pg_res}",
            )

    @testing.combinations(
        *_common_ranges_to_test,
        lambda r, e: Range(r.lower, r.lower, bounds="[]"),
        lambda r, e: Range(r.lower - e, r.upper - e, bounds="[]"),
        lambda r, e: Range(r.lower - e, r.upper + e, bounds="[)"),
        lambda r, e: Range(r.lower - e, r.upper + e, bounds="[]"),
        argnames="r1t",
    )
    @testing.combinations(
        *_common_ranges_to_test,
        lambda r, e: Range(r.lower, r.lower, bounds="[]"),
        lambda r, e: Range(r.lower, r.upper - e, bounds="(]"),
        lambda r, e: Range(r.lower, r.lower + e, bounds="[)"),
        lambda r, e: Range(r.lower - e, r.lower, bounds="(]"),
        lambda r, e: Range(r.lower - e, r.lower + e, bounds="()"),
        lambda r, e: Range(r.lower, r.upper, bounds="[]"),
        lambda r, e: Range(r.lower, r.upper, bounds="()"),
        argnames="r2t",
    )
    def test_intersection(self, connection, r1t, r2t):
        r1 = r1t(self._data_obj(), self._epsilon)
        r2 = r2t(self._data_obj(), self._epsilon)

        RANGE = self._col_type
        range_typ = self._col_str

        q = select(
            cast(r1, RANGE).intersection(r2),
        )
        validate_q = select(
            literal_column(f"'{r1}'::{range_typ}*'{r2}'::{range_typ}", RANGE),
        )

        pg_res = connection.execute(q).scalar()

        validate_intersection = connection.execute(validate_q).scalar()
        eq_(pg_res, validate_intersection)
        py_res = r1.intersection(r2)
        eq_(
            py_res,
            pg_res,
            f"{r1}.intersection({r2}): got {py_res}, expected {pg_res}",
        )

    @testing.combinations(
        *_common_ranges_to_test,
        lambda r, e: Range(r.lower, r.lower, bounds="[]"),
        argnames="r1t",
    )
    @testing.combinations(
        *_common_ranges_to_test,
        lambda r, e: Range(r.lower, r.lower, bounds="[]"),
        lambda r, e: Range(r.lower, r.lower + e, bounds="[)"),
        lambda r, e: Range(r.lower - e, r.lower, bounds="(]"),
        lambda r, e: Range(r.lower - e, r.lower + e, bounds="()"),
        argnames="r2t",
    )
    def test_equality(self, connection, r1t, r2t):
        r1 = r1t(self._data_obj(), self._epsilon)
        r2 = r2t(self._data_obj(), self._epsilon)

        range_typ = self._col_str

        q = select(
            literal_column(f"'{r1}'::{range_typ} = '{r2}'::{range_typ}")
        )
        equal = connection.execute(q).scalar()
        eq_(r1 == r2, equal, f"{r1} == {r2}: got {r1 == r2}, expected {equal}")

        q = select(
            literal_column(f"'{r1}'::{range_typ} <> '{r2}'::{range_typ}")
        )
        different = connection.execute(q).scalar()
        eq_(
            r1 != r2,
            different,
            f"{r1} != {r2}: got {r1 != r2}, expected {different}",
        )

    def test_bool(self):
        is_false(bool(Range(empty=True)))
        is_true(bool(Range(1, 2)))


class _RangeTypeRoundTrip(_RangeComparisonFixtures, fixtures.TablesTest):
    __requires__ = ("range_types",)
    __backend__ = True

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

    def test_stringify(self):
        eq_(str(self._data_obj()), self._data_str())

    def test_auto_cast_back_to_type(self, connection):
        """test that a straight pass of the range type without any context
        will send appropriate casting info so that the driver can round
        trip it.

        This doesn't happen in general across other backends and not for
        types like JSON etc., although perhaps it should, as we now have
        pretty straightforward infrastructure to turn it on; asyncpg
        for example does cast JSONs now in place.  But that's a
        bigger issue; for PG ranges it's likely useful to do this for
        PG backends as this is a fairly narrow use case.

        Brought up in #8540.

        """
        # see also CompileTest::test_range_custom_object_hook
        data_obj = self._data_obj()
        stmt = select(literal(data_obj, type_=self._col_type))
        round_trip = connection.scalar(stmt)
        eq_(round_trip, data_obj)

    def test_auto_cast_back_to_type_without_type(self, connection):
        """use _resolve_for_literal to cast"""
        # see also CompileTest::test_range_custom_object_hook
        data_obj = self._data_obj()
        lit = literal(data_obj)
        round_trip = connection.scalar(select(lit))
        eq_(round_trip, data_obj)
        eq_(type(lit.type), self._col_type)

    def test_actual_type(self):
        eq_(str(self._col_type()), self._col_str)

    def test_reflect(self, connection):
        from sqlalchemy import inspect

        insp = inspect(connection)
        cols = insp.get_columns("data_table")
        assert isinstance(cols[0]["type"], self._col_type)

    def test_type_decorator_round_trip(self, connection, metadata):
        """test #9020"""

        class MyRange(TypeDecorator):
            cache_ok = True
            impl = self._col_type

        table = Table(
            "typedec_table",
            metadata,
            Column("range", MyRange, primary_key=True),
        )
        table.create(connection)
        connection.execute(table.insert(), {"range": self._data_obj()})
        data = connection.execute(
            select(table.c.range).where(table.c.range == self._data_obj())
        ).fetchall()
        eq_(data, [(self._data_obj(),)])

    def test_textual_round_trip_w_dialect_type(self, connection):
        """test #8690"""
        data_table = self.tables.data_table

        data_obj = self._data_obj()
        connection.execute(
            self.tables.data_table.insert(), {"range": data_obj}
        )

        q1 = text("SELECT range from data_table")
        v = connection.scalar(q1)

        q2 = select(data_table).where(data_table.c.range == v)
        v2 = connection.scalar(q2)

        eq_(data_obj, v2)

    def _assert_data(self, conn):
        data = conn.execute(select(self.tables.data_table.c.range)).fetchall()
        eq_(data, [(self._data_obj(),)])

    def test_insert_obj(self, connection):
        connection.execute(
            self.tables.data_table.insert(), {"range": self._data_obj()}
        )
        self._assert_data(connection)

    @testing.requires.psycopg_or_pg8000_compatibility
    def test_insert_text(self, connection):
        connection.execute(
            self.tables.data_table.insert(), {"range": self._data_str()}
        )
        self._assert_data(connection)

    def test_union_result_obj(self, connection):
        # insert
        connection.execute(
            self.tables.data_table.insert(), {"range": self._data_obj()}
        )
        # select
        range_ = self.tables.data_table.c.range
        data = connection.execute(select(range_ + range_)).fetchall()
        eq_(data, [(self._data_obj(),)])

    @testing.requires.psycopg_or_pg8000_compatibility
    def test_union_result_text(self, connection):
        # insert
        connection.execute(
            self.tables.data_table.insert(), {"range": self._data_str()}
        )
        # select
        range_ = self.tables.data_table.c.range
        data = connection.execute(select(range_ + range_)).fetchall()
        eq_(data, [(self._data_obj(),)])

    def test_intersection_result_obj(self, connection):
        # insert
        connection.execute(
            self.tables.data_table.insert(), {"range": self._data_obj()}
        )
        # select
        range_ = self.tables.data_table.c.range
        data = connection.execute(select(range_ * range_)).fetchall()
        eq_(data, [(self._data_obj(),)])

    @testing.requires.psycopg_or_pg8000_compatibility
    def test_intersection_result_text(self, connection):
        # insert
        connection.execute(
            self.tables.data_table.insert(), {"range": self._data_str()}
        )
        # select
        range_ = self.tables.data_table.c.range
        data = connection.execute(select(range_ * range_)).fetchall()
        eq_(data, [(self._data_obj(),)])

    def test_difference_result_obj(self, connection):
        # insert
        connection.execute(
            self.tables.data_table.insert(), {"range": self._data_obj()}
        )
        # select
        range_ = self.tables.data_table.c.range
        data = connection.execute(select(range_ - range_)).fetchall()
        eq_(data, [(self._data_obj().__class__(empty=True),)])

    @testing.requires.psycopg_or_pg8000_compatibility
    def test_difference_result_text(self, connection):
        # insert
        connection.execute(
            self.tables.data_table.insert(), {"range": self._data_str()}
        )
        # select
        range_ = self.tables.data_table.c.range
        data = connection.execute(select(range_ - range_)).fetchall()
        eq_(data, [(self._data_obj().__class__(empty=True),)])


class _Int4RangeTests:
    _col_type = INT4RANGE
    _col_str = "INT4RANGE"
    _col_str_arr = "INT8RANGE"

    def _data_str(self):
        return "[1,4)"

    def _data_obj(self):
        return Range(1, 4)

    _epsilon = 1

    def _step_value_up(self, value):
        return value + 1

    def _step_value_down(self, value):
        return value - 1


class _Int8RangeTests:
    _col_type = INT8RANGE
    _col_str = "INT8RANGE"

    def _data_str(self):
        return "[9223372036854775306,9223372036854775800)"

    def _data_obj(self):
        return Range(9223372036854775306, 9223372036854775800)

    _epsilon = 1

    def _step_value_up(self, value):
        return value + 5

    def _step_value_down(self, value):
        return value - 5


class _NumRangeTests:
    _col_type = NUMRANGE
    _col_str = "NUMRANGE"

    def _data_str(self):
        return "[1.0,9.0)"

    def _data_obj(self):
        return Range(decimal.Decimal("1.0"), decimal.Decimal("9.0"))

    _epsilon = decimal.Decimal(1)

    def _step_value_up(self, value):
        return value + decimal.Decimal("1.8")

    def _step_value_down(self, value):
        return value - decimal.Decimal("1.8")


class _DateRangeTests:
    _col_type = DATERANGE
    _col_str = "DATERANGE"

    def _data_str(self):
        return "[2013-03-23,2013-03-30)"

    def _data_obj(self):
        return Range(datetime.date(2013, 3, 23), datetime.date(2013, 3, 30))

    _epsilon = datetime.timedelta(days=1)

    def _step_value_up(self, value):
        return value + datetime.timedelta(days=1)

    def _step_value_down(self, value):
        return value - datetime.timedelta(days=1)


class _DateTimeRangeTests:
    _col_type = TSRANGE
    _col_str = "TSRANGE"

    def _data_str(self):
        return "[2013-03-23 14:30:00,2013-03-30 23:30:00)"

    def _data_obj(self):
        return Range(
            datetime.datetime(2013, 3, 23, 14, 30),
            datetime.datetime(2013, 3, 30, 23, 30),
        )

    _epsilon = datetime.timedelta(days=1)

    def _step_value_up(self, value):
        return value + datetime.timedelta(days=1)

    def _step_value_down(self, value):
        return value - datetime.timedelta(days=1)


class _DateTimeTZRangeTests:
    _col_type = TSTZRANGE
    _col_str = "TSTZRANGE"

    def tstzs(self):
        tz = datetime.timezone(-datetime.timedelta(hours=5, minutes=30))

        return (
            datetime.datetime(2013, 3, 23, 14, 30, tzinfo=tz),
            datetime.datetime(2013, 3, 30, 23, 30, tzinfo=tz),
        )

    def _data_str(self):
        l, r = self.tstzs()
        return f"[{l},{r})"

    def _data_obj(self):
        return Range(*self.tstzs())

    _epsilon = datetime.timedelta(days=1)

    def _step_value_up(self, value):
        return value + datetime.timedelta(days=1)

    def _step_value_down(self, value):
        return value - datetime.timedelta(days=1)


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


class _MultiRangeTypeCompilation(AssertsCompiledSQL, fixtures.TestBase):
    __dialect__ = "postgresql"

    # operator tests

    @classmethod
    def setup_test_class(cls):
        table = Table(
            "data_table",
            MetaData(),
            Column("multirange", cls._col_type, primary_key=True),
        )
        cls.col = table.c.multirange

    def _test_clause(self, colclause, expected, type_):
        self.assert_compile(colclause, expected)
        is_(colclause.type._type_affinity, type_._type_affinity)

    def test_where_equal(self):
        self._test_clause(
            self.col == self._data_str(),
            "data_table.multirange = %(multirange_1)s",
            sqltypes.BOOLEANTYPE,
        )

    def test_where_equal_obj(self):
        self._test_clause(
            self.col == self._data_obj(),
            f"data_table.multirange = %(multirange_1)s::{self._col_str}",
            sqltypes.BOOLEANTYPE,
        )

    def test_where_not_equal(self):
        self._test_clause(
            self.col != self._data_str(),
            "data_table.multirange != %(multirange_1)s",
            sqltypes.BOOLEANTYPE,
        )

    def test_where_not_equal_obj(self):
        self._test_clause(
            self.col != self._data_obj(),
            f"data_table.multirange != %(multirange_1)s::{self._col_str}",
            sqltypes.BOOLEANTYPE,
        )

    def test_where_is_null(self):
        self._test_clause(
            self.col == None,
            "data_table.multirange IS NULL",
            sqltypes.BOOLEANTYPE,
        )

    def test_where_is_not_null(self):
        self._test_clause(
            self.col != None,
            "data_table.multirange IS NOT NULL",
            sqltypes.BOOLEANTYPE,
        )

    def test_where_less_than(self):
        self._test_clause(
            self.col < self._data_str(),
            "data_table.multirange < %(multirange_1)s",
            sqltypes.BOOLEANTYPE,
        )

    def test_where_greater_than(self):
        self._test_clause(
            self.col > self._data_str(),
            "data_table.multirange > %(multirange_1)s",
            sqltypes.BOOLEANTYPE,
        )

    def test_where_less_than_or_equal(self):
        self._test_clause(
            self.col <= self._data_str(),
            "data_table.multirange <= %(multirange_1)s",
            sqltypes.BOOLEANTYPE,
        )

    def test_where_greater_than_or_equal(self):
        self._test_clause(
            self.col >= self._data_str(),
            "data_table.multirange >= %(multirange_1)s",
            sqltypes.BOOLEANTYPE,
        )

    def test_contains(self):
        self._test_clause(
            self.col.contains(self._data_str()),
            "data_table.multirange @> %(multirange_1)s",
            sqltypes.BOOLEANTYPE,
        )

    def test_contained_by(self):
        self._test_clause(
            self.col.contained_by(self._data_str()),
            "data_table.multirange <@ %(multirange_1)s",
            sqltypes.BOOLEANTYPE,
        )

    def test_contained_by_obj(self):
        self._test_clause(
            self.col.contained_by(self._data_obj()),
            f"data_table.multirange <@ %(multirange_1)s::{self._col_str}",
            sqltypes.BOOLEANTYPE,
        )

    def test_overlaps(self):
        self._test_clause(
            self.col.overlaps(self._data_str()),
            "data_table.multirange && %(multirange_1)s",
            sqltypes.BOOLEANTYPE,
        )

    def test_strictly_left_of(self):
        self._test_clause(
            self.col << self._data_str(),
            "data_table.multirange << %(multirange_1)s",
            sqltypes.BOOLEANTYPE,
        )
        self._test_clause(
            self.col.strictly_left_of(self._data_str()),
            "data_table.multirange << %(multirange_1)s",
            sqltypes.BOOLEANTYPE,
        )

    def test_strictly_right_of(self):
        self._test_clause(
            self.col >> self._data_str(),
            "data_table.multirange >> %(multirange_1)s",
            sqltypes.BOOLEANTYPE,
        )
        self._test_clause(
            self.col.strictly_right_of(self._data_str()),
            "data_table.multirange >> %(multirange_1)s",
            sqltypes.BOOLEANTYPE,
        )

    def test_not_extend_right_of(self):
        self._test_clause(
            self.col.not_extend_right_of(self._data_str()),
            "data_table.multirange &< %(multirange_1)s",
            sqltypes.BOOLEANTYPE,
        )

    def test_not_extend_left_of(self):
        self._test_clause(
            self.col.not_extend_left_of(self._data_str()),
            "data_table.multirange &> %(multirange_1)s",
            sqltypes.BOOLEANTYPE,
        )

    def test_adjacent_to(self):
        self._test_clause(
            self.col.adjacent_to(self._data_str()),
            "data_table.multirange -|- %(multirange_1)s",
            sqltypes.BOOLEANTYPE,
        )

    def test_adjacent_to_obj(self):
        self._test_clause(
            self.col.adjacent_to(self._data_obj()),
            f"data_table.multirange -|- %(multirange_1)s::{self._col_str}",
            sqltypes.BOOLEANTYPE,
        )

    def test_union(self):
        self._test_clause(
            self.col + self.col,
            "data_table.multirange + data_table.multirange",
            self.col.type,
        )

    def test_intersection(self):
        self._test_clause(
            self.col * self.col,
            "data_table.multirange * data_table.multirange",
            self.col.type,
        )

    def test_difference(self):
        self._test_clause(
            self.col - self.col,
            "data_table.multirange - data_table.multirange",
            self.col.type,
        )


class _MultiRangeTypeRoundTrip(fixtures.TablesTest, _RangeTests):
    __requires__ = ("multirange_types",)
    __backend__ = True

    @testing.fixture(params=(True, False), ids=["multirange", "plain_list"])
    def data_obj(self, request):
        if request.param:
            return MultiRange(self._data_obj())
        else:
            return list(self._data_obj())

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

    def test_auto_cast_back_to_type(self, connection, data_obj):
        """test that a straight pass of the range type without any context
        will send appropriate casting info so that the driver can round
        trip it.

        This doesn't happen in general across other backends and not for
        types like JSON etc., although perhaps it should, as we now have
        pretty straightforward infrastructure to turn it on; asyncpg
        for example does cast JSONs now in place.  But that's a
        bigger issue; for PG ranges it's likely useful to do this for
        PG backends as this is a fairly narrow use case.

        Brought up in #8540.

        """
        # see also CompileTest::test_multirange_custom_object_hook
        stmt = select(literal(data_obj, type_=self._col_type))
        round_trip = connection.scalar(stmt)
        eq_(round_trip, data_obj)

    def test_auto_cast_back_to_type_without_type(self, connection):
        """use _resolve_for_literal to cast"""
        # see also CompileTest::test_multirange_custom_object_hook
        data_obj = MultiRange(self._data_obj())
        lit = literal(data_obj)
        round_trip = connection.scalar(select(lit))
        eq_(round_trip, data_obj)
        eq_(type(lit.type), self._col_type)

    @testing.fails("no automatic adaptation of plain list")
    def test_auto_cast_back_to_type_without_type_plain_list(self, connection):
        """use _resolve_for_literal to cast"""
        # see also CompileTest::test_multirange_custom_object_hook
        data_obj = list(self._data_obj())
        lit = literal(data_obj)
        r = connection.scalar(select(lit))
        eq_(type(r), list)

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
        eq_(type(data[0][0]), MultiRange)

    def test_textual_round_trip_w_dialect_type(self, connection, data_obj):
        """test #8690"""
        data_table = self.tables.data_table

        connection.execute(
            self.tables.data_table.insert(), {"range": data_obj}
        )

        q1 = text("SELECT range from data_table")
        v = connection.scalar(q1)

        q2 = select(data_table).where(data_table.c.range == v)
        v2 = connection.scalar(q2)

        eq_(data_obj, v2)

    def test_insert_obj(self, connection, data_obj):
        connection.execute(
            self.tables.data_table.insert(), {"range": data_obj}
        )
        self._assert_data(connection)

    @testing.requires.psycopg_or_pg8000_compatibility
    def test_insert_text(self, connection):
        connection.execute(
            self.tables.data_table.insert(), {"range": self._data_str()}
        )
        self._assert_data(connection)

    @testing.requires.psycopg_or_pg8000_compatibility
    def test_union_result_text(self, connection):
        # insert
        connection.execute(
            self.tables.data_table.insert(), {"range": self._data_str()}
        )
        # select
        range_ = self.tables.data_table.c.range
        data = connection.execute(select(range_ + range_)).fetchall()
        eq_(data, [(self._data_obj(),)])
        eq_(type(data[0][0]), MultiRange)

    @testing.requires.psycopg_or_pg8000_compatibility
    def test_intersection_result_text(self, connection):
        # insert
        connection.execute(
            self.tables.data_table.insert(), {"range": self._data_str()}
        )
        # select
        range_ = self.tables.data_table.c.range
        data = connection.execute(select(range_ * range_)).fetchall()
        eq_(data, [(self._data_obj(),)])
        eq_(type(data[0][0]), MultiRange)

    @testing.requires.psycopg_or_pg8000_compatibility
    def test_difference_result_text(self, connection):
        # insert
        connection.execute(
            self.tables.data_table.insert(), {"range": self._data_str()}
        )
        # select
        range_ = self.tables.data_table.c.range
        data = connection.execute(select(range_ - range_)).fetchall()
        eq_(data, [([],)])
        eq_(type(data[0][0]), MultiRange)


class _Int4MultiRangeTests:
    _col_type = INT4MULTIRANGE
    _col_str = "INT4MULTIRANGE"

    def _data_str(self):
        return "{[1,2), [3, 5), [9, 12)}"

    def _data_obj(self):
        return [Range(1, 2), Range(3, 5), Range(9, 12)]


class _Int8MultiRangeTests:
    _col_type = INT8MULTIRANGE
    _col_str = "INT8MULTIRANGE"

    def _data_str(self):
        return (
            "{[9223372036854775801,9223372036854775803),"
            + "[9223372036854775805,9223372036854775807)}"
        )

    def _data_obj(self):
        return [
            Range(9223372036854775801, 9223372036854775803),
            Range(9223372036854775805, 9223372036854775807),
        ]


class _NumMultiRangeTests:
    _col_type = NUMMULTIRANGE
    _col_str = "NUMMULTIRANGE"

    def _data_str(self):
        return "{[1.0,2.0), [3.0, 5.0), [9.0, 12.0)}"

    def _data_obj(self):
        return [
            Range(decimal.Decimal("1.0"), decimal.Decimal("2.0")),
            Range(decimal.Decimal("3.0"), decimal.Decimal("5.0")),
            Range(decimal.Decimal("9.0"), decimal.Decimal("12.0")),
        ]


class _DateMultiRangeTests:
    _col_type = DATEMULTIRANGE
    _col_str = "DATEMULTIRANGE"

    def _data_str(self):
        return "{[2013-03-23,2013-03-24), [2014-05-23,2014-05-24)}"

    def _data_obj(self):
        return [
            Range(datetime.date(2013, 3, 23), datetime.date(2013, 3, 24)),
            Range(datetime.date(2014, 5, 23), datetime.date(2014, 5, 24)),
        ]


class _DateTimeMultiRangeTests:
    _col_type = TSMULTIRANGE
    _col_str = "TSMULTIRANGE"

    def _data_str(self):
        return (
            "{[2013-03-23 14:30,2013-03-23 23:30),"
            + "[2014-05-23 14:30,2014-05-23 23:30)}"
        )

    def _data_obj(self):
        return [
            Range(
                datetime.datetime(2013, 3, 23, 14, 30),
                datetime.datetime(2013, 3, 23, 23, 30),
            ),
            Range(
                datetime.datetime(2014, 5, 23, 14, 30),
                datetime.datetime(2014, 5, 23, 23, 30),
            ),
        ]


class _DateTimeTZMultiRangeTests:
    _col_type = TSTZMULTIRANGE
    _col_str = "TSTZMULTIRANGE"

    __only_on__ = "postgresql"

    # make sure we use one, steady timestamp with timezone pair
    # for all parts of all these tests
    _tstzs = None
    _tstzs_delta = None

    def tstzs(self):
        # note this was hitting DST issues when these tests were using a
        # live date and running on or near 2024-03-09 :).   hardcoded to a
        # date a few days earlier
        utc_now = datetime.datetime(
            2024, 3, 2, 14, 57, 50, 473566, tzinfo=datetime.timezone.utc
        )

        if self._tstzs is None:
            lower = utc_now
            upper = lower + datetime.timedelta(1)
            self._tstzs = (lower, upper)
        return self._tstzs

    def tstzs_delta(self):
        utc_now = datetime.datetime(
            2024, 3, 2, 14, 57, 50, 473566, tzinfo=datetime.timezone.utc
        )

        if self._tstzs_delta is None:
            lower = utc_now + datetime.timedelta(3)
            upper = lower + datetime.timedelta(2)
            self._tstzs_delta = (lower, upper)

        return self._tstzs_delta

    def _data_str(self):
        tstzs_lower, tstzs_upper = self.tstzs()
        tstzs_delta_lower, tstzs_delta_upper = self.tstzs_delta()
        return "{{[{tl},{tu}), [{tdl},{tdu})}}".format(
            tl=tstzs_lower,
            tu=tstzs_upper,
            tdl=tstzs_delta_lower,
            tdu=tstzs_delta_upper,
        )

    def _data_obj(self):
        return [
            Range(*self.tstzs()),
            Range(*self.tstzs_delta()),
        ]


class Int4MultiRangeCompilationTest(
    _Int4MultiRangeTests, _MultiRangeTypeCompilation
):
    pass


class Int4MultiRangeRoundTripTest(
    _Int4MultiRangeTests, _MultiRangeTypeRoundTrip
):
    pass


class Int8MultiRangeCompilationTest(
    _Int8MultiRangeTests, _MultiRangeTypeCompilation
):
    pass


class Int8MultiRangeRoundTripTest(
    _Int8MultiRangeTests, _MultiRangeTypeRoundTrip
):
    pass


class NumMultiRangeCompilationTest(
    _NumMultiRangeTests, _MultiRangeTypeCompilation
):
    pass


class NumMultiRangeRoundTripTest(
    _NumMultiRangeTests, _MultiRangeTypeRoundTrip
):
    pass


class DateMultiRangeCompilationTest(
    _DateMultiRangeTests, _MultiRangeTypeCompilation
):
    pass


class DateMultiRangeRoundTripTest(
    _DateMultiRangeTests, _MultiRangeTypeRoundTrip
):
    pass


class DateTimeMultiRangeCompilationTest(
    _DateTimeMultiRangeTests, _MultiRangeTypeCompilation
):
    pass


class DateTimeMultiRangeRoundTripTest(
    _DateTimeMultiRangeTests, _MultiRangeTypeRoundTrip
):
    pass


class DateTimeTZMultiRangeCompilationTest(
    _DateTimeTZMultiRangeTests, _MultiRangeTypeCompilation
):
    pass


class DateTimeTZRMultiangeRoundTripTest(
    _DateTimeTZMultiRangeTests, _MultiRangeTypeRoundTrip
):
    pass


class MultiRangeSequenceTest(fixtures.TestBase):
    def test_methods(self):
        plain = [Range(1, 3), Range(5, 9)]
        multi = MultiRange(plain)
        is_true(isinstance(multi, list))
        eq_(multi, plain)
        ne_(multi, plain[:1])
        eq_(str(multi), str(plain))
        eq_(repr(multi), repr(plain))


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

    @property
    def any_(self):
        return any_(array([7]))

    # Test combinations that use path (#>) and astext (->> and #>>) operators
    # These don't change between JSON and JSONB
    @testing.combinations(
        (
            lambda self: self.jsoncol[("foo", 1)] == None,  # noqa
            "(test_table.test_column #> %(test_column_1)s) IS NULL",
        ),
        (
            lambda self: self.jsoncol[("foo", 1)] != None,  # noqa
            "(test_table.test_column #> %(test_column_1)s) IS NOT NULL",
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
            lambda self: self.jsoncol[("foo", 1)].astext == None,  # noqa
            "(test_table.test_column #>> %(test_column_1)s) IS NULL",
        ),
        (
            lambda self: self.jsoncol["bar"].astext == self.any_,
            "(test_table.test_column ->> %(test_column_1)s) = "
            "ANY (ARRAY[%(param_1)s])",
        ),
        (
            lambda self: self.jsoncol["bar"].astext != self.any_,
            "(test_table.test_column ->> %(test_column_1)s) != "
            "ANY (ARRAY[%(param_1)s])",
        ),
        (
            lambda self: self.jsoncol[("foo", 1)] == self.any_,
            "(test_table.test_column #> %(test_column_1)s) = "
            "ANY (ARRAY[%(param_1)s])",
        ),
        (
            lambda self: self.jsoncol[("foo", 1)] != self.any_,
            "(test_table.test_column #> %(test_column_1)s) != "
            "ANY (ARRAY[%(param_1)s])",
        ),
        id_="as",
    )
    def test_where(self, whereclause_fn, expected):
        whereclause = whereclause_fn(self)
        stmt = select(self.test_table).where(whereclause)
        self.assert_compile(
            stmt,
            "SELECT test_table.id, test_table.test_column FROM test_table "
            "WHERE %s" % expected,
        )

    # Test combinations that use subscript (->) operator
    # These differ between JSON (always ->) and JSONB ([] on PG 14+)
    @testing.combinations(
        (
            lambda self: self.jsoncol["bar"] == None,  # noqa
            "(test_table.test_column -> %(test_column_1)s) IS NULL",
        ),
        (
            lambda self: self.jsoncol["bar"] != None,  # noqa
            "(test_table.test_column -> %(test_column_1)s) IS NOT NULL",
        ),
        (
            lambda self: self.jsoncol["bar"].cast(Integer) == 5,
            "CAST((test_table.test_column -> %(test_column_1)s) AS INTEGER) "
            "= %(param_1)s",
        ),
        (
            lambda self: self.jsoncol["bar"] == 42,
            "(test_table.test_column -> %(test_column_1)s) = %(param_1)s",
        ),
        (
            lambda self: self.jsoncol["bar"] != 42,
            "(test_table.test_column -> %(test_column_1)s) != %(param_1)s",
        ),
        (
            lambda self: self.jsoncol["bar"] == self.any_,
            "(test_table.test_column -> %(test_column_1)s) = "
            "ANY (ARRAY[%(param_1)s])",
        ),
        (
            lambda self: self.jsoncol["bar"] != self.any_,
            "(test_table.test_column -> %(test_column_1)s) != "
            "ANY (ARRAY[%(param_1)s])",
        ),
        id_="as",
    )
    def test_where_subscript(self, whereclause_fn, expected):
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

    # Test column selection that uses subscript (->) operator
    # This differs between JSON (always ->) and JSONB ([] on PG 14+)
    @testing.combinations(
        (
            lambda self: self.jsoncol["foo"],
            "test_table.test_column -> %(test_column_1)s AS anon_1",
            True,
        )
    )
    def test_cols_subscript(self, colclause_fn, expected, from_):
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

    @property
    def data_table(self):
        return self.tables.data_table

    def _fixture_data(self, connection):
        data = [
            {"name": "r1", "data": {"k1": "r1v1", "k2": "r1v2"}},
            {"name": "r2", "data": {"k1": "r2v1", "k2": "r2v2"}},
            {"name": "r3", "data": {"k1": "r3v1", "k2": "r3v2"}},
            {"name": "r4", "data": {"k1": "r4v1", "k2": "r4v2"}},
            {"name": "r5", "data": {"k1": "r5v1", "k2": "r5v2", "k3": 5}},
            {"name": "r6", "data": {"k1": {"r6v1": {"subr": [1, 2, 3]}}}},
        ]
        connection.execute(self.data_table.insert(), data)
        return data

    def _assert_data(self, compare, conn, column="data"):
        col = self.data_table.c[column]
        data = conn.execute(
            select(col).order_by(self.data_table.c.name)
        ).fetchall()
        eq_([d for d, in data], compare)

    def _assert_column_is_NULL(self, conn, column="data"):
        col = self.data_table.c[column]
        data = conn.execute(select(col).where(col.is_(null()))).fetchall()
        eq_([d for d, in data], [None])

    def _assert_column_is_JSON_NULL(self, conn, column="data"):
        col = self.data_table.c[column]
        data = conn.execute(
            select(col).where(cast(col, String) == "null")
        ).fetchall()
        eq_([d for d, in data], [None])

    @testing.combinations(
        "key",
        "réve🐍 illé",
        'name_with"quotes"name',
        "name with spaces",
        "name with ' single ' quotes",
        'some_key("idx")',
        argnames="key",
    )
    def test_indexed_special_keys(self, connection, key):
        data_table = self.data_table
        data_element = {key: "some value"}

        connection.execute(
            data_table.insert(),
            {
                "name": "row1",
                "data": data_element,
                "nulldata": data_element,
            },
        )

        row = connection.execute(
            select(data_table.c.data[key], data_table.c.nulldata[key])
        ).one()
        eq_(row, ("some value", "some value"))

    def test_reflect(self, connection):
        insp = inspect(connection)
        cols = insp.get_columns("data_table")
        assert isinstance(cols[2]["type"], self.data_type)

    def test_insert(self, connection):
        connection.execute(
            self.data_table.insert(),
            {"name": "r1", "data": {"k1": "r1v1", "k2": "r1v2"}},
        )
        self._assert_data([{"k1": "r1v1", "k2": "r1v2"}], connection)

    def test_insert_nulls(self, connection):
        connection.execute(
            self.data_table.insert(), {"name": "r1", "data": null()}
        )
        self._assert_data([None], connection)

    def test_insert_none_as_null(self, connection):
        connection.execute(
            self.data_table.insert(),
            {"name": "r1", "nulldata": None},
        )
        self._assert_column_is_NULL(connection, column="nulldata")

    def test_insert_nulljson_into_none_as_null(self, connection):
        connection.execute(
            self.data_table.insert(),
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
        result = connection.execute(
            select(self.data_table.c.data["k1"].astext)
        ).first()
        assert isinstance(result[0], str)

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
                    "réveillé": "réveillé",
                    "data": {"k1": "drôle"},
                },
                self.data_type,
            )
        )
        eq_(
            connection.scalar(s),
            {
                "réveillé": "réveillé",
                "data": {"k1": "drôle"},
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
            lambda self: self.jsoncol.has_key("data"),
            "test_table.test_column ? %(test_column_1)s",
        ),
        (
            lambda self: self.jsoncol.has_key(self.any_),
            "test_table.test_column ? ANY (ARRAY[%(param_1)s])",
        ),
        (
            lambda self: self.jsoncol.has_all(
                {"name": "r1", "data": {"k1": "r1v1", "k2": "r1v2"}}
            ),
            "test_table.test_column ?& %(test_column_1)s::JSONB",
        ),
        (
            lambda self: self.jsoncol.has_all(self.any_),
            "test_table.test_column ?& ANY (ARRAY[%(param_1)s])",
        ),
        (
            lambda self: self.jsoncol.has_any(
                postgresql.array(["name", "data"])
            ),
            "test_table.test_column ?| ARRAY[%(param_1)s, %(param_2)s]",
        ),
        (
            lambda self: self.jsoncol.has_any(self.any_),
            "test_table.test_column ?| ANY (ARRAY[%(param_1)s])",
        ),
        (
            lambda self: self.jsoncol.contains({"k1": "r1v1"}),
            "test_table.test_column @> %(test_column_1)s::JSONB",
        ),
        (
            lambda self: self.jsoncol.contains(self.any_),
            "test_table.test_column @> ANY (ARRAY[%(param_1)s])",
        ),
        (
            lambda self: self.jsoncol.contained_by({"foo": "1", "bar": None}),
            "test_table.test_column <@ %(test_column_1)s::JSONB",
        ),
        (
            lambda self: self.jsoncol.contained_by(self.any_),
            "test_table.test_column <@ ANY (ARRAY[%(param_1)s])",
        ),
        (
            lambda self: self.jsoncol.delete_path(["a", "b"]),
            "test_table.test_column #- CAST(ARRAY[%(param_1)s, "
            "%(param_2)s] AS TEXT[])",
        ),
        (
            lambda self: self.jsoncol.delete_path(array(["a", "b"])),
            "test_table.test_column #- CAST(ARRAY[%(param_1)s, "
            "%(param_2)s] AS TEXT[])",
        ),
        (
            lambda self: self.jsoncol.path_exists("$.k1"),
            "test_table.test_column @? %(test_column_1)s",
        ),
        (
            lambda self: self.jsoncol.path_exists(self.any_),
            "test_table.test_column @? ANY (ARRAY[%(param_1)s])",
        ),
        (
            lambda self: self.jsoncol.path_match("$.k1[0] > 2"),
            "test_table.test_column @@ %(test_column_1)s",
        ),
        (
            lambda self: self.jsoncol.path_match(self.any_),
            "test_table.test_column @@ ANY (ARRAY[%(param_1)s])",
        ),
        id_="as",
    )
    def test_where_jsonb(self, whereclause_fn, expected):
        super().test_where(whereclause_fn, expected)

    # Override test_where_subscript to provide JSONB-specific expectations
    # JSONB uses subscript syntax (e.g., col['key']) on PostgreSQL 14+
    @testing.combinations(
        (
            lambda self: self.jsoncol["bar"] == None,  # noqa
            "test_table.test_column[%(test_column_1)s] IS NULL",
        ),
        (
            lambda self: self.jsoncol["bar"] != None,  # noqa
            "test_table.test_column[%(test_column_1)s] IS NOT NULL",
        ),
        (
            lambda self: self.jsoncol["bar"].cast(Integer) == 5,
            "CAST(test_table.test_column[%(test_column_1)s] AS INTEGER) "
            "= %(param_1)s",
        ),
        (
            lambda self: self.jsoncol["bar"] == 42,
            "test_table.test_column[%(test_column_1)s] = %(param_1)s",
        ),
        (
            lambda self: self.jsoncol["bar"] != 42,
            "test_table.test_column[%(test_column_1)s] != %(param_1)s",
        ),
        (
            lambda self: self.jsoncol["bar"] == self.any_,
            "test_table.test_column[%(test_column_1)s] = "
            "ANY (ARRAY[%(param_1)s])",
        ),
        (
            lambda self: self.jsoncol["bar"] != self.any_,
            "test_table.test_column[%(test_column_1)s] != "
            "ANY (ARRAY[%(param_1)s])",
        ),
        id_="as",
    )
    def test_where_subscript(self, whereclause_fn, expected):
        whereclause = whereclause_fn(self)
        stmt = select(self.test_table).where(whereclause)
        self.assert_compile(
            stmt,
            "SELECT test_table.id, test_table.test_column FROM test_table "
            "WHERE %s" % expected,
        )

    # Override test_cols_subscript to provide JSONB-specific expectations
    # JSONB uses subscript syntax (e.g., col['key']) on PostgreSQL 14+
    @testing.combinations(
        (
            lambda self: self.jsoncol["foo"],
            "test_table.test_column[%(test_column_1)s] AS anon_1",
            True,
        )
    )
    def test_cols_subscript(self, colclause_fn, expected, from_):
        colclause = colclause_fn(self)
        stmt = select(colclause)
        self.assert_compile(
            stmt,
            ("SELECT %s" + (" FROM test_table" if from_ else "")) % expected,
        )


class JSONBRoundTripTest(JSONRoundTripTest):
    __requires__ = ("postgresql_jsonb",)

    data_type = JSONB

    @testing.requires.postgresql_utf8_server_encoding
    def test_unicode_round_trip(self, connection):
        super().test_unicode_round_trip(connection)

    @testing.only_on("postgresql >= 12")
    def test_cast_jsonpath(self, connection):
        self._fixture_data(connection)

        def go(path, res):
            q = select(func.count("*")).where(
                func.jsonb_path_exists(
                    self.data_table.c.data, cast(path, JSONB.JSONPathType)
                )
            )
            eq_(connection.scalar(q), res)

        go("$.k1.k2", 0)
        go("$.k1.r6v1", 1)

    @testing.combinations(
        ["k1", "r6v1", "subr", 1],
        array(["k1", "r6v1", "subr", 1]),
        argnames="path",
    )
    def test_delete_path(self, connection, path):
        self._fixture_data(connection)
        q = select(self.data_table.c.data.delete_path(path)).where(
            self.data_table.c.name == "r6"
        )
        res = connection.scalar(q)
        eq_(res, {"k1": {"r6v1": {"subr": [1, 3]}}})


class JSONBSuiteTest(suite.JSONTest):
    __requires__ = ("postgresql_jsonb",)

    datatype = JSONB


class JSONBCastSuiteTest(suite.JSONLegacyStringCastIndexTest):
    __requires__ = ("postgresql_jsonb",)

    datatype = JSONB


class CITextTest(testing.AssertsCompiledSQL, fixtures.TablesTest):
    __requires__ = ("citext",)
    __only_on__ = "postgresql"
    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "ci_test_table",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("caseignore_text", CITEXT),
        )

    @testing.variation(
        "inserts",
        ["multiple", "single", "insertmanyvalues", "imv_deterministic"],
    )
    def test_citext_round_trip(self, connection, inserts):
        ci_test_table = self.tables.ci_test_table

        data = [
            {"caseignore_text": "Hello World"},
            {"caseignore_text": "greetings all"},
        ]

        if inserts.single:
            for d in data:
                connection.execute(
                    ci_test_table.insert(),
                    d,
                )
        elif inserts.multiple:
            connection.execute(ci_test_table.insert(), data)
        elif inserts.insertmanyvalues:
            result = connection.execute(
                ci_test_table.insert().returning(ci_test_table.c.id), data
            )
            result.all()
        elif inserts.imv_deterministic:
            result = connection.execute(
                ci_test_table.insert().returning(
                    ci_test_table.c.id, sort_by_parameter_order=True
                ),
                data,
            )
            result.all()
        else:
            inserts.fail()

        ret = connection.execute(
            select(func.count(ci_test_table.c.id)).where(
                ci_test_table.c.caseignore_text == "hello world"
            )
        ).scalar()

        eq_(ret, 1)

        ret = connection.execute(
            select(func.count(ci_test_table.c.id)).where(
                ci_test_table.c.caseignore_text == "Greetings All"
            )
        ).scalar()

        eq_(ret, 1)


class CITextCastTest(testing.AssertsCompiledSQL, fixtures.TestBase):
    @testing.combinations(
        (psycopg.dialect(),),
        (psycopg2.dialect(),),
        (asyncpg.dialect(),),
        (pg8000.dialect(),),
    )
    def test_cast(self, dialect):
        ci_test_table = Table(
            "ci_test_table",
            MetaData(),
            Column("id", Integer, primary_key=True),
            Column("caseignore_text", CITEXT),
        )

        stmt = select(ci_test_table).where(
            ci_test_table.c.caseignore_text == "xyz"
        )

        param = {
            "format": "%s",
            "numeric_dollar": "$1",
            "pyformat": "%(caseignore_text_1)s",
        }[dialect.paramstyle]
        expected = (
            "SELECT ci_test_table.id, ci_test_table.caseignore_text "
            "FROM ci_test_table WHERE "
            # currently CITEXT has render_bind_cast turned off.
            # if there's a need to turn it on, change as follows:
            # f"ci_test_table.caseignore_text = {param}::CITEXT"
            f"ci_test_table.caseignore_text = {param}"
        )
        self.assert_compile(stmt, expected, dialect=dialect)


class InetRoundTripTests(fixtures.TestBase):
    __backend__ = True
    __only_on__ = "postgresql"

    def _combinations():
        return testing.combinations(
            (
                postgresql.INET,
                lambda: [
                    "1.1.1.1",
                    "192.168.1.1",
                    "10.1.2.25",
                    "192.168.22.5",
                ],
                IPv4Address,
            ),
            (
                postgresql.INET,
                lambda: [
                    "2001:db8::1000",
                ],
                IPv6Address,
            ),
            (
                postgresql.CIDR,
                lambda: [
                    "10.0.0.0/8",
                    "192.168.1.0/24",
                    "192.168.0.0/16",
                    "192.168.1.25/32",
                ],
                IPv4Network,
            ),
            (
                postgresql.CIDR,
                lambda: [
                    "::ffff:1.2.3.0/120",
                ],
                IPv6Network,
            ),
            argnames="datatype,values,pytype",
        )

    @_combinations()
    def test_default_native_inet_types(
        self, datatype, values, pytype, connection, metadata
    ):
        t = Table(
            "t",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("data", datatype),
        )
        metadata.create_all(connection)

        connection.execute(
            t.insert(),
            [
                {"id": i, "data": val}
                for i, val in enumerate(values(), start=1)
            ],
        )

        if testing.against(["+psycopg", "+asyncpg"]) or (
            testing.against("+pg8000")
            and issubclass(datatype, postgresql.INET)
        ):
            eq_(
                connection.scalars(select(t.c.data).order_by(t.c.id)).all(),
                [pytype(val) for val in values()],
            )
        else:
            eq_(
                connection.scalars(select(t.c.data).order_by(t.c.id)).all(),
                values(),
            )

    @_combinations()
    def test_str_based_inet_handlers(
        self, datatype, values, pytype, testing_engine, metadata
    ):
        t = Table(
            "t",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("data", datatype),
        )

        e = testing_engine(options={"native_inet_types": False})
        with e.begin() as connection:
            metadata.create_all(connection)

            connection.execute(
                t.insert(),
                [
                    {"id": i, "data": val}
                    for i, val in enumerate(values(), start=1)
                ],
            )

        with e.connect() as connection:
            eq_(
                connection.scalars(select(t.c.data).order_by(t.c.id)).all(),
                values(),
            )

    @testing.only_on("+psycopg2")
    def test_not_impl_psycopg2(self, testing_engine):
        with expect_raises_message(
            NotImplementedError,
            "The psycopg2 dialect does not implement ipaddress type handling",
        ):
            testing_engine(options={"native_inet_types": True})

    @testing.only_on("+pg8000")
    def test_not_impl_pg8000(self, testing_engine):
        with expect_raises_message(
            NotImplementedError,
            "The pg8000 dialect does not fully implement "
            "ipaddress type handling",
        ):
            testing_engine(options={"native_inet_types": True})


class PGInsertManyValuesTest(fixtures.TestBase):
    """test pg-specific types for insertmanyvalues"""

    __only_on__ = "postgresql"
    __backend__ = True

    @testing.combinations(
        ("BYTEA", BYTEA(), b"7\xe7\x9f"),
        ("BIT", BIT(3), BitString("011")),
        argnames="type_,value",
        id_="iaa",
    )
    @testing.variation("sort_by_parameter_order", [True, False])
    @testing.variation("multiple_rows", [True, False])
    @testing.requires.insert_returning
    def test_imv_returning_datatypes(
        self,
        connection,
        metadata,
        sort_by_parameter_order,
        type_,
        value,
        multiple_rows,
    ):
        """test #9739, #9808 (similar to #9701) for PG specific types

        this tests insertmanyvalues in conjunction with various datatypes.

        These tests are particularly for the asyncpg driver which needs
        most types to be explicitly cast for the new IMV format

        """
        t = Table(
            "d_t",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("value", type_),
        )

        t.create(connection)

        result = connection.execute(
            t.insert().returning(
                t.c.id,
                t.c.value,
                sort_by_parameter_order=bool(sort_by_parameter_order),
            ),
            (
                [{"value": value} for i in range(10)]
                if multiple_rows
                else {"value": value}
            ),
        )

        if multiple_rows:
            i_range = range(1, 11)
        else:
            i_range = range(1, 2)

        eq_(
            set(result),
            {(id_, value) for id_ in i_range},
        )

        eq_(
            set(connection.scalars(select(t.c.value))),
            {value},
        )
