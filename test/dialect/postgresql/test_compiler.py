import random
import re

from sqlalchemy import all_
from sqlalchemy import and_
from sqlalchemy import any_
from sqlalchemy import BigInteger
from sqlalchemy import bindparam
from sqlalchemy import case
from sqlalchemy import cast
from sqlalchemy import CheckConstraint
from sqlalchemy import Column
from sqlalchemy import Computed
from sqlalchemy import Date
from sqlalchemy import delete
from sqlalchemy import Enum
from sqlalchemy import exc
from sqlalchemy import Float
from sqlalchemy import ForeignKey
from sqlalchemy import ForeignKeyConstraint
from sqlalchemy import func
from sqlalchemy import Identity
from sqlalchemy import Index
from sqlalchemy import Integer
from sqlalchemy import literal
from sqlalchemy import MetaData
from sqlalchemy import null
from sqlalchemy import PrimaryKeyConstraint
from sqlalchemy import schema
from sqlalchemy import select
from sqlalchemy import Sequence
from sqlalchemy import SmallInteger
from sqlalchemy import sql
from sqlalchemy import String
from sqlalchemy import Table
from sqlalchemy import testing
from sqlalchemy import Text
from sqlalchemy import text
from sqlalchemy import true
from sqlalchemy import tuple_
from sqlalchemy import types as sqltypes
from sqlalchemy import UniqueConstraint
from sqlalchemy import update
from sqlalchemy import VARCHAR
from sqlalchemy.dialects import postgresql
from sqlalchemy.dialects.postgresql import aggregate_order_by
from sqlalchemy.dialects.postgresql import ARRAY as PG_ARRAY
from sqlalchemy.dialects.postgresql import array
from sqlalchemy.dialects.postgresql import array_agg as pg_array_agg
from sqlalchemy.dialects.postgresql import distinct_on
from sqlalchemy.dialects.postgresql import DOMAIN
from sqlalchemy.dialects.postgresql import ExcludeConstraint
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.dialects.postgresql import JSON
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import JSONPATH
from sqlalchemy.dialects.postgresql import Range
from sqlalchemy.dialects.postgresql import REGCONFIG
from sqlalchemy.dialects.postgresql import TSQUERY
from sqlalchemy.dialects.postgresql import TSRANGE
from sqlalchemy.dialects.postgresql.base import PGDialect
from sqlalchemy.dialects.postgresql.psycopg2 import PGDialect_psycopg2
from sqlalchemy.dialects.postgresql.ranges import MultiRange
from sqlalchemy.orm import aliased
from sqlalchemy.orm import clear_mappers
from sqlalchemy.orm import Session
from sqlalchemy.sql import column
from sqlalchemy.sql import literal_column
from sqlalchemy.sql import operators
from sqlalchemy.sql import table
from sqlalchemy.sql import util as sql_util
from sqlalchemy.sql.functions import GenericFunction
from sqlalchemy.testing import expect_raises
from sqlalchemy.testing import expect_raises_message
from sqlalchemy.testing import fixtures
from sqlalchemy.testing.assertions import assert_raises
from sqlalchemy.testing.assertions import assert_raises_message
from sqlalchemy.testing.assertions import AssertsCompiledSQL
from sqlalchemy.testing.assertions import eq_
from sqlalchemy.testing.assertions import eq_ignore_whitespace
from sqlalchemy.testing.assertions import expect_deprecated
from sqlalchemy.testing.assertions import expect_warnings
from sqlalchemy.testing.assertions import is_
from sqlalchemy.testing.util import resolve_lambda
from sqlalchemy.types import TypeEngine
from sqlalchemy.util import OrderedDict


class SequenceTest(fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = "postgresql"

    def test_format(self):
        seq = Sequence("my_seq_no_schema")
        dialect = postgresql.dialect()
        assert (
            dialect.identifier_preparer.format_sequence(seq)
            == "my_seq_no_schema"
        )
        seq = Sequence("my_seq", schema="some_schema")
        assert (
            dialect.identifier_preparer.format_sequence(seq)
            == "some_schema.my_seq"
        )
        seq = Sequence("My_Seq", schema="Some_Schema")
        assert (
            dialect.identifier_preparer.format_sequence(seq)
            == '"Some_Schema"."My_Seq"'
        )

    @testing.combinations(
        (None, ""),
        (Integer, "AS INTEGER "),
        (SmallInteger, "AS SMALLINT "),
        (BigInteger, "AS BIGINT "),
    )
    def test_compile_type(self, type_, text):
        s = Sequence("s1", data_type=type_)
        self.assert_compile(
            schema.CreateSequence(s),
            f"CREATE SEQUENCE s1 {text}".strip(),
            dialect=postgresql.dialect(),
        )


class CompileTest(fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = postgresql.dialect()

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
            str(stmt.compile(dialect=postgresql.dialect())),
            "INSERT INTO t (description) VALUES (lower(%(lower_1)s)) "
            "RETURNING t.myid, t.name, t.description",
        )

    def test_update_returning(self):
        dialect = postgresql.dialect()
        table1 = table(
            "mytable",
            column("myid", Integer),
            column("name", String(128)),
            column("description", String(128)),
        )
        u = (
            update(table1)
            .values(dict(name="foo"))
            .returning(table1.c.myid, table1.c.name)
        )
        self.assert_compile(
            u,
            "UPDATE mytable SET name=%(name)s "
            "RETURNING mytable.myid, mytable.name",
            dialect=dialect,
        )
        u = update(table1).values(dict(name="foo")).returning(table1)
        self.assert_compile(
            u,
            "UPDATE mytable SET name=%(name)s "
            "RETURNING mytable.myid, mytable.name, "
            "mytable.description",
            dialect=dialect,
        )
        u = (
            update(table1)
            .values(dict(name="foo"))
            .returning(func.length(table1.c.name))
        )
        self.assert_compile(
            u,
            "UPDATE mytable SET name=%(name)s "
            "RETURNING length(mytable.name) AS length_1",
            dialect=dialect,
        )

    def test_insert_returning(self):
        dialect = postgresql.dialect()
        table1 = table(
            "mytable",
            column("myid", Integer),
            column("name", String(128)),
            column("description", String(128)),
        )

        i = (
            insert(table1)
            .values(dict(name="foo"))
            .returning(table1.c.myid, table1.c.name)
        )
        self.assert_compile(
            i,
            "INSERT INTO mytable (name) VALUES "
            "(%(name)s) RETURNING mytable.myid, "
            "mytable.name",
            dialect=dialect,
        )
        i = insert(table1).values(dict(name="foo")).returning(table1)
        self.assert_compile(
            i,
            "INSERT INTO mytable (name) VALUES "
            "(%(name)s) RETURNING mytable.myid, "
            "mytable.name, mytable.description",
            dialect=dialect,
        )
        i = (
            insert(table1)
            .values(dict(name="foo"))
            .returning(func.length(table1.c.name))
        )
        self.assert_compile(
            i,
            "INSERT INTO mytable (name) VALUES "
            "(%(name)s) RETURNING length(mytable.name) "
            "AS length_1",
            dialect=dialect,
        )

    @testing.fixture
    def column_expression_fixture(self):
        class MyString(TypeEngine):
            def column_expression(self, column):
                return func.lower(column)

        return table(
            "some_table", column("name", String), column("value", MyString)
        )

    @testing.combinations("columns", "table", argnames="use_columns")
    def test_plain_returning_column_expression(
        self, column_expression_fixture, use_columns
    ):
        """test #8770"""
        table1 = column_expression_fixture

        if use_columns == "columns":
            stmt = insert(table1).returning(table1)
        else:
            stmt = insert(table1).returning(table1.c.name, table1.c.value)

        self.assert_compile(
            stmt,
            "INSERT INTO some_table (name, value) "
            "VALUES (%(name)s, %(value)s) RETURNING some_table.name, "
            "lower(some_table.value) AS value",
        )

    def test_create_drop_enum(self):
        # test escaping and unicode within CREATE TYPE for ENUM
        typ = postgresql.ENUM("val1", "val2", "val's 3", "méil", name="myname")
        self.assert_compile(
            postgresql.CreateEnumType(typ),
            "CREATE TYPE myname AS ENUM ('val1', 'val2', 'val''s 3', 'méil')",
        )

        typ = postgresql.ENUM("val1", "val2", "val's 3", name="PleaseQuoteMe")
        self.assert_compile(
            postgresql.CreateEnumType(typ),
            'CREATE TYPE "PleaseQuoteMe" AS ENUM '
            "('val1', 'val2', 'val''s 3')",
        )

    def test_generic_enum(self):
        e1 = Enum("x", "y", "z", name="somename")
        e2 = Enum("x", "y", "z", name="somename", schema="someschema")
        self.assert_compile(
            postgresql.CreateEnumType(e1),
            "CREATE TYPE somename AS ENUM ('x', 'y', 'z')",
        )
        self.assert_compile(
            postgresql.CreateEnumType(e2),
            "CREATE TYPE someschema.somename AS ENUM ('x', 'y', 'z')",
        )
        self.assert_compile(postgresql.DropEnumType(e1), "DROP TYPE somename")
        self.assert_compile(
            postgresql.DropEnumType(e2), "DROP TYPE someschema.somename"
        )
        t1 = Table("sometable", MetaData(), Column("somecolumn", e1))
        self.assert_compile(
            schema.CreateTable(t1),
            "CREATE TABLE sometable (somecolumn somename)",
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

    def test_cast_enum_schema(self):
        """test #6739"""
        e1 = Enum("x", "y", "z", name="somename")
        e2 = Enum("x", "y", "z", name="somename", schema="someschema")

        stmt = select(cast(column("foo"), e1), cast(column("bar"), e2))
        self.assert_compile(
            stmt,
            "SELECT CAST(foo AS somename) AS foo, "
            "CAST(bar AS someschema.somename) AS bar",
        )

    def test_cast_double_pg_double(self):
        """test #5465:

        test sqlalchemy Double/DOUBLE to PostgreSQL DOUBLE PRECISION
        """
        d1 = sqltypes.Double

        stmt = select(cast(column("foo"), d1))
        self.assert_compile(
            stmt, "SELECT CAST(foo AS DOUBLE PRECISION) AS foo"
        )

    def test_cast_enum_schema_translate(self):
        """test #6739"""
        e1 = Enum("x", "y", "z", name="somename")
        e2 = Enum("x", "y", "z", name="somename", schema="someschema")
        schema_translate_map = {None: "bat", "someschema": "hoho"}

        stmt = select(cast(column("foo"), e1), cast(column("bar"), e2))
        self.assert_compile(
            stmt,
            "SELECT CAST(foo AS bat.somename) AS foo, "
            "CAST(bar AS hoho.somename) AS bar",
            schema_translate_map=schema_translate_map,
            render_schema_translate=True,
        )

    def test_create_enum_schema_translate(self):
        e1 = Enum("x", "y", "z", name="somename")
        e2 = Enum("x", "y", "z", name="somename", schema="someschema")
        schema_translate_map = {None: "foo", "someschema": "bar"}

        self.assert_compile(
            postgresql.CreateEnumType(e1),
            "CREATE TYPE foo.somename AS ENUM ('x', 'y', 'z')",
            schema_translate_map=schema_translate_map,
            render_schema_translate=True,
        )

        self.assert_compile(
            postgresql.CreateEnumType(e2),
            "CREATE TYPE bar.somename AS ENUM ('x', 'y', 'z')",
            schema_translate_map=schema_translate_map,
            render_schema_translate=True,
        )

    def test_domain(self):
        self.assert_compile(
            postgresql.CreateDomainType(
                DOMAIN(
                    "x",
                    Integer,
                    default=text("11"),
                    not_null=True,
                    check="VALUE < 0",
                )
            ),
            "CREATE DOMAIN x AS INTEGER DEFAULT 11 NOT NULL CHECK (VALUE < 0)",
        )
        self.assert_compile(
            postgresql.CreateDomainType(
                DOMAIN(
                    "sOmEnAmE",
                    Text,
                    collation="utf8",
                    constraint_name="a constraint",
                    not_null=True,
                )
            ),
            'CREATE DOMAIN "sOmEnAmE" AS TEXT COLLATE utf8 CONSTRAINT '
            '"a constraint" NOT NULL',
        )
        self.assert_compile(
            postgresql.CreateDomainType(
                DOMAIN(
                    "foo",
                    Text,
                    collation="utf8",
                    default="foobar",
                    constraint_name="no_bar",
                    not_null=True,
                    check="VALUE != 'bar'",
                )
            ),
            "CREATE DOMAIN foo AS TEXT COLLATE utf8 DEFAULT 'foobar' "
            "CONSTRAINT no_bar NOT NULL CHECK (VALUE != 'bar')",
        )

    def test_cast_domain_schema(self):
        """test #6739"""
        d1 = DOMAIN("somename", Integer)
        d2 = DOMAIN("somename", Integer, schema="someschema")

        stmt = select(cast(column("foo"), d1), cast(column("bar"), d2))
        self.assert_compile(
            stmt,
            "SELECT CAST(foo AS somename) AS foo, "
            "CAST(bar AS someschema.somename) AS bar",
        )

    def test_create_domain_schema_translate(self):
        d1 = DOMAIN("somename", Integer)
        d2 = DOMAIN("somename", Integer, schema="someschema")
        schema_translate_map = {None: "foo", "someschema": "bar"}

        self.assert_compile(
            postgresql.CreateDomainType(d1),
            "CREATE DOMAIN foo.somename AS INTEGER ",
            schema_translate_map=schema_translate_map,
            render_schema_translate=True,
        )

        self.assert_compile(
            postgresql.CreateDomainType(d2),
            "CREATE DOMAIN bar.somename AS INTEGER ",
            schema_translate_map=schema_translate_map,
            render_schema_translate=True,
        )

    def test_create_table_with_schema_type_schema_translate(self):
        e1 = Enum("x", "y", "z", name="somename")
        e2 = Enum("x", "y", "z", name="somename", schema="someschema")
        schema_translate_map = {None: "foo", "someschema": "bar"}

        table = Table(
            "some_table", MetaData(), Column("q", e1), Column("p", e2)
        )
        from sqlalchemy.schema import CreateTable

        self.assert_compile(
            CreateTable(table),
            "CREATE TABLE foo.some_table (q foo.somename, p bar.somename)",
            schema_translate_map=schema_translate_map,
            render_schema_translate=True,
        )

    def test_create_table_array_embedded_schema_type_schema_translate(self):
        """test #6739"""
        e1 = Enum("x", "y", "z", name="somename")
        e2 = Enum("x", "y", "z", name="somename", schema="someschema")
        schema_translate_map = {None: "foo", "someschema": "bar"}

        table = Table(
            "some_table",
            MetaData(),
            Column("q", PG_ARRAY(e1)),
            Column("p", PG_ARRAY(e2)),
        )
        from sqlalchemy.schema import CreateTable

        self.assert_compile(
            CreateTable(table),
            "CREATE TABLE foo.some_table (q foo.somename[], p bar.somename[])",
            schema_translate_map=schema_translate_map,
            render_schema_translate=True,
        )

    def test_create_table_with_tablespace(self):
        m = MetaData()
        tbl = Table(
            "atable",
            m,
            Column("id", Integer),
            postgresql_tablespace="sometablespace",
        )
        self.assert_compile(
            schema.CreateTable(tbl),
            "CREATE TABLE atable (id INTEGER) TABLESPACE sometablespace",
        )

    def test_create_table_with_tablespace_quoted(self):
        # testing quoting of tablespace name
        m = MetaData()
        tbl = Table(
            "anothertable",
            m,
            Column("id", Integer),
            postgresql_tablespace="table",
        )
        self.assert_compile(
            schema.CreateTable(tbl),
            'CREATE TABLE anothertable (id INTEGER) TABLESPACE "table"',
        )

    def test_create_table_inherits(self):
        m = MetaData()
        tbl = Table(
            "atable", m, Column("id", Integer), postgresql_inherits="i1"
        )
        self.assert_compile(
            schema.CreateTable(tbl),
            "CREATE TABLE atable (id INTEGER) INHERITS ( i1 )",
        )

    def test_create_table_inherits_tuple(self):
        m = MetaData()
        tbl = Table(
            "atable",
            m,
            Column("id", Integer),
            postgresql_inherits=("i1", "i2"),
        )
        self.assert_compile(
            schema.CreateTable(tbl),
            "CREATE TABLE atable (id INTEGER) INHERITS ( i1, i2 )",
        )

    def test_create_table_inherits_quoting(self):
        m = MetaData()
        tbl = Table(
            "atable",
            m,
            Column("id", Integer),
            postgresql_inherits=("Quote Me", "quote Me Too"),
        )
        self.assert_compile(
            schema.CreateTable(tbl),
            "CREATE TABLE atable (id INTEGER) INHERITS "
            '( "Quote Me", "quote Me Too" )',
        )

    def test_create_table_partition_by_list(self):
        m = MetaData()
        tbl = Table(
            "atable",
            m,
            Column("id", Integer),
            Column("part_column", Integer),
            postgresql_partition_by="LIST (part_column)",
        )
        self.assert_compile(
            schema.CreateTable(tbl),
            "CREATE TABLE atable (id INTEGER, part_column INTEGER) "
            "PARTITION BY LIST (part_column)",
        )

    def test_create_table_partition_by_range(self):
        m = MetaData()
        tbl = Table(
            "atable",
            m,
            Column("id", Integer),
            Column("part_column", Integer),
            postgresql_partition_by="RANGE (part_column)",
        )
        self.assert_compile(
            schema.CreateTable(tbl),
            "CREATE TABLE atable (id INTEGER, part_column INTEGER) "
            "PARTITION BY RANGE (part_column)",
        )

    def test_create_table_with_oids(self):
        m = MetaData()
        tbl = Table(
            "atable", m, Column("id", Integer), postgresql_with_oids=True
        )
        self.assert_compile(
            schema.CreateTable(tbl),
            "CREATE TABLE atable (id INTEGER) WITH OIDS",
        )

        tbl2 = Table(
            "anothertable",
            m,
            Column("id", Integer),
            postgresql_with_oids=False,
        )
        self.assert_compile(
            schema.CreateTable(tbl2),
            "CREATE TABLE anothertable (id INTEGER) WITHOUT OIDS",
        )

    def test_create_table_with_oncommit_option(self):
        m = MetaData()
        tbl = Table(
            "atable", m, Column("id", Integer), postgresql_on_commit="drop"
        )
        self.assert_compile(
            schema.CreateTable(tbl),
            "CREATE TABLE atable (id INTEGER) ON COMMIT DROP",
        )

    def test_create_table_with_using_option(self):
        m = MetaData()
        tbl = Table(
            "atable",
            m,
            Column("id", Integer),
            postgresql_using="heap",
        )
        self.assert_compile(
            schema.CreateTable(tbl),
            "CREATE TABLE atable (id INTEGER) USING heap",
        )

    def test_create_table_with_multiple_options(self):
        m = MetaData()
        tbl = Table(
            "atable",
            m,
            Column("id", Integer),
            postgresql_tablespace="sometablespace",
            postgresql_with_oids=False,
            postgresql_on_commit="preserve_rows",
            postgresql_using="heap",
        )
        self.assert_compile(
            schema.CreateTable(tbl),
            "CREATE TABLE atable (id INTEGER) USING heap WITHOUT OIDS "
            "ON COMMIT PRESERVE ROWS TABLESPACE sometablespace",
        )

    def test_create_partial_index(self):
        m = MetaData()
        tbl = Table("testtbl", m, Column("data", Integer))
        idx = Index(
            "test_idx1",
            tbl.c.data,
            postgresql_where=and_(tbl.c.data > 5, tbl.c.data < 10),
        )
        idx = Index(
            "test_idx1",
            tbl.c.data,
            postgresql_where=and_(tbl.c.data > 5, tbl.c.data < 10),
        )

        # test quoting and all that

        idx2 = Index(
            "test_idx2",
            tbl.c.data,
            postgresql_where=and_(tbl.c.data > "a", tbl.c.data < "b's"),
        )
        self.assert_compile(
            schema.CreateIndex(idx),
            "CREATE INDEX test_idx1 ON testtbl (data) "
            "WHERE data > 5 AND data < 10",
            dialect=postgresql.dialect(),
        )
        self.assert_compile(
            schema.CreateIndex(idx2),
            "CREATE INDEX test_idx2 ON testtbl (data) "
            "WHERE data > 'a' AND data < 'b''s'",
            dialect=postgresql.dialect(),
        )

        idx3 = Index(
            "test_idx2",
            tbl.c.data,
            postgresql_where=text("data > 'a' AND data < 'b''s'"),
        )
        self.assert_compile(
            schema.CreateIndex(idx3),
            "CREATE INDEX test_idx2 ON testtbl (data) "
            "WHERE data > 'a' AND data < 'b''s'",
            dialect=postgresql.dialect(),
        )

    def test_create_index_with_ops(self):
        m = MetaData()
        tbl = Table(
            "testtbl",
            m,
            Column("data", String),
            Column("data2", Integer, key="d2"),
        )

        idx = Index(
            "test_idx1",
            tbl.c.data,
            postgresql_ops={"data": "text_pattern_ops"},
        )

        idx2 = Index(
            "test_idx2",
            tbl.c.data,
            tbl.c.d2,
            postgresql_ops={"data": "text_pattern_ops", "d2": "int4_ops"},
        )

        self.assert_compile(
            schema.CreateIndex(idx),
            "CREATE INDEX test_idx1 ON testtbl (data text_pattern_ops)",
            dialect=postgresql.dialect(),
        )
        self.assert_compile(
            schema.CreateIndex(idx2),
            "CREATE INDEX test_idx2 ON testtbl "
            "(data text_pattern_ops, data2 int4_ops)",
            dialect=postgresql.dialect(),
        )

    @testing.combinations(
        (
            lambda tbl: schema.CreateIndex(
                Index(
                    "test_idx1",
                    tbl.c.data,
                    unique=True,
                    postgresql_nulls_not_distinct=True,
                )
            ),
            "CREATE UNIQUE INDEX test_idx1 ON test_tbl "
            "(data) NULLS NOT DISTINCT",
        ),
        (
            lambda tbl: schema.CreateIndex(
                Index(
                    "test_idx2",
                    tbl.c.data2,
                    unique=True,
                    postgresql_nulls_not_distinct=False,
                )
            ),
            "CREATE UNIQUE INDEX test_idx2 ON test_tbl "
            "(data2) NULLS DISTINCT",
        ),
        (
            lambda tbl: schema.CreateIndex(
                Index(
                    "test_idx3",
                    tbl.c.data3,
                    unique=True,
                )
            ),
            "CREATE UNIQUE INDEX test_idx3 ON test_tbl (data3)",
        ),
        (
            lambda tbl: schema.CreateIndex(
                Index(
                    "test_idx3_complex",
                    tbl.c.data3,
                    postgresql_nulls_not_distinct=True,
                    postgresql_include=["data2"],
                    postgresql_where=and_(tbl.c.data3 > 5),
                    postgresql_with={"fillfactor": 50},
                )
            ),
            "CREATE INDEX test_idx3_complex ON test_tbl "
            "(data3) INCLUDE (data2) NULLS NOT DISTINCT WITH "
            "(fillfactor = 50) WHERE data3 > 5",
        ),
        (
            lambda tbl: schema.AddConstraint(
                schema.UniqueConstraint(
                    tbl.c.data,
                    name="uq_data1",
                    postgresql_nulls_not_distinct=True,
                )
            ),
            "ALTER TABLE test_tbl ADD CONSTRAINT uq_data1 UNIQUE "
            "NULLS NOT DISTINCT (data)",
        ),
        (
            lambda tbl: schema.AddConstraint(
                schema.UniqueConstraint(
                    tbl.c.data2,
                    name="uq_data2",
                    postgresql_nulls_not_distinct=False,
                )
            ),
            "ALTER TABLE test_tbl ADD CONSTRAINT uq_data2 UNIQUE "
            "NULLS DISTINCT (data2)",
        ),
        (
            lambda tbl: schema.AddConstraint(
                schema.UniqueConstraint(
                    tbl.c.data3,
                    name="uq_data3",
                )
            ),
            "ALTER TABLE test_tbl ADD CONSTRAINT uq_data3 UNIQUE (data3)",
        ),
    )
    def test_nulls_not_distinct(self, expr_fn, expected):
        dd = PGDialect()
        m = MetaData()
        tbl = Table(
            "test_tbl",
            m,
            Column("data", String),
            Column("data2", Integer),
            Column("data3", Integer),
        )

        expr = testing.resolve_lambda(expr_fn, tbl=tbl)
        self.assert_compile(expr, expected, dialect=dd)

    @testing.combinations(
        (
            lambda tbl: schema.AddConstraint(
                UniqueConstraint(tbl.c.id, postgresql_include=[tbl.c.value])
            ),
            "ALTER TABLE foo ADD UNIQUE (id) INCLUDE (value)",
        ),
        (
            lambda tbl: schema.AddConstraint(
                PrimaryKeyConstraint(
                    tbl.c.id, postgresql_include=[tbl.c.value, "misc"]
                )
            ),
            "ALTER TABLE foo ADD PRIMARY KEY (id) INCLUDE (value, misc)",
        ),
        (
            lambda tbl: schema.CreateIndex(
                Index("idx", tbl.c.id, postgresql_include=[tbl.c.value])
            ),
            "CREATE INDEX idx ON foo (id) INCLUDE (value)",
        ),
    )
    def test_include(self, expr_fn, expected):
        m = MetaData()
        tbl = Table(
            "foo",
            m,
            Column("id", Integer, nullable=False),
            Column("value", Integer, nullable=False),
            Column("misc", String),
        )
        expr = testing.resolve_lambda(expr_fn, tbl=tbl)
        self.assert_compile(expr, expected)

    def test_create_index_with_labeled_ops(self):
        m = MetaData()
        tbl = Table(
            "testtbl",
            m,
            Column("data", String),
            Column("data2", Integer, key="d2"),
        )

        idx = Index(
            "test_idx1",
            func.lower(tbl.c.data).label("data_lower"),
            postgresql_ops={"data_lower": "text_pattern_ops"},
        )

        idx2 = Index(
            "test_idx2",
            (func.xyz(tbl.c.data) + tbl.c.d2).label("bar"),
            tbl.c.d2.label("foo"),
            postgresql_ops={"bar": "text_pattern_ops", "foo": "int4_ops"},
        )

        self.assert_compile(
            schema.CreateIndex(idx),
            "CREATE INDEX test_idx1 ON testtbl "
            "(lower(data) text_pattern_ops)",
            dialect=postgresql.dialect(),
        )
        self.assert_compile(
            schema.CreateIndex(idx2),
            "CREATE INDEX test_idx2 ON testtbl "
            "((xyz(data) + data2) text_pattern_ops, "
            "data2 int4_ops)",
            dialect=postgresql.dialect(),
        )

    def test_create_index_with_text_or_composite(self):
        m = MetaData()
        tbl = Table("testtbl", m, Column("d1", String), Column("d2", Integer))

        idx = Index("test_idx1", text("x"))
        tbl.append_constraint(idx)

        idx2 = Index("test_idx2", text("y"), tbl.c.d2)

        idx3 = Index(
            "test_idx2",
            tbl.c.d1,
            text("y"),
            tbl.c.d2,
            postgresql_ops={"d1": "x1", "d2": "x2"},
        )

        idx4 = Index(
            "test_idx2",
            tbl.c.d1,
            tbl.c.d2 > 5,
            text("q"),
            postgresql_ops={"d1": "x1", "d2": "x2"},
        )

        idx5 = Index(
            "test_idx2",
            tbl.c.d1,
            (tbl.c.d2 > 5).label("g"),
            text("q"),
            postgresql_ops={"d1": "x1", "g": "x2"},
        )

        self.assert_compile(
            schema.CreateIndex(idx), "CREATE INDEX test_idx1 ON testtbl (x)"
        )
        self.assert_compile(
            schema.CreateIndex(idx2),
            "CREATE INDEX test_idx2 ON testtbl (y, d2)",
        )
        self.assert_compile(
            schema.CreateIndex(idx3),
            "CREATE INDEX test_idx2 ON testtbl (d1 x1, y, d2 x2)",
        )

        # note that at the moment we do not expect the 'd2' op to
        # pick up on the "d2 > 5" expression
        self.assert_compile(
            schema.CreateIndex(idx4),
            "CREATE INDEX test_idx2 ON testtbl (d1 x1, (d2 > 5), q)",
        )

        # however it does work if we label!
        self.assert_compile(
            schema.CreateIndex(idx5),
            "CREATE INDEX test_idx2 ON testtbl (d1 x1, (d2 > 5) x2, q)",
        )

    def test_create_index_with_using(self):
        m = MetaData()
        tbl = Table("testtbl", m, Column("data", String))

        idx1 = Index("test_idx1", tbl.c.data)
        idx2 = Index("test_idx2", tbl.c.data, postgresql_using="btree")
        idx3 = Index("test_idx3", tbl.c.data, postgresql_using="hash")

        self.assert_compile(
            schema.CreateIndex(idx1),
            "CREATE INDEX test_idx1 ON testtbl (data)",
            dialect=postgresql.dialect(),
        )
        self.assert_compile(
            schema.CreateIndex(idx2),
            "CREATE INDEX test_idx2 ON testtbl USING btree (data)",
            dialect=postgresql.dialect(),
        )
        self.assert_compile(
            schema.CreateIndex(idx3),
            "CREATE INDEX test_idx3 ON testtbl USING hash (data)",
            dialect=postgresql.dialect(),
        )

    def test_create_index_with_with(self):
        m = MetaData()
        tbl = Table("testtbl", m, Column("data", String))

        idx1 = Index("test_idx1", tbl.c.data)
        idx2 = Index(
            "test_idx2", tbl.c.data, postgresql_with={"fillfactor": 50}
        )
        idx3 = Index(
            "test_idx3",
            tbl.c.data,
            postgresql_using="gist",
            postgresql_with={"buffering": "off"},
        )

        self.assert_compile(
            schema.CreateIndex(idx1),
            "CREATE INDEX test_idx1 ON testtbl (data)",
        )
        self.assert_compile(
            schema.CreateIndex(idx2),
            "CREATE INDEX test_idx2 ON testtbl "
            "(data) "
            "WITH (fillfactor = 50)",
        )
        self.assert_compile(
            schema.CreateIndex(idx3),
            "CREATE INDEX test_idx3 ON testtbl "
            "USING gist (data) "
            "WITH (buffering = off)",
        )

    def test_create_index_with_using_unusual_conditions(self):
        m = MetaData()
        tbl = Table("testtbl", m, Column("data", String))

        self.assert_compile(
            schema.CreateIndex(
                Index("test_idx1", tbl.c.data, postgresql_using="GIST")
            ),
            "CREATE INDEX test_idx1 ON testtbl USING gist (data)",
        )

        self.assert_compile(
            schema.CreateIndex(
                Index(
                    "test_idx1",
                    tbl.c.data,
                    postgresql_using="some_custom_method",
                )
            ),
            "CREATE INDEX test_idx1 ON testtbl "
            "USING some_custom_method (data)",
        )

        assert_raises_message(
            exc.CompileError,
            "Unexpected SQL phrase: 'gin invalid sql'",
            schema.CreateIndex(
                Index(
                    "test_idx2", tbl.c.data, postgresql_using="gin invalid sql"
                )
            ).compile,
            dialect=postgresql.dialect(),
        )

    def test_create_index_with_tablespace(self):
        m = MetaData()
        tbl = Table("testtbl", m, Column("data", String))

        idx1 = Index("test_idx1", tbl.c.data)
        idx2 = Index(
            "test_idx2", tbl.c.data, postgresql_tablespace="sometablespace"
        )
        idx3 = Index(
            "test_idx3",
            tbl.c.data,
            postgresql_tablespace="another table space",
        )

        self.assert_compile(
            schema.CreateIndex(idx1),
            "CREATE INDEX test_idx1 ON testtbl (data)",
            dialect=postgresql.dialect(),
        )
        self.assert_compile(
            schema.CreateIndex(idx2),
            "CREATE INDEX test_idx2 ON testtbl "
            "(data) "
            "TABLESPACE sometablespace",
            dialect=postgresql.dialect(),
        )
        self.assert_compile(
            schema.CreateIndex(idx3),
            "CREATE INDEX test_idx3 ON testtbl "
            "(data) "
            'TABLESPACE "another table space"',
            dialect=postgresql.dialect(),
        )

    def test_create_index_with_multiple_options(self):
        m = MetaData()
        tbl = Table("testtbl", m, Column("data", String))

        idx1 = Index(
            "test_idx1",
            tbl.c.data,
            postgresql_using="btree",
            postgresql_tablespace="atablespace",
            postgresql_with={"fillfactor": 60},
            postgresql_where=and_(tbl.c.data > 5, tbl.c.data < 10),
        )

        self.assert_compile(
            schema.CreateIndex(idx1),
            "CREATE INDEX test_idx1 ON testtbl "
            "USING btree (data) "
            "WITH (fillfactor = 60) "
            "TABLESPACE atablespace "
            "WHERE data > 5 AND data < 10",
            dialect=postgresql.dialect(),
        )

    def test_create_index_expr_gets_parens(self):
        m = MetaData()
        tbl = Table("testtbl", m, Column("x", Integer), Column("y", Integer))

        idx1 = Index("test_idx1", 5 // (tbl.c.x + tbl.c.y))
        self.assert_compile(
            schema.CreateIndex(idx1),
            "CREATE INDEX test_idx1 ON testtbl ((5 / (x + y)))",
        )

    def test_create_index_literals(self):
        m = MetaData()
        tbl = Table("testtbl", m, Column("data", Integer))

        idx1 = Index("test_idx1", tbl.c.data + 5)
        self.assert_compile(
            schema.CreateIndex(idx1),
            "CREATE INDEX test_idx1 ON testtbl ((data + 5))",
        )

    def test_create_index_concurrently(self):
        m = MetaData()
        tbl = Table("testtbl", m, Column("data", Integer))

        idx1 = Index("test_idx1", tbl.c.data, postgresql_concurrently=True)
        self.assert_compile(
            schema.CreateIndex(idx1),
            "CREATE INDEX CONCURRENTLY test_idx1 ON testtbl (data)",
        )

        dialect_8_1 = postgresql.dialect()
        dialect_8_1._supports_create_index_concurrently = False
        self.assert_compile(
            schema.CreateIndex(idx1),
            "CREATE INDEX test_idx1 ON testtbl (data)",
            dialect=dialect_8_1,
        )

    def test_drop_index_concurrently(self):
        m = MetaData()
        tbl = Table("testtbl", m, Column("data", Integer))

        idx1 = Index("test_idx1", tbl.c.data, postgresql_concurrently=True)
        self.assert_compile(
            schema.DropIndex(idx1), "DROP INDEX CONCURRENTLY test_idx1"
        )

        dialect_9_1 = postgresql.dialect()
        dialect_9_1._supports_drop_index_concurrently = False
        self.assert_compile(
            schema.DropIndex(idx1), "DROP INDEX test_idx1", dialect=dialect_9_1
        )

    def test_create_check_constraint_not_valid(self):
        m = MetaData()

        tbl = Table(
            "testtbl",
            m,
            Column("data", Integer),
            CheckConstraint("data = 0", postgresql_not_valid=True),
        )

        self.assert_compile(
            schema.CreateTable(tbl),
            "CREATE TABLE testtbl (data INTEGER, CHECK (data = 0) NOT VALID)",
        )

    def test_create_foreign_key_constraint_not_valid(self):
        m = MetaData()

        tbl = Table(
            "testtbl",
            m,
            Column("a", Integer),
            Column("b", Integer),
            ForeignKeyConstraint(
                "b", ["testtbl.a"], postgresql_not_valid=True
            ),
        )

        self.assert_compile(
            schema.CreateTable(tbl),
            "CREATE TABLE testtbl ("
            "a INTEGER, "
            "b INTEGER, "
            "FOREIGN KEY(b) REFERENCES testtbl (a) NOT VALID"
            ")",
        )

    def test_create_foreign_key_column_not_valid(self):
        m = MetaData()

        tbl = Table(
            "testtbl",
            m,
            Column("a", Integer),
            Column("b", ForeignKey("testtbl.a", postgresql_not_valid=True)),
        )

        self.assert_compile(
            schema.CreateTable(tbl),
            "CREATE TABLE testtbl ("
            "a INTEGER, "
            "b INTEGER, "
            "FOREIGN KEY(b) REFERENCES testtbl (a) NOT VALID"
            ")",
        )

    def test_create_foreign_key_constraint_ondelete_column_list(self):
        m = MetaData()
        pktable = Table(
            "pktable",
            m,
            Column("tid", Integer, primary_key=True),
            Column("id", Integer, primary_key=True),
        )
        fktable = Table(
            "fktable",
            m,
            Column("tid", Integer),
            Column("id", Integer),
            Column("fk_id_del_set_null", Integer),
            Column("fk_id_del_set_default", Integer, server_default=text("0")),
            ForeignKeyConstraint(
                columns=["tid", "fk_id_del_set_null"],
                refcolumns=[pktable.c.tid, pktable.c.id],
                ondelete="SET NULL (fk_id_del_set_null)",
            ),
            ForeignKeyConstraint(
                columns=["tid", "fk_id_del_set_default"],
                refcolumns=[pktable.c.tid, pktable.c.id],
                ondelete="SET DEFAULT(fk_id_del_set_default)",
            ),
        )

        self.assert_compile(
            schema.CreateTable(fktable),
            "CREATE TABLE fktable ("
            "tid INTEGER, id INTEGER, "
            "fk_id_del_set_null INTEGER, "
            "fk_id_del_set_default INTEGER DEFAULT 0, "
            "FOREIGN KEY(tid, fk_id_del_set_null)"
            " REFERENCES pktable (tid, id)"
            " ON DELETE SET NULL (fk_id_del_set_null), "
            "FOREIGN KEY(tid, fk_id_del_set_default)"
            " REFERENCES pktable (tid, id)"
            " ON DELETE SET DEFAULT(fk_id_del_set_default)"
            ")",
        )

    def test_exclude_constraint_min(self):
        m = MetaData()
        tbl = Table("testtbl", m, Column("room", Integer, primary_key=True))
        cons = ExcludeConstraint(("room", "="))
        tbl.append_constraint(cons)
        self.assert_compile(
            schema.AddConstraint(cons),
            "ALTER TABLE testtbl ADD EXCLUDE USING gist (room WITH =)",
            dialect=postgresql.dialect(),
        )

    @testing.combinations(
        (True, "deferred"),
        (False, "immediate"),
        argnames="deferrable_value, initially_value",
    )
    def test_copy_exclude_constraint_adhoc_columns(
        self, deferrable_value, initially_value
    ):
        meta = MetaData()
        table = Table(
            "mytable",
            meta,
            Column("myid", Integer, Sequence("foo_id_seq"), primary_key=True),
            Column("valid_from_date", Date(), nullable=True),
            Column("valid_thru_date", Date(), nullable=True),
        )
        sql_text = "daterange(valid_from_date, valid_thru_date, '[]')"
        cons = ExcludeConstraint(
            (literal_column(sql_text), "&&"),
            where=column("valid_from_date") <= column("valid_thru_date"),
            name="ex_mytable_valid_date_range",
            deferrable=deferrable_value,
            initially=initially_value,
        )

        table.append_constraint(cons)
        eq_(cons.columns.keys(), [sql_text])
        expected = (
            "ALTER TABLE mytable ADD CONSTRAINT ex_mytable_valid_date_range "
            "EXCLUDE USING gist "
            "(daterange(valid_from_date, valid_thru_date, '[]') WITH &&) "
            "WHERE (valid_from_date <= valid_thru_date) "
            "%s %s"
            % (
                "NOT DEFERRABLE" if not deferrable_value else "DEFERRABLE",
                "INITIALLY %s" % initially_value,
            )
        )
        self.assert_compile(
            schema.AddConstraint(cons),
            expected,
            dialect=postgresql.dialect(),
        )

        meta2 = MetaData()
        table2 = table.to_metadata(meta2)
        cons2 = [
            c for c in table2.constraints if isinstance(c, ExcludeConstraint)
        ][0]
        self.assert_compile(
            schema.AddConstraint(cons2),
            expected,
            dialect=postgresql.dialect(),
        )

    def test_exclude_constraint_full(self):
        m = MetaData()
        room = Column("room", Integer, primary_key=True)
        tbl = Table("testtbl", m, room, Column("during", TSRANGE))
        room = Column("room", Integer, primary_key=True)
        cons = ExcludeConstraint(
            (room, "="),
            ("during", "&&"),
            name="my_name",
            using="gist",
            where="room > 100",
            deferrable=True,
            initially="immediate",
            ops={"room": "my_opclass"},
        )
        tbl.append_constraint(cons)
        self.assert_compile(
            schema.AddConstraint(cons),
            "ALTER TABLE testtbl ADD CONSTRAINT my_name "
            "EXCLUDE USING gist "
            "(room my_opclass WITH =, during WITH "
            "&&) WHERE "
            "(room > 100) DEFERRABLE INITIALLY immediate",
            dialect=postgresql.dialect(),
        )

    def test_exclude_constraint_copy(self):
        m = MetaData()
        cons = ExcludeConstraint(("room", "="))
        tbl = Table(
            "testtbl", m, Column("room", Integer, primary_key=True), cons
        )
        # apparently you can't copy a ColumnCollectionConstraint until
        # after it has been bound to a table...
        cons_copy = cons._copy()
        tbl.append_constraint(cons_copy)
        self.assert_compile(
            schema.AddConstraint(cons_copy),
            "ALTER TABLE testtbl ADD EXCLUDE USING gist (room WITH =)",
        )

    def test_exclude_constraint_copy_complex(self):
        m = MetaData()
        tbl = Table("foo", m, Column("x", Integer), Column("y", Integer))
        cons = ExcludeConstraint(
            (tbl.c.x, "*"),
            (text("x-y"), "%"),
            (literal_column("x+y"), "$"),
            (tbl.c.x // tbl.c.y, "??"),
            (func.power(tbl.c.x, 42), "="),
            (func.int8range(column("x"), column("y")), "&&"),
            ("y", "^"),
        )
        tbl.append_constraint(cons)
        expected = (
            "ALTER TABLE {name} ADD EXCLUDE USING gist "
            "(x WITH *, x-y WITH %, x+y WITH $, x / y WITH ??, "
            "power(x, 42) WITH =, int8range(x, y) WITH &&, y WITH ^)"
        )
        self.assert_compile(
            schema.AddConstraint(cons),
            expected.format(name="foo"),
            dialect=postgresql.dialect(),
        )
        m2 = MetaData()
        tbl2 = tbl.to_metadata(m2, name="bar")
        (cons2,) = [
            c for c in tbl2.constraints if isinstance(c, ExcludeConstraint)
        ]
        self.assert_compile(
            schema.AddConstraint(cons2),
            expected.format(name="bar"),
            dialect=postgresql.dialect(),
        )

    def test_exclude_constraint_copy_where_using(self):
        m = MetaData()
        tbl = Table("testtbl", m, Column("room", Integer, primary_key=True))
        cons = ExcludeConstraint(
            (tbl.c.room, "="), where=tbl.c.room > 5, using="foobar"
        )
        tbl.append_constraint(cons)
        self.assert_compile(
            schema.AddConstraint(cons),
            "ALTER TABLE testtbl ADD EXCLUDE USING foobar "
            "(room WITH =) WHERE (testtbl.room > 5)",
        )

        m2 = MetaData()
        tbl2 = tbl.to_metadata(m2)
        self.assert_compile(
            schema.CreateTable(tbl2),
            "CREATE TABLE testtbl (room SERIAL NOT NULL, "
            "PRIMARY KEY (room), "
            "EXCLUDE USING foobar "
            "(room WITH =) WHERE (testtbl.room > 5))",
        )

    def test_exclude_constraint_text(self):
        m = MetaData()
        cons = ExcludeConstraint((text("room::TEXT"), "="))
        Table("testtbl", m, Column("room", String), cons)
        eq_(list(cons.columns), [])
        self.assert_compile(
            schema.AddConstraint(cons),
            "ALTER TABLE testtbl ADD EXCLUDE USING gist "
            "(room::TEXT WITH =)",
        )

    def test_exclude_constraint_colname_needs_quoting(self):
        m = MetaData()
        cons = ExcludeConstraint(("Some Column Name", "="))
        Table("testtbl", m, Column("Some Column Name", String), cons)
        self.assert_compile(
            schema.AddConstraint(cons),
            "ALTER TABLE testtbl ADD EXCLUDE USING gist "
            '("Some Column Name" WITH =)',
        )

    def test_exclude_constraint_with_using_unusual_conditions(self):
        m = MetaData()
        cons = ExcludeConstraint(("q", "="), using="not a keyword")
        Table("testtbl", m, Column("q", String), cons)
        assert_raises_message(
            exc.CompileError,
            "Unexpected SQL phrase: 'not a keyword'",
            schema.AddConstraint(cons).compile,
            dialect=postgresql.dialect(),
        )

    def test_exclude_constraint_cast(self):
        m = MetaData()
        tbl = Table("testtbl", m, Column("room", String))
        cons = ExcludeConstraint((cast(tbl.c.room, Text), "="))
        tbl.append_constraint(cons)
        self.assert_compile(
            schema.AddConstraint(cons),
            "ALTER TABLE testtbl ADD EXCLUDE USING gist "
            "(CAST(room AS TEXT) WITH =)",
        )

    def test_exclude_constraint_cast_quote(self):
        m = MetaData()
        tbl = Table("testtbl", m, Column("Room", String))
        cons = ExcludeConstraint((cast(tbl.c.Room, Text), "="))
        tbl.append_constraint(cons)
        self.assert_compile(
            schema.AddConstraint(cons),
            "ALTER TABLE testtbl ADD EXCLUDE USING gist "
            '(CAST("Room" AS TEXT) WITH =)',
        )

    def test_exclude_constraint_when(self):
        m = MetaData()
        tbl = Table("testtbl", m, Column("room", String))
        cons = ExcludeConstraint(("room", "="), where=tbl.c.room.in_(["12"]))
        tbl.append_constraint(cons)
        self.assert_compile(
            schema.AddConstraint(cons),
            "ALTER TABLE testtbl ADD EXCLUDE USING gist "
            "(room WITH =) WHERE (testtbl.room IN ('12'))",
            dialect=postgresql.dialect(),
        )

    def test_exclude_constraint_ops_many(self):
        m = MetaData()
        tbl = Table(
            "testtbl", m, Column("room", String), Column("during", TSRANGE)
        )
        cons = ExcludeConstraint(
            ("room", "="),
            ("during", "&&"),
            ops={"room": "first_opsclass", "during": "second_opclass"},
        )
        tbl.append_constraint(cons)
        self.assert_compile(
            schema.AddConstraint(cons),
            "ALTER TABLE testtbl ADD EXCLUDE USING gist "
            "(room first_opsclass WITH =, during second_opclass WITH &&)",
            dialect=postgresql.dialect(),
        )

    def test_exclude_constraint_expression(self):
        m = MetaData()
        tbl = Table("foo", m, Column("x", Integer), Column("y", Integer))
        cons = ExcludeConstraint((func.int8range(column("x"), tbl.c.y), "&&"))
        tbl.append_constraint(cons)
        # only the first col is considered. see #9233
        eq_(cons.columns.keys(), ["x"])
        self.assert_compile(
            schema.AddConstraint(cons),
            "ALTER TABLE foo ADD EXCLUDE USING gist "
            "(int8range(x, y) WITH &&)",
            dialect=postgresql.dialect(),
        )

    def test_exclude_constraint_literal_binds(self):
        m = MetaData()
        tbl = Table("foo", m, Column("x", Integer), Column("y", Integer))
        cons = ExcludeConstraint(
            (func.power(tbl.c.x, 42), "="),
            (func.int8range(column("x"), "y"), "&&"),
        )
        tbl.append_constraint(cons)
        self.assert_compile(
            schema.AddConstraint(cons),
            "ALTER TABLE foo ADD EXCLUDE USING gist "
            "(power(x, 42) WITH =, int8range(x, 'y') WITH &&)",
            dialect=postgresql.dialect(),
        )

    def test_substring(self):
        self.assert_compile(
            func.substring("abc", 1, 2),
            "SUBSTRING(%(substring_1)s FROM %(substring_2)s "
            "FOR %(substring_3)s)",
        )
        self.assert_compile(
            func.substring("abc", 1),
            "SUBSTRING(%(substring_1)s FROM %(substring_2)s)",
        )

    def test_for_update(self):
        table1 = table(
            "mytable", column("myid"), column("name"), column("description")
        )

        self.assert_compile(
            table1.select().where(table1.c.myid == 7).with_for_update(),
            "SELECT mytable.myid, mytable.name, mytable.description "
            "FROM mytable WHERE mytable.myid = %(myid_1)s FOR UPDATE",
        )

        self.assert_compile(
            table1.select()
            .where(table1.c.myid == 7)
            .with_for_update(nowait=True),
            "SELECT mytable.myid, mytable.name, mytable.description "
            "FROM mytable WHERE mytable.myid = %(myid_1)s FOR UPDATE NOWAIT",
        )

        self.assert_compile(
            table1.select()
            .where(table1.c.myid == 7)
            .with_for_update(skip_locked=True),
            "SELECT mytable.myid, mytable.name, mytable.description "
            "FROM mytable WHERE mytable.myid = %(myid_1)s "
            "FOR UPDATE SKIP LOCKED",
        )

        self.assert_compile(
            table1.select()
            .where(table1.c.myid == 7)
            .with_for_update(read=True),
            "SELECT mytable.myid, mytable.name, mytable.description "
            "FROM mytable WHERE mytable.myid = %(myid_1)s FOR SHARE",
        )

        self.assert_compile(
            table1.select()
            .where(table1.c.myid == 7)
            .with_for_update(read=True, nowait=True),
            "SELECT mytable.myid, mytable.name, mytable.description "
            "FROM mytable WHERE mytable.myid = %(myid_1)s FOR SHARE NOWAIT",
        )

        self.assert_compile(
            table1.select()
            .where(table1.c.myid == 7)
            .with_for_update(key_share=True, nowait=True),
            "SELECT mytable.myid, mytable.name, mytable.description "
            "FROM mytable WHERE mytable.myid = %(myid_1)s "
            "FOR NO KEY UPDATE NOWAIT",
        )

        self.assert_compile(
            table1.select()
            .where(table1.c.myid == 7)
            .with_for_update(key_share=True, read=True, nowait=True),
            "SELECT mytable.myid, mytable.name, mytable.description "
            "FROM mytable WHERE mytable.myid = %(myid_1)s "
            "FOR KEY SHARE NOWAIT",
        )

        self.assert_compile(
            table1.select()
            .where(table1.c.myid == 7)
            .with_for_update(read=True, skip_locked=True),
            "SELECT mytable.myid, mytable.name, mytable.description "
            "FROM mytable WHERE mytable.myid = %(myid_1)s "
            "FOR SHARE SKIP LOCKED",
        )

        self.assert_compile(
            table1.select()
            .where(table1.c.myid == 7)
            .with_for_update(of=table1.c.myid),
            "SELECT mytable.myid, mytable.name, mytable.description "
            "FROM mytable WHERE mytable.myid = %(myid_1)s "
            "FOR UPDATE OF mytable",
        )

        self.assert_compile(
            table1.select()
            .where(table1.c.myid == 7)
            .with_for_update(read=True, nowait=True, of=table1),
            "SELECT mytable.myid, mytable.name, mytable.description "
            "FROM mytable WHERE mytable.myid = %(myid_1)s "
            "FOR SHARE OF mytable NOWAIT",
        )

        self.assert_compile(
            table1.select()
            .where(table1.c.myid == 7)
            .with_for_update(
                key_share=True, read=True, nowait=True, of=table1
            ),
            "SELECT mytable.myid, mytable.name, mytable.description "
            "FROM mytable WHERE mytable.myid = %(myid_1)s "
            "FOR KEY SHARE OF mytable NOWAIT",
        )

        self.assert_compile(
            table1.select()
            .where(table1.c.myid == 7)
            .with_for_update(read=True, nowait=True, of=table1.c.myid),
            "SELECT mytable.myid, mytable.name, mytable.description "
            "FROM mytable WHERE mytable.myid = %(myid_1)s "
            "FOR SHARE OF mytable NOWAIT",
        )

        self.assert_compile(
            table1.select()
            .where(table1.c.myid == 7)
            .with_for_update(
                read=True, nowait=True, of=[table1.c.myid, table1.c.name]
            ),
            "SELECT mytable.myid, mytable.name, mytable.description "
            "FROM mytable WHERE mytable.myid = %(myid_1)s "
            "FOR SHARE OF mytable NOWAIT",
        )

        self.assert_compile(
            table1.select()
            .where(table1.c.myid == 7)
            .with_for_update(
                read=True,
                skip_locked=True,
                of=[table1.c.myid, table1.c.name],
                key_share=True,
            ),
            "SELECT mytable.myid, mytable.name, mytable.description "
            "FROM mytable WHERE mytable.myid = %(myid_1)s "
            "FOR KEY SHARE OF mytable SKIP LOCKED",
        )

        self.assert_compile(
            table1.select()
            .where(table1.c.myid == 7)
            .with_for_update(
                skip_locked=True, of=[table1.c.myid, table1.c.name]
            ),
            "SELECT mytable.myid, mytable.name, mytable.description "
            "FROM mytable WHERE mytable.myid = %(myid_1)s "
            "FOR UPDATE OF mytable SKIP LOCKED",
        )

        self.assert_compile(
            table1.select()
            .where(table1.c.myid == 7)
            .with_for_update(
                read=True, skip_locked=True, of=[table1.c.myid, table1.c.name]
            ),
            "SELECT mytable.myid, mytable.name, mytable.description "
            "FROM mytable WHERE mytable.myid = %(myid_1)s "
            "FOR SHARE OF mytable SKIP LOCKED",
        )

        self.assert_compile(
            table1.select()
            .where(table1.c.myid == 7)
            .with_for_update(
                key_share=True, nowait=True, of=[table1.c.myid, table1.c.name]
            ),
            "SELECT mytable.myid, mytable.name, mytable.description "
            "FROM mytable WHERE mytable.myid = %(myid_1)s "
            "FOR NO KEY UPDATE OF mytable NOWAIT",
        )

        self.assert_compile(
            table1.select()
            .where(table1.c.myid == 7)
            .with_for_update(
                key_share=True,
                skip_locked=True,
                of=[table1.c.myid, table1.c.name],
            ),
            "SELECT mytable.myid, mytable.name, mytable.description "
            "FROM mytable WHERE mytable.myid = %(myid_1)s "
            "FOR NO KEY UPDATE OF mytable SKIP LOCKED",
        )

        self.assert_compile(
            table1.select()
            .where(table1.c.myid == 7)
            .with_for_update(
                key_share=True, of=[table1.c.myid, table1.c.name]
            ),
            "SELECT mytable.myid, mytable.name, mytable.description "
            "FROM mytable WHERE mytable.myid = %(myid_1)s "
            "FOR NO KEY UPDATE OF mytable",
        )

        self.assert_compile(
            table1.select()
            .where(table1.c.myid == 7)
            .with_for_update(key_share=True),
            "SELECT mytable.myid, mytable.name, mytable.description "
            "FROM mytable WHERE mytable.myid = %(myid_1)s "
            "FOR NO KEY UPDATE",
        )

        self.assert_compile(
            table1.select()
            .where(table1.c.myid == 7)
            .with_for_update(read=True, key_share=True),
            "SELECT mytable.myid, mytable.name, mytable.description "
            "FROM mytable WHERE mytable.myid = %(myid_1)s "
            "FOR KEY SHARE",
        )

        self.assert_compile(
            table1.select()
            .where(table1.c.myid == 7)
            .with_for_update(read=True, key_share=True, of=table1),
            "SELECT mytable.myid, mytable.name, mytable.description "
            "FROM mytable WHERE mytable.myid = %(myid_1)s "
            "FOR KEY SHARE OF mytable",
        )

        self.assert_compile(
            table1.select()
            .where(table1.c.myid == 7)
            .with_for_update(read=True, of=table1),
            "SELECT mytable.myid, mytable.name, mytable.description "
            "FROM mytable WHERE mytable.myid = %(myid_1)s "
            "FOR SHARE OF mytable",
        )

        self.assert_compile(
            table1.select()
            .where(table1.c.myid == 7)
            .with_for_update(read=True, key_share=True, skip_locked=True),
            "SELECT mytable.myid, mytable.name, mytable.description "
            "FROM mytable WHERE mytable.myid = %(myid_1)s "
            "FOR KEY SHARE SKIP LOCKED",
        )

        self.assert_compile(
            table1.select()
            .where(table1.c.myid == 7)
            .with_for_update(key_share=True, skip_locked=True),
            "SELECT mytable.myid, mytable.name, mytable.description "
            "FROM mytable WHERE mytable.myid = %(myid_1)s "
            "FOR NO KEY UPDATE SKIP LOCKED",
        )

        ta = table1.alias()
        self.assert_compile(
            ta.select()
            .where(ta.c.myid == 7)
            .with_for_update(of=[ta.c.myid, ta.c.name]),
            "SELECT mytable_1.myid, mytable_1.name, mytable_1.description "
            "FROM mytable AS mytable_1 "
            "WHERE mytable_1.myid = %(myid_1)s FOR UPDATE OF mytable_1",
        )

        table2 = table("table2", column("mytable_id"))
        join = table2.join(table1, table2.c.mytable_id == table1.c.myid)
        self.assert_compile(
            join.select()
            .where(table2.c.mytable_id == 7)
            .with_for_update(of=[join]),
            "SELECT table2.mytable_id, "
            "mytable.myid, mytable.name, mytable.description "
            "FROM table2 "
            "JOIN mytable ON table2.mytable_id = mytable.myid "
            "WHERE table2.mytable_id = %(mytable_id_1)s "
            "FOR UPDATE OF mytable, table2",
        )

        join = table2.join(ta, table2.c.mytable_id == ta.c.myid)
        self.assert_compile(
            join.select()
            .where(table2.c.mytable_id == 7)
            .with_for_update(of=[join]),
            "SELECT table2.mytable_id, "
            "mytable_1.myid, mytable_1.name, mytable_1.description "
            "FROM table2 "
            "JOIN mytable AS mytable_1 "
            "ON table2.mytable_id = mytable_1.myid "
            "WHERE table2.mytable_id = %(mytable_id_1)s "
            "FOR UPDATE OF mytable_1, table2",
        )

        # ensure of=text() for of works
        self.assert_compile(
            table1.select()
            .where(table1.c.myid == 7)
            .with_for_update(of=text("table1")),
            "SELECT mytable.myid, mytable.name, mytable.description "
            "FROM mytable WHERE mytable.myid = %(myid_1)s "
            "FOR UPDATE OF table1",
        )

        # ensure literal_column of works
        self.assert_compile(
            table1.select()
            .where(table1.c.myid == 7)
            .with_for_update(of=literal_column("table1")),
            "SELECT mytable.myid, mytable.name, mytable.description "
            "FROM mytable WHERE mytable.myid = %(myid_1)s "
            "FOR UPDATE OF table1",
        )

        # test issue #12417
        subquery = select(table1.c.myid).with_for_update(of=table1).lateral()
        statement = select(subquery.c.myid)
        self.assert_compile(
            statement,
            "SELECT anon_1.myid FROM LATERAL (SELECT mytable.myid AS myid "
            "FROM mytable FOR UPDATE OF mytable) AS anon_1",
        )

    def test_for_update_with_schema(self):
        m = MetaData()
        table1 = Table(
            "mytable", m, Column("myid"), Column("name"), schema="testschema"
        )

        self.assert_compile(
            table1.select()
            .where(table1.c.myid == 7)
            .with_for_update(of=table1),
            "SELECT testschema.mytable.myid, testschema.mytable.name "
            "FROM testschema.mytable "
            "WHERE testschema.mytable.myid = %(myid_1)s "
            "FOR UPDATE OF mytable",
        )

    def test_reserved_words(self):
        table = Table(
            "pg_table",
            MetaData(),
            Column("col1", Integer),
            Column("variadic", Integer),
        )
        x = select(table.c.col1, table.c.variadic)

        self.assert_compile(
            x, """SELECT pg_table.col1, pg_table."variadic" FROM pg_table"""
        )

    def _array_any_deprecation(self):
        return testing.expect_deprecated(
            r"The ARRAY.Comparator.any\(\) and "
            r"ARRAY.Comparator.all\(\) methods "
            r"for arrays are deprecated for removal, along with the "
            r"PG-specific Any\(\) "
            r"and All\(\) functions. See any_\(\) and all_\(\) functions for "
            "modern use. "
        )

    def test_array(self):
        c = Column("x", postgresql.ARRAY(Integer))

        self.assert_compile(
            cast(c, postgresql.ARRAY(Integer)), "CAST(x AS INTEGER[])"
        )
        self.assert_compile(c[5], "x[%(x_1)s]", checkparams={"x_1": 5})

        self.assert_compile(
            c[5:7], "x[%(x_1)s:%(x_2)s]", checkparams={"x_2": 7, "x_1": 5}
        )
        self.assert_compile(
            c[5:7][2:3],
            "x[%(x_1)s:%(x_2)s][%(param_1)s:%(param_2)s]",
            checkparams={"x_2": 7, "x_1": 5, "param_1": 2, "param_2": 3},
        )
        self.assert_compile(
            c[5:7][3],
            "x[%(x_1)s:%(x_2)s][%(param_1)s]",
            checkparams={"x_2": 7, "x_1": 5, "param_1": 3},
        )

        self.assert_compile(
            c.contains([1]),
            "x @> %(x_1)s::INTEGER[]",
            checkparams={"x_1": [1]},
            dialect=PGDialect_psycopg2(),
        )
        self.assert_compile(
            c.contained_by([2]),
            "x <@ %(x_1)s::INTEGER[]",
            checkparams={"x_1": [2]},
            dialect=PGDialect_psycopg2(),
        )
        self.assert_compile(
            c.contained_by([2]),
            "x <@ %(x_1)s",
            checkparams={"x_1": [2]},
            dialect=PGDialect(),
        )
        self.assert_compile(
            c.overlap([3]),
            "x && %(x_1)s::INTEGER[]",
            checkparams={"x_1": [3]},
            dialect=PGDialect_psycopg2(),
        )

    def test_array_modern_any_all(self):
        c = Column("x", postgresql.ARRAY(Integer))

        self.assert_compile(
            4 == c.any_(),
            "%(param_1)s = ANY (x)",
            checkparams={"param_1": 4},
        )

        self.assert_compile(
            5 == any_(c),
            "%(param_1)s = ANY (x)",
            checkparams={"param_1": 5},
        )

        self.assert_compile(
            ~(c.any_() == 5),
            "NOT (%(param_1)s = ANY (x))",
            checkparams={"param_1": 5},
        )

        self.assert_compile(
            ~(5 == c.any_()),
            "NOT (%(param_1)s = ANY (x))",
            checkparams={"param_1": 5},
        )

        self.assert_compile(
            5 != any_(c),
            "%(param_1)s != ANY (x)",
            checkparams={"param_1": 5},
        )

        self.assert_compile(
            6 > all_(c),
            "%(param_1)s > ALL (x)",
            checkparams={"param_1": 6},
        )

        self.assert_compile(
            7 < all_(c),
            "%(param_1)s < ALL (x)",
            checkparams={"param_1": 7},
        )

        self.assert_compile(
            c.all_() == 5,
            "%(param_1)s = ALL (x)",
            checkparams={"param_1": 5},
        )

        self.assert_compile(
            5 == c.all_(),
            "%(param_1)s = ALL (x)",
            checkparams={"param_1": 5},
        )

        self.assert_compile(
            ~(5 == all_(c)),
            "NOT (%(param_1)s = ALL (x))",
            checkparams={"param_1": 5},
        )

        self.assert_compile(
            ~(all_(c) == 5),
            "NOT (%(param_1)s = ALL (x))",
            checkparams={"param_1": 5},
        )

    def test_array_deprecated_any_all(self):
        c = Column("x", postgresql.ARRAY(Integer))

        with self._array_any_deprecation():
            self.assert_compile(
                postgresql.Any(4, c),
                "%(x_1)s = ANY (x)",
                checkparams={"x_1": 4},
            )

        with self._array_any_deprecation():
            self.assert_compile(
                c.any(5),
                "%(x_1)s = ANY (x)",
                checkparams={"x_1": 5},
            )

        with self._array_any_deprecation():
            self.assert_compile(
                ~c.any(5),
                "NOT (%(x_1)s = ANY (x))",
                checkparams={"x_1": 5},
            )

        with self._array_any_deprecation():
            self.assert_compile(
                c.any(5, operator=operators.ne),
                "%(x_1)s != ANY (x)",
                checkparams={"x_1": 5},
            )

        with self._array_any_deprecation():
            self.assert_compile(
                postgresql.All(6, c, operator=operators.gt),
                "%(x_1)s > ALL (x)",
                checkparams={"x_1": 6},
            )

        with self._array_any_deprecation():
            self.assert_compile(
                c.all(7, operator=operators.lt),
                "%(x_1)s < ALL (x)",
                checkparams={"x_1": 7},
            )

        with self._array_any_deprecation():
            self.assert_compile(
                c.all(5),
                "%(x_1)s = ALL (x)",
                checkparams={"x_1": 5},
            )

        with self._array_any_deprecation():
            self.assert_compile(
                ~c.all(5),
                "NOT (%(x_1)s = ALL (x))",
                checkparams={"x_1": 5},
            )

    @testing.combinations(
        (lambda c: c.overlap, "&&"),
        (lambda c: c.contains, "@>"),
        (lambda c: c.contained_by, "<@"),
    )
    def test_overlap_no_cartesian(self, op_fn, expected_op):
        """test #6886"""
        t1 = table(
            "t1",
            column("id", Integer),
            column("ancestor_ids", postgresql.ARRAY(Integer)),
        )

        t1a = t1.alias()
        t1b = t1.alias()

        stmt = (
            select(t1, t1a, t1b)
            .where(op_fn(t1a.c.ancestor_ids)(postgresql.array((t1.c.id,))))
            .where(op_fn(t1b.c.ancestor_ids)(postgresql.array((t1.c.id,))))
        )

        self.assert_compile(
            stmt,
            "SELECT t1.id, t1.ancestor_ids, t1_1.id AS id_1, "
            "t1_1.ancestor_ids AS ancestor_ids_1, t1_2.id AS id_2, "
            "t1_2.ancestor_ids AS ancestor_ids_2 "
            "FROM t1, t1 AS t1_1, t1 AS t1_2 "
            "WHERE t1_1.ancestor_ids %(op)s ARRAY[t1.id] "
            "AND t1_2.ancestor_ids %(op)s ARRAY[t1.id]" % {"op": expected_op},
            from_linting=True,
        )

    @testing.combinations((True,), (False,))
    def test_array_zero_indexes(self, zero_indexes):
        c = Column("x", postgresql.ARRAY(Integer, zero_indexes=zero_indexes))

        add_one = 1 if zero_indexes else 0

        self.assert_compile(
            cast(c, postgresql.ARRAY(Integer, zero_indexes=zero_indexes)),
            "CAST(x AS INTEGER[])",
        )
        self.assert_compile(
            c[5], "x[%(x_1)s]", checkparams={"x_1": 5 + add_one}
        )

        self.assert_compile(
            c[5:7],
            "x[%(x_1)s:%(x_2)s]",
            checkparams={"x_2": 7 + add_one, "x_1": 5 + add_one},
        )
        self.assert_compile(
            c[5:7][2:3],
            "x[%(x_1)s:%(x_2)s][%(param_1)s:%(param_2)s]",
            checkparams={
                "x_2": 7 + add_one,
                "x_1": 5 + add_one,
                "param_1": 2 + add_one,
                "param_2": 3 + add_one,
            },
        )
        self.assert_compile(
            c[5:7][3],
            "x[%(x_1)s:%(x_2)s][%(param_1)s]",
            checkparams={
                "x_2": 7 + add_one,
                "x_1": 5 + add_one,
                "param_1": 3 + add_one,
            },
        )

    def test_array_literal_type(self):
        isinstance(postgresql.array([1, 2]).type, postgresql.ARRAY)
        is_(postgresql.array([1, 2]).type.item_type._type_affinity, Integer)

        is_(
            postgresql.array(
                [1, 2], type_=String
            ).type.item_type._type_affinity,
            String,
        )

    @testing.combinations(
        ("with type_", Date, "ARRAY[]::DATE[]"),
        ("no type_", None, "ARRAY[]"),
        id_="iaa",
    )
    def test_array_literal_empty(self, type_, expected):
        self.assert_compile(postgresql.array([], type_=type_), expected)

    def test_array_literal(self):
        self.assert_compile(
            func.array_dims(
                postgresql.array([1, 2]) + postgresql.array([3, 4, 5])
            ),
            "array_dims(ARRAY[%(param_1)s, %(param_2)s] || "
            "ARRAY[%(param_3)s, %(param_4)s, %(param_5)s])",
            checkparams={
                "param_5": 5,
                "param_4": 4,
                "param_1": 1,
                "param_3": 3,
                "param_2": 2,
            },
        )

    def test_array_literal_compare(self):
        self.assert_compile(
            postgresql.array([1, 2]) == [3, 4, 5],
            "ARRAY[%(param_1)s, %(param_2)s] = "
            "ARRAY[%(param_3)s, %(param_4)s, %(param_5)s]",
            checkparams={
                "param_5": 5,
                "param_4": 4,
                "param_1": 1,
                "param_3": 3,
                "param_2": 2,
            },
        )

    def test_array_literal_contains(self):
        self.assert_compile(
            postgresql.array([1, 2]).contains([3, 4, 5]),
            "ARRAY[%(param_1)s, %(param_2)s] @> ARRAY[%(param_3)s, "
            "%(param_4)s, %(param_5)s]",
            checkparams={
                "param_1": 1,
                "param_2": 2,
                "param_3": 3,
                "param_4": 4,
                "param_5": 5,
            },
        )

        self.assert_compile(
            postgresql.array(["a", "b"]).contains([""]),
            "ARRAY[%(param_1)s, %(param_2)s] @> ARRAY[%(param_3)s]",
            checkparams={"param_1": "a", "param_2": "b", "param_3": ""},
        )

        self.assert_compile(
            postgresql.array(["a", "b"]).contains([]),
            "ARRAY[%(param_1)s, %(param_2)s] @> ARRAY[]",
            checkparams={"param_1": "a", "param_2": "b"},
        )

        self.assert_compile(
            postgresql.array(["a", "b"]).contains([0]),
            "ARRAY[%(param_1)s, %(param_2)s] @> ARRAY[%(param_3)s]",
            checkparams={"param_1": "a", "param_2": "b", "param_3": 0},
        )

    def test_array_literal_contained_by(self):
        self.assert_compile(
            postgresql.array(["a", "b"]).contained_by(["a", "b", "c"]),
            "ARRAY[%(param_1)s, %(param_2)s] <@ ARRAY[%(param_3)s, "
            "%(param_4)s, %(param_5)s]",
            checkparams={
                "param_1": "a",
                "param_2": "b",
                "param_3": "a",
                "param_4": "b",
                "param_5": "c",
            },
        )

        self.assert_compile(
            postgresql.array([1, 2]).contained_by([3, 4, 5]),
            "ARRAY[%(param_1)s, %(param_2)s] <@ ARRAY[%(param_3)s, "
            "%(param_4)s, %(param_5)s]",
            checkparams={
                "param_1": 1,
                "param_2": 2,
                "param_3": 3,
                "param_4": 4,
                "param_5": 5,
            },
        )

        self.assert_compile(
            postgresql.array(["a", "b"]).contained_by([""]),
            "ARRAY[%(param_1)s, %(param_2)s] <@ ARRAY[%(param_3)s]",
            checkparams={"param_1": "a", "param_2": "b", "param_3": ""},
        )

        self.assert_compile(
            postgresql.array(["a", "b"]).contained_by([]),
            "ARRAY[%(param_1)s, %(param_2)s] <@ ARRAY[]",
            checkparams={"param_1": "a", "param_2": "b"},
        )

        self.assert_compile(
            postgresql.array(["a", "b"]).contained_by([0]),
            "ARRAY[%(param_1)s, %(param_2)s] <@ ARRAY[%(param_3)s]",
            checkparams={"param_1": "a", "param_2": "b", "param_3": 0},
        )

    def test_array_literal_insert(self):
        m = MetaData()
        t = Table("t", m, Column("data", postgresql.ARRAY(Integer)))
        self.assert_compile(
            t.insert().values(data=array([1, 2, 3])),
            "INSERT INTO t (data) VALUES (ARRAY[%(param_1)s, "
            "%(param_2)s, %(param_3)s])",
        )

    def test_update_array(self):
        m = MetaData()
        t = Table("t", m, Column("data", postgresql.ARRAY(Integer)))
        self.assert_compile(
            t.update().values({t.c.data: [1, 3, 4]}),
            "UPDATE t SET data=%(data)s::INTEGER[]",
            checkparams={"data": [1, 3, 4]},
        )

    def test_update_array_element(self):
        m = MetaData()
        t = Table("t", m, Column("data", postgresql.ARRAY(Integer)))
        self.assert_compile(
            t.update().values({t.c.data[5]: 1}),
            "UPDATE t SET data[%(data_1)s]=%(param_1)s",
            checkparams={"data_1": 5, "param_1": 1},
        )

    def test_update_array_slice(self):
        m = MetaData()
        t = Table("t", m, Column("data", postgresql.ARRAY(Integer)))

        # psycopg2-specific, has a cast
        self.assert_compile(
            t.update().values({t.c.data[2:5]: [2, 3, 4]}),
            "UPDATE t SET data[%(data_1)s:%(data_2)s]="
            "%(param_1)s::INTEGER[]",
            checkparams={"param_1": [2, 3, 4], "data_2": 5, "data_1": 2},
            dialect=PGDialect_psycopg2(),
        )

        # default dialect does not, as DBAPIs may be doing this for us
        self.assert_compile(
            t.update().values({t.c.data[2:5]: [2, 3, 4]}),
            "UPDATE t SET data[%s:%s]=%s",
            checkparams={"param_1": [2, 3, 4], "data_2": 5, "data_1": 2},
            dialect=PGDialect(paramstyle="format"),
        )

    def test_from_only(self):
        m = MetaData()
        tbl1 = Table("testtbl1", m, Column("id", Integer))
        tbl2 = Table("testtbl2", m, Column("id", Integer))

        stmt = tbl1.select().with_hint(tbl1, "ONLY", "postgresql")
        expected = "SELECT testtbl1.id FROM ONLY testtbl1"
        self.assert_compile(stmt, expected)

        talias1 = tbl1.alias("foo")
        stmt = talias1.select().with_hint(talias1, "ONLY", "postgresql")
        expected = "SELECT foo.id FROM ONLY testtbl1 AS foo"
        self.assert_compile(stmt, expected)

        stmt = select(tbl1, tbl2).with_hint(tbl1, "ONLY", "postgresql")
        expected = (
            "SELECT testtbl1.id, testtbl2.id AS id_1 FROM ONLY testtbl1, "
            "testtbl2"
        )
        self.assert_compile(stmt, expected)

        stmt = select(tbl1, tbl2).with_hint(tbl2, "ONLY", "postgresql")
        expected = (
            "SELECT testtbl1.id, testtbl2.id AS id_1 FROM testtbl1, ONLY "
            "testtbl2"
        )
        self.assert_compile(stmt, expected)

        stmt = select(tbl1, tbl2)
        stmt = stmt.with_hint(tbl1, "ONLY", "postgresql")
        stmt = stmt.with_hint(tbl2, "ONLY", "postgresql")
        expected = (
            "SELECT testtbl1.id, testtbl2.id AS id_1 FROM ONLY testtbl1, "
            "ONLY testtbl2"
        )
        self.assert_compile(stmt, expected)

        stmt = update(tbl1).values(dict(id=1))
        stmt = stmt.with_hint("ONLY", dialect_name="postgresql")
        expected = "UPDATE ONLY testtbl1 SET id=%(id)s"
        self.assert_compile(stmt, expected)

        stmt = delete(tbl1).with_hint(
            "ONLY", selectable=tbl1, dialect_name="postgresql"
        )
        expected = "DELETE FROM ONLY testtbl1"
        self.assert_compile(stmt, expected)

        tbl3 = Table("testtbl3", m, Column("id", Integer), schema="testschema")
        stmt = tbl3.select().with_hint(tbl3, "ONLY", "postgresql")
        expected = (
            "SELECT testschema.testtbl3.id FROM ONLY testschema.testtbl3"
        )
        self.assert_compile(stmt, expected)

        assert_raises(
            exc.CompileError,
            tbl3.select().with_hint(tbl3, "FAKE", "postgresql").compile,
            dialect=postgresql.dialect(),
        )

    def test_aggregate_order_by_one(self):
        m = MetaData()
        table = Table("table1", m, Column("a", Integer), Column("b", Integer))
        expr = func.array_agg(aggregate_order_by(table.c.a, table.c.b.desc()))
        stmt = select(expr)

        # note this tests that the object exports FROM objects
        # correctly
        self.assert_compile(
            stmt,
            "SELECT array_agg(table1.a ORDER BY table1.b DESC) "
            "AS array_agg_1 FROM table1",
        )

    def test_aggregate_order_by_two(self):
        m = MetaData()
        table = Table("table1", m, Column("a", Integer), Column("b", Integer))
        expr = func.string_agg(
            table.c.a, aggregate_order_by(literal_column("','"), table.c.a)
        )
        stmt = select(expr)

        self.assert_compile(
            stmt,
            "SELECT string_agg(table1.a, ',' ORDER BY table1.a) "
            "AS string_agg_1 FROM table1",
        )

    def test_aggregate_order_by_multi_col(self):
        m = MetaData()
        table = Table("table1", m, Column("a", Integer), Column("b", Integer))
        expr = func.string_agg(
            table.c.a,
            aggregate_order_by(
                literal_column("','"), table.c.a, table.c.b.desc()
            ),
        )
        stmt = select(expr)

        self.assert_compile(
            stmt,
            "SELECT string_agg(table1.a, "
            "',' ORDER BY table1.a, table1.b DESC) "
            "AS string_agg_1 FROM table1",
        )

    def test_aggregate_orcer_by_no_arg(self):
        assert_raises_message(
            TypeError,
            "at least one ORDER BY element is required",
            aggregate_order_by,
            literal_column("','"),
        )

    def test_pg_array_agg_implicit_pg_array(self):
        expr = pg_array_agg(column("data", Integer))
        assert isinstance(expr.type, PG_ARRAY)
        is_(expr.type.item_type._type_affinity, Integer)

    def test_pg_array_agg_uses_base_array(self):
        expr = pg_array_agg(column("data", sqltypes.ARRAY(Integer)))
        assert isinstance(expr.type, sqltypes.ARRAY)
        assert not isinstance(expr.type, PG_ARRAY)
        is_(expr.type.item_type._type_affinity, Integer)

    def test_pg_array_agg_uses_pg_array(self):
        expr = pg_array_agg(column("data", PG_ARRAY(Integer)))
        assert isinstance(expr.type, PG_ARRAY)
        is_(expr.type.item_type._type_affinity, Integer)

    def test_pg_array_agg_explicit_base_array(self):
        expr = pg_array_agg(
            column("data", sqltypes.ARRAY(Integer)),
            type_=sqltypes.ARRAY(Integer),
        )
        assert isinstance(expr.type, sqltypes.ARRAY)
        assert not isinstance(expr.type, PG_ARRAY)
        is_(expr.type.item_type._type_affinity, Integer)

    def test_pg_array_agg_explicit_pg_array(self):
        expr = pg_array_agg(
            column("data", sqltypes.ARRAY(Integer)), type_=PG_ARRAY(Integer)
        )
        assert isinstance(expr.type, PG_ARRAY)
        is_(expr.type.item_type._type_affinity, Integer)

    def test_aggregate_order_by_adapt(self):
        m = MetaData()
        table = Table("table1", m, Column("a", Integer), Column("b", Integer))
        expr = func.array_agg(aggregate_order_by(table.c.a, table.c.b.desc()))
        stmt = select(expr)

        a1 = table.alias("foo")
        stmt2 = sql_util.ClauseAdapter(a1).traverse(stmt)
        self.assert_compile(
            stmt2,
            "SELECT array_agg(foo.a ORDER BY foo.b DESC) AS array_agg_1 "
            "FROM table1 AS foo",
        )

    def test_array_agg_w_filter_subscript(self):
        series = func.generate_series(1, 100).alias("series")
        series_col = column("series")
        query = select(
            func.array_agg(series_col).filter(series_col % 2 == 0)[3]
        ).select_from(series)
        self.assert_compile(
            query,
            "SELECT (array_agg(series) FILTER "
            "(WHERE series %% %(series_1)s = %(param_1)s))[%(param_2)s] "
            "AS anon_1 FROM "
            "generate_series(%(generate_series_1)s, %(generate_series_2)s) "
            "AS series",
        )

    def test_delete_extra_froms(self):
        t1 = table("t1", column("c1"))
        t2 = table("t2", column("c1"))
        q = delete(t1).where(t1.c.c1 == t2.c.c1)
        self.assert_compile(q, "DELETE FROM t1 USING t2 WHERE t1.c1 = t2.c1")

    def test_delete_extra_froms_alias(self):
        a1 = table("t1", column("c1")).alias("a1")
        t2 = table("t2", column("c1"))
        q = delete(a1).where(a1.c.c1 == t2.c.c1)
        self.assert_compile(
            q, "DELETE FROM t1 AS a1 USING t2 WHERE a1.c1 = t2.c1"
        )

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
            "CREATE TABLE t (x INTEGER, y INTEGER GENERATED "
            "ALWAYS AS (x + 2)%s)" % text,
        )

    def test_column_computed_persisted_false_old_version(self):
        m = MetaData()
        t = Table(
            "t",
            m,
            Column("x", Integer),
            Column("y", Integer, Computed("x + 2", persisted=False)),
        )
        old_dialect = postgresql.dialect()
        old_dialect.supports_virtual_generated_columns = False
        with expect_raises_message(
            exc.CompileError,
            "PostrgreSQL computed columns do not support 'virtual'",
        ):
            schema.CreateTable(t).compile(dialect=old_dialect)

    def test_column_computed_persisted_none_warning_old_version(self):
        m = MetaData()
        t = Table(
            "t",
            m,
            Column("x", Integer),
            Column("y", Integer, Computed("x + 2")),
        )
        old_dialect = postgresql.dialect()
        old_dialect.supports_virtual_generated_columns = False

        with expect_warnings(
            "Computed column t.y is being created as 'STORED' since"
        ):
            self.assert_compile(
                schema.CreateTable(t),
                "CREATE TABLE t (x INTEGER, y INTEGER GENERATED "
                "ALWAYS AS (x + 2) STORED)",
                dialect=old_dialect,
            )

    @testing.combinations(True, False)
    def test_column_identity(self, pk):
        # all other tests are in test_identity_column.py
        m = MetaData()
        t = Table(
            "t",
            m,
            Column(
                "y",
                Integer,
                Identity(always=True, start=4, increment=7),
                primary_key=pk,
            ),
        )
        self.assert_compile(
            schema.CreateTable(t),
            "CREATE TABLE t (y INTEGER GENERATED ALWAYS AS IDENTITY "
            "(INCREMENT BY 7 START WITH 4)%s)"
            % (", PRIMARY KEY (y)" if pk else ""),
        )

    @testing.combinations(True, False)
    def test_column_identity_no_support(self, pk):
        m = MetaData()
        t = Table(
            "t",
            m,
            Column(
                "y",
                Integer,
                Identity(always=True, start=4, increment=7),
                primary_key=pk,
            ),
        )
        dd = PGDialect()
        dd.supports_identity_columns = False
        self.assert_compile(
            schema.CreateTable(t),
            "CREATE TABLE t (y %s%s)"
            % (
                "SERIAL NOT NULL" if pk else "INTEGER NOT NULL",
                ", PRIMARY KEY (y)" if pk else "",
            ),
            dialect=dd,
        )

    def test_column_identity_null(self):
        # all other tests are in test_identity_column.py
        m = MetaData()
        t = Table(
            "t",
            m,
            Column(
                "y",
                Integer,
                Identity(always=True, start=4, increment=7),
                nullable=True,
            ),
        )
        self.assert_compile(
            schema.CreateTable(t),
            "CREATE TABLE t (y INTEGER GENERATED ALWAYS AS IDENTITY "
            "(INCREMENT BY 7 START WITH 4) NULL)",
        )

    def test_index_extra_include_1(self):
        metadata = MetaData()
        tbl = Table(
            "test",
            metadata,
            Column("x", Integer),
            Column("y", Integer),
            Column("z", Integer),
        )
        idx = Index("foo", tbl.c.x, postgresql_include=["y"])
        self.assert_compile(
            schema.CreateIndex(idx), "CREATE INDEX foo ON test (x) INCLUDE (y)"
        )

    def test_index_extra_include_2(self):
        metadata = MetaData()
        tbl = Table(
            "test",
            metadata,
            Column("x", Integer),
            Column("y", Integer),
            Column("z", Integer),
        )
        idx = Index("foo", tbl.c.x, postgresql_include=[tbl.c.y])
        self.assert_compile(
            schema.CreateIndex(idx), "CREATE INDEX foo ON test (x) INCLUDE (y)"
        )

    @testing.fixture
    def update_tables(self):
        self.weather = table(
            "weather",
            column("temp_lo", Integer),
            column("temp_hi", Integer),
            column("prcp", Integer),
            column("city", String),
            column("date", Date),
        )
        self.accounts = table(
            "accounts",
            column("sales_id", Integer),
            column("sales_person", Integer),
            column("contact_first_name", String),
            column("contact_last_name", String),
            column("name", String),
        )
        self.salesmen = table(
            "salesmen",
            column("id", Integer),
            column("first_name", String),
            column("last_name", String),
        )
        self.employees = table(
            "employees",
            column("id", Integer),
            column("sales_count", String),
        )

    # from examples at https://www.postgresql.org/docs/current/sql-update.html
    def test_difficult_update_1(self, update_tables):
        update = (
            self.weather.update()
            .where(self.weather.c.city == "San Francisco")
            .where(self.weather.c.date == "2003-07-03")
            .values(
                {
                    tuple_(
                        self.weather.c.temp_lo,
                        self.weather.c.temp_hi,
                        self.weather.c.prcp,
                    ): tuple_(
                        self.weather.c.temp_lo + 1,
                        self.weather.c.temp_lo + 15,
                        literal_column("DEFAULT"),
                    )
                }
            )
        )

        self.assert_compile(
            update,
            "UPDATE weather SET (temp_lo, temp_hi, prcp)=(weather.temp_lo + "
            "%(temp_lo_1)s, weather.temp_lo + %(temp_lo_2)s, DEFAULT) "
            "WHERE weather.city = %(city_1)s AND weather.date = %(date_1)s",
            {
                "city_1": "San Francisco",
                "date_1": "2003-07-03",
                "temp_lo_1": 1,
                "temp_lo_2": 15,
            },
        )

    def test_difficult_update_2(self, update_tables):
        update = self.accounts.update().values(
            {
                tuple_(
                    self.accounts.c.contact_first_name,
                    self.accounts.c.contact_last_name,
                ): select(
                    self.salesmen.c.first_name, self.salesmen.c.last_name
                )
                .where(self.salesmen.c.id == self.accounts.c.sales_id)
                .scalar_subquery()
            }
        )

        self.assert_compile(
            update,
            "UPDATE accounts SET (contact_first_name, contact_last_name)="
            "(SELECT salesmen.first_name, salesmen.last_name FROM "
            "salesmen WHERE salesmen.id = accounts.sales_id)",
        )

    def test_difficult_update_3(self, update_tables):
        update = (
            self.employees.update()
            .values(
                {
                    self.employees.c.sales_count: self.employees.c.sales_count
                    + 1
                }
            )
            .where(
                self.employees.c.id
                == select(self.accounts.c.sales_person)
                .where(self.accounts.c.name == "Acme Corporation")
                .scalar_subquery()
            )
        )

        self.assert_compile(
            update,
            "UPDATE employees SET sales_count=(employees.sales_count "
            "+ %(sales_count_1)s) WHERE employees.id = (SELECT "
            "accounts.sales_person FROM accounts WHERE "
            "accounts.name = %(name_1)s)",
            {"sales_count_1": 1, "name_1": "Acme Corporation"},
        )

    def test_difficult_update_4(self):
        summary = table(
            "summary",
            column("group_id", Integer),
            column("sum_y", Float),
            column("sum_x", Float),
            column("avg_x", Float),
            column("avg_y", Float),
        )
        data = table(
            "data",
            column("group_id", Integer),
            column("x", Float),
            column("y", Float),
        )

        update = summary.update().values(
            {
                tuple_(
                    summary.c.sum_x,
                    summary.c.sum_y,
                    summary.c.avg_x,
                    summary.c.avg_y,
                ): select(
                    func.sum(data.c.x),
                    func.sum(data.c.y),
                    func.avg(data.c.x),
                    func.avg(data.c.y),
                )
                .where(data.c.group_id == summary.c.group_id)
                .scalar_subquery()
            }
        )
        self.assert_compile(
            update,
            "UPDATE summary SET (sum_x, sum_y, avg_x, avg_y)="
            "(SELECT sum(data.x) AS sum_1, sum(data.y) AS sum_2, "
            "avg(data.x) AS avg_1, avg(data.y) AS avg_2 FROM data "
            "WHERE data.group_id = summary.group_id)",
        )

    @testing.combinations(JSONB.JSONPathType, JSONPATH)
    def test_json_path(self, type_):
        data = table("data", column("id", Integer), column("x", JSONB))
        stmt = select(
            func.jsonb_path_exists(data.c.x, cast("$.data.w", type_))
        )
        self.assert_compile(
            stmt,
            "SELECT jsonb_path_exists(data.x, CAST(%(param_1)s AS JSONPATH)) "
            "AS jsonb_path_exists_1 FROM data",
        )

    @testing.combinations(
        (
            lambda col: col["foo"] + " ",
            "x[%(x_1)s] || %(param_1)s",
        ),
        (
            lambda col: col["foo"] + " " + col["bar"],
            "x[%(x_1)s] || %(param_1)s || x[%(x_2)s]",
        ),
        argnames="expr, expected",
    )
    def test_eager_grouping_flag(self, expr, expected):
        """test #10479"""
        col = Column("x", JSONB)

        expr = testing.resolve_lambda(expr, col=col)

        # Choose expected result based on type
        self.assert_compile(expr, expected)

    @testing.variation("pgversion", ["pg14", "pg13"])
    def test_jsonb_subscripting(self, pgversion):
        """test #10927 - PostgreSQL 14+ JSONB subscripting syntax"""
        data = table("data", column("id", Integer), column("x", JSONB))

        dialect = postgresql.dialect()

        if pgversion.pg13:
            dialect._supports_jsonb_subscripting = False

        # Test SELECT with JSONB indexing
        stmt = select(data.c.x["key"])
        self.assert_compile(
            stmt,
            (
                "SELECT data.x[%(x_1)s] AS anon_1 FROM data"
                if pgversion.pg14
                else "SELECT data.x -> %(x_1)s AS anon_1 FROM data"
            ),
            dialect=dialect,
        )

        # Test UPDATE with JSONB indexing (the original issue case)
        stmt = update(data).values({data.c.x["new_key"]: data.c.x["old_key"]})
        self.assert_compile(
            stmt,
            (
                "UPDATE data SET x[%(x_1)s]=(data.x[%(x_2)s])"
                if pgversion.pg14
                else "UPDATE data SET x -> %(x_1)s=(data.x -> %(x_2)s)"
            ),
            dialect=dialect,
        )

    def test_json_still_uses_arrow_syntax(self):
        """test #10927 - JSON type still uses arrow syntax even on PG 14+"""
        data = table("data", column("id", Integer), column("x", JSON))

        # Test PostgreSQL 14+ still uses arrow syntax for JSON (not JSONB)

        # Test SELECT with JSON indexing
        stmt = select(data.c.x["key"])
        self.assert_compile(
            stmt,
            "SELECT data.x -> %(x_1)s AS anon_1 FROM data",
        )

        # Test UPDATE with JSON indexing
        stmt = update(data).values({data.c.x["new_key"]: data.c.x["old_key"]})
        self.assert_compile(
            stmt,
            "UPDATE data SET x -> %(x_1)s=(data.x -> %(x_2)s)",
        )

    def test_jsonb_functions_use_parentheses_with_subscripting(self):
        """test #12778 - JSONB functions are parenthesized with [] syntax"""
        data = table("data", column("id", Integer), column("x", JSONB))

        # Test that JSONB functions are properly parenthesized with [] syntax
        # This ensures correct PostgreSQL syntax: (function_call)[index]
        # instead of the invalid: function_call[index]

        stmt = select(func.jsonb_array_elements(data.c.x, type_=JSONB)["key"])
        self.assert_compile(
            stmt,
            "SELECT "
            "(jsonb_array_elements(data.x))[%(jsonb_array_elements_1)s] "
            "AS anon_1 FROM data",
        )

        # Test with nested function calls
        stmt = select(
            func.jsonb_array_elements(data.c.x["items"], type_=JSONB)["key"]
        )
        self.assert_compile(
            stmt,
            "SELECT (jsonb_array_elements(data.x[%(x_1)s]))"
            "[%(jsonb_array_elements_1)s] AS anon_1 FROM data",
        )

    def test_range_custom_object_hook(self):
        # See issue #8884
        from datetime import date

        usages = table(
            "usages",
            column("id", Integer),
            column("date", Date),
            column("amount", Integer),
        )
        period = Range(date(2022, 1, 1), (2023, 1, 1))
        stmt = select(func.sum(usages.c.amount)).where(
            usages.c.date.op("<@")(period)
        )
        self.assert_compile(
            stmt,
            "SELECT sum(usages.amount) AS sum_1 FROM usages "
            "WHERE usages.date <@ %(date_1)s::DATERANGE",
        )

    def test_multirange_custom_object_hook(self):
        from datetime import date

        usages = table(
            "usages",
            column("id", Integer),
            column("date", Date),
            column("amount", Integer),
        )
        period = MultiRange(
            [
                Range(date(2022, 1, 1), (2023, 1, 1)),
                Range(date(2024, 1, 1), (2025, 1, 1)),
            ]
        )
        stmt = select(func.sum(usages.c.amount)).where(
            usages.c.date.op("<@")(period)
        )
        self.assert_compile(
            stmt,
            "SELECT sum(usages.amount) AS sum_1 FROM usages "
            "WHERE usages.date <@ %(date_1)s::DATEMULTIRANGE",
        )

    def test_bitwise_xor(self):
        c1 = column("c1", Integer)
        c2 = column("c2", Integer)
        self.assert_compile(
            select(c1.bitwise_xor(c2)),
            "SELECT c1 # c2 AS anon_1",
        )

    def test_ilike_escaping(self):
        dialect = postgresql.dialect()
        self.assert_compile(
            sql.column("foo").ilike("bar", escape="\\"),
            "foo ILIKE %(foo_1)s ESCAPE '\\\\'",
        )

        self.assert_compile(
            sql.column("foo").ilike("bar", escape=""),
            "foo ILIKE %(foo_1)s ESCAPE ''",
            dialect=dialect,
        )

        self.assert_compile(
            sql.column("foo").notilike("bar", escape="\\"),
            "foo NOT ILIKE %(foo_1)s ESCAPE '\\\\'",
        )

        self.assert_compile(
            sql.column("foo").notilike("bar", escape=""),
            "foo NOT ILIKE %(foo_1)s ESCAPE ''",
            dialect=dialect,
        )

    @testing.combinations(
        (lambda t: t.c.a**t.c.b, "power(t.a, t.b)", {}),
        (lambda t: t.c.a**3, "power(t.a, %(pow_1)s)", {"pow_1": 3}),
        (lambda t: func.pow(t.c.a, 3), "power(t.a, %(pow_1)s)", {"pow_1": 3}),
        (lambda t: func.power(t.c.a, t.c.b), "power(t.a, t.b)", {}),
    )
    def test_simple_compile(self, fn, string, params):
        t = table("t", column("a", Integer), column("b", Integer))
        expr = resolve_lambda(fn, t=t)
        self.assert_compile(expr, string, params)


class InsertOnConflictTest(
    fixtures.TablesTest, AssertsCompiledSQL, fixtures.CacheKeySuite
):
    __dialect__ = postgresql.dialect()

    run_create_tables = None

    @classmethod
    def define_tables(cls, metadata):
        cls.table1 = table1 = table(
            "mytable",
            column("myid", Integer),
            column("name", String(128)),
            column("description", String(128)),
        )
        cls.table_with_metadata = Table(
            "mytable",
            metadata,
            Column("myid", Integer, primary_key=True),
            Column("name", String(128)),
            Column("description", String(128)),
        )
        cls.unique_constr = schema.UniqueConstraint(
            table1.c.name, name="uq_name"
        )
        cls.excl_constr = ExcludeConstraint(
            (table1.c.name, "="),
            (table1.c.description, "&&"),
            name="excl_thing",
        )
        cls.excl_constr_anon = ExcludeConstraint(
            (cls.table_with_metadata.c.name, "="),
            (cls.table_with_metadata.c.description, "&&"),
            where=cls.table_with_metadata.c.description != "foo",
        )
        cls.excl_constr_anon_str = ExcludeConstraint(
            (cls.table_with_metadata.c.name, "="),
            (cls.table_with_metadata.c.description, "&&"),
            where="description != 'foo'",
        )
        cls.goofy_index = Index(
            "goofy_index", table1.c.name, postgresql_where=table1.c.name > "m"
        )

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

    @testing.combinations(
        (
            lambda users, stmt: stmt.on_conflict_do_nothing(
                index_elements=["id"], index_where=text("name = 'hi'")
            ),
            "ON CONFLICT (id) WHERE name = 'hi' DO NOTHING",
        ),
        (
            lambda users, stmt: stmt.on_conflict_do_nothing(
                index_elements=[users.c.id], index_where=users.c.name == "hi"
            ),
            "ON CONFLICT (id) WHERE name = %(name_1)s DO NOTHING",
        ),
        (
            lambda users, stmt: stmt.on_conflict_do_nothing(
                index_elements=["id"], index_where="name = 'hi'"
            ),
            exc.ArgumentError,
        ),
        (
            lambda users, stmt: stmt.on_conflict_do_update(
                index_elements=[users.c.id],
                set_={users.c.name: "there"},
                where=users.c.name == "hi",
            ),
            "ON CONFLICT (id) DO UPDATE SET name = %(param_1)s "
            "WHERE users.name = %(name_1)s",
        ),
        (
            lambda users, stmt: stmt.on_conflict_do_update(
                index_elements=[users.c.id],
                set_={users.c.name: "there"},
                where=text("name = 'hi'"),
            ),
            "ON CONFLICT (id) DO UPDATE SET name = %(param_1)s "
            "WHERE name = 'hi'",
        ),
        (
            lambda users, stmt: stmt.on_conflict_do_update(
                index_elements=[users.c.id],
                set_={users.c.name: "there"},
                where="name = 'hi'",
            ),
            exc.ArgumentError,
        ),
    )
    def test_assorted_arg_coercion(self, case, expected):
        stmt = insert(self.tables.users)

        if isinstance(expected, type) and issubclass(expected, Exception):
            with expect_raises(expected):
                testing.resolve_lambda(
                    case, stmt=stmt, users=self.tables.users
                ),
        else:
            self.assert_compile(
                testing.resolve_lambda(
                    case, stmt=stmt, users=self.tables.users
                ),
                f"INSERT INTO users (id, name) VALUES (%(id)s, %(name)s) "
                f"{expected}",
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

        def stmt4():
            stmt = stmt0()

            return stmt.on_conflict_do_update(
                constraint=table.primary_key, set_=stmt.excluded
            )

        def stmt41():
            stmt = stmt0()

            return stmt.on_conflict_do_update(
                constraint=table.primary_key,
                set_=stmt.excluded,
                where=table.c.bar != random.choice(["q", "p", "r", "z"]),
            )

        def stmt42():
            stmt = stmt0()

            return stmt.on_conflict_do_update(
                constraint=table.primary_key,
                set_=stmt.excluded,
                where=table.c.baz != random.choice(["q", "p", "r", "z"]),
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
            stmt4(),
            stmt41(),
            stmt42(),
        ]

    @testing.combinations("control", "excluded", "dict")
    def test_set_excluded(self, scenario):
        """test #8014, sending all of .excluded to set"""

        if scenario == "control":
            users = self.tables.users

            stmt = insert(users)
            self.assert_compile(
                stmt.on_conflict_do_update(
                    constraint=users.primary_key, set_=stmt.excluded
                ),
                "INSERT INTO users (id, name) VALUES (%(id)s, %(name)s) ON "
                "CONFLICT (id) DO UPDATE "
                "SET id = excluded.id, name = excluded.name",
            )
        else:
            users_w_key = self.tables.users_w_key

            stmt = insert(users_w_key)

            if scenario == "excluded":
                self.assert_compile(
                    stmt.on_conflict_do_update(
                        constraint=users_w_key.primary_key, set_=stmt.excluded
                    ),
                    "INSERT INTO users_w_key (id, name) "
                    "VALUES (%(id)s, %(name_keyed)s) ON "
                    "CONFLICT (id) DO UPDATE "
                    "SET id = excluded.id, name = excluded.name",
                )
            else:
                self.assert_compile(
                    stmt.on_conflict_do_update(
                        constraint=users_w_key.primary_key,
                        set_={
                            "id": stmt.excluded.id,
                            "name_keyed": stmt.excluded.name_keyed,
                        },
                    ),
                    "INSERT INTO users_w_key (id, name) "
                    "VALUES (%(id)s, %(name_keyed)s) ON "
                    "CONFLICT (id) DO UPDATE "
                    "SET id = excluded.id, name = excluded.name",
                )

    def test_dont_consume_set_collection(self):
        users = self.tables.users
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
            "INSERT INTO users (name) VALUES (%(name_m0)s), (%(name_m1)s) "
            "ON CONFLICT (name) DO UPDATE SET name = excluded.name",
        )
        stmt = stmt.returning(users)
        self.assert_compile(
            stmt,
            "INSERT INTO users (name) VALUES (%(name_m0)s), (%(name_m1)s) "
            "ON CONFLICT (name) DO UPDATE SET name = excluded.name "
            "RETURNING users.id, users.name",
        )

    def test_on_conflict_do_no_call_twice(self):
        users = self.table1

        for stmt in (
            insert(users).on_conflict_do_nothing(),
            insert(users).on_conflict_do_update(
                index_elements=[users.c.myid], set_=dict(name="foo")
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

    def test_on_conflict_cte_plus_textual(self):
        """test #7798"""

        bar = table("bar", column("id"), column("attr"), column("foo_id"))
        s1 = text("SELECT bar.id, bar.attr FROM bar").columns(
            bar.c.id, bar.c.attr
        )
        s2 = (
            insert(bar)
            .from_select(list(s1.selected_columns), s1)
            .on_conflict_do_update(
                index_elements=[s1.selected_columns.id],
                set_={"attr": s1.selected_columns.attr},
            )
        )

        self.assert_compile(
            s2,
            "INSERT INTO bar (id, attr) SELECT bar.id, bar.attr "
            "FROM bar ON CONFLICT (id) DO UPDATE SET attr = bar.attr",
        )

    def test_do_nothing_no_target(self):
        i = (
            insert(self.table1)
            .values(dict(name="foo"))
            .on_conflict_do_nothing()
        )
        self.assert_compile(
            i,
            "INSERT INTO mytable (name) VALUES "
            "(%(name)s) ON CONFLICT DO NOTHING",
        )

    def test_do_nothing_index_elements_target(self):
        i = (
            insert(self.table1)
            .values(dict(name="foo"))
            .on_conflict_do_nothing(index_elements=["myid"])
        )
        self.assert_compile(
            i,
            "INSERT INTO mytable (name) VALUES "
            "(%(name)s) ON CONFLICT (myid) DO NOTHING",
        )

    def test_do_update_set_clause_none(self):
        i = insert(self.table_with_metadata).values(myid=1, name="foo")
        i = i.on_conflict_do_update(
            index_elements=["myid"],
            set_=OrderedDict([("name", "I'm a name"), ("description", None)]),
        )
        self.assert_compile(
            i,
            "INSERT INTO mytable (myid, name) VALUES "
            "(%(myid)s, %(name)s) ON CONFLICT (myid) "
            "DO UPDATE SET name = %(param_1)s, "
            "description = %(param_2)s",
            {
                "myid": 1,
                "name": "foo",
                "param_1": "I'm a name",
                "param_2": None,
            },
        )

    def test_do_update_set_clause_column_keys(self):
        i = insert(self.table_with_metadata).values(myid=1, name="foo")
        i = i.on_conflict_do_update(
            index_elements=["myid"],
            set_=OrderedDict(
                [
                    (self.table_with_metadata.c.name, "I'm a name"),
                    (self.table_with_metadata.c.description, None),
                ]
            ),
        )
        self.assert_compile(
            i,
            "INSERT INTO mytable (myid, name) VALUES "
            "(%(myid)s, %(name)s) ON CONFLICT (myid) "
            "DO UPDATE SET name = %(param_1)s, "
            "description = %(param_2)s",
            {
                "myid": 1,
                "name": "foo",
                "param_1": "I'm a name",
                "param_2": None,
            },
        )

    def test_do_update_set_clause_literal(self):
        i = insert(self.table_with_metadata).values(myid=1, name="foo")
        i = i.on_conflict_do_update(
            index_elements=["myid"],
            set_=OrderedDict(
                [("name", "I'm a name"), ("description", null())]
            ),
        )
        self.assert_compile(
            i,
            "INSERT INTO mytable (myid, name) VALUES "
            "(%(myid)s, %(name)s) ON CONFLICT (myid) "
            "DO UPDATE SET name = %(param_1)s, "
            "description = NULL",
            {"myid": 1, "name": "foo", "param_1": "I'm a name"},
        )

    def test_do_update_str_index_elements_target_one(self):
        i = insert(self.table_with_metadata).values(myid=1, name="foo")
        i = i.on_conflict_do_update(
            index_elements=["myid"],
            set_=OrderedDict(
                [
                    ("name", i.excluded.name),
                    ("description", i.excluded.description),
                ]
            ),
        )
        self.assert_compile(
            i,
            "INSERT INTO mytable (myid, name) VALUES "
            "(%(myid)s, %(name)s) ON CONFLICT (myid) "
            "DO UPDATE SET name = excluded.name, "
            "description = excluded.description",
        )

    def test_do_update_str_index_elements_target_two(self):
        i = insert(self.table1).values(dict(name="foo"))
        i = i.on_conflict_do_update(
            index_elements=["myid"], set_=dict(name=i.excluded.name)
        )
        self.assert_compile(
            i,
            "INSERT INTO mytable (name) VALUES "
            "(%(name)s) ON CONFLICT (myid) "
            "DO UPDATE SET name = excluded.name",
        )

    def test_do_update_col_index_elements_target(self):
        i = insert(self.table1).values(dict(name="foo"))
        i = i.on_conflict_do_update(
            index_elements=[self.table1.c.myid],
            set_=dict(name=i.excluded.name),
        )
        self.assert_compile(
            i,
            "INSERT INTO mytable (name) VALUES "
            "(%(name)s) ON CONFLICT (myid) "
            "DO UPDATE SET name = excluded.name",
        )

    def test_do_update_unnamed_pk_constraint_target(self):
        i = insert(self.table_with_metadata).values(dict(myid=1, name="foo"))
        i = i.on_conflict_do_update(
            constraint=self.table_with_metadata.primary_key,
            set_=dict(name=i.excluded.name),
        )
        self.assert_compile(
            i,
            "INSERT INTO mytable (myid, name) VALUES "
            "(%(myid)s, %(name)s) ON CONFLICT (myid) "
            "DO UPDATE SET name = excluded.name",
        )

    def test_do_update_pk_constraint_index_elements_target(self):
        i = insert(self.table_with_metadata).values(dict(myid=1, name="foo"))
        i = i.on_conflict_do_update(
            index_elements=self.table_with_metadata.primary_key,
            set_=dict(name=i.excluded.name),
        )
        self.assert_compile(
            i,
            "INSERT INTO mytable (myid, name) VALUES "
            "(%(myid)s, %(name)s) ON CONFLICT (myid) "
            "DO UPDATE SET name = excluded.name",
        )

    def test_do_update_named_unique_constraint_target(self):
        i = insert(self.table1).values(dict(name="foo"))
        i = i.on_conflict_do_update(
            constraint=self.unique_constr, set_=dict(myid=i.excluded.myid)
        )
        self.assert_compile(
            i,
            "INSERT INTO mytable (name) VALUES "
            "(%(name)s) ON CONFLICT ON CONSTRAINT uq_name "
            "DO UPDATE SET myid = excluded.myid",
        )

    def test_do_update_string_constraint_target(self):
        i = insert(self.table1).values(dict(name="foo"))
        i = i.on_conflict_do_update(
            constraint=self.unique_constr.name, set_=dict(myid=i.excluded.myid)
        )
        self.assert_compile(
            i,
            "INSERT INTO mytable (name) VALUES "
            "(%(name)s) ON CONFLICT ON CONSTRAINT uq_name "
            "DO UPDATE SET myid = excluded.myid",
        )

    def test_do_nothing_quoted_string_constraint_target(self):
        """test #6696"""
        i = insert(self.table1).values(dict(name="foo"))
        i = i.on_conflict_do_nothing(constraint="Some Constraint Name")
        self.assert_compile(
            i,
            "INSERT INTO mytable (name) VALUES "
            '(%(name)s) ON CONFLICT ON CONSTRAINT "Some Constraint Name" '
            "DO NOTHING",
        )

    def test_do_nothing_super_long_name_constraint_target(self):
        """test #6755"""

        m = MetaData(
            naming_convention={"uq": "%(table_name)s_%(column_0_N_name)s_key"}
        )

        uq = UniqueConstraint("some_column_name_thats_really_really_long_too")
        Table(
            "some_table_name_thats_really_really",
            m,
            Column("some_column_name_thats_really_really_long_too", Integer),
            uq,
        )

        i = insert(self.table1).values(dict(name="foo"))

        i = i.on_conflict_do_nothing(constraint=uq)
        self.assert_compile(
            i,
            "INSERT INTO mytable (name) VALUES (%(name)s) ON CONFLICT "
            "ON CONSTRAINT "
            "some_table_name_thats_really_really_some_column_name_th_f7ab "
            "DO NOTHING",
        )

    def test_do_nothing_quoted_named_constraint_target(self):
        """test #6696"""
        i = insert(self.table1).values(dict(name="foo"))
        unique_constr = UniqueConstraint(
            self.table1.c.myid, name="Some Constraint Name"
        )
        i = i.on_conflict_do_nothing(constraint=unique_constr)
        self.assert_compile(
            i,
            "INSERT INTO mytable (name) VALUES "
            '(%(name)s) ON CONFLICT ON CONSTRAINT "Some Constraint Name" '
            "DO NOTHING",
        )

    def test_do_update_index_elements_where_target(self):
        i = insert(self.table1).values(dict(name="foo"))
        i = i.on_conflict_do_update(
            index_elements=self.goofy_index.expressions,
            index_where=self.goofy_index.dialect_options["postgresql"][
                "where"
            ],
            set_=dict(name=i.excluded.name),
        )
        self.assert_compile(
            i,
            "INSERT INTO mytable (name) VALUES "
            "(%(name)s) ON CONFLICT (name) "
            "WHERE name > %(name_1)s "
            "DO UPDATE SET name = excluded.name",
        )

    def test_do_update_index_elements_where_target_multivalues(self):
        i = insert(self.table1).values(
            [dict(name="foo"), dict(name="bar"), dict(name="bat")],
        )
        i = i.on_conflict_do_update(
            index_elements=self.goofy_index.expressions,
            index_where=self.goofy_index.dialect_options["postgresql"][
                "where"
            ],
            set_=dict(name=i.excluded.name),
        )
        self.assert_compile(
            i,
            "INSERT INTO mytable (name) "
            "VALUES (%(name_m0)s), (%(name_m1)s), (%(name_m2)s) "
            "ON CONFLICT (name) "
            "WHERE name > %(name_1)s "
            "DO UPDATE SET name = excluded.name",
            checkparams={
                "name_1": "m",
                "name_m0": "foo",
                "name_m1": "bar",
                "name_m2": "bat",
            },
        )

    def test_do_update_unnamed_index_target(self):
        i = insert(self.table1).values(dict(name="foo"))

        unnamed_goofy = Index(
            None, self.table1.c.name, postgresql_where=self.table1.c.name > "m"
        )

        i = i.on_conflict_do_update(
            constraint=unnamed_goofy, set_=dict(name=i.excluded.name)
        )
        self.assert_compile(
            i,
            "INSERT INTO mytable (name) VALUES "
            "(%(name)s) ON CONFLICT (name) "
            "WHERE name > %(name_1)s "
            "DO UPDATE SET name = excluded.name",
        )

    def test_do_update_unnamed_exclude_constraint_target(self):
        i = insert(self.table1).values(dict(name="foo"))
        i = i.on_conflict_do_update(
            constraint=self.excl_constr_anon, set_=dict(name=i.excluded.name)
        )
        self.assert_compile(
            i,
            "INSERT INTO mytable (name) VALUES "
            "(%(name)s) ON CONFLICT (name, description) "
            "WHERE description != %(description_1)s "
            "DO UPDATE SET name = excluded.name",
        )

    def test_do_update_unnamed_exclude_constraint_string_target(self):
        i = insert(self.table1).values(dict(name="foo"))
        i = i.on_conflict_do_update(
            constraint=self.excl_constr_anon_str,
            set_=dict(name=i.excluded.name),
        )
        self.assert_compile(
            i,
            "INSERT INTO mytable (name) VALUES "
            "(%(name)s) ON CONFLICT (name, description) "
            "WHERE description != 'foo' "
            "DO UPDATE SET name = excluded.name",
        )

    def test_do_update_add_whereclause(self):
        i = insert(self.table1).values(dict(name="foo"))
        i = i.on_conflict_do_update(
            constraint=self.excl_constr_anon,
            set_=dict(name=i.excluded.name),
            where=(
                (self.table1.c.name != "brah")
                & (self.table1.c.description != "brah")
            ),
        )
        self.assert_compile(
            i,
            "INSERT INTO mytable (name) VALUES "
            "(%(name)s) ON CONFLICT (name, description) "
            "WHERE description != %(description_1)s "
            "DO UPDATE SET name = excluded.name "
            "WHERE mytable.name != %(name_1)s "
            "AND mytable.description != %(description_2)s",
        )

    def test_do_update_str_index_where(self):
        i = insert(self.table1).values(dict(name="foo"))
        i = i.on_conflict_do_update(
            constraint=self.excl_constr_anon_str,
            set_=dict(name=i.excluded.name),
            where=(
                (self.table1.c.name != "brah")
                & (self.table1.c.description != "brah")
            ),
        )
        self.assert_compile(
            i,
            "INSERT INTO mytable (name) VALUES "
            "(%(name)s) ON CONFLICT (name, description) "
            "WHERE description != 'foo' "
            "DO UPDATE SET name = excluded.name "
            "WHERE mytable.name != %(name_1)s "
            "AND mytable.description != %(description_1)s",
        )

    def test_do_update_add_whereclause_references_excluded(self):
        i = insert(self.table1).values(dict(name="foo"))
        i = i.on_conflict_do_update(
            constraint=self.excl_constr_anon,
            set_=dict(name=i.excluded.name),
            where=(self.table1.c.name != i.excluded.name),
        )
        self.assert_compile(
            i,
            "INSERT INTO mytable (name) VALUES "
            "(%(name)s) ON CONFLICT (name, description) "
            "WHERE description != %(description_1)s "
            "DO UPDATE SET name = excluded.name "
            "WHERE mytable.name != excluded.name",
        )

    def test_do_update_additional_colnames(self):
        i = insert(self.table1).values(dict(name="bar"))
        i = i.on_conflict_do_update(
            constraint=self.excl_constr_anon,
            set_=dict(name="somename", unknown="unknown"),
        )
        with expect_warnings(
            "Additional column names not matching any "
            "column keys in table 'mytable': 'unknown'"
        ):
            self.assert_compile(
                i,
                "INSERT INTO mytable (name) VALUES "
                "(%(name)s) ON CONFLICT (name, description) "
                "WHERE description != %(description_1)s "
                "DO UPDATE SET name = %(param_1)s, "
                "unknown = %(param_2)s",
                checkparams={
                    "name": "bar",
                    "description_1": "foo",
                    "param_1": "somename",
                    "param_2": "unknown",
                },
            )

    def test_on_conflict_as_cte(self):
        i = insert(self.table1).values(dict(name="foo"))
        i = (
            i.on_conflict_do_update(
                constraint=self.excl_constr_anon,
                set_=dict(name=i.excluded.name),
                where=(self.table1.c.name != i.excluded.name),
            )
            .returning(literal_column("1"))
            .cte("i_upsert")
        )

        stmt = select(i)

        self.assert_compile(
            stmt,
            "WITH i_upsert AS "
            "(INSERT INTO mytable (name) VALUES (%(param_1)s) "
            "ON CONFLICT (name, description) "
            "WHERE description != %(description_1)s "
            "DO UPDATE SET name = excluded.name "
            "WHERE mytable.name != excluded.name RETURNING 1) "
            "SELECT i_upsert.1 "
            "FROM i_upsert",
        )

    def test_combined_with_cte(self):
        t = table("t", column("c1"), column("c2"))

        delete_statement_cte = t.delete().where(t.c.c1 < 1).cte("deletions")

        insert_stmt = insert(t).values([{"c1": 1, "c2": 2}])
        update_stmt = insert_stmt.on_conflict_do_update(
            index_elements=[t.c.c1],
            set_={
                col.name: col
                for col in insert_stmt.excluded
                if col.name in ("c1", "c2")
            },
        ).add_cte(delete_statement_cte)

        self.assert_compile(
            update_stmt,
            "WITH deletions AS (DELETE FROM t WHERE t.c1 < %(c1_1)s) "
            "INSERT INTO t (c1, c2) VALUES (%(c1_m0)s, %(c2_m0)s) "
            "ON CONFLICT (c1) DO UPDATE SET c1 = excluded.c1, "
            "c2 = excluded.c2",
            checkparams={"c1_m0": 1, "c2_m0": 2, "c1_1": 1},
        )

    def test_quote_raw_string_col(self):
        t = table("t", column("FancyName"), column("other name"))

        stmt = (
            insert(t)
            .values(FancyName="something new")
            .on_conflict_do_update(
                index_elements=["FancyName", "other name"],
                set_=OrderedDict(
                    [
                        ("FancyName", "something updated"),
                        ("other name", "something else"),
                    ]
                ),
            )
        )

        self.assert_compile(
            stmt,
            'INSERT INTO t ("FancyName") VALUES (%(FancyName)s) '
            'ON CONFLICT ("FancyName", "other name") '
            'DO UPDATE SET "FancyName" = %(param_1)s, '
            '"other name" = %(param_2)s',
            {
                "param_1": "something updated",
                "param_2": "something else",
                "FancyName": "something new",
            },
        )


class DistinctOnTest(
    fixtures.MappedTest,
    AssertsCompiledSQL,
    fixtures.CacheKeySuite,
    fixtures.DistinctOnFixture,
):
    """Test 'DISTINCT' with SQL expression language and orm.Query with
    an emphasis on PG's 'DISTINCT ON' syntax.

    """

    __dialect__ = postgresql.dialect()

    def setup_test(self):
        self.table = Table(
            "t",
            MetaData(),
            Column("id", Integer, primary_key=True),
            Column("a", String),
            Column("b", String),
        )

    def test_distinct_on_no_cols(self, distinct_on_fixture):
        self.assert_compile(
            distinct_on_fixture(select(self.table)),
            "SELECT DISTINCT t.id, t.a, t.b FROM t",
        )

    def test_distinct_on_cols(self, distinct_on_fixture):
        self.assert_compile(
            distinct_on_fixture(select(self.table), self.table.c.a),
            "SELECT DISTINCT ON (t.a) t.id, t.a, t.b FROM t",
        )

        self.assert_compile(
            distinct_on_fixture(
                self.table.select(), self.table.c.a, self.table.c.b
            ),
            "SELECT DISTINCT ON (t.a, t.b) t.id, t.a, t.b FROM t",
            checkparams={},
        )

    def test_distinct_on_columns_generative_multi_call(
        self, distinct_on_fixture
    ):
        stmt = select(self.table)
        stmt = distinct_on_fixture(stmt, self.table.c.a)
        stmt = distinct_on_fixture(stmt, self.table.c.b)

        self.assert_compile(
            stmt,
            "SELECT DISTINCT ON (t.a, t.b) t.id, t.a, t.b FROM t",
        )

    def test_distinct_on_dupe_columns_generative_multi_call(
        self, distinct_on_fixture
    ):
        stmt = select(self.table)
        stmt = distinct_on_fixture(stmt, self.table.c.a)
        stmt = distinct_on_fixture(stmt, self.table.c.a)

        self.assert_compile(
            stmt,
            "SELECT DISTINCT ON (t.a, t.a) t.id, t.a, t.b FROM t",
        )

    def test_legacy_query_plain(self, distinct_on_fixture):
        sess = Session()
        self.assert_compile(
            distinct_on_fixture(sess.query(self.table)),
            "SELECT DISTINCT t.id AS t_id, t.a AS t_a, t.b AS t_b FROM t",
        )

    def test_legacy_query_on_columns(self, distinct_on_fixture):
        sess = Session()
        self.assert_compile(
            distinct_on_fixture(sess.query(self.table), self.table.c.a),
            "SELECT DISTINCT ON (t.a) t.id AS t_id, t.a AS t_a, "
            "t.b AS t_b FROM t",
        )

    def test_legacy_query_distinct_on_columns_multi_call(
        self, distinct_on_fixture
    ):
        sess = Session()
        self.assert_compile(
            distinct_on_fixture(
                distinct_on_fixture(sess.query(self.table), self.table.c.a),
                self.table.c.b,
            ),
            "SELECT DISTINCT ON (t.a, t.b) t.id AS t_id, t.a AS t_a, "
            "t.b AS t_b FROM t",
        )

    def test_legacy_query_distinct_on_columns_subquery(
        self, distinct_on_fixture
    ):
        sess = Session()

        class Foo:
            pass

        clear_mappers()
        self.mapper_registry.map_imperatively(Foo, self.table)
        sess = Session()
        subq = sess.query(Foo).subquery()

        f1 = aliased(Foo, subq)
        self.assert_compile(
            distinct_on_fixture(sess.query(f1), f1.a, f1.b),
            "SELECT DISTINCT ON (anon_1.a, anon_1.b) anon_1.id "
            "AS anon_1_id, anon_1.a AS anon_1_a, anon_1.b "
            "AS anon_1_b FROM (SELECT t.id AS id, t.a AS a, "
            "t.b AS b FROM t) AS anon_1",
        )

    def test_legacy_query_distinct_on_aliased(self, distinct_on_fixture):
        class Foo:
            pass

        clear_mappers()
        self.mapper_registry.map_imperatively(Foo, self.table)
        a1 = aliased(Foo)
        sess = Session()

        q = distinct_on_fixture(sess.query(a1), a1.a)
        self.assert_compile(
            q,
            "SELECT DISTINCT ON (t_1.a) t_1.id AS t_1_id, "
            "t_1.a AS t_1_a, t_1.b AS t_1_b FROM t AS t_1",
        )

    def test_distinct_on_subquery_anon(self, distinct_on_fixture):
        sq = select(self.table).alias()
        q = distinct_on_fixture(
            select(self.table.c.id, sq.c.id), sq.c.id
        ).where(self.table.c.id == sq.c.id)

        self.assert_compile(
            q,
            "SELECT DISTINCT ON (anon_1.id) t.id, anon_1.id AS id_1 "
            "FROM t, (SELECT t.id AS id, t.a AS a, t.b "
            "AS b FROM t) AS anon_1 WHERE t.id = anon_1.id",
        )

    def test_distinct_on_subquery_named(self, distinct_on_fixture):
        sq = select(self.table).alias("sq")
        q = distinct_on_fixture(
            select(self.table.c.id, sq.c.id), sq.c.id
        ).where(self.table.c.id == sq.c.id)
        self.assert_compile(
            q,
            "SELECT DISTINCT ON (sq.id) t.id, sq.id AS id_1 "
            "FROM t, (SELECT t.id AS id, t.a AS a, "
            "t.b AS b FROM t) AS sq WHERE t.id = sq.id",
        )

    @fixtures.CacheKeySuite.run_suite_tests
    def test_distinct_on_ext_cache_key(self):
        def leg():
            with expect_deprecated("Passing expression"):
                return self.table.select().distinct(self.table.c.a)

        return lambda: [
            self.table.select().ext(distinct_on(self.table.c.a)),
            self.table.select().ext(distinct_on(self.table.c.b)),
            self.table.select().ext(
                distinct_on(self.table.c.a, self.table.c.b)
            ),
            self.table.select().ext(
                distinct_on(self.table.c.b, self.table.c.a)
            ),
            self.table.select(),
            self.table.select().distinct(),
            leg(),
        ]

    def test_distinct_on_cache_key_equal(self, distinct_on_fixture):
        self._run_cache_key_equal_fixture(
            lambda: [
                distinct_on_fixture(self.table.select(), self.table.c.a),
                distinct_on_fixture(select(self.table), self.table.c.a),
            ],
            compare_values=True,
        )
        self._run_cache_key_equal_fixture(
            lambda: [
                distinct_on_fixture(
                    distinct_on_fixture(self.table.select(), self.table.c.a),
                    self.table.c.b,
                ),
                distinct_on_fixture(
                    select(self.table), self.table.c.a, self.table.c.b
                ),
            ],
            compare_values=True,
        )

    def test_distinct_on_literal_binds(self, distinct_on_fixture):
        self.assert_compile(
            distinct_on_fixture(select(self.table), self.table.c.a == 10),
            "SELECT DISTINCT ON (t.a = 10) t.id, t.a, t.b FROM t",
            literal_binds=True,
        )

    def test_distinct_on_col_str(self, distinct_on_fixture):
        stmt = distinct_on_fixture(select(self.table), "a")
        self.assert_compile(
            stmt,
            "SELECT DISTINCT ON (t.a) t.id, t.a, t.b FROM t",
            dialect="postgresql",
        )

    def test_distinct_on_label(self, distinct_on_fixture):
        stmt = distinct_on_fixture(select(self.table.c.a.label("foo")), "foo")
        self.assert_compile(stmt, "SELECT DISTINCT ON (foo) t.a AS foo FROM t")

    def test_unresolvable_distinct_label(self, distinct_on_fixture):
        stmt = distinct_on_fixture(
            select(self.table.c.a.label("foo")), "not a label"
        )
        with expect_raises_message(
            exc.CompileError,
            "Can't resolve label reference for.* expression 'not a"
            " label' should be explicitly",
        ):
            self.assert_compile(stmt, "ingored")

    def test_distinct_on_ext_with_legacy_distinct(self):
        with (
            expect_raises_message(
                exc.InvalidRequestError,
                re.escape(
                    "Cannot mix ``select.ext(distinct_on(...))`` and "
                    "``select.distinct(...)``"
                ),
            ),
            expect_deprecated("Passing expression"),
        ):
            s = (
                self.table.select()
                .distinct(self.table.c.b)
                .ext(distinct_on(self.table.c.a))
            )

        # opposite order is not detected...
        with expect_deprecated("Passing expression"):
            s = (
                self.table.select()
                .ext(distinct_on(self.table.c.a))
                .distinct(self.table.c.b)
            )
        # but it raises while compiling
        with expect_raises_message(
            exc.CompileError,
            re.escape(
                "Cannot mix ``select.ext(distinct_on(...))`` and "
                "``select.distinct(...)``"
            ),
        ):
            self.assert_compile(s, "ignored")


class FullTextSearchTest(fixtures.TestBase, AssertsCompiledSQL):
    """Tests for full text searching"""

    __dialect__ = postgresql.dialect()

    def setup_test(self):
        self.table = Table(
            "t",
            MetaData(),
            Column("id", Integer, primary_key=True),
            Column("title", String),
            Column("body", String),
        )
        self.table_alt = table(
            "mytable",
            column("id", Integer),
            column("title", String(128)),
            column("body", String(128)),
        )
        self.matchtable = Table(
            "matchtable",
            MetaData(),
            Column("id", Integer, primary_key=True),
            Column("title", String(200)),
        )

    def _raise_query(self, q):
        """
        useful for debugging. just do...
        self._raise_query(q)
        """
        c = q.compile(dialect=postgresql.dialect())
        raise ValueError(c)

    def test_match_custom(self):
        s = select(self.table_alt.c.id).where(
            func.to_tsquery("fat").bool_op("<->")(func.to_tsquery("rat"))
        )
        self.assert_compile(
            s,
            "SELECT mytable.id FROM mytable WHERE "
            "to_tsquery(%(to_tsquery_1)s) <-> to_tsquery(%(to_tsquery_2)s)",
            {"to_tsquery_1": "fat", "to_tsquery_2": "rat"},
        )

    def test_match_custom_regconfig(self):
        s = select(self.table_alt.c.id).where(
            func.to_tsquery("english", "fat").bool_op("<->")(
                func.to_tsquery("english", "rat")
            )
        )
        self.assert_compile(
            s,
            "SELECT mytable.id FROM mytable WHERE "
            "to_tsquery(%(to_tsquery_1)s, %(to_tsquery_2)s) <-> "
            "to_tsquery(%(to_tsquery_3)s, %(to_tsquery_4)s)",
            {
                "to_tsquery_1": "english",
                "to_tsquery_2": "fat",
                "to_tsquery_3": "english",
                "to_tsquery_4": "rat",
            },
        )

    def test_match_basic(self):
        s = select(self.table_alt.c.id).where(
            self.table_alt.c.title.match("somestring")
        )
        self.assert_compile(
            s,
            "SELECT mytable.id "
            "FROM mytable "
            "WHERE mytable.title @@ plainto_tsquery(%(title_1)s)",
        )

    def test_match_regconfig(self):
        s = select(self.table_alt.c.id).where(
            self.table_alt.c.title.match(
                "somestring", postgresql_regconfig="english"
            )
        )
        self.assert_compile(
            s,
            "SELECT mytable.id "
            "FROM mytable "
            "WHERE mytable.title @@ "
            "plainto_tsquery('english', %(title_1)s)",
        )

    def test_match_tsvector(self):
        s = select(self.table_alt.c.id).where(
            func.to_tsvector(self.table_alt.c.title).match("somestring")
        )
        self.assert_compile(
            s,
            "SELECT mytable.id "
            "FROM mytable "
            "WHERE to_tsvector(mytable.title) "
            "@@ plainto_tsquery(%(to_tsvector_1)s)",
        )

    def test_match_tsvectorconfig(self):
        s = select(self.table_alt.c.id).where(
            func.to_tsvector("english", self.table_alt.c.title).match(
                "somestring"
            )
        )
        self.assert_compile(
            s,
            "SELECT mytable.id "
            "FROM mytable "
            "WHERE to_tsvector(%(to_tsvector_1)s, mytable.title) @@ "
            "plainto_tsquery(%(to_tsvector_2)s)",
        )

    def test_match_tsvectorconfig_regconfig(self):
        s = select(self.table_alt.c.id).where(
            func.to_tsvector("english", self.table_alt.c.title).match(
                "somestring", postgresql_regconfig="english"
            )
        )
        self.assert_compile(
            s,
            "SELECT mytable.id "
            "FROM mytable "
            "WHERE to_tsvector(%(to_tsvector_1)s, mytable.title) @@ "
            """plainto_tsquery('english', %(to_tsvector_2)s)""",
        )

    @testing.combinations(
        ("to_tsvector",),
        ("to_tsquery",),
        ("plainto_tsquery",),
        ("phraseto_tsquery",),
        ("websearch_to_tsquery",),
        ("ts_headline",),
        argnames="to_ts_name",
    )
    def test_dont_compile_non_imported(self, to_ts_name):
        new_func = type(
            to_ts_name,
            (GenericFunction,),
            {
                "_register": False,
                "inherit_cache": True,
            },
        )

        with expect_raises_message(
            exc.CompileError,
            rf"Can't compile \"{to_ts_name}\(\)\" full text search "
            f"function construct that does not originate from the "
            f'"sqlalchemy.dialects.postgresql" package.  '
            f'Please ensure "import sqlalchemy.dialects.postgresql" is '
            f"called before constructing "
            rf"\"sqlalchemy.func.{to_ts_name}\(\)\" to ensure "
            f"registration of the correct "
            f"argument and return types.",
        ):
            select(new_func("x", "y")).compile(dialect=postgresql.dialect())

    @testing.combinations(
        (func.to_tsvector,),
        (func.to_tsquery,),
        (func.plainto_tsquery,),
        (func.phraseto_tsquery,),
        (func.websearch_to_tsquery,),
        argnames="to_ts_func",
    )
    @testing.variation("use_regconfig", [True, False, "literal"])
    def test_to_regconfig_fns(self, to_ts_func, use_regconfig):
        """test #8977"""
        matchtable = self.matchtable

        fn_name = to_ts_func().name

        if use_regconfig.literal:
            regconfig = literal("english", REGCONFIG)
        elif use_regconfig:
            regconfig = "english"
        else:
            regconfig = None

        if regconfig is None:
            if fn_name == "to_tsvector":
                fn = to_ts_func(matchtable.c.title).match("python")
                expected = (
                    "to_tsvector(matchtable.title) @@ "
                    "plainto_tsquery($1::VARCHAR)"
                )
            else:
                fn = func.to_tsvector(matchtable.c.title).op("@@")(
                    to_ts_func("python")
                )
                expected = (
                    f"to_tsvector(matchtable.title) @@ {fn_name}($1::VARCHAR)"
                )
        else:
            if fn_name == "to_tsvector":
                fn = to_ts_func(regconfig, matchtable.c.title).match("python")
                expected = (
                    "to_tsvector($1::REGCONFIG, matchtable.title) @@ "
                    "plainto_tsquery($2::VARCHAR)"
                )
            else:
                fn = func.to_tsvector(matchtable.c.title).op("@@")(
                    to_ts_func(regconfig, "python")
                )
                expected = (
                    f"to_tsvector(matchtable.title) @@ "
                    f"{fn_name}($1::REGCONFIG, $2::VARCHAR)"
                )

        stmt = matchtable.select().where(fn)

        self.assert_compile(
            stmt,
            "SELECT matchtable.id, matchtable.title "
            f"FROM matchtable WHERE {expected}",
            dialect="postgresql+asyncpg",
        )

    @testing.variation("use_regconfig", [True, False, "literal"])
    @testing.variation("include_options", [True, False])
    @testing.variation("tsquery_in_expr", [True, False])
    def test_ts_headline(
        self, connection, use_regconfig, include_options, tsquery_in_expr
    ):
        """test #8977"""
        if use_regconfig.literal:
            regconfig = literal("english", REGCONFIG)
        elif use_regconfig:
            regconfig = "english"
        else:
            regconfig = None

        text = (
            "The most common type of search is to find all documents "
            "containing given query terms and return them in order of "
            "their similarity to the query."
        )
        tsquery = func.to_tsquery("english", "query & similarity")

        if regconfig is None:
            tsquery_str = "to_tsquery($2::REGCONFIG, $3::VARCHAR)"
        else:
            tsquery_str = "to_tsquery($3::REGCONFIG, $4::VARCHAR)"

        if tsquery_in_expr:
            tsquery = case((true(), tsquery), else_=null())
            tsquery_str = f"CASE WHEN true THEN {tsquery_str} ELSE NULL END"

        is_(tsquery.type._type_affinity, TSQUERY)

        args = [text, tsquery]
        if regconfig is not None:
            args.insert(0, regconfig)
        if include_options:
            args.append(
                "MaxFragments=10, MaxWords=7, "
                "MinWords=3, StartSel=<<, StopSel=>>"
            )

        fn = func.ts_headline(*args)
        stmt = select(fn)

        if regconfig is None and not include_options:
            self.assert_compile(
                stmt,
                f"SELECT ts_headline($1::VARCHAR, "
                f"{tsquery_str}) AS ts_headline_1",
                dialect="postgresql+asyncpg",
            )
        elif regconfig is None and include_options:
            self.assert_compile(
                stmt,
                f"SELECT ts_headline($1::VARCHAR, "
                f"{tsquery_str}, $4::VARCHAR) AS ts_headline_1",
                dialect="postgresql+asyncpg",
            )
        elif regconfig is not None and not include_options:
            self.assert_compile(
                stmt,
                f"SELECT ts_headline($1::REGCONFIG, $2::VARCHAR, "
                f"{tsquery_str}) AS ts_headline_1",
                dialect="postgresql+asyncpg",
            )
        else:
            self.assert_compile(
                stmt,
                f"SELECT ts_headline($1::REGCONFIG, $2::VARCHAR, "
                f"{tsquery_str}, $5::VARCHAR) "
                "AS ts_headline_1",
                dialect="postgresql+asyncpg",
            )


class RegexpTest(fixtures.TestBase, testing.AssertsCompiledSQL):
    __dialect__ = "postgresql"

    def setup_test(self):
        self.table = table(
            "mytable", column("myid", String), column("name", String)
        )

    def test_regexp_match(self):
        self.assert_compile(
            self.table.c.myid.regexp_match("pattern"),
            "mytable.myid ~ %(myid_1)s",
            checkparams={"myid_1": "pattern"},
        )

    def test_regexp_match_column(self):
        self.assert_compile(
            self.table.c.myid.regexp_match(self.table.c.name),
            "mytable.myid ~ mytable.name",
            checkparams={},
        )

    def test_regexp_match_str(self):
        self.assert_compile(
            literal("string").regexp_match(self.table.c.name),
            "%(param_1)s ~ mytable.name",
            checkparams={"param_1": "string"},
        )

    def test_regexp_match_flags(self):
        self.assert_compile(
            self.table.c.myid.regexp_match("pattern", flags="ig"),
            "mytable.myid ~ CONCAT('(?', 'ig', ')', %(myid_1)s)",
            checkparams={"myid_1": "pattern"},
        )

    def test_regexp_match_flags_ignorecase(self):
        self.assert_compile(
            self.table.c.myid.regexp_match("pattern", flags="i"),
            "mytable.myid ~* %(myid_1)s",
            checkparams={"myid_1": "pattern"},
        )

    def test_not_regexp_match(self):
        self.assert_compile(
            ~self.table.c.myid.regexp_match("pattern"),
            "mytable.myid !~ %(myid_1)s",
            checkparams={"myid_1": "pattern"},
        )

    def test_not_regexp_match_column(self):
        self.assert_compile(
            ~self.table.c.myid.regexp_match(self.table.c.name),
            "mytable.myid !~ mytable.name",
            checkparams={},
        )

    def test_not_regexp_match_str(self):
        self.assert_compile(
            ~literal("string").regexp_match(self.table.c.name),
            "%(param_1)s !~ mytable.name",
            checkparams={"param_1": "string"},
        )

    def test_not_regexp_match_flags(self):
        self.assert_compile(
            ~self.table.c.myid.regexp_match("pattern", flags="ig"),
            "mytable.myid !~ CONCAT('(?', 'ig', ')', %(myid_1)s)",
            checkparams={"myid_1": "pattern"},
        )

    def test_not_regexp_match_flags_ignorecase(self):
        self.assert_compile(
            ~self.table.c.myid.regexp_match("pattern", flags="i"),
            "mytable.myid !~* %(myid_1)s",
            checkparams={"myid_1": "pattern"},
        )

    def test_regexp_replace(self):
        self.assert_compile(
            self.table.c.myid.regexp_replace("pattern", "replacement"),
            "REGEXP_REPLACE(mytable.myid, %(myid_1)s, %(myid_2)s)",
            checkparams={"myid_1": "pattern", "myid_2": "replacement"},
        )

    def test_regexp_replace_column(self):
        self.assert_compile(
            self.table.c.myid.regexp_replace("pattern", self.table.c.name),
            "REGEXP_REPLACE(mytable.myid, %(myid_1)s, mytable.name)",
            checkparams={"myid_1": "pattern"},
        )

    def test_regexp_replace_column2(self):
        self.assert_compile(
            self.table.c.myid.regexp_replace(self.table.c.name, "replacement"),
            "REGEXP_REPLACE(mytable.myid, mytable.name, %(myid_1)s)",
            checkparams={"myid_1": "replacement"},
        )

    def test_regexp_replace_string(self):
        self.assert_compile(
            literal("string").regexp_replace("pattern", self.table.c.name),
            "REGEXP_REPLACE(%(param_1)s, %(param_2)s, mytable.name)",
            checkparams={"param_2": "pattern", "param_1": "string"},
        )

    def test_regexp_replace_flags(self):
        self.assert_compile(
            self.table.c.myid.regexp_replace(
                "pattern", "replacement", flags="ig"
            ),
            "REGEXP_REPLACE(mytable.myid, %(myid_1)s, %(myid_2)s, 'ig')",
            checkparams={
                "myid_1": "pattern",
                "myid_2": "replacement",
            },
        )

    def test_regexp_replace_flags_safestring(self):
        self.assert_compile(
            self.table.c.myid.regexp_replace(
                "pattern", "replacement", flags="i'g"
            ),
            "REGEXP_REPLACE(mytable.myid, %(myid_1)s, %(myid_2)s, 'i''g')",
            checkparams={
                "myid_1": "pattern",
                "myid_2": "replacement",
            },
        )

    @testing.combinations(
        (
            5,
            10,
            {},
            "OFFSET (%(param_1)s) ROWS FETCH FIRST (%(param_2)s) ROWS ONLY",
            {"param_1": 10, "param_2": 5},
        ),
        (None, 10, {}, "LIMIT ALL OFFSET %(param_1)s", {"param_1": 10}),
        (
            5,
            None,
            {},
            "FETCH FIRST (%(param_1)s) ROWS ONLY",
            {"param_1": 5},
        ),
        (
            0,
            0,
            {},
            "OFFSET (%(param_1)s) ROWS FETCH FIRST (%(param_2)s) ROWS ONLY",
            {"param_1": 0, "param_2": 0},
        ),
        (
            5,
            10,
            {"percent": True},
            "OFFSET (%(param_1)s) ROWS FETCH FIRST "
            "(%(param_2)s) PERCENT ROWS ONLY",
            {"param_1": 10, "param_2": 5},
        ),
        (
            5,
            10,
            {"percent": True, "with_ties": True},
            "OFFSET (%(param_1)s) ROWS FETCH FIRST (%(param_2)s)"
            " PERCENT ROWS WITH TIES",
            {"param_1": 10, "param_2": 5},
        ),
        (
            5,
            10,
            {"with_ties": True},
            "OFFSET (%(param_1)s) ROWS FETCH FIRST "
            "(%(param_2)s) ROWS WITH TIES",
            {"param_1": 10, "param_2": 5},
        ),
        (
            literal_column("Q"),
            literal_column("Y"),
            {},
            "OFFSET (Y) ROWS FETCH FIRST (Q) ROWS ONLY",
            {},
        ),
        (
            column("Q"),
            column("Y"),
            {},
            'OFFSET ("Y") ROWS FETCH FIRST ("Q") ROWS ONLY',
            {},
        ),
        (
            bindparam("Q", 3),
            bindparam("Y", 7),
            {},
            "OFFSET (%(Y)s) ROWS FETCH FIRST (%(Q)s) ROWS ONLY",
            {"Q": 3, "Y": 7},
        ),
        (
            literal_column("Q") + literal_column("Z"),
            literal_column("Y") + literal_column("W"),
            {},
            "OFFSET (Y + W) ROWS FETCH FIRST (Q + Z) ROWS ONLY",
            {},
        ),
    )
    def test_fetch(self, fetch, offset, fetch_kw, exp, params):
        self.assert_compile(
            select(1).fetch(fetch, **fetch_kw).offset(offset),
            "SELECT 1 " + exp,
            checkparams=params,
        )


class CacheKeyTest(fixtures.CacheKeyFixture, fixtures.TestBase):
    def test_aggregate_order_by(self):
        """test #8574"""

        self._run_cache_key_fixture(
            lambda: (
                aggregate_order_by(column("a"), column("a")),
                aggregate_order_by(column("a"), column("b")),
                aggregate_order_by(column("a"), column("a").desc()),
                aggregate_order_by(column("a"), column("a").nulls_first()),
                aggregate_order_by(
                    column("a"), column("a").desc().nulls_first()
                ),
                aggregate_order_by(column("a", Integer), column("b")),
                aggregate_order_by(column("a"), column("b"), column("c")),
                aggregate_order_by(column("a"), column("c"), column("b")),
                aggregate_order_by(
                    column("a"), column("b").desc(), column("c")
                ),
                aggregate_order_by(
                    column("a"), column("b").nulls_first(), column("c")
                ),
                aggregate_order_by(
                    column("a"), column("b").desc().nulls_first(), column("c")
                ),
                aggregate_order_by(
                    column("a", Integer), column("a"), column("b")
                ),
            ),
            compare_values=False,
        )

    def test_array_equivalent_keys_one_element(self):
        self._run_cache_key_equal_fixture(
            lambda: (
                array([random.randint(0, 10)]),
                array([random.randint(0, 10)], type_=Integer),
                array([random.randint(0, 10)], type_=Integer),
            ),
            compare_values=False,
        )

    def test_array_equivalent_keys_two_elements(self):
        self._run_cache_key_equal_fixture(
            lambda: (
                array([random.randint(0, 10), random.randint(0, 10)]),
                array(
                    [random.randint(0, 10), random.randint(0, 10)],
                    type_=Integer,
                ),
                array(
                    [random.randint(0, 10), random.randint(0, 10)],
                    type_=Integer,
                ),
            ),
            compare_values=False,
        )

    def test_array_heterogeneous(self):
        self._run_cache_key_fixture(
            lambda: (
                array([], type_=Integer),
                array([], type_=Text),
                array([]),
                array([random.choice(["t1", "t2", "t3"])]),
                array(
                    [
                        random.choice(["t1", "t2", "t3"]),
                        random.choice(["t1", "t2", "t3"]),
                    ]
                ),
                array([random.choice(["t1", "t2", "t3"])], type_=Text),
                array([random.choice(["t1", "t2", "t3"])], type_=VARCHAR(30)),
                array([random.randint(0, 10), random.randint(0, 10)]),
            ),
            compare_values=False,
        )
