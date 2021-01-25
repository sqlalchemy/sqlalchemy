# coding: utf-8

from sqlalchemy import and_
from sqlalchemy import BigInteger
from sqlalchemy import bindparam
from sqlalchemy import cast
from sqlalchemy import Column
from sqlalchemy import Computed
from sqlalchemy import Date
from sqlalchemy import delete
from sqlalchemy import Enum
from sqlalchemy import exc
from sqlalchemy import func
from sqlalchemy import Identity
from sqlalchemy import Index
from sqlalchemy import Integer
from sqlalchemy import literal
from sqlalchemy import MetaData
from sqlalchemy import null
from sqlalchemy import schema
from sqlalchemy import select
from sqlalchemy import Sequence
from sqlalchemy import SmallInteger
from sqlalchemy import String
from sqlalchemy import Table
from sqlalchemy import testing
from sqlalchemy import Text
from sqlalchemy import text
from sqlalchemy import types as sqltypes
from sqlalchemy import update
from sqlalchemy.dialects import postgresql
from sqlalchemy.dialects.postgresql import aggregate_order_by
from sqlalchemy.dialects.postgresql import ARRAY as PG_ARRAY
from sqlalchemy.dialects.postgresql import array
from sqlalchemy.dialects.postgresql import array_agg as pg_array_agg
from sqlalchemy.dialects.postgresql import ExcludeConstraint
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.dialects.postgresql import TSRANGE
from sqlalchemy.orm import aliased
from sqlalchemy.orm import mapper
from sqlalchemy.orm import Session
from sqlalchemy.sql import column
from sqlalchemy.sql import literal_column
from sqlalchemy.sql import operators
from sqlalchemy.sql import table
from sqlalchemy.sql import util as sql_util
from sqlalchemy.testing import engines
from sqlalchemy.testing import eq_
from sqlalchemy.testing import fixtures
from sqlalchemy.testing.assertions import assert_raises
from sqlalchemy.testing.assertions import assert_raises_message
from sqlalchemy.testing.assertions import AssertsCompiledSQL
from sqlalchemy.testing.assertions import expect_warnings
from sqlalchemy.testing.assertions import is_
from sqlalchemy.util import OrderedDict
from sqlalchemy.util import u


class SequenceTest(fixtures.TestBase, AssertsCompiledSQL):
    __prefer__ = "postgresql"

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

    @testing.only_on("postgresql", "foo")
    @testing.provide_metadata
    def test_reverse_eng_name(self):
        metadata = self.metadata
        engine = engines.testing_engine(options=dict(implicit_returning=False))
        for tname, cname in [
            ("tb1" * 30, "abc"),
            ("tb2", "abc" * 30),
            ("tb3" * 30, "abc" * 30),
            ("tb4", "abc"),
        ]:
            t = Table(
                tname[:57],
                metadata,
                Column(cname[:57], Integer, primary_key=True),
            )
            t.create(engine)
            with engine.begin() as conn:
                r = conn.execute(t.insert())
                eq_(r.inserted_primary_key, (1,))

    @testing.combinations(
        (None, ""),
        (Integer, "AS INTEGER "),
        (SmallInteger, "AS SMALLINT "),
        (BigInteger, "AS BIGINT "),
    )
    def test_create_index_concurrently(self, type_, text):
        s = Sequence("s1", data_type=type_)
        self.assert_compile(
            schema.CreateSequence(s),
            "CREATE SEQUENCE s1 %sSTART WITH 1" % text,
            dialect=postgresql.dialect(),
        )


class CompileTest(fixtures.TestBase, AssertsCompiledSQL):

    __dialect__ = postgresql.dialect()

    def test_update_returning(self):
        dialect = postgresql.dialect()
        table1 = table(
            "mytable",
            column("myid", Integer),
            column("name", String(128)),
            column("description", String(128)),
        )
        u = update(table1, values=dict(name="foo")).returning(
            table1.c.myid, table1.c.name
        )
        self.assert_compile(
            u,
            "UPDATE mytable SET name=%(name)s "
            "RETURNING mytable.myid, mytable.name",
            dialect=dialect,
        )
        u = update(table1, values=dict(name="foo")).returning(table1)
        self.assert_compile(
            u,
            "UPDATE mytable SET name=%(name)s "
            "RETURNING mytable.myid, mytable.name, "
            "mytable.description",
            dialect=dialect,
        )
        u = update(table1, values=dict(name="foo")).returning(
            func.length(table1.c.name)
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

        i = insert(table1, values=dict(name="foo")).returning(
            table1.c.myid, table1.c.name
        )
        self.assert_compile(
            i,
            "INSERT INTO mytable (name) VALUES "
            "(%(name)s) RETURNING mytable.myid, "
            "mytable.name",
            dialect=dialect,
        )
        i = insert(table1, values=dict(name="foo")).returning(table1)
        self.assert_compile(
            i,
            "INSERT INTO mytable (name) VALUES "
            "(%(name)s) RETURNING mytable.myid, "
            "mytable.name, mytable.description",
            dialect=dialect,
        )
        i = insert(table1, values=dict(name="foo")).returning(
            func.length(table1.c.name)
        )
        self.assert_compile(
            i,
            "INSERT INTO mytable (name) VALUES "
            "(%(name)s) RETURNING length(mytable.name) "
            "AS length_1",
            dialect=dialect,
        )

    def test_create_drop_enum(self):
        # test escaping and unicode within CREATE TYPE for ENUM
        typ = postgresql.ENUM(
            "val1", "val2", "val's 3", u("méil"), name="myname"
        )
        self.assert_compile(
            postgresql.CreateEnumType(typ),
            u(
                "CREATE TYPE myname AS "
                "ENUM ('val1', 'val2', 'val''s 3', 'méil')"
            ),
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
            "CREATE TYPE someschema.somename AS ENUM " "('x', 'y', 'z')",
        )
        self.assert_compile(postgresql.DropEnumType(e1), "DROP TYPE somename")
        self.assert_compile(
            postgresql.DropEnumType(e2), "DROP TYPE someschema.somename"
        )
        t1 = Table("sometable", MetaData(), Column("somecolumn", e1))
        self.assert_compile(
            schema.CreateTable(t1),
            "CREATE TABLE sometable (somecolumn " "somename)",
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

    def test_create_type_schema_translate(self):
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

    def test_create_table_with_multiple_options(self):
        m = MetaData()
        tbl = Table(
            "atable",
            m,
            Column("id", Integer),
            postgresql_tablespace="sometablespace",
            postgresql_with_oids=False,
            postgresql_on_commit="preserve_rows",
        )
        self.assert_compile(
            schema.CreateTable(tbl),
            "CREATE TABLE atable (id INTEGER) WITHOUT OIDS "
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
            "CREATE INDEX test_idx1 ON testtbl " "(data text_pattern_ops)",
            dialect=postgresql.dialect(),
        )
        self.assert_compile(
            schema.CreateIndex(idx2),
            "CREATE INDEX test_idx2 ON testtbl "
            "(data text_pattern_ops, data2 int4_ops)",
            dialect=postgresql.dialect(),
        )

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
            "CREATE INDEX test_idx1 ON testtbl " "(data)",
            dialect=postgresql.dialect(),
        )
        self.assert_compile(
            schema.CreateIndex(idx2),
            "CREATE INDEX test_idx2 ON testtbl " "USING btree (data)",
            dialect=postgresql.dialect(),
        )
        self.assert_compile(
            schema.CreateIndex(idx3),
            "CREATE INDEX test_idx3 ON testtbl " "USING hash (data)",
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
            "CREATE INDEX test_idx1 ON testtbl " "(data)",
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
            "CREATE INDEX test_idx1 ON testtbl " "USING gist (data)",
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
            "CREATE INDEX test_idx1 ON testtbl " "(data)",
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

        idx1 = Index("test_idx1", 5 / (tbl.c.x + tbl.c.y))
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

    def test_exclude_constraint_min(self):
        m = MetaData()
        tbl = Table("testtbl", m, Column("room", Integer, primary_key=True))
        cons = ExcludeConstraint(("room", "="))
        tbl.append_constraint(cons)
        self.assert_compile(
            schema.AddConstraint(cons),
            "ALTER TABLE testtbl ADD EXCLUDE USING gist " "(room WITH =)",
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
        cons = ExcludeConstraint(
            (
                literal_column(
                    "daterange(valid_from_date, valid_thru_date, '[]')"
                ),
                "&&",
            ),
            where=column("valid_from_date") <= column("valid_thru_date"),
            name="ex_mytable_valid_date_range",
            deferrable=deferrable_value,
            initially=initially_value,
        )

        table.append_constraint(cons)
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
        cons_copy = cons.copy()
        tbl.append_constraint(cons_copy)
        self.assert_compile(
            schema.AddConstraint(cons_copy),
            "ALTER TABLE testtbl ADD EXCLUDE USING gist " "(room WITH =)",
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
            table1.select(table1.c.myid == 7).with_for_update(),
            "SELECT mytable.myid, mytable.name, mytable.description "
            "FROM mytable WHERE mytable.myid = %(myid_1)s FOR UPDATE",
        )

        self.assert_compile(
            table1.select(table1.c.myid == 7).with_for_update(nowait=True),
            "SELECT mytable.myid, mytable.name, mytable.description "
            "FROM mytable WHERE mytable.myid = %(myid_1)s FOR UPDATE NOWAIT",
        )

        self.assert_compile(
            table1.select(table1.c.myid == 7).with_for_update(
                skip_locked=True
            ),
            "SELECT mytable.myid, mytable.name, mytable.description "
            "FROM mytable WHERE mytable.myid = %(myid_1)s "
            "FOR UPDATE SKIP LOCKED",
        )

        self.assert_compile(
            table1.select(table1.c.myid == 7).with_for_update(read=True),
            "SELECT mytable.myid, mytable.name, mytable.description "
            "FROM mytable WHERE mytable.myid = %(myid_1)s FOR SHARE",
        )

        self.assert_compile(
            table1.select(table1.c.myid == 7).with_for_update(
                read=True, nowait=True
            ),
            "SELECT mytable.myid, mytable.name, mytable.description "
            "FROM mytable WHERE mytable.myid = %(myid_1)s FOR SHARE NOWAIT",
        )

        self.assert_compile(
            table1.select(table1.c.myid == 7).with_for_update(
                key_share=True, nowait=True
            ),
            "SELECT mytable.myid, mytable.name, mytable.description "
            "FROM mytable WHERE mytable.myid = %(myid_1)s "
            "FOR NO KEY UPDATE NOWAIT",
        )

        self.assert_compile(
            table1.select(table1.c.myid == 7).with_for_update(
                key_share=True, read=True, nowait=True
            ),
            "SELECT mytable.myid, mytable.name, mytable.description "
            "FROM mytable WHERE mytable.myid = %(myid_1)s "
            "FOR KEY SHARE NOWAIT",
        )

        self.assert_compile(
            table1.select(table1.c.myid == 7).with_for_update(
                read=True, skip_locked=True
            ),
            "SELECT mytable.myid, mytable.name, mytable.description "
            "FROM mytable WHERE mytable.myid = %(myid_1)s "
            "FOR SHARE SKIP LOCKED",
        )

        self.assert_compile(
            table1.select(table1.c.myid == 7).with_for_update(
                of=table1.c.myid
            ),
            "SELECT mytable.myid, mytable.name, mytable.description "
            "FROM mytable WHERE mytable.myid = %(myid_1)s "
            "FOR UPDATE OF mytable",
        )

        self.assert_compile(
            table1.select(table1.c.myid == 7).with_for_update(
                read=True, nowait=True, of=table1
            ),
            "SELECT mytable.myid, mytable.name, mytable.description "
            "FROM mytable WHERE mytable.myid = %(myid_1)s "
            "FOR SHARE OF mytable NOWAIT",
        )

        self.assert_compile(
            table1.select(table1.c.myid == 7).with_for_update(
                key_share=True, read=True, nowait=True, of=table1
            ),
            "SELECT mytable.myid, mytable.name, mytable.description "
            "FROM mytable WHERE mytable.myid = %(myid_1)s "
            "FOR KEY SHARE OF mytable NOWAIT",
        )

        self.assert_compile(
            table1.select(table1.c.myid == 7).with_for_update(
                read=True, nowait=True, of=table1.c.myid
            ),
            "SELECT mytable.myid, mytable.name, mytable.description "
            "FROM mytable WHERE mytable.myid = %(myid_1)s "
            "FOR SHARE OF mytable NOWAIT",
        )

        self.assert_compile(
            table1.select(table1.c.myid == 7).with_for_update(
                read=True, nowait=True, of=[table1.c.myid, table1.c.name]
            ),
            "SELECT mytable.myid, mytable.name, mytable.description "
            "FROM mytable WHERE mytable.myid = %(myid_1)s "
            "FOR SHARE OF mytable NOWAIT",
        )

        self.assert_compile(
            table1.select(table1.c.myid == 7).with_for_update(
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
            table1.select(table1.c.myid == 7).with_for_update(
                skip_locked=True, of=[table1.c.myid, table1.c.name]
            ),
            "SELECT mytable.myid, mytable.name, mytable.description "
            "FROM mytable WHERE mytable.myid = %(myid_1)s "
            "FOR UPDATE OF mytable SKIP LOCKED",
        )

        self.assert_compile(
            table1.select(table1.c.myid == 7).with_for_update(
                read=True, skip_locked=True, of=[table1.c.myid, table1.c.name]
            ),
            "SELECT mytable.myid, mytable.name, mytable.description "
            "FROM mytable WHERE mytable.myid = %(myid_1)s "
            "FOR SHARE OF mytable SKIP LOCKED",
        )

        self.assert_compile(
            table1.select(table1.c.myid == 7).with_for_update(
                key_share=True, nowait=True, of=[table1.c.myid, table1.c.name]
            ),
            "SELECT mytable.myid, mytable.name, mytable.description "
            "FROM mytable WHERE mytable.myid = %(myid_1)s "
            "FOR NO KEY UPDATE OF mytable NOWAIT",
        )

        self.assert_compile(
            table1.select(table1.c.myid == 7).with_for_update(
                key_share=True,
                skip_locked=True,
                of=[table1.c.myid, table1.c.name],
            ),
            "SELECT mytable.myid, mytable.name, mytable.description "
            "FROM mytable WHERE mytable.myid = %(myid_1)s "
            "FOR NO KEY UPDATE OF mytable SKIP LOCKED",
        )

        self.assert_compile(
            table1.select(table1.c.myid == 7).with_for_update(
                key_share=True, of=[table1.c.myid, table1.c.name]
            ),
            "SELECT mytable.myid, mytable.name, mytable.description "
            "FROM mytable WHERE mytable.myid = %(myid_1)s "
            "FOR NO KEY UPDATE OF mytable",
        )

        self.assert_compile(
            table1.select(table1.c.myid == 7).with_for_update(key_share=True),
            "SELECT mytable.myid, mytable.name, mytable.description "
            "FROM mytable WHERE mytable.myid = %(myid_1)s "
            "FOR NO KEY UPDATE",
        )

        self.assert_compile(
            table1.select(table1.c.myid == 7).with_for_update(
                read=True, key_share=True
            ),
            "SELECT mytable.myid, mytable.name, mytable.description "
            "FROM mytable WHERE mytable.myid = %(myid_1)s "
            "FOR KEY SHARE",
        )

        self.assert_compile(
            table1.select(table1.c.myid == 7).with_for_update(
                read=True, key_share=True, of=table1
            ),
            "SELECT mytable.myid, mytable.name, mytable.description "
            "FROM mytable WHERE mytable.myid = %(myid_1)s "
            "FOR KEY SHARE OF mytable",
        )

        self.assert_compile(
            table1.select(table1.c.myid == 7).with_for_update(
                read=True, of=table1
            ),
            "SELECT mytable.myid, mytable.name, mytable.description "
            "FROM mytable WHERE mytable.myid = %(myid_1)s "
            "FOR SHARE OF mytable",
        )

        self.assert_compile(
            table1.select(table1.c.myid == 7).with_for_update(
                read=True, key_share=True, skip_locked=True
            ),
            "SELECT mytable.myid, mytable.name, mytable.description "
            "FROM mytable WHERE mytable.myid = %(myid_1)s "
            "FOR KEY SHARE SKIP LOCKED",
        )

        self.assert_compile(
            table1.select(table1.c.myid == 7).with_for_update(
                key_share=True, skip_locked=True
            ),
            "SELECT mytable.myid, mytable.name, mytable.description "
            "FROM mytable WHERE mytable.myid = %(myid_1)s "
            "FOR NO KEY UPDATE SKIP LOCKED",
        )

        ta = table1.alias()
        self.assert_compile(
            ta.select(ta.c.myid == 7).with_for_update(
                of=[ta.c.myid, ta.c.name]
            ),
            "SELECT mytable_1.myid, mytable_1.name, mytable_1.description "
            "FROM mytable AS mytable_1 "
            "WHERE mytable_1.myid = %(myid_1)s FOR UPDATE OF mytable_1",
        )

        table2 = table("table2", column("mytable_id"))
        join = table2.join(table1, table2.c.mytable_id == table1.c.myid)
        self.assert_compile(
            join.select(table2.c.mytable_id == 7).with_for_update(of=[join]),
            "SELECT table2.mytable_id, "
            "mytable.myid, mytable.name, mytable.description "
            "FROM table2 "
            "JOIN mytable ON table2.mytable_id = mytable.myid "
            "WHERE table2.mytable_id = %(mytable_id_1)s "
            "FOR UPDATE OF mytable, table2",
        )

        join = table2.join(ta, table2.c.mytable_id == ta.c.myid)
        self.assert_compile(
            join.select(table2.c.mytable_id == 7).with_for_update(of=[join]),
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
            table1.select(table1.c.myid == 7).with_for_update(
                of=text("table1")
            ),
            "SELECT mytable.myid, mytable.name, mytable.description "
            "FROM mytable WHERE mytable.myid = %(myid_1)s "
            "FOR UPDATE OF table1",
        )

        # ensure literal_column of works
        self.assert_compile(
            table1.select(table1.c.myid == 7).with_for_update(
                of=literal_column("table1")
            ),
            "SELECT mytable.myid, mytable.name, mytable.description "
            "FROM mytable WHERE mytable.myid = %(myid_1)s "
            "FOR UPDATE OF table1",
        )

    def test_for_update_with_schema(self):
        m = MetaData()
        table1 = Table(
            "mytable", m, Column("myid"), Column("name"), schema="testschema"
        )

        self.assert_compile(
            table1.select(table1.c.myid == 7).with_for_update(of=table1),
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
            c.contains([1]), "x @> %(x_1)s", checkparams={"x_1": [1]}
        )
        self.assert_compile(
            c.contained_by([2]), "x <@ %(x_1)s", checkparams={"x_1": [2]}
        )
        self.assert_compile(
            c.overlap([3]), "x && %(x_1)s", checkparams={"x_1": [3]}
        )
        self.assert_compile(
            postgresql.Any(4, c),
            "%(param_1)s = ANY (x)",
            checkparams={"param_1": 4},
        )

        self.assert_compile(
            c.any(5),
            "%(param_1)s = ANY (x)",
            checkparams={"param_1": 5},
        )

        self.assert_compile(
            ~c.any(5),
            "NOT (%(param_1)s = ANY (x))",
            checkparams={"param_1": 5},
        )

        self.assert_compile(
            c.all(5),
            "%(param_1)s = ALL (x)",
            checkparams={"param_1": 5},
        )

        self.assert_compile(
            ~c.all(5),
            "NOT (%(param_1)s = ALL (x))",
            checkparams={"param_1": 5},
        )

        self.assert_compile(
            c.any(5, operator=operators.ne),
            "%(param_1)s != ANY (x)",
            checkparams={"param_1": 5},
        )
        self.assert_compile(
            postgresql.All(6, c, operator=operators.gt),
            "%(param_1)s > ALL (x)",
            checkparams={"param_1": 6},
        )
        self.assert_compile(
            c.all(7, operator=operators.lt),
            "%(param_1)s < ALL (x)",
            checkparams={"param_1": 7},
        )

    def _test_array_zero_indexes(self, zero_indexes):
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

    def test_array_zero_indexes_true(self):
        self._test_array_zero_indexes(True)

    def test_array_zero_indexes_false(self):
        self._test_array_zero_indexes(False)

    def test_array_literal_type(self):
        isinstance(postgresql.array([1, 2]).type, postgresql.ARRAY)
        is_(postgresql.array([1, 2]).type.item_type._type_affinity, Integer)

        is_(
            postgresql.array(
                [1, 2], type_=String
            ).type.item_type._type_affinity,
            String,
        )

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
        self.assert_compile(
            t.update().values({t.c.data[2:5]: 2}),
            "UPDATE t SET data[%(data_1)s:%(data_2)s]=%(param_1)s",
            checkparams={"param_1": 2, "data_2": 5, "data_1": 2},
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

        stmt = update(tbl1, values=dict(id=1))
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
            "SELECT testschema.testtbl3.id FROM " "ONLY testschema.testtbl3"
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
        ("no_persisted", " STORED", "ignore"),
        ("persisted_none", " STORED", None),
        ("persisted_true", " STORED", True),
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

    def test_column_computed_persisted_false(self):
        m = MetaData()
        t = Table(
            "t",
            m,
            Column("x", Integer),
            Column("y", Integer, Computed("x + 2", persisted=False)),
        )
        assert_raises_message(
            exc.CompileError,
            "PostrgreSQL computed columns do not support 'virtual'",
            schema.CreateTable(t).compile,
            dialect=postgresql.dialect(),
        )

    def test_column_identity(self):
        # all other tests are in test_identity_column.py
        m = MetaData()
        t = Table(
            "t",
            m,
            Column("y", Integer, Identity(always=True, start=4, increment=7)),
        )
        self.assert_compile(
            schema.CreateTable(t),
            "CREATE TABLE t (y INTEGER GENERATED ALWAYS AS IDENTITY "
            "(INCREMENT BY 7 START WITH 4))",
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


class InsertOnConflictTest(fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = postgresql.dialect()

    def setup_test(self):
        self.table1 = table1 = table(
            "mytable",
            column("myid", Integer),
            column("name", String(128)),
            column("description", String(128)),
        )
        md = MetaData()
        self.table_with_metadata = Table(
            "mytable",
            md,
            Column("myid", Integer, primary_key=True),
            Column("name", String(128)),
            Column("description", String(128)),
        )
        self.unique_constr = schema.UniqueConstraint(
            table1.c.name, name="uq_name"
        )
        self.excl_constr = ExcludeConstraint(
            (table1.c.name, "="),
            (table1.c.description, "&&"),
            name="excl_thing",
        )
        self.excl_constr_anon = ExcludeConstraint(
            (self.table_with_metadata.c.name, "="),
            (self.table_with_metadata.c.description, "&&"),
            where=self.table_with_metadata.c.description != "foo",
        )
        self.goofy_index = Index(
            "goofy_index", table1.c.name, postgresql_where=table1.c.name > "m"
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

    def test_do_nothing_no_target(self):

        i = insert(
            self.table1, values=dict(name="foo")
        ).on_conflict_do_nothing()
        self.assert_compile(
            i,
            "INSERT INTO mytable (name) VALUES "
            "(%(name)s) ON CONFLICT DO NOTHING",
        )

    def test_do_nothing_index_elements_target(self):

        i = insert(
            self.table1, values=dict(name="foo")
        ).on_conflict_do_nothing(index_elements=["myid"])
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
        i = insert(self.table1, values=dict(name="foo"))
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
        i = insert(self.table1, values=dict(name="foo"))
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
        i = insert(self.table_with_metadata, values=dict(myid=1, name="foo"))
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
        i = insert(self.table_with_metadata, values=dict(myid=1, name="foo"))
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
        i = insert(self.table1, values=dict(name="foo"))
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
        i = insert(self.table1, values=dict(name="foo"))
        i = i.on_conflict_do_update(
            constraint=self.unique_constr.name, set_=dict(myid=i.excluded.myid)
        )
        self.assert_compile(
            i,
            "INSERT INTO mytable (name) VALUES "
            "(%(name)s) ON CONFLICT ON CONSTRAINT uq_name "
            "DO UPDATE SET myid = excluded.myid",
        )

    def test_do_update_index_elements_where_target(self):
        i = insert(self.table1, values=dict(name="foo"))
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
        i = insert(
            self.table1,
            values=[dict(name="foo"), dict(name="bar"), dict(name="bat")],
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
        i = insert(self.table1, values=dict(name="foo"))

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
        i = insert(self.table1, values=dict(name="foo"))
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

    def test_do_update_add_whereclause(self):
        i = insert(self.table1, values=dict(name="foo"))
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

    def test_do_update_add_whereclause_references_excluded(self):
        i = insert(self.table1, values=dict(name="foo"))
        i = i.on_conflict_do_update(
            constraint=self.excl_constr_anon,
            set_=dict(name=i.excluded.name),
            where=((self.table1.c.name != i.excluded.name)),
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
        i = insert(self.table1, values=dict(name="bar"))
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
        i = insert(self.table1, values=dict(name="foo"))
        i = (
            i.on_conflict_do_update(
                constraint=self.excl_constr_anon,
                set_=dict(name=i.excluded.name),
                where=((self.table1.c.name != i.excluded.name)),
            )
            .returning(literal_column("1"))
            .cte("i_upsert")
        )

        stmt = select(i)

        self.assert_compile(
            stmt,
            "WITH i_upsert AS "
            "(INSERT INTO mytable (name) VALUES (%(name)s) "
            "ON CONFLICT (name, description) "
            "WHERE description != %(description_1)s "
            "DO UPDATE SET name = excluded.name "
            "WHERE mytable.name != excluded.name RETURNING 1) "
            "SELECT i_upsert.1 "
            "FROM i_upsert",
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


class DistinctOnTest(fixtures.TestBase, AssertsCompiledSQL):

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

    def test_plain_generative(self):
        self.assert_compile(
            select(self.table).distinct(),
            "SELECT DISTINCT t.id, t.a, t.b FROM t",
        )

    def test_on_columns_generative(self):
        self.assert_compile(
            select(self.table).distinct(self.table.c.a),
            "SELECT DISTINCT ON (t.a) t.id, t.a, t.b FROM t",
        )

    def test_on_columns_generative_multi_call(self):
        self.assert_compile(
            select(self.table)
            .distinct(self.table.c.a)
            .distinct(self.table.c.b),
            "SELECT DISTINCT ON (t.a, t.b) t.id, t.a, t.b FROM t",
        )

    def test_plain_inline(self):
        self.assert_compile(
            select(self.table).distinct(),
            "SELECT DISTINCT t.id, t.a, t.b FROM t",
        )

    def test_on_columns_inline_list(self):
        self.assert_compile(
            select(self.table)
            .distinct(self.table.c.a, self.table.c.b)
            .order_by(self.table.c.a, self.table.c.b),
            "SELECT DISTINCT ON (t.a, t.b) t.id, "
            "t.a, t.b FROM t ORDER BY t.a, t.b",
        )

    def test_on_columns_inline_scalar(self):
        self.assert_compile(
            select(self.table).distinct(self.table.c.a),
            "SELECT DISTINCT ON (t.a) t.id, t.a, t.b FROM t",
        )

    def test_literal_binds(self):
        self.assert_compile(
            select(self.table).distinct(self.table.c.a == 10),
            "SELECT DISTINCT ON (t.a = 10) t.id, t.a, t.b FROM t",
            literal_binds=True,
        )

    def test_query_plain(self):
        sess = Session()
        self.assert_compile(
            sess.query(self.table).distinct(),
            "SELECT DISTINCT t.id AS t_id, t.a AS t_a, " "t.b AS t_b FROM t",
        )

    def test_query_on_columns(self):
        sess = Session()
        self.assert_compile(
            sess.query(self.table).distinct(self.table.c.a),
            "SELECT DISTINCT ON (t.a) t.id AS t_id, t.a AS t_a, "
            "t.b AS t_b FROM t",
        )

    def test_query_on_columns_multi_call(self):
        sess = Session()
        self.assert_compile(
            sess.query(self.table)
            .distinct(self.table.c.a)
            .distinct(self.table.c.b),
            "SELECT DISTINCT ON (t.a, t.b) t.id AS t_id, t.a AS t_a, "
            "t.b AS t_b FROM t",
        )

    def test_query_on_columns_subquery(self):
        sess = Session()

        class Foo(object):
            pass

        mapper(Foo, self.table)
        sess = Session()
        subq = sess.query(Foo).subquery()

        f1 = aliased(Foo, subq)
        self.assert_compile(
            sess.query(f1).distinct(f1.a, f1.b),
            "SELECT DISTINCT ON (anon_1.a, anon_1.b) anon_1.id "
            "AS anon_1_id, anon_1.a AS anon_1_a, anon_1.b "
            "AS anon_1_b FROM (SELECT t.id AS id, t.a AS a, "
            "t.b AS b FROM t) AS anon_1",
        )

    def test_query_distinct_on_aliased(self):
        class Foo(object):
            pass

        mapper(Foo, self.table)
        a1 = aliased(Foo)
        sess = Session()
        self.assert_compile(
            sess.query(a1).distinct(a1.a),
            "SELECT DISTINCT ON (t_1.a) t_1.id AS t_1_id, "
            "t_1.a AS t_1_a, t_1.b AS t_1_b FROM t AS t_1",
        )

    def test_distinct_on_subquery_anon(self):

        sq = select(self.table).alias()
        q = (
            select(self.table.c.id, sq.c.id)
            .distinct(sq.c.id)
            .where(self.table.c.id == sq.c.id)
        )

        self.assert_compile(
            q,
            "SELECT DISTINCT ON (anon_1.id) t.id, anon_1.id AS id_1 "
            "FROM t, (SELECT t.id AS id, t.a AS a, t.b "
            "AS b FROM t) AS anon_1 WHERE t.id = anon_1.id",
        )

    def test_distinct_on_subquery_named(self):
        sq = select(self.table).alias("sq")
        q = (
            select(self.table.c.id, sq.c.id)
            .distinct(sq.c.id)
            .where(self.table.c.id == sq.c.id)
        )
        self.assert_compile(
            q,
            "SELECT DISTINCT ON (sq.id) t.id, sq.id AS id_1 "
            "FROM t, (SELECT t.id AS id, t.a AS a, "
            "t.b AS b FROM t) AS sq WHERE t.id = sq.id",
        )


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

    def _raise_query(self, q):
        """
        useful for debugging. just do...
        self._raise_query(q)
        """
        c = q.compile(dialect=postgresql.dialect())
        raise ValueError(c)

    def test_match_basic(self):
        s = select(self.table_alt.c.id).where(
            self.table_alt.c.title.match("somestring")
        )
        self.assert_compile(
            s,
            "SELECT mytable.id "
            "FROM mytable "
            "WHERE mytable.title @@ to_tsquery(%(title_1)s)",
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
            """WHERE mytable.title @@ to_tsquery('english', %(title_1)s)""",
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
            "@@ to_tsquery(%(to_tsvector_1)s)",
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
            "to_tsquery(%(to_tsvector_2)s)",
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
            """to_tsquery('english', %(to_tsvector_2)s)""",
        )


class RegexpTest(fixtures.TestBase, testing.AssertsCompiledSQL):
    __dialect__ = "postgresql"

    def setup_test(self):
        self.table = table(
            "mytable", column("myid", Integer), column("name", String)
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
            "mytable.myid ~ CONCAT('(?', %(myid_1)s, ')', %(myid_2)s)",
            checkparams={"myid_2": "pattern", "myid_1": "ig"},
        )

    def test_regexp_match_flags_ignorecase(self):
        self.assert_compile(
            self.table.c.myid.regexp_match("pattern", flags="i"),
            "mytable.myid ~* %(myid_1)s",
            checkparams={"myid_1": "pattern"},
        )

    def test_regexp_match_flags_col(self):
        self.assert_compile(
            self.table.c.myid.regexp_match("pattern", flags=self.table.c.name),
            "mytable.myid ~ CONCAT('(?', mytable.name, ')', %(myid_1)s)",
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
            "mytable.myid !~ CONCAT('(?', %(myid_1)s, ')', %(myid_2)s)",
            checkparams={"myid_2": "pattern", "myid_1": "ig"},
        )

    def test_not_regexp_match_flags_ignorecase(self):
        self.assert_compile(
            ~self.table.c.myid.regexp_match("pattern", flags="i"),
            "mytable.myid !~* %(myid_1)s",
            checkparams={"myid_1": "pattern"},
        )

    def test_not_regexp_match_flags_col(self):
        self.assert_compile(
            ~self.table.c.myid.regexp_match(
                "pattern", flags=self.table.c.name
            ),
            "mytable.myid !~ CONCAT('(?', mytable.name, ')', %(myid_1)s)",
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
            "REGEXP_REPLACE(mytable.myid, %(myid_1)s, %(myid_3)s, %(myid_2)s)",
            checkparams={
                "myid_1": "pattern",
                "myid_3": "replacement",
                "myid_2": "ig",
            },
        )

    def test_regexp_replace_flags_col(self):
        self.assert_compile(
            self.table.c.myid.regexp_replace(
                "pattern", "replacement", flags=self.table.c.name
            ),
            "REGEXP_REPLACE(mytable.myid, %(myid_1)s,"
            " %(myid_2)s, mytable.name)",
            checkparams={"myid_1": "pattern", "myid_2": "replacement"},
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
