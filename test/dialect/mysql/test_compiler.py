import random

from sqlalchemy import BLOB
from sqlalchemy import BOOLEAN
from sqlalchemy import Boolean
from sqlalchemy import cast
from sqlalchemy import CHAR
from sqlalchemy import CheckConstraint
from sqlalchemy import CLOB
from sqlalchemy import Column
from sqlalchemy import Computed
from sqlalchemy import create_engine
from sqlalchemy import DATE
from sqlalchemy import Date
from sqlalchemy import DATETIME
from sqlalchemy import DateTime
from sqlalchemy import DECIMAL
from sqlalchemy import DOUBLE
from sqlalchemy import Double
from sqlalchemy import exc
from sqlalchemy import extract
from sqlalchemy import FLOAT
from sqlalchemy import Float
from sqlalchemy import ForeignKey
from sqlalchemy import func
from sqlalchemy import Index
from sqlalchemy import INT
from sqlalchemy import Integer
from sqlalchemy import Interval
from sqlalchemy import JSON
from sqlalchemy import LargeBinary
from sqlalchemy import literal
from sqlalchemy import MetaData
from sqlalchemy import NCHAR
from sqlalchemy import NUMERIC
from sqlalchemy import Numeric
from sqlalchemy import NVARCHAR
from sqlalchemy import PrimaryKeyConstraint
from sqlalchemy import schema
from sqlalchemy import select
from sqlalchemy import Sequence
from sqlalchemy import SmallInteger
from sqlalchemy import sql
from sqlalchemy import String
from sqlalchemy import Table
from sqlalchemy import testing
from sqlalchemy import TEXT
from sqlalchemy import Text
from sqlalchemy import text
from sqlalchemy import TIME
from sqlalchemy import Time
from sqlalchemy import TIMESTAMP
from sqlalchemy import types as sqltypes
from sqlalchemy import Unicode
from sqlalchemy import UnicodeText
from sqlalchemy import VARCHAR
from sqlalchemy.dialects.mysql import base as mysql
from sqlalchemy.dialects.mysql import insert
from sqlalchemy.dialects.mysql import limit
from sqlalchemy.dialects.mysql import match
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.sql import column
from sqlalchemy.sql import delete
from sqlalchemy.sql import table
from sqlalchemy.sql import update
from sqlalchemy.sql.ddl import CreateSequence
from sqlalchemy.sql.expression import bindparam
from sqlalchemy.sql.expression import literal_column
from sqlalchemy.testing import assert_raises_message
from sqlalchemy.testing import AssertsCompiledSQL
from sqlalchemy.testing import eq_
from sqlalchemy.testing import eq_ignore_whitespace
from sqlalchemy.testing import expect_raises
from sqlalchemy.testing import expect_warnings
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import mock
from sqlalchemy.testing import Variation
from sqlalchemy.testing.fixtures import CacheKeyFixture


class ReservedWordFixture(AssertsCompiledSQL):
    @testing.fixture()
    def mysql_mariadb_reserved_words(self):
        table = Table(
            "rw_table",
            MetaData(),
            Column("mysql_reserved", Integer),
            Column("mdb_mysql_reserved", Integer),
            Column("mdb_reserved", Integer),
        )

        expected_mysql = (
            "SELECT rw_table.`mysql_reserved`, "
            "rw_table.`mdb_mysql_reserved`, "
            "rw_table.mdb_reserved FROM rw_table"
        )
        expected_mdb = (
            "SELECT rw_table.mysql_reserved, "
            "rw_table.`mdb_mysql_reserved`, "
            "rw_table.`mdb_reserved` FROM rw_table"
        )

        from sqlalchemy.dialects.mysql import reserved_words

        reserved_words.RESERVED_WORDS_MARIADB.add("mdb_reserved")
        reserved_words.RESERVED_WORDS_MYSQL.add("mysql_reserved")
        reserved_words.RESERVED_WORDS_MYSQL.add("mdb_mysql_reserved")
        reserved_words.RESERVED_WORDS_MARIADB.add("mdb_mysql_reserved")

        try:
            yield table, expected_mysql, expected_mdb
        finally:
            reserved_words.RESERVED_WORDS_MARIADB.discard("mdb_reserved")
            reserved_words.RESERVED_WORDS_MYSQL.discard("mysql_reserved")
            reserved_words.RESERVED_WORDS_MYSQL.discard("mdb_mysql_reserved")
            reserved_words.RESERVED_WORDS_MARIADB.discard("mdb_mysql_reserved")


class CompileTest(ReservedWordFixture, fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = mysql.dialect()

    @testing.combinations(
        ("mariadb", True),
        ("mysql", False),
        (mysql.dialect(), False),
        (mysql.dialect(is_mariadb=True), True),
        (
            create_engine(
                "mysql+pymysql://", module=mock.Mock(paramstyle="format")
            ).dialect,
            False,
        ),
        (
            create_engine(
                "mariadb+pymysql://", module=mock.Mock(paramstyle="format")
            ).dialect,
            True,
        ),
        argnames="dialect, expect_mariadb",
    )
    def test_reserved_words_mysql_vs_mariadb(
        self, dialect, expect_mariadb, mysql_mariadb_reserved_words
    ):
        """test #7167 - compiler level

        We want to make sure that the "is mariadb" flag as well as the
        correct identifier preparer are set up for dialects no matter how they
        determine their "is_mariadb" flag.

        """

        table, expected_mysql, expected_mdb = mysql_mariadb_reserved_words
        self.assert_compile(
            select(table),
            expected_mdb if expect_mariadb else expected_mysql,
            dialect=dialect,
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
            str(stmt.compile(dialect=mysql.dialect(is_mariadb=True))),
            "INSERT INTO t (description) VALUES (lower(%s)) "
            "RETURNING t.myid, t.name, t.description",
        )
        eq_ignore_whitespace(
            str(stmt.compile(dialect=mysql.dialect())),
            "INSERT INTO t (description) VALUES (lower(%s))",
        )

    def test_create_index_simple(self):
        m = MetaData()
        tbl = Table("testtbl", m, Column("data", String(255)))
        idx = Index("test_idx1", tbl.c.data)

        self.assert_compile(
            schema.CreateIndex(idx), "CREATE INDEX test_idx1 ON testtbl (data)"
        )

    @testing.combinations("mysql", "mariadb", argnames="dialect_name")
    def test_create_index_with_prefix(self, dialect_name):
        m = MetaData()
        tbl = Table("testtbl", m, Column("data", String(255)))
        idx = Index(
            "test_idx1",
            tbl.c.data,
            **{
                f"{dialect_name}_length": 10,
                f"{dialect_name}_prefix": "FULLTEXT",
            },
        )

        self.assert_compile(
            schema.CreateIndex(idx),
            "CREATE FULLTEXT INDEX test_idx1 ON testtbl (data(10))",
            dialect=dialect_name,
        )

    def test_create_index_with_text(self):
        m = MetaData()
        tbl = Table("testtbl", m, Column("data", String(255)))
        idx = Index("test_idx1", text("created_at desc"), _table=tbl)

        self.assert_compile(
            schema.CreateIndex(idx),
            "CREATE INDEX test_idx1 ON testtbl (created_at desc)",
        )

    def test_create_index_with_arbitrary_column_element(self):
        from sqlalchemy.ext.compiler import compiles

        class _textual_index_element(sql.ColumnElement):
            """alembic's wrapper"""

            __visit_name__ = "_textual_idx_element"

            def __init__(self, table, text):
                self.table = table
                self.text = text

        @compiles(_textual_index_element)
        def _render_textual_index_column(element, compiler, **kw):
            return compiler.process(element.text, **kw)

        m = MetaData()
        tbl = Table("testtbl", m, Column("data", String(255)))
        idx = Index(
            "test_idx1",
            _textual_index_element(tbl, text("created_at desc")),
            _table=tbl,
        )

        self.assert_compile(
            schema.CreateIndex(idx),
            "CREATE INDEX test_idx1 ON testtbl (created_at desc)",
        )

    @testing.combinations("mysql", "mariadb", argnames="dialect_name")
    def test_create_index_with_parser(self, dialect_name):
        m = MetaData()
        tbl = Table("testtbl", m, Column("data", String(255)))
        idx = Index(
            "test_idx1",
            tbl.c.data,
            **{
                f"{dialect_name}_length": 10,
                f"{dialect_name}_prefix": "FULLTEXT",
                f"{dialect_name}_with_parser": "ngram",
            },
        )

        self.assert_compile(
            schema.CreateIndex(idx),
            "CREATE FULLTEXT INDEX test_idx1 "
            "ON testtbl (data(10)) WITH PARSER ngram",
            dialect=dialect_name,
        )

    @testing.combinations("mysql", "mariadb", argnames="dialect_name")
    def test_create_index_with_length(self, dialect_name):
        m = MetaData()
        tbl = Table("testtbl", m, Column("data", String(255)))
        idx1 = Index(
            "test_idx1", tbl.c.data, **{f"{dialect_name}_length": 10}
        )
        idx2 = Index(
            "test_idx2", tbl.c.data, **{f"{dialect_name}_length": 5}
        )

        self.assert_compile(
            schema.CreateIndex(idx1),
            "CREATE INDEX test_idx1 ON testtbl (data(10))",
            dialect=dialect_name,
        )
        self.assert_compile(
            schema.CreateIndex(idx2),
            "CREATE INDEX test_idx2 ON testtbl (data(5))",
            dialect=dialect_name,
        )

    def test_drop_constraint_mysql(self):
        m = MetaData()
        table_name = "testtbl"
        constraint_name = "constraint"
        constraint = CheckConstraint("data IS NOT NULL", name=constraint_name)
        Table(table_name, m, Column("data", String(255)), constraint)
        dialect = mysql.dialect()
        self.assert_compile(
            schema.DropConstraint(constraint),
            "ALTER TABLE %s DROP CHECK `%s`" % (table_name, constraint_name),
            dialect=dialect,
        )

    def test_drop_constraint_mariadb(self):
        m = MetaData()
        table_name = "testtbl"
        constraint_name = "constraint"
        constraint = CheckConstraint("data IS NOT NULL", name=constraint_name)
        Table(table_name, m, Column("data", String(255)), constraint)
        self.assert_compile(
            schema.DropConstraint(constraint),
            "ALTER TABLE %s DROP CONSTRAINT `%s`"
            % (table_name, constraint_name),
            dialect="mariadb",
        )

    def test_create_index_with_length_quoted(self):
        m = MetaData()
        tbl = Table(
            "testtbl", m, Column("some quoted data", String(255), key="s")
        )
        idx1 = Index("test_idx1", tbl.c.s, mysql_length=10)

        self.assert_compile(
            schema.CreateIndex(idx1),
            "CREATE INDEX test_idx1 ON testtbl (`some quoted data`(10))",
        )

    def test_create_composite_index_with_length_quoted(self):
        m = MetaData()
        tbl = Table(
            "testtbl",
            m,
            Column("some Quoted a", String(255), key="a"),
            Column("some Quoted b", String(255), key="b"),
        )
        idx1 = Index(
            "test_idx1",
            tbl.c.a,
            tbl.c.b,
            mysql_length={"some Quoted a": 10, "some Quoted b": 20},
        )

        self.assert_compile(
            schema.CreateIndex(idx1),
            "CREATE INDEX test_idx1 ON testtbl "
            "(`some Quoted a`(10), `some Quoted b`(20))",
        )

    def test_create_composite_index_with_length_quoted_3085_workaround(self):
        m = MetaData()
        tbl = Table(
            "testtbl",
            m,
            Column("some quoted a", String(255), key="a"),
            Column("some quoted b", String(255), key="b"),
        )
        idx1 = Index(
            "test_idx1",
            tbl.c.a,
            tbl.c.b,
            mysql_length={"`some quoted a`": 10, "`some quoted b`": 20},
        )

        self.assert_compile(
            schema.CreateIndex(idx1),
            "CREATE INDEX test_idx1 ON testtbl "
            "(`some quoted a`(10), `some quoted b`(20))",
        )

    def test_create_composite_index_with_length(self):
        m = MetaData()
        tbl = Table(
            "testtbl", m, Column("a", String(255)), Column("b", String(255))
        )

        idx1 = Index(
            "test_idx1", tbl.c.a, tbl.c.b, mysql_length={"a": 10, "b": 20}
        )
        idx2 = Index("test_idx2", tbl.c.a, tbl.c.b, mysql_length={"a": 15})
        idx3 = Index("test_idx3", tbl.c.a, tbl.c.b, mysql_length=30)

        self.assert_compile(
            schema.CreateIndex(idx1),
            "CREATE INDEX test_idx1 ON testtbl (a(10), b(20))",
        )
        self.assert_compile(
            schema.CreateIndex(idx2),
            "CREATE INDEX test_idx2 ON testtbl (a(15), b)",
        )
        self.assert_compile(
            schema.CreateIndex(idx3),
            "CREATE INDEX test_idx3 ON testtbl (a(30), b(30))",
        )

    @testing.combinations("mysql", "mariadb", argnames="dialect_name")
    def test_create_index_with_using(self, dialect_name):
        m = MetaData()
        tbl = Table("testtbl", m, Column("data", String(255)))
        idx1 = Index(
            "test_idx1", tbl.c.data, **{f"{dialect_name}_using": "btree"}
        )
        idx2 = Index(
            "test_idx2", tbl.c.data, **{f"{dialect_name}_using": "hash"}
        )

        self.assert_compile(
            schema.CreateIndex(idx1),
            "CREATE INDEX test_idx1 ON testtbl (data) USING btree",
            dialect=dialect_name,
        )
        self.assert_compile(
            schema.CreateIndex(idx2),
            "CREATE INDEX test_idx2 ON testtbl (data) USING hash",
            dialect=dialect_name,
        )

    def test_create_pk_plain(self):
        m = MetaData()
        tbl = Table(
            "testtbl",
            m,
            Column("data", String(255)),
            PrimaryKeyConstraint("data"),
        )

        self.assert_compile(
            schema.CreateTable(tbl),
            "CREATE TABLE testtbl (data VARCHAR(255) NOT NULL, "
            "PRIMARY KEY (data))",
        )

    @testing.combinations("mysql", "mariadb", argnames="dialect_name")
    def test_create_pk_with_using(self, dialect_name):
        m = MetaData()
        tbl = Table(
            "testtbl",
            m,
            Column("data", String(255)),
            PrimaryKeyConstraint("data", **{f"{dialect_name}_using": "btree"}),
        )

        self.assert_compile(
            schema.CreateTable(tbl),
            "CREATE TABLE testtbl (data VARCHAR(255) NOT NULL, "
            "PRIMARY KEY (data) USING btree)",
            dialect=dialect_name,
        )

    @testing.combinations(
        (True, True, (10, 2, 2)),
        (True, True, (10, 2, 1)),
        (False, True, (10, 2, 0)),
        (True, False, (8, 0, 14)),
        (True, False, (8, 0, 13)),
        (False, False, (8, 0, 12)),
        argnames="has_brackets,is_mariadb,version",
    )
    def test_create_server_default_with_function_using(
        self, has_brackets, is_mariadb, version
    ):
        dialect = mysql.dialect(is_mariadb=is_mariadb)
        dialect.server_version_info = version
        if is_mariadb:
            with testing.expect_warnings(".*"):
                dialect._initialize_mariadb(None)
        else:
            dialect._initialize_mysql(None)

        m = MetaData()
        tbl = Table(
            "testtbl",
            m,
            Column("time", DateTime, server_default=func.current_timestamp()),
            Column("name", String(255), server_default="some str"),
            Column(
                "description", String(255), server_default=func.lower("hi")
            ),
            Column("data", JSON, server_default=func.json_object()),
            Column(
                "updated1",
                DateTime,
                server_default=text("now() on update now()"),
            ),
            Column(
                "updated2",
                DateTime,
                server_default=text("now() On  UpDate now()"),
            ),
            Column(
                "updated3",
                DateTime,
                server_default=text("now() ON UPDATE now()"),
            ),
            Column(
                "updated4",
                DateTime,
                server_default=text("now(3)"),
            ),
            Column(
                "updated5",
                DateTime,
                server_default=text("nOW(3)"),
            ),
            Column(
                "updated6",
                DateTime,
                server_default=text("notnow(1)"),
            ),
            Column(
                "updated7",
                DateTime,
                server_default=text("CURRENT_TIMESTAMP(3)"),
            ),
        )

        eq_(dialect._support_default_function, has_brackets)

        if has_brackets:
            self.assert_compile(
                schema.CreateTable(tbl),
                "CREATE TABLE testtbl ("
                "time DATETIME DEFAULT CURRENT_TIMESTAMP, "
                "name VARCHAR(255) DEFAULT 'some str', "
                "description VARCHAR(255) DEFAULT (lower('hi')), "
                "data JSON DEFAULT (json_object()), "
                "updated1 DATETIME DEFAULT now() on update now(), "
                "updated2 DATETIME DEFAULT now() On  UpDate now(), "
                "updated3 DATETIME DEFAULT now() ON UPDATE now(), "
                "updated4 DATETIME DEFAULT now(3), "
                "updated5 DATETIME DEFAULT nOW(3), "
                "updated6 DATETIME DEFAULT (notnow(1)), "
                "updated7 DATETIME DEFAULT CURRENT_TIMESTAMP(3))",
                dialect=dialect,
            )
        else:
            self.assert_compile(
                schema.CreateTable(tbl),
                "CREATE TABLE testtbl ("
                "time DATETIME DEFAULT CURRENT_TIMESTAMP, "
                "name VARCHAR(255) DEFAULT 'some str', "
                "description VARCHAR(255) DEFAULT lower('hi'), "
                "data JSON DEFAULT json_object(), "
                "updated1 DATETIME DEFAULT now() on update now(), "
                "updated2 DATETIME DEFAULT now() On  UpDate now(), "
                "updated3 DATETIME DEFAULT now() ON UPDATE now(), "
                "updated4 DATETIME DEFAULT now(3), "
                "updated5 DATETIME DEFAULT nOW(3), "
                "updated6 DATETIME DEFAULT notnow(1), "
                "updated7 DATETIME DEFAULT CURRENT_TIMESTAMP(3))",
                dialect=dialect,
            )

    def test_create_index_expr(self):
        m = MetaData()
        t1 = Table("foo", m, Column("x", Integer))
        self.assert_compile(
            schema.CreateIndex(Index("bar", t1.c.x > 5)),
            "CREATE INDEX bar ON foo ((x > 5))",
        )

    def test_create_index_expr_two(self):
        m = MetaData()
        tbl = Table("testtbl", m, Column("x", Integer), Column("y", Integer))
        idx1 = Index("test_idx1", tbl.c.x + tbl.c.y)
        idx2 = Index(
            "test_idx2", tbl.c.x, tbl.c.x + tbl.c.y, tbl.c.y - tbl.c.x
        )
        idx3 = Index("test_idx3", tbl.c.x.desc())

        self.assert_compile(
            schema.CreateIndex(idx1),
            "CREATE INDEX test_idx1 ON testtbl ((x + y))",
        )
        self.assert_compile(
            schema.CreateIndex(idx2),
            "CREATE INDEX test_idx2 ON testtbl (x, (x + y), (y - x))",
        )

        self.assert_compile(
            schema.CreateIndex(idx3),
            "CREATE INDEX test_idx3 ON testtbl (x DESC)",
        )

    def test_create_index_expr_func(self):
        m = MetaData()
        tbl = Table("testtbl", m, Column("data", Integer))
        idx1 = Index("test_idx1", func.radians(tbl.c.data))

        self.assert_compile(
            schema.CreateIndex(idx1),
            "CREATE INDEX test_idx1 ON testtbl ((radians(data)))",
        )

    def test_create_index_expr_func_unary(self):
        m = MetaData()
        tbl = Table("testtbl", m, Column("data", Integer))
        idx1 = Index("test_idx1", -tbl.c.data)

        self.assert_compile(
            schema.CreateIndex(idx1),
            "CREATE INDEX test_idx1 ON testtbl ((-data))",
        )

    def test_deferrable_initially_kw_not_ignored(self):
        m = MetaData()
        Table("t1", m, Column("id", Integer, primary_key=True))
        t2 = Table(
            "t2",
            m,
            Column(
                "id",
                Integer,
                ForeignKey("t1.id", deferrable=True, initially="DEFERRED"),
                primary_key=True,
            ),
        )

        self.assert_compile(
            schema.CreateTable(t2),
            "CREATE TABLE t2 (id INTEGER NOT NULL, "
            "PRIMARY KEY (id), FOREIGN KEY(id) REFERENCES t1 (id) "
            "DEFERRABLE INITIALLY DEFERRED)",
        )

    def test_match_kw_raises(self):
        m = MetaData()
        Table("t1", m, Column("id", Integer, primary_key=True))
        t2 = Table(
            "t2",
            m,
            Column(
                "id",
                Integer,
                ForeignKey("t1.id", match="XYZ"),
                primary_key=True,
            ),
        )

        assert_raises_message(
            exc.CompileError,
            "MySQL ignores the 'MATCH' keyword while at the same time causes "
            "ON UPDATE/ON DELETE clauses to be ignored.",
            schema.CreateTable(t2).compile,
            dialect=mysql.dialect(),
        )

    def test_concat_compile_kw(self):
        expr = literal("x", type_=String) + literal("y", type_=String)
        self.assert_compile(expr, "concat('x', 'y')", literal_binds=True)

    def test_mariadb_for_update(self):
        table1 = table(
            "mytable", column("myid"), column("name"), column("description")
        )

        self.assert_compile(
            table1.select()
            .where(table1.c.myid == 7)
            .with_for_update(of=table1),
            "SELECT mytable.myid, mytable.name, mytable.description "
            "FROM mytable WHERE mytable.myid = %s "
            "FOR UPDATE",
            dialect="mariadb",
        )

    def test_delete_extra_froms(self):
        t1 = table("t1", column("c1"))
        t2 = table("t2", column("c1"))
        q = sql.delete(t1).where(t1.c.c1 == t2.c.c1)
        self.assert_compile(
            q, "DELETE FROM t1 USING t1, t2 WHERE t1.c1 = t2.c1"
        )

    def test_delete_extra_froms_alias(self):
        a1 = table("t1", column("c1")).alias("a1")
        t2 = table("t2", column("c1"))
        q = sql.delete(a1).where(a1.c.c1 == t2.c.c1)
        self.assert_compile(
            q, "DELETE FROM a1 USING t1 AS a1, t2 WHERE a1.c1 = t2.c1"
        )
        self.assert_compile(sql.delete(a1), "DELETE FROM t1 AS a1")

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

    def test_groupby_rollup(self):
        t = table("tt", column("foo"), column("bar"))
        q = sql.select(t.c.foo).group_by(sql.func.rollup(t.c.foo, t.c.bar))
        self.assert_compile(
            q, "SELECT tt.foo FROM tt GROUP BY tt.foo, tt.bar WITH ROLLUP"
        )


class CustomExtensionTest(
    fixtures.TestBase, AssertsCompiledSQL, fixtures.CacheKeySuite
):
    __dialect__ = "mysql"

    @fixtures.CacheKeySuite.run_suite_tests
    def test_insert_on_duplicate_key_cache_key(self):
        table = Table(
            "foos",
            MetaData(),
            Column("id", Integer, primary_key=True),
            Column("bar", String(10)),
            Column("baz", String(10)),
        )

        def stmt0():
            # note a multivalues INSERT is not cacheable; use just one
            # set of values
            return insert(table).values(
                {"id": 1, "bar": "ab"},
            )

        def stmt1():
            stmt = stmt0()
            return stmt.on_duplicate_key_update(
                bar=stmt.inserted.bar, baz=stmt.inserted.baz
            )

        def stmt15():
            stmt = insert(table).values(
                {"id": 1},
            )
            return stmt.on_duplicate_key_update(
                bar=stmt.inserted.bar, baz=stmt.inserted.baz
            )

        def stmt2():
            stmt = stmt0()
            return stmt.on_duplicate_key_update(bar=stmt.inserted.bar)

        def stmt3():
            stmt = stmt0()
            # use different literal values; ensure each cache key is
            # identical
            return stmt.on_duplicate_key_update(
                bar=random.choice(["a", "b", "c"])
            )

        return lambda: [stmt0(), stmt1(), stmt15(), stmt2(), stmt3()]

    @fixtures.CacheKeySuite.run_suite_tests
    def test_dml_limit_cache_key(self):
        t = sql.table("t", sql.column("col1"), sql.column("col2"))
        return lambda: [
            t.update().ext(limit(5)),
            t.delete().ext(limit(5)),
            t.update(),
            t.delete(),
        ]

    def test_update_limit(self):
        t = sql.table("t", sql.column("col1"), sql.column("col2"))

        self.assert_compile(
            t.update().values({"col1": 123}).ext(limit(5)),
            "UPDATE t SET col1=%s LIMIT __[POSTCOMPILE_param_1]",
            params={"col1": 123, "param_1": 5},
            check_literal_execute={"param_1": 5},
        )

        # does not make sense but we want this to compile
        self.assert_compile(
            t.update().values({"col1": 123}).ext(limit(0)),
            "UPDATE t SET col1=%s LIMIT __[POSTCOMPILE_param_1]",
            params={"col1": 123, "param_1": 0},
            check_literal_execute={"param_1": 0},
        )

        # many times is fine too
        self.assert_compile(
            t.update()
            .values({"col1": 123})
            .ext(limit(0))
            .ext(limit(3))
            .ext(limit(42)),
            "UPDATE t SET col1=%s LIMIT __[POSTCOMPILE_param_1]",
            params={"col1": 123, "param_1": 42},
            check_literal_execute={"param_1": 42},
        )

    def test_delete_limit(self):
        t = sql.table("t", sql.column("col1"), sql.column("col2"))

        self.assert_compile(
            t.delete().ext(limit(5)),
            "DELETE FROM t LIMIT __[POSTCOMPILE_param_1]",
            params={"param_1": 5},
            check_literal_execute={"param_1": 5},
        )

        # does not make sense but we want this to compile
        self.assert_compile(
            t.delete().ext(limit(0)),
            "DELETE FROM t LIMIT __[POSTCOMPILE_param_1]",
            params={"param_1": 5},
            check_literal_execute={"param_1": 0},
        )

        # many times is fine too
        self.assert_compile(
            t.delete().ext(limit(0)).ext(limit(3)).ext(limit(42)),
            "DELETE FROM t LIMIT __[POSTCOMPILE_param_1]",
            params={"param_1": 42},
            check_literal_execute={"param_1": 42},
        )

    @testing.combinations((update,), (delete,))
    def test_update_delete_limit_int_only(self, crud_fn):
        t = sql.table("t", sql.column("col1"), sql.column("col2"))

        with expect_raises(ValueError):
            # note using coercions we get an immediate raise
            # without having to wait for compilation
            crud_fn(t).ext(limit("not an int"))

    def test_legacy_update_limit_ext_interaction(self):
        t = sql.table("t", sql.column("col1"), sql.column("col2"))

        stmt = (
            t.update()
            .values({"col1": 123})
            .with_dialect_options(mysql_limit=5)
        )
        stmt.apply_syntax_extension_point(
            lambda existing: [literal_column("this is a clause")],
            "post_criteria",
        )
        self.assert_compile(
            stmt, "UPDATE t SET col1=%s LIMIT 5 this is a clause"
        )

    def test_legacy_delete_limit_ext_interaction(self):
        t = sql.table("t", sql.column("col1"), sql.column("col2"))

        stmt = t.delete().with_dialect_options(mysql_limit=5)
        stmt.apply_syntax_extension_point(
            lambda existing: [literal_column("this is a clause")],
            "post_criteria",
        )
        self.assert_compile(stmt, "DELETE FROM t LIMIT 5 this is a clause")


class SQLTest(fixtures.TestBase, AssertsCompiledSQL, CacheKeyFixture):
    """Tests MySQL-dialect specific compilation."""

    __dialect__ = mysql.dialect()

    def test_precolumns(self):
        dialect = self.__dialect__

        def gen(distinct=None, prefixes=None):
            stmt = select(column("q"))
            if distinct:
                stmt = stmt.distinct()
            if prefixes is not None:
                stmt = stmt.prefix_with(*prefixes)

            return str(stmt.compile(dialect=dialect))

        eq_(gen(None), "SELECT q")
        eq_(gen(True), "SELECT DISTINCT q")

        eq_(gen(prefixes=["ALL"]), "SELECT ALL q")
        eq_(gen(prefixes=["DISTINCTROW"]), "SELECT DISTINCTROW q")

        # Interaction with MySQL prefix extensions
        eq_(gen(None, ["straight_join"]), "SELECT straight_join q")
        eq_(
            gen(False, ["HIGH_PRIORITY", "SQL_SMALL_RESULT", "ALL"]),
            "SELECT HIGH_PRIORITY SQL_SMALL_RESULT ALL q",
        )
        eq_(
            gen(True, ["high_priority", sql.text("sql_cache")]),
            "SELECT high_priority sql_cache DISTINCT q",
        )

    def test_backslash_escaping(self):
        self.assert_compile(
            sql.column("foo").like("bar", escape="\\"),
            "foo LIKE %s ESCAPE '\\\\'",
        )

        dialect = mysql.dialect()
        dialect._backslash_escapes = False
        self.assert_compile(
            sql.column("foo").like("bar", escape="\\"),
            "foo LIKE %s ESCAPE '\\'",
            dialect=dialect,
        )

    def test_limit(self):
        t = sql.table("t", sql.column("col1"), sql.column("col2"))

        self.assert_compile(
            select(t).limit(10).offset(20),
            "SELECT t.col1, t.col2 FROM t  LIMIT %s, %s",
            {"param_1": 20, "param_2": 10},
        )
        self.assert_compile(
            select(t).limit(10),
            "SELECT t.col1, t.col2 FROM t  LIMIT %s",
            {"param_1": 10},
        )

        self.assert_compile(
            select(t).offset(10),
            "SELECT t.col1, t.col2 FROM t  LIMIT %s, 18446744073709551615",
            {"param_1": 10},
        )

    @testing.combinations(
        (String,),
        (VARCHAR,),
        (String(),),
        (VARCHAR(),),
        (NVARCHAR(),),
        (Unicode,),
        (Unicode(),),
    )
    def test_varchar_raise(self, type_):
        type_ = sqltypes.to_instance(type_)
        assert_raises_message(
            exc.CompileError,
            "VARCHAR requires a length on dialect mysql",
            type_.compile,
            dialect=mysql.dialect(),
        )

        t1 = Table("sometable", MetaData(), Column("somecolumn", type_))
        assert_raises_message(
            exc.CompileError,
            r"\(in table 'sometable', column 'somecolumn'\)\: "
            r"(?:N)?VARCHAR requires a length on dialect mysql",
            schema.CreateTable(t1).compile,
            dialect=mysql.dialect(),
        )

    def test_legacy_update_limit(self):
        t = sql.table("t", sql.column("col1"), sql.column("col2"))

        self.assert_compile(
            t.update().values({"col1": 123}), "UPDATE t SET col1=%s"
        )
        self.assert_compile(
            t.update()
            .values({"col1": 123})
            .with_dialect_options(mysql_limit=5),
            "UPDATE t SET col1=%s LIMIT 5",
        )

        # does not make sense but we want this to compile
        self.assert_compile(
            t.update()
            .values({"col1": 123})
            .with_dialect_options(mysql_limit=0),
            "UPDATE t SET col1=%s LIMIT 0",
        )
        self.assert_compile(
            t.update()
            .values({"col1": 123})
            .with_dialect_options(mysql_limit=None),
            "UPDATE t SET col1=%s",
        )
        self.assert_compile(
            t.update()
            .where(t.c.col2 == 456)
            .values({"col1": 123})
            .with_dialect_options(mysql_limit=1),
            "UPDATE t SET col1=%s WHERE t.col2 = %s LIMIT 1",
        )

    def test_legacy_delete_limit(self):
        t = sql.table("t", sql.column("col1"), sql.column("col2"))

        self.assert_compile(t.delete(), "DELETE FROM t")
        self.assert_compile(
            t.delete().with_dialect_options(mysql_limit=5),
            "DELETE FROM t LIMIT 5",
        )
        # does not make sense but we want this to compile
        self.assert_compile(
            t.delete().with_dialect_options(mysql_limit=0),
            "DELETE FROM t LIMIT 0",
        )
        self.assert_compile(
            t.delete().with_dialect_options(mysql_limit=None),
            "DELETE FROM t",
        )
        self.assert_compile(
            t.delete()
            .where(t.c.col2 == 456)
            .with_dialect_options(mysql_limit=1),
            "DELETE FROM t WHERE t.col2 = %s LIMIT 1",
        )

    @testing.combinations((update,), (delete,))
    def test_legacy_update_delete_limit_int_only(self, crud_fn):
        t = sql.table("t", sql.column("col1"), sql.column("col2"))

        with expect_raises(ValueError):
            crud_fn(t).with_dialect_options(mysql_limit="not an int").compile(
                dialect=mysql.dialect()
            )

    def test_utc_timestamp(self):
        self.assert_compile(func.utc_timestamp(), "utc_timestamp()")

    def test_utc_timestamp_fsp(self):
        self.assert_compile(
            func.utc_timestamp(5),
            "utc_timestamp(%s)",
            checkparams={"utc_timestamp_1": 5},
        )

    def test_sysdate(self):
        self.assert_compile(func.sysdate(), "SYSDATE()")

    m = mysql

    @testing.combinations(
        (Integer, "CAST(t.col AS SIGNED INTEGER)"),
        (INT, "CAST(t.col AS SIGNED INTEGER)"),
        (m.MSInteger, "CAST(t.col AS SIGNED INTEGER)"),
        (m.MSInteger(unsigned=True), "CAST(t.col AS UNSIGNED INTEGER)"),
        (SmallInteger, "CAST(t.col AS SIGNED INTEGER)"),
        (m.MSSmallInteger, "CAST(t.col AS SIGNED INTEGER)"),
        (m.MSTinyInteger, "CAST(t.col AS SIGNED INTEGER)"),
        # 'SIGNED INTEGER' is a bigint, so this is ok.
        (m.MSBigInteger, "CAST(t.col AS SIGNED INTEGER)"),
        (m.MSBigInteger(unsigned=False), "CAST(t.col AS SIGNED INTEGER)"),
        (m.MSBigInteger(unsigned=True), "CAST(t.col AS UNSIGNED INTEGER)"),
        # this is kind of sucky.  thank you default arguments!
        (NUMERIC, "CAST(t.col AS DECIMAL)"),
        (DECIMAL, "CAST(t.col AS DECIMAL)"),
        (Numeric, "CAST(t.col AS DECIMAL)"),
        (m.MSNumeric, "CAST(t.col AS DECIMAL)"),
        (m.MSDecimal, "CAST(t.col AS DECIMAL)"),
        (TIMESTAMP, "CAST(t.col AS DATETIME)"),
        (DATETIME, "CAST(t.col AS DATETIME)"),
        (DATE, "CAST(t.col AS DATE)"),
        (TIME, "CAST(t.col AS TIME)"),
        (DateTime, "CAST(t.col AS DATETIME)"),
        (Date, "CAST(t.col AS DATE)"),
        (Time, "CAST(t.col AS TIME)"),
        (DateTime, "CAST(t.col AS DATETIME)"),
        (Date, "CAST(t.col AS DATE)"),
        (m.MSTime, "CAST(t.col AS TIME)"),
        (m.MSTimeStamp, "CAST(t.col AS DATETIME)"),
        (String, "CAST(t.col AS CHAR)"),
        (Unicode, "CAST(t.col AS CHAR)"),
        (UnicodeText, "CAST(t.col AS CHAR)"),
        (VARCHAR, "CAST(t.col AS CHAR)"),
        (NCHAR, "CAST(t.col AS CHAR)"),
        (CHAR, "CAST(t.col AS CHAR)"),
        (m.CHAR(charset="utf8"), "CAST(t.col AS CHAR CHARACTER SET utf8)"),
        (CLOB, "CAST(t.col AS CHAR)"),
        (TEXT, "CAST(t.col AS CHAR)"),
        (m.TEXT(charset="utf8"), "CAST(t.col AS CHAR CHARACTER SET utf8)"),
        (String(32), "CAST(t.col AS CHAR(32))"),
        (Unicode(32), "CAST(t.col AS CHAR(32))"),
        (CHAR(32), "CAST(t.col AS CHAR(32))"),
        (CHAR(0), "CAST(t.col AS CHAR(0))"),
        (m.MSString, "CAST(t.col AS CHAR)"),
        (m.MSText, "CAST(t.col AS CHAR)"),
        (m.MSTinyText, "CAST(t.col AS CHAR)"),
        (m.MSMediumText, "CAST(t.col AS CHAR)"),
        (m.MSLongText, "CAST(t.col AS CHAR)"),
        (m.MSNChar, "CAST(t.col AS CHAR)"),
        (m.MSNVarChar, "CAST(t.col AS CHAR)"),
        (LargeBinary, "CAST(t.col AS BINARY)"),
        (BLOB, "CAST(t.col AS BINARY)"),
        (m.MSBlob, "CAST(t.col AS BINARY)"),
        (m.MSBlob(32), "CAST(t.col AS BINARY)"),
        (m.MSTinyBlob, "CAST(t.col AS BINARY)"),
        (m.MSMediumBlob, "CAST(t.col AS BINARY)"),
        (m.MSLongBlob, "CAST(t.col AS BINARY)"),
        (m.MSBinary, "CAST(t.col AS BINARY)"),
        (m.MSBinary(32), "CAST(t.col AS BINARY)"),
        (m.MSVarBinary, "CAST(t.col AS BINARY)"),
        (m.MSVarBinary(32), "CAST(t.col AS BINARY)"),
        (Interval, "CAST(t.col AS DATETIME)"),
    )
    def test_cast(self, type_, expected):
        t = sql.table("t", sql.column("col"))
        self.assert_compile(cast(t.c.col, type_), expected)

    def test_cast_type_decorator(self):
        class MyInteger(sqltypes.TypeDecorator):
            impl = Integer
            cache_ok = True

        type_ = MyInteger()
        t = sql.table("t", sql.column("col"))
        self.assert_compile(
            cast(t.c.col, type_), "CAST(t.col AS SIGNED INTEGER)"
        )

    def test_cast_literal_bind(self):
        expr = cast(column("foo", Integer) + 5, Integer())

        self.assert_compile(
            expr, "CAST(foo + 5 AS SIGNED INTEGER)", literal_binds=True
        )

    def test_unsupported_cast_literal_bind(self):
        expr = cast(column("foo", Integer) + 5, Float)

        with expect_warnings(
            "Datatype FLOAT does not support CAST on MySQL/MariaDb;"
        ):
            self.assert_compile(expr, "(foo + 5)", literal_binds=True)

    m = mysql

    @testing.combinations(
        (m.MSBit, "t.col"),
        (FLOAT, "t.col"),
        (Float, "t.col"),
        (m.MSFloat, "t.col"),
        (m.MSDouble, "t.col"),
        (DOUBLE, "t.col"),
        (Double, "t.col"),
        (m.MSReal, "t.col"),
        (m.MSYear, "t.col"),
        (m.MSYear(2), "t.col"),
        (Boolean, "t.col"),
        (BOOLEAN, "t.col"),
        (m.MSEnum, "t.col"),
        (m.MSEnum("1", "2"), "t.col"),
        (m.MSSet, "t.col"),
        (m.MSSet("1", "2"), "t.col"),
    )
    def test_unsupported_casts(self, type_, expected):
        t = sql.table("t", sql.column("col"))
        with expect_warnings(
            "Datatype .* does not support CAST on MySQL/MariaDb;"
        ):
            self.assert_compile(cast(t.c.col, type_), expected)

    @testing.combinations(
        (m.FLOAT, "CAST(t.col AS FLOAT)"),
        (Float, "CAST(t.col AS FLOAT)"),
        (FLOAT, "CAST(t.col AS FLOAT)"),
        (Double, "CAST(t.col AS DOUBLE)"),
        (DOUBLE, "CAST(t.col AS DOUBLE)"),
        (m.DOUBLE, "CAST(t.col AS DOUBLE)"),
        (m.FLOAT, "CAST(t.col AS FLOAT)"),
        argnames="type_,expected",
    )
    @testing.combinations(True, False, argnames="maria_db")
    def test_float_cast(self, type_, expected, maria_db):
        dialect = mysql.dialect()
        if maria_db:
            dialect.is_mariadb = maria_db
            dialect.server_version_info = (10, 4, 5)
            dialect._initialize_mariadb(None)
        else:
            dialect.server_version_info = (8, 0, 17)
            dialect._initialize_mysql(None)
        t = sql.table("t", sql.column("col"))
        self.assert_compile(cast(t.c.col, type_), expected, dialect=dialect)

    def test_cast_grouped_expression_non_castable(self):
        with expect_warnings(
            "Datatype FLOAT does not support CAST on MySQL/MariaDb;"
        ):
            self.assert_compile(
                cast(sql.column("x") + sql.column("y"), Float), "(x + y)"
            )

    def test_extract(self):
        t = sql.table("t", sql.column("col1"))

        for field in "year", "month", "day":
            self.assert_compile(
                select(extract(field, t.c.col1)),
                "SELECT EXTRACT(%s FROM t.col1) AS anon_1 FROM t" % field,
            )

        # milliseconds to millisecond
        self.assert_compile(
            select(extract("milliseconds", t.c.col1)),
            "SELECT EXTRACT(millisecond FROM t.col1) AS anon_1 FROM t",
        )

    def test_too_long_index(self):
        exp = "ix_zyrenian_zyme_zyzzogeton_zyzzogeton_zyrenian_zyme_zyz_5cd2"
        tname = "zyrenian_zyme_zyzzogeton_zyzzogeton"
        cname = "zyrenian_zyme_zyzzogeton_zo"

        t1 = Table(tname, MetaData(), Column(cname, Integer, index=True))
        ix1 = list(t1.indexes)[0]

        self.assert_compile(
            schema.CreateIndex(ix1),
            "CREATE INDEX %s ON %s (%s)" % (exp, tname, cname),
        )

    def test_innodb_autoincrement(self):
        t1 = Table(
            "sometable",
            MetaData(),
            Column(
                "assigned_id", Integer(), primary_key=True, autoincrement=False
            ),
            Column("id", Integer(), primary_key=True, autoincrement=True),
            mysql_engine="InnoDB",
        )
        self.assert_compile(
            schema.CreateTable(t1),
            "CREATE TABLE sometable (assigned_id "
            "INTEGER NOT NULL, id INTEGER NOT NULL "
            "AUTO_INCREMENT, PRIMARY KEY (id, assigned_id)"
            ")ENGINE=InnoDB",
        )

        t1 = Table(
            "sometable",
            MetaData(),
            Column(
                "assigned_id", Integer(), primary_key=True, autoincrement=True
            ),
            Column("id", Integer(), primary_key=True, autoincrement=False),
            mysql_engine="InnoDB",
        )
        self.assert_compile(
            schema.CreateTable(t1),
            "CREATE TABLE sometable (assigned_id "
            "INTEGER NOT NULL AUTO_INCREMENT, id "
            "INTEGER NOT NULL, PRIMARY KEY "
            "(assigned_id, id))ENGINE=InnoDB",
        )

    def test_innodb_autoincrement_reserved_word_column_name(self):
        t1 = Table(
            "sometable",
            MetaData(),
            Column("id", Integer(), primary_key=True, autoincrement=False),
            Column("order", Integer(), primary_key=True, autoincrement=True),
            mysql_engine="InnoDB",
        )
        self.assert_compile(
            schema.CreateTable(t1),
            "CREATE TABLE sometable ("
            "id INTEGER NOT NULL, "
            "`order` INTEGER NOT NULL AUTO_INCREMENT, "
            "PRIMARY KEY (`order`, id)"
            ")ENGINE=InnoDB",
        )

    @testing.combinations(
        (Sequence("foo_seq"), "CREATE SEQUENCE foo_seq"),
        (Sequence("foo_seq", cycle=True), "CREATE SEQUENCE foo_seq CYCLE"),
        (Sequence("foo_seq", cycle=False), "CREATE SEQUENCE foo_seq NOCYCLE"),
        (
            Sequence(
                "foo_seq",
                start=1,
                increment=2,
                nominvalue=True,
                nomaxvalue=True,
                cycle=False,
                cache=100,
            ),
            (
                "CREATE SEQUENCE foo_seq INCREMENT BY 2 START WITH 1 NO"
                " MINVALUE NO MAXVALUE CACHE 100 NOCYCLE"
            ),
        ),
        argnames="seq, expected",
    )
    @testing.variation("use_mariadb", [True, False])
    def test_mariadb_sequence_behaviors(self, seq, expected, use_mariadb):
        """test #13073"""
        self.assert_compile(
            CreateSequence(seq),
            expected,
            dialect="mariadb" if use_mariadb else "mysql",
        )

    def test_create_table_with_partition(self):
        t1 = Table(
            "testtable",
            MetaData(),
            Column("id", Integer(), primary_key=True, autoincrement=True),
            Column(
                "other_id", Integer(), primary_key=True, autoincrement=False
            ),
            mysql_partitions="2",
            mysql_partition_by="KEY(other_id)",
        )
        self.assert_compile(
            schema.CreateTable(t1),
            "CREATE TABLE testtable ("
            "id INTEGER NOT NULL AUTO_INCREMENT, "
            "other_id INTEGER NOT NULL, "
            "PRIMARY KEY (id, other_id)"
            ")PARTITION BY KEY(other_id) PARTITIONS 2",
        )

    def test_create_table_with_subpartition(self):
        t1 = Table(
            "testtable",
            MetaData(),
            Column("id", Integer(), primary_key=True, autoincrement=True),
            Column(
                "other_id", Integer(), primary_key=True, autoincrement=False
            ),
            mysql_partitions="2",
            mysql_partition_by="KEY(other_id)",
            mysql_subpartition_by="HASH(some_expr)",
            mysql_subpartitions="2",
        )
        self.assert_compile(
            schema.CreateTable(t1),
            "CREATE TABLE testtable ("
            "id INTEGER NOT NULL AUTO_INCREMENT, "
            "other_id INTEGER NOT NULL, "
            "PRIMARY KEY (id, other_id)"
            ")PARTITION BY KEY(other_id) PARTITIONS 2 "
            "SUBPARTITION BY HASH(some_expr) SUBPARTITIONS 2",
        )

    def test_create_table_with_partition_hash(self):
        t1 = Table(
            "testtable",
            MetaData(),
            Column("id", Integer(), primary_key=True, autoincrement=True),
            Column(
                "other_id", Integer(), primary_key=True, autoincrement=False
            ),
            mysql_partitions="2",
            mysql_partition_by="HASH(other_id)",
        )
        self.assert_compile(
            schema.CreateTable(t1),
            "CREATE TABLE testtable ("
            "id INTEGER NOT NULL AUTO_INCREMENT, "
            "other_id INTEGER NOT NULL, "
            "PRIMARY KEY (id, other_id)"
            ")PARTITION BY HASH(other_id) PARTITIONS 2",
        )

    def test_create_table_with_partition_and_other_opts(self):
        t1 = Table(
            "testtable",
            MetaData(),
            Column("id", Integer(), primary_key=True, autoincrement=True),
            Column(
                "other_id", Integer(), primary_key=True, autoincrement=False
            ),
            mysql_stats_sample_pages="2",
            mysql_partitions="2",
            mysql_partition_by="HASH(other_id)",
        )
        self.assert_compile(
            schema.CreateTable(t1),
            "CREATE TABLE testtable ("
            "id INTEGER NOT NULL AUTO_INCREMENT, "
            "other_id INTEGER NOT NULL, "
            "PRIMARY KEY (id, other_id)"
            ")STATS_SAMPLE_PAGES=2 PARTITION BY HASH(other_id) PARTITIONS 2",
        )

    def test_create_table_with_collate(self):
        # issue #5411
        t1 = Table(
            "testtable",
            MetaData(),
            Column("id", Integer(), primary_key=True, autoincrement=True),
            mysql_engine="InnoDB",
            mysql_collate="utf8_icelandic_ci",
            mysql_charset="utf8",
        )
        first_part = (
            "CREATE TABLE testtable ("
            "id INTEGER NOT NULL AUTO_INCREMENT, "
            "PRIMARY KEY (id))"
        )
        try:
            self.assert_compile(
                schema.CreateTable(t1),
                first_part
                + "ENGINE=InnoDB CHARSET=utf8 COLLATE utf8_icelandic_ci",
            )
        except AssertionError:
            self.assert_compile(
                schema.CreateTable(t1),
                first_part
                + "CHARSET=utf8 ENGINE=InnoDB COLLATE utf8_icelandic_ci",
            )

    def test_inner_join(self):
        t1 = table("t1", column("x"))
        t2 = table("t2", column("y"))

        self.assert_compile(
            t1.join(t2, t1.c.x == t2.c.y), "t1 INNER JOIN t2 ON t1.x = t2.y"
        )

    def test_outer_join(self):
        t1 = table("t1", column("x"))
        t2 = table("t2", column("y"))

        self.assert_compile(
            t1.outerjoin(t2, t1.c.x == t2.c.y),
            "t1 LEFT OUTER JOIN t2 ON t1.x = t2.y",
        )

    def test_full_outer_join(self):
        t1 = table("t1", column("x"))
        t2 = table("t2", column("y"))

        self.assert_compile(
            t1.outerjoin(t2, t1.c.x == t2.c.y, full=True),
            "t1 FULL OUTER JOIN t2 ON t1.x = t2.y",
        )


class InsertOnDuplicateTest(fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = mysql.dialect()

    def setup_test(self):
        self.table = Table(
            "foos",
            MetaData(),
            Column("id", Integer, primary_key=True),
            Column("bar", String(10)),
            Column("baz", String(10)),
        )

    def test_no_call_twice(self):
        stmt = insert(self.table).values(
            [{"id": 1, "bar": "ab"}, {"id": 2, "bar": "b"}]
        )
        stmt = stmt.on_duplicate_key_update(
            bar=stmt.inserted.bar, baz=stmt.inserted.baz
        )
        with testing.expect_raises_message(
            exc.InvalidRequestError,
            "This Insert construct already has an "
            "ON DUPLICATE KEY clause present",
        ):
            stmt = stmt.on_duplicate_key_update(
                bar=stmt.inserted.bar, baz=stmt.inserted.baz
            )

    @testing.variation("version", ["mysql8", "all_others"])
    def test_from_values(self, version: Variation):
        stmt = insert(self.table).values(
            [{"id": 1, "bar": "ab"}, {"id": 2, "bar": "b"}]
        )
        stmt = stmt.on_duplicate_key_update(
            bar=stmt.inserted.bar, baz=stmt.inserted.baz
        )

        if version.all_others:
            expected_sql = (
                "INSERT INTO foos (id, bar) VALUES (%s, %s), (%s, %s) "
                "ON DUPLICATE KEY UPDATE bar = VALUES(bar), baz = VALUES(baz)"
            )
            dialect = None
        elif version.mysql8:
            expected_sql = (
                "INSERT INTO foos (id, bar) VALUES (%s, %s), (%s, %s) "
                "AS new ON DUPLICATE KEY UPDATE "
                "bar = new.bar, "
                "baz = new.baz"
            )
            dialect = mysql.dialect()
            dialect._requires_alias_for_on_duplicate_key = True
        else:
            version.fail()

        self.assert_compile(stmt, expected_sql, dialect=dialect)

    @testing.variation("version", ["mysql8", "all_others"])
    def test_from_select(self, version: Variation):
        stmt = insert(self.table).from_select(
            ["id", "bar"],
            select(self.table.c.id, literal("bar2")),
        )
        stmt = stmt.on_duplicate_key_update(
            bar=stmt.inserted.bar, baz=stmt.inserted.baz
        )

        expected_sql = (
            "INSERT INTO foos (id, bar) SELECT foos.id, %s AS anon_1 "
            "FROM foos "
            "ON DUPLICATE KEY UPDATE bar = VALUES(bar), baz = VALUES(baz)"
        )
        if version.all_others:
            dialect = None
        elif version.mysql8:
            dialect = mysql.dialect()
            dialect._requires_alias_for_on_duplicate_key = True
        else:
            version.fail()

        self.assert_compile(stmt, expected_sql, dialect=dialect)

    def test_from_literal(self):
        stmt = insert(self.table).values(
            [{"id": 1, "bar": "ab"}, {"id": 2, "bar": "b"}]
        )
        stmt = stmt.on_duplicate_key_update(bar=literal_column("bb"))
        expected_sql = (
            "INSERT INTO foos (id, bar) VALUES (%s, %s), (%s, %s) "
            "ON DUPLICATE KEY UPDATE bar = bb"
        )
        self.assert_compile(stmt, expected_sql)

    def test_python_values(self):
        stmt = insert(self.table).values(
            [{"id": 1, "bar": "ab"}, {"id": 2, "bar": "b"}]
        )
        stmt = stmt.on_duplicate_key_update(bar="foobar")
        expected_sql = (
            "INSERT INTO foos (id, bar) VALUES (%s, %s), (%s, %s) "
            "ON DUPLICATE KEY UPDATE bar = %s"
        )
        self.assert_compile(stmt, expected_sql)

    @testing.variation("version", ["mysql8", "all_others"])
    def test_update_sql_expr(self, version: Variation):
        stmt = insert(self.table).values(
            [{"id": 1, "bar": "ab"}, {"id": 2, "bar": "b"}]
        )
        stmt = stmt.on_duplicate_key_update(
            bar=func.coalesce(stmt.inserted.bar),
            baz=stmt.inserted.baz + "some literal" + stmt.inserted.bar,
        )

        if version.all_others:
            expected_sql = (
                "INSERT INTO foos (id, bar) VALUES (%s, %s), (%s, %s) ON "
                "DUPLICATE KEY UPDATE bar = coalesce(VALUES(bar)), "
                "baz = (concat(VALUES(baz), %s, VALUES(bar)))"
            )
            dialect = None
        elif version.mysql8:
            expected_sql = (
                "INSERT INTO foos (id, bar) VALUES (%s, %s), (%s, %s) "
                "AS new ON DUPLICATE KEY UPDATE bar = "
                "coalesce(new.bar), "
                "baz = (concat(new.baz, %s, "
                "new.bar))"
            )
            dialect = mysql.dialect()
            dialect._requires_alias_for_on_duplicate_key = True
        else:
            version.fail()

        self.assert_compile(
            stmt,
            expected_sql,
            checkparams={
                "id_m0": 1,
                "bar_m0": "ab",
                "id_m1": 2,
                "bar_m1": "b",
                "baz_1": "some literal",
            },
            dialect=dialect,
        )

    def test_mysql8_on_update_dont_dup_alias_name(self):
        t = table("new", column("id"), column("bar"), column("baz"))
        stmt = insert(t).values(
            [{"id": 1, "bar": "ab"}, {"id": 2, "bar": "b"}]
        )
        stmt = stmt.on_duplicate_key_update(
            bar=func.coalesce(stmt.inserted.bar),
            baz=stmt.inserted.baz + "some literal" + stmt.inserted.bar,
        )

        expected_sql = (
            "INSERT INTO new (id, bar) VALUES (%s, %s), (%s, %s) "
            "AS new_1 ON DUPLICATE KEY UPDATE bar = "
            "coalesce(new_1.bar), "
            "baz = (concat(new_1.baz, %s, "
            "new_1.bar))"
        )
        dialect = mysql.dialect()
        dialect._requires_alias_for_on_duplicate_key = True
        self.assert_compile(
            stmt,
            expected_sql,
            checkparams={
                "id_m0": 1,
                "bar_m0": "ab",
                "id_m1": 2,
                "bar_m1": "b",
                "baz_1": "some literal",
            },
            dialect=dialect,
        )

    def test_on_update_instrumented_attribute_dict(self):
        class Base(DeclarativeBase):
            pass

        class T(Base):
            __tablename__ = "table"

            foo: Mapped[int] = mapped_column(Integer, primary_key=True)

        q = insert(T).values(foo=1).on_duplicate_key_update({T.foo: 2})
        self.assert_compile(
            q,
            (
                "INSERT INTO `table` (foo) VALUES (%s) "
                "ON DUPLICATE KEY UPDATE foo = %s"
            ),
            {"foo": 1, "param_1": 2},
        )


class RegexpCommon(testing.AssertsCompiledSQL):
    def setup_test(self):
        self.table = table(
            "mytable", column("myid", String), column("name", String)
        )

    def test_regexp_match(self):
        self.assert_compile(
            self.table.c.myid.regexp_match("pattern"),
            "mytable.myid REGEXP %s",
            checkpositional=("pattern",),
        )

    def test_regexp_match_column(self):
        self.assert_compile(
            self.table.c.myid.regexp_match(self.table.c.name),
            "mytable.myid REGEXP mytable.name",
            checkpositional=(),
        )

    def test_regexp_match_str(self):
        self.assert_compile(
            literal("string").regexp_match(self.table.c.name),
            "%s REGEXP mytable.name",
            checkpositional=("string",),
        )

    def test_not_regexp_match(self):
        self.assert_compile(
            ~self.table.c.myid.regexp_match("pattern"),
            "mytable.myid NOT REGEXP %s",
            checkpositional=("pattern",),
        )

    def test_not_regexp_match_column(self):
        self.assert_compile(
            ~self.table.c.myid.regexp_match(self.table.c.name),
            "mytable.myid NOT REGEXP mytable.name",
            checkpositional=(),
        )

    def test_not_regexp_match_str(self):
        self.assert_compile(
            ~literal("string").regexp_match(self.table.c.name),
            "%s NOT REGEXP mytable.name",
            checkpositional=("string",),
        )

    def test_regexp_replace(self):
        self.assert_compile(
            self.table.c.myid.regexp_replace("pattern", "replacement"),
            "REGEXP_REPLACE(mytable.myid, %s, %s)",
            checkpositional=("pattern", "replacement"),
        )

    def test_regexp_replace_column(self):
        self.assert_compile(
            self.table.c.myid.regexp_replace("pattern", self.table.c.name),
            "REGEXP_REPLACE(mytable.myid, %s, mytable.name)",
            checkpositional=("pattern",),
        )

    def test_regexp_replace_column2(self):
        self.assert_compile(
            self.table.c.myid.regexp_replace(self.table.c.name, "replacement"),
            "REGEXP_REPLACE(mytable.myid, mytable.name, %s)",
            checkpositional=("replacement",),
        )

    def test_regexp_replace_string(self):
        self.assert_compile(
            literal("string").regexp_replace("pattern", self.table.c.name),
            "REGEXP_REPLACE(%s, %s, mytable.name)",
            checkpositional=("string", "pattern"),
        )


class RegexpTestMySql(fixtures.TestBase, RegexpCommon):
    __dialect__ = "mysql"

    def test_regexp_match_flags_safestring(self):
        self.assert_compile(
            self.table.c.myid.regexp_match("pattern", flags="i'g"),
            "REGEXP_LIKE(mytable.myid, %s, 'i''g')",
            checkpositional=("pattern",),
        )

    def test_regexp_match_flags(self):
        self.assert_compile(
            self.table.c.myid.regexp_match("pattern", flags="ig"),
            "REGEXP_LIKE(mytable.myid, %s, 'ig')",
            checkpositional=("pattern",),
        )

    def test_not_regexp_match_flags(self):
        self.assert_compile(
            ~self.table.c.myid.regexp_match("pattern", flags="ig"),
            "NOT REGEXP_LIKE(mytable.myid, %s, 'ig')",
            checkpositional=("pattern",),
        )

    def test_regexp_replace_flags(self):
        self.assert_compile(
            self.table.c.myid.regexp_replace(
                "pattern", "replacement", flags="ig"
            ),
            "REGEXP_REPLACE(mytable.myid, %s, %s, 'ig')",
            checkpositional=("pattern", "replacement"),
        )

    def test_regexp_replace_flags_safestring(self):
        self.assert_compile(
            self.table.c.myid.regexp_replace(
                "pattern", "replacement", flags="i'g"
            ),
            "REGEXP_REPLACE(mytable.myid, %s, %s, 'i''g')",
            checkpositional=("pattern", "replacement"),
        )


class RegexpTestMariaDb(fixtures.TestBase, RegexpCommon):
    __dialect__ = "mariadb"

    def test_regexp_match_flags_safestring(self):
        self.assert_compile(
            self.table.c.myid.regexp_match("pattern", flags="i'g"),
            "mytable.myid REGEXP CONCAT('(?', 'i''g', ')', %s)",
            checkpositional=("pattern",),
        )

    def test_regexp_match_flags(self):
        self.assert_compile(
            self.table.c.myid.regexp_match("pattern", flags="ig"),
            "mytable.myid REGEXP CONCAT('(?', 'ig', ')', %s)",
            checkpositional=("pattern",),
        )

    def test_not_regexp_match_flags(self):
        self.assert_compile(
            ~self.table.c.myid.regexp_match("pattern", flags="ig"),
            "mytable.myid NOT REGEXP CONCAT('(?', 'ig', ')', %s)",
            checkpositional=("pattern",),
        )

    def test_regexp_replace_flags(self):
        self.assert_compile(
            self.table.c.myid.regexp_replace(
                "pattern", "replacement", flags="ig"
            ),
            "REGEXP_REPLACE(mytable.myid, CONCAT('(?', 'ig', ')', %s), %s)",
            checkpositional=("pattern", "replacement"),
        )


class MatchExpressionTest(fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = mysql.dialect()

    match_table = table(
        "user",
        column("firstname", String),
        column("lastname", String),
    )

    @testing.combinations(
        (
            lambda title: title.match("somstr", mysql_boolean_mode=False),
            "MATCH (matchtable.title) AGAINST (%s)",
        ),
        (
            lambda title: title.match(
                "somstr",
                mysql_boolean_mode=False,
                mysql_natural_language=True,
            ),
            "MATCH (matchtable.title) AGAINST (%s IN NATURAL LANGUAGE MODE)",
        ),
        (
            lambda title: title.match(
                "somstr",
                mysql_boolean_mode=False,
                mysql_query_expansion=True,
            ),
            "MATCH (matchtable.title) AGAINST (%s WITH QUERY EXPANSION)",
        ),
        (
            lambda title: title.match(
                "somstr",
                mysql_boolean_mode=False,
                mysql_natural_language=True,
                mysql_query_expansion=True,
            ),
            "MATCH (matchtable.title) AGAINST "
            "(%s IN NATURAL LANGUAGE MODE WITH QUERY EXPANSION)",
        ),
    )
    def test_match_expression_single_col(self, case, expected):
        matchtable = table("matchtable", column("title", String))
        title = matchtable.c.title

        expr = case(title)
        self.assert_compile(expr, expected)

    @testing.combinations(
        (
            lambda expr: expr,
            "MATCH (user.firstname, user.lastname) AGAINST (%s)",
        ),
        (
            lambda expr: expr.in_boolean_mode(),
            "MATCH (user.firstname, user.lastname) AGAINST "
            "(%s IN BOOLEAN MODE)",
        ),
        (
            lambda expr: expr.in_natural_language_mode(),
            "MATCH (user.firstname, user.lastname) AGAINST "
            "(%s IN NATURAL LANGUAGE MODE)",
        ),
        (
            lambda expr: expr.with_query_expansion(),
            "MATCH (user.firstname, user.lastname) AGAINST "
            "(%s WITH QUERY EXPANSION)",
        ),
        (
            lambda expr: (
                expr.in_natural_language_mode().with_query_expansion()
            ),
            "MATCH (user.firstname, user.lastname) AGAINST "
            "(%s IN NATURAL LANGUAGE MODE WITH QUERY EXPANSION)",
        ),
    )
    def test_match_expression_multiple_cols(self, case, expected):
        firstname = self.match_table.c.firstname
        lastname = self.match_table.c.lastname

        expr = match(firstname, lastname, against="Firstname Lastname")

        expr = case(expr)
        self.assert_compile(expr, expected)

    @testing.combinations(
        (bindparam("against_expr"), "%s"),
        (
            column("some col") + column("some other col"),
            "`some col` + `some other col`",
        ),
        (column("some col") + bindparam("against_expr"), "`some col` + %s"),
    )
    def test_match_expression_against_expr(self, against, expected_segment):
        firstname = self.match_table.c.firstname
        lastname = self.match_table.c.lastname

        expr = match(firstname, lastname, against=against)

        expected = (
            "MATCH (user.firstname, user.lastname) AGAINST (%s)"
            % expected_segment
        )
        self.assert_compile(expr, expected)

    def test_cols_required(self):
        assert_raises_message(
            exc.ArgumentError,
            "columns are required",
            match,
            against="Firstname Lastname",
        )

    @testing.combinations(
        (True, False, True), (True, True, False), (True, True, True)
    )
    def test_invalid_combinations(
        self, boolean_mode, natural_language, query_expansion
    ):
        firstname = self.match_table.c.firstname
        lastname = self.match_table.c.lastname

        assert_raises_message(
            exc.ArgumentError,
            "columns are required",
            match,
            against="Firstname Lastname",
        )

        expr = match(
            firstname,
            lastname,
            against="Firstname Lastname",
            in_boolean_mode=boolean_mode,
            in_natural_language_mode=natural_language,
            with_query_expansion=query_expansion,
        )
        msg = (
            "Invalid MySQL match flags: "
            "in_boolean_mode=%s, "
            "in_natural_language_mode=%s, "
            "with_query_expansion=%s"
        ) % (boolean_mode, natural_language, query_expansion)

        assert_raises_message(
            exc.CompileError,
            msg,
            expr.compile,
            dialect=self.__dialect__,
        )

    def test_match_operator(self):
        matchtable = table("matchtable", column("title", String))
        self.assert_compile(
            matchtable.c.title.match("somstr"),
            "MATCH (matchtable.title) AGAINST (%s IN BOOLEAN MODE)",
        )

    def test_literal_binds(self):
        expr = literal("x").match(literal("y"))
        self.assert_compile(
            expr,
            "MATCH ('x') AGAINST ('y' IN BOOLEAN MODE)",
            literal_binds=True,
        )

    def test_char_zero(self):
        """test #9544"""

        t1 = Table(
            "sometable",
            MetaData(),
            Column("a", CHAR(0)),
            Column("b", VARCHAR(0)),
            Column("c", String(0)),
            Column("d", NVARCHAR(0)),
            Column("e", NCHAR(0)),
            Column("f", TEXT(0)),
            Column("g", Text(0)),
            Column("h", BLOB(0)),
            Column("i", LargeBinary(0)),
        )
        self.assert_compile(
            schema.CreateTable(t1),
            "CREATE TABLE sometable (a CHAR(0), b VARCHAR(0), "
            "c VARCHAR(0), d NATIONAL VARCHAR(0), e NATIONAL CHAR(0), "
            "f TEXT(0), g TEXT(0), h BLOB(0), i BLOB(0))",
        )
