# coding: utf-8

from sqlalchemy import BLOB
from sqlalchemy import BOOLEAN
from sqlalchemy import Boolean
from sqlalchemy import cast
from sqlalchemy import CHAR
from sqlalchemy import CheckConstraint
from sqlalchemy import CLOB
from sqlalchemy import Column
from sqlalchemy import Computed
from sqlalchemy import DATE
from sqlalchemy import Date
from sqlalchemy import DATETIME
from sqlalchemy import DateTime
from sqlalchemy import DECIMAL
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
from sqlalchemy import SmallInteger
from sqlalchemy import sql
from sqlalchemy import String
from sqlalchemy import Table
from sqlalchemy import testing
from sqlalchemy import TEXT
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
from sqlalchemy.sql import column
from sqlalchemy.sql import table
from sqlalchemy.sql.expression import literal_column
from sqlalchemy.testing import assert_raises_message
from sqlalchemy.testing import AssertsCompiledSQL
from sqlalchemy.testing import eq_
from sqlalchemy.testing import expect_warnings
from sqlalchemy.testing import fixtures


class CompileTest(fixtures.TestBase, AssertsCompiledSQL):

    __dialect__ = mysql.dialect()

    def test_reserved_words(self):
        table = Table(
            "mysql_table",
            MetaData(),
            Column("col1", Integer),
            Column("master_ssl_verify_server_cert", Integer),
        )
        x = select(table.c.col1, table.c.master_ssl_verify_server_cert)

        self.assert_compile(
            x,
            "SELECT mysql_table.col1, "
            "mysql_table.`master_ssl_verify_server_cert` FROM mysql_table",
        )

    def test_create_index_simple(self):
        m = MetaData()
        tbl = Table("testtbl", m, Column("data", String(255)))
        idx = Index("test_idx1", tbl.c.data)

        self.assert_compile(
            schema.CreateIndex(idx), "CREATE INDEX test_idx1 ON testtbl (data)"
        )

    def test_create_index_with_prefix(self):
        m = MetaData()
        tbl = Table("testtbl", m, Column("data", String(255)))
        idx = Index(
            "test_idx1", tbl.c.data, mysql_length=10, mysql_prefix="FULLTEXT"
        )

        self.assert_compile(
            schema.CreateIndex(idx),
            "CREATE FULLTEXT INDEX test_idx1 " "ON testtbl (data(10))",
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

    def test_create_index_with_parser(self):
        m = MetaData()
        tbl = Table("testtbl", m, Column("data", String(255)))
        idx = Index(
            "test_idx1",
            tbl.c.data,
            mysql_length=10,
            mysql_prefix="FULLTEXT",
            mysql_with_parser="ngram",
        )

        self.assert_compile(
            schema.CreateIndex(idx),
            "CREATE FULLTEXT INDEX test_idx1 "
            "ON testtbl (data(10)) WITH PARSER ngram",
        )

    def test_create_index_with_length(self):
        m = MetaData()
        tbl = Table("testtbl", m, Column("data", String(255)))
        idx1 = Index("test_idx1", tbl.c.data, mysql_length=10)
        idx2 = Index("test_idx2", tbl.c.data, mysql_length=5)

        self.assert_compile(
            schema.CreateIndex(idx1),
            "CREATE INDEX test_idx1 ON testtbl (data(10))",
        )
        self.assert_compile(
            schema.CreateIndex(idx2),
            "CREATE INDEX test_idx2 ON testtbl (data(5))",
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

    def test_create_index_with_using(self):
        m = MetaData()
        tbl = Table("testtbl", m, Column("data", String(255)))
        idx1 = Index("test_idx1", tbl.c.data, mysql_using="btree")
        idx2 = Index("test_idx2", tbl.c.data, mysql_using="hash")

        self.assert_compile(
            schema.CreateIndex(idx1),
            "CREATE INDEX test_idx1 ON testtbl (data) USING btree",
        )
        self.assert_compile(
            schema.CreateIndex(idx2),
            "CREATE INDEX test_idx2 ON testtbl (data) USING hash",
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

    def test_create_pk_with_using(self):
        m = MetaData()
        tbl = Table(
            "testtbl",
            m,
            Column("data", String(255)),
            PrimaryKeyConstraint("data", mysql_using="btree"),
        )

        self.assert_compile(
            schema.CreateTable(tbl),
            "CREATE TABLE testtbl (data VARCHAR(255) NOT NULL, "
            "PRIMARY KEY (data) USING btree)",
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

    def test_match(self):
        matchtable = table("matchtable", column("title", String))
        self.assert_compile(
            matchtable.c.title.match("somstr"),
            "MATCH (matchtable.title) AGAINST (%s IN BOOLEAN MODE)",
        )

    def test_match_compile_kw(self):
        expr = literal("x").match(literal("y"))
        self.assert_compile(
            expr,
            "MATCH ('x') AGAINST ('y' IN BOOLEAN MODE)",
            literal_binds=True,
        )

    def test_concat_compile_kw(self):
        expr = literal("x", type_=String) + literal("y", type_=String)
        self.assert_compile(expr, "concat('x', 'y')", literal_binds=True)

    def test_mariadb_for_update(self):
        table1 = table(
            "mytable", column("myid"), column("name"), column("description")
        )

        self.assert_compile(
            table1.select(table1.c.myid == 7).with_for_update(of=table1),
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


class SQLTest(fixtures.TestBase, AssertsCompiledSQL):

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

    def test_update_limit(self):
        t = sql.table("t", sql.column("col1"), sql.column("col2"))

        self.assert_compile(
            t.update(values={"col1": 123}), "UPDATE t SET col1=%s"
        )
        self.assert_compile(
            t.update()
            .values({"col1": 123})
            .with_dialect_options(mysql_limit=5),
            "UPDATE t SET col1=%s LIMIT 5",
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
        else:
            dialect.server_version_info = (8, 0, 17)
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

        # millsecondS to millisecond
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
            "CREATE INDEX %s " "ON %s (%s)" % (exp, tname, cname),
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

    def test_from_values(self):
        stmt = insert(self.table).values(
            [{"id": 1, "bar": "ab"}, {"id": 2, "bar": "b"}]
        )
        stmt = stmt.on_duplicate_key_update(
            bar=stmt.inserted.bar, baz=stmt.inserted.baz
        )
        expected_sql = (
            "INSERT INTO foos (id, bar) VALUES (%s, %s), (%s, %s) "
            "ON DUPLICATE KEY UPDATE bar = VALUES(bar), baz = VALUES(baz)"
        )
        self.assert_compile(stmt, expected_sql)

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

    def test_update_sql_expr(self):
        stmt = insert(self.table).values(
            [{"id": 1, "bar": "ab"}, {"id": 2, "bar": "b"}]
        )
        stmt = stmt.on_duplicate_key_update(
            bar=func.coalesce(stmt.inserted.bar),
            baz=stmt.inserted.baz + "some literal",
        )
        expected_sql = (
            "INSERT INTO foos (id, bar) VALUES (%s, %s), (%s, %s) ON "
            "DUPLICATE KEY UPDATE bar = coalesce(VALUES(bar)), "
            "baz = (concat(VALUES(baz), %s))"
        )
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
        )


class RegexpCommon(testing.AssertsCompiledSQL):
    def setup_test(self):
        self.table = table(
            "mytable", column("myid", Integer), column("name", String)
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

    def test_regexp_match_flags(self):
        self.assert_compile(
            self.table.c.myid.regexp_match("pattern", flags="ig"),
            "REGEXP_LIKE(mytable.myid, %s, %s)",
            checkpositional=("pattern", "ig"),
        )

    def test_not_regexp_match_flags(self):
        self.assert_compile(
            ~self.table.c.myid.regexp_match("pattern", flags="ig"),
            "NOT REGEXP_LIKE(mytable.myid, %s, %s)",
            checkpositional=("pattern", "ig"),
        )

    def test_regexp_replace_flags(self):
        self.assert_compile(
            self.table.c.myid.regexp_replace(
                "pattern", "replacement", flags="ig"
            ),
            "REGEXP_REPLACE(mytable.myid, %s, %s, %s)",
            checkpositional=("pattern", "replacement", "ig"),
        )


class RegexpTestMariaDb(fixtures.TestBase, RegexpCommon):
    __dialect__ = "mariadb"

    def test_regexp_match_flags(self):
        self.assert_compile(
            self.table.c.myid.regexp_match("pattern", flags="ig"),
            "mytable.myid REGEXP CONCAT('(?', %s, ')', %s)",
            checkpositional=("ig", "pattern"),
        )

    def test_not_regexp_match_flags(self):
        self.assert_compile(
            ~self.table.c.myid.regexp_match("pattern", flags="ig"),
            "mytable.myid NOT REGEXP CONCAT('(?', %s, ')', %s)",
            checkpositional=("ig", "pattern"),
        )

    def test_regexp_replace_flags(self):
        self.assert_compile(
            self.table.c.myid.regexp_replace(
                "pattern", "replacement", flags="ig"
            ),
            "REGEXP_REPLACE(mytable.myid, CONCAT('(?', %s, ')', %s), %s)",
            checkpositional=("ig", "pattern", "replacement"),
        )
