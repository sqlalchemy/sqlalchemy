# -*- encoding: utf-8
from sqlalchemy import bindparam
from sqlalchemy import Column
from sqlalchemy import Computed
from sqlalchemy import delete
from sqlalchemy import exc
from sqlalchemy import extract
from sqlalchemy import func
from sqlalchemy import Identity
from sqlalchemy import Index
from sqlalchemy import insert
from sqlalchemy import Integer
from sqlalchemy import literal
from sqlalchemy import literal_column
from sqlalchemy import MetaData
from sqlalchemy import PrimaryKeyConstraint
from sqlalchemy import schema
from sqlalchemy import select
from sqlalchemy import sql
from sqlalchemy import String
from sqlalchemy import Table
from sqlalchemy import testing
from sqlalchemy import text
from sqlalchemy import union
from sqlalchemy import UniqueConstraint
from sqlalchemy import update
from sqlalchemy.dialects import mssql
from sqlalchemy.dialects.mssql import base as mssql_base
from sqlalchemy.dialects.mssql import mxodbc
from sqlalchemy.dialects.mssql.base import try_cast
from sqlalchemy.sql import column
from sqlalchemy.sql import quoted_name
from sqlalchemy.sql import table
from sqlalchemy.testing import assert_raises_message
from sqlalchemy.testing import AssertsCompiledSQL
from sqlalchemy.testing import eq_
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import is_

tbl = table("t", column("a"))


class CompileTest(fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = mssql.dialect()

    @testing.fixture
    def dialect_2012(self):
        dialect = mssql.dialect()
        dialect._supports_offset_fetch = True
        return dialect

    def test_true_false(self):
        self.assert_compile(sql.false(), "0")
        self.assert_compile(sql.true(), "1")

    @testing.combinations(
        ("plain", "sometable", "sometable"),
        ("matched_square_brackets", "colo[u]r", "[colo[u]]r]"),
        ("unmatched_left_square_bracket", "colo[ur", "[colo[ur]"),
        ("unmatched_right_square_bracket", "colou]r", "[colou]]r]"),
        ("double quotes", 'Edwin "Buzz" Aldrin', '[Edwin "Buzz" Aldrin]'),
        ("dash", "Dash-8", "[Dash-8]"),
        ("slash", "tl/dr", "[tl/dr]"),
        ("space", "Red Deer", "[Red Deer]"),
        ("question mark", "OK?", "[OK?]"),
        ("percent", "GST%", "[GST%]"),
        id_="iaa",
    )
    def test_identifier_rendering(self, table_name, rendered_name):
        t = table(table_name, column("somecolumn"))
        self.assert_compile(
            t.select(), "SELECT {0}.somecolumn FROM {0}".format(rendered_name)
        )

    def test_select_with_nolock(self):
        t = table("sometable", column("somecolumn"))
        self.assert_compile(
            t.select().with_hint(t, "WITH (NOLOCK)"),
            "SELECT sometable.somecolumn FROM sometable WITH (NOLOCK)",
        )

    def test_select_with_nolock_schema(self):
        m = MetaData()
        t = Table(
            "sometable", m, Column("somecolumn", Integer), schema="test_schema"
        )
        self.assert_compile(
            t.select().with_hint(t, "WITH (NOLOCK)"),
            "SELECT test_schema.sometable.somecolumn "
            "FROM test_schema.sometable WITH (NOLOCK)",
        )

    def test_select_w_order_by_collate(self):
        m = MetaData()
        t = Table("sometable", m, Column("somecolumn", String))

        self.assert_compile(
            select(t).order_by(
                t.c.somecolumn.collate("Latin1_General_CS_AS_KS_WS_CI").asc()
            ),
            "SELECT sometable.somecolumn FROM sometable "
            "ORDER BY sometable.somecolumn COLLATE "
            "Latin1_General_CS_AS_KS_WS_CI ASC",
        )

    def test_join_with_hint(self):
        t1 = table(
            "t1",
            column("a", Integer),
            column("b", String),
            column("c", String),
        )
        t2 = table(
            "t2",
            column("a", Integer),
            column("b", Integer),
            column("c", Integer),
        )
        join = (
            t1.join(t2, t1.c.a == t2.c.a)
            .select()
            .with_hint(t1, "WITH (NOLOCK)")
        )
        self.assert_compile(
            join,
            "SELECT t1.a, t1.b, t1.c, t2.a AS a_1, t2.b AS b_1, t2.c AS c_1 "
            "FROM t1 WITH (NOLOCK) JOIN t2 ON t1.a = t2.a",
        )

    def test_insert(self):
        t = table("sometable", column("somecolumn"))
        self.assert_compile(
            t.insert(),
            "INSERT INTO sometable (somecolumn) VALUES " "(:somecolumn)",
        )

    def test_update(self):
        t = table("sometable", column("somecolumn"))
        self.assert_compile(
            t.update(t.c.somecolumn == 7),
            "UPDATE sometable SET somecolumn=:somecolum"
            "n WHERE sometable.somecolumn = "
            ":somecolumn_1",
            dict(somecolumn=10),
        )

    def test_insert_hint(self):
        t = table("sometable", column("somecolumn"))
        for targ in (None, t):
            for darg in ("*", "mssql"):
                self.assert_compile(
                    t.insert()
                    .values(somecolumn="x")
                    .with_hint(
                        "WITH (PAGLOCK)", selectable=targ, dialect_name=darg
                    ),
                    "INSERT INTO sometable WITH (PAGLOCK) "
                    "(somecolumn) VALUES (:somecolumn)",
                )

    def test_update_hint(self):
        t = table("sometable", column("somecolumn"))
        for targ in (None, t):
            for darg in ("*", "mssql"):
                self.assert_compile(
                    t.update()
                    .where(t.c.somecolumn == "q")
                    .values(somecolumn="x")
                    .with_hint(
                        "WITH (PAGLOCK)", selectable=targ, dialect_name=darg
                    ),
                    "UPDATE sometable WITH (PAGLOCK) "
                    "SET somecolumn=:somecolumn "
                    "WHERE sometable.somecolumn = :somecolumn_1",
                )

    def test_update_exclude_hint(self):
        t = table("sometable", column("somecolumn"))
        self.assert_compile(
            t.update()
            .where(t.c.somecolumn == "q")
            .values(somecolumn="x")
            .with_hint("XYZ", "mysql"),
            "UPDATE sometable SET somecolumn=:somecolumn "
            "WHERE sometable.somecolumn = :somecolumn_1",
        )

    def test_delete_hint(self):
        t = table("sometable", column("somecolumn"))
        for targ in (None, t):
            for darg in ("*", "mssql"):
                self.assert_compile(
                    t.delete()
                    .where(t.c.somecolumn == "q")
                    .with_hint(
                        "WITH (PAGLOCK)", selectable=targ, dialect_name=darg
                    ),
                    "DELETE FROM sometable WITH (PAGLOCK) "
                    "WHERE sometable.somecolumn = :somecolumn_1",
                )

    def test_delete_exclude_hint(self):
        t = table("sometable", column("somecolumn"))
        self.assert_compile(
            t.delete()
            .where(t.c.somecolumn == "q")
            .with_hint("XYZ", dialect_name="mysql"),
            "DELETE FROM sometable WHERE "
            "sometable.somecolumn = :somecolumn_1",
        )

    def test_delete_extra_froms(self):
        t1 = table("t1", column("c1"))
        t2 = table("t2", column("c1"))
        q = sql.delete(t1).where(t1.c.c1 == t2.c.c1)
        self.assert_compile(
            q, "DELETE FROM t1 FROM t1, t2 WHERE t1.c1 = t2.c1"
        )

    def test_delete_extra_froms_alias(self):
        a1 = table("t1", column("c1")).alias("a1")
        t2 = table("t2", column("c1"))
        q = sql.delete(a1).where(a1.c.c1 == t2.c.c1)
        self.assert_compile(
            q, "DELETE FROM a1 FROM t1 AS a1, t2 WHERE a1.c1 = t2.c1"
        )
        self.assert_compile(sql.delete(a1), "DELETE FROM t1 AS a1")

    def test_update_from(self):
        metadata = MetaData()
        table1 = Table(
            "mytable",
            metadata,
            Column("myid", Integer),
            Column("name", String(30)),
            Column("description", String(50)),
        )
        table2 = Table(
            "myothertable",
            metadata,
            Column("otherid", Integer),
            Column("othername", String(30)),
        )

        mt = table1.alias()

        u = (
            table1.update()
            .values(name="foo")
            .where(table2.c.otherid == table1.c.myid)
        )

        # testing mssql.base.MSSQLCompiler.update_from_clause
        self.assert_compile(
            u,
            "UPDATE mytable SET name=:name "
            "FROM mytable, myothertable WHERE "
            "myothertable.otherid = mytable.myid",
        )

        self.assert_compile(
            u.where(table2.c.othername == mt.c.name),
            "UPDATE mytable SET name=:name "
            "FROM mytable, myothertable, mytable AS mytable_1 "
            "WHERE myothertable.otherid = mytable.myid "
            "AND myothertable.othername = mytable_1.name",
        )

    def test_update_from_hint(self):
        t = table("sometable", column("somecolumn"))
        t2 = table("othertable", column("somecolumn"))
        for darg in ("*", "mssql"):
            self.assert_compile(
                t.update()
                .where(t.c.somecolumn == t2.c.somecolumn)
                .values(somecolumn="x")
                .with_hint("WITH (PAGLOCK)", selectable=t2, dialect_name=darg),
                "UPDATE sometable SET somecolumn=:somecolumn "
                "FROM sometable, othertable WITH (PAGLOCK) "
                "WHERE sometable.somecolumn = othertable.somecolumn",
            )

    def test_update_to_select_schema(self):
        meta = MetaData()
        table = Table(
            "sometable",
            meta,
            Column("sym", String),
            Column("val", Integer),
            schema="schema",
        )
        other = Table(
            "#other", meta, Column("sym", String), Column("newval", Integer)
        )
        stmt = table.update().values(
            val=select(other.c.newval)
            .where(table.c.sym == other.c.sym)
            .scalar_subquery()
        )

        self.assert_compile(
            stmt,
            "UPDATE [schema].sometable SET val="
            "(SELECT [#other].newval FROM [#other] "
            "WHERE [schema].sometable.sym = [#other].sym)",
        )

        stmt = (
            table.update()
            .values(val=other.c.newval)
            .where(table.c.sym == other.c.sym)
        )
        self.assert_compile(
            stmt,
            "UPDATE [schema].sometable SET val="
            "[#other].newval FROM [schema].sometable, "
            "[#other] WHERE [schema].sometable.sym = [#other].sym",
        )

    # TODO: not supported yet.
    # def test_delete_from_hint(self):
    #    t = table('sometable', column('somecolumn'))
    #    t2 = table('othertable', column('somecolumn'))
    #    for darg in ("*", "mssql"):
    #        self.assert_compile(
    #            t.delete().where(t.c.somecolumn==t2.c.somecolumn).
    #                    with_hint("WITH (PAGLOCK)",
    #                            selectable=t2,
    #                            dialect_name=darg),
    #            ""
    #        )

    @testing.combinations(
        (
            lambda: select(literal("x"), literal("y")),
            "SELECT [POSTCOMPILE_param_1] AS anon_1, "
            "[POSTCOMPILE_param_2] AS anon_2",
            {
                "check_literal_execute": {"param_1": "x", "param_2": "y"},
                "check_post_param": {},
            },
        ),
        (
            lambda t: select(t).where(t.c.foo.in_(["x", "y", "z"])),
            "SELECT sometable.foo FROM sometable WHERE sometable.foo "
            "IN ([POSTCOMPILE_foo_1])",
            {
                "check_literal_execute": {"foo_1": ["x", "y", "z"]},
                "check_post_param": {},
            },
        ),
        (lambda t: t.c.foo.in_([None]), "sometable.foo IN (NULL)", {}),
    )
    def test_strict_binds(self, expr, compiled, kw):
        """test the 'strict' compiler binds."""

        from sqlalchemy.dialects.mssql.base import MSSQLStrictCompiler

        mxodbc_dialect = mxodbc.dialect()
        mxodbc_dialect.statement_compiler = MSSQLStrictCompiler

        t = table("sometable", column("foo"))

        expr = testing.resolve_lambda(expr, t=t)
        self.assert_compile(expr, compiled, dialect=mxodbc_dialect, **kw)

    def test_in_with_subqueries(self):
        """Test removal of legacy behavior that converted "x==subquery"
        to use IN.

        """

        t = table("sometable", column("somecolumn"))
        self.assert_compile(
            t.select().where(t.c.somecolumn == t.select().scalar_subquery()),
            "SELECT sometable.somecolumn FROM "
            "sometable WHERE sometable.somecolumn = "
            "(SELECT sometable.somecolumn FROM "
            "sometable)",
        )
        self.assert_compile(
            t.select().where(t.c.somecolumn != t.select().scalar_subquery()),
            "SELECT sometable.somecolumn FROM "
            "sometable WHERE sometable.somecolumn != "
            "(SELECT sometable.somecolumn FROM "
            "sometable)",
        )

    @testing.uses_deprecated
    def test_count(self):
        t = table("sometable", column("somecolumn"))
        self.assert_compile(
            t.count(),
            "SELECT count(sometable.somecolumn) AS "
            "tbl_row_count FROM sometable",
        )

    def test_noorderby_insubquery(self):
        """test "no ORDER BY in subqueries unless TOP / LIMIT / OFFSET"
        present"""

        table1 = table(
            "mytable",
            column("myid", Integer),
            column("name", String),
            column("description", String),
        )

        q = select(table1.c.myid).order_by(table1.c.myid).alias("foo")
        crit = q.c.myid == table1.c.myid
        self.assert_compile(
            select("*").where(crit),
            "SELECT * FROM (SELECT mytable.myid AS "
            "myid FROM mytable) AS foo, mytable WHERE "
            "foo.myid = mytable.myid",
        )

    def test_noorderby_insubquery_limit(self):
        """test "no ORDER BY in subqueries unless TOP / LIMIT / OFFSET"
        present"""

        table1 = table(
            "mytable",
            column("myid", Integer),
            column("name", String),
            column("description", String),
        )

        q = (
            select(table1.c.myid)
            .order_by(table1.c.myid)
            .limit(10)
            .alias("foo")
        )
        crit = q.c.myid == table1.c.myid
        self.assert_compile(
            select("*").where(crit),
            "SELECT * FROM (SELECT TOP [POSTCOMPILE_param_1] mytable.myid AS "
            "myid FROM mytable ORDER BY mytable.myid) AS foo, mytable WHERE "
            "foo.myid = mytable.myid",
        )

    @testing.combinations(10, 0)
    def test_noorderby_insubquery_offset_oldstyle(self, offset):
        """test "no ORDER BY in subqueries unless TOP / LIMIT / OFFSET"
        present"""

        table1 = table(
            "mytable",
            column("myid", Integer),
            column("name", String),
            column("description", String),
        )

        q = (
            select(table1.c.myid)
            .order_by(table1.c.myid)
            .offset(offset)
            .alias("foo")
        )
        crit = q.c.myid == table1.c.myid
        self.assert_compile(
            select("*").where(crit),
            "SELECT * FROM (SELECT anon_1.myid AS myid FROM "
            "(SELECT mytable.myid AS myid, ROW_NUMBER() OVER (ORDER BY "
            "mytable.myid) AS mssql_rn FROM mytable) AS anon_1 "
            "WHERE mssql_rn > :param_1) AS foo, mytable WHERE "
            "foo.myid = mytable.myid",
        )

    @testing.combinations(10, 0, argnames="offset")
    def test_noorderby_insubquery_offset_newstyle(self, dialect_2012, offset):
        """test "no ORDER BY in subqueries unless TOP / LIMIT / OFFSET"
        present"""

        table1 = table(
            "mytable",
            column("myid", Integer),
            column("name", String),
            column("description", String),
        )

        q = (
            select(table1.c.myid)
            .order_by(table1.c.myid)
            .offset(offset)
            .alias("foo")
        )
        crit = q.c.myid == table1.c.myid
        self.assert_compile(
            select("*").where(crit),
            "SELECT * FROM (SELECT mytable.myid AS myid FROM mytable "
            "ORDER BY mytable.myid OFFSET :param_1 ROWS) AS foo, "
            "mytable WHERE foo.myid = mytable.myid",
            dialect=dialect_2012,
        )

    def test_noorderby_insubquery_limit_offset_newstyle(self, dialect_2012):
        """test "no ORDER BY in subqueries unless TOP / LIMIT / OFFSET"
        present"""

        table1 = table(
            "mytable",
            column("myid", Integer),
            column("name", String),
            column("description", String),
        )

        q = (
            select(table1.c.myid)
            .order_by(table1.c.myid)
            .limit(10)
            .offset(10)
            .alias("foo")
        )
        crit = q.c.myid == table1.c.myid
        self.assert_compile(
            select("*").where(crit),
            "SELECT * FROM (SELECT mytable.myid AS myid FROM mytable "
            "ORDER BY mytable.myid OFFSET :param_1 ROWS "
            "FETCH FIRST :param_2 ROWS ONLY) AS foo, "
            "mytable WHERE foo.myid = mytable.myid",
            dialect=dialect_2012,
        )

    def test_noorderby_parameters_insubquery(self):
        """test that the ms-sql dialect does not include ORDER BY
        positional parameters in subqueries"""

        table1 = table(
            "mytable",
            column("myid", Integer),
            column("name", String),
            column("description", String),
        )

        q = (
            select(table1.c.myid, sql.literal("bar").label("c1"))
            .order_by(table1.c.name + "-")
            .alias("foo")
        )
        crit = q.c.myid == table1.c.myid
        dialect = mssql.dialect()
        dialect.paramstyle = "qmark"
        dialect.positional = True
        self.assert_compile(
            select("*").where(crit),
            "SELECT * FROM (SELECT mytable.myid AS "
            "myid, ? AS c1 FROM mytable) AS foo, mytable WHERE "
            "foo.myid = mytable.myid",
            dialect=dialect,
            checkparams={"param_1": "bar"},
            # if name_1 is included, too many parameters are passed to dbapi
            checkpositional=("bar",),
        )

    def test_schema_many_tokens_one(self):
        metadata = MetaData()
        tbl = Table(
            "test",
            metadata,
            Column("id", Integer, primary_key=True),
            schema="abc.def.efg.hij",
        )

        # for now, we don't really know what the above means, at least
        # don't lose the dot
        self.assert_compile(
            select(tbl),
            "SELECT [abc.def.efg].hij.test.id FROM [abc.def.efg].hij.test",
        )

        dbname, owner = mssql_base._schema_elements("abc.def.efg.hij")
        eq_(dbname, "abc.def.efg")
        assert not isinstance(dbname, quoted_name)
        eq_(owner, "hij")

    def test_schema_many_tokens_two(self):
        metadata = MetaData()
        tbl = Table(
            "test",
            metadata,
            Column("id", Integer, primary_key=True),
            schema="[abc].[def].[efg].[hij]",
        )

        self.assert_compile(
            select(tbl),
            "SELECT [abc].[def].[efg].hij.test.id "
            "FROM [abc].[def].[efg].hij.test",
        )

    def test_force_schema_quoted_name_w_dot_case_insensitive(self):
        metadata = MetaData()
        tbl = Table(
            "test",
            metadata,
            Column("id", Integer, primary_key=True),
            schema=quoted_name("foo.dbo", True),
        )
        self.assert_compile(
            select(tbl), "SELECT [foo.dbo].test.id FROM [foo.dbo].test"
        )

    def test_force_schema_quoted_w_dot_case_insensitive(self):
        metadata = MetaData()
        tbl = Table(
            "test",
            metadata,
            Column("id", Integer, primary_key=True),
            schema=quoted_name("foo.dbo", True),
        )
        self.assert_compile(
            select(tbl), "SELECT [foo.dbo].test.id FROM [foo.dbo].test"
        )

    def test_force_schema_quoted_name_w_dot_case_sensitive(self):
        metadata = MetaData()
        tbl = Table(
            "test",
            metadata,
            Column("id", Integer, primary_key=True),
            schema=quoted_name("Foo.dbo", True),
        )
        self.assert_compile(
            select(tbl), "SELECT [Foo.dbo].test.id FROM [Foo.dbo].test"
        )

    def test_force_schema_quoted_w_dot_case_sensitive(self):
        metadata = MetaData()
        tbl = Table(
            "test",
            metadata,
            Column("id", Integer, primary_key=True),
            schema="[Foo.dbo]",
        )
        self.assert_compile(
            select(tbl), "SELECT [Foo.dbo].test.id FROM [Foo.dbo].test"
        )

    def test_schema_autosplit_w_dot_case_insensitive(self):
        metadata = MetaData()
        tbl = Table(
            "test",
            metadata,
            Column("id", Integer, primary_key=True),
            schema="foo.dbo",
        )
        self.assert_compile(
            select(tbl), "SELECT foo.dbo.test.id FROM foo.dbo.test"
        )

    def test_schema_autosplit_w_dot_case_sensitive(self):
        metadata = MetaData()
        tbl = Table(
            "test",
            metadata,
            Column("id", Integer, primary_key=True),
            schema="Foo.dbo",
        )
        self.assert_compile(
            select(tbl), "SELECT [Foo].dbo.test.id FROM [Foo].dbo.test"
        )

    def test_delete_schema(self):
        metadata = MetaData()
        tbl = Table(
            "test",
            metadata,
            Column("id", Integer, primary_key=True),
            schema="paj",
        )
        self.assert_compile(
            tbl.delete(tbl.c.id == 1),
            "DELETE FROM paj.test WHERE paj.test.id = " ":id_1",
        )
        s = select(tbl.c.id).where(tbl.c.id == 1)
        self.assert_compile(
            tbl.delete().where(tbl.c.id.in_(s)),
            "DELETE FROM paj.test WHERE paj.test.id IN "
            "(SELECT paj.test.id FROM paj.test "
            "WHERE paj.test.id = :id_1)",
        )

    def test_delete_schema_multipart(self):
        metadata = MetaData()
        tbl = Table(
            "test",
            metadata,
            Column("id", Integer, primary_key=True),
            schema="banana.paj",
        )
        self.assert_compile(
            tbl.delete(tbl.c.id == 1),
            "DELETE FROM banana.paj.test WHERE " "banana.paj.test.id = :id_1",
        )
        s = select(tbl.c.id).where(tbl.c.id == 1)
        self.assert_compile(
            tbl.delete().where(tbl.c.id.in_(s)),
            "DELETE FROM banana.paj.test WHERE "
            "banana.paj.test.id IN (SELECT banana.paj.test.id "
            "FROM banana.paj.test WHERE "
            "banana.paj.test.id = :id_1)",
        )

    def test_delete_schema_multipart_needs_quoting(self):
        metadata = MetaData()
        tbl = Table(
            "test",
            metadata,
            Column("id", Integer, primary_key=True),
            schema="banana split.paj",
        )
        self.assert_compile(
            tbl.delete(tbl.c.id == 1),
            "DELETE FROM [banana split].paj.test WHERE "
            "[banana split].paj.test.id = :id_1",
        )
        s = select(tbl.c.id).where(tbl.c.id == 1)
        self.assert_compile(
            tbl.delete().where(tbl.c.id.in_(s)),
            "DELETE FROM [banana split].paj.test WHERE "
            "[banana split].paj.test.id IN ("
            "SELECT [banana split].paj.test.id FROM "
            "[banana split].paj.test WHERE "
            "[banana split].paj.test.id = :id_1)",
        )

    def test_delete_schema_multipart_both_need_quoting(self):
        metadata = MetaData()
        tbl = Table(
            "test",
            metadata,
            Column("id", Integer, primary_key=True),
            schema="banana split.paj with a space",
        )
        self.assert_compile(
            tbl.delete(tbl.c.id == 1),
            "DELETE FROM [banana split].[paj with a "
            "space].test WHERE [banana split].[paj "
            "with a space].test.id = :id_1",
        )
        s = select(tbl.c.id).where(tbl.c.id == 1)
        self.assert_compile(
            tbl.delete().where(tbl.c.id.in_(s)),
            "DELETE FROM [banana split].[paj with a space].test "
            "WHERE [banana split].[paj with a space].test.id IN "
            "(SELECT [banana split].[paj with a space].test.id "
            "FROM [banana split].[paj with a space].test "
            "WHERE [banana split].[paj with a space].test.id = :id_1)",
        )

    def test_union(self):
        t1 = table(
            "t1",
            column("col1"),
            column("col2"),
            column("col3"),
            column("col4"),
        )
        t2 = table(
            "t2",
            column("col1"),
            column("col2"),
            column("col3"),
            column("col4"),
        )
        s1, s2 = (
            select(t1.c.col3.label("col3"), t1.c.col4.label("col4")).where(
                t1.c.col2.in_(["t1col2r1", "t1col2r2"]),
            ),
            select(t2.c.col3.label("col3"), t2.c.col4.label("col4")).where(
                t2.c.col2.in_(["t2col2r2", "t2col2r3"]),
            ),
        )
        u = union(s1, s2, order_by=["col3", "col4"])
        self.assert_compile(
            u,
            "SELECT t1.col3 AS col3, t1.col4 AS col4 "
            "FROM t1 WHERE t1.col2 IN ([POSTCOMPILE_col2_1]) "
            "UNION SELECT t2.col3 AS col3, "
            "t2.col4 AS col4 FROM t2 WHERE t2.col2 IN "
            "([POSTCOMPILE_col2_2]) ORDER BY col3, col4",
            checkparams={
                "col2_1": ["t1col2r1", "t1col2r2"],
                "col2_2": ["t2col2r2", "t2col2r3"],
            },
        )
        self.assert_compile(
            u.alias("bar").select(),
            "SELECT bar.col3, bar.col4 FROM (SELECT "
            "t1.col3 AS col3, t1.col4 AS col4 FROM t1 "
            "WHERE t1.col2 IN ([POSTCOMPILE_col2_1]) UNION "
            "SELECT t2.col3 AS col3, t2.col4 AS col4 "
            "FROM t2 WHERE t2.col2 IN ([POSTCOMPILE_col2_2])) AS bar",
            checkparams={
                "col2_1": ["t1col2r1", "t1col2r2"],
                "col2_2": ["t2col2r2", "t2col2r3"],
            },
        )

    def test_function(self):
        self.assert_compile(func.foo(1, 2), "foo(:foo_1, :foo_2)")
        self.assert_compile(func.current_time(), "CURRENT_TIME")
        self.assert_compile(func.foo(), "foo()")
        m = MetaData()
        t = Table(
            "sometable", m, Column("col1", Integer), Column("col2", Integer)
        )
        self.assert_compile(
            select(func.max(t.c.col1)),
            "SELECT max(sometable.col1) AS max_1 FROM " "sometable",
        )

    def test_function_overrides(self):
        self.assert_compile(func.current_date(), "GETDATE()")
        self.assert_compile(func.length(3), "LEN(:length_1)")

    def test_extract(self):
        t = table("t", column("col1"))

        for field in "day", "month", "year":
            self.assert_compile(
                select(extract(field, t.c.col1)),
                "SELECT DATEPART(%s, t.col1) AS anon_1 FROM t" % field,
            )

    def test_update_returning(self):
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
            "UPDATE mytable SET name=:name OUTPUT "
            "inserted.myid, inserted.name",
        )
        u = update(table1, values=dict(name="foo")).returning(table1)
        self.assert_compile(
            u,
            "UPDATE mytable SET name=:name OUTPUT "
            "inserted.myid, inserted.name, "
            "inserted.description",
        )
        u = (
            update(table1, values=dict(name="foo"))
            .returning(table1)
            .where(table1.c.name == "bar")
        )
        self.assert_compile(
            u,
            "UPDATE mytable SET name=:name OUTPUT "
            "inserted.myid, inserted.name, "
            "inserted.description WHERE mytable.name = "
            ":name_1",
        )
        u = update(table1, values=dict(name="foo")).returning(
            func.length(table1.c.name)
        )
        self.assert_compile(
            u,
            "UPDATE mytable SET name=:name OUTPUT "
            "LEN(inserted.name) AS length_1",
        )

    def test_delete_returning(self):
        table1 = table(
            "mytable",
            column("myid", Integer),
            column("name", String(128)),
            column("description", String(128)),
        )
        d = delete(table1).returning(table1.c.myid, table1.c.name)
        self.assert_compile(
            d, "DELETE FROM mytable OUTPUT deleted.myid, " "deleted.name"
        )
        d = (
            delete(table1)
            .where(table1.c.name == "bar")
            .returning(table1.c.myid, table1.c.name)
        )
        self.assert_compile(
            d,
            "DELETE FROM mytable OUTPUT deleted.myid, "
            "deleted.name WHERE mytable.name = :name_1",
        )

    def test_insert_returning(self):
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
            "INSERT INTO mytable (name) OUTPUT "
            "inserted.myid, inserted.name VALUES "
            "(:name)",
        )
        i = insert(table1, values=dict(name="foo")).returning(table1)
        self.assert_compile(
            i,
            "INSERT INTO mytable (name) OUTPUT "
            "inserted.myid, inserted.name, "
            "inserted.description VALUES (:name)",
        )
        i = insert(table1, values=dict(name="foo")).returning(
            func.length(table1.c.name)
        )
        self.assert_compile(
            i,
            "INSERT INTO mytable (name) OUTPUT "
            "LEN(inserted.name) AS length_1 VALUES "
            "(:name)",
        )

    def test_limit_using_top(self):
        t = table("t", column("x", Integer), column("y", Integer))

        s = select(t).where(t.c.x == 5).order_by(t.c.y).limit(10)

        self.assert_compile(
            s,
            "SELECT TOP [POSTCOMPILE_param_1] t.x, t.y FROM t "
            "WHERE t.x = :x_1 ORDER BY t.y",
            checkparams={"x_1": 5, "param_1": 10},
        )

    def test_limit_zero_using_top(self):
        t = table("t", column("x", Integer), column("y", Integer))

        s = select(t).where(t.c.x == 5).order_by(t.c.y).limit(0)

        self.assert_compile(
            s,
            "SELECT TOP [POSTCOMPILE_param_1] t.x, t.y FROM t "
            "WHERE t.x = :x_1 ORDER BY t.y",
            checkparams={"x_1": 5, "param_1": 0},
        )
        c = s.compile(dialect=mssql.dialect())
        eq_(len(c._result_columns), 2)
        assert t.c.x in set(c._create_result_map()["x"][1])

    def test_offset_using_window(self):
        t = table("t", column("x", Integer), column("y", Integer))

        s = select(t).where(t.c.x == 5).order_by(t.c.y).offset(20)

        # test that the select is not altered with subsequent compile
        # calls
        for i in range(2):
            self.assert_compile(
                s,
                "SELECT anon_1.x, anon_1.y FROM (SELECT t.x AS x, t.y "
                "AS y, ROW_NUMBER() OVER (ORDER BY t.y) AS "
                "mssql_rn FROM t WHERE t.x = :x_1) AS "
                "anon_1 WHERE mssql_rn > :param_1",
                checkparams={"param_1": 20, "x_1": 5},
            )

            c = s.compile(dialect=mssql.dialect())
            eq_(len(c._result_columns), 2)
            assert t.c.x in set(c._create_result_map()["x"][1])

    def test_simple_limit_expression_offset_using_window(self):
        t = table("t", column("x", Integer), column("y", Integer))

        s = (
            select(t)
            .where(t.c.x == 5)
            .order_by(t.c.y)
            .limit(10)
            .offset(literal_column("20"))
        )

        self.assert_compile(
            s,
            "SELECT anon_1.x, anon_1.y "
            "FROM (SELECT t.x AS x, t.y AS y, "
            "ROW_NUMBER() OVER (ORDER BY t.y) AS mssql_rn "
            "FROM t "
            "WHERE t.x = :x_1) AS anon_1 "
            "WHERE mssql_rn > 20 AND mssql_rn <= :param_1 + 20",
            checkparams={"param_1": 10, "x_1": 5},
        )

    def test_limit_offset_using_window(self):
        t = table("t", column("x", Integer), column("y", Integer))

        s = select(t).where(t.c.x == 5).order_by(t.c.y).limit(10).offset(20)

        self.assert_compile(
            s,
            "SELECT anon_1.x, anon_1.y "
            "FROM (SELECT t.x AS x, t.y AS y, "
            "ROW_NUMBER() OVER (ORDER BY t.y) AS mssql_rn "
            "FROM t "
            "WHERE t.x = :x_1) AS anon_1 "
            "WHERE mssql_rn > :param_1 AND mssql_rn <= :param_2 + :param_1",
            checkparams={"param_1": 20, "param_2": 10, "x_1": 5},
        )
        c = s.compile(dialect=mssql.dialect())
        eq_(len(c._result_columns), 2)
        assert t.c.x in set(c._create_result_map()["x"][1])
        assert t.c.y in set(c._create_result_map()["y"][1])

    def test_limit_offset_using_offset_fetch(self, dialect_2012):
        t = table("t", column("x", Integer), column("y", Integer))
        s = select(t).where(t.c.x == 5).order_by(t.c.y).limit(10).offset(20)

        self.assert_compile(
            s,
            "SELECT t.x, t.y "
            "FROM t "
            "WHERE t.x = :x_1 ORDER BY t.y "
            "OFFSET :param_1 ROWS "
            "FETCH FIRST :param_2 ROWS ONLY",
            checkparams={"param_1": 20, "param_2": 10, "x_1": 5},
            dialect=dialect_2012,
        )

        c = s.compile(dialect=dialect_2012)
        eq_(len(c._result_columns), 2)
        assert t.c.x in set(c._create_result_map()["x"][1])
        assert t.c.y in set(c._create_result_map()["y"][1])

    def test_limit_offset_w_ambiguous_cols(self):
        t = table("t", column("x", Integer), column("y", Integer))

        cols = [t.c.x, t.c.x.label("q"), t.c.x.label("p"), t.c.y]
        s = select(cols).where(t.c.x == 5).order_by(t.c.y).limit(10).offset(20)

        self.assert_compile(
            s,
            "SELECT anon_1.x, anon_1.q, anon_1.p, anon_1.y "
            "FROM (SELECT t.x AS x, t.x AS q, t.x AS p, t.y AS y, "
            "ROW_NUMBER() OVER (ORDER BY t.y) AS mssql_rn "
            "FROM t "
            "WHERE t.x = :x_1) AS anon_1 "
            "WHERE mssql_rn > :param_1 AND mssql_rn <= :param_2 + :param_1",
            checkparams={"param_1": 20, "param_2": 10, "x_1": 5},
        )
        c = s.compile(dialect=mssql.dialect())
        eq_(len(c._result_columns), 4)

        result_map = c._create_result_map()

        for col in cols:
            is_(result_map[col.key][1][0], col)

    def test_limit_offset_with_correlated_order_by(self):
        t1 = table("t1", column("x", Integer), column("y", Integer))
        t2 = table("t2", column("x", Integer), column("y", Integer))

        order_by = select(t2.c.y).where(t1.c.x == t2.c.x).scalar_subquery()
        s = (
            select(t1)
            .where(t1.c.x == 5)
            .order_by(order_by)
            .limit(10)
            .offset(20)
        )

        self.assert_compile(
            s,
            "SELECT anon_1.x, anon_1.y "
            "FROM (SELECT t1.x AS x, t1.y AS y, "
            "ROW_NUMBER() OVER (ORDER BY "
            "(SELECT t2.y FROM t2 WHERE t1.x = t2.x)"
            ") AS mssql_rn "
            "FROM t1 "
            "WHERE t1.x = :x_1) AS anon_1 "
            "WHERE mssql_rn > :param_1 AND mssql_rn <= :param_2 + :param_1",
            checkparams={"param_1": 20, "param_2": 10, "x_1": 5},
        )

        c = s.compile(dialect=mssql.dialect())
        eq_(len(c._result_columns), 2)
        assert t1.c.x in set(c._create_result_map()["x"][1])
        assert t1.c.y in set(c._create_result_map()["y"][1])

    def test_offset_dont_misapply_labelreference(self):
        m = MetaData()

        t = Table("t", m, Column("x", Integer))

        expr1 = func.foo(t.c.x).label("x")
        expr2 = func.foo(t.c.x).label("y")

        stmt1 = select(expr1).order_by(expr1.desc()).offset(1)
        stmt2 = select(expr2).order_by(expr2.desc()).offset(1)

        self.assert_compile(
            stmt1,
            "SELECT anon_1.x FROM (SELECT foo(t.x) AS x, "
            "ROW_NUMBER() OVER (ORDER BY foo(t.x) DESC) AS mssql_rn FROM t) "
            "AS anon_1 WHERE mssql_rn > :param_1",
        )

        self.assert_compile(
            stmt2,
            "SELECT anon_1.y FROM (SELECT foo(t.x) AS y, "
            "ROW_NUMBER() OVER (ORDER BY foo(t.x) DESC) AS mssql_rn FROM t) "
            "AS anon_1 WHERE mssql_rn > :param_1",
        )

    def test_limit_zero_offset_using_window(self):
        t = table("t", column("x", Integer), column("y", Integer))

        s = select(t).where(t.c.x == 5).order_by(t.c.y).limit(0).offset(0)

        # offset is zero but we need to cache a compatible statement
        self.assert_compile(
            s,
            "SELECT anon_1.x, anon_1.y FROM (SELECT t.x AS x, t.y AS y, "
            "ROW_NUMBER() OVER (ORDER BY t.y) AS mssql_rn FROM t "
            "WHERE t.x = :x_1) AS anon_1 WHERE mssql_rn > :param_1 "
            "AND mssql_rn <= :param_2 + :param_1",
            checkparams={"x_1": 5, "param_1": 0, "param_2": 0},
        )

    def test_limit_zero_using_window(self):
        t = table("t", column("x", Integer), column("y", Integer))

        s = select(t).where(t.c.x == 5).order_by(t.c.y).limit(0)

        # render the LIMIT of zero, but not the OFFSET
        # of zero, so produces TOP 0
        self.assert_compile(
            s,
            "SELECT TOP [POSTCOMPILE_param_1] t.x, t.y FROM t "
            "WHERE t.x = :x_1 ORDER BY t.y",
            checkparams={"x_1": 5, "param_1": 0},
        )

    def test_table_pkc_clustering(self):
        metadata = MetaData()
        tbl = Table(
            "test",
            metadata,
            Column("x", Integer, autoincrement=False),
            Column("y", Integer, autoincrement=False),
            PrimaryKeyConstraint("x", "y", mssql_clustered=True),
        )
        self.assert_compile(
            schema.CreateTable(tbl),
            "CREATE TABLE test (x INTEGER NOT NULL, y INTEGER NOT NULL, "
            "PRIMARY KEY CLUSTERED (x, y))",
        )

    def test_table_pkc_explicit_nonclustered(self):
        metadata = MetaData()
        tbl = Table(
            "test",
            metadata,
            Column("x", Integer, autoincrement=False),
            Column("y", Integer, autoincrement=False),
            PrimaryKeyConstraint("x", "y", mssql_clustered=False),
        )
        self.assert_compile(
            schema.CreateTable(tbl),
            "CREATE TABLE test (x INTEGER NOT NULL, y INTEGER NOT NULL, "
            "PRIMARY KEY NONCLUSTERED (x, y))",
        )

    def test_table_idx_explicit_nonclustered(self):
        metadata = MetaData()
        tbl = Table(
            "test",
            metadata,
            Column("x", Integer, autoincrement=False),
            Column("y", Integer, autoincrement=False),
        )

        idx = Index("myidx", tbl.c.x, tbl.c.y, mssql_clustered=False)
        self.assert_compile(
            schema.CreateIndex(idx),
            "CREATE NONCLUSTERED INDEX myidx ON test (x, y)",
        )

    def test_table_uc_explicit_nonclustered(self):
        metadata = MetaData()
        tbl = Table(
            "test",
            metadata,
            Column("x", Integer, autoincrement=False),
            Column("y", Integer, autoincrement=False),
            UniqueConstraint("x", "y", mssql_clustered=False),
        )
        self.assert_compile(
            schema.CreateTable(tbl),
            "CREATE TABLE test (x INTEGER NULL, y INTEGER NULL, "
            "UNIQUE NONCLUSTERED (x, y))",
        )

    def test_table_uc_clustering(self):
        metadata = MetaData()
        tbl = Table(
            "test",
            metadata,
            Column("x", Integer, autoincrement=False),
            Column("y", Integer, autoincrement=False),
            PrimaryKeyConstraint("x"),
            UniqueConstraint("y", mssql_clustered=True),
        )
        self.assert_compile(
            schema.CreateTable(tbl),
            "CREATE TABLE test (x INTEGER NOT NULL, y INTEGER NULL, "
            "PRIMARY KEY (x), UNIQUE CLUSTERED (y))",
        )

    def test_index_clustering(self):
        metadata = MetaData()
        tbl = Table("test", metadata, Column("id", Integer))
        idx = Index("foo", tbl.c.id, mssql_clustered=True)
        self.assert_compile(
            schema.CreateIndex(idx), "CREATE CLUSTERED INDEX foo ON test (id)"
        )

    def test_index_where(self):
        metadata = MetaData()
        tbl = Table("test", metadata, Column("data", Integer))
        idx = Index("test_idx_data_1", tbl.c.data, mssql_where=tbl.c.data > 1)
        self.assert_compile(
            schema.CreateIndex(idx),
            "CREATE INDEX test_idx_data_1 ON test (data) WHERE data > 1",
        )

        idx = Index("test_idx_data_1", tbl.c.data, mssql_where="data > 1")
        self.assert_compile(
            schema.CreateIndex(idx),
            "CREATE INDEX test_idx_data_1 ON test (data) WHERE data > 1",
        )

    def test_index_ordering(self):
        metadata = MetaData()
        tbl = Table(
            "test",
            metadata,
            Column("x", Integer),
            Column("y", Integer),
            Column("z", Integer),
        )
        idx = Index("foo", tbl.c.x.desc(), "y")
        self.assert_compile(
            schema.CreateIndex(idx), "CREATE INDEX foo ON test (x DESC, y)"
        )

    def test_create_index_expr(self):
        m = MetaData()
        t1 = Table("foo", m, Column("x", Integer))
        self.assert_compile(
            schema.CreateIndex(Index("bar", t1.c.x > 5)),
            "CREATE INDEX bar ON foo (x > 5)",
        )

    def test_drop_index_w_schema(self):
        m = MetaData()
        t1 = Table("foo", m, Column("x", Integer), schema="bar")
        self.assert_compile(
            schema.DropIndex(Index("idx_foo", t1.c.x)),
            "DROP INDEX idx_foo ON bar.foo",
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
        idx = Index("foo", tbl.c.x, mssql_include=["y"])
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
        idx = Index("foo", tbl.c.x, mssql_include=[tbl.c.y])
        self.assert_compile(
            schema.CreateIndex(idx), "CREATE INDEX foo ON test (x) INCLUDE (y)"
        )

    def test_index_include_where(self):
        metadata = MetaData()
        tbl = Table(
            "test",
            metadata,
            Column("x", Integer),
            Column("y", Integer),
            Column("z", Integer),
        )
        idx = Index(
            "foo", tbl.c.x, mssql_include=[tbl.c.y], mssql_where=tbl.c.y > 1
        )
        self.assert_compile(
            schema.CreateIndex(idx),
            "CREATE INDEX foo ON test (x) INCLUDE (y) WHERE y > 1",
        )

        idx = Index(
            "foo", tbl.c.x, mssql_include=[tbl.c.y], mssql_where=text("y > 1")
        )
        self.assert_compile(
            schema.CreateIndex(idx),
            "CREATE INDEX foo ON test (x) INCLUDE (y) WHERE y > 1",
        )

    def test_try_cast(self):
        metadata = MetaData()
        t1 = Table("t1", metadata, Column("id", Integer, primary_key=True))

        self.assert_compile(
            select(try_cast(t1.c.id, Integer)),
            "SELECT TRY_CAST (t1.id AS INTEGER) AS id FROM t1",
        )

    @testing.combinations(
        ("no_persisted", "", "ignore"),
        ("persisted_none", "", None),
        ("persisted_true", " PERSISTED", True),
        ("persisted_false", "", False),
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
            "CREATE TABLE t (x INTEGER NULL, y AS (x + 2)%s)" % text,
        )

    @testing.combinations(
        (
            5,
            10,
            {},
            "OFFSET :param_1 ROWS FETCH FIRST :param_2 ROWS ONLY",
            {"param_1": 10, "param_2": 5},
        ),
        (None, 10, {}, "OFFSET :param_1 ROWS", {"param_1": 10}),
        (
            5,
            None,
            {},
            "OFFSET 0 ROWS FETCH FIRST :param_1 ROWS ONLY",
            {"param_1": 5},
        ),
        (
            0,
            0,
            {},
            "OFFSET :param_1 ROWS FETCH FIRST :param_2 ROWS ONLY",
            {"param_1": 0, "param_2": 0},
        ),
        (
            5,
            0,
            {"percent": True},
            "TOP [POSTCOMPILE_param_1] PERCENT",
            {"param_1": 5},
        ),
        (
            5,
            None,
            {"percent": True, "with_ties": True},
            "TOP [POSTCOMPILE_param_1] PERCENT WITH TIES",
            {"param_1": 5},
        ),
        (
            5,
            0,
            {"with_ties": True},
            "TOP [POSTCOMPILE_param_1] WITH TIES",
            {"param_1": 5},
        ),
        (
            literal_column("Q"),
            literal_column("Y"),
            {},
            "OFFSET Y ROWS FETCH FIRST Q ROWS ONLY",
            {},
        ),
        (
            column("Q"),
            column("Y"),
            {},
            "OFFSET [Y] ROWS FETCH FIRST [Q] ROWS ONLY",
            {},
        ),
        (
            bindparam("Q", 3),
            bindparam("Y", 7),
            {},
            "OFFSET :Y ROWS FETCH FIRST :Q ROWS ONLY",
            {"Q": 3, "Y": 7},
        ),
        (
            literal_column("Q") + literal_column("Z"),
            literal_column("Y") + literal_column("W"),
            {},
            "OFFSET Y + W ROWS FETCH FIRST Q + Z ROWS ONLY",
            {},
        ),
        argnames="fetch, offset, fetch_kw, exp, params",
    )
    def test_fetch(self, dialect_2012, fetch, offset, fetch_kw, exp, params):
        t = table("t", column("a"))
        if "TOP" in exp:
            sel = "SELECT %s t.a FROM t ORDER BY t.a" % exp
        else:
            sel = "SELECT t.a FROM t ORDER BY t.a " + exp

        stmt = select(t).order_by(t.c.a).fetch(fetch, **fetch_kw)
        if "with_ties" not in fetch_kw and "percent" not in fetch_kw:
            stmt = stmt.offset(offset)

        self.assert_compile(
            stmt,
            sel,
            checkparams=params,
            dialect=dialect_2012,
        )

    @testing.combinations(
        (
            5,
            10,
            {},
            "mssql_rn > :param_1 AND mssql_rn <= :param_2 + :param_1",
            {"param_1": 10, "param_2": 5},
        ),
        (None, 10, {}, "mssql_rn > :param_1", {"param_1": 10}),
        (
            5,
            None,
            {},
            "mssql_rn <= :param_1",
            {"param_1": 5},
        ),
        (
            0,
            0,
            {},
            "mssql_rn > :param_1 AND mssql_rn <= :param_2 + :param_1",
            {"param_1": 0, "param_2": 0},
        ),
        (
            5,
            0,
            {"percent": True},
            "TOP [POSTCOMPILE_param_1] PERCENT",
            {"param_1": 5},
        ),
        (
            5,
            None,
            {"percent": True, "with_ties": True},
            "TOP [POSTCOMPILE_param_1] PERCENT WITH TIES",
            {"param_1": 5},
        ),
        (
            5,
            0,
            {"with_ties": True},
            "TOP [POSTCOMPILE_param_1] WITH TIES",
            {"param_1": 5},
        ),
        (
            literal_column("Q"),
            literal_column("Y"),
            {},
            "mssql_rn > Y AND mssql_rn <= Q + Y",
            {},
        ),
        (
            column("Q"),
            column("Y"),
            {},
            "mssql_rn > [Y] AND mssql_rn <= [Q] + [Y]",
            {},
        ),
        (
            bindparam("Q", 3),
            bindparam("Y", 7),
            {},
            "mssql_rn > :Y AND mssql_rn <= :Q + :Y",
            {"Q": 3, "Y": 7},
        ),
        (
            literal_column("Q") + literal_column("Z"),
            literal_column("Y") + literal_column("W"),
            {},
            "mssql_rn > Y + W AND mssql_rn <= Q + Z + Y + W",
            {},
        ),
        argnames="fetch, offset, fetch_kw, exp, params",
    )
    def test_fetch_old_version(self, fetch, offset, fetch_kw, exp, params):
        t = table("t", column("a"))
        if "TOP" in exp:
            sel = "SELECT %s t.a FROM t ORDER BY t.a" % exp
        else:
            sel = (
                "SELECT anon_1.a FROM (SELECT t.a AS a, ROW_NUMBER() "
                "OVER (ORDER BY t.a) AS mssql_rn FROM t) AS anon_1 WHERE "
                + exp
            )

        stmt = select(t).order_by(t.c.a).fetch(fetch, **fetch_kw)
        if "with_ties" not in fetch_kw and "percent" not in fetch_kw:
            stmt = stmt.offset(offset)

        self.assert_compile(
            stmt,
            sel,
            checkparams=params,
        )

    _no_offset = (
        "MSSQL needs TOP to use PERCENT and/or WITH TIES. "
        "Only simple fetch without offset can be used."
    )

    _order_by = (
        "MSSQL requires an order_by when using an OFFSET "
        "or a non-simple LIMIT clause"
    )

    @testing.combinations(
        (
            select(tbl).order_by(tbl.c.a).fetch(5, percent=True).offset(3),
            _no_offset,
        ),
        (
            select(tbl).order_by(tbl.c.a).fetch(5, with_ties=True).offset(3),
            _no_offset,
        ),
        (
            select(tbl)
            .order_by(tbl.c.a)
            .fetch(5, percent=True, with_ties=True)
            .offset(3),
            _no_offset,
        ),
        (
            select(tbl)
            .order_by(tbl.c.a)
            .fetch(bindparam("x"), with_ties=True),
            _no_offset,
        ),
        (select(tbl).fetch(5).offset(3), _order_by),
        (select(tbl).fetch(5), _order_by),
        (select(tbl).offset(5), _order_by),
        argnames="stmt, error",
    )
    def test_row_limit_compile_error(self, dialect_2012, stmt, error):
        with testing.expect_raises_message(exc.CompileError, error):
            print(stmt.compile(dialect=dialect_2012))
        with testing.expect_raises_message(exc.CompileError, error):
            print(stmt.compile(dialect=self.__dialect__))


class CompileIdentityTest(fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = mssql.dialect()

    def assert_compile_with_warning(self, *args, **kwargs):
        with testing.expect_deprecated(
            "The dialect options 'mssql_identity_start' and "
            "'mssql_identity_increment' are deprecated. "
            "Use the 'Identity' object instead."
        ):
            return self.assert_compile(*args, **kwargs)

    def test_primary_key_no_identity(self):
        metadata = MetaData()
        tbl = Table(
            "test",
            metadata,
            Column("id", Integer, autoincrement=False, primary_key=True),
        )
        self.assert_compile(
            schema.CreateTable(tbl),
            "CREATE TABLE test (id INTEGER NOT NULL, PRIMARY KEY (id))",
        )

    def test_primary_key_defaults_to_identity(self):
        metadata = MetaData()
        tbl = Table("test", metadata, Column("id", Integer, primary_key=True))
        self.assert_compile(
            schema.CreateTable(tbl),
            "CREATE TABLE test (id INTEGER NOT NULL IDENTITY, "
            "PRIMARY KEY (id))",
        )

    def test_primary_key_with_identity_object(self):
        metadata = MetaData()
        tbl = Table(
            "test",
            metadata,
            Column(
                "id",
                Integer,
                Identity(start=3, increment=42),
                primary_key=True,
            ),
        )
        self.assert_compile(
            schema.CreateTable(tbl),
            "CREATE TABLE test (id INTEGER NOT NULL IDENTITY(3,42), "
            "PRIMARY KEY (id))",
        )

    def test_identity_no_primary_key(self):
        metadata = MetaData()
        tbl = Table(
            "test", metadata, Column("id", Integer, autoincrement=True)
        )
        self.assert_compile(
            schema.CreateTable(tbl),
            "CREATE TABLE test (id INTEGER NOT NULL IDENTITY)",
        )

    def test_identity_object_no_primary_key(self):
        metadata = MetaData()
        tbl = Table(
            "test",
            metadata,
            Column("id", Integer, Identity(increment=42)),
        )
        self.assert_compile(
            schema.CreateTable(tbl),
            "CREATE TABLE test (id INTEGER NOT NULL IDENTITY(1,42))",
        )

    def test_identity_object_1_1(self):
        metadata = MetaData()
        tbl = Table(
            "test",
            metadata,
            Column("id", Integer, Identity(start=1, increment=1)),
        )
        self.assert_compile(
            schema.CreateTable(tbl),
            "CREATE TABLE test (id INTEGER NOT NULL IDENTITY(1,1))",
        )

    def test_identity_object_no_primary_key_non_nullable(self):
        metadata = MetaData()
        tbl = Table(
            "test",
            metadata,
            Column(
                "id",
                Integer,
                Identity(start=3),
                nullable=False,
            ),
        )
        self.assert_compile(
            schema.CreateTable(tbl),
            "CREATE TABLE test (id INTEGER NOT NULL IDENTITY(3,1)" ")",
        )

    def test_identity_separate_from_primary_key(self):
        metadata = MetaData()
        tbl = Table(
            "test",
            metadata,
            Column("id", Integer, autoincrement=False, primary_key=True),
            Column("x", Integer, autoincrement=True),
        )
        self.assert_compile(
            schema.CreateTable(tbl),
            "CREATE TABLE test (id INTEGER NOT NULL, "
            "x INTEGER NOT NULL IDENTITY, "
            "PRIMARY KEY (id))",
        )

    def test_identity_object_separate_from_primary_key(self):
        metadata = MetaData()
        tbl = Table(
            "test",
            metadata,
            Column("id", Integer, autoincrement=False, primary_key=True),
            Column(
                "x",
                Integer,
                Identity(start=3, increment=42),
            ),
        )
        self.assert_compile(
            schema.CreateTable(tbl),
            "CREATE TABLE test (id INTEGER NOT NULL, "
            "x INTEGER NOT NULL IDENTITY(3,42), "
            "PRIMARY KEY (id))",
        )

    def test_identity_illegal_two_autoincrements(self):
        metadata = MetaData()
        tbl = Table(
            "test",
            metadata,
            Column("id", Integer, autoincrement=True),
            Column("id2", Integer, autoincrement=True),
        )
        # this will be rejected by the database, just asserting this is what
        # the two autoincrements will do right now
        self.assert_compile(
            schema.CreateTable(tbl),
            "CREATE TABLE test (id INTEGER NOT NULL IDENTITY, "
            "id2 INTEGER NOT NULL IDENTITY)",
        )

    def test_identity_object_illegal_two_autoincrements(self):
        metadata = MetaData()
        tbl = Table(
            "test",
            metadata,
            Column(
                "id",
                Integer,
                Identity(start=3, increment=42),
                autoincrement=True,
            ),
            Column(
                "id2",
                Integer,
                Identity(start=7, increment=2),
            ),
        )
        # this will be rejected by the database, just asserting this is what
        # the two autoincrements will do right now
        self.assert_compile(
            schema.CreateTable(tbl),
            "CREATE TABLE test (id INTEGER NOT NULL IDENTITY(3,42), "
            "id2 INTEGER NOT NULL IDENTITY(7,2))",
        )

    def test_identity_start_0(self):
        metadata = MetaData()
        tbl = Table(
            "test",
            metadata,
            Column("id", Integer, mssql_identity_start=0, primary_key=True),
        )
        self.assert_compile_with_warning(
            schema.CreateTable(tbl),
            "CREATE TABLE test (id INTEGER NOT NULL IDENTITY(0,1), "
            "PRIMARY KEY (id))",
        )

    def test_identity_increment_5(self):
        metadata = MetaData()
        tbl = Table(
            "test",
            metadata,
            Column(
                "id", Integer, mssql_identity_increment=5, primary_key=True
            ),
        )
        self.assert_compile_with_warning(
            schema.CreateTable(tbl),
            "CREATE TABLE test (id INTEGER NOT NULL IDENTITY(1,5), "
            "PRIMARY KEY (id))",
        )

    @testing.combinations(
        schema.CreateTable(
            Table(
                "test",
                MetaData(),
                Column(
                    "id",
                    Integer,
                    Identity(start=2, increment=2),
                    mssql_identity_start=0,
                ),
            )
        ),
        schema.CreateTable(
            Table(
                "test1",
                MetaData(),
                Column(
                    "id2",
                    Integer,
                    Identity(start=3, increment=3),
                    mssql_identity_increment=5,
                ),
            )
        ),
    )
    def test_identity_options_ignored_with_identity_object(self, create_table):
        assert_raises_message(
            exc.CompileError,
            "Cannot specify options 'mssql_identity_start' and/or "
            "'mssql_identity_increment' while also using the "
            "'Identity' construct.",
            create_table.compile,
            dialect=self.__dialect__,
        )

    def test_identity_object_no_options(self):
        metadata = MetaData()
        tbl = Table(
            "test",
            metadata,
            Column("id", Integer, Identity()),
        )
        self.assert_compile(
            schema.CreateTable(tbl),
            "CREATE TABLE test (id INTEGER NOT NULL IDENTITY)",
        )


class SchemaTest(fixtures.TestBase):
    def setup_test(self):
        t = Table(
            "sometable",
            MetaData(),
            Column("pk_column", Integer),
            Column("test_column", String),
        )
        self.column = t.c.test_column

        dialect = mssql.dialect()
        self.ddl_compiler = dialect.ddl_compiler(
            dialect, schema.CreateTable(t)
        )

    def _column_spec(self):
        return self.ddl_compiler.get_column_specification(self.column)

    def test_that_mssql_default_nullability_emits_null(self):
        eq_("test_column VARCHAR(max) NULL", self._column_spec())

    def test_that_mssql_none_nullability_does_not_emit_nullability(self):
        self.column.nullable = None
        eq_("test_column VARCHAR(max)", self._column_spec())

    def test_that_mssql_specified_nullable_emits_null(self):
        self.column.nullable = True
        eq_("test_column VARCHAR(max) NULL", self._column_spec())

    def test_that_mssql_specified_not_nullable_emits_not_null(self):
        self.column.nullable = False
        eq_("test_column VARCHAR(max) NOT NULL", self._column_spec())
