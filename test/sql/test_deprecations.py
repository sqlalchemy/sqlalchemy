#! coding: utf-8

from sqlalchemy import alias
from sqlalchemy import and_
from sqlalchemy import bindparam
from sqlalchemy import CHAR
from sqlalchemy import column
from sqlalchemy import create_engine
from sqlalchemy import exc
from sqlalchemy import ForeignKey
from sqlalchemy import func
from sqlalchemy import INT
from sqlalchemy import Integer
from sqlalchemy import join
from sqlalchemy import literal_column
from sqlalchemy import MetaData
from sqlalchemy import null
from sqlalchemy import or_
from sqlalchemy import select
from sqlalchemy import sql
from sqlalchemy import String
from sqlalchemy import table
from sqlalchemy import testing
from sqlalchemy import text
from sqlalchemy import util
from sqlalchemy import VARCHAR
from sqlalchemy.engine import default
from sqlalchemy.sql import coercions
from sqlalchemy.sql import quoted_name
from sqlalchemy.sql import roles
from sqlalchemy.sql import visitors
from sqlalchemy.sql.selectable import SelectStatementGrouping
from sqlalchemy.testing import assert_raises
from sqlalchemy.testing import assert_raises_message
from sqlalchemy.testing import AssertsCompiledSQL
from sqlalchemy.testing import engines
from sqlalchemy.testing import eq_
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import in_
from sqlalchemy.testing import is_true
from sqlalchemy.testing import mock
from sqlalchemy.testing import not_in_
from sqlalchemy.testing.schema import Column
from sqlalchemy.testing.schema import Table


class DeprecationWarningsTest(fixtures.TestBase, AssertsCompiledSQL):
    __backend__ = True

    def test_ident_preparer_force(self):
        preparer = testing.db.dialect.identifier_preparer
        preparer.quote("hi")
        with testing.expect_deprecated(
            "The IdentifierPreparer.quote.force parameter is deprecated"
        ):
            preparer.quote("hi", True)

        with testing.expect_deprecated(
            "The IdentifierPreparer.quote.force parameter is deprecated"
        ):
            preparer.quote("hi", False)

        preparer.quote_schema("hi")
        with testing.expect_deprecated(
            "The IdentifierPreparer.quote_schema.force parameter is deprecated"
        ):
            preparer.quote_schema("hi", True)

        with testing.expect_deprecated(
            "The IdentifierPreparer.quote_schema.force parameter is deprecated"
        ):
            preparer.quote_schema("hi", True)

    def test_string_convert_unicode(self):
        with testing.expect_deprecated(
            "The String.convert_unicode parameter is deprecated and "
            "will be removed in a future release."
        ):
            String(convert_unicode=True)

    def test_string_convert_unicode_force(self):
        with testing.expect_deprecated(
            "The String.convert_unicode parameter is deprecated and "
            "will be removed in a future release."
        ):
            String(convert_unicode="force")

    def test_engine_convert_unicode(self):
        with testing.expect_deprecated(
            "The create_engine.convert_unicode parameter and "
            "corresponding dialect-level"
        ):
            create_engine("mysql://", convert_unicode=True, module=mock.Mock())

    def test_empty_and_or(self):
        with testing.expect_deprecated(
            r"Invoking and_\(\) without arguments is deprecated, and "
            r"will be disallowed in a future release.   For an empty "
            r"and_\(\) construct, use and_\(True, \*args\)"
        ):
            self.assert_compile(or_(and_()), "")


class ConvertUnicodeDeprecationTest(fixtures.TestBase):

    __backend__ = True

    data = util.u(
        "Alors vous imaginez ma surprise, au lever du jour, quand "
        "une drôle de petite voix m’a réveillé. "
        "Elle disait: « S’il vous plaît… dessine-moi un mouton! »"
    )

    def test_unicode_warnings_dialectlevel(self):

        unicodedata = self.data

        with testing.expect_deprecated(
            "The create_engine.convert_unicode parameter and "
            "corresponding dialect-level"
        ):
            dialect = default.DefaultDialect(convert_unicode=True)
        dialect.supports_unicode_binds = False

        s = String()
        uni = s.dialect_impl(dialect).bind_processor(dialect)

        uni(util.b("x"))
        assert isinstance(uni(unicodedata), util.binary_type)

        eq_(uni(unicodedata), unicodedata.encode("utf-8"))

    def test_ignoring_unicode_error(self):
        """checks String(unicode_error='ignore') is passed to
        underlying codec."""

        unicodedata = self.data

        with testing.expect_deprecated(
            "The String.convert_unicode parameter is deprecated and "
            "will be removed in a future release.",
            "The String.unicode_errors parameter is deprecated and "
            "will be removed in a future release.",
        ):
            type_ = String(
                248, convert_unicode="force", unicode_error="ignore"
            )
        dialect = default.DefaultDialect(encoding="ascii")
        proc = type_.result_processor(dialect, 10)

        utfdata = unicodedata.encode("utf8")
        eq_(proc(utfdata), unicodedata.encode("ascii", "ignore").decode())


class SubqueryCoercionsTest(fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = "default"

    table1 = table(
        "mytable",
        column("myid", Integer),
        column("name", String),
        column("description", String),
    )

    table2 = table(
        "myothertable", column("otherid", Integer), column("othername", String)
    )

    def test_select_of_select(self):
        stmt = select([self.table1.c.myid])

        with testing.expect_deprecated(
            r"The SelectBase.select\(\) method is deprecated and will be "
            "removed"
        ):
            self.assert_compile(
                stmt.select(),
                "SELECT anon_1.myid FROM (SELECT mytable.myid AS myid "
                "FROM mytable) AS anon_1",
            )

    def test_join_of_select(self):
        stmt = select([self.table1.c.myid])

        with testing.expect_deprecated(
            r"The SelectBase.join\(\) method is deprecated and will be "
            "removed"
        ):
            self.assert_compile(
                stmt.join(
                    self.table2, self.table2.c.otherid == self.table1.c.myid
                ),
                # note the SQL is wrong here as the subquery now has a name.
                # however, even SQLite which accepts unnamed subqueries in a
                # JOIN cannot actually join with how SQLAlchemy 1.3 and
                # earlier would render:
                # sqlite> select myid, otherid from (select myid from mytable)
                # join myothertable on mytable.myid=myothertable.otherid;
                # Error: no such column: mytable.myid
                # if using stmt.c.col, that fails often as well if there are
                # any naming overlaps:
                # sqlalchemy.exc.OperationalError: (sqlite3.OperationalError)
                # ambiguous column name: id
                # [SQL: SELECT id, data
                # FROM (SELECT a.id AS id, a.data AS data
                # FROM a) JOIN b ON b.a_id = id]
                # so that shows that nobody is using this anyway
                "(SELECT mytable.myid AS myid FROM mytable) AS anon_1 "
                "JOIN myothertable ON myothertable.otherid = mytable.myid",
            )

    def test_outerjoin_of_select(self):
        stmt = select([self.table1.c.myid])

        with testing.expect_deprecated(
            r"The SelectBase.outerjoin\(\) method is deprecated and will be "
            "removed"
        ):
            self.assert_compile(
                stmt.outerjoin(
                    self.table2, self.table2.c.otherid == self.table1.c.myid
                ),
                # note the SQL is wrong here as the subquery now has a name
                "(SELECT mytable.myid AS myid FROM mytable) AS anon_1 "
                "LEFT OUTER JOIN myothertable "
                "ON myothertable.otherid = mytable.myid",
            )

    def test_standalone_alias(self):
        with testing.expect_deprecated(
            "Implicit coercion of SELECT and textual SELECT constructs"
        ):
            stmt = alias(select([self.table1.c.myid]), "foo")

        self.assert_compile(stmt, "SELECT mytable.myid FROM mytable")

        is_true(
            stmt.compare(select([self.table1.c.myid]).subquery().alias("foo"))
        )

    def test_as_scalar(self):
        with testing.expect_deprecated(
            r"The SelectBase.as_scalar\(\) method is deprecated and "
            "will be removed in a future release."
        ):
            stmt = select([self.table1.c.myid]).as_scalar()

        is_true(stmt.compare(select([self.table1.c.myid]).scalar_subquery()))

    def test_as_scalar_from_subquery(self):
        with testing.expect_deprecated(
            r"The Subquery.as_scalar\(\) method, which was previously "
            r"``Alias.as_scalar\(\)`` prior to version 1.4"
        ):
            stmt = select([self.table1.c.myid]).subquery().as_scalar()

        is_true(stmt.compare(select([self.table1.c.myid]).scalar_subquery()))

    def test_fromclause_subquery(self):
        stmt = select([self.table1.c.myid])
        with testing.expect_deprecated(
            "Implicit coercion of SELECT and textual SELECT constructs "
            "into FROM clauses is deprecated"
        ):
            coerced = coercions.expect(
                roles.StrictFromClauseRole, stmt, allow_select=True
            )

        is_true(coerced.compare(stmt.subquery()))

    def test_plain_fromclause_select_to_subquery(self):
        with testing.expect_deprecated(
            "Implicit coercion of SELECT and textual SELECT "
            "constructs into FROM clauses is deprecated;"
        ):
            element = coercions.expect(
                roles.FromClauseRole,
                SelectStatementGrouping(select([self.table1])),
            )
            is_true(
                element.compare(
                    SelectStatementGrouping(select([self.table1])).subquery()
                )
            )

    def test_functions_select_method_two(self):
        expr = func.rows("foo")
        with testing.expect_deprecated(
            "Implicit coercion of SELECT and textual SELECT constructs "
            "into FROM clauses is deprecated"
        ):
            stmt = select(["*"]).select_from(expr.select())
        self.assert_compile(
            stmt, "SELECT * FROM (SELECT rows(:rows_2) AS rows_1) AS anon_1"
        )

    def test_functions_with_cols(self):
        users = table(
            "users", column("id"), column("name"), column("fullname")
        )
        calculate = select(
            [column("q"), column("z"), column("r")],
            from_obj=[
                func.calculate(bindparam("x", None), bindparam("y", None))
            ],
        )

        with testing.expect_deprecated(
            "The SelectBase.c and SelectBase.columns attributes are "
            "deprecated and will be removed"
        ):
            self.assert_compile(
                select([users], users.c.id > calculate.c.z),
                "SELECT users.id, users.name, users.fullname "
                "FROM users, (SELECT q, z, r "
                "FROM calculate(:x, :y)) AS anon_1 "
                "WHERE users.id > anon_1.z",
            )


class LateralSubqueryCoercionsTest(fixtures.TablesTest, AssertsCompiledSQL):
    __dialect__ = default.DefaultDialect(supports_native_boolean=True)

    run_setup_bind = None

    run_create_tables = None

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "people",
            metadata,
            Column("people_id", Integer, primary_key=True),
            Column("age", Integer),
            Column("name", String(30)),
        )
        Table(
            "bookcases",
            metadata,
            Column("bookcase_id", Integer, primary_key=True),
            Column(
                "bookcase_owner_id", Integer, ForeignKey("people.people_id")
            ),
            Column("bookcase_shelves", Integer),
            Column("bookcase_width", Integer),
        )
        Table(
            "books",
            metadata,
            Column("book_id", Integer, primary_key=True),
            Column(
                "bookcase_id", Integer, ForeignKey("bookcases.bookcase_id")
            ),
            Column("book_owner_id", Integer, ForeignKey("people.people_id")),
            Column("book_weight", Integer),
        )


class SelectableTest(fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = "default"

    metadata = MetaData()
    table1 = Table(
        "table1",
        metadata,
        Column("col1", Integer, primary_key=True),
        Column("col2", String(20)),
        Column("col3", Integer),
        Column("colx", Integer),
    )

    table2 = Table(
        "table2",
        metadata,
        Column("col1", Integer, primary_key=True),
        Column("col2", Integer, ForeignKey("table1.col1")),
        Column("col3", String(20)),
        Column("coly", Integer),
    )

    def _c_deprecated(self):
        return testing.expect_deprecated(
            "The SelectBase.c and SelectBase.columns attributes are "
            "deprecated"
        )

    def test_deprecated_subquery_standalone(self):
        from sqlalchemy import subquery

        with testing.expect_deprecated(
            r"The standalone subquery\(\) function is deprecated"
        ):
            stmt = subquery(
                None,
                [literal_column("1").label("a")],
                order_by=literal_column("1"),
            )

        self.assert_compile(
            select([stmt]),
            "SELECT anon_1.a FROM (SELECT 1 AS a ORDER BY 1) AS anon_1",
        )

    def test_column(self):
        stmt = select([column("x")])
        with testing.expect_deprecated(
            r"The Select.column\(\) method is deprecated and will be "
            "removed in a future release."
        ):
            stmt = stmt.column(column("q"))

        self.assert_compile(stmt, "SELECT x, q")

    def test_append_column_after_replace_selectable(self):
        basesel = select([literal_column("1").label("a")])
        tojoin = select(
            [literal_column("1").label("a"), literal_column("2").label("b")]
        )
        basefrom = basesel.alias("basefrom")
        joinfrom = tojoin.alias("joinfrom")
        sel = select([basefrom.c.a])

        with testing.expect_deprecated(
            r"The Selectable.replace_selectable\(\) " "method is deprecated"
        ):
            replaced = sel.replace_selectable(
                basefrom, basefrom.join(joinfrom, basefrom.c.a == joinfrom.c.a)
            )
        self.assert_compile(
            replaced,
            "SELECT basefrom.a FROM (SELECT 1 AS a) AS basefrom "
            "JOIN (SELECT 1 AS a, 2 AS b) AS joinfrom "
            "ON basefrom.a = joinfrom.a",
        )

        with testing.expect_deprecated(r"The Select.append_column\(\)"):
            replaced.append_column(joinfrom.c.b)

        self.assert_compile(
            replaced,
            "SELECT basefrom.a, joinfrom.b FROM (SELECT 1 AS a) AS basefrom "
            "JOIN (SELECT 1 AS a, 2 AS b) AS joinfrom "
            "ON basefrom.a = joinfrom.a",
        )

    def test_against_cloned_non_table(self):
        # test that corresponding column digs across
        # clone boundaries with anonymous labeled elements
        col = func.count().label("foo")
        sel = select([col])

        sel2 = visitors.ReplacingCloningVisitor().traverse(sel)
        with testing.expect_deprecated("The SelectBase.c"):
            assert (
                sel2._implicit_subquery.corresponding_column(col) is sel2.c.foo
            )

        sel3 = visitors.ReplacingCloningVisitor().traverse(sel2)
        with testing.expect_deprecated("The SelectBase.c"):
            assert (
                sel3._implicit_subquery.corresponding_column(col) is sel3.c.foo
            )

    def test_alias_union(self):

        # same as testunion, except its an alias of the union

        u = (
            select(
                [
                    self.table1.c.col1,
                    self.table1.c.col2,
                    self.table1.c.col3,
                    self.table1.c.colx,
                    null().label("coly"),
                ]
            )
            .union(
                select(
                    [
                        self.table2.c.col1,
                        self.table2.c.col2,
                        self.table2.c.col3,
                        null().label("colx"),
                        self.table2.c.coly,
                    ]
                )
            )
            .alias("analias")
        )
        s1 = self.table1.select(use_labels=True)
        s2 = self.table2.select(use_labels=True)
        with self._c_deprecated():
            assert u.corresponding_column(s1.c.table1_col2) is u.c.col2
            assert u.corresponding_column(s2.c.table2_col2) is u.c.col2
            assert u.corresponding_column(s2.c.table2_coly) is u.c.coly
            assert s2.c.corresponding_column(u.c.coly) is s2.c.table2_coly

    def test_join_against_self_implicit_subquery(self):
        jj = select([self.table1.c.col1.label("bar_col1")])
        with testing.expect_deprecated(
            "The SelectBase.c and SelectBase.columns attributes are "
            "deprecated and will be removed",
            "Implicit coercion of SELECT",
        ):
            jjj = join(self.table1, jj, self.table1.c.col1 == jj.c.bar_col1)

        jjj_bar_col1 = jjj.c["%s_bar_col1" % jj._implicit_subquery.name]
        assert jjj_bar_col1 is not None

        # test column directly against itself

        assert jjj.corresponding_column(jjj.c.table1_col1) is jjj.c.table1_col1
        with testing.expect_deprecated(
            "The SelectBase.c and SelectBase.columns attributes are "
            "deprecated and will be removed"
        ):
            assert jjj.corresponding_column(jj.c.bar_col1) is jjj_bar_col1

        # test alias of the join

        j2 = jjj.alias("foo")
        assert j2.corresponding_column(self.table1.c.col1) is j2.c.table1_col1

    def test_select_labels(self):
        a = self.table1.select(use_labels=True)
        j = join(a._implicit_subquery, self.table2)

        criterion = a._implicit_subquery.c.table1_col1 == self.table2.c.col2
        self.assert_(criterion.compare(j.onclause))


class QuoteTest(fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = "default"

    def test_literal_column_label_embedded_select_samename_explcit_quote(self):
        col = sql.literal_column("NEEDS QUOTES").label(
            quoted_name("NEEDS QUOTES", True)
        )

        with testing.expect_deprecated(
            r"The SelectBase.select\(\) method is deprecated"
        ):
            self.assert_compile(
                select([col]).select(),
                'SELECT anon_1."NEEDS QUOTES" FROM '
                '(SELECT NEEDS QUOTES AS "NEEDS QUOTES") AS anon_1',
            )

    def test_literal_column_label_embedded_select_diffname_explcit_quote(self):
        col = sql.literal_column("NEEDS QUOTES").label(
            quoted_name("NEEDS QUOTES_", True)
        )

        with testing.expect_deprecated(
            r"The SelectBase.select\(\) method is deprecated"
        ):
            self.assert_compile(
                select([col]).select(),
                'SELECT anon_1."NEEDS QUOTES_" FROM '
                '(SELECT NEEDS QUOTES AS "NEEDS QUOTES_") AS anon_1',
            )

    def test_literal_column_label_embedded_select_diffname(self):
        col = sql.literal_column("NEEDS QUOTES").label("NEEDS QUOTES_")

        with testing.expect_deprecated(
            r"The SelectBase.select\(\) method is deprecated"
        ):
            self.assert_compile(
                select([col]).select(),
                'SELECT anon_1."NEEDS QUOTES_" FROM (SELECT NEEDS QUOTES AS '
                '"NEEDS QUOTES_") AS anon_1',
            )

    def test_literal_column_label_embedded_select_samename(self):
        col = sql.literal_column("NEEDS QUOTES").label("NEEDS QUOTES")

        with testing.expect_deprecated(
            r"The SelectBase.select\(\) method is deprecated"
        ):
            self.assert_compile(
                select([col]).select(),
                'SELECT anon_1."NEEDS QUOTES" FROM (SELECT NEEDS QUOTES AS '
                '"NEEDS QUOTES") AS anon_1',
            )


class TextualSelectTest(fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = "default"

    table1 = table(
        "mytable",
        column("myid", Integer),
        column("name", String),
        column("description", String),
    )

    table2 = table(
        "myothertable", column("otherid", Integer), column("othername", String)
    )

    def test_basic_subquery_resultmap(self):
        table1 = self.table1
        t = text("select id, name from user").columns(id=Integer, name=String)

        with testing.expect_deprecated(
            "The SelectBase.c and SelectBase.columns", "Implicit coercion"
        ):
            stmt = select([table1.c.myid]).select_from(
                table1.join(t, table1.c.myid == t.c.id)
            )
        compiled = stmt.compile()
        eq_(
            compiled._create_result_map(),
            {
                "myid": (
                    "myid",
                    (table1.c.myid, "myid", "myid", "mytable_myid"),
                    table1.c.myid.type,
                )
            },
        )

    def test_column_collection_ordered(self):
        t = text("select a, b, c from foo").columns(
            column("a"), column("b"), column("c")
        )
        with testing.expect_deprecated(
            "The SelectBase.c and SelectBase.columns"
        ):
            eq_(t.c.keys(), ["a", "b", "c"])

    def test_column_collection_pos_plus_bykey(self):
        # overlapping positional names + type names
        t = text("select a, b, c from foo").columns(
            column("a"), column("b"), b=Integer, c=String
        )

        with testing.expect_deprecated(
            "The SelectBase.c and SelectBase.columns"
        ):
            eq_(t.c.keys(), ["a", "b", "c"])
        with testing.expect_deprecated(
            "The SelectBase.c and SelectBase.columns"
        ):
            eq_(t.c.b.type._type_affinity, Integer)
        with testing.expect_deprecated(
            "The SelectBase.c and SelectBase.columns"
        ):
            eq_(t.c.c.type._type_affinity, String)


class DeprecatedAppendMethTest(fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = "default"

    def _expect_deprecated(self, clsname, methname, newmeth):
        return testing.expect_deprecated(
            r"The %s.append_%s\(\) method is deprecated "
            r"and will be removed in a future release.  Use the generative "
            r"method %s.%s\(\)." % (clsname, methname, clsname, newmeth)
        )

    def test_append_whereclause(self):
        t = table("t", column("q"))
        stmt = select([t])

        with self._expect_deprecated("Select", "whereclause", "where"):
            stmt.append_whereclause(t.c.q == 5)

        self.assert_compile(stmt, "SELECT t.q FROM t WHERE t.q = :q_1")

    def test_append_having(self):
        t = table("t", column("q"))
        stmt = select([t]).group_by(t.c.q)

        with self._expect_deprecated("Select", "having", "having"):
            stmt.append_having(t.c.q == 5)

        self.assert_compile(
            stmt, "SELECT t.q FROM t GROUP BY t.q HAVING t.q = :q_1"
        )

    def test_append_order_by(self):
        t = table("t", column("q"), column("x"))
        stmt = select([t]).where(t.c.q == 5)

        with self._expect_deprecated(
            "GenerativeSelect", "order_by", "order_by"
        ):
            stmt.append_order_by(t.c.x)

        self.assert_compile(
            stmt, "SELECT t.q, t.x FROM t WHERE t.q = :q_1 ORDER BY t.x"
        )

    def test_append_group_by(self):
        t = table("t", column("q"))
        stmt = select([t])

        with self._expect_deprecated(
            "GenerativeSelect", "group_by", "group_by"
        ):
            stmt.append_group_by(t.c.q)

        stmt = stmt.having(t.c.q == 5)

        self.assert_compile(
            stmt, "SELECT t.q FROM t GROUP BY t.q HAVING t.q = :q_1"
        )

    def test_append_correlation(self):
        t1 = table("t1", column("q"))
        t2 = table("t2", column("q"), column("p"))

        inner = select([t2.c.p]).where(t2.c.q == t1.c.q)

        with self._expect_deprecated("Select", "correlation", "correlate"):
            inner.append_correlation(t1)
        stmt = select([t1]).where(t1.c.q == inner.scalar_subquery())

        self.assert_compile(
            stmt,
            "SELECT t1.q FROM t1 WHERE t1.q = "
            "(SELECT t2.p FROM t2 WHERE t2.q = t1.q)",
        )

    def test_append_column(self):
        t1 = table("t1", column("q"), column("p"))
        stmt = select([t1.c.q])
        with self._expect_deprecated("Select", "column", "column"):
            stmt.append_column(t1.c.p)
        self.assert_compile(stmt, "SELECT t1.q, t1.p FROM t1")

    def test_append_prefix(self):
        t1 = table("t1", column("q"), column("p"))
        stmt = select([t1.c.q])
        with self._expect_deprecated("Select", "prefix", "prefix_with"):
            stmt.append_prefix("FOO BAR")
        self.assert_compile(stmt, "SELECT FOO BAR t1.q FROM t1")

    def test_append_from(self):
        t1 = table("t1", column("q"))
        t2 = table("t2", column("q"))

        stmt = select([t1])
        with self._expect_deprecated("Select", "from", "select_from"):
            stmt.append_from(t1.join(t2, t1.c.q == t2.c.q))
        self.assert_compile(stmt, "SELECT t1.q FROM t1 JOIN t2 ON t1.q = t2.q")


class KeyTargetingTest(fixtures.TablesTest):
    run_inserts = "once"
    run_deletes = None
    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "keyed1",
            metadata,
            Column("a", CHAR(2), key="b"),
            Column("c", CHAR(2), key="q"),
        )
        Table("keyed2", metadata, Column("a", CHAR(2)), Column("b", CHAR(2)))
        Table("keyed3", metadata, Column("a", CHAR(2)), Column("d", CHAR(2)))
        Table("keyed4", metadata, Column("b", CHAR(2)), Column("q", CHAR(2)))
        Table("content", metadata, Column("t", String(30), key="type"))
        Table("bar", metadata, Column("ctype", String(30), key="content_type"))

        if testing.requires.schemas.enabled:
            Table(
                "wschema",
                metadata,
                Column("a", CHAR(2), key="b"),
                Column("c", CHAR(2), key="q"),
                schema=testing.config.test_schema,
            )

    @classmethod
    def insert_data(cls, connection):
        conn = connection
        conn.execute(cls.tables.keyed1.insert(), dict(b="a1", q="c1"))
        conn.execute(cls.tables.keyed2.insert(), dict(a="a2", b="b2"))
        conn.execute(cls.tables.keyed3.insert(), dict(a="a3", d="d3"))
        conn.execute(cls.tables.keyed4.insert(), dict(b="b4", q="q4"))
        conn.execute(cls.tables.content.insert(), type="t1")

        if testing.requires.schemas.enabled:
            conn.execute(
                cls.tables["%s.wschema" % testing.config.test_schema].insert(),
                dict(b="a1", q="c1"),
            )

    def test_column_label_overlap_fallback(self, connection):
        content, bar = self.tables.content, self.tables.bar
        row = connection.execute(
            select([content.c.type.label("content_type")])
        ).first()

        not_in_(content.c.type, row)
        not_in_(bar.c.content_type, row)

        with testing.expect_deprecated(
            "Retreiving row values using Column objects "
            "with only matching names"
        ):
            in_(sql.column("content_type"), row)

        row = connection.execute(
            select([func.now().label("content_type")])
        ).first()
        not_in_(content.c.type, row)
        not_in_(bar.c.content_type, row)
        with testing.expect_deprecated(
            "Retreiving row values using Column objects "
            "with only matching names"
        ):
            in_(sql.column("content_type"), row)

    def test_columnclause_schema_column_one(self, connection):
        keyed2 = self.tables.keyed2

        # this is addressed by [ticket:2932]
        # ColumnClause._compare_name_for_result allows the
        # columns which the statement is against to be lightweight
        # cols, which results in a more liberal comparison scheme
        a, b = sql.column("a"), sql.column("b")
        stmt = select([a, b]).select_from(table("keyed2"))
        row = connection.execute(stmt).first()

        with testing.expect_deprecated(
            "Retreiving row values using Column objects "
            "with only matching names"
        ):
            in_(keyed2.c.a, row)
        with testing.expect_deprecated(
            "Retreiving row values using Column objects "
            "with only matching names"
        ):
            in_(keyed2.c.b, row)

    def test_columnclause_schema_column_two(self, connection):
        keyed2 = self.tables.keyed2

        a, b = sql.column("a"), sql.column("b")
        stmt = select([keyed2.c.a, keyed2.c.b])
        row = connection.execute(stmt).first()

        with testing.expect_deprecated(
            "Retreiving row values using Column objects "
            "with only matching names"
        ):
            in_(a, row)
        with testing.expect_deprecated(
            "Retreiving row values using Column objects "
            "with only matching names"
        ):
            in_(b, row)

    def test_columnclause_schema_column_three(self, connection):
        keyed2 = self.tables.keyed2

        # originally addressed by [ticket:2932], however liberalized
        # Column-targeting rules are deprecated

        a, b = sql.column("a"), sql.column("b")
        stmt = text("select a, b from keyed2").columns(a=CHAR, b=CHAR)
        row = connection.execute(stmt).first()

        with testing.expect_deprecated(
            "Retreiving row values using Column objects "
            "with only matching names"
        ):
            in_(keyed2.c.a, row)
        with testing.expect_deprecated(
            "Retreiving row values using Column objects "
            "with only matching names"
        ):
            in_(keyed2.c.b, row)
        with testing.expect_deprecated(
            "Retreiving row values using Column objects "
            "with only matching names"
        ):
            in_(a, row)
        with testing.expect_deprecated(
            "Retreiving row values using Column objects "
            "with only matching names"
        ):
            in_(b, row)
        with testing.expect_deprecated(
            "Retreiving row values using Column objects "
            "with only matching names",
            "The SelectBase.c and SelectBase.columns",
        ):
            in_(stmt.c.a, row)
        with testing.expect_deprecated(
            "Retreiving row values using Column objects "
            "with only matching names",
            "The SelectBase.c and SelectBase.columns",
        ):
            in_(stmt.c.b, row)

    def test_columnclause_schema_column_four(self, connection):
        keyed2 = self.tables.keyed2

        # this is also addressed by [ticket:2932]

        a, b = sql.column("keyed2_a"), sql.column("keyed2_b")
        stmt = text("select a AS keyed2_a, b AS keyed2_b from keyed2").columns(
            a, b
        )
        row = connection.execute(stmt).first()

        with testing.expect_deprecated(
            "Retreiving row values using Column objects "
            "with only matching names"
        ):
            in_(keyed2.c.a, row)
        with testing.expect_deprecated(
            "Retreiving row values using Column objects "
            "with only matching names"
        ):
            in_(keyed2.c.b, row)
        with testing.expect_deprecated(
            "Retreiving row values using Column objects "
            "with only matching names",
            "The SelectBase.c and SelectBase.columns",
        ):
            in_(stmt.c.keyed2_a, row)
        with testing.expect_deprecated(
            "Retreiving row values using Column objects "
            "with only matching names",
            "The SelectBase.c and SelectBase.columns",
        ):
            in_(stmt.c.keyed2_b, row)

    def test_columnclause_schema_column_five(self, connection):
        keyed2 = self.tables.keyed2

        # this is also addressed by [ticket:2932]

        stmt = text("select a AS keyed2_a, b AS keyed2_b from keyed2").columns(
            keyed2_a=CHAR, keyed2_b=CHAR
        )
        row = connection.execute(stmt).first()

        with testing.expect_deprecated(
            "Retreiving row values using Column objects "
            "with only matching names"
        ):
            in_(keyed2.c.a, row)
        with testing.expect_deprecated(
            "Retreiving row values using Column objects "
            "with only matching names"
        ):
            in_(keyed2.c.b, row)
        with testing.expect_deprecated(
            "Retreiving row values using Column objects "
            "with only matching names",
            "The SelectBase.c and SelectBase.columns",
        ):
            in_(stmt.c.keyed2_a, row)
        with testing.expect_deprecated(
            "Retreiving row values using Column objects "
            "with only matching names",
            "The SelectBase.c and SelectBase.columns",
        ):
            in_(stmt.c.keyed2_b, row)


class CursorResultTest(fixtures.TablesTest):
    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "users",
            metadata,
            Column(
                "user_id", INT, primary_key=True, test_needs_autoincrement=True
            ),
            Column("user_name", VARCHAR(20)),
            test_needs_acid=True,
        )
        Table(
            "addresses",
            metadata,
            Column(
                "address_id",
                Integer,
                primary_key=True,
                test_needs_autoincrement=True,
            ),
            Column("user_id", Integer, ForeignKey("users.user_id")),
            Column("address", String(30)),
            test_needs_acid=True,
        )

        Table(
            "users2",
            metadata,
            Column("user_id", INT, primary_key=True),
            Column("user_name", VARCHAR(20)),
            test_needs_acid=True,
        )

    @classmethod
    def insert_data(cls, connection):
        users = cls.tables.users

        connection.execute(
            users.insert(),
            dict(user_id=1, user_name="john"),
            dict(user_id=2, user_name="jack"),
        )

    def test_column_accessor_textual_select(self, connection):
        users = self.tables.users

        with testing.expect_deprecated(
            #           "Retreiving row values using Column objects "
            #           "with only matching names",
            #           "Using non-integer/slice indices on Row is "
            #           "deprecated and will be removed in version 2.0",
        ):
            # this will create column() objects inside
            # the select(), these need to match on name anyway
            r = connection.execute(
                select([column("user_id"), column("user_name")])
                .select_from(table("users"))
                .where(text("user_id=2"))
            ).first()

            eq_(r[users.c.user_id], 2)

        r._keymap.pop(users.c.user_id)  # reset lookup
        with testing.expect_deprecated(
            #           "Retreiving row values using Column objects "
            #           "with only matching names"
        ):
            eq_(r._mapping[users.c.user_id], 2)

        with testing.expect_deprecated(
            #           "Retreiving row values using Column objects "
            #           "with only matching names"
        ):
            eq_(r._mapping[users.c.user_name], "jack")

    def test_column_accessor_basic_text(self, connection):
        users = self.tables.users

        with testing.expect_deprecated(
            "Using non-integer/slice indices on Row is deprecated "
            "and will be removed in version 2.0",
            "Retreiving row values using Column objects "
            "with only matching names",
        ):
            r = connection.execute(
                text("select * from users where user_id=2")
            ).first()

            eq_(r[users.c.user_id], 2)

        r._keymap.pop(users.c.user_id)
        with testing.expect_deprecated(
            "Retreiving row values using Column objects "
            "with only matching names"
        ):
            eq_(r._mapping[users.c.user_id], 2)

        with testing.expect_deprecated(
            "Using non-integer/slice indices on Row is deprecated "
            "and will be removed in version 2.0",
            "Retreiving row values using Column objects "
            "with only matching names",
        ):
            eq_(r[users.c.user_name], "jack")

        r._keymap.pop(users.c.user_name)
        with testing.expect_deprecated(
            "Retreiving row values using Column objects "
            "with only matching names"
        ):
            eq_(r._mapping[users.c.user_name], "jack")

    @testing.provide_metadata
    def test_column_label_overlap_fallback(self, connection):
        content = Table("content", self.metadata, Column("type", String(30)))
        bar = Table("bar", self.metadata, Column("content_type", String(30)))
        self.metadata.create_all(testing.db)
        connection.execute(content.insert().values(type="t1"))

        row = connection.execute(content.select(use_labels=True)).first()
        in_(content.c.type, row._mapping)
        not_in_(bar.c.content_type, row)
        with testing.expect_deprecated(
            "Retreiving row values using Column objects "
            "with only matching names"
        ):
            in_(sql.column("content_type"), row)

        row = connection.execute(
            select([content.c.type.label("content_type")])
        ).first()
        with testing.expect_deprecated(
            "Retreiving row values using Column objects "
            "with only matching names"
        ):
            in_(content.c.type, row)

        not_in_(bar.c.content_type, row)

        with testing.expect_deprecated(
            "Retreiving row values using Column objects "
            "with only matching names"
        ):
            in_(sql.column("content_type"), row)

        row = connection.execute(
            select([func.now().label("content_type")])
        ).first()

        not_in_(content.c.type, row)

        not_in_(bar.c.content_type, row)

        with testing.expect_deprecated(
            "Retreiving row values using Column objects "
            "with only matching names"
        ):
            in_(sql.column("content_type"), row)

    def test_pickled_rows(self):
        users = self.tables.users
        addresses = self.tables.addresses
        with testing.db.connect() as conn:
            conn.execute(users.delete())
            conn.execute(
                users.insert(),
                {"user_id": 7, "user_name": "jack"},
                {"user_id": 8, "user_name": "ed"},
                {"user_id": 9, "user_name": "fred"},
            )

            for pickle in False, True:
                for use_labels in False, True:
                    result = conn.execute(
                        users.select(use_labels=use_labels).order_by(
                            users.c.user_id
                        )
                    ).fetchall()

                    if pickle:
                        result = util.pickle.loads(util.pickle.dumps(result))

                    if pickle:
                        with testing.expect_deprecated(
                            "Retreiving row values using Column objects "
                            "from a row that was unpickled"
                        ):
                            eq_(result[0]._mapping[users.c.user_id], 7)

                        result[0]._keymap.pop(users.c.user_id)
                        with testing.expect_deprecated(
                            "Retreiving row values using Column objects "
                            "from a row that was unpickled"
                        ):
                            eq_(result[0]._mapping[users.c.user_id], 7)

                        with testing.expect_deprecated(
                            "Retreiving row values using Column objects "
                            "from a row that was unpickled"
                        ):
                            eq_(result[0]._mapping[users.c.user_name], "jack")

                        result[0]._keymap.pop(users.c.user_name)
                        with testing.expect_deprecated(
                            "Retreiving row values using Column objects "
                            "from a row that was unpickled"
                        ):
                            eq_(result[0]._mapping[users.c.user_name], "jack")

                    if not pickle or use_labels:
                        assert_raises(
                            exc.NoSuchColumnError,
                            lambda: result[0][addresses.c.user_id],
                        )

                        assert_raises(
                            exc.NoSuchColumnError,
                            lambda: result[0]._mapping[addresses.c.user_id],
                        )
                    else:
                        # test with a different table.  name resolution is
                        # causing 'user_id' to match when use_labels wasn't
                        # used.
                        with testing.expect_deprecated(
                            "Retreiving row values using Column objects "
                            "from a row that was unpickled"
                        ):
                            eq_(result[0]._mapping[addresses.c.user_id], 7)

                        result[0]._keymap.pop(addresses.c.user_id)
                        with testing.expect_deprecated(
                            "Retreiving row values using Column objects "
                            "from a row that was unpickled"
                        ):
                            eq_(result[0]._mapping[addresses.c.user_id], 7)

                    assert_raises(
                        exc.NoSuchColumnError,
                        lambda: result[0][addresses.c.address_id],
                    )

                    assert_raises(
                        exc.NoSuchColumnError,
                        lambda: result[0]._mapping[addresses.c.address_id],
                    )

    @testing.requires.duplicate_names_in_cursor_description
    def test_ambiguous_column_case_sensitive(self):
        with testing.expect_deprecated(
            "The create_engine.case_sensitive parameter is deprecated"
        ):
            eng = engines.testing_engine(options=dict(case_sensitive=False))

        with eng.connect() as conn:
            row = conn.execute(
                select(
                    [
                        literal_column("1").label("SOMECOL"),
                        literal_column("1").label("SOMECOL"),
                    ]
                )
            ).first()

            assert_raises_message(
                exc.InvalidRequestError,
                "Ambiguous column name",
                lambda: row._mapping["somecol"],
            )

    def test_row_getitem_string(self, connection):
        col = literal_column("1").label("foo")

        with testing.expect_deprecated(
            "Using non-integer/slice indices on Row is deprecated "
            "and will be removed in version 2.0;"
        ):
            row = connection.execute(select([col])).first()
            eq_(row["foo"], 1)

        eq_(row._mapping["foo"], 1)

    def test_row_getitem_column(self, connection):
        col = literal_column("1").label("foo")

        with testing.expect_deprecated(
            "Using non-integer/slice indices on Row is deprecated "
            "and will be removed in version 2.0;"
        ):
            row = connection.execute(select([col])).first()
            eq_(row[col], 1)

        eq_(row._mapping[col], 1)

    def test_row_case_insensitive(self):
        with testing.expect_deprecated(
            "The create_engine.case_sensitive parameter is deprecated"
        ):
            with engines.testing_engine(
                options={"case_sensitive": False}
            ).connect() as ins_conn:
                row = ins_conn.execute(
                    select(
                        [
                            literal_column("1").label("case_insensitive"),
                            literal_column("2").label("CaseSensitive"),
                        ]
                    )
                ).first()

                eq_(
                    list(row._mapping.keys()),
                    ["case_insensitive", "CaseSensitive"],
                )

                in_("case_insensitive", row._keymap)
                in_("CaseSensitive", row._keymap)
                in_("casesensitive", row._keymap)

                eq_(row._mapping["case_insensitive"], 1)
                eq_(row._mapping["CaseSensitive"], 2)
                eq_(row._mapping["Case_insensitive"], 1)
                eq_(row._mapping["casesensitive"], 2)

    def test_row_case_insensitive_unoptimized(self):
        with testing.expect_deprecated(
            "The create_engine.case_sensitive parameter is deprecated"
        ):
            with engines.testing_engine(
                options={"case_sensitive": False}
            ).connect() as ins_conn:
                row = ins_conn.execute(
                    select(
                        [
                            literal_column("1").label("case_insensitive"),
                            literal_column("2").label("CaseSensitive"),
                            text("3 AS screw_up_the_cols"),
                        ]
                    )
                ).first()

                eq_(
                    list(row._mapping.keys()),
                    ["case_insensitive", "CaseSensitive", "screw_up_the_cols"],
                )

                in_("case_insensitive", row._keymap)
                in_("CaseSensitive", row._keymap)
                in_("casesensitive", row._keymap)

                eq_(row._mapping["case_insensitive"], 1)
                eq_(row._mapping["CaseSensitive"], 2)
                eq_(row._mapping["screw_up_the_cols"], 3)
                eq_(row._mapping["Case_insensitive"], 1)
                eq_(row._mapping["casesensitive"], 2)
                eq_(row._mapping["screw_UP_the_cols"], 3)

    def test_row_keys_deprecated(self, connection):
        r = connection.execute(
            text("select * from users where user_id=2")
        ).first()

        with testing.expect_deprecated_20(
            r"The Row.keys\(\) function/method is considered legacy "
        ):
            eq_(r.keys(), ["user_id", "user_name"])

    def test_row_contains_key_deprecated(self, connection):
        r = connection.execute(
            text("select * from users where user_id=2")
        ).first()

        with testing.expect_deprecated(
            "Using the 'in' operator to test for string or column keys, or "
            "integer indexes, .* is deprecated"
        ):
            in_("user_name", r)

        # no warning if the key is not there
        not_in_("foobar", r)

        # this seems to happen only with Python BaseRow
        # with testing.expect_deprecated(
        #    "Using the 'in' operator to test for string or column keys, or "
        #   "integer indexes, .* is deprecated"
        # ):
        #    in_(1, r)


class PositionalTextTest(fixtures.TablesTest):
    run_inserts = "once"
    run_deletes = None
    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "text1",
            metadata,
            Column("a", CHAR(2)),
            Column("b", CHAR(2)),
            Column("c", CHAR(2)),
            Column("d", CHAR(2)),
        )

    @classmethod
    def insert_data(cls, connection):
        connection.execute(
            cls.tables.text1.insert(), [dict(a="a1", b="b1", c="c1", d="d1")],
        )

    def test_anon_aliased_overlapping(self, connection):
        text1 = self.tables.text1

        c1 = text1.c.a.label(None)
        c2 = text1.alias().c.a
        c3 = text1.alias().c.a.label(None)
        c4 = text1.c.a.label(None)

        stmt = text("select a, b, c, d from text1").columns(c1, c2, c3, c4)
        result = connection.execute(stmt)
        row = result.first()

        with testing.expect_deprecated(
            "Retreiving row values using Column objects "
            "with only matching names"
        ):
            eq_(row._mapping[text1.c.a], "a1")

    def test_anon_aliased_unique(self, connection):
        text1 = self.tables.text1

        c1 = text1.c.a.label(None)
        c2 = text1.alias().c.c
        c3 = text1.alias().c.b
        c4 = text1.alias().c.d.label(None)

        stmt = text("select a, b, c, d from text1").columns(c1, c2, c3, c4)
        result = connection.execute(stmt)
        row = result.first()

        eq_(row._mapping[c1], "a1")
        eq_(row._mapping[c2], "b1")
        eq_(row._mapping[c3], "c1")
        eq_(row._mapping[c4], "d1")

        # key fallback rules still match this to a column
        # unambiguously based on its name
        with testing.expect_deprecated(
            "Retreiving row values using Column objects "
            "with only matching names"
        ):
            eq_(row._mapping[text1.c.a], "a1")

        # key fallback rules still match this to a column
        # unambiguously based on its name
        with testing.expect_deprecated(
            "Retreiving row values using Column objects "
            "with only matching names"
        ):
            eq_(row._mapping[text1.c.d], "d1")

        # text1.c.b goes nowhere....because we hit key fallback
        # but the text1.c.b doesn't derive from text1.c.c
        assert_raises_message(
            exc.NoSuchColumnError,
            "Could not locate column in row for column 'text1.b'",
            lambda: row[text1.c.b],
        )

        assert_raises_message(
            exc.NoSuchColumnError,
            "Could not locate column in row for column 'text1.b'",
            lambda: row._mapping[text1.c.b],
        )


class DefaultTest(fixtures.TestBase):
    __backend__ = True

    @testing.provide_metadata
    def test_close_on_branched(self):
        metadata = self.metadata

        def mydefault_using_connection(ctx):
            conn = ctx.connection
            try:
                return conn.execute(select([text("12")])).scalar()
            finally:
                # ensure a "close()" on this connection does nothing,
                # since its a "branched" connection
                conn.close()

        table = Table(
            "foo",
            metadata,
            Column("x", Integer),
            Column("y", Integer, default=mydefault_using_connection),
        )

        metadata.create_all(testing.db)
        with testing.db.connect() as conn:
            with testing.expect_deprecated_20(
                r"The .close\(\) method on a so-called 'branched' "
                r"connection is deprecated as of 1.4, as are "
                r"'branched' connections overall"
            ):
                conn.execute(table.insert().values(x=5))

            eq_(conn.execute(select([table])).first(), (5, 12))


class DMLTest(fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = "default"

    def test_insert_inline_kw_defaults(self):
        m = MetaData()
        foo = Table("foo", m, Column("id", Integer))

        t = Table(
            "test",
            m,
            Column("col1", Integer, default=func.foo(1)),
            Column(
                "col2",
                Integer,
                default=select([func.coalesce(func.max(foo.c.id))]),
            ),
        )

        with testing.expect_deprecated_20(
            "The insert.inline parameter will be removed in SQLAlchemy 2.0."
        ):
            stmt = t.insert(inline=True, values={})

        self.assert_compile(
            stmt,
            "INSERT INTO test (col1, col2) VALUES (foo(:foo_1), "
            "(SELECT coalesce(max(foo.id)) AS coalesce_1 FROM "
            "foo))",
            inline_flag=True,
        )

    def test_insert_inline_kw_default(self):
        metadata = MetaData()
        table = Table(
            "sometable",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("foo", Integer, default=func.foobar()),
        )

        with testing.expect_deprecated_20(
            "The insert.inline parameter will be removed in SQLAlchemy 2.0."
        ):
            stmt = table.insert(values={}, inline=True)

        self.assert_compile(
            stmt,
            "INSERT INTO sometable (foo) VALUES (foobar())",
            inline_flag=True,
        )

        with testing.expect_deprecated_20(
            "The insert.inline parameter will be removed in SQLAlchemy 2.0."
        ):
            stmt = table.insert(inline=True)

        self.assert_compile(
            stmt,
            "INSERT INTO sometable (foo) VALUES (foobar())",
            params={},
            inline_flag=True,
        )

    def test_update_inline_kw_defaults(self):
        m = MetaData()
        foo = Table("foo", m, Column("id", Integer))

        t = Table(
            "test",
            m,
            Column("col1", Integer, onupdate=func.foo(1)),
            Column(
                "col2",
                Integer,
                onupdate=select([func.coalesce(func.max(foo.c.id))]),
            ),
            Column("col3", String(30)),
        )

        with testing.expect_deprecated_20(
            "The update.inline parameter will be removed in SQLAlchemy 2.0."
        ):
            stmt = t.update(inline=True, values={"col3": "foo"})

        self.assert_compile(
            stmt,
            "UPDATE test SET col1=foo(:foo_1), col2=(SELECT "
            "coalesce(max(foo.id)) AS coalesce_1 FROM foo), "
            "col3=:col3",
            inline_flag=True,
        )

    def test_update_dialect_kwargs(self):
        t = table("foo", column("bar"))

        with testing.expect_deprecated_20("Passing dialect keyword arguments"):
            stmt = t.update(mysql_limit=10)

        self.assert_compile(
            stmt, "UPDATE foo SET bar=%s LIMIT 10", dialect="mysql"
        )
