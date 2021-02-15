#! coding: utf-8

import itertools
import random

from sqlalchemy import alias
from sqlalchemy import and_
from sqlalchemy import bindparam
from sqlalchemy import case
from sqlalchemy import CHAR
from sqlalchemy import column
from sqlalchemy import create_engine
from sqlalchemy import exc
from sqlalchemy import exists
from sqlalchemy import ForeignKey
from sqlalchemy import func
from sqlalchemy import inspect
from sqlalchemy import INT
from sqlalchemy import Integer
from sqlalchemy import join
from sqlalchemy import literal_column
from sqlalchemy import MetaData
from sqlalchemy import null
from sqlalchemy import or_
from sqlalchemy import schema
from sqlalchemy import select
from sqlalchemy import Sequence
from sqlalchemy import sql
from sqlalchemy import String
from sqlalchemy import table
from sqlalchemy import testing
from sqlalchemy import text
from sqlalchemy import util
from sqlalchemy import VARCHAR
from sqlalchemy.engine import default
from sqlalchemy.sql import coercions
from sqlalchemy.sql import LABEL_STYLE_TABLENAME_PLUS_COL
from sqlalchemy.sql import literal
from sqlalchemy.sql import operators
from sqlalchemy.sql import quoted_name
from sqlalchemy.sql import roles
from sqlalchemy.sql import update
from sqlalchemy.sql import visitors
from sqlalchemy.sql.selectable import LABEL_STYLE_NONE
from sqlalchemy.sql.selectable import SelectStatementGrouping
from sqlalchemy.testing import assert_raises
from sqlalchemy.testing import assert_raises_message
from sqlalchemy.testing import assertions
from sqlalchemy.testing import AssertsCompiledSQL
from sqlalchemy.testing import engines
from sqlalchemy.testing import eq_
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import in_
from sqlalchemy.testing import is_
from sqlalchemy.testing import is_false
from sqlalchemy.testing import is_true
from sqlalchemy.testing import mock
from sqlalchemy.testing import not_in
from sqlalchemy.testing.schema import Column
from sqlalchemy.testing.schema import Table
from .test_update import _UpdateFromTestBase


class ToMetaDataTest(fixtures.TestBase):
    def test_deprecate_tometadata(self):
        m1 = MetaData()
        t1 = Table("t", m1, Column("q", Integer))

        with testing.expect_deprecated(
            r"Table.tometadata\(\) is renamed to Table.to_metadata\(\)"
        ):
            m2 = MetaData()
            t2 = t1.tometadata(m2)
            eq_(t2.name, "t")


class BoundMetadataTest(fixtures.TestBase):
    def test_arg_deprecated(self):
        with testing.expect_deprecated_20(
            "The MetaData.bind argument is deprecated"
        ):
            m1 = MetaData(testing.db)

        Table("t", m1, Column("q", Integer))

        with testing.expect_deprecated_20(
            "The ``bind`` argument for schema methods that invoke SQL "
            "against an engine or connection will be required"
        ):
            m1.create_all()
        try:
            assert "t" in inspect(testing.db).get_table_names()
        finally:
            m1.drop_all(testing.db)

        assert "t" not in inspect(testing.db).get_table_names()

    def test_bind_arg_text(self):
        with testing.expect_deprecated_20(
            "The text.bind argument is deprecated and will be "
            "removed in SQLAlchemy 2.0."
        ):
            t1 = text("ASdf", bind=testing.db)

        # no warnings emitted
        is_(t1.bind, testing.db)
        eq_(str(t1), "ASdf")

    def test_bind_arg_function(self):
        with testing.expect_deprecated_20(
            "The text.bind argument is deprecated and will be "
            "removed in SQLAlchemy 2.0."
        ):
            f1 = func.foobar(bind=testing.db)

        # no warnings emitted
        is_(f1.bind, testing.db)
        eq_(str(f1), "foobar()")

    def test_bind_arg_select(self):
        with testing.expect_deprecated_20(
            "The select.bind argument is deprecated and will be "
            "removed in SQLAlchemy 2.0."
        ):
            s1 = select([column("q")], bind=testing.db)

        # no warnings emitted
        is_(s1.bind, testing.db)
        eq_(str(s1), "SELECT q")

    def test_bind_attr_join_no_warning(self):
        t1 = table("t1", column("a"))
        t2 = table("t2", column("b"))
        j1 = join(t1, t2, t1.c.a == t2.c.b)

        # no warnings emitted
        is_(j1.bind, None)
        eq_(str(j1), "t1 JOIN t2 ON t1.a = t2.b")


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
        stmt = select(self.table1.c.myid)

        with testing.expect_deprecated(
            r"The SelectBase.select\(\) method is deprecated and will be "
            "removed"
        ):
            self.assert_compile(
                stmt.select(),
                "SELECT anon_1.myid FROM (SELECT mytable.myid AS myid "
                "FROM mytable) AS anon_1",
            )

    def test_standalone_alias(self):
        with testing.expect_deprecated(
            "Implicit coercion of SELECT and textual SELECT constructs"
        ):
            stmt = alias(select(self.table1.c.myid), "foo")

        self.assert_compile(stmt, "SELECT mytable.myid FROM mytable")

        is_true(
            stmt.compare(select(self.table1.c.myid).subquery().alias("foo"))
        )

    def test_as_scalar(self):
        with testing.expect_deprecated(
            r"The SelectBase.as_scalar\(\) method is deprecated and "
            "will be removed in a future release."
        ):
            stmt = select(self.table1.c.myid).as_scalar()

        is_true(stmt.compare(select(self.table1.c.myid).scalar_subquery()))

    def test_as_scalar_from_subquery(self):
        with testing.expect_deprecated(
            r"The Subquery.as_scalar\(\) method, which was previously "
            r"``Alias.as_scalar\(\)`` prior to version 1.4"
        ):
            stmt = select(self.table1.c.myid).subquery().as_scalar()

        is_true(stmt.compare(select(self.table1.c.myid).scalar_subquery()))

    def test_fromclause_subquery(self):
        stmt = select(self.table1.c.myid)
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
                SelectStatementGrouping(select(self.table1)),
            )
            is_true(
                element.compare(
                    SelectStatementGrouping(select(self.table1)).subquery()
                )
            )

    def test_functions_select_method_two(self):
        expr = func.rows("foo")
        with testing.expect_deprecated(
            "Implicit coercion of SELECT and textual SELECT constructs "
            "into FROM clauses is deprecated"
        ):
            stmt = select("*").select_from(expr.select())
        self.assert_compile(
            stmt, "SELECT * FROM (SELECT rows(:rows_2) AS rows_1) AS anon_1"
        )

    def test_functions_with_cols(self):
        users = table(
            "users", column("id"), column("name"), column("fullname")
        )
        calculate = select(column("q"), column("z"), column("r")).select_from(
            func.calculate(bindparam("x", None), bindparam("y", None))
        )

        with testing.expect_deprecated(
            "The SelectBase.c and SelectBase.columns attributes are "
            "deprecated and will be removed"
        ):
            self.assert_compile(
                select(users).where(users.c.id > calculate.c.z),
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

    def test_select_list_argument(self):

        with testing.expect_deprecated_20(
            r"The legacy calling style of select\(\) is deprecated "
            "and will be removed in SQLAlchemy 2.0"
        ):
            stmt = select([column("q")])
        self.assert_compile(stmt, "SELECT q")

    def test_select_column_collection_argument(self):
        t1 = table("t1", column("q"))

        with testing.expect_deprecated_20(
            r"The legacy calling style of select\(\) is deprecated "
            "and will be removed in SQLAlchemy 2.0"
        ):
            stmt = select(t1.c)
        self.assert_compile(stmt, "SELECT t1.q FROM t1")

    def test_select_kw_argument(self):

        with testing.expect_deprecated_20(
            r"The legacy calling style of select\(\) is deprecated "
            "and will be removed in SQLAlchemy 2.0"
        ):
            stmt = select(whereclause=column("q") == 5).add_columns(
                column("q")
            )
        self.assert_compile(stmt, "SELECT q WHERE q = :q_1")

    @testing.combinations(
        (
            lambda table1: table1.select(table1.c.col1 == 5),
            "FromClause",
            "whereclause",
            "SELECT table1.col1, table1.col2, table1.col3, table1.colx "
            "FROM table1 WHERE table1.col1 = :col1_1",
        ),
        (
            lambda table1: table1.select(whereclause=table1.c.col1 == 5),
            "FromClause",
            "whereclause",
            "SELECT table1.col1, table1.col2, table1.col3, table1.colx "
            "FROM table1 WHERE table1.col1 = :col1_1",
        ),
        (
            lambda table1: table1.select(order_by=table1.c.col1),
            "FromClause",
            "kwargs",
            "SELECT table1.col1, table1.col2, table1.col3, table1.colx "
            "FROM table1 ORDER BY table1.col1",
        ),
        (
            lambda table1: exists().select(table1.c.col1 == 5),
            "Exists",
            "whereclause",
            "SELECT EXISTS (SELECT *) AS anon_1 FROM table1 "
            "WHERE table1.col1 = :col1_1",
        ),
        (
            lambda table1: exists().select(whereclause=table1.c.col1 == 5),
            "Exists",
            "whereclause",
            "SELECT EXISTS (SELECT *) AS anon_1 FROM table1 "
            "WHERE table1.col1 = :col1_1",
        ),
        (
            lambda table1: exists().select(
                order_by=table1.c.col1, from_obj=table1
            ),
            "Exists",
            "kwargs",
            "SELECT EXISTS (SELECT *) AS anon_1 FROM table1 "
            "ORDER BY table1.col1",
        ),
        (
            lambda table1, table2: table1.join(table2)
            .select(table1.c.col1 == 5)
            .set_label_style(LABEL_STYLE_NONE),
            "Join",
            "whereclause",
            "SELECT table1.col1, table1.col2, table1.col3, table1.colx, "
            "table2.col1, table2.col2, table2.col3, table2.coly FROM table1 "
            "JOIN table2 ON table1.col1 = table2.col2 "
            "WHERE table1.col1 = :col1_1",
        ),
        (
            lambda table1, table2: table1.join(table2)
            .select(whereclause=table1.c.col1 == 5)
            .set_label_style(LABEL_STYLE_NONE),
            "Join",
            "whereclause",
            "SELECT table1.col1, table1.col2, table1.col3, table1.colx, "
            "table2.col1, table2.col2, table2.col3, table2.coly FROM table1 "
            "JOIN table2 ON table1.col1 = table2.col2 "
            "WHERE table1.col1 = :col1_1",
        ),
        (
            lambda table1, table2: table1.join(table2)
            .select(order_by=table1.c.col1)
            .set_label_style(LABEL_STYLE_NONE),
            "Join",
            "kwargs",
            "SELECT table1.col1, table1.col2, table1.col3, table1.colx, "
            "table2.col1, table2.col2, table2.col3, table2.coly FROM table1 "
            "JOIN table2 ON table1.col1 = table2.col2 "
            "ORDER BY table1.col1",
        ),
    )
    def test_select_method_parameters(
        self, stmt, clsname, paramname, expected_sql
    ):
        if paramname == "whereclause":
            warning_txt = (
                r"The %s.select\(\).whereclause parameter is deprecated "
                "and will be removed in version 2.0" % clsname
            )
        else:
            warning_txt = (
                r"The %s.select\(\) method will no longer accept "
                "keyword arguments in version 2.0. " % clsname
            )
        with testing.expect_deprecated_20(
            warning_txt,
            r"The legacy calling style of select\(\) is deprecated "
            "and will be removed in SQLAlchemy 2.0",
        ):
            stmt = testing.resolve_lambda(
                stmt, table1=self.table1, table2=self.table2
            )

        self.assert_compile(stmt, expected_sql)

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
            select(stmt),
            "SELECT anon_1.a FROM (SELECT 1 AS a ORDER BY 1) AS anon_1",
        )

    def test_case_list_legacy(self):
        t1 = table("t", column("q"))

        with testing.expect_deprecated(
            r"The \"whens\" argument to case\(\) is now passed"
        ):
            stmt = select(t1).where(
                case(
                    [(t1.c.q == 5, "foo"), (t1.c.q == 10, "bar")], else_="bat"
                )
                != "bat"
            )

        self.assert_compile(
            stmt,
            "SELECT t.q FROM t WHERE CASE WHEN (t.q = :q_1) "
            "THEN :param_1 WHEN (t.q = :q_2) THEN :param_2 "
            "ELSE :param_3 END != :param_4",
        )

    def test_case_whens_kw(self):
        t1 = table("t", column("q"))

        with testing.expect_deprecated(
            r"The \"whens\" argument to case\(\) is now passed"
        ):
            stmt = select(t1).where(
                case(
                    whens=[(t1.c.q == 5, "foo"), (t1.c.q == 10, "bar")],
                    else_="bat",
                )
                != "bat"
            )

        self.assert_compile(
            stmt,
            "SELECT t.q FROM t WHERE CASE WHEN (t.q = :q_1) "
            "THEN :param_1 WHEN (t.q = :q_2) THEN :param_2 "
            "ELSE :param_3 END != :param_4",
        )

    def test_case_whens_dict_kw(self):
        t1 = table("t", column("q"))

        with testing.expect_deprecated(
            r"The \"whens\" argument to case\(\) is now passed"
        ):
            stmt = select(t1).where(
                case(
                    whens={t1.c.q == 5: "foo"},
                    else_="bat",
                )
                != "bat"
            )

        self.assert_compile(
            stmt,
            "SELECT t.q FROM t WHERE CASE WHEN (t.q = :q_1) THEN "
            ":param_1 ELSE :param_2 END != :param_3",
        )

    def test_case_kw_arg_detection(self):
        # because we support py2k, case() has to parse **kw for now

        assert_raises_message(
            TypeError,
            "unknown arguments: bat, foo",
            case,
            (column("x") == 10, 5),
            else_=15,
            foo="bar",
            bat="hoho",
        )

    def test_with_only_generative(self):
        table1 = table(
            "table1",
            column("col1"),
            column("col2"),
            column("col3"),
            column("colx"),
        )
        s1 = table1.select().scalar_subquery()

        with testing.expect_deprecated(
            r"The \"columns\" argument to "
            r"Select.with_only_columns\(\) is now passed"
        ):
            stmt = s1.with_only_columns([s1])
        self.assert_compile(
            stmt,
            "SELECT (SELECT table1.col1, table1.col2, "
            "table1.col3, table1.colx FROM table1) AS anon_1",
        )

    def test_from_list_with_columns(self):
        table1 = table("t1", column("a"))
        table2 = table("t2", column("b"))
        s1 = select(table1.c.a, table2.c.b)
        self.assert_compile(s1, "SELECT t1.a, t2.b FROM t1, t2")

        with testing.expect_deprecated(
            r"The \"columns\" argument to "
            r"Select.with_only_columns\(\) is now passed"
        ):
            s2 = s1.with_only_columns([table2.c.b])

        self.assert_compile(s2, "SELECT t2.b FROM t2")

    def test_column(self):
        stmt = select(column("x"))
        with testing.expect_deprecated(
            r"The Select.column\(\) method is deprecated and will be "
            "removed in a future release."
        ):
            stmt = stmt.column(column("q"))

        self.assert_compile(stmt, "SELECT x, q")

    def test_append_column_after_replace_selectable(self):
        basesel = select(literal_column("1").label("a"))
        tojoin = select(
            literal_column("1").label("a"), literal_column("2").label("b")
        )
        basefrom = basesel.alias("basefrom")
        joinfrom = tojoin.alias("joinfrom")
        sel = select(basefrom.c.a)

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
        sel = select(col)

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
                self.table1.c.col1,
                self.table1.c.col2,
                self.table1.c.col3,
                self.table1.c.colx,
                null().label("coly"),
            )
            .union(
                select(
                    self.table2.c.col1,
                    self.table2.c.col2,
                    self.table2.c.col3,
                    null().label("colx"),
                    self.table2.c.coly,
                )
            )
            .alias("analias")
        )
        s1 = self.table1.select().set_label_style(
            LABEL_STYLE_TABLENAME_PLUS_COL
        )
        s2 = self.table2.select().set_label_style(
            LABEL_STYLE_TABLENAME_PLUS_COL
        )
        with self._c_deprecated():
            assert u.corresponding_column(s1.c.table1_col2) is u.c.col2
            assert u.corresponding_column(s2.c.table2_col2) is u.c.col2
            assert u.corresponding_column(s2.c.table2_coly) is u.c.coly
            assert s2.c.corresponding_column(u.c.coly) is s2.c.table2_coly

    def test_join_alias(self):
        j1 = self.table1.join(self.table2)

        with testing.expect_deprecated_20(
            r"The Join.alias\(\) method is considered legacy"
        ):
            self.assert_compile(
                j1.alias(),
                "SELECT table1.col1 AS table1_col1, table1.col2 AS "
                "table1_col2, table1.col3 AS table1_col3, table1.colx "
                "AS table1_colx, table2.col1 AS table2_col1, "
                "table2.col2 AS table2_col2, table2.col3 AS table2_col3, "
                "table2.coly AS table2_coly FROM table1 JOIN table2 "
                "ON table1.col1 = table2.col2",
            )

        with testing.expect_deprecated_20(
            r"The Join.alias\(\) method is considered legacy"
        ):
            self.assert_compile(
                j1.alias(flat=True),
                "table1 AS table1_1 JOIN table2 AS table2_1 "
                "ON table1_1.col1 = table2_1.col2",
            )

    def test_join_against_self_implicit_subquery(self):
        jj = select(self.table1.c.col1.label("bar_col1"))
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

        with testing.expect_deprecated(
            r"The Join.alias\(\) method is considered legacy"
        ):
            j2 = jjj.alias("foo")
            assert (
                j2.corresponding_column(self.table1.c.col1) is j2.c.table1_col1
            )

    def test_select_labels(self):
        a = self.table1.select().set_label_style(
            LABEL_STYLE_TABLENAME_PLUS_COL
        )
        j = join(a._implicit_subquery, self.table2)

        criterion = a._implicit_subquery.c.table1_col1 == self.table2.c.col2
        self.assert_(criterion.compare(j.onclause))


class QuoteTest(fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = "default"

    def test_literal_column_label_embedded_select_samename_explicit_quote(
        self,
    ):
        col = sql.literal_column("NEEDS QUOTES").label(
            quoted_name("NEEDS QUOTES", True)
        )

        with testing.expect_deprecated(
            r"The SelectBase.select\(\) method is deprecated"
        ):
            self.assert_compile(
                select(col).select(),
                'SELECT anon_1."NEEDS QUOTES" FROM '
                '(SELECT NEEDS QUOTES AS "NEEDS QUOTES") AS anon_1',
            )

    def test_literal_column_label_embedded_select_diffname_explicit_quote(
        self,
    ):
        col = sql.literal_column("NEEDS QUOTES").label(
            quoted_name("NEEDS QUOTES_", True)
        )

        with testing.expect_deprecated(
            r"The SelectBase.select\(\) method is deprecated"
        ):
            self.assert_compile(
                select(col).select(),
                'SELECT anon_1."NEEDS QUOTES_" FROM '
                '(SELECT NEEDS QUOTES AS "NEEDS QUOTES_") AS anon_1',
            )

    def test_literal_column_label_embedded_select_diffname(self):
        col = sql.literal_column("NEEDS QUOTES").label("NEEDS QUOTES_")

        with testing.expect_deprecated(
            r"The SelectBase.select\(\) method is deprecated"
        ):
            self.assert_compile(
                select(col).select(),
                'SELECT anon_1."NEEDS QUOTES_" FROM (SELECT NEEDS QUOTES AS '
                '"NEEDS QUOTES_") AS anon_1',
            )

    def test_literal_column_label_embedded_select_samename(self):
        col = sql.literal_column("NEEDS QUOTES").label("NEEDS QUOTES")

        with testing.expect_deprecated(
            r"The SelectBase.select\(\) method is deprecated"
        ):
            self.assert_compile(
                select(col).select(),
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
            stmt = select(table1.c.myid).select_from(
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
                    0,
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
        stmt = select(t)

        with self._expect_deprecated("Select", "whereclause", "where"):
            stmt.append_whereclause(t.c.q == 5)

        self.assert_compile(stmt, "SELECT t.q FROM t WHERE t.q = :q_1")

    def test_append_having(self):
        t = table("t", column("q"))
        stmt = select(t).group_by(t.c.q)

        with self._expect_deprecated("Select", "having", "having"):
            stmt.append_having(t.c.q == 5)

        self.assert_compile(
            stmt, "SELECT t.q FROM t GROUP BY t.q HAVING t.q = :q_1"
        )

    def test_append_order_by(self):
        t = table("t", column("q"), column("x"))
        stmt = select(t).where(t.c.q == 5)

        with self._expect_deprecated(
            "GenerativeSelect", "order_by", "order_by"
        ):
            stmt.append_order_by(t.c.x)

        self.assert_compile(
            stmt, "SELECT t.q, t.x FROM t WHERE t.q = :q_1 ORDER BY t.x"
        )

    def test_append_group_by(self):
        t = table("t", column("q"))
        stmt = select(t)

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

        inner = select(t2.c.p).where(t2.c.q == t1.c.q)

        with self._expect_deprecated("Select", "correlation", "correlate"):
            inner.append_correlation(t1)
        stmt = select(t1).where(t1.c.q == inner.scalar_subquery())

        self.assert_compile(
            stmt,
            "SELECT t1.q FROM t1 WHERE t1.q = "
            "(SELECT t2.p FROM t2 WHERE t2.q = t1.q)",
        )

    def test_append_column(self):
        t1 = table("t1", column("q"), column("p"))
        stmt = select(t1.c.q)
        with self._expect_deprecated("Select", "column", "add_columns"):
            stmt.append_column(t1.c.p)
        self.assert_compile(stmt, "SELECT t1.q, t1.p FROM t1")

    def test_append_prefix(self):
        t1 = table("t1", column("q"), column("p"))
        stmt = select(t1.c.q)
        with self._expect_deprecated("Select", "prefix", "prefix_with"):
            stmt.append_prefix("FOO BAR")
        self.assert_compile(stmt, "SELECT FOO BAR t1.q FROM t1")

    def test_append_from(self):
        t1 = table("t1", column("q"))
        t2 = table("t2", column("q"))

        stmt = select(t1)
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
        conn.execute(cls.tables.content.insert(), dict(type="t1"))

        if testing.requires.schemas.enabled:
            conn.execute(
                cls.tables["%s.wschema" % testing.config.test_schema].insert(),
                dict(b="a1", q="c1"),
            )

    def test_column_label_overlap_fallback(self, connection):
        content, bar = self.tables.content, self.tables.bar
        row = connection.execute(
            select(content.c.type.label("content_type"))
        ).first()

        not_in(content.c.type, row)
        not_in(bar.c.content_type, row)

        with testing.expect_deprecated(
            "Retrieving row values using Column objects "
            "with only matching names"
        ):
            in_(sql.column("content_type"), row)

        row = connection.execute(
            select(func.now().label("content_type"))
        ).first()
        not_in(content.c.type, row)
        not_in(bar.c.content_type, row)
        with testing.expect_deprecated(
            "Retrieving row values using Column objects "
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
        stmt = select(a, b).select_from(table("keyed2"))
        row = connection.execute(stmt).first()

        with testing.expect_deprecated(
            "Retrieving row values using Column objects "
            "with only matching names"
        ):
            in_(keyed2.c.a, row)
        with testing.expect_deprecated(
            "Retrieving row values using Column objects "
            "with only matching names"
        ):
            in_(keyed2.c.b, row)

    def test_columnclause_schema_column_two(self, connection):
        keyed2 = self.tables.keyed2

        a, b = sql.column("a"), sql.column("b")
        stmt = select(keyed2.c.a, keyed2.c.b)
        row = connection.execute(stmt).first()

        with testing.expect_deprecated(
            "Retrieving row values using Column objects "
            "with only matching names"
        ):
            in_(a, row)
        with testing.expect_deprecated(
            "Retrieving row values using Column objects "
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
            "Retrieving row values using Column objects "
            "with only matching names"
        ):
            in_(keyed2.c.a, row)
        with testing.expect_deprecated(
            "Retrieving row values using Column objects "
            "with only matching names"
        ):
            in_(keyed2.c.b, row)
        with testing.expect_deprecated(
            "Retrieving row values using Column objects "
            "with only matching names"
        ):
            in_(a, row)
        with testing.expect_deprecated(
            "Retrieving row values using Column objects "
            "with only matching names"
        ):
            in_(b, row)
        with testing.expect_deprecated(
            "Retrieving row values using Column objects "
            "with only matching names",
            "The SelectBase.c and SelectBase.columns",
        ):
            in_(stmt.c.a, row)
        with testing.expect_deprecated(
            "Retrieving row values using Column objects "
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
            "Retrieving row values using Column objects "
            "with only matching names"
        ):
            in_(keyed2.c.a, row)
        with testing.expect_deprecated(
            "Retrieving row values using Column objects "
            "with only matching names"
        ):
            in_(keyed2.c.b, row)
        with testing.expect_deprecated(
            "Retrieving row values using Column objects "
            "with only matching names",
            "The SelectBase.c and SelectBase.columns",
        ):
            in_(stmt.c.keyed2_a, row)
        with testing.expect_deprecated(
            "Retrieving row values using Column objects "
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
            "Retrieving row values using Column objects "
            "with only matching names"
        ):
            in_(keyed2.c.a, row)
        with testing.expect_deprecated(
            "Retrieving row values using Column objects "
            "with only matching names"
        ):
            in_(keyed2.c.b, row)
        with testing.expect_deprecated(
            "Retrieving row values using Column objects "
            "with only matching names",
            "The SelectBase.c and SelectBase.columns",
        ):
            in_(stmt.c.keyed2_a, row)
        with testing.expect_deprecated(
            "Retrieving row values using Column objects "
            "with only matching names",
            "The SelectBase.c and SelectBase.columns",
        ):
            in_(stmt.c.keyed2_b, row)


class PKIncrementTest(fixtures.TablesTest):
    run_define_tables = "each"
    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "aitable",
            metadata,
            Column(
                "id",
                Integer,
                Sequence("ai_id_seq", optional=True),
                primary_key=True,
            ),
            Column("int1", Integer),
            Column("str1", String(20)),
        )

    def _test_autoincrement(self, connection):
        aitable = self.tables.aitable

        ids = set()
        rs = connection.execute(aitable.insert(), int1=1)
        last = rs.inserted_primary_key[0]
        self.assert_(last)
        self.assert_(last not in ids)
        ids.add(last)

        rs = connection.execute(aitable.insert(), str1="row 2")
        last = rs.inserted_primary_key[0]
        self.assert_(last)
        self.assert_(last not in ids)
        ids.add(last)

        rs = connection.execute(aitable.insert(), int1=3, str1="row 3")
        last = rs.inserted_primary_key[0]
        self.assert_(last)
        self.assert_(last not in ids)
        ids.add(last)

        rs = connection.execute(
            aitable.insert().values({"int1": func.length("four")})
        )
        last = rs.inserted_primary_key[0]
        self.assert_(last)
        self.assert_(last not in ids)
        ids.add(last)

        eq_(
            ids,
            set(
                range(
                    testing.db.dialect.default_sequence_base,
                    testing.db.dialect.default_sequence_base + 4,
                )
            ),
        )

        eq_(
            list(connection.execute(aitable.select().order_by(aitable.c.id))),
            [
                (testing.db.dialect.default_sequence_base, 1, None),
                (testing.db.dialect.default_sequence_base + 1, None, "row 2"),
                (testing.db.dialect.default_sequence_base + 2, 3, "row 3"),
                (testing.db.dialect.default_sequence_base + 3, 4, None),
            ],
        )

    def test_autoincrement_autocommit(self):
        with testing.db.connect() as conn:
            with testing.expect_deprecated_20(
                "The current statement is being autocommitted using "
                "implicit autocommit, "
            ):
                self._test_autoincrement(conn)


class ConnectionlessCursorResultTest(fixtures.TablesTest):
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

    def test_connectionless_autoclose_rows_exhausted(self):
        users = self.tables.users
        with testing.db.begin() as conn:
            conn.execute(users.insert(), dict(user_id=1, user_name="john"))

        with testing.expect_deprecated_20(
            r"The (?:Executable|Engine)\.(?:execute|scalar)\(\) method"
        ):
            result = testing.db.execute(text("select * from users"))
        connection = result.connection
        assert not connection.closed
        eq_(result.fetchone(), (1, "john"))
        assert not connection.closed
        eq_(result.fetchone(), None)
        assert connection.closed

    @testing.requires.returning
    def test_connectionless_autoclose_crud_rows_exhausted(self):
        users = self.tables.users
        stmt = (
            users.insert()
            .values(user_id=1, user_name="john")
            .returning(users.c.user_id)
        )
        with testing.expect_deprecated_20(
            r"The (?:Executable|Engine)\.(?:execute|scalar)\(\) method"
        ):
            result = testing.db.execute(stmt)
        connection = result.connection
        assert not connection.closed
        eq_(result.fetchone(), (1,))
        assert not connection.closed
        eq_(result.fetchone(), None)
        assert connection.closed

    def test_connectionless_autoclose_no_rows(self):
        with testing.expect_deprecated_20(
            r"The (?:Executable|Engine)\.(?:execute|scalar)\(\) method"
        ):
            result = testing.db.execute(text("select * from users"))
        connection = result.connection
        assert not connection.closed
        eq_(result.fetchone(), None)
        assert connection.closed

    @testing.requires.updateable_autoincrement_pks
    def test_connectionless_autoclose_no_metadata(self):
        with testing.expect_deprecated_20(
            r"The (?:Executable|Engine)\.(?:execute|scalar)\(\) method"
        ):
            result = testing.db.execute(text("update users set user_id=5"))
        connection = result.connection
        assert connection.closed

        assert_raises_message(
            exc.ResourceClosedError,
            "This result object does not return rows.",
            result.fetchone,
        )
        assert_raises_message(
            exc.ResourceClosedError,
            "This result object does not return rows.",
            result.keys,
        )


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
            [
                dict(user_id=1, user_name="john"),
                dict(user_id=2, user_name="jack"),
            ],
        )

    def test_column_accessor_textual_select(self, connection):
        users = self.tables.users

        with testing.expect_deprecated(
            "Retrieving row values using Column objects "
            "with only matching names",
            "Using non-integer/slice indices on Row is "
            "deprecated and will be removed in version 2.0",
        ):
            # this will create column() objects inside
            # the select(), these need to match on name anyway
            r = connection.execute(
                select(column("user_id"), column("user_name"))
                .select_from(table("users"))
                .where(text("user_id=2"))
            ).first()

            eq_(r[users.c.user_id], 2)

        r._keymap.pop(users.c.user_id)  # reset lookup
        with testing.expect_deprecated(
            "Retrieving row values using Column objects "
            "with only matching names"
        ):
            eq_(r._mapping[users.c.user_id], 2)

        with testing.expect_deprecated(
            "Retrieving row values using Column objects "
            "with only matching names"
        ):
            eq_(r._mapping[users.c.user_name], "jack")

    def test_column_accessor_basic_text(self, connection):
        users = self.tables.users

        with testing.expect_deprecated(
            "Using non-integer/slice indices on Row is deprecated "
            "and will be removed in version 2.0",
            "Retrieving row values using Column objects "
            "with only matching names",
        ):
            r = connection.execute(
                text("select * from users where user_id=2")
            ).first()

            eq_(r[users.c.user_id], 2)

        r._keymap.pop(users.c.user_id)
        with testing.expect_deprecated(
            "Retrieving row values using Column objects "
            "with only matching names"
        ):
            eq_(r._mapping[users.c.user_id], 2)

        with testing.expect_deprecated(
            "Using non-integer/slice indices on Row is deprecated "
            "and will be removed in version 2.0",
            "Retrieving row values using Column objects "
            "with only matching names",
        ):
            eq_(r[users.c.user_name], "jack")

        r._keymap.pop(users.c.user_name)
        with testing.expect_deprecated(
            "Retrieving row values using Column objects "
            "with only matching names"
        ):
            eq_(r._mapping[users.c.user_name], "jack")

    @testing.provide_metadata
    def test_column_label_overlap_fallback(self, connection):
        content = Table("content", self.metadata, Column("type", String(30)))
        bar = Table("bar", self.metadata, Column("content_type", String(30)))
        self.metadata.create_all(testing.db)
        connection.execute(content.insert().values(type="t1"))

        row = connection.execute(
            content.select().set_label_style(LABEL_STYLE_TABLENAME_PLUS_COL)
        ).first()
        in_(content.c.type, row._mapping)
        not_in(bar.c.content_type, row)
        with testing.expect_deprecated(
            "Retrieving row values using Column objects "
            "with only matching names"
        ):
            in_(sql.column("content_type"), row)

        row = connection.execute(
            select(content.c.type.label("content_type"))
        ).first()
        with testing.expect_deprecated(
            "Retrieving row values using Column objects "
            "with only matching names"
        ):
            in_(content.c.type, row)

        not_in(bar.c.content_type, row)

        with testing.expect_deprecated(
            "Retrieving row values using Column objects "
            "with only matching names"
        ):
            in_(sql.column("content_type"), row)

        row = connection.execute(
            select(func.now().label("content_type"))
        ).first()

        not_in(content.c.type, row)

        not_in(bar.c.content_type, row)

        with testing.expect_deprecated(
            "Retrieving row values using Column objects "
            "with only matching names"
        ):
            in_(sql.column("content_type"), row)

    def test_pickled_rows(self):
        users = self.tables.users
        addresses = self.tables.addresses
        with testing.db.begin() as conn:
            conn.execute(users.delete())
            conn.execute(
                users.insert(),
                [
                    {"user_id": 7, "user_name": "jack"},
                    {"user_id": 8, "user_name": "ed"},
                    {"user_id": 9, "user_name": "fred"},
                ],
            )

            for pickle in False, True:
                for use_labels in False, True:
                    stmt = users.select()
                    if use_labels:
                        stmt = stmt.set_label_style(
                            LABEL_STYLE_TABLENAME_PLUS_COL
                        )

                    result = conn.execute(
                        stmt.order_by(users.c.user_id)
                    ).fetchall()

                    if pickle:
                        result = util.pickle.loads(util.pickle.dumps(result))

                    if pickle:
                        with testing.expect_deprecated(
                            "Retrieving row values using Column objects "
                            "from a row that was unpickled"
                        ):
                            eq_(result[0]._mapping[users.c.user_id], 7)

                        result[0]._keymap.pop(users.c.user_id)
                        with testing.expect_deprecated(
                            "Retrieving row values using Column objects "
                            "from a row that was unpickled"
                        ):
                            eq_(result[0]._mapping[users.c.user_id], 7)

                        with testing.expect_deprecated(
                            "Retrieving row values using Column objects "
                            "from a row that was unpickled"
                        ):
                            eq_(result[0]._mapping[users.c.user_name], "jack")

                        result[0]._keymap.pop(users.c.user_name)
                        with testing.expect_deprecated(
                            "Retrieving row values using Column objects "
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
                            "Retrieving row values using Column objects "
                            "from a row that was unpickled"
                        ):
                            eq_(result[0]._mapping[addresses.c.user_id], 7)

                        result[0]._keymap.pop(addresses.c.user_id)
                        with testing.expect_deprecated(
                            "Retrieving row values using Column objects "
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
                    literal_column("1").label("SOMECOL"),
                    literal_column("1").label("SOMECOL"),
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
            row = connection.execute(select(col)).first()
            eq_(row["foo"], 1)

        eq_(row._mapping["foo"], 1)

    def test_row_getitem_column(self, connection):
        col = literal_column("1").label("foo")

        with testing.expect_deprecated(
            "Using non-integer/slice indices on Row is deprecated "
            "and will be removed in version 2.0;"
        ):
            row = connection.execute(select(col)).first()
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
                        literal_column("1").label("case_insensitive"),
                        literal_column("2").label("CaseSensitive"),
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
                        literal_column("1").label("case_insensitive"),
                        literal_column("2").label("CaseSensitive"),
                        text("3 AS screw_up_the_cols"),
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
            r"The Row.keys\(\) method is considered legacy "
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
        not_in("foobar", r)

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
            cls.tables.text1.insert(),
            [dict(a="a1", b="b1", c="c1", d="d1")],
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
            "Retrieving row values using Column objects "
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
            "Retrieving row values using Column objects "
            "with only matching names"
        ):
            eq_(row._mapping[text1.c.a], "a1")

        # key fallback rules still match this to a column
        # unambiguously based on its name
        with testing.expect_deprecated(
            "Retrieving row values using Column objects "
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
                return conn.execute(select(text("12"))).scalar()
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

            eq_(conn.execute(select(table)).first(), (5, 12))


class DMLTest(_UpdateFromTestBase, fixtures.TablesTest, AssertsCompiledSQL):
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
                default=select(func.coalesce(func.max(foo.c.id))),
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
        )

        with testing.expect_deprecated_20(
            "The insert.inline parameter will be removed in SQLAlchemy 2.0."
        ):
            stmt = table.insert(inline=True)

        self.assert_compile(
            stmt,
            "INSERT INTO sometable (foo) VALUES (foobar())",
            params={},
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
                onupdate=select(func.coalesce(func.max(foo.c.id))),
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
        )

    def test_update_dialect_kwargs(self):
        t = table("foo", column("bar"))

        with testing.expect_deprecated_20("Passing dialect keyword arguments"):
            stmt = t.update(mysql_limit=10)

        self.assert_compile(
            stmt, "UPDATE foo SET bar=%s LIMIT 10", dialect="mysql"
        )

    def test_update_whereclause(self):
        table1 = table(
            "mytable",
            Column("myid", Integer),
            Column("name", String(30)),
        )

        with testing.expect_deprecated_20(
            "The update.whereclause parameter will be "
            "removed in SQLAlchemy 2.0"
        ):
            self.assert_compile(
                table1.update(table1.c.myid == 7),
                "UPDATE mytable SET myid=:myid, name=:name "
                "WHERE mytable.myid = :myid_1",
            )

    def test_update_values(self):
        table1 = table(
            "mytable",
            Column("myid", Integer),
            Column("name", String(30)),
        )

        with testing.expect_deprecated_20(
            "The update.values parameter will be removed in SQLAlchemy 2.0"
        ):
            self.assert_compile(
                table1.update(values={table1.c.myid: 7}),
                "UPDATE mytable SET myid=:myid",
            )

    def test_delete_whereclause(self):
        table1 = table(
            "mytable",
            Column("myid", Integer),
        )

        with testing.expect_deprecated_20(
            "The delete.whereclause parameter will be "
            "removed in SQLAlchemy 2.0"
        ):
            self.assert_compile(
                table1.delete(table1.c.myid == 7),
                "DELETE FROM mytable WHERE mytable.myid = :myid_1",
            )

    def test_update_ordered_parameters_fire_onupdate(self):
        table = self.tables.update_w_default

        values = [(table.c.y, table.c.x + 5), ("x", 10)]

        with testing.expect_deprecated_20(
            "The update.preserve_parameter_order parameter will be "
            "removed in SQLAlchemy 2.0."
        ):
            self.assert_compile(
                table.update(preserve_parameter_order=True).values(values),
                "UPDATE update_w_default "
                "SET ycol=(update_w_default.x + :x_1), "
                "x=:x, data=:data",
            )

    def test_update_ordered_parameters_override_onupdate(self):
        table = self.tables.update_w_default

        values = [
            (table.c.y, table.c.x + 5),
            (table.c.data, table.c.x + 10),
            ("x", 10),
        ]

        with testing.expect_deprecated_20(
            "The update.preserve_parameter_order parameter will be "
            "removed in SQLAlchemy 2.0."
        ):
            self.assert_compile(
                table.update(preserve_parameter_order=True).values(values),
                "UPDATE update_w_default "
                "SET ycol=(update_w_default.x + :x_1), "
                "data=(update_w_default.x + :x_2), x=:x",
            )

    def test_update_ordered_parameters_oldstyle_1(self):
        table1 = self.tables.mytable

        # Confirm that we can pass values as list value pairs
        # note these are ordered *differently* from table.c
        values = [
            (table1.c.name, table1.c.name + "lala"),
            (table1.c.myid, func.do_stuff(table1.c.myid, literal("hoho"))),
        ]

        with testing.expect_deprecated_20(
            "The update.preserve_parameter_order parameter will be "
            "removed in SQLAlchemy 2.0.",
            "The update.whereclause parameter will be "
            "removed in SQLAlchemy 2.0",
            "The update.values parameter will be removed in SQLAlchemy 2.0",
        ):
            self.assert_compile(
                update(
                    table1,
                    (table1.c.myid == func.hoho(4))
                    & (
                        table1.c.name
                        == literal("foo") + table1.c.name + literal("lala")
                    ),
                    preserve_parameter_order=True,
                    values=values,
                ),
                "UPDATE mytable "
                "SET "
                "name=(mytable.name || :name_1), "
                "myid=do_stuff(mytable.myid, :param_1) "
                "WHERE "
                "mytable.myid = hoho(:hoho_1) AND "
                "mytable.name = :param_2 || mytable.name || :param_3",
            )

    def test_update_ordered_parameters_oldstyle_2(self):
        table1 = self.tables.mytable

        # Confirm that we can pass values as list value pairs
        # note these are ordered *differently* from table.c
        values = [
            (table1.c.name, table1.c.name + "lala"),
            ("description", "some desc"),
            (table1.c.myid, func.do_stuff(table1.c.myid, literal("hoho"))),
        ]

        with testing.expect_deprecated_20(
            "The update.preserve_parameter_order parameter will be "
            "removed in SQLAlchemy 2.0.",
            "The update.whereclause parameter will be "
            "removed in SQLAlchemy 2.0",
        ):
            self.assert_compile(
                update(
                    table1,
                    (table1.c.myid == func.hoho(4))
                    & (
                        table1.c.name
                        == literal("foo") + table1.c.name + literal("lala")
                    ),
                    preserve_parameter_order=True,
                ).values(values),
                "UPDATE mytable "
                "SET "
                "name=(mytable.name || :name_1), "
                "description=:description, "
                "myid=do_stuff(mytable.myid, :param_1) "
                "WHERE "
                "mytable.myid = hoho(:hoho_1) AND "
                "mytable.name = :param_2 || mytable.name || :param_3",
            )

    def test_update_preserve_order_reqs_listtups(self):
        table1 = self.tables.mytable

        with testing.expect_deprecated_20(
            "The update.preserve_parameter_order parameter will be "
            "removed in SQLAlchemy 2.0."
        ):
            testing.assert_raises_message(
                ValueError,
                r"When preserve_parameter_order is True, values\(\) "
                r"only accepts a list of 2-tuples",
                table1.update(preserve_parameter_order=True).values,
                {"description": "foo", "name": "bar"},
            )

    @testing.fixture
    def randomized_param_order_update(self):
        from sqlalchemy.sql.dml import UpdateDMLState

        super_process_ordered_values = UpdateDMLState._process_ordered_values

        # this fixture is needed for Python 3.6 and above to work around
        # dictionaries being insert-ordered.  in python 2.7 the previous
        # logic fails pretty easily without this fixture.
        def _process_ordered_values(self, statement):
            super_process_ordered_values(self, statement)

            tuples = list(self._dict_parameters.items())
            random.shuffle(tuples)
            self._dict_parameters = dict(tuples)

        dialect = default.StrCompileDialect()
        dialect.paramstyle = "qmark"
        dialect.positional = True

        with mock.patch.object(
            UpdateDMLState, "_process_ordered_values", _process_ordered_values
        ):
            yield

    def random_update_order_parameters():
        from sqlalchemy import ARRAY

        t = table(
            "foo",
            column("data1", ARRAY(Integer)),
            column("data2", ARRAY(Integer)),
            column("data3", ARRAY(Integer)),
            column("data4", ARRAY(Integer)),
        )

        idx_to_value = [
            (t.c.data1, 5, 7),
            (t.c.data2, 10, 18),
            (t.c.data3, 8, 4),
            (t.c.data4, 12, 14),
        ]

        def combinations():
            while True:
                random.shuffle(idx_to_value)
                yield list(idx_to_value)

        return testing.combinations(
            *[
                (t, combination)
                for i, combination in zip(range(10), combinations())
            ],
            argnames="t, idx_to_value"
        )

    @random_update_order_parameters()
    def test_update_to_expression_ppo(
        self, randomized_param_order_update, t, idx_to_value
    ):
        dialect = default.StrCompileDialect()
        dialect.paramstyle = "qmark"
        dialect.positional = True

        with testing.expect_deprecated_20(
            "The update.preserve_parameter_order parameter will be "
            "removed in SQLAlchemy 2.0."
        ):
            stmt = t.update(preserve_parameter_order=True).values(
                [(col[idx], val) for col, idx, val in idx_to_value]
            )

        self.assert_compile(
            stmt,
            "UPDATE foo SET %s"
            % (
                ", ".join(
                    "%s[?]=?" % col.key for col, idx, val in idx_to_value
                )
            ),
            dialect=dialect,
            checkpositional=tuple(
                itertools.chain.from_iterable(
                    (idx, val) for col, idx, val in idx_to_value
                )
            ),
        )


class TableDeprecationTest(fixtures.TestBase):
    def test_mustexists(self):
        with testing.expect_deprecated("Deprecated alias of .*must_exist"):

            with testing.expect_raises_message(
                exc.InvalidRequestError, "Table 'foo' not defined"
            ):
                Table("foo", MetaData(), mustexist=True)


class LegacyOperatorTest(AssertsCompiledSQL, fixtures.TestBase):
    """
    Several operators were renamed for SqlAlchemy 2.0 in #5429 and #5435

    This test class is designed to ensure the deprecated legacy operators
    are still available and equivalent to their modern replacements.

    These tests should be removed when the legacy operators are removed.

    Note: Although several of these tests simply check to see if two functions
    are the same, some platforms in the test matrix require an `==` comparison
    and will fail on an `is` comparison.

    .. seealso::

        :ref:`change_5429`
        :ref:`change_5435`
    """

    __dialect__ = "default"

    def test_issue_5429_compile(self):
        self.assert_compile(column("x").isnot("foo"), "x IS NOT :x_1")

        self.assert_compile(
            column("x").notin_(["foo", "bar"]), "x NOT IN ([POSTCOMPILE_x_1])"
        )

    def test_issue_5429_operators(self):
        # functions
        # is_not
        assert hasattr(operators, "is_not")  # modern
        assert hasattr(operators, "isnot")  # legacy
        is_(operators.is_not, operators.isnot)
        # not_in
        assert hasattr(operators, "not_in_op")  # modern
        assert hasattr(operators, "notin_op")  # legacy
        is_(operators.not_in_op, operators.notin_op)

        # precedence mapping
        # since they are the same item, only 1 precedence check needed
        # is_not
        assert operators.isnot in operators._PRECEDENCE  # legacy

        # not_in_op
        assert operators.notin_op in operators._PRECEDENCE  # legacy

        # ColumnOperators
        # is_not
        assert hasattr(operators.ColumnOperators, "is_not")  # modern
        assert hasattr(operators.ColumnOperators, "isnot")  # legacy
        assert (
            operators.ColumnOperators.is_not == operators.ColumnOperators.isnot
        )
        # not_in
        assert hasattr(operators.ColumnOperators, "not_in")  # modern
        assert hasattr(operators.ColumnOperators, "notin_")  # legacy
        assert (
            operators.ColumnOperators.not_in
            == operators.ColumnOperators.notin_
        )

    def test_issue_5429_assertions(self):
        """
        2) ensure compatibility across sqlalchemy.testing.assertions
        """
        # functions
        # is_not
        assert hasattr(assertions, "is_not")  # modern
        assert hasattr(assertions, "is_not_")  # legacy
        assert assertions.is_not == assertions.is_not_
        # not_in
        assert hasattr(assertions, "not_in")  # modern
        assert hasattr(assertions, "not_in_")  # legacy
        assert assertions.not_in == assertions.not_in_

    @testing.combinations(
        (
            "is_not_distinct_from",
            "isnot_distinct_from",
            "a IS NOT DISTINCT FROM b",
        ),
        ("not_contains_op", "notcontains_op", "a NOT LIKE '%' || b || '%'"),
        ("not_endswith_op", "notendswith_op", "a NOT LIKE '%' || b"),
        ("not_ilike_op", "notilike_op", "lower(a) NOT LIKE lower(b)"),
        ("not_like_op", "notlike_op", "a NOT LIKE b"),
        ("not_match_op", "notmatch_op", "NOT a MATCH b"),
        ("not_startswith_op", "notstartswith_op", "a NOT LIKE b || '%'"),
    )
    def test_issue_5435_binary_operators(self, modern, legacy, txt):
        a, b = column("a"), column("b")
        _op_modern = getattr(operators, modern)
        _op_legacy = getattr(operators, legacy)

        eq_(str(_op_modern(a, b)), txt)

        eq_(str(_op_modern(a, b)), str(_op_legacy(a, b)))

    @testing.combinations(
        ("nulls_first_op", "nullsfirst_op", "a NULLS FIRST"),
        ("nulls_last_op", "nullslast_op", "a NULLS LAST"),
    )
    def test_issue_5435_unary_operators(self, modern, legacy, txt):
        a = column("a")
        _op_modern = getattr(operators, modern)
        _op_legacy = getattr(operators, legacy)

        eq_(str(_op_modern(a)), txt)

        eq_(str(_op_modern(a)), str(_op_legacy(a)))

    @testing.combinations(
        ("not_between_op", "notbetween_op", "a NOT BETWEEN b AND c")
    )
    def test_issue_5435_between_operators(self, modern, legacy, txt):
        a, b, c = column("a"), column("b"), column("c")
        _op_modern = getattr(operators, modern)
        _op_legacy = getattr(operators, legacy)

        eq_(str(_op_modern(a, b, c)), txt)

        eq_(str(_op_modern(a, b, c)), str(_op_legacy(a, b, c)))

    @testing.combinations(
        ("is_false", "isfalse", True),
        ("is_true", "istrue", True),
        ("is_not_distinct_from", "isnot_distinct_from", True),
        ("not_between_op", "notbetween_op", True),
        ("not_contains_op", "notcontains_op", False),
        ("not_endswith_op", "notendswith_op", False),
        ("not_ilike_op", "notilike_op", True),
        ("not_like_op", "notlike_op", True),
        ("not_match_op", "notmatch_op", True),
        ("not_startswith_op", "notstartswith_op", False),
        ("nulls_first_op", "nullsfirst_op", False),
        ("nulls_last_op", "nullslast_op", False),
    )
    def test_issue_5435_operators_precedence(
        self, _modern, _legacy, _in_precedence
    ):
        # (modern, legacy, in_precendence)
        # core operators
        assert hasattr(operators, _modern)
        assert hasattr(operators, _legacy)
        _op_modern = getattr(operators, _modern)
        _op_legacy = getattr(operators, _legacy)
        assert _op_modern == _op_legacy
        # since they are the same item, only 1 precedence check needed
        if _in_precedence:
            assert _op_legacy in operators._PRECEDENCE
        else:
            assert _op_legacy not in operators._PRECEDENCE

    @testing.combinations(
        ("is_not_distinct_from", "isnot_distinct_from"),
        ("not_ilike", "notilike"),
        ("not_like", "notlike"),
        ("nulls_first", "nullsfirst"),
        ("nulls_last", "nullslast"),
    )
    def test_issue_5435_operators_column(self, _modern, _legacy):
        # (modern, legacy)
        # Column operators
        assert hasattr(operators.ColumnOperators, _modern)
        assert hasattr(operators.ColumnOperators, _legacy)
        _op_modern = getattr(operators.ColumnOperators, _modern)
        _op_legacy = getattr(operators.ColumnOperators, _legacy)
        assert _op_modern == _op_legacy


class LegacySequenceExecTest(fixtures.TestBase):
    __requires__ = ("sequences",)
    __backend__ = True

    @classmethod
    def setup_test_class(cls):
        cls.seq = Sequence("my_sequence")
        cls.seq.create(testing.db)

    @classmethod
    def teardown_test_class(cls):
        cls.seq.drop(testing.db)

    def _assert_seq_result(self, ret):
        """asserts return of next_value is an int"""

        assert isinstance(ret, util.int_types)
        assert ret >= testing.db.dialect.default_sequence_base

    def test_implicit_connectionless(self):
        with testing.expect_deprecated_20(
            r"The MetaData.bind argument is deprecated"
        ):
            s = Sequence("my_sequence", metadata=MetaData(testing.db))

        with testing.expect_deprecated_20(
            r"The DefaultGenerator.execute\(\) method is considered legacy "
            "as of the 1.x",
        ):
            self._assert_seq_result(s.execute())

    def test_explicit(self, connection):
        s = Sequence("my_sequence")
        with testing.expect_deprecated_20(
            r"The DefaultGenerator.execute\(\) method is considered legacy"
        ):
            self._assert_seq_result(s.execute(connection))

    def test_explicit_optional(self):
        """test dialect executes a Sequence, returns nextval, whether
        or not "optional" is set"""

        s = Sequence("my_sequence", optional=True)
        with testing.expect_deprecated_20(
            r"The DefaultGenerator.execute\(\) method is considered legacy"
        ):
            self._assert_seq_result(s.execute(testing.db))

    def test_func_implicit_connectionless_execute(self):
        """test func.next_value().execute()/.scalar() works
        with connectionless execution."""

        with testing.expect_deprecated_20(
            r"The MetaData.bind argument is deprecated"
        ):
            s = Sequence("my_sequence", metadata=MetaData(testing.db))
        with testing.expect_deprecated_20(
            r"The Executable.execute\(\) method is considered legacy"
        ):
            self._assert_seq_result(s.next_value().execute().scalar())

    def test_func_explicit(self):
        s = Sequence("my_sequence")
        with testing.expect_deprecated_20(
            r"The Engine.scalar\(\) method is considered legacy"
        ):
            self._assert_seq_result(testing.db.scalar(s.next_value()))

    def test_func_implicit_connectionless_scalar(self):
        """test func.next_value().execute()/.scalar() works. """

        with testing.expect_deprecated_20(
            r"The MetaData.bind argument is deprecated"
        ):
            s = Sequence("my_sequence", metadata=MetaData(testing.db))
        with testing.expect_deprecated_20(
            r"The Executable.execute\(\) method is considered legacy"
        ):
            self._assert_seq_result(s.next_value().scalar())

    def test_func_embedded_select(self):
        """test can use next_value() in select column expr"""

        s = Sequence("my_sequence")
        with testing.expect_deprecated_20(
            r"The Engine.scalar\(\) method is considered legacy"
        ):
            self._assert_seq_result(testing.db.scalar(select(s.next_value())))


class DDLDeprecatedBindTest(fixtures.TestBase):
    def teardown_test(self):
        with testing.db.begin() as conn:
            if inspect(conn).has_table("foo"):
                conn.execute(schema.DropTable(table("foo")))

    def test_bind_ddl_deprecated(self, connection):
        with testing.expect_deprecated_20(
            "The DDL.bind argument is deprecated"
        ):
            ddl = schema.DDL("create table foo(id integer)", bind=connection)

        with testing.expect_deprecated_20(
            r"The DDLElement.execute\(\) method is considered legacy"
        ):
            ddl.execute()

    def test_bind_create_table_deprecated(self, connection):
        t1 = Table("foo", MetaData(), Column("id", Integer))

        with testing.expect_deprecated_20(
            "The CreateTable.bind argument is deprecated"
        ):
            ddl = schema.CreateTable(t1, bind=connection)

        with testing.expect_deprecated_20(
            r"The DDLElement.execute\(\) method is considered legacy"
        ):
            ddl.execute()

        is_true(inspect(connection).has_table("foo"))

    def test_bind_create_index_deprecated(self, connection):
        t1 = Table("foo", MetaData(), Column("id", Integer))
        t1.create(connection)

        idx = schema.Index("foo_idx", t1.c.id)

        with testing.expect_deprecated_20(
            "The CreateIndex.bind argument is deprecated"
        ):
            ddl = schema.CreateIndex(idx, bind=connection)

        with testing.expect_deprecated_20(
            r"The DDLElement.execute\(\) method is considered legacy"
        ):
            ddl.execute()

        is_true(
            "foo_idx"
            in [ix["name"] for ix in inspect(connection).get_indexes("foo")]
        )

    def test_bind_drop_table_deprecated(self, connection):
        t1 = Table("foo", MetaData(), Column("id", Integer))

        t1.create(connection)

        with testing.expect_deprecated_20(
            "The DropTable.bind argument is deprecated"
        ):
            ddl = schema.DropTable(t1, bind=connection)

        with testing.expect_deprecated_20(
            r"The DDLElement.execute\(\) method is considered legacy"
        ):
            ddl.execute()

        is_false(inspect(connection).has_table("foo"))

    def test_bind_drop_index_deprecated(self, connection):
        t1 = Table("foo", MetaData(), Column("id", Integer))
        idx = schema.Index("foo_idx", t1.c.id)
        t1.create(connection)

        is_true(
            "foo_idx"
            in [ix["name"] for ix in inspect(connection).get_indexes("foo")]
        )

        with testing.expect_deprecated_20(
            "The DropIndex.bind argument is deprecated"
        ):
            ddl = schema.DropIndex(idx, bind=connection)

        with testing.expect_deprecated_20(
            r"The DDLElement.execute\(\) method is considered legacy"
        ):
            ddl.execute()

        is_false(
            "foo_idx"
            in [ix["name"] for ix in inspect(connection).get_indexes("foo")]
        )

    @testing.combinations(
        (schema.AddConstraint,),
        (schema.DropConstraint,),
        (schema.CreateSequence,),
        (schema.DropSequence,),
        (schema.CreateSchema,),
        (schema.DropSchema,),
        (schema.SetTableComment,),
        (schema.DropTableComment,),
        (schema.SetColumnComment,),
        (schema.DropColumnComment,),
    )
    def test_bind_other_constructs(self, const):
        m1 = mock.Mock()

        with testing.expect_deprecated_20(
            "The DDLElement.bind argument is deprecated"
        ):
            c1 = const(m1, bind=testing.db)

            is_(c1.bind, testing.db)
