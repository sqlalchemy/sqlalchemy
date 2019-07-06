#! coding: utf-8

from sqlalchemy import alias
from sqlalchemy import bindparam
from sqlalchemy import Column
from sqlalchemy import column
from sqlalchemy import create_engine
from sqlalchemy import exc
from sqlalchemy import ForeignKey
from sqlalchemy import func
from sqlalchemy import Integer
from sqlalchemy import join
from sqlalchemy import literal_column
from sqlalchemy import MetaData
from sqlalchemy import null
from sqlalchemy import select
from sqlalchemy import sql
from sqlalchemy import String
from sqlalchemy import Table
from sqlalchemy import table
from sqlalchemy import testing
from sqlalchemy import text
from sqlalchemy import util
from sqlalchemy.engine import default
from sqlalchemy.schema import DDL
from sqlalchemy.sql import coercions
from sqlalchemy.sql import quoted_name
from sqlalchemy.sql import roles
from sqlalchemy.sql import util as sql_util
from sqlalchemy.sql import visitors
from sqlalchemy.sql.selectable import SelectStatementGrouping
from sqlalchemy.testing import assert_raises
from sqlalchemy.testing import assert_raises_message
from sqlalchemy.testing import AssertsCompiledSQL
from sqlalchemy.testing import engines
from sqlalchemy.testing import eq_
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import is_true
from sqlalchemy.testing import mock


class DeprecationWarningsTest(fixtures.TestBase):
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

    def test_join_condition_ignore_nonexistent_tables(self):
        m = MetaData()
        t1 = Table("t1", m, Column("id", Integer))
        t2 = Table(
            "t2", m, Column("id", Integer), Column("t1id", ForeignKey("t1.id"))
        )
        with testing.expect_deprecated(
            "The join_condition.ignore_nonexistent_tables "
            "parameter is deprecated"
        ):
            join_cond = sql_util.join_condition(
                t1, t2, ignore_nonexistent_tables=True
            )

        t1t2 = t1.join(t2)

        assert t1t2.onclause.compare(join_cond)

    def test_select_autocommit(self):
        with testing.expect_deprecated(
            "The select.autocommit parameter is deprecated and "
            "will be removed in a future release."
        ):
            select([column("x")], autocommit=True)

    def test_select_for_update(self):
        with testing.expect_deprecated(
            "The select.for_update parameter is deprecated and "
            "will be removed in a future release."
        ):
            select([column("x")], for_update=True)

    @testing.provide_metadata
    def test_table_useexisting(self):
        meta = self.metadata

        Table("t", meta, Column("x", Integer))
        meta.create_all()

        with testing.expect_deprecated(
            "The Table.useexisting parameter is deprecated and "
            "will be removed in a future release."
        ):
            Table("t", meta, useexisting=True, autoload_with=testing.db)

        with testing.expect_deprecated(
            "The Table.useexisting parameter is deprecated and "
            "will be removed in a future release."
        ):
            assert_raises_message(
                exc.ArgumentError,
                "useexisting is synonymous with extend_existing.",
                Table,
                "t",
                meta,
                useexisting=True,
                extend_existing=True,
                autoload_with=testing.db,
            )


class DDLListenerDeprecationsTest(fixtures.TestBase):
    def setup(self):
        self.bind = self.engine = engines.mock_engine()
        self.metadata = MetaData(self.bind)
        self.table = Table("t", self.metadata, Column("id", Integer))
        self.users = Table(
            "users",
            self.metadata,
            Column("user_id", Integer, primary_key=True),
            Column("user_name", String(40)),
        )

    def test_append_listener(self):
        metadata, table = self.metadata, self.table

        def fn(*a):
            return None

        with testing.expect_deprecated(".* is deprecated .*"):
            table.append_ddl_listener("before-create", fn)
        with testing.expect_deprecated(".* is deprecated .*"):
            assert_raises(
                exc.InvalidRequestError, table.append_ddl_listener, "blah", fn
            )

        with testing.expect_deprecated(".* is deprecated .*"):
            metadata.append_ddl_listener("before-create", fn)
        with testing.expect_deprecated(".* is deprecated .*"):
            assert_raises(
                exc.InvalidRequestError,
                metadata.append_ddl_listener,
                "blah",
                fn,
            )

    def test_deprecated_append_ddl_listener_table(self):
        metadata, users, engine = self.metadata, self.users, self.engine
        canary = []
        with testing.expect_deprecated(".* is deprecated .*"):
            users.append_ddl_listener(
                "before-create", lambda e, t, b: canary.append("mxyzptlk")
            )
        with testing.expect_deprecated(".* is deprecated .*"):
            users.append_ddl_listener(
                "after-create", lambda e, t, b: canary.append("klptzyxm")
            )
        with testing.expect_deprecated(".* is deprecated .*"):
            users.append_ddl_listener(
                "before-drop", lambda e, t, b: canary.append("xyzzy")
            )
        with testing.expect_deprecated(".* is deprecated .*"):
            users.append_ddl_listener(
                "after-drop", lambda e, t, b: canary.append("fnord")
            )

        metadata.create_all()
        assert "mxyzptlk" in canary
        assert "klptzyxm" in canary
        assert "xyzzy" not in canary
        assert "fnord" not in canary
        del engine.mock[:]
        canary[:] = []
        metadata.drop_all()
        assert "mxyzptlk" not in canary
        assert "klptzyxm" not in canary
        assert "xyzzy" in canary
        assert "fnord" in canary

    def test_deprecated_append_ddl_listener_metadata(self):
        metadata, engine = self.metadata, self.engine
        canary = []
        with testing.expect_deprecated(".* is deprecated .*"):
            metadata.append_ddl_listener(
                "before-create",
                lambda e, t, b, tables=None: canary.append("mxyzptlk"),
            )
        with testing.expect_deprecated(".* is deprecated .*"):
            metadata.append_ddl_listener(
                "after-create",
                lambda e, t, b, tables=None: canary.append("klptzyxm"),
            )
        with testing.expect_deprecated(".* is deprecated .*"):
            metadata.append_ddl_listener(
                "before-drop",
                lambda e, t, b, tables=None: canary.append("xyzzy"),
            )
        with testing.expect_deprecated(".* is deprecated .*"):
            metadata.append_ddl_listener(
                "after-drop",
                lambda e, t, b, tables=None: canary.append("fnord"),
            )

        metadata.create_all()
        assert "mxyzptlk" in canary
        assert "klptzyxm" in canary
        assert "xyzzy" not in canary
        assert "fnord" not in canary
        del engine.mock[:]
        canary[:] = []
        metadata.drop_all()
        assert "mxyzptlk" not in canary
        assert "klptzyxm" not in canary
        assert "xyzzy" in canary
        assert "fnord" in canary

    def test_filter_deprecated(self):
        cx = self.engine

        tbl = Table("t", MetaData(), Column("id", Integer))
        target = cx.name

        assert DDL("")._should_execute_deprecated("x", tbl, cx)
        with testing.expect_deprecated(".* is deprecated .*"):
            assert DDL("", on=target)._should_execute_deprecated("x", tbl, cx)
        with testing.expect_deprecated(".* is deprecated .*"):
            assert not DDL("", on="bogus")._should_execute_deprecated(
                "x", tbl, cx
            )
        with testing.expect_deprecated(".* is deprecated .*"):
            assert DDL(
                "", on=lambda d, x, y, z: True
            )._should_execute_deprecated("x", tbl, cx)
        with testing.expect_deprecated(".* is deprecated .*"):
            assert DDL(
                "", on=lambda d, x, y, z: z.engine.name != "bogus"
            )._should_execute_deprecated("x", tbl, cx)


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


class ForUpdateTest(fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = "default"

    def _assert_legacy(self, leg, read=False, nowait=False):
        t = table("t", column("c"))

        with testing.expect_deprecated(
            "The select.for_update parameter is deprecated and "
            "will be removed in a future release."
        ):
            s1 = select([t], for_update=leg)

        if leg is False:
            assert s1._for_update_arg is None
            assert s1.for_update is None
        else:
            eq_(s1._for_update_arg.read, read)
            eq_(s1._for_update_arg.nowait, nowait)
            eq_(s1.for_update, leg)

    def test_false_legacy(self):
        self._assert_legacy(False)

    def test_plain_true_legacy(self):
        self._assert_legacy(True)

    def test_read_legacy(self):
        self._assert_legacy("read", read=True)

    def test_nowait_legacy(self):
        self._assert_legacy("nowait", nowait=True)

    def test_read_nowait_legacy(self):
        self._assert_legacy("read_nowait", read=True, nowait=True)

    def test_unknown_mode(self):
        t = table("t", column("c"))

        with testing.expect_deprecated(
            "The select.for_update parameter is deprecated and "
            "will be removed in a future release."
        ):
            assert_raises_message(
                exc.ArgumentError,
                "Unknown for_update argument: 'unknown_mode'",
                t.select,
                t.c.c == 7,
                for_update="unknown_mode",
            )

    def test_legacy_setter(self):
        t = table("t", column("c"))
        s = select([t])
        s.for_update = "nowait"
        eq_(s._for_update_arg.nowait, True)


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

    def test_column_roles(self):
        stmt = select([self.table1.c.myid])

        for role in [
            roles.WhereHavingRole,
            roles.ExpressionElementRole,
            roles.ByOfRole,
            roles.OrderByRole,
            # roles.LabeledColumnExprRole
        ]:
            with testing.expect_deprecated(
                "coercing SELECT object to scalar "
                "subquery in a column-expression context is deprecated"
            ):
                coerced = coercions.expect(role, stmt)
                is_true(coerced.compare(stmt.scalar_subquery()))

            with testing.expect_deprecated(
                "coercing SELECT object to scalar "
                "subquery in a column-expression context is deprecated"
            ):
                coerced = coercions.expect(role, stmt.alias())
                is_true(coerced.compare(stmt.scalar_subquery()))

    def test_labeled_role(self):
        stmt = select([self.table1.c.myid])

        with testing.expect_deprecated(
            "coercing SELECT object to scalar "
            "subquery in a column-expression context is deprecated"
        ):
            coerced = coercions.expect(roles.LabeledColumnExprRole, stmt)
            is_true(coerced.compare(stmt.scalar_subquery().label(None)))

        with testing.expect_deprecated(
            "coercing SELECT object to scalar "
            "subquery in a column-expression context is deprecated"
        ):
            coerced = coercions.expect(
                roles.LabeledColumnExprRole, stmt.alias()
            )
            is_true(coerced.compare(stmt.scalar_subquery().label(None)))

    def test_scalar_select(self):

        with testing.expect_deprecated(
            "coercing SELECT object to scalar "
            "subquery in a column-expression context is deprecated"
        ):
            self.assert_compile(
                func.coalesce(select([self.table1.c.myid])),
                "coalesce((SELECT mytable.myid FROM mytable))",
            )

        with testing.expect_deprecated(
            "coercing SELECT object to scalar "
            "subquery in a column-expression context is deprecated"
        ):
            s = select([self.table1.c.myid]).alias()
            self.assert_compile(
                select([self.table1.c.myid]).where(self.table1.c.myid == s),
                "SELECT mytable.myid FROM mytable WHERE "
                "mytable.myid = (SELECT mytable.myid FROM "
                "mytable)",
            )

        with testing.expect_deprecated(
            "coercing SELECT object to scalar "
            "subquery in a column-expression context is deprecated"
        ):
            self.assert_compile(
                select([self.table1.c.myid]).where(s > self.table1.c.myid),
                "SELECT mytable.myid FROM mytable WHERE "
                "mytable.myid < (SELECT mytable.myid FROM "
                "mytable)",
            )

        with testing.expect_deprecated(
            "coercing SELECT object to scalar "
            "subquery in a column-expression context is deprecated"
        ):
            s = select([self.table1.c.myid]).alias()
            self.assert_compile(
                select([self.table1.c.myid]).where(self.table1.c.myid == s),
                "SELECT mytable.myid FROM mytable WHERE "
                "mytable.myid = (SELECT mytable.myid FROM "
                "mytable)",
            )

        with testing.expect_deprecated(
            "coercing SELECT object to scalar "
            "subquery in a column-expression context is deprecated"
        ):
            self.assert_compile(
                select([self.table1.c.myid]).where(s > self.table1.c.myid),
                "SELECT mytable.myid FROM mytable WHERE "
                "mytable.myid < (SELECT mytable.myid FROM "
                "mytable)",
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


class TextTest(fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = "default"

    def test_legacy_bindparam(self):
        with testing.expect_deprecated(
            "The text.bindparams parameter is deprecated"
        ):
            t = text(
                "select * from foo where lala=:bar and hoho=:whee",
                bindparams=[bindparam("bar", 4), bindparam("whee", 7)],
            )

        self.assert_compile(
            t,
            "select * from foo where lala=:bar and hoho=:whee",
            checkparams={"bar": 4, "whee": 7},
        )

    def test_legacy_typemap(self):
        table1 = table(
            "mytable",
            column("myid", Integer),
            column("name", String),
            column("description", String),
        )
        with testing.expect_deprecated(
            "The text.typemap parameter is deprecated"
        ):
            t = text(
                "select id, name from user",
                typemap=dict(id=Integer, name=String),
            ).subquery()

        stmt = select([table1.c.myid]).select_from(
            table1.join(t, table1.c.myid == t.c.id)
        )
        compiled = stmt.compile()
        eq_(
            compiled._create_result_map(),
            {
                "myid": (
                    "myid",
                    (table1.c.myid, "myid", "myid"),
                    table1.c.myid.type,
                )
            },
        )

    def test_autocommit(self):
        with testing.expect_deprecated(
            "The text.autocommit parameter is deprecated"
        ):
            text("select id, name from user", autocommit=True)


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
                    (table1.c.myid, "myid", "myid"),
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
