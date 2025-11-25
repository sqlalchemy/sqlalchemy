from sqlalchemy import alias
from sqlalchemy import and_
from sqlalchemy import bindparam
from sqlalchemy import CHAR
from sqlalchemy import column
from sqlalchemy import exc
from sqlalchemy import ForeignKey
from sqlalchemy import func
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
from sqlalchemy.engine import default
from sqlalchemy.sql import coercions
from sqlalchemy.sql import LABEL_STYLE_TABLENAME_PLUS_COL
from sqlalchemy.sql import operators
from sqlalchemy.sql import quoted_name
from sqlalchemy.sql import roles
from sqlalchemy.sql import visitors
from sqlalchemy.sql.selectable import SelectStatementGrouping
from sqlalchemy.testing import assertions
from sqlalchemy.testing import AssertsCompiledSQL
from sqlalchemy.testing import config
from sqlalchemy.testing import eq_
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import is_
from sqlalchemy.testing import is_true
from sqlalchemy.testing import mock
from sqlalchemy.testing.provision import normalize_sequence
from sqlalchemy.testing.schema import Column
from sqlalchemy.testing.schema import Table


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


class DeprecationWarningsTest(fixtures.TestBase, AssertsCompiledSQL):
    __sparse_driver_backend__ = True

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

    def test_empty_and_or(self):
        with testing.expect_deprecated(
            r"Invoking and_\(\) without arguments is deprecated, and "
            r"will be disallowed in a future release.   For an empty "
            r"and_\(\) construct, use 'and_\(true\(\), \*args\)' or "
            r"'and_\(True, \*args\)'"
        ):
            self.assert_compile(or_(and_()), "")

    @testing.combinations(
        (schema.Column),
        (schema.UniqueConstraint,),
        (schema.PrimaryKeyConstraint,),
        (schema.CheckConstraint,),
        (schema.ForeignKeyConstraint,),
        (schema.ForeignKey,),
        (schema.Identity,),
    )
    def test_copy_dep_warning(self, cls):
        obj = cls.__new__(cls)
        with mock.patch.object(cls, "_copy") as _copy:
            with testing.expect_deprecated(
                r"The %s\(\) method is deprecated" % cls.copy.__qualname__
            ):
                obj.copy(schema="s", target_table="tt", arbitrary="arb")

        eq_(
            _copy.mock_calls,
            [mock.call(target_table="tt", schema="s", arbitrary="arb")],
        )


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

    def test_froms_renamed(self):
        t1 = table("t1", column("q"))

        stmt = select(t1)

        with testing.expect_deprecated(
            r"The Select.froms attribute is moved to the "
            r"Select.get_final_froms\(\) method."
        ):
            eq_(stmt.froms, [t1])

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
            r"The Selectable.replace_selectable\(\) method is deprecated"
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


class KeyTargetingTest(fixtures.TablesTest):
    run_inserts = "once"
    run_deletes = None
    __sparse_driver_backend__ = True

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


class PKIncrementTest(fixtures.TablesTest):
    run_define_tables = "each"
    __sparse_driver_backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "aitable",
            metadata,
            Column(
                "id",
                Integer,
                normalize_sequence(
                    config, Sequence("ai_id_seq", optional=True)
                ),
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
            column("x").notin_(["foo", "bar"]),
            "(x NOT IN (__[POSTCOMPILE_x_1]))",
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
        # (modern, legacy, in_precedence)
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


class FutureSelectTest(fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = "default"

    @testing.fixture
    def table_fixture(self):
        table1 = table(
            "mytable",
            column("myid", Integer),
            column("name", String),
            column("description", String),
        )

        table2 = table(
            "myothertable",
            column("otherid", Integer),
            column("othername", String),
        )
        return table1, table2
