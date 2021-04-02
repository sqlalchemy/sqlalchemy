from sqlalchemy import bindparam
from sqlalchemy import Column
from sqlalchemy import column
from sqlalchemy import exc
from sqlalchemy import func
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import select
from sqlalchemy import String
from sqlalchemy import Table
from sqlalchemy import table
from sqlalchemy import testing
from sqlalchemy import text
from sqlalchemy import update
from sqlalchemy.schema import DDL
from sqlalchemy.schema import Sequence
from sqlalchemy.sql import ClauseElement
from sqlalchemy.sql import coercions
from sqlalchemy.sql import false
from sqlalchemy.sql import False_
from sqlalchemy.sql import literal
from sqlalchemy.sql import roles
from sqlalchemy.sql import true
from sqlalchemy.sql import True_
from sqlalchemy.sql.coercions import expect
from sqlalchemy.sql.elements import _truncated_label
from sqlalchemy.sql.elements import Null
from sqlalchemy.sql.selectable import FromGrouping
from sqlalchemy.sql.selectable import ScalarSelect
from sqlalchemy.sql.selectable import SelectStatementGrouping
from sqlalchemy.testing import assert_raises
from sqlalchemy.testing import assert_raises_message
from sqlalchemy.testing import AssertsCompiledSQL
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import is_
from sqlalchemy.testing import is_instance_of
from sqlalchemy.testing import is_true

m = MetaData()

t = Table("t", m, Column("q", Integer))


class NotAThing1(object):
    pass


not_a_thing1 = NotAThing1()


class NotAThing2(ClauseElement):
    pass


not_a_thing2 = NotAThing2()


class NotAThing3(object):
    def __clause_element__(self):
        return not_a_thing2


not_a_thing3 = NotAThing3()


class RoleTest(fixtures.TestBase):
    # TODO: the individual role tests here are incomplete.  The functionality
    # of each role is covered by other tests in the sql testing suite however
    # ideally they'd all have direct tests here as well.

    def _test_role_neg_comparisons(self, role):
        impl = coercions._impl_lookup[role]
        role_name = impl.name

        assert_raises_message(
            exc.ArgumentError,
            r"%s expected, got .*NotAThing1" % role_name,
            expect,
            role,
            not_a_thing1,
        )

        assert_raises_message(
            exc.ArgumentError,
            r"%s expected, got .*NotAThing2" % role_name,
            expect,
            role,
            not_a_thing2,
        )

        assert_raises_message(
            exc.ArgumentError,
            r"%s expected, got .*NotAThing3" % role_name,
            expect,
            role,
            not_a_thing3,
        )

        assert_raises_message(
            exc.ArgumentError,
            r"%s expected for argument 'foo'; got .*NotAThing3" % role_name,
            expect,
            role,
            not_a_thing3,
            argname="foo",
        )

    def test_const_expr_role(self):
        t = true()
        is_(expect(roles.ConstExprRole, t), t)

        f = false()
        is_(expect(roles.ConstExprRole, f), f)

        is_instance_of(expect(roles.ConstExprRole, True), True_)

        is_instance_of(expect(roles.ConstExprRole, False), False_)

        is_instance_of(expect(roles.ConstExprRole, None), Null)

    def test_truncated_label_role(self):
        is_instance_of(
            expect(roles.TruncatedLabelRole, "foobar"), _truncated_label
        )

    def test_labeled_column_expr_role(self):
        c = column("q")
        is_true(expect(roles.LabeledColumnExprRole, c).compare(c))

        is_true(
            expect(roles.LabeledColumnExprRole, c.label("foo")).compare(
                c.label("foo")
            )
        )

        is_true(
            expect(
                roles.LabeledColumnExprRole,
                select(column("q")).scalar_subquery(),
            ).compare(select(column("q")).label(None))
        )

        is_true(
            expect(roles.LabeledColumnExprRole, not_a_thing1).compare(
                literal(not_a_thing1).label(None)
            )
        )

    def test_untyped_scalar_subquery(self):
        """test for :ticket:`6181` """

        c = column("q")
        subq = select(c).scalar_subquery()

        assert isinstance(
            subq._with_binary_element_type(Integer()), ScalarSelect
        )

        expr = column("a", Integer) == subq
        assert isinstance(expr.right, ScalarSelect)

    def test_no_clauseelement_in_bind(self):
        with testing.expect_raises_message(
            exc.ArgumentError,
            r"Literal Python value expected, got BindParameter",
        ):
            literal(bindparam("x"))

        with testing.expect_raises_message(
            exc.ArgumentError,
            r"Literal Python value expected, got .*ColumnClause",
        ):
            literal(column("q"))

    def test_scalar_select_no_coercion(self):
        with testing.expect_warnings(
            "implicitly coercing SELECT object to scalar subquery"
        ):
            expect(
                roles.LabeledColumnExprRole,
                select(column("q")),
            )

        with testing.expect_warnings(
            "implicitly coercing SELECT object to scalar subquery"
        ):
            expect(
                roles.LabeledColumnExprRole,
                select(column("q")).alias(),
            )

    def test_table_valued_advice(self):
        msg = (
            r"SQL expression element expected, got %s. To create a "
            r"column expression from a FROM clause row as a whole, "
            r"use the .table_valued\(\) method."
        )
        assert_raises_message(
            exc.ArgumentError,
            msg % ("Table.*",),
            expect,
            roles.ExpressionElementRole,
            t,
        )

        # no table_valued() message here right now, it goes to scalar subquery
        with testing.expect_warnings(
            "implicitly coercing SELECT object to scalar subquery"
        ):
            expect(roles.ExpressionElementRole, t.select().alias())

    def test_statement_no_text_coercion(self):
        assert_raises_message(
            exc.ArgumentError,
            r"Textual SQL expression 'select \* from table' should be "
            r"explicitly declared",
            expect,
            roles.StatementRole,
            "select * from table",
        )

    def test_statement_text_coercion(self):
        with testing.expect_deprecated_20(
            "Using plain strings to indicate SQL statements"
        ):
            is_true(
                expect(
                    roles.CoerceTextStatementRole, "select * from table"
                ).compare(text("select * from table"))
            )

    def test_select_statement_no_text_coercion(self):
        assert_raises_message(
            exc.ArgumentError,
            r"Textual SQL expression 'select \* from table' should be "
            r"explicitly declared",
            expect,
            roles.SelectStatementRole,
            "select * from table",
        )

    def test_select_is_coerced_into_fromclause_w_deprecation(self):
        with testing.expect_deprecated(
            "Implicit coercion of SELECT and textual SELECT "
            "constructs into FROM clauses is deprecated;"
        ):
            element = expect(
                roles.FromClauseRole, SelectStatementGrouping(select(t))
            )
            is_true(
                element.compare(SelectStatementGrouping(select(t)).subquery())
            )

    def test_offset_or_limit_role_only_ints_or_clauseelement(self):
        assert_raises(ValueError, select(t).limit, "some limit")

        assert_raises(ValueError, select(t).offset, "some offset")

    def test_offset_or_limit_role_clauseelement(self):
        bind = bindparam("x")
        stmt = select(t).limit(bind)
        is_(stmt._limit_clause, bind)

        stmt = select(t).offset(bind)
        is_(stmt._offset_clause, bind)

    def test_from_clause_is_not_a_select(self):
        assert_raises_message(
            exc.ArgumentError,
            r"SELECT construct or equivalent text\(\) construct expected,",
            expect,
            roles.SelectStatementRole,
            FromGrouping(t),
        )

    def test_text_as_from_select_statement(self):
        is_true(
            expect(
                roles.SelectStatementRole,
                text("select * from table").columns(t.c.q),
            ).compare(text("select * from table").columns(t.c.q))
        )

    def test_statement_coercion_select(self):
        is_true(
            expect(roles.CoerceTextStatementRole, select(t)).compare(select(t))
        )

    def test_statement_coercion_ddl(self):
        d1 = DDL("hi")
        is_(expect(roles.CoerceTextStatementRole, d1), d1)

    def test_strict_from_clause_role(self):
        stmt = select(t).subquery()
        is_true(
            expect(roles.StrictFromClauseRole, stmt).compare(
                select(t).subquery()
            )
        )

    def test_strict_from_clause_role_disallow_select(self):
        stmt = select(t)
        assert_raises_message(
            exc.ArgumentError,
            r"FROM expression, such as a Table or alias\(\) "
            "object expected, got .*Select",
            expect,
            roles.StrictFromClauseRole,
            stmt,
        )

    def test_anonymized_from_clause_role(self):
        is_true(expect(roles.AnonymizedFromClauseRole, t).compare(t.alias()))

        # note the compare for subquery().alias(), even if it is two
        # plain Alias objects (which it won't be once we introduce the
        # Subquery class), still compares based on alias() being present
        # twice, that is, alias().alias() builds an alias of an alias, rather
        # than just replacing the outer alias.
        is_true(
            expect(
                roles.AnonymizedFromClauseRole, select(t).subquery()
            ).compare(select(t).subquery().alias())
        )

    def test_statement_coercion_sequence(self):
        s1 = Sequence("hi")
        is_(expect(roles.CoerceTextStatementRole, s1), s1)

    def test_columns_clause_role(self):
        is_(expect(roles.ColumnsClauseRole, t.c.q), t.c.q)

    def test_truncated_label_role_neg(self):
        self._test_role_neg_comparisons(roles.TruncatedLabelRole)

    def test_where_having_role_neg(self):
        self._test_role_neg_comparisons(roles.WhereHavingRole)

    def test_by_of_role_neg(self):
        self._test_role_neg_comparisons(roles.ByOfRole)

    def test_const_expr_role_neg(self):
        self._test_role_neg_comparisons(roles.ConstExprRole)

    def test_columns_clause_role_neg(self):
        self._test_role_neg_comparisons(roles.ColumnsClauseRole)


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

    def test_column_roles(self):
        stmt = select(self.table1.c.myid)

        for role in [
            roles.WhereHavingRole,
            roles.ExpressionElementRole,
            roles.ByOfRole,
            roles.OrderByRole,
            # roles.LabeledColumnExprRole
        ]:
            with testing.expect_warnings(
                "implicitly coercing SELECT object to scalar subquery"
            ):
                coerced = coercions.expect(role, stmt)
                is_true(coerced.compare(stmt.scalar_subquery()))

            with testing.expect_warnings(
                "implicitly coercing SELECT object to scalar subquery"
            ):
                coerced = coercions.expect(role, stmt.alias())
                is_true(coerced.compare(stmt.scalar_subquery()))

    def test_labeled_role(self):
        stmt = select(self.table1.c.myid)

        with testing.expect_warnings(
            "implicitly coercing SELECT object to scalar subquery"
        ):
            coerced = coercions.expect(roles.LabeledColumnExprRole, stmt)
            is_true(coerced.compare(stmt.scalar_subquery().label(None)))

        with testing.expect_warnings(
            "implicitly coercing SELECT object to scalar subquery"
        ):
            coerced = coercions.expect(
                roles.LabeledColumnExprRole, stmt.alias()
            )
            is_true(coerced.compare(stmt.scalar_subquery().label(None)))

    def test_scalar_select(self):

        with testing.expect_warnings(
            "implicitly coercing SELECT object to scalar subquery"
        ):
            self.assert_compile(
                func.coalesce(select(self.table1.c.myid)),
                "coalesce((SELECT mytable.myid FROM mytable))",
            )

        with testing.expect_warnings(
            "implicitly coercing SELECT object to scalar subquery"
        ):
            s = select(self.table1.c.myid).alias()
            self.assert_compile(
                select(self.table1.c.myid).where(self.table1.c.myid == s),
                "SELECT mytable.myid FROM mytable WHERE "
                "mytable.myid = (SELECT mytable.myid FROM "
                "mytable)",
            )

        with testing.expect_warnings(
            "implicitly coercing SELECT object to scalar subquery"
        ):
            self.assert_compile(
                select(self.table1.c.myid).where(s > self.table1.c.myid),
                "SELECT mytable.myid FROM mytable WHERE "
                "mytable.myid < (SELECT mytable.myid FROM "
                "mytable)",
            )

        with testing.expect_warnings(
            "implicitly coercing SELECT object to scalar subquery"
        ):
            s = select(self.table1.c.myid).alias()
            self.assert_compile(
                select(self.table1.c.myid).where(self.table1.c.myid == s),
                "SELECT mytable.myid FROM mytable WHERE "
                "mytable.myid = (SELECT mytable.myid FROM "
                "mytable)",
            )

        with testing.expect_warnings(
            "implicitly coercing SELECT object to scalar subquery"
        ):
            self.assert_compile(
                select(self.table1.c.myid).where(s > self.table1.c.myid),
                "SELECT mytable.myid FROM mytable WHERE "
                "mytable.myid < (SELECT mytable.myid FROM "
                "mytable)",
            )

    @testing.fixture()
    def update_from_fixture(self):
        metadata = MetaData()

        mytable = Table(
            "mytable",
            metadata,
            Column("myid", Integer),
            Column("name", String(30)),
            Column("description", String(50)),
        )
        myothertable = Table(
            "myothertable",
            metadata,
            Column("otherid", Integer),
            Column("othername", String(30)),
        )
        return mytable, myothertable

    def test_correlated_update_two(self, update_from_fixture):
        table1, t2 = update_from_fixture

        mt = table1.alias()
        with testing.expect_warnings(
            "implicitly coercing SELECT object to scalar subquery"
        ):
            u = update(table1).values(
                {
                    table1.c.name: select(mt.c.name).where(
                        mt.c.myid == table1.c.myid
                    )
                },
            )
        self.assert_compile(
            u,
            "UPDATE mytable SET name=(SELECT mytable_1.name FROM "
            "mytable AS mytable_1 WHERE "
            "mytable_1.myid = mytable.myid)",
        )

    def test_correlated_update_three(self, update_from_fixture):
        table1, table2 = update_from_fixture

        # test against a regular constructed subquery
        s = select(table2).where(table2.c.otherid == table1.c.myid)
        with testing.expect_warnings(
            "implicitly coercing SELECT object to scalar subquery"
        ):
            u = (
                update(table1)
                .where(table1.c.name == "jack")
                .values({table1.c.name: s})
            )
        self.assert_compile(
            u,
            "UPDATE mytable SET name=(SELECT myothertable.otherid, "
            "myothertable.othername FROM myothertable WHERE "
            "myothertable.otherid = mytable.myid) "
            "WHERE mytable.name = :name_1",
        )
