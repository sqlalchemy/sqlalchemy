from sqlalchemy import Column
from sqlalchemy import exc
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import select
from sqlalchemy import Table
from sqlalchemy import text
from sqlalchemy.schema import DDL
from sqlalchemy.schema import Sequence
from sqlalchemy.sql import ClauseElement
from sqlalchemy.sql import coercions
from sqlalchemy.sql import column
from sqlalchemy.sql import false
from sqlalchemy.sql import False_
from sqlalchemy.sql import literal
from sqlalchemy.sql import roles
from sqlalchemy.sql import true
from sqlalchemy.sql import True_
from sqlalchemy.sql.coercions import expect
from sqlalchemy.sql.elements import _truncated_label
from sqlalchemy.sql.elements import Null
from sqlalchemy.testing import assert_raises_message
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
                select([column("q")]).scalar_subquery(),
            ).compare(select([column("q")]).label(None))
        )

        is_true(
            expect(roles.LabeledColumnExprRole, not_a_thing1).compare(
                literal(not_a_thing1).label(None)
            )
        )

    def test_scalar_select_no_coercion(self):
        # this is also tested in test/sql/test_deprecations.py; when the
        # deprecation is turned to an error, those tests go away, and these
        # will assert the correct exception plus informative error message.
        assert_raises_message(
            exc.SADeprecationWarning,
            "coercing SELECT object to scalar subquery in a column-expression "
            "context is deprecated",
            expect,
            roles.LabeledColumnExprRole,
            select([column("q")]),
        )

        assert_raises_message(
            exc.SADeprecationWarning,
            "coercing SELECT object to scalar subquery in a column-expression "
            "context is deprecated",
            expect,
            roles.LabeledColumnExprRole,
            select([column("q")]).alias(),
        )

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

    def test_statement_coercion_select(self):
        is_true(
            expect(roles.CoerceTextStatementRole, select([t])).compare(
                select([t])
            )
        )

    def test_statement_coercion_ddl(self):
        d1 = DDL("hi")
        is_(expect(roles.CoerceTextStatementRole, d1), d1)

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
